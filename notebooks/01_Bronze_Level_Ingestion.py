# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — OpenFDA Adverse Events (Full Ingestion)
# MAGIC ### Pharmacovigilance & Regulatory Compliance Pipeline
# MAGIC
# MAGIC Upgrades the earlier single-file test script into a real ingestion job:
# MAGIC - Loops across a date range in **weekly chunks** (keeps each query's result count
# MAGIC   safely under OpenFDA's 25,000 skip+limit ceiling)
# MAGIC - **Paginates** within each week using `limit` + `skip` until all matching records
# MAGIC   for that week are pulled
# MAGIC - **Rate-limits** itself with small pauses and retries failed requests
# MAGIC - Writes each page as its own JSON blob into `bronze/adverse_events/YYYY/MM/...`
# MAGIC   using the `azure-storage-blob` SDK directly — this bypasses Spark entirely,
# MAGIC   so it works the same whether you're on Serverless or a cluster.

# COMMAND ----------

import requests
import json
import time
from azure.storage.blob import BlobServiceClient
from datetime import date, timedelta

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0 — Config
# MAGIC
# MAGIC Adjust `start_date` / `end_date` to control how much history you pull. Wider
# MAGIC ranges mean more requests and longer runtime — start with a few months while
# MAGIC you're validating, then widen once you're confident it's stable.

# COMMAND ----------

storage_account_name = "pharmacovigilance01"
container_name = "bronze"
secret_scope = "pharmacovigilance"
secret_key_name = "adls_pharmacovigilance01_key"

start_date = date(2024, 1, 1)
end_date = date(2024, 4, 10)   # ~13 weeks + 10 days of data — a reasonable first real pull

page_limit = 1000              # OpenFDA's max records per request
max_skip_per_query = 25000     # OpenFDA's hard ceiling on skip + limit combined
request_pause_seconds = 0.3    # small pause between requests, stays well under rate limits
max_retries = 3

# Optional but recommended: get a free key at https://open.fda.gov/apis/authentication/
# Raises your rate limit and makes 403 bot-protection blocks far less likely.
# Leave as None to run without one.
openfda_api_key = dbutils.secrets.get(scope=secret_scope, key="openfda_api_key")

# If True, weeks that already have files in Bronze are skipped entirely — no OpenFDA
# calls made for them at all. Set to False to force a full re-pull of every week
# regardless of what's already there (e.g. if you suspect a prior run was incomplete).
skip_existing = True

# A default requests User-Agent (python-requests/x.x) can get blocked by OpenFDA's
# edge bot-protection with a 403, even though nothing is actually wrong with the
# request itself. Sending a normal browser-style header avoids this.
request_headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
}

# COMMAND ----------

access_key = dbutils.secrets.get(scope=secret_scope, key=secret_key_name)
blob_service_client = BlobServiceClient(
    account_url=f"https://{storage_account_name}.blob.core.windows.net",
    credential=access_key
)
container_client = blob_service_client.get_container_client(container_name)

print("Connected to storage account:", storage_account_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Build the list of weekly date ranges to pull
# MAGIC
# MAGIC OpenFDA's `receiptdate` field uses `YYYYMMDD` format inside the search query.
# MAGIC We generate `[start, end)` pairs one week apart, from `start_date` to `end_date`.

# COMMAND ----------

def generate_weekly_ranges(start, end):
    ranges = []
    current = start
    while current < end:
        week_end = min(current + timedelta(days=7), end)
        ranges.append((current, week_end))
        current = week_end
    return ranges

weekly_ranges = generate_weekly_ranges(start_date, end_date)
print(f"Generated {len(weekly_ranges)} weekly chunks from {start_date} to {end_date}")
for r in weekly_ranges[:3]:
    print(" ", r)
print("  ...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Helper: fetch one page, with retries
# MAGIC
# MAGIC A single function that hits the API once and retries on transient failures
# MAGIC (network blips, momentary 5xx errors from OpenFDA). It does NOT retry on a
# MAGIC clean "no results" response — that's a valid outcome, not a failure.

# COMMAND ----------

def fetch_page(search_query, skip, limit, retries_left=max_retries):
    api_url = (
        "https://api.fda.gov/drug/event.json"
        f"?search={search_query}&limit={limit}&skip={skip}"
    )
    if openfda_api_key:
        api_url += f"&api_key={openfda_api_key}"
    try:
        response = requests.get(api_url, headers=request_headers, timeout=30)
        if response.status_code == 404:
            # OpenFDA returns 404 when a query matches zero records — not an error
            return {"meta": {"results": {"total": 0}}, "results": []}
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        if retries_left > 0:
            print(f"    Request failed ({e}), retrying... ({retries_left} left)")
            time.sleep(2)
            return fetch_page(search_query, skip, limit, retries_left - 1)
        else:
            print(f"    Request failed after all retries: {e}")
            raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Helper: upload one page as a blob
# MAGIC
# MAGIC Path pattern: `adverse_events/{year}/{month:02d}/events_{week_start}_part{n}.json`
# MAGIC — this is exactly the partitioned structure your Silver notebook's wildcard
# MAGIC path (`adverse_events/*/*/*.json`) expects.

# COMMAND ----------

def upload_page(json_data, week_start, part_number):
    year = week_start.year
    month = week_start.month
    blob_path = f"adverse_events/{year}/{month:02d}/events_{week_start.isoformat()}_part{part_number}.json"

    blob_client = container_client.get_blob_client(blob_path)
    blob_client.upload_blob(json.dumps(json_data), overwrite=True)
    return blob_path

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3b — Helper: check if a week was already ingested
# MAGIC
# MAGIC Looks for any blob whose name starts with this week's prefix
# MAGIC (`adverse_events/{year}/{month}/events_{week_start}_part`). If even one exists,
# MAGIC we treat the whole week as already done and skip it — cheap check (one API call
# MAGIC to storage), no OpenFDA requests spent on it at all.

# COMMAND ----------

def week_already_ingested(week_start):
    year = week_start.year
    month = week_start.month
    prefix = f"adverse_events/{year}/{month:02d}/events_{week_start.isoformat()}_part"
    existing = list(container_client.list_blobs(name_starts_with=prefix))
    return len(existing) > 0

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Main ingestion loop
# MAGIC
# MAGIC For each week: query the total record count first (from `meta.results.total`),
# MAGIC then page through with `skip` in steps of `page_limit` until either all records
# MAGIC are pulled or we hit `max_skip_per_query`. If a week hits that ceiling, it's
# MAGIC flagged in the summary so you know that week's data may be incomplete and could
# MAGIC be re-pulled in daily chunks later.

# COMMAND ----------

ingestion_log = []

for i, (week_start, week_end) in enumerate(weekly_ranges):
    is_most_recent_week = (i == len(weekly_ranges) - 1)
    print(f"\nWeek {week_start} to {week_end}")

    # The most recent chunk is always re-fetched, even if skip_existing is True —
    # this protects against end_date having grown since the last run (e.g. adding
    # a few more days within the same week). Blob uploads use overwrite=True below,
    # so re-fetching already-known days is harmless and just re-writes the same data.
    if skip_existing and not is_most_recent_week and week_already_ingested(week_start):
        print("  Already ingested — skipping (no OpenFDA calls made).")
        ingestion_log.append({"week_start": str(week_start), "records": "skipped", "pages": "skipped", "hit_ceiling": False})
        continue
    
    elif is_most_recent_week and skip_existing:
        print("  Most recent week — always re-fetched, even if files already exist.")

    search_query = f"receiptdate:[{week_start.strftime('%Y%m%d')}+TO+{week_end.strftime('%Y%m%d')}]"

    # First request tells us the total record count for this week via meta.results.total
    first_page = fetch_page(search_query, skip=0, limit=page_limit)
    total_available = first_page.get("meta", {}).get("results", {}).get("total", 0)
    print(f"  Total records available: {total_available}")

    if total_available == 0:
        ingestion_log.append({"week_start": str(week_start), "records": 0, "pages": 0, "hit_ceiling": False})
        continue

    hit_ceiling = total_available > max_skip_per_query
    if hit_ceiling:
        print(f"  WARNING: this week has more records than the {max_skip_per_query} pagination ceiling.")
        print(f"  Only the first {max_skip_per_query} will be pulled. Consider daily chunks for this week later.")

    records_pulled = 0
    part_number = 0
    skip = 0

    # Upload the first page we already fetched above
    results = first_page.get("results", [])
    if results:
        blob_path = upload_page(first_page, week_start, part_number)
        records_pulled += len(results)
        part_number += 1
        print(f"    Wrote part {part_number}: {len(results)} records -> {blob_path}")

    skip = page_limit
    while skip < min(total_available, max_skip_per_query):
        time.sleep(request_pause_seconds)
        page = fetch_page(search_query, skip=skip, limit=page_limit)
        results = page.get("results", [])
        if not results:
            break
        blob_path = upload_page(page, week_start, part_number)
        records_pulled += len(results)
        part_number += 1
        print(f"    Wrote part {part_number}: {len(results)} records -> {blob_path}")
        skip += page_limit

    ingestion_log.append({
        "week_start": str(week_start),
        "records": records_pulled,
        "pages": part_number,
        "hit_ceiling": hit_ceiling,
    })

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Summary
# MAGIC
# MAGIC Quick sanity check before moving to Silver — total records pulled, and a flag
# MAGIC for any week that hit the pagination ceiling and may need a follow-up daily pull.

# COMMAND ----------

skipped_weeks = [w["week_start"] for w in ingestion_log if w["records"] == "skipped"]
processed = [w for w in ingestion_log if w["records"] != "skipped"]

total_records = sum(w["records"] for w in processed)
total_pages = sum(w["pages"] for w in processed)
weeks_at_ceiling = [w["week_start"] for w in processed if w["hit_ceiling"]]

print("=== Ingestion summary ===")
print(f"Weeks total: {len(ingestion_log)} | skipped (already existed): {len(skipped_weeks)} | newly processed: {len(processed)}")
print(f"Total records written this run: {total_records}")
print(f"Total JSON files written this run: {total_pages}")
print(f"Weeks that hit the pagination ceiling: {weeks_at_ceiling if weeks_at_ceiling else 'none'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recap & next steps
# MAGIC
# MAGIC Bronze now has a properly partitioned dataset at
# MAGIC `bronze/adverse_events/YYYY/MM/events_{week}_part{n}.json` spanning your
# MAGIC configured date range — ready for the Silver notebook's wildcard read path
# MAGIC exactly as originally designed, no changes needed there.
# MAGIC
# MAGIC If any weeks hit the pagination ceiling, note them — a good future exercise is
# MAGIC re-running just those weeks in daily chunks instead of weekly, to guarantee full
# MAGIC coverage even on unusually high-volume weeks.
