# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — OpenFDA Adverse Events (Full Ingestion)
# MAGIC ### Pharmacovigilance & Regulatory Compliance Pipeline — Databricks Free Edition
# MAGIC
# MAGIC Migrated from Azure Blob Storage to Unity Catalog Volumes — no cloud storage
# MAGIC account needed. Files are written directly to a managed Volume, which is a
# MAGIC POSIX-style file path Databricks provides for free within Unity Catalog.

# COMMAND ----------

import requests
import json
import os
import time
from datetime import date, timedelta

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0 — Config

# COMMAND ----------

catalog_name = "pharmacovigilance_ws"
bronze_schema = "bronze"
volume_name = "raw_adverse_events"

# This is the Volume's root path — behaves like a normal filesystem path
volume_root = f"/Volumes/{catalog_name}/{bronze_schema}/{volume_name}"

secret_scope = "pharmacovigilance"

start_date = date(2024, 1, 1)
end_date = date(2024, 4, 16)

page_limit = 1000
max_skip_per_query = 25000
request_pause_seconds = 0.3
max_retries = 3

openfda_api_key = dbutils.secrets.get(scope=secret_scope, key="openfda_api_key")

skip_existing = True

request_headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
}

print("Volume root path:", volume_root)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Weekly date ranges

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Fetch one page, with retries

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
# MAGIC ## Step 3 — Write one page to the Volume
# MAGIC
# MAGIC `/Volumes/...` paths support normal Python file I/O — `os.makedirs` and
# MAGIC `open()` work exactly like a local filesystem. No SDK, no credentials.

# COMMAND ----------

def upload_page(json_data, week_start, part_number):
    year = week_start.year
    month = week_start.month
    dir_path = f"{volume_root}/adverse_events/{year}/{month:02d}"
    os.makedirs(dir_path, exist_ok=True)

    file_path = f"{dir_path}/events_{week_start.isoformat()}_part{part_number}.json"
    with open(file_path, "w") as f:
        json.dump(json_data, f)
    return file_path

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3b — Check if a week was already ingested

# COMMAND ----------

def week_already_ingested(week_start):
    year = week_start.year
    month = week_start.month
    dir_path = f"{volume_root}/adverse_events/{year}/{month:02d}"
    prefix = f"events_{week_start.isoformat()}_part"
    if not os.path.isdir(dir_path):
        return False
    return any(f.startswith(prefix) for f in os.listdir(dir_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3c — Track last run's end_date

# COMMAND ----------

metadata_file_path = f"{volume_root}/_metadata/last_run.json"

def read_last_end_date():
    if os.path.exists(metadata_file_path):
        with open(metadata_file_path) as f:
            return date.fromisoformat(json.load(f)["end_date"])
    return None

def write_last_end_date(value):
    os.makedirs(os.path.dirname(metadata_file_path), exist_ok=True)
    with open(metadata_file_path, "w") as f:
        json.dump({"end_date": value.isoformat()}, f)

last_end_date = read_last_end_date()
end_date_changed = (last_end_date is None) or (end_date != last_end_date)

print(f"Last run's end_date: {last_end_date}")
print(f"This run's end_date: {end_date}")
print(f"end_date changed since last run: {end_date_changed}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Main ingestion loop

# COMMAND ----------

ingestion_log = []

for i, (week_start, week_end) in enumerate(weekly_ranges):
    is_most_recent_week = (i == len(weekly_ranges) - 1)
    print(f"\nWeek {week_start} to {week_end}")

    force_refetch_this_week = is_most_recent_week and end_date_changed

    if skip_existing and not force_refetch_this_week and week_already_ingested(week_start):
        print("  Already ingested — skipping (no OpenFDA calls made).")
        ingestion_log.append({"week_start": str(week_start), "records": "skipped", "pages": "skipped", "hit_ceiling": False})
        continue
    elif force_refetch_this_week:
        print("  Most recent week — end_date changed since last run, re-fetching to capture new days.")

    search_query = f"receiptdate:[{week_start.strftime('%Y%m%d')}+TO+{week_end.strftime('%Y%m%d')}]"

    first_page = fetch_page(search_query, skip=0, limit=page_limit)
    total_available = first_page.get("meta", {}).get("results", {}).get("total", 0)
    print(f"  Total records available: {total_available}")

    if total_available == 0:
        ingestion_log.append({"week_start": str(week_start), "records": 0, "pages": 0, "hit_ceiling": False})
        continue

    hit_ceiling = total_available > max_skip_per_query
    if hit_ceiling:
        print(f"  WARNING: this week has more records than the {max_skip_per_query} pagination ceiling.")

    records_pulled = 0
    part_number = 0

    results = first_page.get("results", [])
    if results:
        file_path = upload_page(first_page, week_start, part_number)
        records_pulled += len(results)
        part_number += 1
        print(f"    Wrote part {part_number}: {len(results)} records -> {file_path}")

    skip = page_limit
    while skip < min(total_available, max_skip_per_query):
        time.sleep(request_pause_seconds)
        page = fetch_page(search_query, skip=skip, limit=page_limit)
        results = page.get("results", [])
        if not results:
            break
        file_path = upload_page(page, week_start, part_number)
        records_pulled += len(results)
        part_number += 1
        print(f"    Wrote part {part_number}: {len(results)} records -> {file_path}")
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

# COMMAND ----------

skipped_weeks = [w["week_start"] for w in ingestion_log if w["records"] == "skipped"]
processed = [w for w in ingestion_log if w["records"] != "skipped"]

total_records = sum(w["records"] for w in processed)
total_pages = sum(w["pages"] for w in processed)
weeks_at_ceiling = [w["week_start"] for w in processed if w["hit_ceiling"]]

print("=== Ingestion summary ===")
print(f"Weeks total: {len(ingestion_log)} | skipped: {len(skipped_weeks)} | newly processed: {len(processed)}")
print(f"Total records written this run: {total_records}")
print(f"Total JSON files written this run: {total_pages}")
print(f"Weeks that hit the pagination ceiling: {weeks_at_ceiling if weeks_at_ceiling else 'none'}")

write_last_end_date(end_date)
print(f"\nSaved end_date={end_date} as this run's checkpoint for next time.")