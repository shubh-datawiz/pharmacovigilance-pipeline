# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — OpenFDA Adverse Events
# MAGIC ### Pharmacovigilance & Regulatory Compliance Pipeline
# MAGIC
# MAGIC **Goal:** Read raw Bronze JSON, flatten nested `patient.drug[]` and `patient.reaction[]`
# MAGIC arrays using `explode()`, cleanse/standardize types, deduplicate, and write three
# MAGIC clean Delta tables to the `silver` container:
# MAGIC
# MAGIC 1. `silver_events`        — one row per adverse event report (safetyreportid)
# MAGIC 2. `silver_event_drug`    — one row per (event, drug) pair
# MAGIC 3. `silver_event_reaction`— one row per (event, reaction) pair
# MAGIC
# MAGIC These three tables are the direct feed for your Gold-layer Star Schema:
# MAGIC `Fact_Adverse_Event`, `Dim_Patient`, `Dim_Drug`, `Dim_Reaction`,
# MAGIC `Bridge_Event_Drug`, `Bridge_Event_Reaction`.
# MAGIC
# MAGIC **Cost note:** Every write below uses `.coalesce()` before saving. On a free-tier
# MAGIC single-node cluster, this avoids the "small file problem" (hundreds of tiny output
# MAGIC files that are slow and costly to list/read later) — keep this pattern for every
# MAGIC Silver/Gold write you build going forward.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0 — Config & Authentication
# MAGIC We reuse the same Databricks Secret Scope (`pharmacovigilance`) you already set up
# MAGIC for Bronze ingestion, so no credentials are hardcoded here.

# COMMAND ----------

from pyspark.sql import functions as F

# --- Storage account / container config ---
catalog_name = "pharmacovigilance_ws"

bronze_path = f"/Volumes/{catalog_name}/bronze/raw_adverse_events/adverse_events/*/*/*.json"


print("Bronze read path:", bronze_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0b — Skip if Silver already exists
# MAGIC
# MAGIC Unlike Bronze (which skips week by week), Silver always fully rebuilds all three
# MAGIC tables from every Bronze file in one pass — there's no natural "partial" unit to
# MAGIC skip. So instead we do one check right here: if all three Silver tables already
# MAGIC exist, we stop the whole notebook immediately with `dbutils.notebook.exit()`,
# MAGIC before any of the expensive read/flatten/write cells below even run.
# MAGIC
# MAGIC Set `force_refresh = True` whenever you actually want a full rebuild — e.g. after
# MAGIC widening Bronze's date range with new months, or if you suspect Silver is stale
# MAGIC or corrupted.

# COMMAND ----------

# --- Incremental load control table ---
# Tracks which Bronze files have already been processed into Silver, so re-runs
# only pick up genuinely new files instead of reprocessing everything.

spark.sql("CREATE SCHEMA IF NOT EXISTS silver")

spark.sql("""
    CREATE TABLE IF NOT EXISTS silver.load_log (
        file_path STRING,
        loaded_at TIMESTAMP
    ) USING DELTA
""")

def list_bronze_files(base_path):
    """Recursively lists all .json files under adverse_events/YYYY/MM/"""
    files = []
    for year_dir in dbutils.fs.ls(base_path):
        if year_dir.isDir():
            for month_dir in dbutils.fs.ls(year_dir.path):
                if month_dir.isDir():
                    for f in dbutils.fs.ls(month_dir.path):
                        if f.path.endswith(".json"):
                            files.append(f.path)
    return files

bronze_base_path = f"abfss://{bronze_container}@{storage_account}.dfs.core.windows.net/adverse_events/"
all_bronze_files = list_bronze_files(bronze_base_path)

already_loaded_files = set(
    row.file_path for row in spark.table("silver.load_log").select("file_path").collect()
)

new_bronze_files = [f for f in all_bronze_files if f not in already_loaded_files]

print(f"Total Bronze files found: {len(all_bronze_files)}")
print(f"Already processed (in load_log): {len(already_loaded_files)}")
print(f"New files to process this run: {len(new_bronze_files)}")

if not new_bronze_files:
    print("No new Bronze files since last run — nothing to do.")
    dbutils.notebook.exit("Skipped: no new Bronze files found")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Read raw Bronze JSON
# MAGIC
# MAGIC Two options worth knowing:
# MAGIC - `multiline = true` — each of your bronze files is a single JSON *object*
# MAGIC   (`{"meta": ..., "results": [...]}`) spread across multiple lines. Spark's default
# MAGIC   JSON reader expects one JSON object per line, so without this option it will
# MAGIC   fail to parse your files correctly.
# MAGIC - `columnNameOfCorruptRecord` — if any file is malformed (e.g. a truncated API
# MAGIC   response from a network hiccup during Bronze ingestion), Spark routes the raw
# MAGIC   text into this column instead of crashing the whole read. Cheap insurance.

# COMMAND ----------

from pyspark.sql.functions import input_file_name

df_raw_all = (
    spark.read
    .option("multiline", "true")
    .option("mode", "PERMISSIVE")
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    .json(bronze_path)
    .withColumn("_source_file", F.col("_metadata.file_path"))
)

df_raw = df_raw_all.filter(df_raw_all["_source_file"].isin(new_bronze_files))

print("Files have been parsed. Row count (1 row = 1 raw API response file):", df_raw.count())
df_raw.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Check quickly for any corrupted files before proceeding — worth a glance every run.

# COMMAND ----------

if "_corrupt_record" in df_raw.columns:
    corrupt_count = df_raw.filter(F.col("_corrupt_record").isNotNull()).count()
    print(f"Corrupt records found: {corrupt_count}")
else:
    print("No _corrupt_record column present — nothing was malformed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Explode `results[]` (one row per adverse event report)
# MAGIC
# MAGIC Each raw file is `{"meta": ..., "results": [event1, event2, ...]}`.
# MAGIC `explode("results")` turns the single `results` array into one row per event —
# MAGIC this is the first, outer level of flattening.

# COMMAND ----------

df_events_exploded = df_raw.select(F.explode("results").alias("event"))

print("Total individual adverse event reports:", df_events_exploded.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Flatten event + patient-level fields
# MAGIC
# MAGIC Dot notation (`event.patient.patientsex`) drills into nested structs — no join
# MAGIC required, since the nesting came from JSON, not from separate tables.
# MAGIC
# MAGIC We keep `drug_array` and `reaction_array` as-is here — we'll explode *those*
# MAGIC separately in Steps 4 and 5, then drop them from the final events table since
# MAGIC they'll live in their own tables.

# COMMAND ----------

df_events_flat = df_events_exploded.select(
    F.col("event.safetyreportid").alias("safety_report_id"),
    F.col("event.safetyreportversion").alias("safety_report_version"),
    F.try_to_date(F.col("event.receivedate"), "yyyyMMdd").alias("received_date"),
    F.try_to_date(F.col("event.receiptdate"), "yyyyMMdd").alias("receipt_date"),
    F.col("event.serious").alias("is_serious_flag"),
    F.col("event.seriousnessdeath").alias("seriousness_death_flag"),
    F.col("event.seriousnesshospitalization").alias("seriousness_hospitalization_flag"),
    F.col("event.seriousnesslifethreatening").alias("seriousness_life_threatening_flag"),
    F.col("event.patient.patientonsetage").cast("double").alias("patient_onset_age"),
    F.col("event.patient.patientonsetageunit").alias("patient_onset_age_unit_code"),
    F.col("event.patient.patientsex").alias("patient_sex_code"),
    F.col("event.patient.patientweight").cast("double").alias("patient_weight_kg"),
    F.col("event.patient.drug").alias("drug_array"),
    F.col("event.patient.reaction").alias("reaction_array"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Deduplicate at event grain
# MAGIC
# MAGIC Because Bronze was ingested in overlapping weekly batches, the same
# MAGIC `safetyreportid` can appear more than once. We deduplicate here, once, before
# MAGIC branching into the drug/reaction tables — so both downstream tables inherit a
# MAGIC clean, unique set of events.

# COMMAND ----------

before_count = df_events_flat.count()

# First, dedupe within this run's own batch
df_events_dedup_local = df_events_flat.dropDuplicates(["safety_report_id"])

# Then, exclude any safety_report_id that's already in the target table
# (protects against the same report appearing in a later incremental run's Bronze files)
if spark.catalog.tableExists("silver.events"):
    existing_ids = spark.table("silver.events").select("safety_report_id")
    df_events_dedup = df_events_dedup_local.join(existing_ids, on="safety_report_id", how="left_anti")
else:
    df_events_dedup = df_events_dedup_local

after_count = df_events_dedup.count()
print(f"Rows before dedup: {before_count} | after local dedup: {df_events_dedup_local.count()} | after excluding existing: {after_count}")

# Cache this, since we're about to reuse it three times below (once per output table).
# Caching avoids Spark re-reading and re-parsing all the raw JSON from scratch each time.
# NOTE: .cache() is commented out — Serverless compute doesn't support persisting
# data in memory across cells the way a dedicated cluster does.
# df_events_dedup.cache()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Build `silver_event_drug` (explode `drug[]`)
# MAGIC
# MAGIC This is the second, *inner* level of flattening — one row per (event, drug) pair.
# MAGIC This table is what `Bridge_Event_Drug` and `Dim_Drug` will be built from in Gold.
# MAGIC
# MAGIC `openfda.generic_name` / `brand_name` / `manufacturer_name` are themselves arrays
# MAGIC inside `drug` (OpenFDA sometimes returns multiple names for one drug entry) — we
# MAGIC take the first element with `[0]` to keep this table at a clean grain. If you later
# MAGIC want every alternate name captured, that would need its own explode — flag it for
# MAGIC a future iteration rather than solving it now.

# COMMAND ----------

df_event_drug = (
    df_events_dedup
    .select("safety_report_id", "drug_array")
    .withColumn("drug", F.explode("drug_array"))
    .select(
        "safety_report_id",
        F.trim(F.col("drug.medicinalproduct")).alias("medicinal_product_name"),
        F.col("drug.drugcharacterization").alias("drug_characterization_code"),
        F.col("drug.drugdosagetext").alias("drug_dosage_text"),
        F.col("drug.drugadministrationroute").alias("drug_administration_route_code"),
        F.col("drug.drugindication").alias("drug_indication"),
        F.col("drug.actiondrug").alias("action_taken_with_drug_code"),
        F.try_to_date(F.col("drug.drugstartdate"), "yyyyMMdd").alias("drug_start_date"),
        F.try_to_date(F.col("drug.drugenddate"), "yyyyMMdd").alias("drug_end_date"),
        F.col("drug.openfda.generic_name")[0].alias("generic_name"),
        F.col("drug.openfda.brand_name")[0].alias("brand_name"),
        F.col("drug.openfda.manufacturer_name")[0].alias("manufacturer_name"),
    )
    .filter(F.col("medicinal_product_name").isNotNull())
    .dropDuplicates()  # guards against exact duplicate drug entries within the same report
)

print("silver_event_drug row count:", df_event_drug.count())
df_event_drug.show(5, truncate=50)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 — Build `silver_event_reaction` (explode `reaction[]`)
# MAGIC
# MAGIC Same pattern as drugs. MedDRA reaction terms (`reactionmeddrapt`) are notoriously
# MAGIC inconsistent in casing/spacing across reports, so we standardize with
# MAGIC `trim()` + `upper()` — this matters a lot once you're grouping/counting reactions
# MAGIC in Gold; inconsistent casing would silently split what should be one reaction into
# MAGIC multiple rows.

# COMMAND ----------

df_event_reaction = (
    df_events_dedup
    .select("safety_report_id", "reaction_array")
    .withColumn("reaction", F.explode("reaction_array"))
    .select(
        "safety_report_id",
        F.upper(F.trim(F.col("reaction.reactionmeddrapt"))).alias("reaction_term"),
        F.col("reaction.reactionoutcome").alias("reaction_outcome_code"),
    )
    .filter(F.col("reaction_term").isNotNull())
    .dropDuplicates()
)

print("silver_event_reaction row count:", df_event_reaction.count())
df_event_reaction.show(5, truncate=50)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 — Finalize `silver_events` (drop the now-redundant arrays)
# MAGIC
# MAGIC The `drug_array` / `reaction_array` columns have done their job feeding Steps 5-6.
# MAGIC We drop them here so `silver_events` stays at a clean one-row-per-report grain with
# MAGIC no leftover nested structures — this is what will become `Fact_Adverse_Event` +
# MAGIC `Dim_Patient` in Gold.
# MAGIC
# MAGIC We also add `event_year` / `event_month` columns purely to partition the Delta
# MAGIC write — this lets future queries (and Power BI's DirectQuery/Import refreshes)
# MAGIC skip irrelevant partitions instead of scanning the whole table, a technique called
# MAGIC **partition pruning**.

# COMMAND ----------

df_silver_events = (
    df_events_dedup
    .drop("drug_array", "reaction_array")
    .withColumn("event_year", F.year("received_date"))
    .withColumn("event_month", F.month("received_date"))
)

print("silver_events row count:", df_silver_events.count())
df_silver_events.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8 — Write all three tables to Silver as Delta
# MAGIC
# MAGIC Notes on choices below:
# MAGIC - `.format("delta")` — gives you ACID-safe writes (no half-written table if a job
# MAGIC   fails midway) and the ability to time-travel to previous versions if you ever
# MAGIC   need to debug what the data looked like on a prior run.
# MAGIC - `.coalesce(n)` — forces Spark to consolidate output into a small, fixed number
# MAGIC   of files rather than one-file-per-task. Critical on a single-node free-tier
# MAGIC   cluster where you want to minimize storage transaction costs and avoid
# MAGIC   slow listing operations later.
# MAGIC - `mode("overwrite")` — simplest correct choice for now while you're iterating.
# MAGIC   Once this pipeline is scheduled to run repeatedly, revisit this: overwrite
# MAGIC   discards history each run, so you'll want either an append + dedupe-on-read
# MAGIC   pattern or a Delta `MERGE` (upsert) — a good next lesson once Silver is stable.

# COMMAND ----------

(
    df_silver_events
    .coalesce(4)
    .write
    .format("delta")
    .mode("append")
    .partitionBy("event_year", "event_month")
    .saveAsTable("silver.events")
)
print("silver_events written to: silver.events")

# COMMAND ----------

(
    df_event_drug
    .coalesce(2)
    .write
    .format("delta")
    .mode("append")
    .saveAsTable("silver.event_drug")
)
print("silver_event_drug written to: silver.event_drug")

# COMMAND ----------

(
    df_event_reaction
    .coalesce(2)
    .write
    .format("delta")
    .mode("append")
    .saveAsTable("silver.event_reaction")
)
print("silver_event_reaction written to: silver.event_reaction")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8b — Log processed files
# MAGIC
# MAGIC Marks this run's files as done, so the next run's incremental check
# MAGIC correctly skips them.

# COMMAND ----------

import datetime
from pyspark.sql import Row

new_log_df = spark.createDataFrame(
    [Row(file_path=f, loaded_at=datetime.datetime.now()) for f in new_bronze_files]
)
new_log_df.write.format("delta").mode("append").saveAsTable("silver.load_log")

print(f"Logged {len(new_bronze_files)} files as processed in silver.load_log")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9 — (Optional but recommended) Register as metastore tables
# MAGIC
# MAGIC Registering the Delta paths as named tables lets you query them with plain SQL
# MAGIC (`SELECT * FROM silver.events`) instead of always referencing the ADLS path —
# MAGIC handy for ad-hoc checks and for Power BI's Databricks connector later.
# MAGIC Note that the `saveAsTable()` calls above already did this for you, so this step is
# MAGIC only needed if you want to explicitly control the table names or locations.

# COMMAND ----------

# spark.sql("CREATE DATABASE IF NOT EXISTS silver")

# spark.sql(f"""
#     CREATE TABLE IF NOT EXISTS silver.events
#     USING DELTA
#     LOCATION '{silver_events_path}'
# """)

# spark.sql(f"""
#     CREATE TABLE IF NOT EXISTS silver.event_drug
#     USING DELTA
#     LOCATION '{silver_event_drug_path}'
# """)

# spark.sql(f"""
#     CREATE TABLE IF NOT EXISTS silver.event_reaction
#     USING DELTA
#     LOCATION '{silver_event_reaction_path}'
# """)

# print("Silver tables registered in metastore: silver.events, silver.event_drug, silver.event_reaction")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10 — Sanity checks
# MAGIC
# MAGIC Quick validation before you move on to Gold. Worth running every time you
# MAGIC re-execute this notebook.

# COMMAND ----------

print("=== Row counts ===")
print("silver.events:         ", spark.table("silver.events").count())
print("silver.event_drug:     ", spark.table("silver.event_drug").count())
print("silver.event_reaction: ", spark.table("silver.event_reaction").count())

print("\n=== Null checks on key columns ===")
spark.table("silver.events").select(
    F.sum(F.col("safety_report_id").isNull().cast("int")).alias("null_safety_report_ids"),
    F.sum(F.col("received_date").isNull().cast("int")).alias("null_received_dates"),
).show()

print("\n=== Sample joined view (event -> drug -> reaction) ===")
spark.sql("""
    SELECT e.safety_report_id, e.received_date, e.patient_sex_code,
           d.generic_name, d.brand_name,
           r.reaction_term
    FROM silver.events e
    JOIN silver.event_drug d ON e.safety_report_id = d.safety_report_id
    JOIN silver.event_reaction r ON e.safety_report_id = r.safety_report_id
    LIMIT 10
""").show(truncate=40)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recap & what's next
# MAGIC
# MAGIC You now have three clean, deduplicated, typed Delta tables in Silver:
# MAGIC - `silver.events` — partitioned by year/month, ready to become `Fact_Adverse_Event` + `Dim_Patient`
# MAGIC - `silver.event_drug` — ready to become `Bridge_Event_Drug` + `Dim_Drug`
# MAGIC - `silver.event_reaction` — ready to become `Bridge_Event_Reaction` + `Dim_Reaction`
# MAGIC
# MAGIC **Next notebook (Gold layer)** will:
# MAGIC 1. Deduplicate `generic_name` / `manufacturer_name` / `reaction_term` into proper
# MAGIC    dimension tables with surrogate keys (matching your existing SCM star schema
# MAGIC    conventions).
# MAGIC 2. Build `Dim_Date` (you already have a pattern for this from your SCM work).
# MAGIC 3. Collapse `silver.event_drug` / `silver.event_reaction` into the bridge tables
# MAGIC    using surrogate keys instead of raw text.
# MAGIC 4. Revisit the `overwrite` mode above and introduce an incremental `MERGE` pattern
# MAGIC    once you're running this on a schedule.
