# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer — Star Schema
# MAGIC ### Pharmacovigilance & Regulatory Compliance Pipeline
# MAGIC
# MAGIC Builds the final BI-ready star schema from the three Silver Delta tables:
# MAGIC - `silver.events`, `silver.event_drug`, `silver.event_reaction`
# MAGIC
# MAGIC **Output (all as Unity Catalog managed tables under `pharmacovigilance_ws.gold`):**
# MAGIC - Dimensions: `dim_date`, `dim_patient`, `dim_drug`, `dim_manufacturer`, `dim_reaction`
# MAGIC - Fact: `fact_adverse_event`
# MAGIC - Bridges: `bridge_event_drug`, `bridge_event_reaction`
# MAGIC
# MAGIC **Note on `dim_patient`:** OpenFDA data is de-identified — there is no real,
# MAGIC repeating patient identity to model. This dimension represents distinct
# MAGIC *patient profiles* (sex + age group), not individual people. Two different real
# MAGIC patients who happen to share the same sex and age group will share one row here.
# MAGIC This is standard and expected for de-identified regulatory data.
# MAGIC
# MAGIC **Note on managed vs external tables:** Silver tables live at specific ADLS paths
# MAGIC (external tables, via the Unity Catalog External Locations we set up earlier).
# MAGIC Gold tables here are newly created output, not raw files landing from outside —
# MAGIC so we let Unity Catalog manage their storage automatically (managed tables),
# MAGIC avoiding the need to set up a third External Location.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark.sql("CREATE SCHEMA IF NOT EXISTS pharmacovigilance_ws.gold")

df_events = spark.table("silver.events")
df_event_drug = spark.table("silver.event_drug")
df_event_reaction = spark.table("silver.event_reaction")

print("Silver row counts:")
print("  events:        ", df_events.count())
print("  event_drug:    ", df_event_drug.count())
print("  event_reaction:", df_event_reaction.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dim_Manufacturer
# MAGIC
# MAGIC Distinct manufacturer names from the drug data, each given a surrogate key.
# MAGIC `row_number()` over an alphabetically ordered list gives stable, reproducible
# MAGIC keys — re-running this notebook assigns the same keys to the same manufacturers,
# MAGIC as long as the underlying distinct set doesn't change.

# COMMAND ----------

dim_manufacturer = (
    df_event_drug
    .select(F.coalesce(F.col("manufacturer_name"), F.lit("UNKNOWN")).alias("manufacturer_name"))
    .distinct()
    .withColumn("manufacturer_key", F.row_number().over(Window.orderBy("manufacturer_name")))
    .select("manufacturer_key", "manufacturer_name")
)

dim_manufacturer.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable("pharmacovigilance_ws.gold.dim_manufacturer")

print("dim_manufacturer rows:", dim_manufacturer.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dim_Drug
# MAGIC
# MAGIC Distinct drug identities (generic name + brand name + product name), linked to
# MAGIC `Dim_Manufacturer` by surrogate key rather than repeating the manufacturer's name
# MAGIC on every drug row — this keeps the model close to a clean star schema with one
# MAGIC small "snowflake" branch off `Dim_Drug`, which Power BI handles natively as a
# MAGIC two-hop relationship.

# COMMAND ----------

drug_with_manufacturer_key = (
    df_event_drug
    .withColumn("manufacturer_name", F.coalesce(F.col("manufacturer_name"), F.lit("UNKNOWN")))
    .join(dim_manufacturer, on="manufacturer_name", how="left")
)

dim_drug = (
    drug_with_manufacturer_key
    .select(
        F.coalesce(F.col("generic_name"), F.lit("UNKNOWN")).alias("generic_name"),
        F.coalesce(F.col("brand_name"), F.lit("UNKNOWN")).alias("brand_name"),
        F.col("medicinal_product_name"),
        F.col("manufacturer_key"),
    )
    .distinct()
    .withColumn("drug_key", F.row_number().over(
        Window.orderBy("generic_name", "brand_name", "medicinal_product_name")
    ))
    .select("drug_key", "generic_name", "brand_name", "medicinal_product_name", "manufacturer_key")
)

dim_drug.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable("pharmacovigilance_ws.gold.dim_drug")

print("dim_drug rows:", dim_drug.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dim_Reaction
# MAGIC
# MAGIC Distinct reaction terms only. Note `reaction_outcome_code` (how the reaction
# MAGIC resolved) is deliberately NOT here — it describes a specific event-reaction pair,
# MAGIC not the reaction concept itself, so it belongs in the bridge table instead.

# COMMAND ----------

dim_reaction = (
    df_event_reaction
    .select("reaction_term")
    .distinct()
    .withColumn("reaction_key", F.row_number().over(Window.orderBy("reaction_term")))
    .select("reaction_key", "reaction_term")
)

dim_reaction.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable("pharmacovigilance_ws.gold.dim_reaction")

print("dim_reaction rows:", dim_reaction.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dim_Patient
# MAGIC
# MAGIC Built as a profile dimension: sex code + a bucketed age group. OpenFDA's
# MAGIC `patient_onset_age_unit_code` tells us what unit the age number is in (801 = Years
# MAGIC is by far the most common; other codes are Decade/Month/Week/Day/Hour). We only
# MAGIC convert clean Year-unit ages into buckets — anything else becomes "Unknown" rather
# MAGIC than risk silently misinterpreting units.

# COMMAND ----------

df_age_prepared = df_events.withColumn(
    "age_group",
    F.when(F.col("patient_onset_age_unit_code") != "801", "Unknown")
     .when(F.col("patient_onset_age").isNull(), "Unknown")
     .when(F.col("patient_onset_age") < 18, "Under 18")
     .when(F.col("patient_onset_age") < 41, "18-40")
     .when(F.col("patient_onset_age") < 66, "41-65")
     .otherwise("65+")
).withColumn(
    "patient_sex_label",
    F.when(F.col("patient_sex_code") == "1", "Male")
     .when(F.col("patient_sex_code") == "2", "Female")
     .otherwise("Unknown")
)

dim_patient = (
    df_age_prepared
    .select("patient_sex_label", "age_group")
    .distinct()
    .withColumn("patient_key", F.row_number().over(Window.orderBy("patient_sex_label", "age_group")))
    .select("patient_key", "patient_sex_label", "age_group")
)

dim_patient.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable("pharmacovigilance_ws.gold.dim_patient")

print("dim_patient rows:", dim_patient.count())
dim_patient.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dim_Date
# MAGIC
# MAGIC A standard, self-generated calendar table — not derived from the API at all.
# MAGIC `sequence()` generates one row per day across the range; we then break each date
# MAGIC into the attributes Power BI's date hierarchies and slicers expect.

# COMMAND ----------

date_range_df = spark.sql("""
    SELECT explode(sequence(to_date('2023-12-01'), to_date('2024-04-30'), interval 1 day)) AS full_date
""")

dim_date = (
    date_range_df
    .withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast("int"))
    .withColumn("year", F.year("full_date"))
    .withColumn("quarter", F.quarter("full_date"))
    .withColumn("month", F.month("full_date"))
    .withColumn("month_name", F.date_format("full_date", "MMMM"))
    .withColumn("day", F.dayofmonth("full_date"))
    .withColumn("day_name", F.date_format("full_date", "EEEE"))
    .withColumn("is_weekend", F.dayofweek("full_date").isin([1, 7]))
    .select("date_key", "full_date", "year", "quarter", "month", "month_name", "day", "day_name", "is_weekend")
)

dim_date.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable("pharmacovigilance_ws.gold.dim_date")

print("dim_date rows:", dim_date.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fact_Adverse_Event
# MAGIC
# MAGIC Grain: **one row per adverse event report** (matches `silver.events` exactly).
# MAGIC Joins in `date_key` (via `received_date`) and `patient_key` (via sex + age group)
# MAGIC to replace those raw values with surrogate keys, and generates its own
# MAGIC `event_key` surrogate — this is what the bridge tables will reference, instead of
# MAGIC the long text `safety_report_id`.

# COMMAND ----------

fact_adverse_event = (
    df_age_prepared
    .join(dim_patient, on=["patient_sex_label", "age_group"], how="left")
    .withColumn("date_key", F.date_format("received_date", "yyyyMMdd").cast("int"))
    .withColumn("event_key", F.row_number().over(Window.orderBy("safety_report_id")))
    .select(
        "event_key",
        "safety_report_id",
        "date_key",
        "patient_key",
        "is_serious_flag",
        "seriousness_death_flag",
        "seriousness_hospitalization_flag",
        "seriousness_life_threatening_flag",
        "patient_onset_age",
        "patient_weight_kg",
    )
)

fact_adverse_event.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable("pharmacovigilance_ws.gold.fact_adverse_event")

print("fact_adverse_event rows:", fact_adverse_event.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bridge_Event_Drug
# MAGIC
# MAGIC One row per (event, drug) pair. Carries `event_key` and `drug_key` (both
# MAGIC surrogate integers) plus the attributes that are specific to *this* drug's
# MAGIC involvement in *this* event — dosage, route, dates — which don't belong in
# MAGIC `Dim_Drug` itself since they vary per event, not per drug.

# COMMAND ----------

event_key_lookup = fact_adverse_event.select("event_key", "safety_report_id")

bridge_event_drug = (
    df_event_drug
    .withColumn("manufacturer_name", F.coalesce(F.col("manufacturer_name"), F.lit("UNKNOWN")))
    .withColumn("generic_name", F.coalesce(F.col("generic_name"), F.lit("UNKNOWN")))
    .withColumn("brand_name", F.coalesce(F.col("brand_name"), F.lit("UNKNOWN")))
    .join(event_key_lookup, on="safety_report_id", how="inner")
    .join(dim_drug, on=["generic_name", "brand_name", "medicinal_product_name"], how="left")
    .select(
        "event_key",
        "drug_key",
        "drug_characterization_code",
        "drug_dosage_text",
        "drug_administration_route_code",
        "drug_indication",
        "action_taken_with_drug_code",
        "drug_start_date",
        "drug_end_date",
    )
)

bridge_event_drug.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable("pharmacovigilance_ws.gold.bridge_event_drug")

print("bridge_event_drug rows:", bridge_event_drug.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bridge_Event_Reaction
# MAGIC
# MAGIC Same pattern — one row per (event, reaction) pair, carrying the
# MAGIC event-specific `reaction_outcome_code`.

# COMMAND ----------

bridge_event_reaction = (
    df_event_reaction
    .join(event_key_lookup, on="safety_report_id", how="inner")
    .join(dim_reaction, on="reaction_term", how="left")
    .select("event_key", "reaction_key", "reaction_outcome_code")
)

bridge_event_reaction.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable("pharmacovigilance_ws.gold.bridge_event_reaction")

print("bridge_event_reaction rows:", bridge_event_reaction.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sanity checks
# MAGIC
# MAGIC A full star-schema join, exactly the kind of query Power BI will run — proof the
# MAGIC model holds together before we connect a BI tool to it.

# COMMAND ----------

print("=== Gold table row counts ===")
for t in ["dim_date", "dim_patient", "dim_drug", "dim_manufacturer", "dim_reaction",
          "fact_adverse_event", "bridge_event_drug", "bridge_event_reaction"]:
    print(f"  {t}: {spark.table(f'pharmacovigilance_ws.gold.{t}').count()}")

print("\n=== Sample full star join ===")
spark.sql("""
    SELECT
        d.full_date, p.patient_sex_label, p.age_group,
        dr.generic_name, dr.brand_name, m.manufacturer_name,
        r.reaction_term, f.is_serious_flag
    FROM pharmacovigilance_ws.gold.fact_adverse_event f
    JOIN pharmacovigilance_ws.gold.dim_date d ON f.date_key = d.date_key
    JOIN pharmacovigilance_ws.gold.dim_patient p ON f.patient_key = p.patient_key
    JOIN pharmacovigilance_ws.gold.bridge_event_drug bd ON f.event_key = bd.event_key
    JOIN pharmacovigilance_ws.gold.dim_drug dr ON bd.drug_key = dr.drug_key
    JOIN pharmacovigilance_ws.gold.dim_manufacturer m ON dr.manufacturer_key = m.manufacturer_key
    JOIN pharmacovigilance_ws.gold.bridge_event_reaction br ON f.event_key = br.event_key
    JOIN pharmacovigilance_ws.gold.dim_reaction r ON br.reaction_key = r.reaction_key
    LIMIT 10
""").show(truncate=30)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recap & what's next
# MAGIC
# MAGIC The full star schema now exists under `pharmacovigilance_ws.gold` — five dimensions, one
# MAGIC fact table, two bridge tables, all as Unity Catalog managed Delta tables.
# MAGIC
# MAGIC **Next: connect Power BI.**
# MAGIC 1. In your SQL Warehouse's connection details tab, grab the server hostname and
# MAGIC    HTTP path.
# MAGIC 2. In Power BI Desktop: Get Data -> Databricks -> paste those two values.
# MAGIC 3. Authenticate with a personal access token (generate one under
# MAGIC    User Settings -> Developer -> Access Tokens in Databricks).
# MAGIC 4. Import mode is the sensible default at this data volume.
# MAGIC 5. Build relationships in Power BI's model view exactly matching the joins above
# MAGIC    — Power BI won't auto-detect them since there are no foreign key constraints
# MAGIC    enforced at the Delta table level.
