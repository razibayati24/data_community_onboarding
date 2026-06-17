# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Explore the raw data (read-only)
# MAGIC
# MAGIC This notebook **writes nothing** — it profiles the raw Parquet you generated in `01` so you
# MAGIC understand the data *before* cleaning it. It answers the question every data engineer asks:
# MAGIC *"what's wrong with this data, and what rules will Silver need to enforce?"*
# MAGIC
# MAGIC We do it two ways so you can practice both surfaces:
# MAGIC 1. **PySpark** profiling (this notebook), and
# MAGIC 2. **SQL** — the same checks, ready to paste into the **SQL Editor** (Step 7 below).
# MAGIC
# MAGIC > Run `01_generate_parquet` first. Use the same `catalog` / `schema` / `volume` values.

# COMMAND ----------

dbutils.widgets.text("catalog", "main",               "1 · Catalog")
dbutils.widgets.text("schema",  "network_onboarding", "2 · Schema")
dbutils.widgets.text("volume",  "raw_landing",        "3 · Volume")

catalog  = dbutils.widgets.get("catalog").strip()
schema   = dbutils.widgets.get("schema").strip()
volume   = dbutils.widgets.get("volume").strip()
vol_path = f"/Volumes/{catalog}/{schema}/{volume}"

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA  {schema}")

alarms = spark.read.parquet(f"{vol_path}/alarms")
sites  = spark.read.parquet(f"{vol_path}/sites")
print(f"alarms: {alarms.count():,} rows | sites: {sites.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Eyeball each source
# MAGIC Always look at real rows before trusting a schema.

# COMMAND ----------

display(alarms.limit(20))

# COMMAND ----------

display(sites.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Null profile on `alarms`
# MAGIC These are the rows Silver will **drop**. Expect ~2% null `site_id` and ~3% null `severity`.

# COMMAND ----------

from pyspark.sql import functions as F

display(alarms.select(
    F.sum(F.col("site_id").isNull().cast("int")).alias("null_site_id"),
    F.sum(F.col("severity").isNull().cast("int")).alias("null_severity"),
    F.count("*").alias("total_rows"),
))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Duplicate `alarm_id`s
# MAGIC Silver will dedupe on `alarm_id` (keep one). Expect ~5,000 duplicate rows.

# COMMAND ----------

display(alarms.select(
    F.count("*").alias("rows_total"),
    F.countDistinct("alarm_id").alias("distinct_alarm_ids"),
    (F.count("*") - F.countDistinct("alarm_id")).alias("duplicate_rows"),
))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Distributions (severity & alarm type)
# MAGIC Sanity-check the shape of the data — no single value should dominate unrealistically.

# COMMAND ----------

display(alarms.groupBy("severity").count().orderBy(F.desc("count")))

# COMMAND ----------

display(alarms.groupBy("alarm_type").count().orderBy(F.desc("count")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — `sites` dimension: regions & coordinate ranges
# MAGIC Confirms the lat/long bounding box is sane (these coordinates will drive a map later).

# COMMAND ----------

display(sites.groupBy("region").agg(
    F.count("*").alias("sites"),
    F.round(F.min("latitude"), 2).alias("min_lat"),
    F.round(F.max("latitude"), 2).alias("max_lat"),
    F.round(F.min("longitude"), 2).alias("min_lon"),
    F.round(F.max("longitude"), 2).alias("max_lon"),
).orderBy(F.desc("sites")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 — Preview the join (what Silver will hold)
# MAGIC Inner-joining on `site_id` enriches each alarm with region / vendor / coordinates **and**
# MAGIC naturally drops alarms whose `site_id` is null or unknown.

# COMMAND ----------

preview = (alarms.alias("a")
           .join(sites.alias("s"), "site_id")
           .where(F.col("a.site_id").isNotNull() & F.col("a.severity").isNotNull())
           .select("a.alarm_id", "a.site_id", "s.region", "s.vendor",
                   "a.severity", "s.latitude", "s.longitude"))
print(f"Rows surviving the join + null filter ≈ what Silver will hold: {preview.count():,}")
display(preview.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 — Same checks in pure SQL (for the SQL Editor)
# MAGIC The cells below first register the Parquet as two tables, then repeat the profiling in SQL.
# MAGIC Copy any of these into the **SQL Editor** to explore the data outside a notebook.
# MAGIC The `_raw` suffix keeps these separate from the Silver/Gold tables built in notebook 03.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Turn the raw Parquet into queryable tables (CTAS straight off the Volume)
# MAGIC CREATE OR REPLACE TABLE alarms_raw AS
# MAGIC   SELECT * FROM read_files('/Volumes/${catalog}/${schema}/${volume}/alarms/', format => 'parquet');
# MAGIC CREATE OR REPLACE TABLE sites_raw AS
# MAGIC   SELECT * FROM read_files('/Volumes/${catalog}/${schema}/${volume}/sites/', format => 'parquet');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Null profile (these rows get dropped in Silver)
# MAGIC SELECT
# MAGIC   SUM(CASE WHEN site_id  IS NULL THEN 1 ELSE 0 END) AS null_site_id,
# MAGIC   SUM(CASE WHEN severity IS NULL THEN 1 ELSE 0 END) AS null_severity,
# MAGIC   COUNT(*) AS total_rows
# MAGIC FROM alarms_raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Duplicate alarm_ids (deduped in Silver)
# MAGIC SELECT COUNT(*) AS rows_total,
# MAGIC        COUNT(DISTINCT alarm_id) AS distinct_alarm_ids,
# MAGIC        COUNT(*) - COUNT(DISTINCT alarm_id) AS duplicate_rows
# MAGIC FROM alarms_raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Severity distribution
# MAGIC SELECT severity, COUNT(*) AS alarms
# MAGIC FROM alarms_raw
# MAGIC GROUP BY severity ORDER BY alarms DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Preview the join (what Silver will hold)
# MAGIC SELECT a.alarm_id, a.site_id, s.region, s.vendor, a.severity, s.latitude, s.longitude
# MAGIC FROM alarms_raw a
# MAGIC JOIN sites_raw  s ON a.site_id = s.site_id
# MAGIC WHERE a.site_id IS NOT NULL AND a.severity IS NOT NULL
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Takeaway
# MAGIC You've confirmed the cleanup rules the pipeline must enforce: **drop null `site_id` / `severity`**,
# MAGIC **dedupe on `alarm_id`**, and **inner-join to `sites`**. Next, **`03_silver_gold`** applies
# MAGIC exactly these rules to build the Silver and Gold tables.
