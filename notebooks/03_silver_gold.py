# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Build Silver and Gold tables
# MAGIC
# MAGIC Now we turn raw Parquet into a clean, governed **medallion** model:
# MAGIC
# MAGIC ```
# MAGIC raw Parquet ──► silver_alarms ──► gold_site_health
# MAGIC (alarms+sites)   join + clean       1 row per site,
# MAGIC                  + dedupe           KPIs + coordinates
# MAGIC ```
# MAGIC
# MAGIC - **Silver** = join `alarms` ⨝ `sites` on `site_id`, drop null `site_id`/`severity`, dedupe on
# MAGIC   `alarm_id`, add derived columns (`time_to_clear_min`, `is_active`).
# MAGIC - **Gold** = one row per `site_id` with KPIs and a severity-weighted `fault_score`, carrying
# MAGIC   `latitude`/`longitude` so it can drive a map.
# MAGIC
# MAGIC This notebook builds them with **SQL** (the same logic the Lakeflow pipeline compiles to).
# MAGIC Run `01` and ideally `02` first; use the same `catalog` / `schema` / `volume`.

# COMMAND ----------

dbutils.widgets.text("catalog", "main",               "1 · Catalog")
dbutils.widgets.text("schema",  "network_onboarding", "2 · Schema")
dbutils.widgets.text("volume",  "raw_landing",        "3 · Volume")

catalog = dbutils.widgets.get("catalog").strip()
schema  = dbutils.widgets.get("schema").strip()
volume  = dbutils.widgets.get("volume").strip()

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA  {schema}")
print(f"Building Silver + Gold in {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Silver: join + clean + dedupe
# MAGIC `EXCEPT`-style data-quality is enforced inline here with a `WHERE` filter and a window dedupe.
# MAGIC (In a Lakeflow Declarative Pipeline these become `EXPECT … ON VIOLATION DROP ROW` constraints.)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silver_alarms AS
# MAGIC WITH raw_alarms AS (
# MAGIC   SELECT * FROM read_files('/Volumes/${catalog}/${schema}/${volume}/alarms/', format => 'parquet')
# MAGIC ),
# MAGIC raw_sites AS (
# MAGIC   SELECT * FROM read_files('/Volumes/${catalog}/${schema}/${volume}/sites/', format => 'parquet')
# MAGIC ),
# MAGIC dedup AS (
# MAGIC   SELECT *, ROW_NUMBER() OVER (PARTITION BY alarm_id ORDER BY raised_ts) AS rn
# MAGIC   FROM raw_alarms
# MAGIC )
# MAGIC SELECT
# MAGIC   a.alarm_id, a.site_id, a.node_id, a.alarm_type, a.severity,
# MAGIC   a.raised_ts, a.cleared_ts, a.status, a.probable_cause, a.event_date,
# MAGIC   s.site_name, s.region, s.state, s.vendor, s.equipment_type,
# MAGIC   s.latitude, s.longitude, s.install_date,
# MAGIC   CASE WHEN a.cleared_ts IS NOT NULL
# MAGIC        THEN timestampdiff(MINUTE, a.raised_ts, a.cleared_ts) END AS time_to_clear_min,
# MAGIC   (a.cleared_ts IS NULL) AS is_active
# MAGIC FROM dedup a
# MAGIC JOIN raw_sites s ON a.site_id = s.site_id     -- inner join also drops null/unknown site_id
# MAGIC WHERE a.rn = 1                                -- keep one row per alarm_id (dedupe)
# MAGIC   AND a.site_id  IS NOT NULL                  -- drop null site_id
# MAGIC   AND a.severity IS NOT NULL;                 -- drop null severity

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validate Silver
# MAGIC Confirm the cleanup worked: zero nulls, zero duplicates, and a visible row-count drop vs raw.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   (SELECT COUNT(*) FROM read_files('/Volumes/${catalog}/${schema}/${volume}/alarms/', format => 'parquet')) AS raw_alarm_rows,
# MAGIC   (SELECT COUNT(*) FROM silver_alarms)                                                                       AS silver_rows,
# MAGIC   (SELECT COUNT(*) FROM silver_alarms WHERE site_id  IS NULL)                                                AS null_site_id,
# MAGIC   (SELECT COUNT(*) FROM silver_alarms WHERE severity IS NULL)                                                AS null_severity,
# MAGIC   (SELECT COUNT(*) - COUNT(DISTINCT alarm_id) FROM silver_alarms)                                            AS duplicate_alarm_ids;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Gold: one row per site, with KPIs + coordinates
# MAGIC `fault_score` is severity-weighted (**Critical 10 · Major 5 · Minor 2 · Warning 1**) — this is
# MAGIC the single metric you rank "worst sites" by. `latitude`/`longitude` use `MAX()` (constant per
# MAGIC site) so they survive the `GROUP BY` and can plot on a map.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold_site_health AS
# MAGIC SELECT
# MAGIC   site_id,
# MAGIC   MAX(site_name)       AS site_name,
# MAGIC   MAX(region)          AS region,
# MAGIC   MAX(state)           AS state,
# MAGIC   MAX(vendor)          AS vendor,
# MAGIC   MAX(equipment_type)  AS equipment_type,
# MAGIC   MAX(latitude)        AS latitude,
# MAGIC   MAX(longitude)       AS longitude,
# MAGIC   COUNT(*)                                                  AS total_alarms,
# MAGIC   SUM(CASE WHEN severity = 'Critical' THEN 1 ELSE 0 END)    AS critical_alarms,
# MAGIC   SUM(CASE WHEN is_active THEN 1 ELSE 0 END)                AS active_alarms,
# MAGIC   ROUND(AVG(time_to_clear_min), 1)                          AS avg_mttr_min,
# MAGIC   SUM(CASE severity WHEN 'Critical' THEN 10
# MAGIC                     WHEN 'Major'    THEN 5
# MAGIC                     WHEN 'Minor'    THEN 2
# MAGIC                     ELSE 1 END)                             AS fault_score
# MAGIC FROM silver_alarms
# MAGIC GROUP BY site_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validate Gold
# MAGIC One row per site, no null coordinates, and the worst sites ranked by `fault_score`.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   (SELECT COUNT(*)        FROM gold_site_health)                          AS site_rows,
# MAGIC   (SELECT COUNT(DISTINCT site_id) FROM gold_site_health)                  AS distinct_sites,
# MAGIC   (SELECT COUNT(*)        FROM gold_site_health WHERE latitude IS NULL)   AS null_coords;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- The headline question this whole pipeline exists to answer:
# MAGIC SELECT site_id, site_name, region, vendor,
# MAGIC        total_alarms, critical_alarms, active_alarms, avg_mttr_min, fault_score
# MAGIC FROM gold_site_health
# MAGIC ORDER BY fault_score DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Regional roll-up (handy for a bar chart / dashboard)
# MAGIC SELECT region,
# MAGIC        SUM(critical_alarms) AS critical_alarms,
# MAGIC        SUM(active_alarms)   AS active_alarms,
# MAGIC        ROUND(AVG(avg_mttr_min), 1) AS avg_mttr_min
# MAGIC FROM gold_site_health
# MAGIC GROUP BY region
# MAGIC ORDER BY critical_alarms DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done ✅ — you've built a medallion lakehouse
# MAGIC ```
# MAGIC <catalog>.<schema>.silver_alarms      ← clean, joined, deduped per-alarm detail
# MAGIC <catalog>.<schema>.gold_site_health   ← one row per site, KPIs + coordinates
# MAGIC ```
# MAGIC **Where to go next (covered in the live demo):**
# MAGIC - Point a **Genie room** at `gold_site_health` for natural-language Q&A
# MAGIC   (instruct it to rank "worst sites" by `fault_score`).
# MAGIC - Build a **Lakeview dashboard** — counters for critical/active alarms, a symbol **map** on
# MAGIC   `latitude`/`longitude`, and a worst-sites table.
# MAGIC - Or rebuild this whole flow no-code in **Lakeflow Designer** (these same Bronze→Silver→Gold steps).

# COMMAND ----------

print("Silver + Gold built. Tables:")
display(spark.sql(f"SHOW TABLES IN {catalog}.{schema}"))
