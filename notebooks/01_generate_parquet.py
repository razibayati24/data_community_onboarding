# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Generate raw Parquet sources
# MAGIC
# MAGIC **What this notebook does:** creates a catalog, schema, and Volume of *your* choice, then
# MAGIC generates two realistic synthetic network feeds and lands them as **Parquet** on the Volume:
# MAGIC
# MAGIC | Source | Rows (default) | What it is |
# MAGIC |---|---|---|
# MAGIC | `alarms` | 200,000 | Fault / alarm event stream (with seeded nulls + duplicates) |
# MAGIC | `sites`  | 1,000   | Cell-site inventory dimension (carries lat/long for maps) |
# MAGIC
# MAGIC This mirrors the AT&T Network Fault Analytics demo — raw OSS/EMS exports landing on a Volume —
# MAGIC so the next two notebooks can explore the raw data and build Silver / Gold tables.
# MAGIC
# MAGIC > **Run order:** `01_generate_parquet` → `02_explore_raw` → `03_silver_gold`.
# MAGIC > Use **serverless** or any running compute. Defaults run in well under a minute.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Choose your destination
# MAGIC Edit the widgets at the top of the notebook (or the defaults below). Everything downstream
# MAGIC keys off these four values.

# COMMAND ----------

dbutils.widgets.text("catalog", "main",                "1 · Catalog")
dbutils.widgets.text("schema",  "network_onboarding",  "2 · Schema")
dbutils.widgets.text("volume",  "raw_landing",         "3 · Volume")
dbutils.widgets.text("num_sites",  "1000",             "4 · # sites")
dbutils.widgets.text("num_alarms", "200000",           "5 · # alarms")

catalog    = dbutils.widgets.get("catalog").strip()
schema     = dbutils.widgets.get("schema").strip()
volume     = dbutils.widgets.get("volume").strip()
N_SITES    = int(dbutils.widgets.get("num_sites"))
N_ALARMS   = int(dbutils.widgets.get("num_alarms"))

vol_path   = f"/Volumes/{catalog}/{schema}/{volume}"
print(f"Target : {catalog}.{schema}")
print(f"Volume : {vol_path}")
print(f"Scale  : {N_SITES:,} sites · {N_ALARMS:,} alarms")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Create (or reuse) the catalog, schema and Volume
# MAGIC This is **permission-tolerant**: it *tries* to create each object, but if you lack `CREATE`
# MAGIC privileges it quietly skips and **falls back to using what's already there**. The assumption is
# MAGIC that you at least have access to an existing catalog + schema (and a writable Volume) — so we
# MAGIC **check that first** and stop with a clear message if not.
# MAGIC
# MAGIC > No `CREATE CATALOG` rights? Just point the `catalog`/`schema`/`volume` widgets at ones you
# MAGIC > can already write to — the notebook will use them as-is.

# COMMAND ----------

def try_sql(stmt, label):
    """Run a statement; return True on success, False (with a note) if it's not permitted."""
    try:
        spark.sql(stmt)
        return True
    except Exception as e:
        print(f"  ⚠️  Skipped — {label}: {str(e).splitlines()[0][:140]}")
        return False

# 1) Try to create catalog / schema / volume, but tolerate missing CREATE privileges.
try_sql(f"CREATE CATALOG IF NOT EXISTS {catalog}",                 "create catalog")
try_sql(f"CREATE SCHEMA  IF NOT EXISTS {catalog}.{schema}",        "create schema")
try_sql(f"CREATE VOLUME  IF NOT EXISTS {catalog}.{schema}.{volume}", "create volume")

# 2) Check first — you MUST be able to use the catalog + schema to continue.
assert try_sql(f"USE CATALOG {catalog}", "use catalog"), \
    f"❌ No access to catalog '{catalog}'. Point the widget at one you can write to, or ask an admin."
assert try_sql(f"USE SCHEMA {schema}", "use schema"), \
    f"❌ No access to schema '{catalog}.{schema}'. Point the widget at one you can write to, or ask an admin."

# 3) The Volume must exist to land Parquet (creation above may have been skipped).
if not try_sql(f"DESCRIBE VOLUME {catalog}.{schema}.{volume}", "describe volume"):
    raise Exception(f"❌ Volume '{vol_path}' not available. Set the 'volume' widget to an existing "
                    f"Volume you can write to, or ask an admin to create it.")

print(f"✅ Using {catalog}.{schema} · Volume {vol_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Generate the `sites` dimension (3,000 → 1,000 by default)
# MAGIC One row per cell site, with coordinates that later drive a map. Region picks a real
# MAGIC US metro so latitude/longitude land in a believable bounding box.

# COMMAND ----------

from pyspark.sql import functions as F

# region -> (state, base_lat, base_lon)
region_meta = [
    ("Dallas-FortWorth", "TX", 32.78,  -96.80),
    ("Atlanta",          "GA", 33.75,  -84.39),
    ("LosAngeles",       "CA", 34.05, -118.24),
    ("NewYork",          "NY", 40.71,  -74.01),
    ("Chicago",          "IL", 41.88,  -87.63),
    ("Houston",          "TX", 29.76,  -95.37),
    ("Miami",            "FL", 25.76,  -80.19),
    ("Seattle",          "WA", 47.61, -122.33),
]
regions_arr = F.array(*[F.lit(r[0]) for r in region_meta])
states_arr  = F.array(*[F.lit(r[1]) for r in region_meta])
lat_arr     = F.array(*[F.lit(r[2]) for r in region_meta])
lon_arr     = F.array(*[F.lit(r[3]) for r in region_meta])
vendors_arr = F.array(F.lit("Ericsson"), F.lit("Nokia"), F.lit("Samsung"))
equip_arr   = F.array(F.lit("eNodeB"), F.lit("gNodeB"), F.lit("Router"), F.lit("Microwave"), F.lit("Core"))

NR = len(region_meta)
sites = (
    spark.range(N_SITES).withColumnRenamed("id", "idx")
    .withColumn("ridx", (F.col("idx") % F.lit(NR)).cast("int"))
    .withColumn("region",  F.element_at(regions_arr, F.col("ridx") + 1))
    .withColumn("state",   F.element_at(states_arr,  F.col("ridx") + 1))
    .withColumn("site_id", F.format_string("SITE-%05d", F.col("idx")))
    .withColumn("site_name",
                F.concat_ws("-", F.col("state"), F.upper(F.substring(F.col("region"), 1, 3)),
                            F.lpad(F.col("idx").cast("string"), 4, "0")))
    .withColumn("vendor",         F.element_at(vendors_arr, (F.rand() * 3 + 1).cast("int")))
    .withColumn("equipment_type", F.element_at(equip_arr,   (F.rand() * 5 + 1).cast("int")))
    .withColumn("latitude",  F.round(F.element_at(lat_arr, F.col("ridx") + 1) + (F.rand() - 0.5) * 0.6, 5))
    .withColumn("longitude", F.round(F.element_at(lon_arr, F.col("ridx") + 1) + (F.rand() - 0.5) * 0.6, 5))
    .withColumn("install_date", F.expr("current_timestamp() - make_interval(0,0,0, cast(rand()*2000 as int),0,0,0)"))
    .select("site_id", "site_name", "region", "state", "vendor",
            "equipment_type", "latitude", "longitude", "install_date")
)
sites.write.mode("overwrite").parquet(f"{vol_path}/sites")
print(f"Wrote {sites.count():,} sites → {vol_path}/sites")
display(sites.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Generate the `alarms` fault stream
# MAGIC ~1.5M in the demo; 200K here by default. **Data-quality issues are seeded on purpose** so the
# MAGIC Silver clean-up in notebook 03 is real:
# MAGIC - **~2% null `site_id`** (dropped in Silver)
# MAGIC - **~3% null `severity`** (dropped in Silver)
# MAGIC - **5,000 duplicate rows** (deduped on `alarm_id` in Silver)

# COMMAND ----------

alarm_types_arr = F.array(F.lit("LinkDown"), F.lit("HighTemperature"), F.lit("PowerFault"),
                          F.lit("CellOutage"), F.lit("PacketLoss"), F.lit("LicenseExpiry"))
severity_arr    = F.array(F.lit("Critical"), F.lit("Major"), F.lit("Minor"), F.lit("Warning"))
cause_arr       = F.array(F.lit("FiberCut"), F.lit("PowerGridDip"), F.lit("Overheating"),
                          F.lit("ConfigDrift"), F.lit("HardwareFailure"), F.lit("Congestion"))

base = (
    spark.range(N_ALARMS).withColumnRenamed("id", "n")
    .withColumn("alarm_id", F.format_string("ALM-%08d", F.col("n")))
    # FK to a real site, with ~2% nulled out
    .withColumn("_site_n", (F.rand() * N_SITES).cast("long"))
    .withColumn("site_id", F.when(F.rand() < 0.02, F.lit(None))
                            .otherwise(F.format_string("SITE-%05d", F.col("_site_n"))))
    .withColumn("node_id", F.format_string("ENB-%05d", (F.rand() * 50000).cast("int")))
    .withColumn("alarm_type", F.element_at(alarm_types_arr, (F.rand() * 6 + 1).cast("int")))
    # ~3% null severity
    .withColumn("severity", F.when(F.rand() < 0.03, F.lit(None))
                             .otherwise(F.element_at(severity_arr, (F.rand() * 4 + 1).cast("int"))))
    .withColumn("raised_ts", F.expr(
        "current_timestamp() - make_interval(0,0,0, cast(rand()*60 as int), cast(rand()*24 as int), cast(rand()*60 as int), 0)"))
    # ~15% still active (cleared_ts null); otherwise cleared 5..600 min later
    .withColumn("cleared_ts", F.when(F.rand() < 0.15, F.lit(None).cast("timestamp"))
                .otherwise(F.expr("raised_ts + make_interval(0,0,0,0,0, cast(rand()*595+5 as int),0)")))
    .withColumn("status", F.when(F.col("cleared_ts").isNull(),
                                 F.when(F.rand() < 0.5, F.lit("ACTIVE")).otherwise(F.lit("ACKNOWLEDGED")))
                           .otherwise(F.lit("CLEARED")))
    .withColumn("probable_cause", F.element_at(cause_arr, (F.rand() * 6 + 1).cast("int")))
    .withColumn("event_date", F.date_format("raised_ts", "yyyy-MM-dd"))
    .select("alarm_id", "site_id", "node_id", "alarm_type", "severity",
            "raised_ts", "cleared_ts", "status", "probable_cause", "event_date")
)

# Seed 5,000 duplicate rows (copy the first 5,000 alarm_ids verbatim)
dupes  = base.limit(5000)
alarms = base.unionByName(dupes)

alarms.write.mode("overwrite").parquet(f"{vol_path}/alarms")
print(f"Wrote {alarms.count():,} alarms → {vol_path}/alarms  (includes 5,000 duplicates)")
display(alarms.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done ✅
# MAGIC Two raw Parquet feeds now sit on your Volume:
# MAGIC ```
# MAGIC /Volumes/<catalog>/<schema>/<volume>/
# MAGIC ├── alarms/   ← fault events (seeded nulls + duplicates)
# MAGIC └── sites/    ← cell-site inventory (lat/long)
# MAGIC ```
# MAGIC **Next:** open **`02_explore_raw`** to profile this data and see *why* the Silver rules exist.

# COMMAND ----------

print("Parquet landed. Next notebook: 02_explore_raw")
print(f"  alarms → {vol_path}/alarms")
print(f"  sites  → {vol_path}/sites")
