# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Create the Genie room (natural-language Q&A on Gold)
# MAGIC
# MAGIC A Genie room (a.k.a. data room / Genie space) lets people ask questions in plain English and get
# MAGIC SQL-backed answers. Unlike the dashboard, a Genie space isn't a single importable file — so this
# MAGIC notebook **builds it via the API** against the tables you created in 01–03, including the same
# MAGIC instructions, example queries, and benchmark questions from the demo.
# MAGIC
# MAGIC It creates a space backed by **`gold_site_health`** + **`silver_alarms`** and loads:
# MAGIC - **Instructions** — the metric/ranking rules that make answers deterministic (rank by `fault_score`).
# MAGIC - **Example SQL** — two pinned queries Genie can learn from.
# MAGIC - **Benchmark questions** — for evaluating answer accuracy.
# MAGIC
# MAGIC > Run notebooks **01–03 first** (the tables must exist). Uses the Databricks SDK, so no manual
# MAGIC > tokens. The data-rooms API is in preview — shapes here are verified working as of 2026-06.

# COMMAND ----------

dbutils.widgets.text("catalog",      "main",               "1 · Catalog")
dbutils.widgets.text("schema",       "network_onboarding", "2 · Schema")
dbutils.widgets.text("warehouse_id", "",                   "3 · Warehouse ID (blank = auto-pick)")
dbutils.widgets.dropdown("recreate", "false", ["false", "true"], "4 · Recreate if it exists?")

catalog   = dbutils.widgets.get("catalog").strip()
schema    = dbutils.widgets.get("schema").strip()
wh_id     = dbutils.widgets.get("warehouse_id").strip()
recreate  = dbutils.widgets.get("recreate") == "true"

TITLE = "AT&T Network Operations — Genie"
gold   = f"{catalog}.{schema}.gold_site_health"
silver = f"{catalog}.{schema}.silver_alarms"

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

def api(method, path, body=None):
    """Thin wrapper over the SDK's generic API client."""
    return w.api_client.do(method, path, body=body)

# Auto-pick a warehouse if none was provided
if not wh_id:
    whs = list(w.warehouses.list())
    if not whs:
        raise Exception("No SQL warehouse found — set the warehouse_id widget.")
    chosen = next((x for x in whs if str(x.state) == "RUNNING"), whs[0])
    wh_id = chosen.id
    print(f"Auto-picked warehouse: {chosen.name} ({wh_id})")
print(f"Tables: {gold} · {silver}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Create the space (or reuse an existing one)
# MAGIC Skips creation if a room with the same title already exists, unless `recreate = true`.

# COMMAND ----------

existing = next((s for s in api("GET", "/api/2.0/genie/spaces").get("spaces", [])
                 if s.get("title") == TITLE), None)

if existing and not recreate:
    space_id = existing["space_id"]
    print(f"Reusing existing space {space_id}. Set recreate=true to build a fresh one.")
else:
    room = api("POST", "/api/2.0/data-rooms", body={
        "display_name": TITLE,
        "description": ("Ask about network faults/alarms by site, region, vendor. Backed by "
                        "gold_site_health (per-site KPIs + coordinates) and silver_alarms (per-alarm detail)."),
        "warehouse_id": wh_id,
        "run_as_type": "VIEWER",
        "table_identifiers": [gold, silver],
    })
    space_id = room["id"]
    print(f"Created space {space_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Add instructions
# MAGIC One **text** instruction (metric definitions + ranking rules + table guidance) and two **SQL**
# MAGIC examples. These are what make "show me the worst sites" resolve to `fault_score DESC` every time.

# COMMAND ----------

TEXT_INSTRUCTION = """Metric definitions and ranking rules:
- fault_score is a severity-weighted score per site: Critical=10, Major=5, Minor=2, Warning=1, summed across the site's alarms. ALWAYS rank 'worst', 'most problematic', 'unhealthiest', 'priority', or 'top' sites by fault_score DESC.
- critical_alarms = count of Critical-severity alarms. active_alarms = alarms not yet cleared (cleared_ts IS NULL / is_active = true).
- avg_mttr_min = mean minutes to clear a fault (mean time to repair); LOWER is better.
- latitude/longitude are the site coordinates — use them when the user asks for a map or 'where'.
- A site is identified by site_id; site_name is a human label; region is the market (e.g. Dallas-FortWorth, Atlanta).

Table guidance:
- gold_site_health: ONE row per site, pre-aggregated KPIs with coordinates. Use for site rankings, regional roll-ups, the map, and 'how many sites' questions.
- silver_alarms: one row per individual alarm (already joined to site inventory: region, vendor, lat/long). Use for time trends (group by event_date), per-alarm detail, severity breakdowns, and time-to-clear distributions.
Prefer gold_site_health unless the question needs alarm-level or time detail.

Defaults: when no time range is given, use all data. When the user asks about a region or market, group by region. Express MTTR in minutes. Never count rows with NULL severity or NULL site_id — the Silver layer already removed them."""

instructions = [
    {"title": "Metrics & ranking rules", "instruction_type": "TEXT_INSTRUCTION", "content": TEXT_INSTRUCTION},
    {"title": "Worst 5 sites", "instruction_type": "SQL_INSTRUCTION", "content":
        f"SELECT site_id, site_name, region, fault_score, critical_alarms, active_alarms\n"
        f"FROM {gold}\nORDER BY fault_score DESC\nLIMIT 5"},
    {"title": "Daily critical alarm trend", "instruction_type": "SQL_INSTRUCTION", "content":
        f"SELECT event_date, COUNT(*) AS critical_alarms\n"
        f"FROM {silver}\nWHERE severity = 'Critical'\nGROUP BY event_date\nORDER BY event_date"},
]

for ins in instructions:
    api("POST", f"/api/2.0/data-rooms/{space_id}/instructions", body=ins)
    print(f"  + {ins['instruction_type']}: {ins['title']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Add benchmark questions
# MAGIC Curated Q→SQL pairs. They double as **benchmarks** you can score under
# MAGIC *Genie → Evaluation* to quantify answer accuracy.

# COMMAND ----------

benchmarks = [
    {"question_text": "which region has the most critical alarms?",
     "answer_text": f"SELECT region, SUM(critical_alarms) AS total_critical\nFROM {gold}\nGROUP BY region\nORDER BY total_critical DESC"},
    {"question_text": "show me the worst performing sites",
     "answer_text": f"SELECT site_id, site_name, region, fault_score, critical_alarms\nFROM {gold}\nORDER BY fault_score DESC\nLIMIT 10"},
]

for q in benchmarks:
    api("POST", f"/api/2.0/data-rooms/{space_id}/curated-questions",
        body={"curated_question": {**q, "conversation_type": "NORMAL"}})
    print(f"  + Q: {q['question_text']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done ✅ — open your Genie room
# MAGIC Click the link, then try **"show me the worst performing sites"** — open **Instructions** to show
# MAGIC the `fault_score` rule, re-ask, and watch it answer deterministically.

# COMMAND ----------

host = w.config.host.rstrip("/")
print(f"Genie room: {host}/genie/rooms/{space_id}")
print("\nTry asking:")
print("  • show me the worst performing sites")
print("  • which region has the most critical alarms?")
print("  • what is the daily trend of critical alarms?")
print("  • map the sites by fault score")
