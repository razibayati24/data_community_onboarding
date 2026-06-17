# Databricks notebook source
# MAGIC %md
# MAGIC # 07 · Genie+ — benchmark, tune instructions, re-benchmark
# MAGIC
# MAGIC This notebook **measures** how much Genie instructions actually help. It:
# MAGIC 1. Builds a throwaway **benchmark room** on your tables, seeded with the *base* instructions
# MAGIC    from notebook 05.
# MAGIC 2. **Round 1** — asks a set of benchmark questions and scores each answer against ground
# MAGIC    truth computed directly from your tables.
# MAGIC 3. Adds **more instructions + join/SQL samples** targeting the questions that fail.
# MAGIC 4. **Round 2** — asks the same questions again and scores.
# MAGIC 5. Prints the **before → after** accuracy so you can see the lift.
# MAGIC
# MAGIC Each question is graded automatically: we compute the correct answer with SQL on your own
# MAGIC `gold_site_health` / `silver_alarms`, then check Genie's answer (its executed SQL result **and**
# MAGIC its text reply) contains it.
# MAGIC
# MAGIC > Run notebooks **01–03** first. Genie is non-deterministic, so the exact numbers vary run to
# MAGIC > run — but the harder, instruction-dependent questions should improve in Round 2.

# COMMAND ----------

dbutils.widgets.text("catalog",      "main",               "1 · Catalog")
dbutils.widgets.text("schema",       "network_onboarding", "2 · Schema")
dbutils.widgets.text("warehouse_id", "",                   "3 · Warehouse ID (blank = auto-pick)")
dbutils.widgets.dropdown("cleanup",  "true", ["true", "false"], "4 · Trash benchmark room at end?")

catalog = dbutils.widgets.get("catalog").strip()
schema  = dbutils.widgets.get("schema").strip()
wh_id   = dbutils.widgets.get("warehouse_id").strip()
cleanup = dbutils.widgets.get("cleanup") == "true"

gold   = f"{catalog}.{schema}.gold_site_health"
silver = f"{catalog}.{schema}.silver_alarms"

# COMMAND ----------

import time
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
def api(method, path, body=None):
    return w.api_client.do(method, path, body=body)

if not wh_id:
    whs = list(w.warehouses.list())
    if not whs:
        raise Exception("No SQL warehouse found — set the warehouse_id widget.")
    chosen = next((x for x in whs if str(x.state) == "RUNNING"), whs[0])
    wh_id = chosen.id
    print(f"Auto-picked warehouse: {chosen.name} ({wh_id})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helpers — ask Genie, and grade an answer against ground truth

# COMMAND ----------

def ask_genie(space_id, question, max_wait=150):
    """Ask one question in a fresh conversation; return (generated_sql, text_answer, status)."""
    start = api("POST", f"/api/2.0/genie/spaces/{space_id}/start-conversation", {"content": question})
    conv, msg = start["conversation_id"], start["message_id"]
    waited = 0
    m = {}
    while waited < max_wait:
        m = api("GET", f"/api/2.0/genie/spaces/{space_id}/conversations/{conv}/messages/{msg}")
        if m.get("status") in ("COMPLETED", "FAILED", "CANCELLED"):
            break
        time.sleep(4); waited += 4
    sql, texts = None, []
    for a in m.get("attachments", []):
        if "query" in a and a["query"].get("query"):
            sql = a["query"]["query"]
        if "text" in a and a["text"].get("content"):
            texts.append(a["text"]["content"])
    return sql, " ".join(texts), m.get("status")

def grade(space_id, question, truth_sql):
    """Pass if the ground-truth value appears in Genie's executed result OR its text answer."""
    truth = str(spark.sql(truth_sql).first()[0])
    sql, text, status = ask_genie(space_id, question)
    hay = text or ""
    if sql:
        try:
            row = spark.sql(sql.rstrip().rstrip(";")).limit(1).collect()
            if row:
                hay += " " + " ".join(str(v) for v in row[0])
        except Exception:
            hay += " [genie-sql-failed-to-run]"
    passed = truth.lower() in hay.lower()
    return {"question": question, "truth": truth, "pass": passed,
            "genie_sql": (sql or "(text-only answer)")[:160]}

def run_benchmark(space_id, label):
    print(f"\n=== {label} ===")
    results = []
    for b in BENCHMARKS:
        r = grade(space_id, b["q"], b["truth_sql"])
        results.append(r)
        print(f"  [{'PASS' if r['pass'] else 'FAIL'}] {b['q']}  (truth={r['truth']})")
    score = sum(r["pass"] for r in results)
    print(f"  → {score}/{len(results)} correct")
    return results, score

# COMMAND ----------

# MAGIC %md
# MAGIC ## The benchmark questions (with ground truth from *your* data)
# MAGIC The first two are covered by the base instructions; the last three are **harder** —
# MAGIC they need metric nuance or a join, which the base room doesn't yet guide.

# COMMAND ----------

BENCHMARKS = [
    {"q": "show me the worst performing sites",
     "truth_sql": f"SELECT site_id FROM {gold} ORDER BY fault_score DESC LIMIT 1"},
    {"q": "which region has the most critical alarms?",
     "truth_sql": f"SELECT region FROM {gold} GROUP BY region ORDER BY SUM(critical_alarms) DESC LIMIT 1"},
    # --- harder: average-of-averages pitfall (must use alarm-level silver, not avg of gold averages) ---
    {"q": "which vendor has the worst (highest) mean time to repair?",
     "truth_sql": f"SELECT vendor FROM {silver} WHERE time_to_clear_min IS NOT NULL GROUP BY vendor ORDER BY AVG(time_to_clear_min) DESC LIMIT 1"},
    # --- harder: needs a subquery/join from gold (worst site) into silver (its alarms) ---
    {"q": "for the single worst site by fault score, what is its most frequent probable cause?",
     "truth_sql": f"SELECT probable_cause FROM {silver} WHERE site_id = (SELECT site_id FROM {gold} ORDER BY fault_score DESC LIMIT 1) GROUP BY probable_cause ORDER BY COUNT(*) DESC LIMIT 1"},
    # --- harder: ratio, not a raw count ---
    {"q": "which region has the highest share of active alarms relative to its total alarms?",
     "truth_sql": f"SELECT region FROM {gold} GROUP BY region ORDER BY SUM(active_alarms)/SUM(total_alarms) DESC LIMIT 1"},
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build the benchmark room with the **base** instructions (mirrors notebook 05)

# COMMAND ----------

BASE_TEXT = """Metric definitions and ranking rules:
- fault_score is a severity-weighted score per site (Critical=10, Major=5, Minor=2, Warning=1). ALWAYS rank 'worst', 'most problematic', 'top' sites by fault_score DESC.
- critical_alarms = count of Critical alarms. active_alarms = uncleared alarms (is_active = true). avg_mttr_min = mean minutes to clear; lower is better.
- latitude/longitude are site coordinates. A site is identified by site_id; region is the market.

Table guidance: gold_site_health = one row per site (rankings, roll-ups, map). silver_alarms = one row per alarm (time trends, per-alarm detail). Prefer gold unless alarm-level detail is needed.
Defaults: no time range = all data; group by region for market questions; never count NULL severity/site_id."""

room = api("POST", "/api/2.0/data-rooms", body={
    "display_name": "AT&T Network Operations — Genie (benchmark)",
    "description": "Throwaway room for notebook 07 benchmark.",
    "warehouse_id": wh_id, "run_as_type": "VIEWER",
    "table_identifiers": [gold, silver],
})
space_id = room["id"]
api("POST", f"/api/2.0/data-rooms/{space_id}/instructions",
    body={"title": "Metrics & ranking rules", "instruction_type": "TEXT_INSTRUCTION", "content": BASE_TEXT})
print(f"Benchmark room: {space_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Round 1 — benchmark with base instructions only

# COMMAND ----------

round1, score1 = run_benchmark(space_id, "ROUND 1 · base instructions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add more instructions + SQL/join samples
# MAGIC These target exactly the harder questions: the **average-of-averages** trap for vendor MTTR,
# MAGIC the **worst-site → its alarms** join, and the **active-share ratio**.

# COMMAND ----------

PLUS_TEXT = """Advanced metric rules:
- VENDOR or REGION mean-time-to-repair must be computed from silver_alarms.time_to_clear_min (alarm-level), grouped by vendor/region. Do NOT average gold_site_health.avg_mttr_min — that is an average of per-site averages and is biased.
- 'Share of active alarms' for a region = SUM(active_alarms) / SUM(total_alarms) from gold_site_health, grouped by region.
- To analyze the alarms of a specific site (e.g. the worst site), find the site in gold_site_health (ORDER BY fault_score DESC), then query silver_alarms filtered to that site_id for per-alarm detail like probable_cause."""

plus_instructions = [
    {"title": "Advanced metric rules", "instruction_type": "TEXT_INSTRUCTION", "content": PLUS_TEXT},
    {"title": "Vendor MTTR (alarm-level, no avg-of-avg)", "instruction_type": "SQL_INSTRUCTION", "content":
        f"SELECT vendor, ROUND(AVG(time_to_clear_min),1) AS mttr_min\n"
        f"FROM {silver}\nWHERE time_to_clear_min IS NOT NULL\nGROUP BY vendor\nORDER BY mttr_min DESC"},
    {"title": "Worst site → its top probable causes (join)", "instruction_type": "SQL_INSTRUCTION", "content":
        f"SELECT s.probable_cause, COUNT(*) AS alarms\n"
        f"FROM {silver} s\n"
        f"JOIN (SELECT site_id FROM {gold} ORDER BY fault_score DESC LIMIT 1) w\n"
        f"  ON s.site_id = w.site_id\nGROUP BY s.probable_cause\nORDER BY alarms DESC"},
    {"title": "Active-alarm share by region", "instruction_type": "SQL_INSTRUCTION", "content":
        f"SELECT region, ROUND(SUM(active_alarms)/SUM(total_alarms),3) AS active_share\n"
        f"FROM {gold}\nGROUP BY region\nORDER BY active_share DESC"},
]
for ins in plus_instructions:
    api("POST", f"/api/2.0/data-rooms/{space_id}/instructions", body=ins)
    print(f"  + {ins['instruction_type']}: {ins['title']}")

# small pause so the new instructions are picked up
time.sleep(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Round 2 — same questions, richer instructions

# COMMAND ----------

round2, score2 = run_benchmark(space_id, "ROUND 2 · base + advanced instructions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results — before → after

# COMMAND ----------

import pandas as pd
rows = []
for b, r1, r2 in zip(BENCHMARKS, round1, round2):
    rows.append({
        "question": b["q"],
        "ground_truth": r1["truth"],
        "round1": "✅" if r1["pass"] else "❌",
        "round2": "✅" if r2["pass"] else "❌",
        "improved": "⬆️" if (r2["pass"] and not r1["pass"]) else "",
    })
summary = pd.DataFrame(rows)
print(f"Round 1 (base):     {score1}/{len(BENCHMARKS)}")
print(f"Round 2 (tuned):    {score2}/{len(BENCHMARKS)}")
print(f"Lift:               {score2 - score1:+d} question(s)")
display(summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Takeaway & cleanup
# MAGIC The control questions pass in both rounds; the harder ones (vendor MTTR, the worst-site join,
# MAGIC the active-share ratio) are where targeted instructions + SQL samples move the needle. This is
# MAGIC the loop you run on a real Genie space: **benchmark → read the failures → add an instruction or
# MAGIC pinned query → re-benchmark.**
# MAGIC
# MAGIC In the product this is **Genie → Evaluation**, where curated questions are scored for you.

# COMMAND ----------

if cleanup:
    api("DELETE", f"/api/2.0/data-rooms/{space_id}")
    print(f"Trashed benchmark room {space_id}.")
else:
    host = w.config.host.rstrip("/")
    print(f"Kept benchmark room: {host}/genie/rooms/{space_id}")
