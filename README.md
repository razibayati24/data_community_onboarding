# Data Community Onboarding — Lakehouse hands-on

A three-notebook, self-contained walkthrough that takes you from **raw Parquet → Bronze → Silver → Gold**
in your own Unity Catalog catalog and schema. It mirrors the AT&T Network Fault Analytics demo
(telecom fault/alarm events + cell-site inventory) so you can build the same medallion model hands-on.

No external data and no pre-existing Volume required — notebook 01 generates everything.

## Prerequisites

- A Databricks workspace with **Unity Catalog** and **serverless** (or any running compute).
- `CREATE` privilege on a catalog you can write to. Don't have one? Point the `catalog` widget at an
  existing catalog/schema where you have write access (the notebooks use `IF NOT EXISTS`).

## How to run

1. **Add this repo as a Git folder** in Databricks: *Workspace → Create → Git folder →*
   `https://github.com/razibayati24/data_community_onboarding.git`
2. Open the notebooks under `notebooks/` and run them **in order**. At the top of each, set the
   four widgets to the same values (catalog / schema / volume — and scale, on notebook 01):

| Order | Notebook | What it does |
|---|---|---|
| 1 | [`01_generate_parquet`](notebooks/01_generate_parquet.py) | Creates your catalog/schema/Volume and lands two raw Parquet feeds (`alarms`, `sites`) with seeded nulls + duplicates. |
| 2 | [`02_explore_raw`](notebooks/02_explore_raw.py) | Read-only profiling — nulls, duplicates, distributions, the join — in **PySpark** and in **SQL** (paste into the SQL Editor). |
| 3 | [`03_silver_gold`](notebooks/03_silver_gold.py) | Builds **`silver_alarms`** (join + clean + dedupe) and **`gold_site_health`** (per-site KPIs, `fault_score`, coordinates) with SQL. |

Defaults (1,000 sites · 200,000 alarms) run in well under a minute on serverless. Bump the
`num_sites` / `num_alarms` widgets on notebook 01 for a heavier dataset.

## What you'll end up with

```
<catalog>.<schema>.silver_alarms      clean, joined, deduped per-alarm detail
<catalog>.<schema>.gold_site_health   one row per site — KPIs + lat/long for a map
```

`fault_score` is severity-weighted (**Critical 10 · Major 5 · Minor 2 · Warning 1**) — the metric you
rank "worst sites" by.

## Where to go next

- **Genie** — point a room at `gold_site_health`; instruct it to rank "worst sites" by `fault_score`.
- **Dashboard** — counters (critical / active alarms, MTTR), a symbol **map** on `latitude`/`longitude`,
  and a worst-sites table.
- **Lakeflow Designer** — rebuild this same Bronze→Silver→Gold flow no-code.

These three surfaces (notebook + SQL Editor + UC) are what we covered live; Genie and the dashboard are
the natural follow-ons.
