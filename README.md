# Data Community Onboarding — Lakehouse hands-on

A self-contained, end-to-end walkthrough of the Databricks lakehouse: **raw Parquet → Bronze →
Silver → Gold → govern → dashboard → Genie**, all in *your own* Unity Catalog catalog and schema.
It mirrors the AT&T Network Fault Analytics demo (telecom fault/alarm events + cell-site inventory)
so you can build the whole thing hands-on.

No external data and no pre-existing Volume required — **notebook 01 generates everything**.

## Prerequisites

- A Databricks workspace with **Unity Catalog** and **serverless** (or any running compute).
- Write access to **a catalog + schema + Volume**. `CREATE` privilege is nice-to-have but **not
  required**: notebook 01 *tries* to create them and, if you lack permission, **skips creation and
  uses the existing ones** named in the widgets (it checks access first and stops with a clear
  message if they aren't reachable). Can't create a catalog? Just point the widgets at one you can
  already write to.
- A **SQL warehouse** — used by the dashboard and the Genie notebooks (06, 07).
- **Optional:** read access to `system.access.*` for the full observability notebook (04). It's an
  account-level grant from a metastore/account admin; the rest of the repo works without it.

Prefer clicking over code? See **[docs/ui_guide.md](docs/ui_guide.md)** — browse the catalog, create
a table from a Volume file, and explore in the SQL Editor, all in the UI.

## How to run

1. **Add this repo as a Git folder** in Databricks: *Workspace → Create → Git folder →*
   `https://github.com/razibayati24/data_community_onboarding.git`
2. Run the steps **in order**. Set the **same `catalog` / `schema` widgets** on every notebook
   (notebook 01 adds `volume` + scale; the Genie notebooks add `warehouse_id`).

| Step | File | What it does |
|---|---|---|
| **1 · Generate** | [`notebooks/01_generate_parquet`](notebooks/01_generate_parquet.py) | Creates your catalog/schema/Volume and lands two raw Parquet feeds (`alarms`, `sites`) with nulls + duplicates seeded on purpose. |
| **2 · Explore** | [`notebooks/02_explore_raw`](notebooks/02_explore_raw.py) | Read-only profiling — nulls, duplicates, distributions, the join — in **PySpark** and in **SQL** (paste into the SQL Editor). |
| **3 · Model** | [`notebooks/03_silver_gold`](notebooks/03_silver_gold.py) | Builds **`silver_alarms`** (join + clean + dedupe) and **`gold_site_health`** (per-site KPIs, `fault_score`, coordinates) with SQL. |
| **4 · Govern** | [`observability/04_table_observability`](observability/04_table_observability.py) | Audit **who's querying** the tables (`system.access.audit`), **who's granted** access, **lineage**, table history — plus a template to **email users automatically** on a schedule. |
| **5 · Visualize** | [`dashboard/`](dashboard/README.md) | The exec **AI/BI dashboard** on `gold_site_health` — KPIs, a fault **map**, regional bars, worst-sites table, an alarm forecast. Import the `.lvdash.json`, point it at a warehouse. |
| **6 · Ask** | [`genie/06_create_genie_room`](genie/06_create_genie_room.py) | Builds a **Genie room** (natural-language Q&A) on Gold + Silver via the API — instructions, example SQL, and benchmark questions so "worst sites" ranks by `fault_score`. |
| **7 · Prove** | [`genie/07_genie_plus_benchmark`](genie/07_genie_plus_benchmark.py) | **Benchmark → tune → re-benchmark**: scores Genie answers against ground truth, adds advanced instructions + join/SQL samples, re-scores, and prints the before→after accuracy lift. |

Steps 1–3 are the core path and run in well under a minute on serverless (defaults: 1,000 sites ·
200,000 alarms — bump the `num_sites` / `num_alarms` widgets on 01 for more). Steps 4–7 are
independent add-ons that each build on the tables from step 3.

## Repo layout

```
data_community_onboarding/
├── notebooks/        01 generate · 02 explore · 03 silver+gold   (the core path)
├── observability/    04 audit usage / grants / lineage + notify
├── dashboard/        exec dashboard (.lvdash.json) + import guide
├── genie/            06 create Genie room · 07 benchmark & tune
└── docs/             ui_guide.md — the point-and-click path
```

## What you'll build

```
<catalog>.<schema>.silver_alarms      clean, joined, deduped per-alarm detail
<catalog>.<schema>.gold_site_health   one row per site — KPIs + lat/long for a map
```

`fault_score` is severity-weighted (**Critical 10 · Major 5 · Minor 2 · Warning 1**) — the single
metric everything ranks "worst sites" by, from the dashboard to Genie.

## Extend it

- **Lakeflow Designer** — rebuild the same Bronze→Silver→Gold flow no-code as a Declarative Pipeline.
- **Schedule it** — wrap notebooks 01→03 (and 04) in a Databricks Workflows Job with a cron trigger.
- **Genie Evaluation** — promote the notebook-07 benchmark questions into the product's Evaluation UI.

## A note on clouds

Doc links throughout use the `…/aws/en/…` path. On **Azure** swap to
`learn.microsoft.com/en-us/azure/databricks/`; on **GCP** use `docs.databricks.com/gcp/en/`. The SQL
and notebook code is cloud-agnostic.

## How to get updates

This repo is updated over time. In your Databricks **Git folder**, click the branch/git icon →
**Pull** to fast-forward to the latest. (A ZIP download is a static snapshot — clone or use a Git
folder so you can pull.)
