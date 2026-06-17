# Dashboard — AT&T Network Health (Exec)

The final consumption layer: a Lakeview (AI/BI) dashboard built on **`gold_site_health`** (and
`silver_alarms` for the forecast). It's the same exec dashboard from the live demo — KPIs, a network
fault **map**, regional breakdowns, a worst-sites table, and an `ai_forecast` of alarms by region.

## Files

| File | Use this if… |
|---|---|
| `att_network_health_exec_dashboard.onboarding.lvdash.json` | You ran notebooks 01–03 with the **default** widgets (`main.network_onboarding`). Queries already point at your tables — import and run. |
| `att_network_health_exec_dashboard.lvdash.json` | You want the **original** demo export (points at `cmegdemos_catalog.att_data_community`). Use as reference, or if you built into that catalog/schema. |

> Both are the same dashboard; the only difference is the catalog.schema the 5 datasets query.

## Datasets (what each widget reads)

- **KPIs** — total critical / active alarms, network MTTR, site count
- **Site health** — per-site rows incl. `latitude`/`longitude` → the **symbol map**
- **By region** — regional roll-up (bar)
- **Top sites** — 15 worst by `fault_score`
- **Alarm Forecast by Region** — `ai_forecast(...)` over daily alarm counts from `silver_alarms`

## How to import

**UI:** Workspace → **Create → Dashboard → ⋮ → Import dashboard from file** → pick the
`.lvdash.json` → then set the dashboard's **SQL warehouse** (top-right) and **Publish**.

**CLI:**
```bash
databricks lakeview create \
  --display-name "AT&T Network Health — Exec Dashboard" \
  --warehouse-id <your_warehouse_id> \
  --serialized-dashboard "$(cat dashboard/att_network_health_exec_dashboard.onboarding.lvdash.json)" \
  --profile <your_profile>
```

## If your tables live somewhere else

Used non-default widget values? Repoint the datasets with a one-line find/replace, then import:
```bash
sed 's/main\.network_onboarding/<your_catalog>.<your_schema>/g' \
  dashboard/att_network_health_exec_dashboard.onboarding.lvdash.json > my_dashboard.lvdash.json
```
(Or edit each dataset's SQL in the dashboard UI after importing.)

## Prerequisite

`gold_site_health` and `silver_alarms` must exist — run **`03_silver_gold`** first. The map needs
non-null `latitude`/`longitude` (Gold carries these through), and the forecast needs `event_date`
in `silver_alarms`.
