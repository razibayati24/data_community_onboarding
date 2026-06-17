# Doing it in the UI — no code required

The three notebooks do everything in code, but several of these steps can also be done by clicking
through the Databricks UI. This is great when you're new to the platform, exploring, or showing
someone *what* is happening before showing *how* it's coded.

> Doc links use the `…/aws/en/…` path. On **Azure** swap `docs.databricks.com/aws/en/` →
> `learn.microsoft.com/en-us/azure/databricks/`; on **GCP** use `docs.databricks.com/gcp/en/`.

---

## 1. Browse your catalog, schema and Volume (Catalog Explorer)

1. Left sidebar → **Catalog**.
2. Expand your **catalog → schema**. You'll see **Tables** and **Volumes**.
3. Click a table to see the **Columns**, **Sample Data**, **Details**, **Permissions**, **History**,
   and **Lineage** tabs — no query needed.
4. Click your **Volume** (e.g. `raw_landing`) to browse the raw files (`alarms/`, `sites/`).

📖 [Catalog Explorer](https://docs.databricks.com/aws/en/discover/catalog-explorer)

---

## 2. Upload / view files on a Volume

After notebook `01` runs, the Parquet is already on the Volume. To add or inspect files yourself:

1. **Catalog Explorer → your Volume →** the file browser.
2. Use **Upload to this volume** to drop files in, or click into `alarms/` / `sites/` to see what
   landed.

📖 [Manage files in Unity Catalog Volumes](https://docs.databricks.com/aws/en/files/volumes)

---

## 3. Create a table **from a file on a Volume** (point-and-click)

This is the UI equivalent of the `read_files(...)` CTAS in notebook `02`.

1. **Catalog Explorer → your Volume →** navigate into `sites/` (or `alarms/`).
2. Select a Parquet file → **⋮ (kebab menu) → Create table**, *or* use **+ (New) → Add or upload
   data → Create or modify table**.
3. In the **Create table** wizard: pick the target **catalog + schema**, set the **table name**,
   confirm the **column types** it inferred, then **Create table**.
4. The new managed table appears under your schema — query it from the SQL Editor.

📖 [Create a table from files (UI)](https://docs.databricks.com/aws/en/ingestion/file-upload/create-table-from-files)

> The notebook approach (`CREATE TABLE … AS SELECT * FROM read_files(...)`) does the same thing but
> is repeatable and version-controlled — which is why the demo uses it for the real pipeline.

---

## 4. Explore & manipulate data in the SQL Editor

1. Left sidebar → **SQL Editor**.
2. Pick a **SQL warehouse** (top right). Start it if it's stopped.
3. Use the **schema browser** on the left to find your tables; click a table name to insert it.
4. Run any of the SQL from notebook `02` (null profile, duplicates, distributions, the join) or
   `03` (build Silver / Gold). Results render as a table; click the **+** on results to add a quick
   **visualization** (bar / map / counter) without leaving the editor.

📖 [SQL Editor](https://docs.databricks.com/aws/en/sql/user/sql-editor/)

---

## 5. Light data manipulation without writing SQL

- **Filter / sort / search** results directly in the results grid of the SQL Editor or a notebook
  `display()` output.
- **Sample Data** tab in Catalog Explorer shows rows instantly (no query, no warehouse cost).
- **Insights** tab on a table shows the most frequent users and queries over the last 30 days.

📖 [Table Insights](https://docs.databricks.com/aws/en/discover/table-insights)

---

## When to use UI vs. notebooks

| Use the **UI** for… | Use the **notebooks** for… |
|---|---|
| One-off exploration, "what does this data look like?" | Repeatable, version-controlled pipelines |
| Onboarding / showing someone the platform | Anything you'll run more than once or schedule |
| Quick table-from-file, ad-hoc charts | The Bronze→Silver→Gold logic with data-quality rules |

The two are complementary — these notebooks are the production path; this guide is the
click-through path to understand each step first.
