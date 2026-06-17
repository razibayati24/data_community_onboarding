# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Table observability — who's using your tables (and notify them)
# MAGIC
# MAGIC Now that you've built `silver_alarms` and `gold_site_health` (notebooks 01–03), let's answer the
# MAGIC questions a **data owner** asks once a table is live:
# MAGIC - *Who is actually querying it?*
# MAGIC - *Who has been granted access — and is that access still used?*
# MAGIC - *Where did this data come from / what feeds off it?* (lineage)
# MAGIC - *Can I notify the users automatically, on a schedule?*
# MAGIC
# MAGIC We use the Gold/Silver tables from the previous steps as the subject of every example, so this
# MAGIC runs end-to-end against data you already own.
# MAGIC
# MAGIC > Two access levels appear below. **Option 1 (UI Insights)** needs nothing but access to the
# MAGIC > table. **Option 2+ (system tables)** needs read access to `system.access.*`, an account-level
# MAGIC > governance schema a **metastore/account admin** grants. If a system-table cell errors with a
# MAGIC > permission message, that's the one ask to send your admin — everything else still works.
# MAGIC >
# MAGIC > Doc links use the `…/aws/en/…` path. On **Azure** swap to
# MAGIC > `learn.microsoft.com/en-us/azure/databricks/`; on **GCP** use `docs.databricks.com/gcp/en/`.

# COMMAND ----------

dbutils.widgets.text("catalog", "main",               "1 · Catalog")
dbutils.widgets.text("schema",  "network_onboarding", "2 · Schema")
dbutils.widgets.text("table",   "gold_site_health",   "3 · Table to audit")

catalog = dbutils.widgets.get("catalog").strip()
schema  = dbutils.widgets.get("schema").strip()
table   = dbutils.widgets.get("table").strip()
fqn     = f"{catalog}.{schema}.{table}"

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA  {schema}")
print(f"Subject table: {fqn}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Option 1 — No setup: the **Insights** tab (UI, recommended first stop)
# MAGIC If you don't have system-table access yet, start here. In **Catalog Explorer**, open your table
# MAGIC (e.g. `gold_site_health`) and click the **Insights** tab — for the last 30 days it shows your
# MAGIC **most frequent users**, **most frequent queries**, and related tables. No code, no extra
# MAGIC permissions beyond access to the table itself.
# MAGIC
# MAGIC - 📖 [Catalog Explorer](https://docs.databricks.com/aws/en/discover/catalog-explorer)
# MAGIC - 📖 [Table Insights — frequent users & queries](https://docs.databricks.com/aws/en/discover/table-insights)
# MAGIC - 📖 [View & manage table permissions in the UI](https://docs.databricks.com/aws/en/data-governance/unity-catalog/manage-privileges/)
# MAGIC
# MAGIC Great for a quick look. For automation (emailing users on a schedule), continue below.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Option 2 — Who actually queried the table (system tables)
# MAGIC `system.access.audit` logs every access event. This returns each user who touched
# MAGIC **your Gold table** in the last 30 days, how often, and when they last did.
# MAGIC
# MAGIC - `getTable` → metadata / read access; `generateTemporaryTableCredential` → direct data reads.
# MAGIC - Audit data has a short ingestion delay (minutes), so look back over a window — not "right now".
# MAGIC - 📖 [System tables overview](https://docs.databricks.com/aws/en/admin/system-tables/) ·
# MAGIC   [Audit log system table](https://docs.databricks.com/aws/en/admin/system-tables/audit-logs)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   user_identity.email          AS user_email,
# MAGIC   COUNT(*)                     AS access_count,
# MAGIC   MAX(event_time)              AS last_access,
# MAGIC   MIN(event_time)              AS first_access
# MAGIC FROM system.access.audit
# MAGIC WHERE service_name = 'unityCatalog'
# MAGIC   AND action_name IN ('getTable', 'generateTemporaryTableCredential')
# MAGIC   AND request_params.full_name_arg = '${catalog}.${schema}.${table}'
# MAGIC   AND event_time > current_timestamp() - INTERVAL 30 DAYS
# MAGIC GROUP BY ALL
# MAGIC ORDER BY access_count DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Option 3 — Who is *granted* access (and stale-access check)
# MAGIC Usage tells you who *used* the table; this tells you who is *allowed* to. Comparing the two
# MAGIC surfaces grants that are no longer being exercised — a classic governance clean-up.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT grantee, privilege_type, is_grantable
# MAGIC FROM system.information_schema.table_privileges
# MAGIC WHERE table_catalog = '${catalog}'
# MAGIC   AND table_schema  = '${schema}'
# MAGIC   AND table_name    = '${table}';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Option 4 — Lineage: where this data came from
# MAGIC Your Gold table is derived from Silver, which is derived from the raw Parquet. `table_lineage`
# MAGIC makes that dependency explicit — useful for impact analysis ("if I change Silver, what breaks?").
# MAGIC You should see `silver_alarms` show up as an upstream source of `gold_site_health`.
# MAGIC
# MAGIC - 📖 [Data lineage with system tables](https://docs.databricks.com/aws/en/admin/system-tables/lineage)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT source_table_full_name, target_table_full_name, MAX(event_time) AS last_seen
# MAGIC FROM system.access.table_lineage
# MAGIC WHERE target_table_full_name = '${catalog}.${schema}.${table}'
# MAGIC   AND source_table_full_name IS NOT NULL
# MAGIC GROUP BY source_table_full_name, target_table_full_name
# MAGIC ORDER BY last_seen DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Option 5 — Table-level observability (no special access needed)
# MAGIC `DESCRIBE DETAIL` and `DESCRIBE HISTORY` work on your own Delta tables without system-table
# MAGIC access — handy for size, file count, and the full audit trail of writes/optimizes.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL ${catalog}.${schema}.${table};

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Every operation that ever touched the table (CREATE, WRITE, OPTIMIZE…), newest first
# MAGIC DESCRIBE HISTORY ${catalog}.${schema}.${table};

# COMMAND ----------

# MAGIC %md
# MAGIC ## Option 6 — Notify the users automatically
# MAGIC Take the result of the **Option 2** query and email each user. This is a **template** — fill in
# MAGIC your SMTP relay (or use SendGrid / Azure Communication Services / Amazon SES) and store the
# MAGIC credentials in a **Databricks secret scope**, never in code. Left un-run by default so it can't
# MAGIC send mail accidentally.
# MAGIC
# MAGIC - 📖 [Secret scopes](https://docs.databricks.com/aws/en/security/secrets/)

# COMMAND ----------

# --- TEMPLATE: review and fill in before running ---
SEND_EMAILS = False  # flip to True only when SMTP details below are real

users = spark.sql(f"""
    SELECT user_identity.email AS user_email,
           COUNT(*)            AS access_count,
           MAX(event_time)     AS last_access
    FROM system.access.audit
    WHERE service_name = 'unityCatalog'
      AND action_name IN ('getTable','generateTemporaryTableCredential')
      AND request_params.full_name_arg = '{fqn}'
      AND event_time > current_timestamp() - INTERVAL 30 DAYS
    GROUP BY ALL
""").collect()

print(f"{len(users)} user(s) accessed {fqn} in the last 30 days.")

if SEND_EMAILS:
    import smtplib
    from email.mime.text import MIMEText

    SMTP_HOST = "smtp.yourcompany.com"
    SENDER    = "data-governance@yourcompany.com"

    with smtplib.SMTP(SMTP_HOST) as server:
        # server.starttls(); server.login(USER, PASSWORD)  # if your relay requires auth
        for r in users:
            body = (
                f"Hello,\n\n"
                f"Automated notice: you accessed {fqn} {r['access_count']} time(s) "
                f"in the last 30 days (most recent: {r['last_access']}).\n\n"
                f"If this access is no longer needed, please let the data owner know.\n\n"
                f"- Data Governance"
            )
            msg = MIMEText(body)
            msg["Subject"] = f"Table access notification: {table}"
            msg["From"]    = SENDER
            msg["To"]      = r["user_email"]
            server.send_message(msg)
    print(f"Sent {len(users)} notification(s).")
else:
    print("SEND_EMAILS is False — no mail sent. Review the template, then set it to True.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Option 7 — Run it on a schedule
# MAGIC Wrap this notebook in a **Databricks Workflows Job** with a cron trigger (e.g. weekly, Monday
# MAGIC 08:00). It then audits the table and notifies users automatically, with zero manual effort.
# MAGIC Add **failure notifications** so you're alerted if the job itself breaks.
# MAGIC
# MAGIC - 📖 [Schedule a job](https://docs.databricks.com/aws/en/jobs/schedule-jobs) ·
# MAGIC   [Job notifications](https://docs.databricks.com/aws/en/jobs/notifications)
# MAGIC
# MAGIC ### TL;DR
# MAGIC - **Just want to look?** Catalog Explorer → **Insights** tab — no setup.
# MAGIC - **Want it automated + emailing users?** Needs **system-table access** (ask your metastore /
# MAGIC   account admin), then it's this query + the email template + a scheduled job.
