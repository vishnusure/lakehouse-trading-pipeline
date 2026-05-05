# Databricks notebook source
# MAGIC %md
# MAGIC ## Job runner: orchestrates 01 -> 02 -> 03 and persists run metrics
# MAGIC
# MAGIC Sole task in the bundle. Runs the medallion notebooks in sequence,
# MAGIC captures the metrics each one returns via dbutils.notebook.exit, and
# MAGIC appends a row per layer to workspace.audit.pipeline_metrics.

# COMMAND ----------

"""
Orchestrator for the Stooq medallion pipeline.

Reads:
  Whatever 01-03 read.

Writes:
  workspace.audit.pipeline_metrics  (one row per medallion layer per run)

The local Stooq ingest (scripts/ingest_stooq_local.py) MUST run before this
notebook. It deposits CSVs in /Volumes/workspace/bronze/stooq_raw_volume/
which 01_bronze consumes. This notebook does not fetch from Stooq.

Idempotent: each child notebook is idempotent, this notebook only appends to
the audit table, and OPTIMIZE (when triggered) is safe to repeat.
"""

# COMMAND ----------

import json
import uuid
from datetime import datetime

from pyspark.sql import Row

dbutils.widgets.text("catalog_name", "workspace")
dbutils.widgets.text("env", "dev")
dbutils.widgets.text("run_date", "")

CATALOG = dbutils.widgets.get("catalog_name")
ENV = dbutils.widgets.get("env")
RUN_DATE_PARAM = dbutils.widgets.get("run_date")

AUDIT_TABLE = f"{CATALOG}.audit.pipeline_metrics"
NOTEBOOK_TIMEOUT_SECONDS = 600

# COMMAND ----------

# Audit table schema is fixed and kept in sync with the metric blobs the
# child notebooks return. CREATE TABLE IF NOT EXISTS is idempotent and
# carries the column comments.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {AUDIT_TABLE} (
    run_id     STRING    COMMENT 'UUID minted per orchestration; shared across the three rows of one run.',
    task       STRING    COMMENT 'Medallion layer name: bronze, silver, or gold.',
    catalog    STRING    COMMENT 'Unity Catalog target.',
    schema     STRING    COMMENT 'Schema name; matches task.',
    rows_in    LONG      COMMENT 'Input rows for the layer.',
    rows_out   LONG      COMMENT 'Output rows for the layer (sum across all output tables).',
    rejects    LONG      COMMENT 'Rows quarantined by silver DQ rules; zero for bronze and gold.',
    duplicates LONG      COMMENT 'Rows collapsed by MERGE (bronze) or removed by latest-version-wins dedup (silver); zero for gold.',
    run_ts     TIMESTAMP COMMENT 'When the audit row was written.',
    env        STRING    COMMENT 'Environment label from the env widget.',
    run_date   STRING    COMMENT 'Logical run date from the run_date widget; defaults to today when empty.'
)
USING DELTA
COMMENT 'One row per medallion task per orchestration run, written by notebooks/04_job_runner.'
""")

# COMMAND ----------

# Run each child notebook with the same widget passthrough. Notebook paths
# are resolved relative to this notebook's parent folder by Databricks, so
# they work both when imported into the workspace UI and when deployed via
# the bundle.
run_id = str(uuid.uuid4())

passthrough = {
    "catalog_name": CATALOG,
    "env": ENV,
    "run_date": RUN_DATE_PARAM,
}

results = []
for nb in ("01_bronze", "02_silver", "03_gold"):
    metrics_blob = dbutils.notebook.run(nb, NOTEBOOK_TIMEOUT_SECONDS, passthrough)
    results.append(json.loads(metrics_blob))

# COMMAND ----------

now = datetime.now()
effective_run_date = RUN_DATE_PARAM or now.strftime("%Y-%m-%d")

audit_rows = [
    Row(
        run_id=run_id,
        task=r["task"],
        catalog=CATALOG,
        schema=r["schema"],
        rows_in=int(r["rows_in"]),
        rows_out=int(r["rows_out"]),
        rejects=int(r["rejects"]),
        duplicates=int(r["duplicates"]),
        run_ts=now,
        env=ENV,
        run_date=effective_run_date,
    )
    for r in results
]

(spark.createDataFrame(audit_rows)
    .write
    .format("delta")
    .mode("append")
    .saveAsTable(AUDIT_TABLE))

# COMMAND ----------

# Periodic compaction: every 50th run, OPTIMIZE the medallion tables to
# keep file counts in check on Free Edition. VACUUM is intentionally not
# run here; it removes Delta time-travel history and the brief did not
# require it.
run_count = spark.sql(f"SELECT COUNT(*) AS c FROM {AUDIT_TABLE}").first()["c"]

medallion_tables = [
    f"{CATALOG}.bronze.market_prices_raw",
    f"{CATALOG}.bronze.trades_raw",
    f"{CATALOG}.silver.market_prices",
    f"{CATALOG}.silver.trades_clean",
    f"{CATALOG}.silver.trades_quarantine",
    f"{CATALOG}.gold.daily_pnl",
    f"{CATALOG}.gold.positions",
]

if run_count >= 50 and run_count % 50 == 0:
    for tbl in medallion_tables:
        spark.sql(f"OPTIMIZE {tbl}")

# COMMAND ----------

display(spark.sql(f"""
    SELECT task, rows_in, rows_out, rejects, duplicates, run_ts
    FROM {AUDIT_TABLE}
    WHERE run_id = '{run_id}'
    ORDER BY task
"""))
