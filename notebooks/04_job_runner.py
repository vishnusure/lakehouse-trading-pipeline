# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # 04 — Job Runner & Audit
# MAGIC
# MAGIC **DAB mode** (`standalone=false`, default when run as the 4th sequential DAB task):
# MAGIC Tasks 01–03 have already completed. This notebook queries current table counts,
# MAGIC writes a `pipeline` summary row to `audit.pipeline_metrics`, and returns.
# MAGIC
# MAGIC **Standalone mode** (`standalone=true`, for one-shot manual runs):
# MAGIC Calls 01–03 via `dbutils.notebook.run()` with a 30-minute timeout each, writes
# MAGIC per-task rows AND a summary `pipeline` row to `audit.pipeline_metrics`.
# MAGIC
# MAGIC **`audit.pipeline_metrics`** is also used by notebooks 01–03 for the OPTIMIZE
# MAGIC throttle: OPTIMIZE fires only when the run count for that task is a multiple of 50.

# COMMAND ----------

dbutils.widgets.text("run_date",     "",       "Run Date (YYYY-MM-DD, blank = today)")
dbutils.widgets.text("env",          "prod",   "Environment")
dbutils.widgets.text("catalog_name", "workspace", "Unity Catalog name")
dbutils.widgets.text("standalone",   "false",  "Standalone mode: calls 01-03 internally (true/false)")

# COMMAND ----------

import datetime, json, posixpath

from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, TimestampType,
)

catalog    = dbutils.widgets.get("catalog_name")
env        = dbutils.widgets.get("env")
_run_date  = dbutils.widgets.get("run_date").strip()
run_date   = datetime.date.fromisoformat(_run_date) if _run_date else datetime.date.today()
standalone = dbutils.widgets.get("standalone").strip().lower() == "true"

print(f"catalog={catalog}  env={env}  run_date={run_date}  standalone={standalone}")

# COMMAND ----------
# ── Create audit.pipeline_metrics ─────────────────────────────────────────────

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.audit.pipeline_metrics (
    task        STRING     NOT NULL COMMENT 'Task label: bronze | silver | gold | pipeline',
    rows_in     LONG                COMMENT 'Total input rows read by this task',
    rows_out    LONG                COMMENT 'Total output rows written by this task',
    rejects     LONG                COMMENT 'Rows rejected by DQ checks',
    duplicates  LONG                COMMENT 'Duplicate rows eliminated by deduplication',
    run_ts      TIMESTAMP  NOT NULL COMMENT 'UTC timestamp of this pipeline run',
    catalog     STRING              COMMENT 'Unity Catalog name used in this run',
    run_schema  STRING              COMMENT 'Primary schema written by this task',
    env         STRING              COMMENT 'Environment tag (prod, dev, etc.)'
)
USING DELTA
COMMENT 'Audit: append-only execution metrics. One row per task per pipeline run. The OPTIMIZE throttle in each notebook reads this table to determine if maintenance is due (every 50 runs).'
CLUSTER BY (task, run_ts)
""")

print("audit.pipeline_metrics ready")

# COMMAND ----------

METRICS_SCHEMA = StructType([
    StructField("task",        StringType(),    False),
    StructField("rows_in",     LongType(),      True),
    StructField("rows_out",    LongType(),      True),
    StructField("rejects",     LongType(),      True),
    StructField("duplicates",  LongType(),      True),
    StructField("run_ts",      TimestampType(), False),
    StructField("catalog",     StringType(),    True),
    StructField("run_schema",  StringType(),    True),
    StructField("env",         StringType(),    True),
])

def _write_metrics(m: dict):
    row = Row(
        task        = m.get("task", "unknown"),
        rows_in     = int(m.get("rows_in", 0)),
        rows_out    = int(m.get("rows_out", 0)),
        rejects     = int(m.get("rejects", 0)),
        duplicates  = int(m.get("duplicates", 0)),
        run_ts      = datetime.datetime.fromisoformat(
                          m.get("run_ts", datetime.datetime.utcnow().isoformat())
                      ),
        catalog     = m.get("catalog", catalog),
        run_schema  = m.get("schema", ""),
        env         = env,
    )
    (
        spark.createDataFrame([row], schema=METRICS_SCHEMA)
             .write.mode("append")
             .saveAsTable(f"{catalog}.audit.pipeline_metrics")
    )
    print(f"  metrics → task={row.task}  rows_in={row.rows_in}  "
          f"rows_out={row.rows_out}  rejects={row.rejects}  duplicates={row.duplicates}")

# COMMAND ----------
# ── Standalone mode: orchestrate 01 → 02 → 03 ─────────────────────────────────

if standalone:
    # Derive sibling notebook paths from this notebook's own workspace path
    # so the run works regardless of which workspace folder the bundle deploys to.
    _ctx    = dbutils.entry_point.getDbutils().notebook().getContext()
    _folder = posixpath.dirname(_ctx.notebookPath().get())

    nb_params = {
        "run_date":     run_date.isoformat(),
        "env":          env,
        "catalog_name": catalog,
    }

    print("=== 01_bronze ===")
    bronze_m = json.loads(dbutils.notebook.run(f"{_folder}/01_bronze", 1800, nb_params))
    _write_metrics(bronze_m)

    print("=== 02_silver ===")
    silver_m = json.loads(dbutils.notebook.run(f"{_folder}/02_silver", 1800, nb_params))
    _write_metrics(silver_m)

    print("=== 03_gold ===")
    gold_m = json.loads(dbutils.notebook.run(f"{_folder}/03_gold", 1800, nb_params))
    _write_metrics(gold_m)

    pipeline_metrics = {
        "task":       "pipeline",
        "rows_in":    bronze_m.get("rows_in", 0),
        "rows_out":   gold_m.get("rows_out", 0),
        "rejects":    silver_m.get("rejects", 0),
        "duplicates": silver_m.get("duplicates", 0),
        "run_ts":     datetime.datetime.utcnow().isoformat(),
        "catalog":    catalog,
        "schema":     "audit",
    }

else:
    # DAB mode: 01-03 ran as upstream tasks; derive summary from current table state.
    print("=== DAB mode: querying table counts ===")

    def _safe_count(tbl):
        try:
            return spark.table(tbl).count()
        except Exception:
            return 0

    pipeline_metrics = {
        "task":       "pipeline",
        "rows_in":    _safe_count(f"{catalog}.bronze.market_prices_raw")
                    + _safe_count(f"{catalog}.bronze.trades_raw"),
        "rows_out":   _safe_count(f"{catalog}.gold.positions")
                    + _safe_count(f"{catalog}.gold.daily_pnl"),
        "rejects":    _safe_count(f"{catalog}.silver.trades_quarantine"),
        "duplicates": 0,
        "run_ts":     datetime.datetime.utcnow().isoformat(),
        "catalog":    catalog,
        "schema":     "audit",
    }

# COMMAND ----------
# ── Write pipeline summary row ─────────────────────────────────────────────────

print("=== Pipeline summary ===")
_write_metrics(pipeline_metrics)
print(json.dumps(pipeline_metrics, indent=2))

# COMMAND ----------
# ── Show last 10 audit rows ────────────────────────────────────────────────────

display(
    spark.table(f"{catalog}.audit.pipeline_metrics")
         .orderBy("run_ts", ascending=False)
         .limit(10)
)

dbutils.notebook.exit(json.dumps(pipeline_metrics))
