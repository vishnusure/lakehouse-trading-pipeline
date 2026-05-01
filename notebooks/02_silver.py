# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # 02 — Silver Layer: Clean & Conform
# MAGIC
# MAGIC Reads from Bronze, enforces schema, runs DQ checks, deduplicates, and writes
# MAGIC clean rows to Silver. Bad rows are quarantined with a rejection reason.
# MAGIC
# MAGIC **Inputs**
# MAGIC - `{catalog}.bronze.market_prices_raw`
# MAGIC - `{catalog}.bronze.trades_raw`
# MAGIC
# MAGIC **Outputs**
# MAGIC - `{catalog}.silver.market_prices`      — MERGE key: `(ticker, trade_date)`
# MAGIC - `{catalog}.silver.trades_clean`       — MERGE key: `(trade_id)`
# MAGIC - `{catalog}.silver.trades_quarantine`  — MERGE key: `(trade_id, version)`
# MAGIC
# MAGIC **DQ checks applied to trades**
# MAGIC 1. `null_price`             — price IS NULL
# MAGIC 2. `null_quantity`          — quantity IS NULL
# MAGIC 3. `invalid_price_format`   — price cannot be cast to DOUBLE (e.g. "N/A")
# MAGIC 4. `non_positive_price`     — CAST(price) <= 0
# MAGIC 5. `non_positive_quantity`  — CAST(quantity) <= 0
# MAGIC 6. `symbol_not_in_prices`   — symbol has no matching ticker in bronze prices

# COMMAND ----------

dbutils.widgets.text("run_date",     "",          "Run Date (YYYY-MM-DD, blank = today)")
dbutils.widgets.text("env",          "prod",      "Environment")
dbutils.widgets.text("catalog_name", "workspace", "Unity Catalog name")

# COMMAND ----------

import datetime, json

from pyspark.sql import functions as F
from pyspark.sql.window import Window

catalog   = dbutils.widgets.get("catalog_name")
env       = dbutils.widgets.get("env")
_run_date = dbutils.widgets.get("run_date").strip()
run_date  = datetime.date.fromisoformat(_run_date) if _run_date else datetime.date.today()

print(f"catalog={catalog}  env={env}  run_date={run_date}")

# COMMAND ----------
# ── Silver market prices ───────────────────────────────────────────────────────
# Bronze prices are already typed correctly; Silver adds:
#   • a sanity filter (close_price > 0)
#   • lineage columns (_bronze_ts renamed, _silver_ts added)
#   • idempotent MERGE on (ticker, trade_date)

bronze_prices = (
    spark.table(f"{catalog}.bronze.market_prices_raw")
         .filter(F.col("close_price") > 0)                       # sanity guard
         .withColumnRenamed("_ingest_ts", "_bronze_ts")
         .withColumn("_silver_ts", F.current_timestamp())
         .coalesce(1)
)

prices_rows_in  = spark.table(f"{catalog}.bronze.market_prices_raw").count()
prices_rows_out = bronze_prices.count()
prices_rejected = prices_rows_in - prices_rows_out
print(f"market_prices — in={prices_rows_in}  rejected={prices_rejected}  staging={prices_rows_out}")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.silver.market_prices (
    ticker       STRING    NOT NULL COMMENT 'Stock ticker symbol (e.g. AAPL)',
    trade_date   DATE      NOT NULL COMMENT 'Trading date',
    open_price   DOUBLE             COMMENT 'Opening price in USD',
    high_price   DOUBLE             COMMENT 'Intraday high price in USD',
    low_price    DOUBLE             COMMENT 'Intraday low price in USD',
    close_price  DOUBLE             COMMENT 'Closing price in USD — validated > 0',
    volume       LONG               COMMENT 'Number of shares traded',
    _source      STRING             COMMENT 'Source system identifier',
    _bronze_ts   TIMESTAMP          COMMENT 'UTC ingest timestamp from Bronze layer',
    _silver_ts   TIMESTAMP          COMMENT 'UTC timestamp when Silver processed this row'
)
USING DELTA
COMMENT 'Silver layer: cleaned, type-validated daily OHLCV prices. Sanity-filtered (close_price > 0). Idempotent MERGE on (ticker, trade_date).'
CLUSTER BY (ticker, trade_date)
""")

bronze_prices.createOrReplaceTempView("_silver_prices_stage")

spark.sql(f"""
MERGE INTO {catalog}.silver.market_prices AS t
USING _silver_prices_stage AS s
  ON t.ticker = s.ticker AND t.trade_date = s.trade_date
WHEN MATCHED THEN
    UPDATE SET *
WHEN NOT MATCHED THEN
    INSERT *
""")

prices_total_out = spark.table(f"{catalog}.silver.market_prices").count()
print(f"silver.market_prices total rows after MERGE: {prices_total_out}")

# COMMAND ----------
# ── Silver trades: deduplication ──────────────────────────────────────────────
# Keep the highest version per trade_id. This eliminates the 20 v1 rows that
# have a corresponding v2 update (v2 wins). Dirty rows (null price, bad type,
# etc.) are new unique trade_ids so they survive dedup and are caught by DQ.

dedup_window = Window.partitionBy("trade_id").orderBy(F.col("version").desc())

bronze_trades_raw = spark.table(f"{catalog}.bronze.trades_raw")
trades_rows_in    = bronze_trades_raw.count()

deduped = (
    bronze_trades_raw
    .withColumn("_rn", F.row_number().over(dedup_window))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)
trades_rows_deduped = deduped.count()
rows_deduplicated   = trades_rows_in - trades_rows_deduped
print(f"trades dedup — in={trades_rows_in}  removed={rows_deduplicated}  after_dedup={trades_rows_deduped}")

# COMMAND ----------
# ── Silver trades: DQ checks ───────────────────────────────────────────────────
# Collect the valid symbol set from bronze prices (small list — safe to broadcast)

valid_symbols = [
    r["ticker"]
    for r in spark.table(f"{catalog}.bronze.market_prices_raw")
                  .select("ticker").distinct().collect()
]
print(f"Valid symbols from bronze prices: {valid_symbols}")

dq_checked = deduped.withColumn(
    "rejection_reason",
    F.when(F.col("price").isNull(),                                   F.lit("null_price"))
     .when(F.col("quantity").isNull(),                                F.lit("null_quantity"))
     .when(F.expr("TRY_CAST(price AS DOUBLE)").isNull(),             F.lit("invalid_price_format"))
     .when(F.expr("TRY_CAST(price AS DOUBLE)") <= 0,                 F.lit("non_positive_price"))
     .when(F.expr("TRY_CAST(quantity AS DOUBLE)") <= 0,              F.lit("non_positive_quantity"))
     .when(~F.col("symbol").isin(valid_symbols),                      F.lit("symbol_not_in_prices"))
     .otherwise(F.lit(None).cast("string"))
)

clean_df     = dq_checked.filter(F.col("rejection_reason").isNull()).drop("rejection_reason")
quarantine_df = dq_checked.filter(F.col("rejection_reason").isNotNull())

trades_rows_clean    = clean_df.count()
trades_rows_rejected = quarantine_df.count()
print(f"DQ result — clean={trades_rows_clean}  quarantined={trades_rows_rejected}")

# COMMAND ----------
# ── Prepare clean trades for Silver ───────────────────────────────────────────
# Cast price and quantity from STRING to DOUBLE; add lineage timestamps.

clean_silver = (
    clean_df
    .withColumn("price",      F.expr("CAST(price AS DOUBLE)"))
    .withColumn("quantity",   F.expr("CAST(quantity AS DOUBLE)"))
    .withColumnRenamed("_ingest_ts", "_bronze_ts")
    .withColumn("_silver_ts", F.current_timestamp())
    .coalesce(1)
)

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.silver.trades_clean (
    trade_id     STRING    NOT NULL COMMENT 'Unique trade identifier — one row per trade_id after dedup',
    version      INT       NOT NULL COMMENT 'Highest version seen in Bronze for this trade_id',
    symbol       STRING             COMMENT 'Instrument symbol — validated against bronze price table',
    book         STRING             COMMENT 'Trading book (e.g. EQUITY-US, FIXED-INCOME)',
    trade_date   DATE               COMMENT 'Trade execution date',
    quantity     DOUBLE             COMMENT 'Quantity cast from Bronze STRING — validated > 0',
    price        DOUBLE             COMMENT 'Price cast from Bronze STRING — validated > 0',
    counterparty STRING             COMMENT 'Counterparty short name',
    trader_id    STRING             COMMENT 'Trader identifier',
    _source      STRING             COMMENT 'Source system identifier',
    _bronze_ts   TIMESTAMP          COMMENT 'UTC ingest timestamp from Bronze layer',
    _silver_ts   TIMESTAMP          COMMENT 'UTC timestamp when Silver processed this row'
)
USING DELTA
COMMENT 'Silver layer: deduped (max version per trade_id), DQ-checked trades. Referentially consistent with silver.market_prices. MERGE key: (trade_id).'
CLUSTER BY (trade_id, trade_date)
""")

clean_silver.createOrReplaceTempView("_trades_clean_stage")

spark.sql(f"""
MERGE INTO {catalog}.silver.trades_clean AS t
USING _trades_clean_stage AS s
  ON t.trade_id = s.trade_id
WHEN MATCHED THEN
    UPDATE SET *
WHEN NOT MATCHED THEN
    INSERT *
""")

trades_clean_total = spark.table(f"{catalog}.silver.trades_clean").count()
print(f"silver.trades_clean total rows after MERGE: {trades_clean_total}")

# COMMAND ----------
# ── Uniqueness assertion on trades_clean ──────────────────────────────────────

distinct_ids = spark.table(f"{catalog}.silver.trades_clean").select("trade_id").distinct().count()
assert distinct_ids == trades_clean_total, (
    f"Uniqueness FAILED: {distinct_ids} distinct trade_ids vs {trades_clean_total} rows"
)
print(f"Uniqueness assertion PASSED: {distinct_ids} unique trade_ids")

# COMMAND ----------
# ── Quarantine table ───────────────────────────────────────────────────────────

quarantine_silver = (
    quarantine_df
    .withColumnRenamed("price",    "price_raw")
    .withColumnRenamed("quantity", "quantity_raw")
    .withColumn("_quarantine_ts", F.current_timestamp())
    .drop("_ingest_ts")
    .coalesce(1)
)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.silver.trades_quarantine (
    trade_id         STRING    NOT NULL COMMENT 'Trade identifier of the rejected row',
    version          INT       NOT NULL COMMENT 'Version of the rejected row',
    symbol           STRING             COMMENT 'Instrument symbol',
    book             STRING             COMMENT 'Trading book',
    trade_date       DATE               COMMENT 'Trade execution date',
    quantity_raw     STRING             COMMENT 'Original raw quantity string from Bronze (preserved for audit)',
    price_raw        STRING             COMMENT 'Original raw price string from Bronze (preserved for audit)',
    counterparty     STRING             COMMENT 'Counterparty short name',
    trader_id        STRING             COMMENT 'Trader identifier',
    rejection_reason STRING    NOT NULL COMMENT 'DQ failure code: null_price | null_quantity | invalid_price_format | non_positive_price | non_positive_quantity | symbol_not_in_prices',
    _quarantine_ts   TIMESTAMP          COMMENT 'UTC timestamp when this row was quarantined',
    _source          STRING             COMMENT 'Source system identifier'
)
USING DELTA
COMMENT 'Silver layer: trades rejected by DQ checks. Each row carries a rejection_reason explaining why it failed. MERGE key: (trade_id, version).'
CLUSTER BY (trade_id)
""")

quarantine_silver.createOrReplaceTempView("_trades_quarantine_stage")

spark.sql(f"""
MERGE INTO {catalog}.silver.trades_quarantine AS t
USING _trades_quarantine_stage AS s
  ON t.trade_id = s.trade_id AND t.version = s.version
WHEN MATCHED THEN
    UPDATE SET *
WHEN NOT MATCHED THEN
    INSERT *
""")

quarantine_total = spark.table(f"{catalog}.silver.trades_quarantine").count()
print(f"silver.trades_quarantine total rows after MERGE: {quarantine_total}")

# COMMAND ----------
# ── Conditional OPTIMIZE (every 50 silver runs) ────────────────────────────────

try:
    silver_runs = spark.sql(
        f"SELECT COUNT(*) AS n FROM {catalog}.audit.pipeline_metrics WHERE task = 'silver'"
    ).collect()[0]["n"]
except Exception:
    silver_runs = 0

if silver_runs > 0 and silver_runs % 50 == 0:
    print(f"Run #{silver_runs}: triggering OPTIMIZE on silver tables")
    spark.sql(f"OPTIMIZE {catalog}.silver.market_prices")
    spark.sql(f"OPTIMIZE {catalog}.silver.trades_clean")
    spark.sql(f"OPTIMIZE {catalog}.silver.trades_quarantine")
else:
    print(f"Skipping OPTIMIZE — silver run #{silver_runs + 1} (fires at multiples of 50)")

# COMMAND ----------
# ── Metrics payload ────────────────────────────────────────────────────────────

metrics = {
    "task":                   "silver",
    "rows_in":                prices_rows_in + trades_rows_in,
    "rows_out":               prices_total_out + trades_clean_total,
    "rows_in_prices":         prices_rows_in,
    "rows_out_prices":        prices_total_out,
    "rows_in_trades":         trades_rows_in,
    "rows_out_trades":        trades_clean_total,
    "rejects":                trades_rows_rejected + prices_rejected,
    "rejects_prices":         prices_rejected,
    "rejects_trades":         trades_rows_rejected,
    "duplicates":             rows_deduplicated,
    "run_ts":                 datetime.datetime.utcnow().isoformat(),
    "catalog":                catalog,
    "schema":                 "silver",
}
print(json.dumps(metrics, indent=2))
dbutils.notebook.exit(json.dumps(metrics))
