# Databricks notebook source
# MAGIC %md
# MAGIC ## Silver: typed, validated and deduplicated prices and trades
# MAGIC
# MAGIC Reads bronze, casts the string-typed trade columns, applies DQ rules,
# MAGIC quarantines failures with the reason, and keeps only the latest version
# MAGIC per trade_id in the clean output.

# COMMAND ----------

"""
Silver layer for the Stooq trading pipeline.

Inputs (consumed):
  workspace.bronze.market_prices_raw
  workspace.bronze.trades_raw

Outputs (written, all overwritten on each run):
  workspace.silver.market_prices
  workspace.silver.trades_clean
  workspace.silver.trades_quarantine

Idempotent. Silver always reflects the current bronze truth; rows that move
from quarantine to clean (or vice versa) on subsequent runs are handled
naturally by the overwrite write strategy.
"""

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F

dbutils.widgets.text("catalog_name", "workspace")
dbutils.widgets.text("env", "dev")
dbutils.widgets.text("run_date", "")

CATALOG = dbutils.widgets.get("catalog_name")

BRONZE_PRICES = f"{CATALOG}.bronze.market_prices_raw"
BRONZE_TRADES = f"{CATALOG}.bronze.trades_raw"
SILVER_PRICES = f"{CATALOG}.silver.market_prices"
SILVER_CLEAN = f"{CATALOG}.silver.trades_clean"
SILVER_QUARANTINE = f"{CATALOG}.silver.trades_quarantine"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Market prices

# COMMAND ----------

# Bronze prices are already typed; silver's job is to enforce non-null on the
# join keys and rename ticker -> symbol so trades and prices can join on a
# same-named column in gold without an explicit alias.
prices_silver = (
    spark.table(BRONZE_PRICES)
        .filter(
            F.col("ticker").isNotNull()
            & F.col("date").isNotNull()
            & F.col("close").isNotNull()
        )
        .select(
            F.col("ticker").alias("symbol"),
            F.col("date"),
            F.col("open"),
            F.col("high"),
            F.col("low"),
            F.col("close"),
            F.col("volume"),
            F.col("_ingested_at").alias("_bronze_ingested_at"),
            F.current_timestamp().alias("_silver_processed_at"),
        )
)

(prices_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_PRICES))

# COMMAND ----------

# MAGIC %sql
# MAGIC COMMENT ON TABLE workspace.silver.market_prices IS
# MAGIC   'Daily OHLCV per symbol, non-null on (symbol, date, close). Overwritten from bronze on each run.';
# MAGIC
# MAGIC ALTER TABLE workspace.silver.market_prices ALTER COLUMN symbol               COMMENT 'Trading symbol including exchange suffix, e.g. AAPL.US.';
# MAGIC ALTER TABLE workspace.silver.market_prices ALTER COLUMN date                 COMMENT 'Trading date.';
# MAGIC ALTER TABLE workspace.silver.market_prices ALTER COLUMN open                 COMMENT 'Opening price.';
# MAGIC ALTER TABLE workspace.silver.market_prices ALTER COLUMN high                 COMMENT 'Intraday high.';
# MAGIC ALTER TABLE workspace.silver.market_prices ALTER COLUMN low                  COMMENT 'Intraday low.';
# MAGIC ALTER TABLE workspace.silver.market_prices ALTER COLUMN close                COMMENT 'Closing price; used as mark for daily P&L in gold.';
# MAGIC ALTER TABLE workspace.silver.market_prices ALTER COLUMN volume               COMMENT 'Daily traded share count.';
# MAGIC ALTER TABLE workspace.silver.market_prices ALTER COLUMN _bronze_ingested_at  COMMENT 'Carried from bronze: when this row was MERGE-loaded.';
# MAGIC ALTER TABLE workspace.silver.market_prices ALTER COLUMN _silver_processed_at COMMENT 'When the silver overwrite that produced this row ran.';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Trades

# COMMAND ----------

# Authoritative symbol set for referential integrity. Computed once and
# inlined as a Python list because the catalogue is small (~7 tickers);
# isin() against a literal list is cheaper than a broadcast join here.
known_symbols = [
    row["ticker"]
    for row in spark.table(BRONZE_PRICES).select("ticker").distinct().collect()
    if row["ticker"]
]

if not known_symbols:
    raise RuntimeError(
        f"No tickers in {BRONZE_PRICES}; cannot run RI checks. "
        "Run 01_bronze before this notebook."
    )

# COMMAND ----------

# Cast the string-typed bronze columns alongside the originals so cast
# failures (returning null via try_cast) can be distinguished from genuinely
# null inputs in the rejection-reason logic below. ANSI SQL is on by default
# on serverless, so plain cast() would throw on malformed values such as
# 'abc' or '$50' instead of returning null.
typed = (
    spark.table(BRONZE_TRADES)
        .withColumn("qty_cast",   F.expr("try_cast(qty AS LONG)"))
        .withColumn("price_cast", F.expr("try_cast(price AS DOUBLE)"))
        .withColumn("ts_cast",    F.expr("try_to_timestamp(trade_ts)"))
)

# One flag column per rule. Each evaluates to the rejection reason string
# when the rule fires and null otherwise. concat_ws skips nulls when joining,
# so rows that pass every rule end up with rejection_reason == ''.
flagged = (
    typed
        .withColumn("flag_id",
            F.when((F.col("trade_id").isNull()) | (F.col("trade_id") == ""),
                   F.lit("null_or_empty_trade_id")))
        .withColumn("flag_version",
            F.when(F.col("version").isNull(),
                   F.lit("null_version")))
        .withColumn("flag_qty_cast",
            F.when(F.col("qty").isNotNull() & F.col("qty_cast").isNull(),
                   F.lit("non_numeric_qty")))
        .withColumn("flag_price_cast",
            F.when(F.col("price").isNotNull() & F.col("price_cast").isNull(),
                   F.lit("non_numeric_price")))
        .withColumn("flag_ts_cast",
            F.when(F.col("trade_ts").isNotNull() & F.col("ts_cast").isNull(),
                   F.lit("non_parseable_trade_ts")))
        .withColumn("flag_qty_range",
            F.when(F.col("qty_cast") <= 0,
                   F.lit("non_positive_qty")))
        .withColumn("flag_price_range",
            F.when(F.col("price_cast") <= 0,
                   F.lit("non_positive_price")))
        .withColumn("flag_side",
            F.when(F.col("side").isNull() | ~F.col("side").isin("BUY", "SELL"),
                   F.lit("invalid_side")))
        .withColumn("flag_symbol",
            F.when(F.col("symbol").isNull() | ~F.col("symbol").isin(*known_symbols),
                   F.lit("unknown_symbol")))
        .withColumn("rejection_reason",
            F.concat_ws("; ",
                F.col("flag_id"),
                F.col("flag_version"),
                F.col("flag_qty_cast"),
                F.col("flag_price_cast"),
                F.col("flag_ts_cast"),
                F.col("flag_qty_range"),
                F.col("flag_price_range"),
                F.col("flag_side"),
                F.col("flag_symbol"),
            ))
)

# COMMAND ----------

# Rows that fail any rule go to quarantine with the original raw strings
# preserved so analysts can see exactly what came in.
quarantine = (
    flagged.filter(F.col("rejection_reason") != "")
        .select(
            F.col("trade_id"),
            F.col("version"),
            F.col("symbol"),
            F.col("side"),
            F.col("qty"),
            F.col("price"),
            F.col("trade_ts"),
            F.col("book"),
            F.col("_ingested_at").alias("_bronze_ingested_at"),
            F.col("rejection_reason"),
            F.current_timestamp().alias("_quarantine_ts"),
        )
)

(quarantine.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_QUARANTINE))

# COMMAND ----------

# Latest-version-wins per trade_id. _ingested_at breaks ties when two rows
# share both trade_id and version (shouldn't happen post bronze MERGE, but
# the tiebreaker keeps the window deterministic).
window_latest = Window.partitionBy("trade_id").orderBy(
    F.col("version").desc(),
    F.col("_ingested_at").desc(),
)

clean_candidate = flagged.filter(F.col("rejection_reason") == "")

clean = (
    clean_candidate
        .withColumn("_rn", F.row_number().over(window_latest))
        .filter(F.col("_rn") == 1)
        .select(
            F.col("trade_id"),
            F.col("version"),
            F.col("symbol"),
            F.col("side"),
            F.col("qty_cast").alias("qty"),
            F.col("price_cast").alias("price"),
            F.col("ts_cast").alias("trade_ts"),
            F.to_date("ts_cast").alias("trade_date"),
            F.col("book"),
            F.col("_ingested_at").alias("_bronze_ingested_at"),
            F.current_timestamp().alias("_silver_processed_at"),
        )
)

(clean.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_CLEAN))

# COMMAND ----------

# MAGIC %sql
# MAGIC COMMENT ON TABLE workspace.silver.trades_clean IS
# MAGIC   'Validated trades, latest version per trade_id only. Cast types enforced; bad rows in trades_quarantine.';
# MAGIC
# MAGIC ALTER TABLE workspace.silver.trades_clean ALTER COLUMN trade_id             COMMENT 'Trade identifier; non-null after silver.';
# MAGIC ALTER TABLE workspace.silver.trades_clean ALTER COLUMN version              COMMENT 'Latest version surviving dedup; max(version) per trade_id.';
# MAGIC ALTER TABLE workspace.silver.trades_clean ALTER COLUMN symbol               COMMENT 'Trading symbol; guaranteed to exist in silver.market_prices.';
# MAGIC ALTER TABLE workspace.silver.trades_clean ALTER COLUMN side                 COMMENT 'BUY or SELL.';
# MAGIC ALTER TABLE workspace.silver.trades_clean ALTER COLUMN qty                  COMMENT 'Trade quantity, positive long.';
# MAGIC ALTER TABLE workspace.silver.trades_clean ALTER COLUMN price                COMMENT 'Trade price, positive double.';
# MAGIC ALTER TABLE workspace.silver.trades_clean ALTER COLUMN trade_ts             COMMENT 'Trade timestamp, parsed from bronze string.';
# MAGIC ALTER TABLE workspace.silver.trades_clean ALTER COLUMN trade_date           COMMENT 'Date portion of trade_ts; gold joins to market_prices on (symbol, trade_date).';
# MAGIC ALTER TABLE workspace.silver.trades_clean ALTER COLUMN book                 COMMENT 'Trading book the trade is allocated to.';
# MAGIC ALTER TABLE workspace.silver.trades_clean ALTER COLUMN _bronze_ingested_at  COMMENT 'Carried from bronze: when this row was MERGE-loaded.';
# MAGIC ALTER TABLE workspace.silver.trades_clean ALTER COLUMN _silver_processed_at COMMENT 'When the silver overwrite that produced this row ran.';

# COMMAND ----------

# MAGIC %sql
# MAGIC COMMENT ON TABLE workspace.silver.trades_quarantine IS
# MAGIC   'Trades rejected by silver DQ rules. Raw bronze strings preserved; rejection_reason holds all violations.';
# MAGIC
# MAGIC ALTER TABLE workspace.silver.trades_quarantine ALTER COLUMN trade_id             COMMENT 'Trade identifier as it arrived; possibly null or empty.';
# MAGIC ALTER TABLE workspace.silver.trades_quarantine ALTER COLUMN version              COMMENT 'Version as it arrived; possibly null.';
# MAGIC ALTER TABLE workspace.silver.trades_quarantine ALTER COLUMN symbol               COMMENT 'Trading symbol as quoted; may not exist in market_prices.';
# MAGIC ALTER TABLE workspace.silver.trades_quarantine ALTER COLUMN side                 COMMENT 'Trade side as it arrived; may not be BUY or SELL.';
# MAGIC ALTER TABLE workspace.silver.trades_quarantine ALTER COLUMN qty                  COMMENT 'Raw quantity string; may be non-numeric or non-positive.';
# MAGIC ALTER TABLE workspace.silver.trades_quarantine ALTER COLUMN price                COMMENT 'Raw price string; may be non-numeric or non-positive.';
# MAGIC ALTER TABLE workspace.silver.trades_quarantine ALTER COLUMN trade_ts             COMMENT 'Raw timestamp string; may not parse.';
# MAGIC ALTER TABLE workspace.silver.trades_quarantine ALTER COLUMN book                 COMMENT 'Trading book the trade is allocated to.';
# MAGIC ALTER TABLE workspace.silver.trades_quarantine ALTER COLUMN _bronze_ingested_at  COMMENT 'Carried from bronze: when this row was MERGE-loaded.';
# MAGIC ALTER TABLE workspace.silver.trades_quarantine ALTER COLUMN rejection_reason     COMMENT 'Semicolon-separated list of every DQ rule this row failed.';
# MAGIC ALTER TABLE workspace.silver.trades_quarantine ALTER COLUMN _quarantine_ts       COMMENT 'When the silver run that quarantined this row finished.';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run metrics

# COMMAND ----------

# Counts are computed once here. Task 4 will persist them to
# workspace.audit.pipeline_metrics; for now they render in the notebook.
prices_rows_in = spark.table(BRONZE_PRICES).count()
prices_rows_out = spark.table(SILVER_PRICES).count()

trades_rows_in = spark.table(BRONZE_TRADES).count()
trades_rows_quarantined = spark.table(SILVER_QUARANTINE).count()
trades_rows_pre_dedup = trades_rows_in - trades_rows_quarantined
trades_rows_out = spark.table(SILVER_CLEAN).count()
trades_rows_deduped = trades_rows_pre_dedup - trades_rows_out

metrics = spark.createDataFrame(
    [(
        prices_rows_in, prices_rows_out,
        trades_rows_in, trades_rows_quarantined,
        trades_rows_pre_dedup, trades_rows_deduped, trades_rows_out,
    )],
    schema=(
        "prices_rows_in LONG, prices_rows_out LONG, "
        "trades_rows_in LONG, trades_rows_quarantined LONG, "
        "trades_rows_pre_dedup LONG, trades_rows_deduped LONG, trades_rows_out LONG"
    ),
)
display(metrics)

# COMMAND ----------

display(spark.sql(f"""
    SELECT rejection_reason, COUNT(*) AS rows
    FROM {SILVER_QUARANTINE}
    GROUP BY rejection_reason
    ORDER BY rows DESC
"""))

# COMMAND ----------

# Metrics handed back to 04_job_runner. rejects is the silver quarantine
# count; duplicates is the latest-version-wins drop count.
import json

dbutils.notebook.exit(json.dumps({
    "task": "silver",
    "schema": "silver",
    "rows_in": int(prices_rows_in + trades_rows_in),
    "rows_out": int(prices_rows_out + trades_rows_out),
    "rejects": int(trades_rows_quarantined),
    "duplicates": int(trades_rows_deduped),
}))
