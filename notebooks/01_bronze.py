# Databricks notebook source
# MAGIC %md
# MAGIC ## Bronze: raw market prices and synthesised trades
# MAGIC
# MAGIC Reads CSVs landed in the UC volume by the local ingest, merges them into
# MAGIC the prices table, and synthesises a deliberately dirty trades table for
# MAGIC the Silver layer to clean and quarantine.

# COMMAND ----------

"""
Bronze layer ingest for the Stooq trading pipeline.

Inputs (consumed):
  /Volumes/workspace/bronze/stooq_raw_volume/*.csv  (landed by scripts/ingest_stooq_local.py)

Outputs (written):
  workspace.bronze.market_prices_raw  (typed; MERGE on (ticker, date))
  workspace.bronze.trades_raw         (mostly-string; MERGE on (trade_id, version))

The trades table is synthesised in-notebook with intentional null, duplicate,
malformed and out-of-range rows so the Silver layer's cast, dedup and
referential-integrity checks have realistic work to do.

Idempotent on re-run for both tables.
"""

# COMMAND ----------

import random
from datetime import datetime, timedelta

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

dbutils.widgets.text("catalog_name", "workspace")
dbutils.widgets.text("env", "dev")
dbutils.widgets.text("run_date", "")

CATALOG = dbutils.widgets.get("catalog_name")

VOLUME_PATH = f"/Volumes/{CATALOG}/bronze/stooq_raw_volume/"
PRICES_TABLE = f"{CATALOG}.bronze.market_prices_raw"
TRADES_TABLE = f"{CATALOG}.bronze.trades_raw"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Market prices

# COMMAND ----------

# Stooq CSV header is "Date,Open,High,Low,Close,Volume". Schema is enforced
# on read; malformed rows are nulled (default PERMISSIVE mode).
prices_csv_schema = StructType([
    StructField("Date", DateType(), True),
    StructField("Open", DoubleType(), True),
    StructField("High", DoubleType(), True),
    StructField("Low", DoubleType(), True),
    StructField("Close", DoubleType(), True),
    StructField("Volume", LongType(), True),
])

# Filename convention is fixed by the local ingest:
#   {TICKER_WITH_EXCHANGE_SUFFIX}_{YYYYMMDD}_{YYYYMMDD}.csv  e.g. AAPL.US_20260204_20260505.csv
ticker_pattern = r"([^/]+)_\d{8}_\d{8}\.csv$"

prices_in = (
    spark.read
        .option("header", "true")
        .schema(prices_csv_schema)
        .csv(VOLUME_PATH)
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("ticker", F.regexp_extract(F.col("_source_file"), ticker_pattern, 1))
        .withColumn("_ingested_at", F.current_timestamp())
        .select(
            F.col("ticker"),
            F.col("Date").alias("date"),
            F.col("Open").alias("open"),
            F.col("High").alias("high"),
            F.col("Low").alias("low"),
            F.col("Close").alias("close"),
            F.col("Volume").alias("volume"),
            F.col("_source_file"),
            F.col("_ingested_at"),
        )
)

# COMMAND ----------

# Create on first run, MERGE on subsequent runs so re-ingesting overlapping
# date windows updates in place rather than appending duplicates.
if not spark.catalog.tableExists(PRICES_TABLE):
    (prices_in.write
        .format("delta")
        .saveAsTable(PRICES_TABLE))
else:
    (DeltaTable.forName(spark, PRICES_TABLE).alias("t")
        .merge(
            prices_in.alias("s"),
            "t.ticker = s.ticker AND t.date = s.date",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())

# COMMAND ----------

# MAGIC %sql
# MAGIC COMMENT ON TABLE workspace.bronze.market_prices_raw IS
# MAGIC   'Daily OHLCV per ticker, MERGE-loaded from CSVs in stooq_raw_volume. Bronze: typed but unvalidated.';
# MAGIC
# MAGIC ALTER TABLE workspace.bronze.market_prices_raw ALTER COLUMN ticker       COMMENT 'Stooq ticker including exchange suffix, e.g. AAPL.US.';
# MAGIC ALTER TABLE workspace.bronze.market_prices_raw ALTER COLUMN date         COMMENT 'Trading date.';
# MAGIC ALTER TABLE workspace.bronze.market_prices_raw ALTER COLUMN open         COMMENT 'Opening price.';
# MAGIC ALTER TABLE workspace.bronze.market_prices_raw ALTER COLUMN high         COMMENT 'Intraday high.';
# MAGIC ALTER TABLE workspace.bronze.market_prices_raw ALTER COLUMN low          COMMENT 'Intraday low.';
# MAGIC ALTER TABLE workspace.bronze.market_prices_raw ALTER COLUMN close        COMMENT 'Closing price.';
# MAGIC ALTER TABLE workspace.bronze.market_prices_raw ALTER COLUMN volume       COMMENT 'Daily traded share count.';
# MAGIC ALTER TABLE workspace.bronze.market_prices_raw ALTER COLUMN _source_file COMMENT 'Originating CSV path in the volume.';
# MAGIC ALTER TABLE workspace.bronze.market_prices_raw ALTER COLUMN _ingested_at COMMENT 'Timestamp this row was MERGE-loaded.';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Trades (synthesised dirty data)

# COMMAND ----------

# Symbols are drawn from the prices DF rather than re-reading config/tickers.csv,
# which is a Mac-side artefact not present on the cluster. Distinct tickers
# already in bronze are the authoritative set for downstream RI checks.
known_tickers = [
    row["ticker"]
    for row in prices_in.select("ticker").distinct().collect()
    if row["ticker"]
]

if not known_tickers:
    raise RuntimeError(
        f"No tickers parsed from {VOLUME_PATH}. "
        "Run scripts/ingest_stooq_local.py before this notebook."
    )

prices_window = (
    spark.table(PRICES_TABLE)
        .agg(F.min("date").alias("min_d"), F.max("date").alias("max_d"))
        .first()
)
window_start = prices_window["min_d"]
window_end = prices_window["max_d"]

# COMMAND ----------

# Synthesise 200 rows. Seeded so re-runs produce the same dirty distribution;
# duplicates are appended at the end and collapsed by MERGE on insert.
random.seed(42)

UNKNOWN_SYMBOL = "ZZZZ.US"
SIDES_VALID = ["BUY", "SELL"]
BOOKS = ["EQUITY-A", "EQUITY-B", "MACRO", "PROP"]

n_unique = 180
n_dup_appends = 20
window_days = (window_end - window_start).days + 1


def _synth_row(i: int) -> tuple:
    trade_id = f"T{i:05d}"
    if random.random() < 0.10:
        trade_id = None
    elif random.random() < 0.05:
        trade_id = ""

    # Most trades are v1; ~10% of valid ids get a v2, a few a v3.
    version = 1
    if trade_id and random.random() < 0.10:
        version = 2 if random.random() > 0.20 else 3
    if random.random() < 0.05:
        version = None

    symbol = UNKNOWN_SYMBOL if random.random() < 0.05 else random.choice(known_tickers)
    side = random.choice(SIDES_VALID + [None, "garbage"])
    book = random.choice(BOOKS)

    r = random.random()
    if r < 0.05:
        qty = random.choice(["abc", "ten", " "])
    elif r < 0.10:
        qty = str(random.choice([0, -50, -1]))
    else:
        qty = str(random.randint(1, 5000))

    r = random.random()
    if r < 0.05:
        price = random.choice(["$50", "n/a", "--"])
    elif r < 0.10:
        price = f"{random.uniform(-100, 0):.2f}"
    else:
        price = f"{random.uniform(50, 500):.2f}"

    if random.random() < 0.05:
        trade_ts = random.choice(["not-a-date", "2026-13-40", ""])
    else:
        offset = random.randint(0, max(window_days - 1, 0))
        ts = datetime.combine(window_start, datetime.min.time()) + timedelta(
            days=offset,
            hours=random.randint(9, 16),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59),
        )
        trade_ts = ts.strftime("%Y-%m-%d %H:%M:%S")

    return (trade_id, version, symbol, side, qty, price, trade_ts, book)


rows = [_synth_row(i) for i in range(n_unique)]

# Inject corrections so silver's latest-version-wins dedup has work to do.
# A correction is a same-trade_id row with a higher version number and
# mutated qty/price; symbol, side and book carry over from v1.
def _correction_of(base: tuple, version: int) -> tuple:
    trade_id, _v, symbol, side, _qty, _price, trade_ts, book = base
    return (
        trade_id,
        version,
        symbol,
        side,
        str(random.randint(1, 5000)),
        f"{random.uniform(50, 500):.2f}",
        trade_ts,
        book,
    )

v1_correctable = [r for r in rows if r[0] not in (None, "") and r[1] == 1]
v2_targets = random.sample(v1_correctable, k=min(15, len(v1_correctable)))
rows.extend(_correction_of(r, 2) for r in v2_targets)
v3_targets = random.sample(v2_targets, k=min(3, len(v2_targets)))
rows.extend(_correction_of(r, 3) for r in v3_targets)

# Append duplicates of existing rows so the in-memory set has true (id, version)
# duplicates. The pre-MERGE dedup below collapses them to one row in bronze.
dup_candidates = [r for r in rows if r[0] not in (None, "")]
rows.extend(random.choice(dup_candidates) for _ in range(n_dup_appends))

trades_schema = StructType([
    StructField("trade_id", StringType(), True),
    StructField("version",  IntegerType(), True),
    StructField("symbol",   StringType(), True),
    StructField("side",     StringType(), True),
    StructField("qty",      StringType(), True),
    StructField("price",    StringType(), True),
    StructField("trade_ts", StringType(), True),
    StructField("book",     StringType(), True),
])

# Dedup on the merge keys (null-safe in dropDuplicates) so MERGE never sees
# multiple source matches for a single target row. Bronze keeps the row but
# attribution to "raw" is preserved by _ingested_at.
trades_in = (
    spark.createDataFrame(rows, schema=trades_schema)
        .dropDuplicates(["trade_id", "version"])
        .withColumn("_ingested_at", F.current_timestamp())
        .coalesce(1)
)

# COMMAND ----------

# Null-safe equality (<=>) keeps re-runs idempotent for rows whose trade_id
# or version is null: standard "=" treats NULL != NULL and would re-insert
# them on every run.
if not spark.catalog.tableExists(TRADES_TABLE):
    (trades_in.write
        .format("delta")
        .saveAsTable(TRADES_TABLE))
else:
    (DeltaTable.forName(spark, TRADES_TABLE).alias("t")
        .merge(
            trades_in.alias("s"),
            "t.trade_id <=> s.trade_id AND t.version <=> s.version",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())

# COMMAND ----------

# MAGIC %sql
# MAGIC COMMENT ON TABLE workspace.bronze.trades_raw IS
# MAGIC   'Synthesised dirty trades for the silver pipeline to clean. String columns hold raw, possibly invalid values.';
# MAGIC
# MAGIC ALTER TABLE workspace.bronze.trades_raw ALTER COLUMN trade_id     COMMENT 'Trade identifier; nullable in raw form.';
# MAGIC ALTER TABLE workspace.bronze.trades_raw ALTER COLUMN version      COMMENT 'Monotonic version per trade_id; latest-wins in silver.';
# MAGIC ALTER TABLE workspace.bronze.trades_raw ALTER COLUMN symbol       COMMENT 'Ticker as quoted on the trade ticket; checked against bronze prices in silver.';
# MAGIC ALTER TABLE workspace.bronze.trades_raw ALTER COLUMN side         COMMENT 'BUY or SELL; other values quarantined.';
# MAGIC ALTER TABLE workspace.bronze.trades_raw ALTER COLUMN qty          COMMENT 'Trade quantity, raw string. Cast and range-checked in silver.';
# MAGIC ALTER TABLE workspace.bronze.trades_raw ALTER COLUMN price        COMMENT 'Trade price, raw string. Cast and range-checked in silver.';
# MAGIC ALTER TABLE workspace.bronze.trades_raw ALTER COLUMN trade_ts     COMMENT 'Trade timestamp, raw string. Cast in silver.';
# MAGIC ALTER TABLE workspace.bronze.trades_raw ALTER COLUMN book         COMMENT 'Trading book the trade is allocated to.';
# MAGIC ALTER TABLE workspace.bronze.trades_raw ALTER COLUMN _ingested_at COMMENT 'Timestamp this row was MERGE-loaded.';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation

# COMMAND ----------

display(spark.sql(f"""
    SELECT ticker, COUNT(*) AS rows, MIN(date) AS min_date, MAX(date) AS max_date
    FROM {PRICES_TABLE}
    GROUP BY ticker
    ORDER BY ticker
"""))

# COMMAND ----------

display(spark.sql(f"""
    SELECT
        COUNT(*)                                                                AS total,
        SUM(CASE WHEN trade_id IS NULL OR trade_id = '' THEN 1 ELSE 0 END)      AS bad_trade_id,
        SUM(CASE WHEN version  IS NULL                  THEN 1 ELSE 0 END)      AS null_version,
        SUM(CASE WHEN qty   NOT RLIKE '^-?[0-9]+$'      THEN 1 ELSE 0 END)      AS non_numeric_qty,
        SUM(CASE WHEN price NOT RLIKE '^-?[0-9]+(\\\\.[0-9]+)?$' THEN 1 ELSE 0 END) AS non_numeric_price,
        SUM(CASE WHEN symbol = 'ZZZZ.US'                THEN 1 ELSE 0 END)      AS unknown_symbol_rows,
        COUNT(DISTINCT trade_id)                                                AS distinct_trade_ids
    FROM {TRADES_TABLE}
"""))

# COMMAND ----------

# Metrics handed back to 04_job_runner. duplicates is rows_in - rows_out:
# bronze prices MERGE collapses re-ingested (ticker, date) keys, and the
# trades synth dedupes on (trade_id, version) before the MERGE.
import json

bronze_csv_rows   = prices_in.count()
bronze_synth_rows = len(rows)
prices_out_rows   = spark.table(PRICES_TABLE).count()
trades_out_rows   = spark.table(TRADES_TABLE).count()

_rows_in  = bronze_csv_rows + bronze_synth_rows
_rows_out = prices_out_rows + trades_out_rows

dbutils.notebook.exit(json.dumps({
    "task": "bronze",
    "schema": "bronze",
    "rows_in": _rows_in,
    "rows_out": _rows_out,
    "rejects": 0,
    "duplicates": max(_rows_in - _rows_out, 0),
}))
