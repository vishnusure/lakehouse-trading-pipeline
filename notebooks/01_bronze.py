# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # 01 — Bronze Layer: Raw Ingestion
# MAGIC
# MAGIC Fetches daily OHLCV prices from Stooq (7 tickers, last 90 days) and synthesises
# MAGIC dirty trades in-memory. Both tables land in Unity Catalog as managed Delta tables
# MAGIC via idempotent MERGE — safe to re-run at any time.
# MAGIC
# MAGIC **Outputs**
# MAGIC - `{catalog}.bronze.market_prices_raw` — MERGE key: `(ticker, trade_date)`
# MAGIC - `{catalog}.bronze.trades_raw`        — MERGE key: `(trade_id, version)`

# COMMAND ----------

dbutils.widgets.text("run_date",     "",          "Run Date (YYYY-MM-DD, blank = today)")
dbutils.widgets.text("env",          "prod",      "Environment")
dbutils.widgets.text("catalog_name", "workspace", "Unity Catalog name")

# COMMAND ----------

import datetime, json, random, string, time

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, LongType, DateType, TimestampType,
)

catalog    = dbutils.widgets.get("catalog_name")
env        = dbutils.widgets.get("env")
_run_date  = dbutils.widgets.get("run_date").strip()
run_date   = datetime.date.fromisoformat(_run_date) if _run_date else datetime.date.today()
start_date = run_date - datetime.timedelta(days=90)

print(f"catalog={catalog}  env={env}  run_date={run_date}  window={start_date} → {run_date}")

# COMMAND ----------
# ── Schema bootstrap (all 4 layers created here once) ─────────────────────────

for _s in ["bronze", "silver", "gold", "audit"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{_s}")
    print(f"Schema ready: {catalog}.{_s}")

# COMMAND ----------
# ── Stooq OHLCV price ingestion ───────────────────────────────────────────────

TICKERS   = ["AAPL", "MSFT", "GOOG", "AMZN", "TSLA", "JPM", "NVDA"]
START_STR = start_date.strftime("%Y%m%d")
END_STR   = run_date.strftime("%Y%m%d")

# Staging schema: volume kept as Double to survive Stooq's occasional float output;
# the target DDL declares it LONG — Delta MERGE performs the implicit numeric cast.
PRICES_STAGE_SCHEMA = StructType([
    StructField("ticker",      StringType(),  False),
    StructField("trade_date",  DateType(),    True),
    StructField("open_price",  DoubleType(),  True),
    StructField("high_price",  DoubleType(),  True),
    StructField("low_price",   DoubleType(),  True),
    StructField("close_price", DoubleType(),  True),
    StructField("volume",      DoubleType(),  True),
    StructField("_source",     StringType(),  False),
])

price_frames = []
for ticker in TICKERS:
    url = (
        f"https://stooq.com/q/d/l/"
        f"?s={ticker.lower()}.us&i=d&d1={START_STR}&d2={END_STR}"
        f"&apikey=l8FmVeftQxDpkSlOZHNo7abwY05GjX1y"
    )
    try:
        pdf = pd.read_csv(url)
        if pdf.empty or "Date" not in pdf.columns:
            print(f"[WARN] {ticker}: unexpected/empty response — skipping")
        else:
            pdf = pdf.rename(columns={
                "Date": "trade_date", "Open": "open_price",
                "High": "high_price", "Low": "low_price",
                "Close": "close_price", "Volume": "volume",
            })
            pdf["ticker"]     = ticker
            pdf["_source"]    = "stooq"
            pdf["trade_date"] = pd.to_datetime(pdf["trade_date"]).dt.date
            pdf["volume"]     = pd.to_numeric(pdf["volume"], errors="coerce")
            price_frames.append(
                pdf[["ticker","trade_date","open_price","high_price",
                      "low_price","close_price","volume","_source"]]
            )
            print(f"[OK] {ticker}: {len(pdf)} rows")
    except Exception as ex:
        print(f"[WARN] {ticker}: fetch error ({ex}) — skipping")
    time.sleep(2)

if not price_frames:
    raise RuntimeError("No price data retrieved from Stooq — check network connectivity")

prices_pdf = pd.concat(price_frames, ignore_index=True)

prices_sdf = (
    spark.createDataFrame(prices_pdf, schema=PRICES_STAGE_SCHEMA)
         .withColumn("_ingest_ts", F.current_timestamp())
         .coalesce(1)
)
prices_count_in = prices_sdf.count()
print(f"Total price rows staged: {prices_count_in}")

# COMMAND ----------
# ── Create market_prices_raw (if first run) then idempotent MERGE ─────────────

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.bronze.market_prices_raw (
    ticker       STRING    NOT NULL COMMENT 'Stock ticker symbol (e.g. AAPL)',
    trade_date   DATE      NOT NULL COMMENT 'Trading date sourced from Stooq',
    open_price   DOUBLE             COMMENT 'Opening price in USD',
    high_price   DOUBLE             COMMENT 'Intraday high price in USD',
    low_price    DOUBLE             COMMENT 'Intraday low price in USD',
    close_price  DOUBLE             COMMENT 'Closing price in USD',
    volume       LONG               COMMENT 'Number of shares traded on this day',
    _source      STRING             COMMENT 'Source system identifier (stooq)',
    _ingest_ts   TIMESTAMP          COMMENT 'UTC timestamp when this row was ingested'
)
USING DELTA
COMMENT 'Bronze layer: raw daily OHLCV prices fetched from Stooq one ticker at a time. Idempotent via MERGE on (ticker, trade_date). No transformations applied.'
CLUSTER BY (ticker, trade_date)
""")

prices_sdf.createOrReplaceTempView("_prices_stage")

spark.sql(f"""
MERGE INTO {catalog}.bronze.market_prices_raw AS t
USING _prices_stage AS s
  ON t.ticker = s.ticker AND t.trade_date = s.trade_date
WHEN MATCHED THEN
    UPDATE SET *
WHEN NOT MATCHED THEN
    INSERT *
""")

prices_count_out = spark.table(f"{catalog}.bronze.market_prices_raw").count()
print(f"market_prices_raw — total rows after MERGE: {prices_count_out}")

# COMMAND ----------
# ── Synthetic dirty trades (in-memory, no external call) ──────────────────────
#
# Dirt breakdown (intentional, for Silver quarantine testing):
#   160 clean rows  (version=1)
#    20 duplicates  (same trade_id, version=2, price shifted ±2 %)
#    15 null price  (new trade_ids, version=1, price=NULL)
#    10 null qty    (new trade_ids, version=1, quantity=NULL)
#     5 bad type    (new trade_ids, version=1, price="N/A")
#   ─────────────────
#   210 total rows staged

random.seed(42)

def _rid(prefix="TRD-", n=6):
    return prefix + "".join(random.choices(string.digits, k=n))

SYMBOLS        = ["AAPL", "MSFT", "GOOG", "AMZN", "TSLA", "JPM", "NVDA"]
BOOKS          = ["EQUITY-US", "EQUITY-EU", "FIXED-INCOME", "DERIVATIVES"]
COUNTERPARTIES = ["GS", "MS", "BARC", "DB", "CITI", "JPM", "UBS"]
TRADERS        = [_rid("T", 4) for _ in range(10)]
_BASE          = run_date - datetime.timedelta(days=30)

def _rdate():
    return (_BASE + datetime.timedelta(days=random.randint(0, 29))).isoformat()

clean_rows = [
    {
        "trade_id":    _rid(),
        "version":     1,
        "symbol":      random.choice(SYMBOLS),
        "book":        random.choice(BOOKS),
        "trade_date":  _rdate(),
        "quantity":    str(random.randint(100, 10_000)),
        "price":       str(round(random.uniform(10.0, 800.0), 2)),
        "counterparty":random.choice(COUNTERPARTIES),
        "trader_id":   random.choice(TRADERS),
        "_source":     "synthetic",
    }
    for _ in range(160)
]

dup_rows = [
    {**r, "version": 2, "price": str(round(float(r["price"]) * random.uniform(0.98, 1.02), 2))}
    for r in random.sample(clean_rows, 20)
]

null_price_rows = [
    {**r, "trade_id": _rid(), "version": 1, "price": None}
    for r in random.sample(clean_rows, 15)
]

null_qty_rows = [
    {**r, "trade_id": _rid(), "version": 1, "quantity": None}
    for r in random.sample(clean_rows, 10)
]

bad_type_rows = [
    {**r, "trade_id": _rid(), "version": 1, "price": "N/A"}
    for r in random.sample(clean_rows, 5)
]

all_trades = clean_rows + dup_rows + null_price_rows + null_qty_rows + bad_type_rows
print(f"Synthetic dirty trades synthesised: {len(all_trades)} rows")

# trade_date kept as STRING in this schema; cast to DATE via withColumn below
TRADES_STAGE_SCHEMA = StructType([
    StructField("trade_id",     StringType(),  False),
    StructField("version",      IntegerType(), False),
    StructField("symbol",       StringType(),  True),
    StructField("book",         StringType(),  True),
    StructField("trade_date",   StringType(),  True),
    StructField("quantity",     StringType(),  True),
    StructField("price",        StringType(),  True),
    StructField("counterparty", StringType(),  True),
    StructField("trader_id",    StringType(),  True),
    StructField("_source",      StringType(),  False),
])

trades_sdf = (
    spark.createDataFrame(pd.DataFrame(all_trades), schema=TRADES_STAGE_SCHEMA)
         .withColumn("trade_date", F.to_date("trade_date"))
         .withColumn("_ingest_ts", F.current_timestamp())
         .coalesce(1)
)
trades_count_in = trades_sdf.count()
print(f"Trades staged in Spark: {trades_count_in}")

# COMMAND ----------
# ── Create trades_raw (if first run) then idempotent MERGE ────────────────────

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.bronze.trades_raw (
    trade_id      STRING    NOT NULL COMMENT 'Unique trade identifier',
    version       INT       NOT NULL COMMENT 'Record version — higher version supersedes lower for the same trade_id',
    symbol        STRING             COMMENT 'Instrument symbol (may contain dirty values)',
    book          STRING             COMMENT 'Trading book (e.g. EQUITY-US, FIXED-INCOME, DERIVATIVES)',
    trade_date    DATE               COMMENT 'Trade execution date',
    quantity      STRING             COMMENT 'Raw quantity kept as STRING to preserve dirty values for Silver quarantine',
    price         STRING             COMMENT 'Raw price kept as STRING to preserve dirty values (e.g. NULL, N/A) for Silver quarantine',
    counterparty  STRING             COMMENT 'Counterparty short name (e.g. GS, MS, BARC)',
    trader_id     STRING             COMMENT 'Trader identifier',
    _source       STRING             COMMENT 'Source system identifier (synthetic)',
    _ingest_ts    TIMESTAMP          COMMENT 'UTC timestamp when this row was ingested'
)
USING DELTA
COMMENT 'Bronze layer: raw synthetic trades with intentional dirt — nulls in price/quantity, version duplicates, bad type strings. Idempotent MERGE on (trade_id, version). Silver applies DQ and quarantines bad rows.'
CLUSTER BY (trade_id)
""")

trades_sdf.createOrReplaceTempView("_trades_stage")

spark.sql(f"""
MERGE INTO {catalog}.bronze.trades_raw AS t
USING _trades_stage AS s
  ON t.trade_id = s.trade_id AND t.version = s.version
WHEN MATCHED THEN
    UPDATE SET *
WHEN NOT MATCHED THEN
    INSERT *
""")

trades_count_out = spark.table(f"{catalog}.bronze.trades_raw").count()
print(f"trades_raw — total rows after MERGE: {trades_count_out}")

# COMMAND ----------
# ── Conditional OPTIMIZE (triggers every 50 bronze runs) ──────────────────────
# audit.pipeline_metrics is created by 04_job_runner; guard with try/except on
# first run before that table exists.

try:
    bronze_runs = spark.sql(
        f"SELECT COUNT(*) AS n FROM {catalog}.audit.pipeline_metrics WHERE task = 'bronze'"
    ).collect()[0]["n"]
except Exception:
    bronze_runs = 0

if bronze_runs > 0 and bronze_runs % 50 == 0:
    print(f"Run #{bronze_runs}: triggering OPTIMIZE on bronze tables")
    spark.sql(f"OPTIMIZE {catalog}.bronze.market_prices_raw")
    spark.sql(f"OPTIMIZE {catalog}.bronze.trades_raw")
else:
    print(f"Skipping OPTIMIZE — run #{bronze_runs + 1} (fires at multiples of 50)")

# COMMAND ----------
# ── Metrics payload (returned to 04_job_runner via dbutils.notebook.exit) ─────

metrics = {
    "task":           "bronze",
    "rows_in":        prices_count_in + trades_count_in,
    "rows_out":       prices_count_out + trades_count_out,
    "rows_in_prices": prices_count_in,
    "rows_out_prices":prices_count_out,
    "rows_in_trades": trades_count_in,
    "rows_out_trades":trades_count_out,
    "rejects":        0,
    "duplicates":     len(dup_rows),
    "run_ts":         datetime.datetime.utcnow().isoformat(),
    "catalog":        catalog,
    "schema":         "bronze",
}
print(json.dumps(metrics, indent=2))
dbutils.notebook.exit(json.dumps(metrics))
