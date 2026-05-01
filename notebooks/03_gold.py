# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # 03 — Gold Layer: Business Outputs
# MAGIC
# MAGIC Builds two business-ready tables from Silver:
# MAGIC
# MAGIC - **`gold.positions`** — net quantity and weighted-average price per (book, symbol)
# MAGIC - **`gold.daily_pnl`** — unrealized P&L per trade, marked at the last available
# MAGIC   close price on or before the trade date (as-of join via LATERAL subquery —
# MAGIC   no Spark 3.4+ `asof_join` API, Free Edition compatible)
# MAGIC
# MAGIC **Inputs**
# MAGIC - `{catalog}.silver.trades_clean`
# MAGIC - `{catalog}.silver.market_prices`

# COMMAND ----------

dbutils.widgets.text("run_date",     "",          "Run Date (YYYY-MM-DD, blank = today)")
dbutils.widgets.text("env",          "prod",      "Environment")
dbutils.widgets.text("catalog_name", "workspace", "Unity Catalog name")

# COMMAND ----------

import datetime, json

from pyspark.sql import functions as F

catalog   = dbutils.widgets.get("catalog_name")
env       = dbutils.widgets.get("env")
_run_date = dbutils.widgets.get("run_date").strip()
run_date  = datetime.date.fromisoformat(_run_date) if _run_date else datetime.date.today()

print(f"catalog={catalog}  env={env}  run_date={run_date}")

# COMMAND ----------
# ── Gold positions ─────────────────────────────────────────────────────────────
# Net position per (book, symbol): total quantity and quantity-weighted avg price.
# Full recompute each run (small table); MERGE on (book, symbol) for idempotency.

trades = spark.table(f"{catalog}.silver.trades_clean")
trades_rows_in = trades.count()
print(f"Silver trades_clean rows read: {trades_rows_in}")

positions_df = (
    trades
    .groupBy("book", "symbol")
    .agg(
        F.sum("quantity").alias("total_qty"),
        (F.sum(F.col("price") * F.col("quantity")) / F.sum("quantity")).alias("avg_price"),
    )
    .withColumn("_gold_ts", F.current_timestamp())
    .coalesce(1)
)

positions_rows = positions_df.count()
print(f"Positions computed: {positions_rows} book/symbol combinations")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.positions (
    book       STRING    NOT NULL COMMENT 'Trading book (e.g. EQUITY-US, FIXED-INCOME)',
    symbol     STRING    NOT NULL COMMENT 'Instrument symbol (e.g. AAPL)',
    total_qty  DOUBLE             COMMENT 'Net quantity held in this book/symbol position',
    avg_price  DOUBLE             COMMENT 'Quantity-weighted average entry price in USD',
    _gold_ts   TIMESTAMP          COMMENT 'UTC timestamp when this Gold row was last computed'
)
USING DELTA
COMMENT 'Gold layer: current positions — net quantity and weighted-average entry price per (book, symbol) aggregated from silver.trades_clean.'
CLUSTER BY (book, symbol)
""")

positions_df.createOrReplaceTempView("_positions_stage")

spark.sql(f"""
MERGE INTO {catalog}.gold.positions AS t
USING _positions_stage AS s
  ON t.book = s.book AND t.symbol = s.symbol
WHEN MATCHED THEN
    UPDATE SET *
WHEN NOT MATCHED THEN
    INSERT *
""")

positions_total = spark.table(f"{catalog}.gold.positions").count()
print(f"gold.positions total rows after MERGE: {positions_total}")

# COMMAND ----------
# ── Gold daily P&L ─────────────────────────────────────────────────────────────
# For each trade, mark unrealized P&L using the last available close price on or
# before the trade date.
#
# As-of join strategy (Free Edition compatible, no Spark 3.4+ asof_join API):
#   LATERAL subquery selects the most recent price row where:
#     prices.ticker = trades.symbol AND prices.trade_date <= trades.trade_date
#   ordered by price date DESC, LIMIT 1.
#
# LEFT JOIN so trades with no price data at all still appear (close_price=NULL,
# unrealized_pnl=NULL) rather than being silently dropped.

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.gold.daily_pnl (
    trade_id        STRING    NOT NULL COMMENT 'Unique trade identifier from silver.trades_clean',
    symbol          STRING             COMMENT 'Instrument symbol',
    book            STRING             COMMENT 'Trading book',
    trade_date      DATE               COMMENT 'Trade execution date',
    quantity        DOUBLE             COMMENT 'Trade quantity in shares',
    trade_price     DOUBLE             COMMENT 'Execution price in USD (from silver.trades_clean)',
    close_price     DOUBLE             COMMENT 'Last available close price on or before trade_date (from silver.market_prices)',
    unrealized_pnl  DOUBLE             COMMENT 'Unrealized P&L in USD: (close_price - trade_price) * quantity',
    _gold_ts        TIMESTAMP          COMMENT 'UTC timestamp when this Gold row was last computed'
)
USING DELTA
COMMENT 'Gold layer: unrealized P&L per trade, marked at the last available close price on or before the trade date. As-of join implemented via LATERAL subquery for Free Edition compatibility.'
CLUSTER BY (symbol, trade_date)
""")

# Register the Silver tables as views so they can be referenced in the LATERAL subquery
spark.table(f"{catalog}.silver.trades_clean").createOrReplaceTempView("_gold_trades")
spark.table(f"{catalog}.silver.market_prices").createOrReplaceTempView("_gold_prices")

daily_pnl_df = spark.sql(f"""
    SELECT
        t.trade_id,
        t.symbol,
        t.book,
        t.trade_date,
        t.quantity,
        t.price                                       AS trade_price,
        p.close_price,
        (p.close_price - t.price) * t.quantity        AS unrealized_pnl,
        current_timestamp()                            AS _gold_ts
    FROM _gold_trades t
    LEFT JOIN LATERAL (
        SELECT close_price
        FROM   _gold_prices
        WHERE  ticker     = t.symbol
          AND  trade_date <= t.trade_date
        ORDER BY trade_date DESC
        LIMIT 1
    ) p ON TRUE
""").coalesce(1)

pnl_rows_staged = daily_pnl_df.count()
print(f"daily_pnl rows computed: {pnl_rows_staged}")

daily_pnl_df.createOrReplaceTempView("_daily_pnl_stage")

spark.sql(f"""
MERGE INTO {catalog}.gold.daily_pnl AS t
USING _daily_pnl_stage AS s
  ON t.trade_id = s.trade_id
WHEN MATCHED THEN
    UPDATE SET *
WHEN NOT MATCHED THEN
    INSERT *
""")

pnl_total = spark.table(f"{catalog}.gold.daily_pnl").count()
print(f"gold.daily_pnl total rows after MERGE: {pnl_total}")

# COMMAND ----------
# ── Sanity checks ──────────────────────────────────────────────────────────────
# Log how many trades had a matching close price vs. no price available.

matched = spark.table(f"{catalog}.gold.daily_pnl").filter(F.col("close_price").isNotNull()).count()
unmatched = pnl_total - matched
print(f"daily_pnl — matched to a close price: {matched}  no price found: {unmatched}")

# COMMAND ----------
# ── Conditional OPTIMIZE (every 50 gold runs) ──────────────────────────────────

try:
    gold_runs = spark.sql(
        f"SELECT COUNT(*) AS n FROM {catalog}.audit.pipeline_metrics WHERE task = 'gold'"
    ).collect()[0]["n"]
except Exception:
    gold_runs = 0

if gold_runs > 0 and gold_runs % 50 == 0:
    print(f"Run #{gold_runs}: triggering OPTIMIZE on gold tables")
    spark.sql(f"OPTIMIZE {catalog}.gold.positions")
    spark.sql(f"OPTIMIZE {catalog}.gold.daily_pnl")
else:
    print(f"Skipping OPTIMIZE — gold run #{gold_runs + 1} (fires at multiples of 50)")

# COMMAND ----------
# ── Metrics payload ────────────────────────────────────────────────────────────

metrics = {
    "task":            "gold",
    "rows_in":         trades_rows_in,
    "rows_out":        positions_total + pnl_total,
    "rows_out_positions": positions_total,
    "rows_out_pnl":    pnl_total,
    "rejects":         0,
    "duplicates":      0,
    "run_ts":          datetime.datetime.utcnow().isoformat(),
    "catalog":         catalog,
    "schema":          "gold",
}
print(json.dumps(metrics, indent=2))
dbutils.notebook.exit(json.dumps(metrics))
