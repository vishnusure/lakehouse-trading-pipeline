# Databricks notebook source
# MAGIC %md
# MAGIC ## Gold: per-trade daily P&L and book-level positions
# MAGIC
# MAGIC Joins clean trades to closing prices via an asof match (latest close on
# MAGIC or before the trade date) for daily P&L, and aggregates clean trades by
# MAGIC book and symbol for net/gross position and VWAP.

# COMMAND ----------

"""
Gold layer for the Stooq trading pipeline.

Inputs (consumed):
  workspace.silver.trades_clean
  workspace.silver.market_prices

Outputs (written, overwritten on each run):
  workspace.gold.daily_pnl
  workspace.gold.positions

Idempotent. Both tables are recomputed from silver each run.
"""

# COMMAND ----------

dbutils.widgets.text("catalog_name", "workspace")
dbutils.widgets.text("env", "dev")
dbutils.widgets.text("run_date", "")

CATALOG = dbutils.widgets.get("catalog_name")

SILVER_CLEAN = f"{CATALOG}.silver.trades_clean"
SILVER_PRICES = f"{CATALOG}.silver.market_prices"
GOLD_PNL = f"{CATALOG}.gold.daily_pnl"
GOLD_POSITIONS = f"{CATALOG}.gold.positions"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Per-trade daily P&L

# COMMAND ----------

# Asof match: for each trade, attach the close on or before trade_date for the
# same symbol, keeping the most recent such row. QUALIFY collapses the joined
# rows in a single SQL pass without a separate window-then-filter step.
#
# Free Edition serverless does not expose Spark 3.4's asof_join, so this is
# the canonical pre-3.4 pattern. signed_qty makes the brief's
# (close - trade_price) * qty formula correct for SELLs without changing the
# trade-side accounting elsewhere.
spark.sql(f"""
CREATE OR REPLACE TABLE {GOLD_PNL} AS
WITH asof AS (
    SELECT
        t.trade_id,
        t.book,
        t.symbol,
        t.side,
        t.qty,
        t.price       AS trade_price,
        t.trade_date,
        p.date        AS close_date,
        p.close       AS close_price
    FROM {SILVER_CLEAN} t
    LEFT JOIN {SILVER_PRICES} p
      ON p.symbol = t.symbol
     AND p.date  <= t.trade_date
    QUALIFY row_number() OVER (PARTITION BY t.trade_id ORDER BY p.date DESC) = 1
),
signed AS (
    SELECT
        *,
        CASE WHEN side = 'BUY' THEN qty ELSE -qty END AS signed_qty
    FROM asof
)
SELECT
    trade_id,
    book,
    symbol,
    side,
    qty,
    trade_price,
    trade_date,
    close_date,
    close_price,
    signed_qty,
    (close_price - trade_price) * signed_qty AS pnl,
    current_timestamp() AS _gold_processed_at
FROM signed
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC COMMENT ON TABLE workspace.gold.daily_pnl IS
# MAGIC   'One row per silver trade with mark-to-market P&L against the latest close on or before the trade date.';
# MAGIC
# MAGIC ALTER TABLE workspace.gold.daily_pnl ALTER COLUMN trade_id            COMMENT 'Trade identifier from silver.trades_clean.';
# MAGIC ALTER TABLE workspace.gold.daily_pnl ALTER COLUMN book                COMMENT 'Trading book the trade is allocated to.';
# MAGIC ALTER TABLE workspace.gold.daily_pnl ALTER COLUMN symbol              COMMENT 'Trading symbol.';
# MAGIC ALTER TABLE workspace.gold.daily_pnl ALTER COLUMN side                COMMENT 'BUY or SELL.';
# MAGIC ALTER TABLE workspace.gold.daily_pnl ALTER COLUMN qty                 COMMENT 'Trade quantity (always positive).';
# MAGIC ALTER TABLE workspace.gold.daily_pnl ALTER COLUMN trade_price         COMMENT 'Execution price.';
# MAGIC ALTER TABLE workspace.gold.daily_pnl ALTER COLUMN trade_date          COMMENT 'Date the trade was executed.';
# MAGIC ALTER TABLE workspace.gold.daily_pnl ALTER COLUMN close_date          COMMENT 'Price date used for the mark; equal to trade_date when prices exist for that day, otherwise the most recent prior trading day.';
# MAGIC ALTER TABLE workspace.gold.daily_pnl ALTER COLUMN close_price         COMMENT 'Close price as of close_date.';
# MAGIC ALTER TABLE workspace.gold.daily_pnl ALTER COLUMN signed_qty          COMMENT 'qty for BUY, -qty for SELL.';
# MAGIC ALTER TABLE workspace.gold.daily_pnl ALTER COLUMN pnl                 COMMENT 'Mark-to-market P&L: (close_price - trade_price) * signed_qty.';
# MAGIC ALTER TABLE workspace.gold.daily_pnl ALTER COLUMN _gold_processed_at  COMMENT 'When the gold overwrite that produced this row ran.';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Positions

# COMMAND ----------

# avg_price is the volume-weighted average price across all clean trades for
# the (book, symbol) pair. NULLIF guards against gross_qty=0 even though
# silver guarantees qty > 0 per row.
spark.sql(f"""
CREATE OR REPLACE TABLE {GOLD_POSITIONS} AS
SELECT
    book,
    symbol,
    SUM(CASE WHEN side = 'BUY' THEN qty ELSE -qty END) AS net_qty,
    SUM(qty)                                           AS gross_qty,
    SUM(qty * price) / NULLIF(SUM(qty), 0)             AS avg_price,
    COUNT(*)                                           AS trade_count,
    current_timestamp()                                AS _gold_processed_at
FROM {SILVER_CLEAN}
GROUP BY book, symbol
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC COMMENT ON TABLE workspace.gold.positions IS
# MAGIC   'Net and gross position with VWAP per (book, symbol), aggregated from silver.trades_clean.';
# MAGIC
# MAGIC ALTER TABLE workspace.gold.positions ALTER COLUMN book                COMMENT 'Trading book.';
# MAGIC ALTER TABLE workspace.gold.positions ALTER COLUMN symbol              COMMENT 'Trading symbol.';
# MAGIC ALTER TABLE workspace.gold.positions ALTER COLUMN net_qty             COMMENT 'Sum of signed quantities (BUY positive, SELL negative).';
# MAGIC ALTER TABLE workspace.gold.positions ALTER COLUMN gross_qty           COMMENT 'Sum of absolute quantities; total transacted volume.';
# MAGIC ALTER TABLE workspace.gold.positions ALTER COLUMN avg_price           COMMENT 'Volume-weighted average execution price across all trades for this (book, symbol).';
# MAGIC ALTER TABLE workspace.gold.positions ALTER COLUMN trade_count         COMMENT 'Number of clean trades aggregated.';
# MAGIC ALTER TABLE workspace.gold.positions ALTER COLUMN _gold_processed_at  COMMENT 'When the gold overwrite that produced this row ran.';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation

# COMMAND ----------

display(spark.sql(f"""
    SELECT
        (SELECT COUNT(*) FROM {SILVER_CLEAN})                              AS clean_trades,
        (SELECT COUNT(*) FROM {GOLD_PNL})                                  AS pnl_rows,
        (SELECT COUNT(*) FROM {GOLD_PNL} WHERE close_price IS NULL)        AS pnl_no_close_match,
        (SELECT COUNT(*) FROM {GOLD_POSITIONS})                            AS positions_rows
"""))

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {GOLD_PNL} ORDER BY trade_date, trade_id LIMIT 10"))

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {GOLD_POSITIONS} ORDER BY book, symbol"))

# COMMAND ----------

# Metrics handed back to 04_job_runner. Gold doesn't reject or dedup;
# rows_in is the silver clean count and rows_out sums both gold tables.
import json

_clean_rows = spark.table(SILVER_CLEAN).count()
_pnl_rows   = spark.table(GOLD_PNL).count()
_pos_rows   = spark.table(GOLD_POSITIONS).count()

dbutils.notebook.exit(json.dumps({
    "task": "gold",
    "schema": "gold",
    "rows_in": _clean_rows,
    "rows_out": _pnl_rows + _pos_rows,
    "rejects": 0,
    "duplicates": 0,
}))
