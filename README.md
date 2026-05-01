# Lakehouse Trading Pipeline

Medallion-architecture pipeline (Bronze → Silver → Gold) for trading and financial data.  
Target platform: **Databricks Free Edition** (serverless only, `workspace` Unity Catalog).

---

## Architecture

```
Stooq API (OHLCV)          Synthetic dirty trades
     │                              │
     ▼                              ▼
workspace.bronze.market_prices_raw   workspace.bronze.trades_raw
     │                              │
     └──────────┬───────────────────┘
                ▼
     workspace.silver.market_prices   workspace.silver.trades_clean
                                      workspace.silver.trades_quarantine
                │                              │
                └──────────┬──────────────────┘
                           ▼
              workspace.gold.positions
              workspace.gold.daily_pnl

              workspace.audit.pipeline_metrics  ← every run appends here
```

| Layer | Tables | Key logic |
|-------|--------|-----------|
| Bronze | `market_prices_raw`, `trades_raw` | Stooq fetch (1 ticker at a time, 2 s sleep), synthetic dirty trades; idempotent MERGE |
| Silver | `market_prices`, `trades_clean`, `trades_quarantine` | Schema cast, 6 DQ checks, window dedup, uniqueness assertion |
| Gold | `positions`, `daily_pnl` | Weighted-avg positions; as-of P&L via LATERAL subquery (Free Edition compatible) |
| Audit | `pipeline_metrics` | Append-only; also drives OPTIMIZE throttle (fires every 50 runs) |

---

## Prerequisites

1. **Databricks CLI** authenticated:
   ```bash
   databricks auth profiles          # confirm DEFAULT profile is VALID
   ```

2. **Python 3.8+** with the CLI installed (`pip install databricks-cli` or the newer unified CLI).

---

## Deploy with Asset Bundles

```bash
# From the repo root:

# 1. Validate the bundle config
databricks bundle validate

# 2. Deploy all notebooks and create the job
databricks bundle deploy

# 3. Run the full pipeline immediately (all 4 tasks)
databricks bundle run lakehouse_trading_pipeline
```

The job is also scheduled to run **daily at 06:00 UTC** automatically after deploy.

### Override parameters at run time

```bash
# Run for a specific date
databricks bundle run lakehouse_trading_pipeline \
  --job-params '{"run_date":"2026-04-15","env":"prod","catalog_name":"workspace"}'
```

---

## Standalone single-notebook run (no DAB required)

Open `04_job_runner` in the Databricks workspace and set the `standalone` widget to `true`.  
It will call notebooks 01 → 02 → 03 internally via `dbutils.notebook.run()`, then write all audit metrics.

---

## Verify data after a run

```sql
-- Bronze counts
SELECT COUNT(*) FROM workspace.bronze.market_prices_raw;
SELECT COUNT(*) FROM workspace.bronze.trades_raw;

-- Silver counts + quarantine breakdown
SELECT COUNT(*) FROM workspace.silver.market_prices;
SELECT COUNT(*) FROM workspace.silver.trades_clean;
SELECT rejection_reason, COUNT(*) FROM workspace.silver.trades_quarantine
GROUP BY rejection_reason ORDER BY 2 DESC;

-- Gold: positions and P&L samples
SELECT * FROM workspace.gold.positions LIMIT 10;
SELECT * FROM workspace.gold.daily_pnl ORDER BY unrealized_pnl DESC LIMIT 10;

-- Audit: last 10 pipeline runs
SELECT * FROM workspace.audit.pipeline_metrics ORDER BY run_ts DESC LIMIT 10;

-- Unity Catalog: all pipeline tables
SELECT table_catalog, table_schema, table_name, table_type, created
FROM workspace.information_schema.tables
WHERE table_schema IN ('bronze','silver','gold','audit')
ORDER BY table_schema, table_name;
```

---

## View end-to-end lineage in Catalog Explorer

Unity Catalog automatically tracks table-level lineage every time a notebook reads or writes a managed Delta table.

**Steps:**

1. In the Databricks workspace left nav, click **Catalog** (the database icon).

2. Navigate to **workspace → gold → daily_pnl**.

3. Click the **Lineage** tab (next to Schema, Sample Data, etc.).

4. You will see an upstream lineage graph like:

   ```
   bronze.market_prices_raw ──► silver.market_prices ──► gold.daily_pnl
   bronze.trades_raw        ──► silver.trades_clean   ──► gold.daily_pnl
                                                      └──► gold.positions
   bronze.trades_raw        ──► silver.trades_quarantine
   ```

5. Click any upstream table node to jump to that table's own lineage view.

6. For **column-level lineage**, click on a specific column (e.g. `close_price`) in the Schema tab — Catalog Explorer shows which upstream columns it derives from when column tracking has been collected.

> **Note:** Lineage is populated after at least one successful pipeline run. If the graph appears empty, trigger a run and wait ~5 minutes for the metadata to propagate.

---

## Project structure

```
lakehouse-trading-pipeline/
├── databricks.yml             # Asset Bundle: 4 sequential tasks, daily cron
├── notebooks/
│   ├── 01_bronze.py           # Stooq fetch + synthetic trades → Bronze MERGE
│   ├── 02_silver.py           # Schema enforcement, DQ checks, dedup → Silver MERGE
│   ├── 03_gold.py             # Positions + as-of P&L → Gold MERGE
│   └── 04_job_runner.py       # Audit logging; standalone orchestrator
└── README.md
```

---

## Free Edition constraints respected

| Constraint | Implementation |
|-----------|----------------|
| Serverless only | No `new_cluster` in `databricks.yml`; no DBFS paths |
| No Account Console | Uses auto-provisioned `workspace` catalog only |
| Stooq rate limiting | 1 ticker at a time, 2 s sleep between requests |
| Max 7 tickers | AAPL, MSFT, GOOG, AMZN, TSLA, JPM, NVDA |
| 90-day window | Parameterised; default = today − 90 days |
| Small DataFrames | `coalesce(1)` on all staged DataFrames |
| OPTIMIZE throttle | Fires only every 50 runs per table (tracked via audit table) |
| No VACUUM on every run | Same 50-run threshold (extend to VACUUM if needed) |
| Hive Metastore | Zero references — all tables use `workspace.<schema>.<table>` |
