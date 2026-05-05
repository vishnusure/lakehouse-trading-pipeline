# lakehouse-trading-pipeline

A Databricks Free Edition medallion pipeline for daily Stooq trading data. Bronze ingests raw CSVs landed by a local script and synthesises a deliberately dirty trades table. Silver casts, validates, deduplicates, and quarantines. Gold computes per-trade P&L (asof match against the latest available close) and book-level positions.

## Why local-then-cloud

Stooq blocks or rate-limits cloud IP ranges, including Databricks Free Edition. A local Python script runs on a residential IP and acts as the ingest gateway; it uploads CSVs to a Unity Catalog volume that the Databricks job then consumes. The Databricks job never calls Stooq directly.

## Layout

```
.
├── notebooks/
│   ├── 00_setup_volume.py       Bootstrap: schemas + raw landing volume.
│   ├── 01_bronze.py             Reads CSVs from the volume, synthesises trades.
│   ├── 02_silver.py             Casts, deduplicates, quarantines.
│   ├── 03_gold.py               Daily P&L (asof) and positions.
│   └── 04_job_runner.py         Orchestrates 01-03 and writes audit metrics.
├── scripts/
│   └── ingest_stooq_local.py    Local Mac script that downloads CSVs and uploads to UC.
├── config/
│   ├── tickers.csv              Up to 7 Stooq tickers, one per line.
│   ├── .env.example             Template; copy to .env and fill in STOOQ_API_KEY.
│   └── .env                     Gitignored. Holds STOOQ_API_KEY.
├── databricks.yml               Asset Bundle: one job, daily cron (paused), serverless.
├── requirements-local.txt       Mac-side dependencies for the local script.
└── README.md
```

## Tables produced

| Table | Description |
|---|---|
| `workspace.bronze.market_prices_raw` | Daily OHLCV per ticker, MERGE-loaded from CSVs. |
| `workspace.bronze.trades_raw` | Synthesised dirty trades; string columns hold raw values. |
| `workspace.silver.market_prices` | Non-null prices renamed for join consistency. |
| `workspace.silver.trades_clean` | Validated, type-enforced, latest-version-only trades. |
| `workspace.silver.trades_quarantine` | Trades that failed any DQ rule, with the reason. |
| `workspace.gold.daily_pnl` | Per-trade mark-to-market P&L. |
| `workspace.gold.positions` | Net/gross qty and VWAP per (book, symbol). |
| `workspace.audit.pipeline_metrics` | One row per layer per run. |

## Prerequisites (one-time)

1. `~/.databrickscfg` configured for the `DEFAULT` profile pointing at your Free Edition workspace.
2. `config/.env` created (from `config/.env.example`) with a valid `STOOQ_API_KEY`.
3. Local Python deps installed: `python -m pip install -r requirements-local.txt`.
4. Bootstrap notebook 00 has been run once from the workspace UI; it creates the schemas (`bronze`, `silver`, `gold`, `audit`) and the managed volume `workspace.bronze.stooq_raw_volume`.

## Daily run procedure

```bash
# 1. Local: download CSVs from Stooq and upload to the UC volume.
python scripts/ingest_stooq_local.py

# Optional: override the date window (default is the last 90 days).
python scripts/ingest_stooq_local.py --start 2026-04-01 --end 2026-05-01

# 2. Databricks: deploy the bundle (only required after editing notebooks or databricks.yml)
#    and trigger a one-off run.
databricks bundle deploy
databricks bundle run medallion_pipeline

# 3. Verify: the most recent run wrote three rows to the audit table.
#    Use Catalog Explorer or the SQL editor:
#
#      SELECT task, rows_in, rows_out, rejects, duplicates, run_ts
#      FROM workspace.audit.pipeline_metrics
#      ORDER BY run_ts DESC LIMIT 10;

# 4. Lineage: in Catalog Explorer, navigate to
#    workspace -> bronze -> stooq_raw_volume and follow the "Lineage" tab
#    through bronze.market_prices_raw -> silver.market_prices ->
#    gold.daily_pnl. Trades follow the same chain via bronze.trades_raw.
```

## Recovery

- **Local script reports skipped tickers in its summary.** Stooq occasionally rate-limits or returns empty payloads. Re-run with a smaller date window (`--start` / `--end`) until all tickers succeed. The Databricks job will see the volume as it stands at trigger time, so don't trigger it until the local summary is clean.
- **Databricks job fails mid-run.** Re-run it. All writes are idempotent: bronze MERGEs by key, silver and gold overwrite, audit appends with a fresh `run_id`.
- **Volume is empty.** Bronze raises a `RuntimeError` rather than producing an empty pipeline state. Run the local script first.

## Schedule

The cron in `databricks.yml` is `0 0 7 * * ?` UTC daily, **paused by default**. Unpause from the Workflows UI once the daily cadence is confirmed.

## Asset Bundle CLI cheat sheet

```bash
databricks bundle validate            # YAML and resource checks, no workspace mutation
databricks bundle deploy              # Pushes notebooks and creates/updates the job
databricks bundle run medallion_pipeline  # Triggers a one-off run
databricks bundle destroy             # Removes the bundle's resources
```
