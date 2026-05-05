# CLAUDE.md

## Project context
- Project: lakehouse-trading-pipeline
- Target platform: Databricks Free Edition (serverless only)
- Unity Catalog: use `workspace` catalog (auto-provisioned)
- Python: PySpark notebooks deployed via Databricks Asset Bundles
- Git: simple push to main branch on GitHub
- Behaviour: always ask for approval before executing each task

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A Databricks lakehouse pipeline for trading/financial data sourced from [Stooq](https://stooq.com). The pipeline follows the **medallion architecture** (Bronze → Silver → Gold layers), with the Bronze ingestion layer as the starting point.

## Databricks MCP Server

This project is configured with the Databricks MCP server (`.mcp.json`). All Databricks operations — executing SQL, managing jobs, pipelines, Unity Catalog objects, clusters, etc. — should be done via MCP tools rather than shell commands.

The MCP server uses `DATABRICKS_CONFIG_PROFILE=DEFAULT` from the local `~/.databrickscfg`. To check or switch the active workspace:

```
/databricks-config
```

## Databricks AI Dev Kit Skills

The Databricks AI Dev Kit (v0.1.10) is installed with a full set of skills. Invoke relevant skills before implementing Databricks features — they contain critical patterns and gotchas:

| Task | Skill |
|------|-------|
| Spark Declarative Pipelines (SDP/LDP) | `/databricks-spark-declarative-pipelines` |
| Spark Structured Streaming | `/databricks-spark-structured-streaming` |
| Jobs and orchestration | `/databricks-jobs` |
| Unity Catalog (tables, volumes, grants) | `/databricks-unity-catalog` |
| Databricks SQL / materialized views | `/databricks-dbsql` |
| AI/BI dashboards | `/databricks-aibi-dashboards` |
| MLflow tracing and evaluation | `/instrumenting-with-mlflow-tracing`, `/databricks-mlflow-evaluation` |
| Vector Search / RAG | `/databricks-vector-search` |
| Asset Bundles (CI/CD) | `/databricks-bundles` |

## Architecture Direction

- **Medallion layers**: Bronze (raw ingest from Stooq), Silver (cleaned/normalized), Gold (aggregated/feature-ready)
- Pipelines should be built using **Spark Declarative Pipelines** (preferred over DLT notebooks) or **Spark Structured Streaming** for real-time flows
- Data governance via **Unity Catalog** (catalog → schema → table hierarchy)
- Orchestration via **Databricks Jobs**
