# Databricks notebook source
# MAGIC %md
# MAGIC ## Bootstrap: Unity Catalog schemas and raw landing volume
# MAGIC
# MAGIC One-time setup. Run from the workspace UI before the local ingest script
# MAGIC and before notebooks 01-04. Idempotent.

# COMMAND ----------

"""
Provisions the Unity Catalog objects the medallion pipeline depends on: schemas
workspace.bronze, silver, gold and audit, and the managed volume
workspace.bronze.stooq_raw_volume that receives CSVs from the local Stooq ingest.

This notebook reads no upstream tables and writes no data; it only creates
catalog objects and emits grants. Re-running is safe and never drops existing
tables.

Run once before scripts/ingest_stooq_local.py and notebooks 01-04.
"""

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.bronze
# MAGIC COMMENT 'Raw Stooq CSVs landed by the local ingest, plus synthesised dirty trades.';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.silver
# MAGIC COMMENT 'Type-enforced, deduplicated prices and trades, with a quarantine table for rejects.';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.gold
# MAGIC COMMENT 'Business outputs: positions and daily P&L derived from silver.';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.audit
# MAGIC COMMENT 'Operational metrics emitted by each notebook task on every run.';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Raw landing volume
# MAGIC
# MAGIC Managed volume for CSVs uploaded by scripts/ingest_stooq_local.py.
# MAGIC The bronze notebook reads from this path; nothing else writes to it.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.bronze.stooq_raw_volume
# MAGIC COMMENT 'Landing zone for Stooq CSVs uploaded from the local ingest script. Path: /Volumes/workspace/bronze/stooq_raw_volume/.';

# COMMAND ----------

# Grants are emitted against the resolved running identity so the notebook is
# portable across workspaces where the literal user differs. On Free Edition
# the creator already owns these objects, so the grants are largely a no-op
# for self-runs but document the required privileges explicitly.
run_identity = spark.sql("SELECT current_user() AS u").first()["u"]

grants = [
    f"GRANT USE CATALOG ON CATALOG workspace TO `{run_identity}`",
    f"GRANT USE SCHEMA, CREATE TABLE ON SCHEMA workspace.bronze TO `{run_identity}`",
    f"GRANT USE SCHEMA, CREATE TABLE ON SCHEMA workspace.silver TO `{run_identity}`",
    f"GRANT USE SCHEMA, CREATE TABLE ON SCHEMA workspace.gold   TO `{run_identity}`",
    f"GRANT USE SCHEMA, CREATE TABLE ON SCHEMA workspace.audit  TO `{run_identity}`",
    f"GRANT READ VOLUME, WRITE VOLUME ON VOLUME workspace.bronze.stooq_raw_volume TO `{run_identity}`",
]

for stmt in grants:
    spark.sql(stmt)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification

# COMMAND ----------

display(
    spark.sql(
        """
        SELECT schema_name
        FROM workspace.information_schema.schemata
        WHERE schema_name IN ('bronze','silver','gold','audit')
        ORDER BY schema_name
        """
    )
)

# COMMAND ----------

display(spark.sql("DESCRIBE VOLUME workspace.bronze.stooq_raw_volume"))

# COMMAND ----------

# Surface whether the local ingest has run yet. An empty listing is the
# expected state on first bootstrap and is the cue to run the local script.
volume_path = "/Volumes/workspace/bronze/stooq_raw_volume/"
files = dbutils.fs.ls(volume_path)

if not files:
    print(
        f"{volume_path} is empty.\n"
        "Run scripts/ingest_stooq_local.py from your local machine before executing 01_bronze."
    )
else:
    print(f"{len(files)} file(s) present in {volume_path}:")
    for f in files:
        print(f" - {f.name}  ({f.size} bytes)")
