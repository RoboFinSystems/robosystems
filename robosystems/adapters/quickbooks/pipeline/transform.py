"""QuickBooks Transform Asset.

Runs dbt-duckdb models against extracted parquet files to produce
OLTP tables in DuckDB for loading into PostgreSQL.
"""

import json
import os
import subprocess

from dagster import AssetExecutionContext, MaterializeResult, asset

from .configs import QBSyncConfig
from .utils import DBT_PROJECT_DIR, QB_LEDGER_TABLES, get_pipeline_work_dir


@asset(
  group_name="qb_pipeline",
  description="Transform QB data via dbt-duckdb, produce OLTP tables in DuckDB",
  deps=["qb_extract"],
  kinds={"dbt", "duckdb"},
  metadata={
    "pipeline": "quickbooks",
    "stage": "transform",
  },
)
def qb_transform(
  context: AssetExecutionContext,
  config: QBSyncConfig,
) -> MaterializeResult:
  """Run dbt build on extracted QB data.

  Reads extract parquet from the shared pipeline work directory,
  runs dbt build, producing OLTP tables in DuckDB.
  The load asset reads from this DuckDB directly.
  """
  work_dir = get_pipeline_work_dir(config.graph_id)
  extract_dir = work_dir / "extract"
  duckdb_path = work_dir / "quickbooks.duckdb"
  target_path = work_dir / "dbt_target"

  context.log.info(f"Transform: extract_dir={extract_dir}, realm_id={config.realm_id}")

  # Build dbt vars
  dbt_vars = json.dumps(
    {
      "realm_id": config.realm_id,
      "qb_extract_path": str(extract_dir),
      "use_seeds": False,
    }
  )

  # Run dbt build
  context.log.info("Running dbt build...")
  result = subprocess.run(
    [
      "dbt",
      "build",
      "--profiles-dir",
      str(DBT_PROJECT_DIR),
      "--project-dir",
      str(DBT_PROJECT_DIR),
      "--target-path",
      str(target_path),
      "--vars",
      dbt_vars,
    ],
    capture_output=True,
    text=True,
    cwd=str(DBT_PROJECT_DIR),
    env={
      **os.environ,
      "DBT_DUCKDB_PATH": str(duckdb_path),
    },
  )

  # Log stdout/stderr regardless of outcome
  if result.stdout:
    for line in result.stdout.strip().split("\n")[-20:]:
      context.log.info(f"dbt: {line.strip()}")
  if result.returncode != 0:
    if result.stderr:
      context.log.error(f"dbt stderr:\n{result.stderr[-1000:]}")
    raise RuntimeError(f"dbt build failed (exit code {result.returncode})")

  context.log.info("dbt build succeeded")

  # Count rows in OLTP tables
  import duckdb

  table_counts = {}
  con = duckdb.connect(str(duckdb_path), read_only=True)
  try:
    existing_tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}
    for table in QB_LEDGER_TABLES:
      if table in existing_tables:
        row_count = con.execute(f"SELECT count(*) FROM {table}").fetchone()
        table_counts[table] = row_count[0] if row_count else 0
  finally:
    con.close()

  total_rows = sum(table_counts.values())
  context.log.info(f"OLTP tables: {len(table_counts)} tables, {total_rows} total rows")

  return MaterializeResult(
    metadata={
      "duckdb_path": str(duckdb_path),
      "graph_id": config.graph_id,
      "tables": len(table_counts),
      "total_rows": total_rows,
      **{f"rows_{k}": v for k, v in table_counts.items()},
    }
  )
