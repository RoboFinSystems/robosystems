"""QuickBooks Transform Asset.

Runs dbt-duckdb models against extracted parquet files and exports
the graph output tables as qb_*.parquet for loading.
"""

import json
import subprocess

from dagster import AssetExecutionContext, MaterializeResult, asset

from .configs import QBSyncConfig
from .utils import DBT_PROJECT_DIR, export_duckdb_tables, get_pipeline_work_dir


@asset(
  group_name="qb_pipeline",
  description="Transform QB data via dbt-duckdb, export graph tables as parquet",
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
  """Run dbt build on extracted QB data and export graph tables.

  Reads extract parquet from the shared pipeline work directory,
  runs dbt build, then exports graph tables as qb_*.parquet.

  Returns:
      MaterializeResult with output statistics
  """
  work_dir = get_pipeline_work_dir(config.graph_id)
  extract_dir = work_dir / "extract"
  duckdb_path = work_dir / "quickbooks.duckdb"
  target_path = work_dir / "dbt_target"
  output_dir = work_dir / "output"

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
      **__import__("os").environ,
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

  # Export graph tables from DuckDB to parquet
  table_counts = export_duckdb_tables(duckdb_path, output_dir)
  total_rows = sum(table_counts.values())

  context.log.info(
    f"Exported {len(table_counts)} tables ({total_rows} total rows) → {output_dir}"
  )

  return MaterializeResult(
    metadata={
      "output_path": str(output_dir),
      "graph_id": config.graph_id,
      "tables_exported": len(table_counts),
      "total_rows": total_rows,
      **{f"rows_{k}": v for k, v in table_counts.items()},
    }
  )
