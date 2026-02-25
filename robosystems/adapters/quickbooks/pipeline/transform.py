"""QuickBooks Transform Asset.

Runs dbt-duckdb models against extracted parquet files and exports
the graph output tables as qb_*.parquet for loading.
"""

import json
import subprocess
import tempfile
from pathlib import Path

from dagster import AssetExecutionContext, MaterializeResult, asset

from .configs import QBSyncConfig
from .utils import DBT_PROJECT_DIR, export_duckdb_tables


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

  1. Runs dbt build with the extracted parquet path as a var
  2. Opens the output DuckDB and exports each graph table as qb_*.parquet

  Reads extract_path from the qb_extract asset's metadata.

  Returns:
      MaterializeResult with output_path metadata (dir with qb_*.parquet files)
  """
  # Get extract path from upstream asset metadata
  extract_events = context.instance.get_latest_materialization_event(
    context.asset_key_for_asset("qb_extract")
  )
  if not extract_events or not extract_events.asset_materialization:
    raise ValueError("No extract metadata found — run qb_extract first")

  extract_metadata = extract_events.asset_materialization.metadata
  extract_path = extract_metadata.get("extract_path")
  if not extract_path:
    raise ValueError("extract_path not found in qb_extract metadata")
  extract_path = (
    extract_path.value if hasattr(extract_path, "value") else str(extract_path)
  )

  context.log.info(
    f"Transform: extract_path={extract_path}, realm_id={config.realm_id}"
  )

  # Create temp directories for dbt output
  work_dir = Path(tempfile.mkdtemp(prefix=f"qb_transform_{config.graph_id}_"))
  duckdb_path = work_dir / "quickbooks.duckdb"
  target_path = work_dir / "target"
  output_dir = work_dir / "output"

  # Build dbt vars
  dbt_vars = json.dumps(
    {
      "realm_id": config.realm_id,
      "qb_extract_path": extract_path,
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
      "QB_DUCKDB_PATH": str(duckdb_path),
    },
  )

  if result.returncode != 0:
    context.log.error(f"dbt build failed:\n{result.stderr}")
    raise RuntimeError(f"dbt build failed: {result.stderr[-500:]}")

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
