"""SEC DuckDB Analytics Asset.

Skeleton Dagster asset that demonstrates the DuckDBAnalyticsContext framework.
Runs analytics queries on the SEC DuckDB staging file and writes parquet output.

Replace the example query with real analytics as needed.
"""

from dagster import (
  AssetExecutionContext,
  Config,
  MaterializeResult,
  asset,
)


class SECAnalyticsConfig(Config):
  """Configuration for SEC analytics asset."""

  analysis_name: str = "example"
  duckdb_source: str = "sec"
  memory_limit: str = "8GB"


@asset(
  group_name="sec_pipeline",
  description="Run analytics on SEC DuckDB staging",
  kinds={"duckdb", "analytics"},
  deps=["sec_duckdb_staged"],
  metadata={"pipeline": "sec", "stage": "analytics"},
)
def sec_analytics_computed(
  context: AssetExecutionContext,
  config: SECAnalyticsConfig,
) -> MaterializeResult:
  """Skeleton analytics asset.

  Replace the example query with real analytics.
  """
  from robosystems.adapters.sec.analytics.framework import DuckDBAnalyticsContext

  with DuckDBAnalyticsContext(
    duckdb_source=config.duckdb_source,
    memory_limit=config.memory_limit,
  ) as ctx:
    # Example: entity filing counts
    result = ctx.query("""
      SELECT identifier, name, COUNT(*) as report_count
      FROM Entity
      GROUP BY identifier, name
    """)
    ctx.write_parquet(result, "EntityStats")
    uploaded = ctx.upload_outputs(config.analysis_name)

  return MaterializeResult(
    metadata={
      "analysis_name": config.analysis_name,
      "tables_uploaded": list(uploaded.keys()),
    }
  )
