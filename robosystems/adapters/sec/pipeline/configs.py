"""SEC Pipeline Configuration Classes.

All configuration classes for SEC Dagster assets are centralized here
for maintainability and reuse.
"""

from datetime import UTC, datetime

from dagster import Config, StaticPartitionsDefinition
from pydantic import Field

from robosystems.config.constants import SEC_PROCESS_BATCH_LIMIT

# =============================================================================
# Constants
# =============================================================================

# Start year for SEC data loading (XBRL filings began 2009)
SEC_START_YEAR = 2009

# Tiered graph boundaries
SEC_HISTORICAL_END_YEAR = 2023  # sec_historical: 2009-2023
SEC_PRIMARY_START_YEAR = 2024  # sec (primary): 2024+

# Form types for historical graph (annual reports only)
# sec_historical includes 10-K and foreign equivalents (20-F, 40-F)
SEC_HISTORICAL_FORM_TYPES = ["10-K", "20-F", "40-F"]

# Form type batches for EFTS queries to avoid 10k result limit
# Q2 (proxy season) can exceed 10k when all forms are included
# Batch 1: Core financial statements
# Batch 2: Supplementary filings (proxies, registrations)
SEC_FORM_TYPE_BATCHES = [
  ["10-K", "10-Q", "20-F", "40-F"],  # Core financials (~7k in Q2)
  ["DEF 14A", "S-1"],  # Supplementary (~3k in Q2)
]

# Quarter partitions for SEC data (SEC_START_YEAR-Q1 through current year Q4)
# EFTS has a 10k result limit per query; quarterly partitions typically return 5-7k filings
# Dynamically includes current year so no manual updates needed on Jan 1
_current_year = datetime.now(UTC).year
SEC_QUARTERS = [
  f"{year}-Q{q}"
  for year in range(SEC_START_YEAR, _current_year + 1)
  for q in range(1, 5)
]
sec_quarter_partitions = StaticPartitionsDefinition(SEC_QUARTERS)


# =============================================================================
# Download Configuration
# =============================================================================


class SECDownloadConfig(Config):
  """Configuration for SEC raw filings download.

  Production Scaling Notes:
  - Each year partition runs independently (can parallelize years)
  - Submissions fetching: ~8 req/sec, 5 concurrent (configurable)
  - Filing downloads: ~5 req/sec, 10 concurrent (configurable)
  - For full year (~5000 companies, ~10000 filings): ~45 min total
  - Use max_filings for testing, dry_run for discovery only
  """

  skip_existing: bool = True  # Skip already downloaded filings
  skip_submissions: bool = False  # Skip fetching/updating submissions.json files
  form_types: list[str] = [
    "10-K",
    "10-Q",
    "20-F",
    "40-F",
    "DEF 14A",
    "S-1",
  ]  # Form types to download
  tickers: list[str] = []  # Optional ticker filter (empty = all companies)
  ciks: list[str] = []  # Optional CIK filter
  max_filings: int = 0  # Max filings to download (0 = unlimited)
  dry_run: bool = False  # If True, discover only - don't download

  # Concurrency controls for production
  submissions_rate: float = 8.0  # Submissions requests per second
  submissions_concurrency: int = 5  # Max concurrent submission fetches
  download_rate: float = 5.0  # Download requests per second
  download_concurrency: int = 10  # Max concurrent downloads


# =============================================================================
# Processing Configuration
# =============================================================================


class SECProcessConfig(Config):
  """Configuration for batch filing processing by quarter.

  Each Dagster run processes up to batch_limit filings and then exits.
  The sensor will trigger another run if pending files remain, enabling
  natural memory release between batches and better crash resilience.

  Individual filing failures are tracked in SourceFile records,
  but the job continues processing remaining filings in the batch.

  Memory Management:
  - Each job processes at most batch_limit filings (default 2,000)
  - Job exits gracefully after batch, releasing all memory
  - Sensor re-triggers if more pending files exist
  - Processing is disk-buffered (not memory-intensive)

  Note: Smaller batches leverage the merge strategy for crash resilience.
  Each batch flushes to S3 (merging with existing data), so a crash loses
  at most one batch instead of an entire quarter.
  """

  # Max filings to process per job run before exiting gracefully.
  # Smaller batches provide crash resilience via incremental S3 merging.
  batch_limit: int = SEC_PROCESS_BATCH_LIMIT

  # Continue processing even if some filings fail
  # If False, job fails on first error (for debugging)
  continue_on_error: bool = True

  # Form types to include (None = all types, no filtering).
  # Filings with non-matching form types are marked "skipped" in SourceFile.
  # Example: ["10-K", "20-F", "40-F"] for annual reports only.
  form_types: list[str] | None = None


# =============================================================================
# Staging Configuration
# =============================================================================


class SECStageConfig(Config):
  """Configuration for DuckDB staging (full rebuild) for the primary sec graph.

  Creates DuckDB tables from scratch using S3 parquet files from 2024 onwards.

  Note: This step only stages data to DuckDB. LadybugDB rebuild is handled
  by the materialize step (sec_graph_materialized) via SECMaterializeConfig.rebuild_graph.

  Year filtering:
    - year: Single year filter (e.g., 2024)
    - start_year: Defaults to SEC_PRIMARY_START_YEAR (2024). Override for broader range.

  Common scenarios:
    - Normal re-run: Use defaults (reset_staging=False). Tables are overwritten.
    - Enable skip_taxonomy_relationships: Set reset_staging=True AND skip_taxonomy_relationships=True.
      Without reset_staging, old taxonomy tables would remain in DuckDB.
    - Fresh start after corruption: Set reset_staging=True to delete all staging.
  """

  graph_id: str = "sec"  # Target graph ID
  year: int | None = None  # Optional single year filter
  start_year: int = SEC_PRIMARY_START_YEAR  # Start of year range (default: 2024)
  end_year: int | None = None  # No upper bound (stages through current year)
  reset_staging: bool = False  # Delete entire DuckDB staging database first
  skip_taxonomy_relationships: bool = False  # Skip taxonomy structure tables


class SECHistoricalStageConfig(Config):
  """Configuration for SEC historical DuckDB staging.

  Stages historical SEC data to a separate DuckDB database for the
  sec_historical subgraph. Year range defaults are visible and overridable.
  """

  graph_id: str = "sec_historical"  # Target graph ID
  start_year: int = SEC_START_YEAR  # Start of year range (default: 2009)
  end_year: int = SEC_HISTORICAL_END_YEAR  # End of year range (default: 2023)
  reset_staging: bool = False  # Delete entire DuckDB staging database first
  skip_taxonomy_relationships: bool = False  # Skip taxonomy structure tables


class SECIncrementalStageConfig(Config):
  """Configuration for incremental SEC staging.

  Stages current quarter's files incrementally using INSERT INTO with
  deduplication. Safe to run daily - only net new rows are added.

  Precondition: Initial full staging must have been done (tables exist).
  """

  graph_id: str = "sec"  # Target graph ID
  year: int | None = None  # Year to stage (default: current year)
  quarter: int | None = Field(
    default=None, ge=1, le=4
  )  # Quarter 1-4 (default: current)
  skip_taxonomy_relationships: bool = False  # Skip taxonomy structure tables


# =============================================================================
# Materialization Configuration
# =============================================================================


class SECMaterializeConfig(Config):
  """Configuration for graph materialization (Stage 2).

  Use this config with sec_graph_materialized asset to materialize
  from DuckDB staging to LadybugDB.

  Options:
    graph_id: Target graph ID (default: "sec")
    rebuild_graph: If True (default), delete and recreate the LadybugDB database
                   with the roboledger SEC schema before materializing.
                   DuckDB staging is preserved. Set to False only for retry scenarios
                   where you want to resume without losing existing graph data.
    skip_taxonomy_relationships: If True, skip materializing taxonomy structure
                                 tables (Association, Structure, TAXONOMY_HAS_*, etc.)
    batch_materialization: If True (default), use hash-based batching for tables
                           with more rows than materialization_batch_size.
    materialization_batch_size: Rows per batch when batch_materialization is enabled
                                (default: 20M rows).
  """

  graph_id: str = "sec"  # Target graph ID
  rebuild_graph: bool = True  # Rebuild LadybugDB before materialization
  skip_taxonomy_relationships: bool = False  # Skip taxonomy structure tables
  batch_materialization: bool = True  # Hash-based batching for large tables
  materialization_batch_size: int = Field(
    default=20_000_000, ge=1_000_000
  )  # Rows per batch


# =============================================================================
# Entity Update Configuration
# =============================================================================


class SECEntityUpdateConfig(Config):
  """Configuration for incremental Entity update asset."""

  graph_id: str = "sec"
  year: int | None = Field(default=None, description="Year (default: current year)")
  quarter: int | None = Field(
    default=None, ge=1, le=4, description="Quarter 1-4 (default: current)"
  )


# =============================================================================
# Backup Configuration
# =============================================================================


class SECBackupConfig(Config):
  """Configuration for SEC backup generation."""

  graph_id: str = "sec"
  retention_days: int = 14
  compression: bool = True
  encryption: bool = False  # Unencrypted for downloads
  backup_type: str = "full"
  backup_format: str = "full_dump"
