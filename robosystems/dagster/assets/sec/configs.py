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


# =============================================================================
# Staging Configuration
# =============================================================================


class SECStageConfig(Config):
  """Configuration for DuckDB staging (full rebuild).

  Creates DuckDB tables from scratch using all S3 parquet files.

  Note: This step only stages data to DuckDB. LadybugDB rebuild is handled
  by the materialize step (sec_graph_materialized) via SECMaterializeConfig.rebuild_graph.

  Common scenarios:
    - Normal re-run: Use defaults (reset_staging=False). Tables are overwritten.
    - Enable skip_taxonomy_relationships: Set reset_staging=True AND skip_taxonomy_relationships=True.
      Without reset_staging, old taxonomy tables would remain in DuckDB.
    - Fresh start after corruption: Set reset_staging=True to delete all staging.
  """

  graph_id: str = "sec"  # Target graph ID
  year: int | None = None  # Optional year filter
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


class SECDirectCopyConfig(Config):
  """Configuration for direct S3 → LadybugDB copy (bypasses DuckDB staging).

  This approach:
  1. Reads parquet files directly from S3 using LadybugDB's httpfs extension
  2. Uses spill_to_disk=true for memory-efficient loading of large tables
  3. Handles duplicates via ignore_errors=true (constraint violations skipped)

  Benefits over DuckDB staging:
  - No memory pressure from DuckDB merge/dedupe operations
  - Proven to work at scale (200M+ rows)
  - Simpler pipeline with fewer moving parts

  Trade-offs:
  - Relies on LadybugDB constraints for deduplication (not pre-deduped)
  - May load some duplicate rows that get rejected at insert time
  """

  graph_id: str = "sec"  # Target graph ID
  rebuild_graph: bool = True  # Rebuild LadybugDB before copy
  skip_taxonomy_relationships: bool = False  # Skip taxonomy structure tables
  skip_tables: list[str] = []  # Tables to skip
  year: int | None = None  # Optional year filter (None = all years)
  quarter_copy_timeout: int = 1800  # Timeout per quarter (seconds)
  single_table_timeout: int = 3600  # Timeout for small tables (seconds)


class SECIncrementalCopyConfig(Config):
  """Configuration for incremental S3 → LadybugDB copy (bypasses DuckDB staging).

  This is the preferred approach for daily incremental updates:
  1. Copies directly from S3 parquet to LadybugDB
  2. Uses ignore_errors=true to skip duplicates (constraint violations)
  3. Only scans current quarter + previous quarter during 5-day overlap

  Benefits over DuckDB incremental staging:
  - No need to diff what's new in DuckDB vs LadybugDB
  - Simpler and faster for daily updates
  - LadybugDB handles deduplication via constraints

  When to use this vs sec_duckdb_incremental_staged:
  - Use this for daily incremental updates (simpler, faster)
  - Use DuckDB staging for backfills or when you need DuckDB queries
  """

  graph_id: str = "sec"  # Target graph ID
  year: int | None = None  # Year to copy (default: current year)
  quarter: int | None = Field(
    default=None, ge=1, le=4
  )  # Quarter 1-4 (default: current)
  skip_taxonomy_relationships: bool = False  # Skip taxonomy structure tables
  copy_timeout: int = 600  # Timeout per table copy (seconds)


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
