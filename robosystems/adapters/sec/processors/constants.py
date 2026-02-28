"""
SEC Processing Constants.

Centralized constants used across SEC processing, consolidation, and ingestion operations.
"""

# Shared node tables that should be deduplicated across filings during consolidation.
# These tables have deterministic IDs (UUID5 based on content) so same content = same identifier.
# Deduplication reduces memory pressure during DuckDB staging by removing duplicates
# that would otherwise need to be handled via memory-intensive window functions.
SHARED_NODE_TABLES = frozenset(
  {
    "nodes/Element",
    "nodes/Label",
    "nodes/Reference",
    "nodes/Unit",
    "nodes/Period",
  }
)

# Maximum parquet files to consolidate in a single chunk during flush_to_s3.
# Limits peak Arrow memory to ~chunk_size * avg_file_size.
# For Label with 1000 files at ~1.3 MB each, chunks of 100 = ~130 MB peak
# instead of 1.3 GB all at once.
SEC_CONSOLIDATION_CHUNK_SIZE = 100

# Quarter end dates mapping for partitioning
QUARTER_END_DAYS = {
  1: "-03-31",
  2: "-06-30",
  3: "-09-30",
  4: "-12-31",
}
