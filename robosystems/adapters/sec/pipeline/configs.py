"""Dagster config classes and partition constants for the SEC pipeline."""

from datetime import UTC, datetime
from typing import Literal

from dagster import Config, StaticPartitionsDefinition
from pydantic import Field

from robosystems.config.constants import SEC_PROCESS_BATCH_SIZE

# XBRL filings began in 2009.
SEC_START_YEAR = 2009

# Tiered graph boundaries
SEC_HISTORICAL_END_YEAR = 2023  # sec_historical: 2009-2023
SEC_PRIMARY_START_YEAR = 2024  # sec (primary): 2024+

# sec_historical holds annual reports only: 10-K and its foreign equivalents.
SEC_HISTORICAL_FORM_TYPES = ["10-K", "20-F", "40-F"]

# EFTS caps a query at 10k results; Q2 (proxy season) exceeds that with all
# forms in one query, so forms are queried in batches.
SEC_FORM_TYPE_BATCHES = [
  ["10-K", "10-Q", "20-F", "40-F"],  # Core financials (~7k in Q2)
  ["DEF 14A", "S-1"],  # Supplementary (~3k in Q2)
]

# SEC_START_YEAR-Q1 through next year's Q4. Quarterly keeps each EFTS query
# under its 10k cap (typically 5-7k). Computed at import, so next year's
# quarters are included and a deploy keeps accepting new partition keys past Jan 1.
_current_year = datetime.now(UTC).year
SEC_QUARTERS = [
  f"{year}-Q{q}"
  for year in range(SEC_START_YEAR, _current_year + 2)
  for q in range(1, 5)
]
sec_quarter_partitions = StaticPartitionsDefinition(SEC_QUARTERS)


class SECDownloadConfig(Config):
  """SEC raw filings download. A full year (~10k filings) takes ~45 min."""

  skip_existing: bool = True
  skip_submissions: bool = False  # Skip fetching/updating submissions.json files
  form_types: list[str] = [
    "10-K",
    "10-Q",
    "20-F",
    "40-F",
    "DEF 14A",
    "S-1",
  ]
  tickers: list[str] = []  # empty = all companies
  ciks: list[str] = []
  max_filings: int = 0  # Max filings to download (0 = unlimited)
  dry_run: bool = False  # discover only, don't download

  submissions_rate: float = 8.0  # requests per second
  submissions_concurrency: int = 5
  download_rate: float = 5.0  # requests per second
  download_concurrency: int = 10


class SECProcessConfig(Config):
  """Batch filing processing for one quarter.

  Each run processes one batch, writes one UUID-named part file per table
  (``filed=<quarter>/nodes/<Table>/part_<uuid>.parquet``), and exits; the
  sensor re-triggers while pending files remain, which releases memory between
  batches. The small batch keeps the Arrow concat under ~325 MB peak. Shared
  tables are deduped within a batch; DuckDB dedups across batches at staging.
  Per-filing failures are recorded on SourceFile and the batch continues.
  """

  batch_size: int = SEC_PROCESS_BATCH_SIZE

  # False fails the job on the first error (for debugging).
  continue_on_error: bool = True

  # Per-filing outputs cached to S3 so a Spot restart restores rather than
  # reprocesses.
  enable_cache: bool = True

  # None = all forms; non-matching filings are marked "skipped" in SourceFile.
  form_types: list[str] | None = None


class SECStageConfig(Config):
  """Full DuckDB staging rebuild for the primary sec graph.

  Stages only; the LadybugDB rebuild is the materialize step's
  (``SECMaterializeConfig.rebuild_graph``). Re-runs overwrite tables;
  ``reset_staging`` wipes staging after corruption.
  """

  graph_id: str = "sec"
  year: int | None = None  # single-year filter
  start_year: int | None = SEC_PRIMARY_START_YEAR  # None = all years
  end_year: int | None = None  # None = through current
  reset_staging: bool = False  # delete the whole staging database first


class SECHistoricalStageConfig(Config):
  """DuckDB staging for the sec_historical subgraph (a separate database)."""

  graph_id: str = "sec_historical"
  start_year: int = SEC_START_YEAR
  end_year: int = SEC_HISTORICAL_END_YEAR
  reset_staging: bool = False  # delete the whole staging database first


class SECIncrementalStageConfig(Config):
  """Incremental staging of one quarter: INSERT of net-new rows only.

  Requires a prior full staging (the tables must exist).
  """

  graph_id: str = "sec"
  year: int | None = None  # None = current year
  quarter: int | None = Field(default=None, ge=1, le=4)  # None = current quarter


class SECMaterializeConfig(Config):
  """DuckDB staging to LadybugDB materialization.

  ``full`` rebuilds the database (when ``rebuild_graph``) and COPYs every
  table into an empty target. ``incremental`` never rebuilds and COPYs only
  rows missing from the live graph (per-table keyset anti-join), so it is safe
  to repeat. ``rebuild_graph=False`` is for resuming a failed full run without
  losing graph data. Tables larger than ``materialization_batch_size`` are
  hash-batched when ``batch_materialization`` is on.
  """

  graph_id: str = "sec"
  materialize_mode: Literal["full", "incremental"] = "full"
  rebuild_graph: bool = True
  batch_materialization: bool = True
  materialization_batch_size: int = Field(default=20_000_000, ge=1_000_000)


class SECNarrativeIndexConfig(Config):
  """Narrative section extraction and OpenSearch indexing, per quarter."""

  graph_id: str = "sec"
  part_size: int = Field(
    default=25000,
    description="Split a section longer than this into parts of about this many "
    "characters, cut at paragraph boundaries (0 = never split)",
  )
  form_types: list[str] = Field(
    default=["10-K", "10-Q", "20-F"],
    description="Form types to extract narratives from",
  )
  force_reindex: bool = Field(
    default=False, description="Re-index all documents (ignore incremental skip)"
  )
  skip_embeddings: bool = Field(
    default=False,
    description="Index without embeddings (BM25 only). Reduces memory pressure on OpenSearch.",
  )


class SECiXBRLIndexConfig(Config):
  """iXBRL disclosure extraction and OpenSearch indexing, per quarter."""

  graph_id: str = "sec"
  part_size: int = Field(
    default=25000,
    description="Split a section longer than this into parts of about this many "
    "characters, cut at paragraph boundaries (0 = never split)",
  )
  form_types: list[str] = Field(
    default=["10-K", "10-Q", "20-F"],
    description="Form types to extract iXBRL disclosures from",
  )
  force_reindex: bool = Field(
    default=False, description="Re-index all documents (ignore incremental skip)"
  )
  skip_embeddings: bool = Field(
    default=False,
    description="Index without embeddings (BM25 only). Reduces memory pressure on OpenSearch.",
  )


class SECFilingCatalogConfig(Config):
  """Per-filer catalog on the public CDN, partitioned by quarter.

  A run rewrites the catalog file of every filer
  with a filing in its partitions and the corpus index always; the corpus
  view it folds spans every partition from ``start_year`` on.

  The catalog lists everything processed, not the graph's range: the files it
  points at are read off the CDN, never the graph, so a filing that is only on
  the CDN would otherwise sit there unlisted. Years with nothing processed fold
  as empty.
  """

  graph_id: str = "sec"
  start_year: int = Field(
    default=SEC_START_YEAR,
    description="First filing year in the catalog (every processed year)",
  )
  form_types: list[str] = Field(
    default=["10-K", "10-Q", "20-F", "40-F"],
    description="Forms listed per filer; other forms carry no statements",
  )
  require_ticker: bool = Field(
    default=True,
    description="List only filers with a ticker (the pages are keyed by ticker)",
  )
  full_rebuild: bool = Field(
    default=False,
    description="Rewrite every filer's catalog file, not only the partition's",
  )
  manifest_workers: int = Field(
    default=16, description="Concurrent manifest reads from the public bucket"
  )
  viewer_url: str = Field(
    default="https://xbrlkit.com",
    description="The xbrlkit viewer the catalog's viewer links open",
  )


class SECHFPublishConfig(Config):
  """Manual Hugging Face dataset publish, run as an HF Job (R2 -> Job -> Hub).

  cpu-basic suffices: its job filesystem is ~1.7 TB in practice (the table
  says 50 GB), so a larger flavor buys speed, not room.
  """

  job_flavor: str = Field(
    default="cpu-basic", description="Hugging Face Job hardware flavor"
  )
  job_timeout: str = Field(
    default="6h", description="Hugging Face Job timeout (download + upload)"
  )
  prune_previous: bool = Field(
    default=True,
    description="Delete superseded snapshots' LFS objects after a verified upload",
  )
