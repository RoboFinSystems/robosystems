"""DuckDB Analytics Framework for SEC data.

Provides a sync context manager for running analytics on DuckDB staging files.

- Dev: opens local file at {DUCKDB_STAGING_PATH}/{source}.duckdb
- Prod: downloads from S3 first, then opens locally

Usage:
    with DuckDBAnalyticsContext("sec") as ctx:
        result = ctx.query("SELECT * FROM Entity LIMIT 10")
        ctx.write_parquet(result, "Entity")
        uploaded = ctx.upload_outputs("my_analysis")
"""

import logging
import tempfile
from pathlib import Path

import duckdb

from robosystems.config import env
from robosystems.config.storage.shared import get_staging_duckdb_path

logger = logging.getLogger(__name__)


class DuckDBAnalyticsContext:
  """Opens a read-only DuckDB connection for analytics.

  In dev, opens the local staging file directly. In staging/prod, downloads
  from S3 to an ephemeral directory first. Output parquet files are written
  to a local output directory and optionally uploaded to S3.
  """

  def __init__(
    self,
    duckdb_source: str = "sec",
    memory_limit: str = "8GB",
    threads: int | None = None,
    local_dir: str | None = None,
  ):
    self._duckdb_source = duckdb_source
    self._memory_limit = memory_limit
    self._threads = threads
    self._local_dir = Path(local_dir) if local_dir else None
    self._conn: duckdb.DuckDBPyConnection | None = None
    self._tmpdir: tempfile.TemporaryDirectory | None = None
    self._output_dir: Path | None = None
    self._db_path: Path | None = None

  def __enter__(self) -> "DuckDBAnalyticsContext":
    if env.is_development():
      db_path = Path(get_staging_duckdb_path(self._duckdb_source))
      if not db_path.exists():
        raise FileNotFoundError(
          f"DuckDB staging file not found: {db_path}\n"
          f"Run the SEC staging pipeline first, or download from S3:\n"
          f"  aws s3 cp s3://.../{self._duckdb_source}.duckdb {db_path}"
        )
      self._db_path = db_path
    else:
      self._tmpdir = tempfile.TemporaryDirectory(prefix="duckdb_analytics_")
      tmp_path = Path(self._tmpdir.name)
      db_path = tmp_path / f"{self._duckdb_source}.duckdb"
      self._download_from_s3(db_path)
      self._db_path = db_path

    self._conn = duckdb.connect(str(self._db_path), read_only=True)
    self._conn.execute(f"SET memory_limit = '{self._memory_limit}'")
    if self._threads is not None:
      self._conn.execute(f"SET threads = {self._threads}")

    # Set up output directory
    if self._local_dir:
      self._output_dir = self._local_dir / "output"
    else:
      self._output_dir = self._db_path.parent / "output"
    self._output_dir.mkdir(parents=True, exist_ok=True)

    logger.info(
      "Opened DuckDB analytics: source=%s, path=%s, output=%s",
      self._duckdb_source,
      self._db_path,
      self._output_dir,
    )
    return self

  def __exit__(self, exc_type, exc_val, exc_tb) -> None:
    try:
      if self._conn is not None:
        self._conn.close()
        self._conn = None
    finally:
      # In prod: clean up downloaded DuckDB file
      if self._tmpdir is not None:
        self._tmpdir.cleanup()
        self._tmpdir = None

  @property
  def conn(self) -> duckdb.DuckDBPyConnection:
    if self._conn is None:
      raise RuntimeError("DuckDBAnalyticsContext is not open. Use as context manager.")
    return self._conn

  @property
  def db_path(self) -> Path:
    if self._db_path is None:
      raise RuntimeError("DuckDBAnalyticsContext is not open. Use as context manager.")
    return self._db_path

  def query(self, sql: str) -> duckdb.DuckDBPyRelation:
    """Execute a SQL query and return a DuckDB relation."""
    return self.conn.sql(sql)

  def write_parquet(
    self,
    relation: duckdb.DuckDBPyRelation,
    table_name: str,
    table_type: str = "nodes",
  ) -> Path:
    """Write a DuckDB relation to a parquet file.

    Args:
        relation: DuckDB relation to write
        table_name: Name for the output file (without extension)
        table_type: "nodes" or "relationships" (organizes output subdirectory)

    Returns:
        Path to the written parquet file
    """
    if self._output_dir is None:
      raise RuntimeError("Output directory not set — use as context manager")
    type_dir = self._output_dir / table_type
    type_dir.mkdir(parents=True, exist_ok=True)
    output_path = type_dir / f"{table_name}.parquet"
    relation.write_parquet(str(output_path))
    logger.info("Wrote parquet: %s (%s)", output_path, table_type)
    return output_path

  def upload_outputs(
    self,
    analysis_name: str,
    bucket: str | None = None,
  ) -> dict[str, str]:
    """Upload all parquet files from the output directory to S3.

    In dev: skips upload, returns local paths instead.

    Args:
        analysis_name: Name for this analysis run (used in S3 prefix)
        bucket: Override S3 bucket (default: SHARED_PROCESSED_BUCKET)

    Returns:
        Mapping of table_name to S3 URI (or local path in dev)
    """
    if self._output_dir is None:
      raise RuntimeError("Output directory not set — use as context manager")
    results: dict[str, str] = {}

    parquet_files = list(self._output_dir.rglob("*.parquet"))
    if not parquet_files:
      logger.warning("No parquet files found in %s", self._output_dir)
      return results

    if env.is_development():
      for pf in parquet_files:
        rel_path = pf.relative_to(self._output_dir)
        table_name = pf.stem
        results[table_name] = str(pf)
        logger.info("Dev output: %s -> %s", rel_path, pf)
      return results

    import boto3
    from boto3.s3.transfer import TransferConfig

    target_bucket = bucket or env.SHARED_PROCESSED_BUCKET
    s3 = boto3.client("s3", region_name=env.AWS_REGION)
    transfer_config = TransferConfig(
      multipart_threshold=64 * 1024 * 1024,
      multipart_chunksize=64 * 1024 * 1024,
      max_concurrency=4,
    )

    for pf in parquet_files:
      rel_path = pf.relative_to(self._output_dir)
      # Convention: sec/analytics/{analysis_name}/{table_type}/{table_name}.parquet
      s3_key = f"sec/analytics/{analysis_name}/{rel_path}"
      s3_uri = f"s3://{target_bucket}/{s3_key}"

      logger.info("Uploading %s -> %s", rel_path, s3_uri)
      s3.upload_file(
        str(pf),
        target_bucket,
        s3_key,
        Config=transfer_config,
      )

      table_name = pf.stem
      results[table_name] = s3_uri

    logger.info("Uploaded %d parquet files to S3", len(results))
    return results

  def _download_from_s3(self, dest_path: Path) -> None:
    """Download DuckDB file from S3 to local path."""
    import boto3
    from boto3.s3.transfer import TransferConfig

    from robosystems.config.storage.graph import get_shared_repo_database_key

    sts = boto3.client("sts", region_name=env.AWS_REGION)
    account_id = sts.get_caller_identity()["Account"]
    bucket = f"robosystems-{account_id}-user-{env.ENVIRONMENT}"
    s3_key = get_shared_repo_database_key(self._duckdb_source, ".duckdb")

    logger.info("Downloading s3://%s/%s -> %s", bucket, s3_key, dest_path)

    s3 = boto3.client("s3", region_name=env.AWS_REGION)
    transfer_config = TransferConfig(
      multipart_threshold=64 * 1024 * 1024,
      multipart_chunksize=64 * 1024 * 1024,
      max_concurrency=8,
    )
    s3.download_file(
      bucket,
      s3_key,
      str(dest_path),
      Config=transfer_config,
    )
    logger.info(
      "Downloaded DuckDB: %s (%.2f GB)",
      dest_path,
      dest_path.stat().st_size / (1024**3),
    )
