#!/usr/bin/env python3
# type: ignore
"""
SEC Pipeline - XBRL Data Processing via Dagster.

This script manages SEC XBRL data processing through 3 independent phases:

  Phase 1 - Download: sec_download job
    Downloads raw XBRL ZIPs to S3 (quarterly partitions).
    Years are automatically converted to quarters (e.g., 2024 -> 2024-Q1..Q4).
    Creates SourceFile records in PostgreSQL for processing tracking.

  Phase 2 - Process: sec_process job (quarterly batch, sensor-driven)
    The sec_processing_sensor discovers quarters with pending SourceFile records
    and triggers one Dagster run per quarter. Each quarter's filings are processed
    together, with output consolidated by filing date (filed=YYYY-MM-DD).
    Dagster's QueuedRunCoordinator controls concurrency via DAGSTER_MAX_CONCURRENT_RUNS.

    In production: Enable sec_processing_sensor in Dagster UI (auto-disabled in dev).
    In development: Use Dagster UI to manually launch sec_process runs, or use
    'just sec-process' to trigger runs via this script.

  Phase 3 - Materialize (decoupled for retry safety):
    sec_stage job: Stage to persistent DuckDB (2+ hours for full SEC)
    sec_materialize job: Materialize from DuckDB to LadybugDB (retry-safe)

    If materialization fails, just re-run sec_materialize - DuckDB is preserved.

Usage:
    # All-in-one (chains all 3 phases):
    just sec-load NVDA 2024        # Single company, all 4 quarters
    just sec-pipeline 5 2024       # Top 5 companies, all 4 quarters

    # Step-by-step (for production use):
    just sec-download 10 2024      # Phase 1: Download (creates SourceFile records)
    # Enable sec_processing_sensor in Dagster UI for Phase 2
    just sec-materialize           # Phase 3: Stage to DuckDB + Materialize to LadybugDB

    # Local development (sensor disabled):
    just sec-download 1 2024       # Phase 1: Download
    just sec-process               # Phase 2: Trigger processing runs manually
    just sec-materialize           # Phase 3: Materialize

    # Decoupled materialization (for checkpointing/retry):
    just sec-stage                 # Stage 1: Stage to DuckDB only
    just sec-materialize-graph     # Stage 2: Materialize to LadybugDB (retry-safe)

    # Reset database
    just sec-reset
"""

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml

from robosystems.config.storage.shared import (
  DataSourceType,
  get_raw_key,
)
from robosystems.logger import logger

# Primary graph starts at 2024 (source of truth: adapters/sec/pipeline/configs.py)
SEC_PRIMARY_START_YEAR = 2024

# Top companies by market cap (as of 2024)
# Used when --count is specified without --tickers
# Note: Only US companies with SEC filings (no foreign ADRs like TSM)
# Note: Excludes mega-financials (JPM, BRK-B) whose filings exceed local memory limits
# LocalStack limits S3 listings to ~1000 files, so capped at 15 companies for local dev
TOP_COMPANIES_BY_MARKET_CAP = [
  "AAPL",  # Apple - Tech
  "MSFT",  # Microsoft - Tech
  "NVDA",  # NVIDIA - Tech/AI
  "GOOGL",  # Alphabet - Tech
  "AMZN",  # Amazon - Tech/Retail
  "META",  # Meta - Tech
  "LLY",  # Eli Lilly - Pharma
  "AVGO",  # Broadcom - Tech
  "WMT",  # Walmart - Retail
  "V",  # Visa - Finance
  "XOM",  # Exxon - Energy
  "UNH",  # UnitedHealth - Healthcare
  "COST",  # Costco - Retail
  "ORCL",  # Oracle - Tech
  "CRM",  # Salesforce - Tech
]

DEFAULT_COMPANY_COUNT = 5
_current_year = datetime.now(UTC).year
ALL_YEARS = list(range(SEC_PRIMARY_START_YEAR, _current_year + 1))


def year_to_quarters(year: int | str) -> list[str]:
  """Convert a year to quarterly partition keys.

  Args:
      year: Year as int or string (e.g., 2024 or "2024")

  Returns:
      List of quarterly partition keys (e.g., ["2024-Q1", "2024-Q2", "2024-Q3", "2024-Q4"])
  """
  y = int(year)
  return [f"{y}-Q{q}" for q in range(1, 5)]


def years_to_quarters(years: list[str]) -> list[str]:
  """Convert a list of years to quarterly partition keys."""
  quarters = []
  for year in years:
    quarters.extend(year_to_quarters(year))
  return quarters


# Default timeouts in seconds (generous for large batch processing)
DEFAULT_DOWNLOAD_TIMEOUT = 7200  # 2 hours per quarter partition
DEFAULT_MATERIALIZE_TIMEOUT = 14400  # 4 hours for full materialization


def get_top_companies(count: int, use_sec_api: bool = False) -> list[str]:
  """Get top N companies by market cap."""
  if use_sec_api:
    try:
      from robosystems.adapters.sec.client.edgar import edgar_client

      companies = edgar_client().company_tickers()
      tickers = []
      for idx in sorted(companies.keys(), key=lambda x: int(x)):
        ticker = companies[idx].get("ticker", "")
        if ticker and len(tickers) < count:
          tickers.append(ticker)
        if len(tickers) >= count:
          break
      return tickers
    except Exception as e:
      logger.warning(f"Failed to fetch from SEC API: {e}, using hardcoded list")

  available = len(TOP_COMPANIES_BY_MARKET_CAP)
  if count > available:
    logger.warning(
      f"Requested {count} companies but only {available} available in hardcoded list. "
      f"Use --tickers to specify additional companies."
    )
  return TOP_COMPANIES_BY_MARKET_CAP[:count]


@dataclass
class StageResult:
  """Result from a pipeline stage."""

  stage: str
  year: str
  success: bool
  duration_seconds: float
  metadata: dict = field(default_factory=dict)
  error: str | None = None


class SECPipeline:
  """SEC pipeline runner - processes companies via Dagster jobs."""

  def __init__(
    self,
    tickers: list[str],
    years: list[str],
    skip_download: bool = False,
    skip_processing: bool = False,
    skip_reset: bool = False,
    verbose: bool = False,
    download_timeout: int = DEFAULT_DOWNLOAD_TIMEOUT,
    materialize_timeout: int = DEFAULT_MATERIALIZE_TIMEOUT,
  ):
    self.tickers = [t.upper() for t in tickers]
    self.years = years
    self.skip_download = skip_download
    self.skip_processing = skip_processing
    self.skip_reset = skip_reset
    self.verbose = verbose
    self.download_timeout = download_timeout
    self.materialize_timeout = materialize_timeout

  def _exec_docker(self, cmd: list[str], timeout: int = 600) -> tuple[bool, str, str]:
    """Execute command in Docker container."""
    try:
      result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
      success = result.returncode == 0 or "RUN_SUCCESS" in result.stdout
      return success, result.stdout, result.stderr
    except subprocess.TimeoutExpired:
      return False, "", "Command timed out"
    except Exception as e:
      return False, "", str(e)

  def _create_job_config(
    self,
    tickers: list[str],
    year: str | None = None,
    skip_existing: bool = True,
    job_type: str = "download_only",
    graph_id: str = "sec",
    reset_staging: bool = False,
    rebuild_graph: bool = False,
    skip_taxonomy: bool = False,
  ) -> str:
    """Create YAML config for Dagster job.

    Args:
        job_type: "download_only", "stage", or "materialize_duckdb"
        graph_id: Graph ID for staging/materialization jobs
        reset_staging: Whether to delete DuckDB file before staging (fresh start)
        rebuild_graph: Whether to rebuild LadybugDB (materialize jobs)
    """

    if job_type == "stage":
      # sec_stage job - stages to persistent DuckDB only (Stage 1)
      # Note: staging doesn't touch LadybugDB - rebuild is handled by materialize
      stage_config: dict[str, Any] = {
        "graph_id": graph_id,
        "reset_staging": reset_staging,
        "start_year": None,
        "end_year": None,
      }
      if year:
        stage_config["year"] = int(year)
      config = {
        "ops": {
          "sec_duckdb_staged": {"config": stage_config},
        }
      }
    elif job_type == "materialize_duckdb":
      # sec_materialize job - materializes from DuckDB to LadybugDB
      # batch_materialization=False for local dev (small data, no memory pressure)
      config = {
        "ops": {
          "sec_graph_materialized": {
            "config": {
              "graph_id": graph_id,
              "rebuild_graph": rebuild_graph,
              "batch_materialization": False,  # Disable hash-based batching for local dev
            }
          },
        }
      }
    elif job_type == "narratives_index":
      config = {
        "ops": {
          "sec_narratives_indexed": {
            "config": {
              "graph_id": graph_id,
            }
          },
        }
      }
    elif job_type == "ixbrl_index":
      config = {
        "ops": {
          "sec_ixbrl_disclosures_indexed": {
            "config": {
              "graph_id": graph_id,
            }
          },
        }
      }
    else:
      # sec_download job: download raw ZIPs only (no processing)
      # EFTS-based discovery - resolves tickers to CIKs in the asset
      config = {
        "ops": {
          "sec_raw_filings": {
            "config": {
              "skip_existing": skip_existing,
              "form_types": ["10-K", "10-Q", "20-F", "40-F", "DEF 14A", "S-1"],
              "tickers": tickers,
            }
          },
        }
      }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
      yaml.dump(config, f, default_flow_style=False)
      config_path = f.name

    os.chmod(config_path, 0o644)

    timestamp = int(time.time() * 1000)
    container_path = f"/tmp/sec_config_{timestamp}.yaml"

    subprocess.run(
      ["docker", "cp", config_path, f"robosystems-dagster-webserver:{container_path}"],
      check=True,
      capture_output=True,
    )
    Path(config_path).unlink()
    return container_path

  def run_stage(
    self,
    job_name: str,
    config_path: str,
    year: str | None = None,
    timeout: int = 600,
  ) -> StageResult:
    """Run a pipeline stage via Dagster."""
    start_time = time.time()

    cmd = [
      "docker",
      "compose",
      "exec",
      "-T",
      "dagster-webserver",
      "dagster",
      "job",
      "execute",
      "-m",
      "robosystems.dagster",
      "--job",
      job_name,
      "-c",
      config_path,
    ]

    if year:
      cmd.extend(["--tags", json.dumps({"dagster/partition": year})])

    if self.verbose:
      logger.info(f"Executing: {' '.join(cmd)}")

    success, stdout, stderr = self._exec_docker(cmd, timeout)
    duration = time.time() - start_time

    error = None
    if not success:
      if stderr:
        # Include first 250 + last 250 chars to preserve context from both ends
        if len(stderr) <= 500:
          error = stderr
        else:
          error = f"{stderr[:250]}...{stderr[-250:]}"
      else:
        error = "Unknown error"

    return StageResult(
      stage=job_name,
      year=year or "all",
      success=success,
      duration_seconds=duration,
      error=error,
    )

  def run(self) -> dict[str, Any]:
    """Run the full pipeline."""
    # Convert years to quarterly partitions
    quarters = years_to_quarters(self.years)

    logger.info("=" * 60)
    logger.info("SEC Pipeline")
    logger.info("=" * 60)
    logger.info(f"Companies: {', '.join(self.tickers)}")
    logger.info(f"Years: {', '.join(self.years)}")
    logger.info(f"Quarters: {len(quarters)} partitions")
    logger.info(f"Skip download: {self.skip_download}")
    logger.info(f"Skip processing: {self.skip_processing}")
    logger.info("=" * 60)

    overall_start = time.time()
    all_results: list[StageResult] = []

    # Reset database first (clean state) - skip if additive mode
    if not self.skip_reset:
      logger.info("\n[SETUP] Resetting SEC database...")
      if not self._reset_database():
        logger.error("Database reset failed - aborting")
        return {"status": "error", "reason": "Database reset failed"}
    else:
      logger.info("\n[SETUP] Skipping database reset (additive mode)")

    # Phase 1: Download each quarterly partition
    for quarter in quarters:
      logger.info(f"\n{'=' * 60}")
      logger.info(f"QUARTER: {quarter}")
      logger.info(f"{'=' * 60}")

      if not self.skip_download:
        logger.info(f"\n[DOWNLOAD] Downloading filings for {quarter}...")
        config_path = self._create_job_config(
          tickers=self.tickers,
          year=None,  # Not used for quarterly partitions
          skip_existing=True,
          job_type="download_only",
        )

        result = self.run_stage(
          job_name="sec_download",
          config_path=config_path,
          year=quarter,  # Pass quarter as partition key
          timeout=self.download_timeout,
        )
        all_results.append(result)

        if result.success:
          logger.info(f"  Complete ({result.duration_seconds:.1f}s)")
        else:
          logger.warning(f"  Issues: {result.error}")

    # Phase 2: Process pending files (quarterly batch, sensor-driven in prod)
    if not self.skip_download and not self.skip_processing:
      logger.info(f"\n{'=' * 60}")
      logger.info("PROCESSING (Quarterly Batch)")
      logger.info(f"{'=' * 60}")
      # Run quarterly batch processing for all quarters with pending files
      process_result = self._run_quarterly_batch_processing()
      if process_result:
        all_results.append(process_result)

    # Phase 3: DuckDB Staging & Materialization (decoupled)
    if not self.skip_processing:
      # Stage 1: DuckDB staging
      logger.info(f"\n{'=' * 60}")
      logger.info("STAGING (DuckDB)")
      logger.info(f"{'=' * 60}")

      stage_config_path = self._create_job_config(
        tickers=self.tickers,
        year=None,
        job_type="stage",
      )

      stage_result = self.run_stage(
        job_name="sec_stage",
        config_path=stage_config_path,
        timeout=self.materialize_timeout,
      )
      all_results.append(stage_result)

      if stage_result.success:
        logger.info(f"  Staging complete ({stage_result.duration_seconds:.1f}s)")

        # Stage 2: LadybugDB materialization (only if staging succeeded)
        logger.info(f"\n{'=' * 60}")
        logger.info("MATERIALIZATION (LadybugDB)")
        logger.info(f"{'=' * 60}")

        mat_config_path = self._create_job_config(
          tickers=self.tickers,
          year=None,
          job_type="materialize_duckdb",
        )

        mat_result = self.run_stage(
          job_name="sec_materialize",
          config_path=mat_config_path,
          timeout=self.materialize_timeout,
        )
        all_results.append(mat_result)

        if mat_result.success:
          logger.info(
            f"  Materialization complete ({mat_result.duration_seconds:.1f}s)"
          )
        else:
          logger.error(f"  Materialization failed: {mat_result.error}")
          logger.info(
            "  Retry with 'just sec-materialize-graph' - staging is preserved"
          )
      else:
        logger.error(f"  Staging failed: {stage_result.error}")

    # Phase 4: Text Search Indexing (OpenSearch) — partitioned by quarter
    if not self.skip_processing:
      logger.info(f"\n{'=' * 60}")
      logger.info("TEXT SEARCH INDEXING (OpenSearch)")
      logger.info(f"{'=' * 60}")

      for quarter in quarters:
        logger.info(f"\n[INDEX {quarter}] Narratives...")
        narr_config_path = self._create_job_config(
          tickers=self.tickers,
          year=None,
          job_type="narratives_index",
        )
        narr_result = self.run_stage(
          job_name="sec_narratives_index",
          config_path=narr_config_path,
          year=quarter,
          timeout=self.materialize_timeout,
        )
        all_results.append(narr_result)
        if narr_result.success:
          logger.info(f"  Narratives indexed ({narr_result.duration_seconds:.1f}s)")
        else:
          logger.warning(f"  Narrative indexing issues: {narr_result.error}")

        logger.info(f"\n[INDEX {quarter}] iXBRL disclosures...")
        ixbrl_config_path = self._create_job_config(
          tickers=self.tickers,
          year=None,
          job_type="ixbrl_index",
        )
        ixbrl_result = self.run_stage(
          job_name="sec_ixbrl_index",
          config_path=ixbrl_config_path,
          year=quarter,
          timeout=self.materialize_timeout,
        )
        all_results.append(ixbrl_result)
        if ixbrl_result.success:
          logger.info(
            f"  iXBRL disclosures indexed ({ixbrl_result.duration_seconds:.1f}s)"
          )
        else:
          logger.warning(f"  iXBRL indexing issues: {ixbrl_result.error}")

    # Summary
    overall_duration = time.time() - overall_start
    successful = sum(1 for r in all_results if r.success)
    failed = sum(1 for r in all_results if not r.success)

    logger.info(f"\n{'=' * 60}")
    logger.info("SUMMARY")
    logger.info(f"{'=' * 60}")
    logger.info(f"Total stages: {len(all_results)}")
    logger.info(f"Successful: {successful}")
    logger.info(f"Failed: {failed}")
    logger.info(f"Duration: {overall_duration:.1f}s ({overall_duration / 60:.1f} min)")

    if failed > 0:
      logger.info("\nFailed stages:")
      for r in all_results:
        if not r.success:
          logger.error(f"  - {r.stage} (year={r.year}): {r.error}")

    return {
      "status": "success" if failed == 0 else "partial_failure",
      "total_stages": len(all_results),
      "successful": successful,
      "failed": failed,
      "duration_seconds": overall_duration,
      "companies": self.tickers,
      "years": self.years,
    }

  def _reset_database(self, clear_s3: bool = False) -> bool:
    """Reset SEC database."""
    import asyncio

    import requests

    graph_api_url = "http://localhost:8001"

    try:
      # Delete existing database
      logger.info("  Deleting existing SEC database...")
      try:
        resp = requests.delete(f"{graph_api_url}/databases/sec", timeout=30)
        if resp.status_code == 200:
          logger.info("  Deleted existing database")
        elif resp.status_code == 404:
          logger.info("  Database didn't exist (OK)")
      except Exception as e:
        logger.warning(f"  Delete failed: {e}")

      # Create database via Graph API REST endpoint
      logger.info("  Creating SEC database...")
      try:
        resp = requests.post(
          f"{graph_api_url}/databases",
          json={
            "graph_id": "sec",
            "schema_type": "shared",
            "repository_name": "sec",
          },
          timeout=60,
        )
        if resp.status_code == 200:
          logger.info("  SEC database created")
        elif resp.status_code == 409:
          logger.info("  SEC database already exists (OK)")
        else:
          logger.error(f"  Create failed: HTTP {resp.status_code} - {resp.text[:300]}")
          return False
      except Exception as e:
        logger.error(f"  Create request failed: {e}")
        return False

      # Ensure PostgreSQL repository metadata exists (Graph + GraphSchema records)
      # This is required for user subscriptions to work
      logger.info("  Ensuring repository metadata exists...")
      try:
        from robosystems.operations.graph.shared_repository_service import (
          ensure_shared_repository_exists,
        )

        result = asyncio.run(
          ensure_shared_repository_exists(
            repository_name="sec",
            created_by="system",
            instance_id="local-dev",
          )
        )
        logger.info(f"  Repository metadata: {result.get('status', 'unknown')}")
      except Exception as e:
        logger.error(f"  Repository metadata creation failed: {e}")
        return False

      # Clear S3 if requested
      if clear_s3:
        self._clear_s3_buckets()
        self._clear_source_files()

      return True

    except Exception as e:
      logger.error(f"Reset failed: {e}")
      return False

  def _clear_s3_buckets(self):
    """Clear SEC data from shared S3 buckets."""
    from robosystems.config import env as app_env

    sec_prefix = get_raw_key(DataSourceType.SEC)  # "sec"

    # Clear SEC prefix in shared buckets
    bucket_prefixes = [
      (app_env.SHARED_RAW_BUCKET, sec_prefix),
      (app_env.SHARED_PROCESSED_BUCKET, sec_prefix),
    ]
    logger.info("  Clearing SEC data from shared buckets...")
    for bucket, prefix in bucket_prefixes:
      if not bucket:
        continue
      try:
        cmd = [
          "aws",
          "s3",
          "rm",
          f"s3://{bucket}/{prefix}/",
          "--recursive",
        ]
        if app_env.AWS_ENDPOINT_URL:
          cmd.extend(["--endpoint-url", app_env.AWS_ENDPOINT_URL])
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
          logger.info(f"    Cleared: {bucket}/{prefix}/")
        elif "NoSuchBucket" in result.stderr:
          logger.debug(f"    Bucket doesn't exist: {bucket}")
      except Exception as e:
        logger.warning(f"    Error clearing {bucket}/{prefix}/: {e}")

  def _clear_source_files(self):
    """Clear SourceFile records for SEC graph.

    This keeps PostgreSQL tracking in sync with S3 when buckets are cleared.
    Without this, stale SourceFile records would prevent reprocessing.
    """
    from robosystems.database import SessionFactory
    from robosystems.models.core import SourceFile

    logger.info("  Clearing SEC SourceFile records...")
    try:
      session = SessionFactory()
      try:
        deleted = (
          session.query(SourceFile).filter(SourceFile.graph_id == "sec").delete()
        )
        session.commit()
        logger.info(f"    Cleared {deleted} SourceFile records")
      finally:
        session.close()
    except Exception as e:
      logger.warning(f"    Error clearing SourceFile records: {e}")

  def _run_quarterly_batch_processing(self) -> StageResult | None:
    """Trigger quarterly batch processing for all quarters with pending SourceFiles.

    In production, the sec_processing_sensor handles this automatically.
    This method is for local development where the sensor is disabled.

    Each quarter's filings are processed together with output consolidated
    by filing date (filed=YYYY-MM-DD/nodes/Table.parquet).

    Returns:
        StageResult with processing outcome
    """
    start_time = time.time()

    # Get quarters with pending files and counts
    quarters_with_pending = self._get_quarters_with_pending_files()
    pending_count, error_count = self._get_source_file_counts()

    if not quarters_with_pending:
      if error_count > 0:
        logger.info(f"  No pending quarters ({error_count} files in error state)")
        logger.info("  Use 'just sec-process --reset-errors' to retry failed files")
      else:
        logger.info("  No pending filings to process")
      return StageResult(
        stage="process_quarterly_batch",
        year="all",
        success=True,
        duration_seconds=time.time() - start_time,
        metadata={"pending": 0, "errors": error_count, "quarters": 0},
      )

    logger.info(
      f"  Found {len(quarters_with_pending)} quarters with {pending_count} pending files"
    )
    logger.info(f"  Quarters: {', '.join(sorted(quarters_with_pending))}")
    if error_count > 0:
      logger.info(f"  ({error_count} files in error state)")

    # Process each quarter
    success_count = 0
    failure_count = 0
    last_error = None

    for quarter in sorted(quarters_with_pending):
      logger.info(f"\n  Processing quarter {quarter}...")

      # Execute the job with partition key
      cmd = [
        "docker",
        "compose",
        "exec",
        "-T",
        "dagster-webserver",
        "dagster",
        "job",
        "execute",
        "-m",
        "robosystems.dagster",
        "--job",
        "sec_process",
        "--tags",
        json.dumps({"dagster/partition": quarter}),
      ]

      # Use longer timeout for batch processing (30 min per quarter)
      success, stdout, stderr = self._exec_docker(cmd, timeout=1800)

      if success:
        success_count += 1
        logger.info(f"    Quarter {quarter} complete")
      else:
        failure_count += 1
        last_error = stderr[:200] if stderr else "Unknown error"
        logger.warning(f"    Quarter {quarter} failed: {last_error}")

    duration = time.time() - start_time
    logger.info(
      f"\n  Processed {success_count} quarters successfully, "
      f"{failure_count} failed ({duration:.1f}s)"
    )

    return StageResult(
      stage="process_quarterly_batch",
      year="all",
      success=failure_count == 0,
      duration_seconds=duration,
      metadata={
        "quarters_processed": success_count,
        "quarters_failed": failure_count,
        "initial_quarters": len(quarters_with_pending),
      },
      error=last_error if failure_count > 0 else None,
    )

  def _get_quarters_with_pending_files(self) -> list[str]:
    """Get list of quarters that have pending SourceFiles."""
    cmd = [
      "docker",
      "compose",
      "exec",
      "-T",
      "dagster-webserver",
      "python",
      "-c",
      """
from robosystems.database import session as SessionLocal
from robosystems.models.core import SourceFile
session = SessionLocal()
try:
    # Get all partition keys for pending files
    files = session.query(SourceFile.partition_key).filter(
        SourceFile.graph_id == "sec",
        SourceFile.status == "pending",
        SourceFile.partition_key.isnot(None),
    ).all()
    # Extract unique quarters from partition keys (format: "2024-Q1_cik_accession")
    quarters = set()
    for (partition_key,) in files:
        if partition_key and "_" in partition_key:
            quarter = partition_key.split("_")[0]
            if "-Q" in quarter:
                quarters.add(quarter)
    for q in sorted(quarters):
        print(q)
finally:
    session.close()
""",
    ]

    result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    if result.returncode == 0:
      quarters = []
      for line in result.stdout.strip().split("\n"):
        line = line.strip()
        if line and "-Q" in line:
          quarters.append(line)
      return quarters

    return []

  def _get_source_file_counts(self) -> tuple[int, int]:
    """Get pending and error counts from SourceFile table."""
    cmd = [
      "docker",
      "compose",
      "exec",
      "-T",
      "dagster-webserver",
      "python",
      "-c",
      """
from robosystems.database import session as SessionLocal
from robosystems.models.core import SourceFile
session = SessionLocal()
try:
    pending = session.query(SourceFile).filter(
        SourceFile.graph_id == "sec",
        SourceFile.status == "pending"
    ).count()
    errors = session.query(SourceFile).filter(
        SourceFile.graph_id == "sec",
        SourceFile.status == "error"
    ).count()
    print(f"{pending},{errors}")
finally:
    session.close()
""",
    ]

    result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    if result.returncode == 0:
      try:
        parts = result.stdout.strip().split(",")
        return int(parts[0]), int(parts[1])
      except (ValueError, IndexError):
        pass

    return 0, 0

  def _reset_error_files(self) -> int:
    """Reset all error status files to pending for retry."""
    cmd = [
      "docker",
      "compose",
      "exec",
      "-T",
      "dagster-webserver",
      "python",
      "-c",
      """
from robosystems.database import session as SessionLocal
from robosystems.models.core import SourceFile
session = SessionLocal()
try:
    updated = session.query(SourceFile).filter(
        SourceFile.graph_id == "sec",
        SourceFile.status == "error"
    ).update({"status": "pending", "error_reason": None})
    session.commit()
    print(f"{updated}")
finally:
    session.close()
""",
    ]

    result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    if result.returncode == 0:
      try:
        return int(result.stdout.strip())
      except ValueError:
        pass

    return 0


def cmd_run(args):
  """Run pipeline command."""
  # Determine companies
  if args.tickers:
    tickers = [t.upper() for t in args.tickers]
  else:
    tickers = get_top_companies(args.count, use_sec_api=args.from_sec)

  # Determine years (will be converted to quarterly partitions by SECPipeline)
  if args.year:
    years = [args.year]
  elif args.years:
    years = args.years
  else:
    years = [str(y) for y in ALL_YEARS]

  pipeline = SECPipeline(
    tickers=tickers,
    years=years,
    skip_download=args.skip_download,
    skip_processing=args.skip_processing,
    verbose=args.verbose,
    download_timeout=args.download_timeout,
    materialize_timeout=args.materialize_timeout,
  )

  # Log timeout settings if non-default
  if args.download_timeout != DEFAULT_DOWNLOAD_TIMEOUT:
    logger.info(
      f"Download timeout: {args.download_timeout}s ({args.download_timeout / 3600:.1f}h)"
    )
  if args.materialize_timeout != DEFAULT_MATERIALIZE_TIMEOUT:
    logger.info(
      f"Materialize timeout: {args.materialize_timeout}s ({args.materialize_timeout / 3600:.1f}h)"
    )

  results = pipeline.run()

  if args.json:
    print(json.dumps(results, indent=2))

  return 0 if results.get("status") == "success" else 1


def cmd_reset(args):
  """Reset database command."""
  logger.info("Resetting SEC database...")
  pipeline = SECPipeline(tickers=[], years=[])
  success = pipeline._reset_database(clear_s3=args.clear_s3)
  if success:
    logger.info("SEC database reset complete")
  return 0 if success else 1


def cmd_download(args):
  """Download only command - downloads raw XBRL ZIPs without processing.

  Use with sec_processing_sensor to trigger parallel processing after download.
  """
  # Determine companies
  if args.tickers:
    tickers = [t.upper() for t in args.tickers]
  else:
    tickers = get_top_companies(args.count, use_sec_api=args.from_sec)

  # Determine years and convert to quarterly partitions
  if args.year:
    years = [args.year]
  elif args.years:
    years = args.years
  else:
    years = [str(y) for y in ALL_YEARS]

  quarters = years_to_quarters(years)

  logger.info("=" * 60)
  logger.info("SEC Download Only (Phase 1)")
  logger.info("=" * 60)
  logger.info(f"Companies: {', '.join(tickers)}")
  logger.info(f"Years: {', '.join(years)}")
  logger.info(f"Quarters: {len(quarters)} partitions")
  logger.info("=" * 60)
  logger.info("After download, enable sec_processing_sensor in Dagster UI")
  logger.info("for parallel processing, then run 'just sec-materialize'")
  logger.info("=" * 60)

  overall_start = time.time()
  all_results = []

  # Create a minimal pipeline for running the download job
  pipeline = SECPipeline(tickers=tickers, years=years, verbose=args.verbose)

  for quarter in quarters:
    logger.info(f"\n[DOWNLOAD] Quarter {quarter}...")
    config_path = pipeline._create_job_config(
      tickers=tickers,
      year=None,  # Not used for quarterly partitions
      skip_existing=True,
      job_type="download_only",
    )

    result = pipeline.run_stage(
      job_name="sec_download",
      config_path=config_path,
      year=quarter,  # Pass quarter as partition key
      timeout=args.timeout,
    )
    all_results.append(result)

    if result.success:
      logger.info(f"  Complete ({result.duration_seconds:.1f}s)")
    else:
      logger.warning(f"  Issues: {result.error}")

  # Summary
  overall_duration = time.time() - overall_start
  successful = sum(1 for r in all_results if r.success)
  failed = sum(1 for r in all_results if not r.success)

  logger.info(f"\n{'=' * 60}")
  logger.info("DOWNLOAD SUMMARY")
  logger.info(f"{'=' * 60}")
  logger.info(f"Total quarters: {len(all_results)}")
  logger.info(f"Successful: {successful}")
  logger.info(f"Failed: {failed}")
  logger.info(f"Duration: {overall_duration:.1f}s ({overall_duration / 60:.1f} min)")

  if args.json:
    print(
      json.dumps(
        {
          "status": "success" if failed == 0 else "partial_failure",
          "total_quarters": len(all_results),
          "successful": successful,
          "failed": failed,
          "duration_seconds": overall_duration,
          "companies": tickers,
          "years": years,
          "quarters": quarters,
        },
        indent=2,
      )
    )

  return 0 if failed == 0 else 1


def cmd_stage(args):
  """Stage command - stages parquet files to persistent DuckDB (decoupled Stage 1).

  This is the first stage of the decoupled pipeline. It persists staging to disk,
  enabling independent retry of materialization without re-running staging.

  Use this when:
  - You want to save 2+ hours of staging work that persists if materialization fails
  - You need to retry materialization without re-staging
  """
  logger.info("=" * 60)
  logger.info("SEC DuckDB Staging (Decoupled Stage 1)")
  logger.info("=" * 60)
  logger.info("Staging processed parquet files to persistent DuckDB")
  logger.info("This enables retry of materialization without re-staging")
  logger.info("=" * 60)

  # Log settings
  if args.year:
    logger.info(f"Year filter: {args.year}")
  if args.reset_staging:
    logger.info("Reset staging enabled - will delete DuckDB file for fresh start")

  # Create minimal pipeline for staging
  pipeline = SECPipeline(
    tickers=[],
    years=[],
    skip_download=True,
    skip_processing=True,
    skip_reset=True,
    verbose=args.verbose,
    materialize_timeout=args.timeout,
  )

  # Run stage job (DuckDB only - LadybugDB rebuild is handled by materialize)
  config_path = pipeline._create_job_config(
    tickers=[],
    year=args.year,
    job_type="stage",
    graph_id=args.graph_id,
    reset_staging=args.reset_staging,
  )

  result = pipeline.run_stage(
    job_name="sec_stage",
    config_path=config_path,
    timeout=args.timeout,
  )

  if result.success:
    logger.info(f"Staging complete ({result.duration_seconds:.1f}s)")
    logger.info("Run 'just sec-materialize-graph' to materialize to LadybugDB")
  else:
    logger.error(f"Staging failed: {result.error}")

  if args.json:
    print(
      json.dumps(
        {
          "status": "success" if result.success else "failure",
          "duration_seconds": result.duration_seconds,
          "error": result.error,
          "graph_id": args.graph_id,
        },
        indent=2,
      )
    )

  return 0 if result.success else 1


def cmd_materialize_graph(args):
  """Materialize from DuckDB command - materializes from existing DuckDB staging (decoupled Stage 2).

  This is the second stage of the decoupled pipeline. It reads from the persistent
  DuckDB staging created by 'just sec-stage' and materializes to LadybugDB.

  Use this when:
  - Staging completed but materialization failed (retry without re-staging)
  - You ran 'just sec-stage' and want to complete the pipeline
  """
  logger.info("=" * 60)
  logger.info("SEC LadybugDB Materialization (Decoupled Stage 2)")
  logger.info("=" * 60)
  logger.info("Materializing from existing DuckDB staging to LadybugDB")
  if args.rebuild_graph:
    logger.info("Rebuild enabled - will delete and recreate LadybugDB")
  logger.info("=" * 60)

  # Create minimal pipeline for materialization
  pipeline = SECPipeline(
    tickers=[],
    years=[],
    skip_download=True,
    skip_processing=True,
    skip_reset=True,
    verbose=args.verbose,
    materialize_timeout=args.timeout,
  )

  # Run materialize_from_duckdb job
  config_path = pipeline._create_job_config(
    tickers=[],
    year=None,
    job_type="materialize_duckdb",
    graph_id=args.graph_id,
    rebuild_graph=args.rebuild_graph,
  )

  result = pipeline.run_stage(
    job_name="sec_materialize",
    config_path=config_path,
    timeout=args.timeout,
  )

  if result.success:
    logger.info(f"Materialization complete ({result.duration_seconds:.1f}s)")
  else:
    logger.error(f"Materialization failed: {result.error}")
    logger.info(
      "You can retry with 'just sec-materialize-graph' - staging is preserved"
    )

  if args.json:
    print(
      json.dumps(
        {
          "status": "success" if result.success else "failure",
          "duration_seconds": result.duration_seconds,
          "error": result.error,
          "graph_id": args.graph_id,
        },
        indent=2,
      )
    )

  return 0 if result.success else 1


def cmd_process(args):
  """Process pending filings via SourceFile queue (quarterly batch processing).

  This command triggers sec_process runs for quarters with pending SourceFiles.
  In production, the sec_processing_sensor handles this automatically.
  This command is for local development where the sensor is disabled.

  Each quarter runs as a separate Dagster job execution, processing all
  filings for that quarter together with output consolidated by filing date.
  """
  logger.info("=" * 60)
  logger.info("SEC Quarterly Batch Processing (Phase 2)")
  logger.info("=" * 60)
  logger.info("Note: In production, enable sec_processing_sensor in Dagster UI")
  logger.info("=" * 60)

  # Create minimal pipeline for processing
  pipeline = SECPipeline(
    tickers=[],
    years=[],
    skip_download=True,
    skip_processing=False,
    skip_reset=True,
    verbose=args.verbose,
  )

  # Reset error files if requested
  if args.reset_errors:
    logger.info("Resetting error files to pending...")
    reset_count = pipeline._reset_error_files()
    logger.info(f"  Reset {reset_count} files")

  # Get initial counts
  pending_count, error_count = pipeline._get_source_file_counts()
  quarters = pipeline._get_quarters_with_pending_files()
  logger.info(f"SourceFile queue: {pending_count} pending, {error_count} errors")
  logger.info(f"Quarters with pending: {len(quarters)}")

  if pending_count == 0:
    if error_count > 0:
      logger.info("No pending files. Use --reset-errors to retry failed files.")
    else:
      logger.info("No files to process.")
    return 0

  # Process quarters
  start_time = time.time()
  result = pipeline._run_quarterly_batch_processing()

  if not result:
    logger.error("Processing returned no result")
    return 1

  # Summary
  duration = time.time() - start_time
  final_pending, final_errors = pipeline._get_source_file_counts()

  logger.info(f"\n{'=' * 60}")
  logger.info("SUMMARY")
  logger.info(f"{'=' * 60}")
  logger.info(f"Quarters processed: {result.metadata.get('quarters_processed', 0)}")
  logger.info(f"Quarters failed: {result.metadata.get('quarters_failed', 0)}")
  logger.info(f"Remaining: {final_pending} pending, {final_errors} errors")
  logger.info(f"Duration: {duration:.1f}s ({duration / 60:.1f} min)")

  if args.json:
    print(
      json.dumps(
        {
          "status": "success" if final_pending == 0 else "partial",
          "quarters_processed": result.metadata.get("quarters_processed", 0),
          "quarters_failed": result.metadata.get("quarters_failed", 0),
          "remaining_pending": final_pending,
          "remaining_errors": final_errors,
          "duration_seconds": duration,
        },
        indent=2,
      )
    )

  return 0 if result.success else 1


def cmd_index(args):
  """Index text content into OpenSearch (Phase 4).

  Runs two indexing jobs (partitioned by quarter):
  1. sec_narratives_indexed — reads raw ZIPs, extracts sections, externalizes, indexes
  2. sec_ixbrl_disclosures_indexed — parses iXBRL disclosure sections with XBRL element metadata + CDN URLs

  Requires processed parquets to exist in S3 (run after processing completes).
  """
  quarter = args.quarter

  logger.info("=" * 60)
  logger.info("SEC Text Search Indexing (Phase 4)")
  logger.info("=" * 60)
  logger.info(f"Quarter: {quarter}")

  pipeline = SECPipeline(
    tickers=[],
    years=[],
    skip_download=True,
    skip_processing=True,
    skip_reset=True,
    verbose=args.verbose,
    materialize_timeout=args.timeout,
  )

  results = []
  start_time = time.time()

  # Narratives
  logger.info("\n[NARRATIVES] Extracting and indexing narrative sections...")
  narr_config = pipeline._create_job_config(
    tickers=[],
    year=None,
    job_type="narratives_index",
    graph_id=args.graph_id,
  )
  narr_result = pipeline.run_stage(
    job_name="sec_narratives_index",
    config_path=narr_config,
    year=quarter,
    timeout=args.timeout,
  )
  results.append(narr_result)
  if narr_result.success:
    logger.info(f"  Narratives indexed ({narr_result.duration_seconds:.1f}s)")
  else:
    logger.error(f"  Narrative indexing failed: {narr_result.error}")

  # iXBRL disclosures
  logger.info("\n[IXBRL] Extracting and indexing iXBRL disclosure sections...")
  ixbrl_config = pipeline._create_job_config(
    tickers=[],
    year=None,
    job_type="ixbrl_index",
    graph_id=args.graph_id,
  )
  ixbrl_result = pipeline.run_stage(
    job_name="sec_ixbrl_index",
    config_path=ixbrl_config,
    year=quarter,
    timeout=args.timeout,
  )
  results.append(ixbrl_result)
  if ixbrl_result.success:
    logger.info(f"  iXBRL disclosures indexed ({ixbrl_result.duration_seconds:.1f}s)")
  else:
    logger.error(f"  iXBRL indexing failed: {ixbrl_result.error}")

  duration = time.time() - start_time
  successful = sum(1 for r in results if r.success)
  failed = sum(1 for r in results if not r.success)

  logger.info(f"\n{'=' * 60}")
  logger.info("SUMMARY")
  logger.info(f"{'=' * 60}")
  logger.info(f"Jobs: {successful} succeeded, {failed} failed")
  logger.info(f"Duration: {duration:.1f}s ({duration / 60:.1f} min)")

  if args.json:
    print(
      json.dumps(
        {
          "status": "success" if failed == 0 else "failure",
          "graph_id": args.graph_id,
          "narratives": {
            "success": narr_result.success,
            "duration": narr_result.duration_seconds,
          },
          "duration_seconds": duration,
        },
        indent=2,
      )
    )

  return 0 if failed == 0 else 1


def main():
  parser = argparse.ArgumentParser(
    description="SEC Pipeline - XBRL Data Processing via Dagster",
    formatter_class=argparse.RawDescriptionHelpFormatter,
  )
  subparsers = parser.add_subparsers(dest="command", help="Commands")

  # Run command
  run_parser = subparsers.add_parser("run", help="Run SEC pipeline")
  run_parser.add_argument(
    "-n",
    "--count",
    type=int,
    default=DEFAULT_COMPANY_COUNT,
    help=f"Number of top companies (default: {DEFAULT_COMPANY_COUNT})",
  )
  run_parser.add_argument(
    "--tickers", nargs="+", help="Specific tickers (overrides --count)"
  )
  run_parser.add_argument(
    "--from-sec", action="store_true", help="Fetch companies from SEC API"
  )
  run_parser.add_argument("--year", type=str, help="Single year to process")
  run_parser.add_argument("--years", nargs="+", help="Specific years to process")
  run_parser.add_argument(
    "--skip-download", action="store_true", help="Skip download stage"
  )
  run_parser.add_argument(
    "--skip-processing", action="store_true", help="Skip processing stage"
  )
  run_parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
  run_parser.add_argument("--json", action="store_true", help="JSON output")
  run_parser.add_argument(
    "--download-timeout",
    type=int,
    default=DEFAULT_DOWNLOAD_TIMEOUT,
    help=f"Timeout per download stage in seconds (default: {DEFAULT_DOWNLOAD_TIMEOUT})",
  )
  run_parser.add_argument(
    "--materialize-timeout",
    type=int,
    default=DEFAULT_MATERIALIZE_TIMEOUT,
    help=f"Timeout for materialization in seconds (default: {DEFAULT_MATERIALIZE_TIMEOUT})",
  )

  # Reset command
  reset_parser = subparsers.add_parser("reset", help="Reset SEC database")
  reset_parser.add_argument(
    "--clear-s3", action="store_true", help="Also clear S3 buckets"
  )

  # Download command (Phase 1 only - no processing)
  download_parser = subparsers.add_parser(
    "download",
    help="Download only (no processing). Use with sensor for parallel processing.",
  )
  download_parser.add_argument(
    "-n",
    "--count",
    type=int,
    default=DEFAULT_COMPANY_COUNT,
    help=f"Number of top companies (default: {DEFAULT_COMPANY_COUNT})",
  )
  download_parser.add_argument(
    "--tickers", nargs="+", help="Specific tickers (overrides --count)"
  )
  download_parser.add_argument(
    "--from-sec", action="store_true", help="Fetch companies from SEC API"
  )
  download_parser.add_argument("--year", type=str, help="Single year")
  download_parser.add_argument("--years", nargs="+", help="Specific years")
  download_parser.add_argument(
    "--timeout",
    type=int,
    default=DEFAULT_DOWNLOAD_TIMEOUT,
    help=f"Timeout per year in seconds (default: {DEFAULT_DOWNLOAD_TIMEOUT})",
  )
  download_parser.add_argument(
    "-v", "--verbose", action="store_true", help="Verbose output"
  )
  download_parser.add_argument("--json", action="store_true", help="JSON output")

  # Stage command - Stage 1 (persistent DuckDB staging)
  stage_parser = subparsers.add_parser(
    "stage",
    help="Stage processed files to persistent DuckDB (Stage 1)",
  )
  stage_parser.add_argument(
    "--graph-id", type=str, default="sec", help="Graph ID (default: sec)"
  )
  stage_parser.add_argument("--year", type=str, help="Optional year filter")
  stage_parser.add_argument(
    "--reset-staging",
    action="store_true",
    help="Delete DuckDB file before staging (fresh start)",
  )
  stage_parser.add_argument(
    "--timeout",
    type=int,
    default=DEFAULT_MATERIALIZE_TIMEOUT,
    help=f"Timeout in seconds (default: {DEFAULT_MATERIALIZE_TIMEOUT})",
  )
  stage_parser.add_argument(
    "-v", "--verbose", action="store_true", help="Verbose output"
  )
  stage_parser.add_argument("--json", action="store_true", help="JSON output")

  # Materialize-duckdb command - Stage 2 (materialize from existing DuckDB)
  mat_graph_parser = subparsers.add_parser(
    "materialize-graph",
    help="Materialize to LadybugDB from existing DuckDB staging (Stage 2, retry-safe)",
  )
  mat_graph_parser.add_argument(
    "--graph-id", type=str, default="sec", help="Graph ID (default: sec)"
  )
  mat_graph_parser.add_argument(
    "--rebuild-graph",
    action="store_true",
    help="Delete and rebuild LadybugDB before materializing (default: append)",
  )
  mat_graph_parser.add_argument(
    "--timeout",
    type=int,
    default=DEFAULT_MATERIALIZE_TIMEOUT,
    help=f"Timeout in seconds (default: {DEFAULT_MATERIALIZE_TIMEOUT})",
  )
  mat_graph_parser.add_argument(
    "-v", "--verbose", action="store_true", help="Verbose output"
  )
  mat_graph_parser.add_argument("--json", action="store_true", help="JSON output")

  # Process command (quarterly batch processing via SourceFile queue)
  process_parser = subparsers.add_parser(
    "process",
    help="Process pending filings by quarter (Phase 2) - for local dev, sensor handles prod",
  )
  process_parser.add_argument(
    "--reset-errors",
    action="store_true",
    help="Reset error files to pending for retry",
  )
  process_parser.add_argument(
    "-v", "--verbose", action="store_true", help="Verbose output"
  )
  process_parser.add_argument("--json", action="store_true", help="JSON output")

  # Index command (Phase 4 - text search indexing)
  index_parser = subparsers.add_parser(
    "index",
    help="Index text blocks + narratives into OpenSearch (Phase 4)",
  )
  index_parser.add_argument(
    "quarter",
    type=str,
    help="Quarter to index (e.g. 2026-Q1)",
  )
  index_parser.add_argument(
    "--graph-id", type=str, default="sec", help="Graph ID (default: sec)"
  )
  index_parser.add_argument(
    "--timeout",
    type=int,
    default=DEFAULT_MATERIALIZE_TIMEOUT,
    help=f"Timeout in seconds (default: {DEFAULT_MATERIALIZE_TIMEOUT})",
  )
  index_parser.add_argument(
    "-v", "--verbose", action="store_true", help="Verbose output"
  )
  index_parser.add_argument("--json", action="store_true", help="JSON output")

  args = parser.parse_args()

  if args.command == "run":
    sys.exit(cmd_run(args))
  elif args.command == "reset":
    sys.exit(cmd_reset(args))
  elif args.command == "download":
    sys.exit(cmd_download(args))
  elif args.command == "stage":
    sys.exit(cmd_stage(args))
  elif args.command == "materialize-graph":
    sys.exit(cmd_materialize_graph(args))
  elif args.command == "process":
    sys.exit(cmd_process(args))
  elif args.command == "index":
    sys.exit(cmd_index(args))
  else:
    parser.print_help()
    sys.exit(0)


if __name__ == "__main__":
  main()
