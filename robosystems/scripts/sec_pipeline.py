#!/usr/bin/env python3
# type: ignore
"""
SEC Pipeline - XBRL Data Processing via Dagster.

This script manages SEC XBRL data processing through 3 independent phases:

  Phase 1 - Download: sec_download job
    Downloads raw XBRL ZIPs to S3 (year-partitioned).

  Phase 2 - Process: sec_process job (parallel)
    Processes each filing to parquet via dynamic partitions.

  Phase 3 - Materialize: sec_materialize job
    Stages parquet files in DuckDB and materializes to LadybugDB.

Usage:
    # All-in-one (chains all 3 phases):
    just sec-load NVDA 2024        # Single company
    just sec-pipeline 5 2024       # Top 5 companies

    # Step-by-step (for production use):
    just sec-download 10 2024      # Phase 1: Download top 10 companies
    just sec-process-parallel 2024 # Phase 2: Process in parallel
    just sec-materialize           # Phase 3: Materialize to graph

    # Reset database
    just sec-reset
"""

import argparse
import json
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from robosystems.config import env
from robosystems.config.storage.shared import (
  DataSourceType,
  get_processed_key,
  get_raw_key,
)
from robosystems.logger import logger

# Top companies by market cap (as of 2024)
# Used when --count is specified without --tickers
TOP_COMPANIES_BY_MARKET_CAP = [
  "AAPL",  # Apple - Tech
  "MSFT",  # Microsoft - Tech
  "NVDA",  # NVIDIA - Tech/AI
  "GOOGL",  # Alphabet - Tech
  "AMZN",  # Amazon - Tech/Retail
  "META",  # Meta - Tech
  "BRK-B",  # Berkshire Hathaway - Finance
  "LLY",  # Eli Lilly - Pharma
  "TSM",  # TSMC - Semiconductors
  "AVGO",  # Broadcom - Tech
  "JPM",  # JPMorgan - Finance
  "WMT",  # Walmart - Retail
  "V",  # Visa - Finance
  "XOM",  # Exxon - Energy
  "UNH",  # UnitedHealth - Healthcare
  "MA",  # Mastercard - Finance
  "JNJ",  # Johnson & Johnson - Pharma
  "PG",  # Procter & Gamble - Consumer
  "HD",  # Home Depot - Retail
  "COST",  # Costco - Retail
]

DEFAULT_COMPANY_COUNT = 5
ALL_YEAR_PARTITIONS = ["2019", "2020", "2021", "2022", "2023", "2024", "2025"]

# Default timeouts in seconds (generous for large batch processing)
DEFAULT_DOWNLOAD_TIMEOUT = 7200  # 2 hours per year partition
DEFAULT_MATERIALIZE_TIMEOUT = 14400  # 4 hours for full materialization


def get_top_companies(count: int, use_sec_api: bool = False) -> list[str]:
  """Get top N companies by market cap."""
  if use_sec_api:
    try:
      from robosystems.adapters.sec import SECClient

      client = SECClient()
      companies = client.get_companies()
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
  ) -> str:
    """Create YAML config for Dagster job.

    Args:
        job_type: "download_only" or "materialize"
    """
    if job_type == "materialize":
      # sec_materialize job - ingests all processed data to graph
      # Always rebuilds graph from scratch - incremental not yet supported
      config = {
        "ops": {
          "sec_duckdb_staging": {"config": {}},
          "sec_graph_materialized": {
            "config": {"graph_id": "sec", "ignore_errors": True}
          },
        }
      }
    else:
      # sec_download job: download raw ZIPs only (no processing)
      config = {
        "ops": {
          "sec_companies_list": {"config": {"ticker_filter": tickers}},
          "sec_raw_filings": {
            "config": {
              "skip_existing": skip_existing,
              "form_types": ["10-K", "10-Q"],
              "tickers": tickers,
            }
          },
        }
      }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
      yaml.dump(config, f, default_flow_style=False)
      config_path = f.name

    import os

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
    logger.info("=" * 60)
    logger.info("SEC Pipeline")
    logger.info("=" * 60)
    logger.info(f"Companies: {', '.join(self.tickers)}")
    logger.info(f"Years: {', '.join(self.years)}")
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

    # Phase 1: Download each year partition
    for year in self.years:
      logger.info(f"\n{'=' * 60}")
      logger.info(f"YEAR: {year}")
      logger.info(f"{'=' * 60}")

      if not self.skip_download:
        logger.info(f"\n[DOWNLOAD] Downloading filings for {year}...")
        config_path = self._create_job_config(
          tickers=self.tickers,
          year=year,
          skip_existing=True,
          job_type="download_only",
        )

        result = self.run_stage(
          job_name="sec_download",
          config_path=config_path,
          year=year,
          timeout=self.download_timeout,
        )
        all_results.append(result)

        if result.success:
          logger.info(f"  Complete ({result.duration_seconds:.1f}s)")
        else:
          logger.warning(f"  Issues: {result.error}")

    # Phase 2: Process in parallel
    if not self.skip_download and not self.skip_processing:
      logger.info(f"\n{'=' * 60}")
      logger.info("PROCESSING (Parallel)")
      logger.info(f"{'=' * 60}")
      process_result = self._run_parallel_processing()
      if process_result:
        all_results.append(process_result)

    # Phase 3: DuckDB Staging & Materialization
    if not self.skip_processing:
      logger.info(f"\n{'=' * 60}")
      logger.info("MATERIALIZATION")
      logger.info(f"{'=' * 60}")

      config_path = self._create_job_config(
        tickers=self.tickers,
        year=None,
        job_type="materialize",
      )

      result = self.run_stage(
        job_name="sec_materialize",
        config_path=config_path,
        timeout=self.materialize_timeout,
      )
      all_results.append(result)

      if result.success:
        logger.info(f"  Complete ({result.duration_seconds:.1f}s)")
      else:
        logger.error(f"  Failed: {result.error}")

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

  def _run_parallel_processing(self) -> StageResult | None:
    """Run parallel processing for downloaded filings.

    Discovers unprocessed filings in S3 and triggers parallel Dagster jobs.
    """
    import boto3

    from robosystems.config import env as app_env

    start_time = time.time()

    raw_bucket = app_env.SHARED_RAW_BUCKET
    processed_bucket = app_env.SHARED_PROCESSED_BUCKET

    if not raw_bucket or not processed_bucket:
      logger.error("  Missing SHARED_RAW_BUCKET or SHARED_PROCESSED_BUCKET")
      return None

    # Create S3 client (with LocalStack support)
    s3_kwargs = {"region_name": app_env.AWS_REGION or "us-east-1"}
    if app_env.AWS_ENDPOINT_URL:
      s3_kwargs["endpoint_url"] = app_env.AWS_ENDPOINT_URL
    s3_client = boto3.client("s3", **s3_kwargs)

    # Get SEC prefix from shared_data.py
    sec_prefix = f"{get_raw_key(DataSourceType.SEC)}/"  # "sec/"

    # Discover unprocessed filings
    raw_files = []
    paginator = s3_client.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=raw_bucket, Prefix=sec_prefix):
      for obj in page.get("Contents", []):
        key = obj["Key"]
        if key.endswith(".zip"):
          raw_files.append(key)

    if not raw_files:
      logger.info("  No raw filings found")
      return None

    # Check which need processing
    unprocessed = []
    for raw_key in raw_files:
      # Parse: sec/year=2024/320193/0000320193-24-000081.zip
      parts = raw_key.split("/")
      if len(parts) < 4:
        continue

      year_part = parts[1].replace("year=", "")
      cik = parts[2]
      accession = parts[-1].replace(".zip", "")
      partition_key = f"{year_part}_{cik}_{accession}"

      # Check if already processed using shared_data.py helper
      processed_key = get_processed_key(
        DataSourceType.SEC,
        f"year={year_part}",
        "nodes",
        "Entity",
        f"{cik}_{accession}.parquet",
      )
      try:
        s3_client.head_object(Bucket=processed_bucket, Key=processed_key)
        continue  # Already processed
      except Exception:
        pass

      unprocessed.append(partition_key)

    if not unprocessed:
      logger.info("  All filings already processed")
      return StageResult(
        stage="process_parallel",
        year="all",
        success=True,
        duration_seconds=time.time() - start_time,
      )

    logger.info(f"  Found {len(unprocessed)} unprocessed filings")

    # Register dynamic partitions
    partitions_json = json.dumps(unprocessed)
    register_cmd = [
      "docker",
      "compose",
      "exec",
      "-T",
      "dagster-webserver",
      "python",
      "-c",
      f"""
from dagster import DagsterInstance
instance = DagsterInstance.get()
partitions = {partitions_json}
instance.add_dynamic_partitions(
    partitions_def_name="sec_filings",
    partition_keys=partitions,
)
print(f"Registered {{len(partitions)}} partitions")
""",
    ]

    result = subprocess.run(register_cmd, capture_output=True, text=True)
    if result.returncode != 0:
      logger.error(f"  Failed to register partitions: {result.stderr}")
      return StageResult(
        stage="process_parallel",
        year="all",
        success=False,
        duration_seconds=time.time() - start_time,
        error=f"Failed to register partitions: {result.stderr[:200]}",
      )

    logger.info(f"  {result.stdout.strip()}")

    # Trigger parallel jobs (configurable via env var, default 2)
    concurrency = env.SEC_PARALLEL_CONCURRENCY
    triggered = 0
    failed = 0

    for i in range(0, len(unprocessed), concurrency):
      batch = unprocessed[i : i + concurrency]
      processes = []

      for partition_key in batch:
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
          json.dumps({"dagster/partition": partition_key}),
        ]
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        processes.append((partition_key, proc))

      for partition_key, proc in processes:
        try:
          stdout, stderr = proc.communicate(timeout=600)
          if proc.returncode == 0 or "RUN_SUCCESS" in stdout.decode():
            triggered += 1
            if self.verbose:
              logger.info(f"    [OK] {partition_key}")
          else:
            failed += 1
            logger.warning(f"    [FAIL] {partition_key}")
        except subprocess.TimeoutExpired:
          proc.kill()
          proc.communicate()  # Clean up to prevent zombie processes
          failed += 1
          logger.warning(f"    [TIMEOUT] {partition_key}")

    duration = time.time() - start_time
    logger.info(f"  Processed: {triggered} OK, {failed} failed ({duration:.1f}s)")

    return StageResult(
      stage="process_parallel",
      year="all",
      success=failed == 0,
      duration_seconds=duration,
      metadata={"triggered": triggered, "failed": failed},
      error=f"{failed} filings failed" if failed > 0 else None,
    )


def cmd_run(args):
  """Run pipeline command."""
  # Determine companies
  if args.tickers:
    tickers = [t.upper() for t in args.tickers]
  else:
    tickers = get_top_companies(args.count, use_sec_api=args.from_sec)

  # Determine years
  if args.year:
    years = [args.year]
  elif args.years:
    years = args.years
  else:
    years = ALL_YEAR_PARTITIONS

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

  # Determine years
  if args.year:
    years = [args.year]
  elif args.years:
    years = args.years
  else:
    years = ALL_YEAR_PARTITIONS

  logger.info("=" * 60)
  logger.info("SEC Download Only (Phase 1)")
  logger.info("=" * 60)
  logger.info(f"Companies: {', '.join(tickers)}")
  logger.info(f"Years: {', '.join(years)}")
  logger.info("=" * 60)
  logger.info("After download, enable sec_processing_sensor in Dagster UI")
  logger.info("for parallel processing, then run 'just sec-materialize'")
  logger.info("=" * 60)

  overall_start = time.time()
  all_results = []

  # Create a minimal pipeline for running the download job
  pipeline = SECPipeline(tickers=tickers, years=years, verbose=args.verbose)

  for year in years:
    logger.info(f"\n[DOWNLOAD] Year {year}...")
    config_path = pipeline._create_job_config(
      tickers=tickers,
      year=year,
      skip_existing=True,
      job_type="download_only",
    )

    result = pipeline.run_stage(
      job_name="sec_download",
      config_path=config_path,
      year=year,
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
  logger.info(f"Total years: {len(all_results)}")
  logger.info(f"Successful: {successful}")
  logger.info(f"Failed: {failed}")
  logger.info(f"Duration: {overall_duration:.1f}s ({overall_duration / 60:.1f} min)")

  if args.json:
    print(
      json.dumps(
        {
          "status": "success" if failed == 0 else "partial_failure",
          "total_years": len(all_results),
          "successful": successful,
          "failed": failed,
          "duration_seconds": overall_duration,
          "companies": tickers,
          "years": years,
        },
        indent=2,
      )
    )

  return 0 if failed == 0 else 1


def cmd_materialize(args):
  """Materialize command - ingests all processed parquet files to graph."""
  logger.info("=" * 60)
  logger.info("SEC Materialization (Phase 3)")
  logger.info("=" * 60)
  logger.info("Ingesting all processed parquet files to LadybugDB graph")
  logger.info("=" * 60)

  # Log timeout settings if non-default
  if args.materialize_timeout != DEFAULT_MATERIALIZE_TIMEOUT:
    logger.info(
      f"Materialize timeout: {args.materialize_timeout}s ({args.materialize_timeout / 3600:.1f}h)"
    )

  # Create minimal pipeline just for materialize stage
  pipeline = SECPipeline(
    tickers=[],  # Not used for materialize
    years=[],  # Not used - ingests all available data
    skip_download=True,
    skip_processing=False,
    skip_reset=True,  # Graph rebuild is handled by process_files()
    verbose=args.verbose,
    materialize_timeout=args.materialize_timeout,
  )

  # Run materialize stage directly
  config_path = pipeline._create_job_config(
    tickers=[],
    year=None,
    job_type="materialize",
  )

  result = pipeline.run_stage(
    job_name="sec_materialize",
    config_path=config_path,
    timeout=args.materialize_timeout,
  )

  if result.success:
    logger.info(f"Materialization complete ({result.duration_seconds:.1f}s)")
  else:
    logger.error(f"Materialization failed: {result.error}")

  if args.json:
    print(
      json.dumps(
        {
          "status": "success" if result.success else "failure",
          "duration_seconds": result.duration_seconds,
          "error": result.error,
        },
        indent=2,
      )
    )

  return 0 if result.success else 1


def cmd_process_parallel(args):
  """Process filings in parallel via Dagster dynamic partitions.

  This command:
  1. Lists raw XBRL ZIPs in S3
  2. Checks which don't have corresponding parquet output
  3. Registers dynamic partitions for unprocessed filings
  4. Triggers parallel processing via dagster job execute

  Unlike sec_batch_process which processes sequentially in one task,
  this spawns multiple parallel Dagster runs.
  """
  import boto3

  from robosystems.config import env as app_env

  logger.info("=" * 60)
  logger.info("SEC Parallel Processing (Phase 2)")
  logger.info("=" * 60)

  raw_bucket = app_env.SHARED_RAW_BUCKET
  processed_bucket = app_env.SHARED_PROCESSED_BUCKET

  if not raw_bucket or not processed_bucket:
    logger.error("Missing SHARED_RAW_BUCKET or SHARED_PROCESSED_BUCKET")
    return 1

  # Create S3 client (with LocalStack support)
  s3_kwargs = {"region_name": app_env.AWS_REGION or "us-east-1"}
  if app_env.AWS_ENDPOINT_URL:
    s3_kwargs["endpoint_url"] = app_env.AWS_ENDPOINT_URL
  s3_client = boto3.client("s3", **s3_kwargs)

  # Get SEC prefix from shared_data.py
  sec_prefix = get_raw_key(DataSourceType.SEC)  # "sec"

  # List raw filings
  logger.info(f"Scanning S3 bucket: {raw_bucket}")
  paginator = s3_client.get_paginator("list_objects_v2")

  # Filter by year if specified
  prefix = f"{sec_prefix}/year={args.year}/" if args.year else f"{sec_prefix}/"
  raw_files = []

  for page in paginator.paginate(Bucket=raw_bucket, Prefix=prefix):
    for obj in page.get("Contents", []):
      key = obj["Key"]
      if key.endswith(".zip"):
        raw_files.append(key)

  if not raw_files:
    logger.warning(f"No raw filings found in {raw_bucket}/{prefix}")
    return 0

  logger.info(f"Found {len(raw_files)} raw filings")

  # Check which need processing
  unprocessed = []
  for raw_key in raw_files:
    # Parse: sec/year=2024/320193/0000320193-24-000081.zip
    parts = raw_key.split("/")
    if len(parts) < 4:
      continue

    year_part = parts[1].replace("year=", "")
    cik = parts[2]
    accession = parts[-1].replace(".zip", "")
    partition_key = f"{year_part}_{cik}_{accession}"

    # Check if already processed using shared_data.py helper
    processed_key = get_processed_key(
      DataSourceType.SEC,
      f"year={year_part}",
      "nodes",
      "Entity",
      f"{cik}_{accession}.parquet",
    )
    try:
      s3_client.head_object(Bucket=processed_bucket, Key=processed_key)
      continue  # Already processed
    except Exception:
      pass

    unprocessed.append(partition_key)

  if not unprocessed:
    logger.info("All filings already processed")
    return 0

  logger.info(f"Found {len(unprocessed)} unprocessed filings")

  # Limit if specified
  if args.limit and args.limit > 0:
    unprocessed = unprocessed[: args.limit]
    logger.info(f"Limited to {len(unprocessed)} filings")

  # Register dynamic partitions and trigger jobs
  logger.info(f"\n{'=' * 60}")
  logger.info("TRIGGERING PARALLEL PROCESSING")
  logger.info(f"{'=' * 60}")

  # Use Dagster CLI to trigger runs for each partition
  # First, register the dynamic partitions
  logger.info("Registering dynamic partitions...")

  partitions_json = json.dumps(unprocessed)
  register_cmd = [
    "docker",
    "compose",
    "exec",
    "-T",
    "dagster-webserver",
    "python",
    "-c",
    f"""
from dagster import DagsterInstance
instance = DagsterInstance.get()
partitions = {partitions_json}
instance.add_dynamic_partitions(
    partitions_def_name="sec_filings",
    partition_keys=partitions,
)
print(f"Registered {{len(partitions)}} partitions")
""",
  ]

  result = subprocess.run(register_cmd, capture_output=True, text=True)
  if result.returncode != 0:
    logger.error(f"Failed to register partitions: {result.stderr}")
    return 1
  logger.info(result.stdout.strip())

  # Trigger jobs in parallel (up to concurrency limit)
  concurrency = args.concurrency or 2
  logger.info(f"Triggering jobs with concurrency: {concurrency}")

  triggered = 0
  failed = 0

  # Process in batches to control local concurrency
  for i in range(0, len(unprocessed), concurrency):
    batch = unprocessed[i : i + concurrency]

    processes = []
    for partition_key in batch:
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
        json.dumps({"dagster/partition": partition_key}),
      ]

      # Run in background
      proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
      processes.append((partition_key, proc))

    # Wait for batch to complete
    for partition_key, proc in processes:
      try:
        stdout, stderr = proc.communicate(timeout=600)
        if proc.returncode == 0 or "RUN_SUCCESS" in stdout.decode():
          triggered += 1
          logger.info(f"  [OK] {partition_key}")
        else:
          failed += 1
          logger.warning(f"  [FAIL] {partition_key}: {stderr.decode()[:100]}")
      except subprocess.TimeoutExpired:
        proc.kill()
        proc.communicate()  # Clean up to prevent zombie processes
        failed += 1
        logger.warning(f"  [TIMEOUT] {partition_key}")

  # Summary
  logger.info(f"\n{'=' * 60}")
  logger.info("SUMMARY")
  logger.info(f"{'=' * 60}")
  logger.info(f"Total filings: {len(unprocessed)}")
  logger.info(f"Triggered: {triggered}")
  logger.info(f"Failed: {failed}")

  if args.json:
    print(
      json.dumps(
        {
          "status": "success" if failed == 0 else "partial_failure",
          "total": len(unprocessed),
          "triggered": triggered,
          "failed": failed,
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

  # Materialize command - ingests all processed data to graph
  mat_parser = subparsers.add_parser(
    "materialize", help="Ingest all processed parquet files to LadybugDB graph"
  )
  mat_parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
  mat_parser.add_argument("--json", action="store_true", help="JSON output")
  mat_parser.add_argument(
    "--materialize-timeout",
    type=int,
    default=DEFAULT_MATERIALIZE_TIMEOUT,
    help=f"Timeout in seconds (default: {DEFAULT_MATERIALIZE_TIMEOUT})",
  )

  # Process-parallel command
  parallel_parser = subparsers.add_parser(
    "process-parallel", help="Process filings in parallel (Phase 2 only)"
  )
  parallel_parser.add_argument("--year", type=str, help="Filter to specific year")
  parallel_parser.add_argument(
    "--limit", type=int, default=0, help="Limit number of filings to process"
  )
  parallel_parser.add_argument(
    "--concurrency",
    type=int,
    default=2,
    help="Number of parallel jobs to run locally (default: 2)",
  )
  parallel_parser.add_argument(
    "-v", "--verbose", action="store_true", help="Verbose output"
  )
  parallel_parser.add_argument("--json", action="store_true", help="JSON output")

  args = parser.parse_args()

  if args.command == "run":
    sys.exit(cmd_run(args))
  elif args.command == "reset":
    sys.exit(cmd_reset(args))
  elif args.command == "download":
    sys.exit(cmd_download(args))
  elif args.command == "materialize":
    sys.exit(cmd_materialize(args))
  elif args.command == "process-parallel":
    sys.exit(cmd_process_parallel(args))
  else:
    parser.print_help()
    sys.exit(0)


if __name__ == "__main__":
  main()
