#!/usr/bin/env python3
# type: ignore
"""
SEC Pipeline - XBRL Data Processing via Dagster.

This script manages SEC XBRL data processing through Dagster jobs:
- Download SEC filings from EDGAR
- Process XBRL to parquet
- Stage in DuckDB
- Materialize to LadybugDB graph

Supports both single-company (sec-load) and multi-company (sec-pipeline) modes.

Usage:
    # Single company (like sec-load)
    just sec-load NVDA 2024

    # Top N companies by market cap
    just sec-pipeline              # Top 5, all years
    just sec-pipeline 10           # Top 10, all years
    just sec-pipeline 3 2024       # Top 3, single year

    # Quick test
    just sec-pipeline-quick        # 2 companies, 2024 only

    # Reset database
    just sec-reset

    # Materialize only (skip download/processing)
    just sec-pipeline-materialize
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

from robosystems.logger import logger

# Top companies by market cap (as of 2024)
# Used when --count is specified without --tickers
TOP_COMPANIES_BY_MARKET_CAP = [
    "AAPL",   # Apple - Tech
    "MSFT",   # Microsoft - Tech
    "NVDA",   # NVIDIA - Tech/AI
    "GOOGL",  # Alphabet - Tech
    "AMZN",   # Amazon - Tech/Retail
    "META",   # Meta - Tech
    "BRK-B",  # Berkshire Hathaway - Finance
    "LLY",    # Eli Lilly - Pharma
    "TSM",    # TSMC - Semiconductors
    "AVGO",   # Broadcom - Tech
    "JPM",    # JPMorgan - Finance
    "WMT",    # Walmart - Retail
    "V",      # Visa - Finance
    "XOM",    # Exxon - Energy
    "UNH",    # UnitedHealth - Healthcare
    "MA",     # Mastercard - Finance
    "JNJ",    # Johnson & Johnson - Pharma
    "PG",     # Procter & Gamble - Consumer
    "HD",     # Home Depot - Retail
    "COST",   # Costco - Retail
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
        verbose: bool = False,
        download_timeout: int = DEFAULT_DOWNLOAD_TIMEOUT,
        materialize_timeout: int = DEFAULT_MATERIALIZE_TIMEOUT,
    ):
        self.tickers = [t.upper() for t in tickers]
        self.years = years
        self.skip_download = skip_download
        self.skip_processing = skip_processing
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
        refresh: bool = False,
        rebuild: bool = True,
        job_type: str = "download",
    ) -> str:
        """Create YAML config for Dagster job.

        Args:
            job_type: "download" for sec_download_and_process job, "materialize" for sec_materialize job
        """
        if job_type == "materialize":
            # sec_materialize job only has staging and graph ops
            config = {
                "ops": {
                    "sec_duckdb_staging": {
                        "config": {"rebuild": rebuild, "year_filter": [int(year)] if year else []}
                    },
                    "sec_graph_materialized": {
                        "config": {"graph_id": "sec", "ignore_errors": True, "rebuild": rebuild}
                    },
                }
            }
        else:
            # sec_download_and_process job: download + batch process
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
                    "sec_batch_process": {
                        "config": {
                            "refresh": refresh,
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
            "docker", "compose", "exec", "-T",
            "dagster-webserver",
            "dagster", "job", "execute",
            "-m", "robosystems.dagster",
            "--job", job_name,
            "-c", config_path,
        ]

        if year:
            cmd.extend(["--tags", json.dumps({"dagster/partition": year})])

        if self.verbose:
            logger.info(f"Executing: {' '.join(cmd)}")

        success, stdout, stderr = self._exec_docker(cmd, timeout)
        duration = time.time() - start_time

        error = None
        if not success:
            error = stderr[-500:] if stderr else "Unknown error"

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

        # Reset database first (clean state)
        logger.info("\n[SETUP] Resetting SEC database...")
        if not self._reset_database():
            logger.error("Database reset failed - aborting")
            return {"status": "error", "reason": "Database reset failed"}

        # Process each year partition
        for year in self.years:
            logger.info(f"\n{'='*60}")
            logger.info(f"YEAR: {year}")
            logger.info(f"{'='*60}")

            if not self.skip_download:
                logger.info(f"\n[DOWNLOAD] Downloading filings for {year}...")
                config_path = self._create_job_config(
                    tickers=self.tickers,
                    year=year,
                    skip_existing=True,
                    rebuild=False,
                )

                result = self.run_stage(
                    job_name="sec_download_and_process",
                    config_path=config_path,
                    year=year,
                    timeout=self.download_timeout,
                )
                all_results.append(result)

                if result.success:
                    logger.info(f"  Complete ({result.duration_seconds:.1f}s)")
                else:
                    logger.warning(f"  Issues: {result.error}")

        # DuckDB Staging & Materialization
        if not self.skip_processing:
            logger.info(f"\n{'='*60}")
            logger.info("MATERIALIZATION")
            logger.info(f"{'='*60}")

            config_path = self._create_job_config(
                tickers=self.tickers,
                year=None,
                rebuild=True,
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

        logger.info(f"\n{'='*60}")
        logger.info("SUMMARY")
        logger.info(f"{'='*60}")
        logger.info(f"Total stages: {len(all_results)}")
        logger.info(f"Successful: {successful}")
        logger.info(f"Failed: {failed}")
        logger.info(f"Duration: {overall_duration:.1f}s ({overall_duration/60:.1f} min)")

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

            # Clear S3 if requested
            if clear_s3:
                self._clear_s3_buckets()

            return True

        except Exception as e:
            logger.error(f"Reset failed: {e}")
            return False

    def _clear_s3_buckets(self):
        """Clear LocalStack S3 buckets."""
        buckets = [
            "robosystems-sec-raw",
            "robosystems-sec-processed",
            "robosystems-sec-textblocks",
        ]
        logger.info("  Clearing S3 buckets...")
        for bucket in buckets:
            try:
                cmd = [
                    "aws", "s3", "rm", f"s3://{bucket}",
                    "--recursive", "--endpoint-url", "http://localhost:4566",
                ]
                result = subprocess.run(cmd, capture_output=True, text=True)
                if result.returncode == 0:
                    logger.info(f"    Cleared: {bucket}")
                elif "NoSuchBucket" in result.stderr:
                    logger.debug(f"    Bucket doesn't exist: {bucket}")
            except Exception as e:
                logger.warning(f"    Error clearing {bucket}: {e}")


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
        logger.info(f"Download timeout: {args.download_timeout}s ({args.download_timeout/3600:.1f}h)")
    if args.materialize_timeout != DEFAULT_MATERIALIZE_TIMEOUT:
        logger.info(f"Materialize timeout: {args.materialize_timeout}s ({args.materialize_timeout/3600:.1f}h)")

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


def cmd_materialize(args):
    """Materialize only command."""
    # Determine companies
    if args.tickers:
        tickers = [t.upper() for t in args.tickers]
    else:
        tickers = get_top_companies(args.count)

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
        skip_download=True,
        skip_processing=False,
        verbose=args.verbose,
        materialize_timeout=args.materialize_timeout,
    )

    # Log timeout settings if non-default
    if args.materialize_timeout != DEFAULT_MATERIALIZE_TIMEOUT:
        logger.info(f"Materialize timeout: {args.materialize_timeout}s ({args.materialize_timeout/3600:.1f}h)")

    results = pipeline.run()

    if args.json:
        print(json.dumps(results, indent=2))

    return 0 if results.get("status") == "success" else 1


def main():
    parser = argparse.ArgumentParser(
        description="SEC Pipeline - XBRL Data Processing via Dagster",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="command", help="Commands")

    # Run command
    run_parser = subparsers.add_parser("run", help="Run SEC pipeline")
    run_parser.add_argument("-n", "--count", type=int, default=DEFAULT_COMPANY_COUNT,
                           help=f"Number of top companies (default: {DEFAULT_COMPANY_COUNT})")
    run_parser.add_argument("--tickers", nargs="+", help="Specific tickers (overrides --count)")
    run_parser.add_argument("--from-sec", action="store_true", help="Fetch companies from SEC API")
    run_parser.add_argument("--year", type=str, help="Single year to process")
    run_parser.add_argument("--years", nargs="+", help="Specific years to process")
    run_parser.add_argument("--skip-download", action="store_true", help="Skip download stage")
    run_parser.add_argument("--skip-processing", action="store_true", help="Skip processing stage")
    run_parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    run_parser.add_argument("--json", action="store_true", help="JSON output")
    run_parser.add_argument("--download-timeout", type=int, default=DEFAULT_DOWNLOAD_TIMEOUT,
                           help=f"Timeout per download stage in seconds (default: {DEFAULT_DOWNLOAD_TIMEOUT})")
    run_parser.add_argument("--materialize-timeout", type=int, default=DEFAULT_MATERIALIZE_TIMEOUT,
                           help=f"Timeout for materialization in seconds (default: {DEFAULT_MATERIALIZE_TIMEOUT})")

    # Reset command
    reset_parser = subparsers.add_parser("reset", help="Reset SEC database")
    reset_parser.add_argument("--clear-s3", action="store_true", help="Also clear S3 buckets")

    # Materialize command
    mat_parser = subparsers.add_parser("materialize", help="Materialize only (skip download)")
    mat_parser.add_argument("-n", "--count", type=int, default=DEFAULT_COMPANY_COUNT,
                           help=f"Number of top companies (default: {DEFAULT_COMPANY_COUNT})")
    mat_parser.add_argument("--tickers", nargs="+", help="Specific tickers")
    mat_parser.add_argument("--year", type=str, help="Single year")
    mat_parser.add_argument("--years", nargs="+", help="Specific years")
    mat_parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    mat_parser.add_argument("--json", action="store_true", help="JSON output")
    mat_parser.add_argument("--materialize-timeout", type=int, default=DEFAULT_MATERIALIZE_TIMEOUT,
                           help=f"Timeout for materialization in seconds (default: {DEFAULT_MATERIALIZE_TIMEOUT})")

    args = parser.parse_args()

    if args.command == "run":
        sys.exit(cmd_run(args))
    elif args.command == "reset":
        sys.exit(cmd_reset(args))
    elif args.command == "materialize":
        sys.exit(cmd_materialize(args))
    else:
        parser.print_help()
        sys.exit(0)


if __name__ == "__main__":
    main()
