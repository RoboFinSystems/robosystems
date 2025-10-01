#!/usr/bin/env python3
# type: ignore
"""
SEC Local Pipeline - Testing and Development Tool

A unified local pipeline for SEC data management focused on:
- Single company loading by ticker symbol
- Database reset with proper schema creation
- Health checks and data validation
- Local testing and development

This replaces the old sec_pipeline.py, reset_sec_pipeline.py, and sec_health_check.py
"""

import argparse
import sys
import subprocess

# No typing imports needed
from datetime import datetime

from robosystems.logger import logger


class SECLocalPipeline:
  """Local SEC pipeline for testing and development."""

  def __init__(self):
    """Initialize the local pipeline."""
    self.sec_database = "sec"

  def reset_database(self, clear_s3: bool = True) -> bool:
    """
    Reset SEC database with proper schema creation.

    Args:
        clear_s3: Whether to also clear S3 buckets

    Returns:
        True if successful, False otherwise
    """
    logger.info("🔄 Starting SEC database reset...")

    # Use the maintenance task to reset the database through the Kuzu API
    from robosystems.tasks.sec_xbrl.maintenance import reset_sec_database

    # Call as a Celery task (even locally, we use the same pattern as production)
    task = reset_sec_database.apply_async(kwargs={"confirm": True})

    logger.info("⏳ Waiting for database reset to complete...")
    try:
      result = task.get(timeout=60)  # 1 minute timeout

      if result.get("status") == "success":
        logger.info("✅ Database reset successfully")
        logger.info(f"  Node types: {result.get('node_types', 0)}")
        logger.info(f"  Relationship types: {result.get('relationship_types', 0)}")
      else:
        logger.error(f"Database reset failed: {result.get('error', 'Unknown error')}")
        return False

    except Exception as e:
      logger.error(f"Failed to reset database: {e}")
      return False

    # Clear S3 buckets if requested
    if clear_s3:
      logger.info("Clearing S3 buckets...")
      self._clear_s3_buckets()

    logger.info("✅ SEC database reset complete")
    return True

  def _clear_s3_buckets(self):
    """Clear LocalStack S3 buckets."""
    buckets = [
      "robosystems-sec-raw",
      "robosystems-sec-processed",
      "robosystems-sec-textblocks",
    ]

    for bucket in buckets:
      try:
        cmd = [
          "aws",
          "s3",
          "rm",
          f"s3://{bucket}",
          "--recursive",
          "--endpoint-url",
          "http://localhost:4566",
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
          logger.info(f"  Cleared: {bucket}")
        elif "NoSuchBucket" in result.stderr:
          logger.debug(f"  Bucket doesn't exist: {bucket}")
        else:
          logger.warning(f"  Failed to clear {bucket}: {result.stderr}")
      except Exception as e:
        logger.warning(f"  Error clearing {bucket}: {e}")

  def load_company(self, ticker: str, year: int) -> bool:
    """
    Load a single company's data by ticker symbol using orchestrated Celery tasks.

    Pipeline phases:
    1. Download - Fetch XBRL files from SEC
    2. Process - Convert XBRL to parquet format
    3. Consolidate - Combine small parquet files into larger ones
    4. Ingest - Load consolidated files into Kuzu database

    Note: Consolidation processes ALL available files across all years
    for optimal Kuzu ingestion performance.

    Args:
        ticker: Company ticker symbol (e.g., "NVDA", "AAPL")
        year: Year to load data for

    Returns:
        True if successful, False otherwise
    """
    logger.info(f"📊 Loading {ticker} data for {year} using orchestrated pipeline...")

    try:
      # Use the orchestration tasks just like production
      from robosystems.tasks.sec_xbrl.orchestration import (
        plan_phased_processing,
        start_phase,
      )
      from robosystems.adapters.sec import SECClient

      # Get CIK from ticker for filtering
      sec_client = SECClient()
      companies_df = sec_client.get_companies_df()
      company = companies_df[companies_df["ticker"] == ticker.upper()]
      if company.empty:
        logger.error(f"Company not found: {ticker}")
        return False

      cik = str(company.iloc[0]["cik_str"])
      company_name = company.iloc[0]["title"]
      logger.info(f"Found: {company_name} (CIK: {cik})")

      # Step 1: Create a plan for just this company and one year
      logger.info("Creating processing plan...")
      plan_task = plan_phased_processing.apply_async(
        kwargs={
          "start_year": year,
          "end_year": year,  # Single year constraint
          "cik_filter": cik,  # Filter to specific CIK
        }
      )

      plan_result = plan_task.get(timeout=60)
      if plan_result.get("status") != "success":
        logger.error(f"Failed to create plan: {plan_result.get('error')}")
        return False

      logger.info(f"✅ Plan created for {company_name} ({year})")

      # Step 2: Run download phase
      logger.info("Starting download phase...")
      download_task = start_phase.apply_async(kwargs={"phase": "download"})

      download_result = download_task.get(timeout=120)
      if download_result.get("status") != "started":
        logger.error(f"Failed to start download: {download_result.get('error')}")
        return False

      # Wait for downloads to complete
      import time
      from robosystems.config import env

      # In dev environment, use minimal delays for faster iteration
      wait_time = 0.5 if env.is_development() else 5
      time.sleep(wait_time)  # Give it a moment to process

      # Step 3: Run process phase
      logger.info("Starting processing phase...")
      process_task = start_phase.apply_async(kwargs={"phase": "process"})

      process_result = process_task.get(timeout=120)
      if process_result.get("status") != "started":
        logger.error(f"Failed to start processing: {process_result.get('error')}")
        return False

      # Wait for processing
      time.sleep(wait_time)

      # Step 3: Run consolidation phase (consolidates all files across years)
      logger.info("Starting consolidation phase...")
      consolidate_task = start_phase.apply_async(kwargs={"phase": "consolidate"})

      consolidate_result = consolidate_task.get(timeout=120)
      if consolidate_result.get("status") != "started":
        logger.error(
          f"Failed to start consolidation: {consolidate_result.get('error')}"
        )
        return False

      # Wait for consolidation tasks to complete
      # The consolidation phase returns a job_id for the group of tasks
      consolidation_job_id = consolidate_result.get("job_id")

      # Define wait time for after ingestion starts
      ingestion_wait = 5 if env.is_development() else 30

      if consolidation_job_id:
        from celery.result import GroupResult

        logger.info("Waiting for all consolidation tasks to complete...")
        max_wait = 60 if env.is_development() else 300  # 1 min dev, 5 min prod
        check_interval = 2 if env.is_development() else 5
        waited = 0

        while waited < max_wait:
          # Check if all tasks in the group are complete
          group_result = GroupResult.restore(consolidation_job_id)
          if group_result and group_result.ready():
            logger.info("✅ All consolidation tasks completed")
            break

          time.sleep(check_interval)
          waited += check_interval

          # Log progress every 10 seconds
          if waited % 10 == 0:
            if group_result:
              completed = sum(1 for r in group_result.results if r.ready())
              total = len(group_result.results)
              logger.info(
                f"Consolidation progress: {completed}/{total} tasks complete ({waited}s)"
              )
            else:
              logger.debug(f"Still waiting for consolidation... ({waited}s)")

        if waited >= max_wait:
          logger.warning(
            f"Consolidation may not be complete after {max_wait}s, proceeding anyway"
          )
      else:
        # Fallback to fixed wait if no job ID
        consolidation_wait = 10 if env.is_development() else 30
        logger.info(
          f"No job ID found, waiting {consolidation_wait}s for consolidation to complete..."
        )
        time.sleep(consolidation_wait)

      # Step 4: Run ingestion phase (reads from consolidated files)
      logger.info("Starting ingestion phase...")
      ingest_task = start_phase.apply_async(kwargs={"phase": "ingest"})

      ingest_result = ingest_task.get(timeout=300)
      if ingest_result.get("status") != "started":
        logger.error(f"Failed to start ingestion: {ingest_result.get('error')}")
        return False

      # Wait for ingestion to complete
      time.sleep(ingestion_wait)

      logger.info(f"✅ Successfully loaded {ticker} data for {year}")
      return True

    except Exception as e:
      logger.error(f"Failed to load company: {e}")
      import traceback

      traceback.print_exc()
      return False


def main():
  """Main entry point for local SEC pipeline."""

  parser = argparse.ArgumentParser(
    description="SEC Local Pipeline - Testing and Development Tool",
    formatter_class=argparse.RawDescriptionHelpFormatter,
    epilog="""
Examples:
  # Reset database and start fresh
  %(prog)s reset

  # Load NVIDIA data for 2024
  %(prog)s load --ticker NVDA --year 2024

  # Load Apple data for 2023
  %(prog)s load --ticker AAPL --year 2023

  # Full reset and load NVIDIA
  %(prog)s reset && %(prog)s load --ticker NVDA --year 2024
""",
  )

  subparsers = parser.add_subparsers(dest="command", help="Commands")

  # Reset command
  reset_parser = subparsers.add_parser("reset", help="Reset SEC database")
  reset_parser.add_argument(
    "--keep-s3", action="store_true", help="Keep S3 data (only reset database)"
  )

  # Load command
  load_parser = subparsers.add_parser("load", help="Load company data by ticker")
  load_parser.add_argument(
    "--ticker", required=True, help="Company ticker symbol (e.g., NVDA, AAPL)"
  )
  load_parser.add_argument(
    "--year",
    type=int,
    default=datetime.now().year - 1,
    help=f"Year to load (default: {datetime.now().year - 1})",
  )

  args = parser.parse_args()

  if not args.command:
    parser.print_help()
    return

  # Initialize pipeline
  pipeline = SECLocalPipeline()

  # Execute command
  if args.command == "reset":
    success = pipeline.reset_database(clear_s3=not args.keep_s3)
    sys.exit(0 if success else 1)

  elif args.command == "load":
    success = pipeline.load_company(ticker=args.ticker, year=args.year)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
  main()
