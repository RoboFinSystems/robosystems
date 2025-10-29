"""
SEC XBRL Filings Pipeline - Multi-Stage Architecture

This module implements a multi-stage pipeline for processing SEC XBRL data.

Stage 1: Raw Data Collection (by year)
  - Download XBRL ZIP files from SEC API
  - Store in S3 raw bucket with year partitioning
  - Cost-controlled: only collect years we're ready to process

Stage 2: Processing (by year)
  - Process raw XBRL ZIP files to parquet format
  - Store in S3 processed bucket with year partitioning
  - Uses RoboLedger schema for standardized output

Stage 3: Ingestion (by year)
  - Ingest processed parquet files into Kuzu
  - Year-by-year ingestion for controlled batch processing
  - Resumable if interrupted

Benefits:
- Cost control (only store what we need)
- Resumability (each stage independent)
- Parallelization (multiple years simultaneously)
- Clear audit trail (raw → processed → ingested)
- Idempotency with refresh override
- Raw file storage for reprocessing
"""

import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional
import boto3
import redis
import requests
from retrying import retry

from ...adapters.sec import SECClient, SEC_HEADERS
from ...config import env
from ...logger import logger
from ...processors.xbrl_graph import XBRLGraphProcessor


class SECXBRLPipeline:
  """
  Production-ready staged SEC XBRL processing pipeline.

  This pipeline separates data collection, processing, and ingestion
  for better control, cost management, and resumability.
  """

  def __init__(self, pipeline_run_id: str):
    """
    Initialize staged SEC pipeline.

    Args:
        pipeline_run_id: Unique identifier for this pipeline run
    """
    self.pipeline_run_id = pipeline_run_id
    self.pipeline_type = "sec_staged"

    # Initialize Redis for tracking using pipeline tracking database from registry
    # Use factory method to handle SSL params correctly

    # Initialize Redis with connection retry and error handling
    self.redis_client = self._init_redis_client()
    self.redis_key = f"pipeline:{self.pipeline_type}:{pipeline_run_id}"
    self.ttl = 86400 * 7  # 7 days for staged pipeline

    # Initialize service clients
    self.sec_client = SECClient()

    # Initialize S3 client
    self.s3_client = self._init_s3_client()

    # S3 bucket configuration with year partitioning
    self.raw_bucket = env.SEC_RAW_BUCKET
    self.processed_bucket = env.SEC_PROCESSED_BUCKET

    logger.info(f"Initialized SEC Staged Pipeline: {pipeline_run_id}")

  @retry(
    stop_max_attempt_number=3,
    wait_fixed=2000,  # Wait 2 seconds between retries
    retry_on_exception=lambda e: isinstance(e, redis.ConnectionError),
  )
  def _init_redis_client(self) -> redis.Redis:
    """
    Initialize Redis client with connection retry and error handling.

    Ensures Redis is available before proceeding with pipeline operations.
    """
    from robosystems.config.valkey_registry import ValkeyDatabase, create_redis_client

    try:
      # Use factory method to handle SSL params correctly
      client = create_redis_client(
        ValkeyDatabase.PIPELINE_TRACKING, decode_responses=True
      )
      # Test connection
      client.ping()
      logger.info("Successfully connected to Redis for pipeline tracking")
      return client
    except redis.ConnectionError as e:
      logger.error(f"Failed to connect to Redis: {e}")
      raise
    except Exception as e:
      logger.error(f"Unexpected error connecting to Redis: {e}")
      raise redis.ConnectionError(f"Redis initialization failed: {e}")

  # STAGE 1: Raw Data Collection
  def collect_raw_data_by_year(
    self,
    year: int,
    max_companies: Optional[int] = None,
    max_filings_per_entity: Optional[int] = None,
    storage_class: str = "STANDARD",
  ) -> Dict:
    """
    Stage 1: Collect raw XBRL ZIP files for a specific year.

    Args:
        year: Year to collect (e.g., 2025)
        max_companies: Maximum companies to process
        max_filings_per_entity: Maximum filings per entity
        storage_class: S3 storage class (STANDARD, IA, GLACIER, etc.)

    Returns:
        Dict with collection results
    """
    logger.info(f"Starting Stage 1: Raw data collection for year {year}")

    # Initialize tracking
    stage_key = f"{self.redis_key}:stage1:year{year}"

    try:
      stage_data = {
        "stage": "raw_collection",
        "year": year,
        "status": "running",
        "started_at": datetime.now(timezone.utc).isoformat(),
        "max_companies": max_companies or 0,
        "max_filings_per_entity": max_filings_per_entity or 0,
        "storage_class": storage_class,
        "files_collected": 0,
        "companies_processed": 0,
      }

      self.redis_client.hset(stage_key, mapping=stage_data)
      self.redis_client.expire(stage_key, self.ttl)

      # Discover companies
      companies = self._discover_companies(max_companies)
      if not companies:
        raise Exception("No companies discovered")

      # Discover and collect filings for the specified year
      collected_files = []
      companies_processed = 0
      rate_limit_errors = 0
      max_rate_limit_errors = 5  # Circuit breaker threshold

      for entity in companies:
        cik = str(entity.get("cik_str", entity.get("cik", "")))

        try:
          # Get filings for this entity in the specified year
          entity_filings = self._discover_entity_filings_by_year(
            cik, year, max_filings_per_entity
          )

          # Collect raw ZIP files for each filing
          filings_skipped = 0
          for filing in entity_filings:
            collected_file = self._collect_raw_filing(cik, filing, year, storage_class)
            if collected_file:
              collected_files.append(collected_file)
              # Reset rate limit counter on successful download
              rate_limit_errors = max(0, rate_limit_errors - 1)
            elif collected_file is None:
              # None indicates a skip (likely rate limited)
              filings_skipped += 1
              rate_limit_errors += 1

              # Circuit breaker check
              if rate_limit_errors >= max_rate_limit_errors:
                logger.error(
                  f"Hit rate limit threshold ({max_rate_limit_errors}), stopping collection"
                )
                logger.info(
                  f"Successfully collected {len(collected_files)} files before rate limiting"
                )
                break

          if filings_skipped > 0:
            logger.info(
              f"Skipped {filings_skipped} filings for {cik} due to rate limits"
            )

          companies_processed += 1

          # Update progress
          self.redis_client.hset(stage_key, "companies_processed", companies_processed)
          self.redis_client.hset(stage_key, "files_collected", len(collected_files))

          # Break outer loop if circuit breaker triggered
          if rate_limit_errors >= max_rate_limit_errors:
            break

        except Exception as e:
          logger.error(f"Failed to collect filings for entity {cik}: {e}")
          continue

      # Mark stage complete
      stage_data.update(
        {
          "status": "completed",
          "completed_at": datetime.now(timezone.utc).isoformat(),
          "files_collected": len(collected_files),
          "companies_processed": companies_processed,
        }
      )
      self.redis_client.hset(stage_key, mapping=stage_data)

      logger.info(
        f"Stage 1 complete: Collected {len(collected_files)} files "
        f"from {companies_processed} companies for year {year}"
      )

      return {
        "stage": "raw_collection",
        "status": "completed",
        "year": year,
        "files_collected": len(collected_files),
        "companies_processed": companies_processed,
        "collected_files": collected_files,
      }

    except Exception as e:
      logger.error(f"Stage 1 failed for year {year}: {e}")
      self.redis_client.hset(
        stage_key,
        mapping={
          "status": "failed",
          "error": str(e),
          "failed_at": datetime.now(timezone.utc).isoformat(),
        },
      )
      raise

  # STAGE 2: Processing
  def process_year_data(
    self, year: int, refresh: bool = False, batch_size: int = 100
  ) -> Dict:
    """
    Stage 2: Process raw XBRL files for a year into parquet format.

    Args:
        year: Year to process
        refresh: Force reprocessing even if parquet files exist
        batch_size: Number of files to process per batch

    Returns:
        Dict with processing results
    """
    logger.info(f"Starting Stage 2: Processing year {year} data (refresh={refresh})")

    # Initialize tracking
    stage_key = f"{self.redis_key}:stage2:year{year}"

    try:
      # Get list of raw files for this year
      raw_files = self._list_raw_files_by_year(year)

      if not raw_files:
        logger.warning(f"No raw files found for year {year}")
        return {
          "stage": "processing",
          "status": "completed",
          "year": year,
          "message": "No raw files to process",
        }

      stage_data = {
        "stage": "processing",
        "year": year,
        "status": "running",
        "started_at": datetime.now(timezone.utc).isoformat(),
        "total_files": len(raw_files),
        "processed_files": 0,
        "skipped_files": 0,
        "failed_files": 0,
        "refresh": str(refresh),  # Convert boolean to string for Redis
        "batch_size": batch_size,
      }

      self.redis_client.hset(stage_key, mapping=stage_data)
      self.redis_client.expire(stage_key, self.ttl)

      # Process files in batches using existing XBRL pipeline
      xbrl_pipeline = SECXBRLPipeline(f"{self.pipeline_run_id}_processing_year{year}")

      processed_files = 0
      skipped_files = 0
      failed_files = 0

      for i in range(0, len(raw_files), batch_size):
        batch_files = raw_files[i : i + batch_size]
        logger.info(f"Processing batch {i // batch_size + 1}: {len(batch_files)} files")

        for raw_file_info in batch_files:
          try:
            result = self._process_single_raw_file(
              raw_file_info, year, xbrl_pipeline, refresh
            )

            if result["status"] == "completed":
              processed_files += 1
            elif result["status"] == "skipped":
              skipped_files += 1
            else:
              failed_files += 1

          except Exception as e:
            logger.error(f"Failed to process {raw_file_info}: {e}")
            failed_files += 1

        # Update progress after each batch
        self.redis_client.hset(stage_key, "processed_files", processed_files)
        self.redis_client.hset(stage_key, "skipped_files", skipped_files)
        self.redis_client.hset(stage_key, "failed_files", failed_files)

      # Mark stage complete
      stage_data.update(
        {
          "status": "completed",
          "completed_at": datetime.now(timezone.utc).isoformat(),
          "processed_files": processed_files,
          "skipped_files": skipped_files,
          "failed_files": failed_files,
        }
      )
      self.redis_client.hset(stage_key, mapping=stage_data)

      logger.info(
        f"Stage 2 complete: Processed {processed_files} files, "
        f"skipped {skipped_files}, failed {failed_files} for year {year}"
      )

      return {
        "stage": "processing",
        "status": "completed",
        "year": year,
        "processed_files": processed_files,
        "skipped_files": skipped_files,
        "failed_files": failed_files,
      }

    except Exception as e:
      logger.error(f"Stage 2 failed for year {year}: {e}")
      self.redis_client.hset(
        stage_key,
        mapping={
          "status": "failed",
          "error": str(e),
          "failed_at": datetime.now(timezone.utc).isoformat(),
        },
      )
      raise

  # STAGE 3: Ingestion - DEPRECATED
  # This method is no longer used. The DuckDB-based ingestion pattern
  # (robosystems/tasks/sec_xbrl/duckdb_ingestion.py) is now the default.
  # The consolidation-based approach has been replaced.

  # Helper methods
  def _discover_companies(self, max_companies: Optional[int] = None) -> List[Dict]:
    """Discover companies using existing SEC client."""
    companies_df = self.sec_client.get_companies_df()
    if max_companies:
      companies_df = companies_df.head(max_companies)
    return companies_df.to_dict("records")

  def _discover_entity_filings_by_year(
    self, cik: str, year: int, max_filings: Optional[int] = None
  ) -> List[Dict]:
    """
    Discover filings for an entity in a specific year and store submissions snapshot.

    This method collects and preserves a complete snapshot of the entity's
    submissions metadata at this point in time, including all historical filings
    across all pages. This snapshot is valuable for:
    - Historical analysis of how company metadata evolves
    - Offline processing without hitting SEC API limits
    - Audit trails of what data was available at collection time
    """
    try:
      # First check if we have a recent snapshot (within 24 hours)
      recent_snapshot = self._get_recent_snapshot_if_valid(cik, hours=24)

      if recent_snapshot:
        logger.info(
          f"Using recent entity snapshot for CIK {cik} (less than 24 hours old)"
        )
        submissions_data = recent_snapshot

        # Process the existing snapshot into DataFrame for filtering
        import pandas as pd

        # Reconstruct DataFrame from snapshot data
        filings_df = pd.DataFrame(submissions_data["filings"]["recent"])

        # Add additional pages if they exist
        if "additional_pages" in submissions_data:
          for page_data in submissions_data["additional_pages"]:
            if isinstance(page_data, list):
              df = pd.DataFrame(page_data)
              filings_df = pd.concat([filings_df, df], ignore_index=True)

        submissions_df = filings_df
        bool_cols = ["isXBRL", "isInlineXBRL"]
        for col in bool_cols:
          if col in submissions_df.columns:
            submissions_df[col] = submissions_df[col].astype(bool)

      else:
        # No recent snapshot, fetch from SEC API
        logger.info(f"No recent snapshot found for CIK {cik}, fetching from SEC API")

        try:
          entity_sec_client = SECClient(cik=cik)

          # Fetch all submissions data (recent + all pages)
          logger.info(f"Fetching complete submissions data for CIK {cik}")
          submissions_data = entity_sec_client.get_submissions()

          # Also fetch additional pages if they exist
          if "filings" in submissions_data and "files" in submissions_data["filings"]:
            logger.info(
              f"Found {len(submissions_data['filings']['files'])} additional submission pages"
            )
            additional_pages = []
            for file_ref in submissions_data["filings"]["files"]:
              logger.debug(f"Fetching additional page: {file_ref['name']}")
              additional_data = entity_sec_client.get_submissions(file_ref["name"])
              additional_pages.append(additional_data)

            # Store references to additional pages in the main data
            submissions_data["additional_pages"] = additional_pages

          # Store the complete submissions snapshot to S3 with timestamp
          self._store_entity_submissions_snapshot(cik, year, submissions_data)

          # Now process into DataFrame for filtering
          submissions_df = entity_sec_client.submissions_df()

        except (requests.HTTPError, ValueError) as e:
          if "rate limit" in str(e).lower() or "invalid json" in str(e).lower():
            logger.warning(f"Rate limited when fetching submissions for {cik}: {e}")
            # Try to use any existing snapshot as fallback
            fallback_snapshot = self._load_entity_submissions_snapshot(cik)
            if fallback_snapshot:
              logger.info(f"Using fallback snapshot for {cik} due to rate limiting")
              return self._process_snapshot_to_filings(
                fallback_snapshot, year, max_filings
              )
            else:
              logger.error(f"No fallback snapshot available for {cik}")
              return []
          raise

      # Filter for XBRL filings in the specified year
      xbrl_filings = submissions_df[submissions_df["isXBRL"]]
      xbrl_filings = xbrl_filings[xbrl_filings.form.isin(["10-K", "10-Q"])]

      # Filter by year
      import pandas as pd

      xbrl_filings["filingDate"] = pd.to_datetime(
        xbrl_filings["filingDate"], errors="coerce"
      )
      year_filings = xbrl_filings[xbrl_filings["filingDate"].dt.year == year]

      if max_filings:
        year_filings = year_filings.head(max_filings)

      return year_filings.to_dict("records")  # type: ignore[arg-type]

    except Exception as e:
      logger.error(f"Failed to discover filings for {cik} in year {year}: {e}")
      return []

  def _get_recent_snapshot_if_valid(self, cik: str, hours: int = 24) -> Optional[Dict]:
    """
    Get entity snapshot if it was created within the specified hours.

    Args:
        cik: Company CIK
        hours: Maximum age in hours for snapshot to be considered valid

    Returns:
        Snapshot data if recent enough, None otherwise
    """
    try:
      # List all submissions files for this CIK
      prefix = "raw/year="

      response = self.s3_client.list_objects_v2(
        Bucket=self.raw_bucket, Prefix=prefix, MaxKeys=1000
      )

      if "Contents" not in response:
        return None

      # Find all submissions files for this CIK
      submissions_files = []
      for obj in response["Contents"]:
        key = obj["Key"]
        if f"/{cik}/submissions_" in key and key.endswith(".json"):
          submissions_files.append({"key": key, "last_modified": obj["LastModified"]})

      if not submissions_files:
        return None

      # Sort by last modified date and get the most recent
      submissions_files.sort(key=lambda x: x["last_modified"], reverse=True)
      latest_file = submissions_files[0]

      # Check if it's recent enough
      from datetime import timedelta

      age_limit = datetime.now(timezone.utc) - timedelta(hours=hours)

      if latest_file["last_modified"] < age_limit:
        logger.debug(
          f"Latest snapshot for {cik} is too old: {latest_file['last_modified']}"
        )
        return None

      logger.info(f"Found recent snapshot for {cik}: {latest_file['key']}")

      # Download and parse the JSON
      response = self.s3_client.get_object(
        Bucket=self.raw_bucket, Key=latest_file["key"]
      )

      import json

      submissions_data = json.loads(response["Body"].read().decode("utf-8"))

      return submissions_data

    except Exception as e:
      logger.error(f"Failed to check recent snapshot for {cik}: {e}")
      return None

  def _process_snapshot_to_filings(
    self, snapshot_data: Dict, year: int, max_filings: Optional[int] = None
  ) -> List[Dict]:
    """
    Process a submissions snapshot into filtered filings for a specific year.

    Args:
        snapshot_data: Entity submissions snapshot
        year: Year to filter for
        max_filings: Maximum number of filings to return

    Returns:
        List of filing dictionaries
    """
    try:
      import pandas as pd

      # Reconstruct DataFrame from snapshot data
      filings_df = pd.DataFrame(snapshot_data["filings"]["recent"])

      # Add additional pages if they exist
      if "additional_pages" in snapshot_data:
        for page_data in snapshot_data["additional_pages"]:
          if isinstance(page_data, list):
            df = pd.DataFrame(page_data)
            filings_df = pd.concat([filings_df, df], ignore_index=True)

      # Convert boolean columns
      bool_cols = ["isXBRL", "isInlineXBRL"]
      for col in bool_cols:
        if col in filings_df.columns:
          filings_df[col] = filings_df[col].astype(bool)

      # Filter for XBRL filings
      xbrl_filings = filings_df[filings_df["isXBRL"]]
      xbrl_filings = xbrl_filings[xbrl_filings.form.isin(["10-K", "10-Q"])]

      # Filter by year
      xbrl_filings["filingDate"] = pd.to_datetime(
        xbrl_filings["filingDate"], errors="coerce"
      )
      year_filings = xbrl_filings[xbrl_filings["filingDate"].dt.year == year]

      if max_filings:
        year_filings = year_filings.head(max_filings)

      return year_filings.to_dict("records")  # type: ignore[arg-type]

    except Exception as e:
      logger.error(f"Failed to process snapshot to filings: {e}")
      return []

  def _load_entity_submissions_snapshot(self, cik: str) -> Optional[Dict]:
    """
    Load the most recent entity submissions snapshot for a CIK.

    First tries direct access to latest submissions cache, then falls back
    to searching all timestamped versions across years.

    Args:
        cik: Company CIK

    Returns:
        Entity submissions snapshot data or None if not found
    """
    try:
      # First try direct access to latest submissions cache
      latest_key = f"submissions/{cik}/latest.json"
      try:
        logger.debug(f"Trying direct access to latest submissions: {latest_key}")
        response = self.s3_client.get_object(Bucket=self.raw_bucket, Key=latest_key)
        import json

        submissions_data = json.loads(response["Body"].read().decode("utf-8"))
        logger.info(f"Found latest submissions cache: {latest_key}")
        return submissions_data
      except Exception as e:
        logger.debug(f"Latest submissions cache not found: {e}")

      # Fallback: List all submissions files for this CIK across all years
      # We'll look for the most recent one
      logger.info(f"Falling back to timestamped submissions search for CIK {cik}")
      prefix = "raw/year="

      # Find all submissions files for this CIK with pagination support
      submissions_files = []
      continuation_token = None

      while True:
        # List objects with the CIK in the path
        list_params = {"Bucket": self.raw_bucket, "Prefix": prefix, "MaxKeys": 1000}
        if continuation_token:
          list_params["ContinuationToken"] = continuation_token

        response = self.s3_client.list_objects_v2(**list_params)

        if "Contents" not in response:
          break

        # Find submissions files for this CIK in this batch
        for obj in response["Contents"]:
          key = obj["Key"]
          # Match pattern: raw/year=YYYY/CIK/submissions_TIMESTAMP.json
          if f"/{cik}/submissions_" in key and key.endswith(".json"):
            submissions_files.append({"key": key, "last_modified": obj["LastModified"]})

        # Check if there are more results to fetch
        if not response.get("IsTruncated", False):
          break
        continuation_token = response.get("NextContinuationToken")
        if not continuation_token:
          break

      if not submissions_files:
        return None

      # Sort by last modified date and get the most recent
      submissions_files.sort(key=lambda x: x["last_modified"], reverse=True)
      latest_file = submissions_files[0]

      logger.info(f"Loading entity submissions snapshot from: {latest_file['key']}")

      # Download and parse the JSON
      response = self.s3_client.get_object(
        Bucket=self.raw_bucket, Key=latest_file["key"]
      )

      import json

      submissions_data = json.loads(response["Body"].read().decode("utf-8"))

      return submissions_data

    except Exception as e:
      logger.error(f"Failed to load entity submissions snapshot for {cik}: {e}")
      return None

  def _store_entity_submissions_snapshot(
    self, cik: str, year: int, submissions_data: Dict
  ) -> Optional[str]:
    """
    Store entity submissions snapshot to S3 with proper versioning.

    Submissions are stored at the root level (not year-partitioned) since they
    contain cumulative data spanning all years.

    Args:
        cik: Company CIK
        year: Year being processed (kept for compatibility, not used in path)
        submissions_data: Complete submissions data including all pages

    Returns:
        S3 key where data was stored
    """
    try:
      # Create timestamped version identifier
      timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

      # Convert to JSON
      import json

      submissions_json = json.dumps(submissions_data, default=str)

      # Store as latest (primary location)
      latest_s3_key = f"submissions/{cik}/latest.json"
      self.s3_client.put_object(
        Bucket=self.raw_bucket,
        Key=latest_s3_key,
        Body=submissions_json.encode("utf-8"),
        ContentType="application/json",
        StorageClass="STANDARD",  # Keep metadata in STANDARD for quick access
      )

      # Store versioned copy for history/rollback
      version_s3_key = f"submissions/{cik}/versions/v{timestamp}.json"
      self.s3_client.put_object(
        Bucket=self.raw_bucket,
        Key=version_s3_key,
        Body=submissions_json.encode("utf-8"),
        ContentType="application/json",
        StorageClass="STANDARD_IA",  # Use Infrequent Access for versions
      )

      # Update metadata about last update
      metadata_key = f"submissions/metadata/{cik}_last_updated.json"
      metadata = {
        "cik": cik,
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "version": timestamp,
        "filing_count": len(
          submissions_data.get("filings", {})
          .get("recent", {})
          .get("accessionNumber", [])
        ),
      }
      self.s3_client.put_object(
        Bucket=self.raw_bucket,
        Key=metadata_key,
        Body=json.dumps(metadata).encode("utf-8"),
        ContentType="application/json",
        StorageClass="STANDARD",
      )

      logger.info(f"Updated submissions: s3://{self.raw_bucket}/{latest_s3_key}")
      logger.info(f"Archived version: s3://{self.raw_bucket}/{version_s3_key}")

      # Return the latest key (primary reference)
      return latest_s3_key

    except Exception as e:
      logger.error(f"Failed to store entity submissions snapshot for {cik}: {e}")
      # Don't fail the pipeline if snapshot storage fails
      return None

  def _collect_raw_filing(
    self, cik: str, filing: Dict, year: int, storage_class: str
  ) -> Optional[str]:
    """
    Collect a single raw XBRL ZIP file with smart caching and rate limit handling.

    Strategy:
    1. Check if file already exists in S3 (skip download)
    2. If not, download with retry and exponential backoff
    3. Handle rate limits gracefully with fallback strategies
    """
    accession_number = filing.get("accessionNumber", "").replace("-", "")
    try:
      # S3 key with year partitioning
      raw_s3_key = f"raw/year={year}/{cik}/{accession_number}.zip"

      # Check if already exists in S3
      try:
        obj_metadata = self.s3_client.head_object(
          Bucket=self.raw_bucket, Key=raw_s3_key
        )
        file_size = obj_metadata.get("ContentLength", 0)

        # Basic validation: XBRL ZIP files should be at least 1KB
        # Most are 100KB-10MB, but some can be smaller
        min_valid_size = 1024  # 1KB minimum

        if file_size >= min_valid_size:
          logger.info(
            f"Raw file already exists: {raw_s3_key} (size: {file_size:,} bytes)"
          )
          return raw_s3_key
        else:
          logger.warning(
            f"Found suspiciously small file at {raw_s3_key} ({file_size} bytes), re-downloading"
          )
          # Note: We could optionally delete the invalid file here

      except Exception:
        pass  # File doesn't exist, need to download

      # Download from SEC with proper rate limit handling
      try:
        entity_sec_client = SECClient(cik=cik)
        xbrlzip_url = entity_sec_client.get_xbrlzip_url(filing)

        logger.info(f"Downloading raw XBRL: {xbrlzip_url}")

        # Use the helper method with retry decorator
        content = self._download_with_retry(xbrlzip_url)

        # Upload to S3 with specified storage class
        self.s3_client.put_object(
          Bucket=self.raw_bucket,
          Key=raw_s3_key,
          Body=content,
          StorageClass=storage_class,
        )

        logger.info(f"Stored raw file: s3://{self.raw_bucket}/{raw_s3_key}")
        return raw_s3_key

      except (requests.HTTPError, requests.Timeout) as e:
        error_msg = str(e).lower()
        if "rate limit" in error_msg or "429" in error_msg or "503" in error_msg:
          logger.warning(
            f"Rate limited when downloading XBRL for {cik}/{accession_number}"
          )

          # Strategy: Mark as skipped but don't fail the pipeline
          # This filing can be retried in a future run
          logger.info(f"Marking {raw_s3_key} as skipped due to rate limit")
          return None  # Return None to indicate skip, not failure
        else:
          # Non-rate-limit error, re-raise
          raise

    except Exception as e:
      logger.error(f"Failed to collect raw filing {cik}/{accession_number}: {e}")
      return None

  @retry(
    stop_max_attempt_number=3,
    wait_exponential_multiplier=2000,  # Start with 2 seconds
    wait_exponential_max=30000,  # Max 30 seconds between retries for rate limits
    retry_on_exception=lambda e: isinstance(
      e, (requests.RequestException, requests.Timeout)
    )
    and not ("404" in str(e) or "400" in str(e)),  # Don't retry client errors
  )
  def _download_with_retry(self, xbrlzip_url: str) -> bytes:
    """
    Download XBRL ZIP file with retry logic and enhanced rate limiting detection.

    Uses exponential backoff and retries on network errors.
    Detects various forms of rate limiting from the SEC API.
    """
    try:
      response = requests.get(
        xbrlzip_url,
        headers=SEC_HEADERS,
        timeout=(30, 60),  # (connect timeout, read timeout)
        stream=True,
      )

      # Enhanced rate limit detection
      if response.status_code == 429:  # Too Many Requests
        logger.warning(f"Rate limited (429) for {xbrlzip_url}")
        raise requests.HTTPError(f"Rate limited: HTTP {response.status_code}")

      if response.status_code == 503:  # Service Unavailable
        logger.warning(
          f"Service unavailable (503) for {xbrlzip_url} - likely rate limited"
        )
        raise requests.HTTPError(f"Service unavailable: HTTP {response.status_code}")

      if response.status_code == 403:  # Forbidden (sometimes used for rate limiting)
        logger.warning(f"Forbidden (403) for {xbrlzip_url} - possible rate limit")
        raise requests.HTTPError(f"Forbidden: HTTP {response.status_code}")

      if response.status_code != 200:
        raise requests.HTTPError(f"Failed to download: HTTP {response.status_code}")

      # Stream content to avoid memory issues with large files
      content_chunks = []
      total_size = 0
      for chunk in response.iter_content(chunk_size=1024 * 1024):  # 1MB chunks
        if chunk:
          content_chunks.append(chunk)
          total_size += len(chunk)

      # Check for suspiciously small files (possible rate limit response)
      if total_size < 100:  # Less than 100 bytes is suspicious for a ZIP file
        logger.warning(
          f"Suspiciously small response ({total_size} bytes) for {xbrlzip_url}"
        )
        content = b"".join(content_chunks)
        # Check if it's HTML (error page) instead of binary ZIP
        if content.startswith(b"<!DOCTYPE") or content.startswith(b"<html"):
          logger.error("Received HTML error page instead of ZIP file")
          raise requests.HTTPError("Received HTML error page - likely rate limited")

      return b"".join(content_chunks)

    except requests.Timeout as e:
      logger.warning(f"Timeout downloading {xbrlzip_url}: {e}")
      raise
    except requests.RequestException as e:
      logger.warning(f"Request error downloading {xbrlzip_url}: {e}")
      raise

  def _list_raw_files_by_year(self, year: int) -> List[str]:
    """List raw files for a specific year."""
    try:
      response = self.s3_client.list_objects_v2(
        Bucket=self.raw_bucket, Prefix=f"raw/year={year}/"
      )

      if "Contents" in response:
        return [
          obj["Key"] for obj in response["Contents"] if obj["Key"].endswith(".zip")
        ]
      return []

    except Exception as e:
      logger.error(f"Failed to list raw files for year {year}: {e}")
      return []

  def _list_processed_files_by_year(self, year: int) -> List[str]:
    """List processed parquet files for a specific year."""
    try:
      response = self.s3_client.list_objects_v2(
        Bucket=self.processed_bucket, Prefix=f"processed/year={year}/"
      )

      if "Contents" in response:
        return [
          obj["Key"] for obj in response["Contents"] if obj["Key"].endswith(".parquet")
        ]
      return []

    except Exception as e:
      logger.error(f"Failed to list processed files for year {year}: {e}")
      return []

  def _process_raw_filing(
    self, raw_file_key: str, year: int, refresh: bool = False
  ) -> Optional[List[str]]:
    """
    Process a raw XBRL filing from S3.

    This is the main entry point for processing individual filings from the bulk pipeline.

    Args:
        raw_file_key: S3 key of the raw XBRL ZIP file
        year: Year being processed
        refresh: Whether to force reprocessing

    Returns:
        List of processed S3 keys if successful, None otherwise
    """
    try:
      # XBRL processor needs to be created per filing with proper context
      # We'll pass None and let _process_single_raw_file handle it

      # Process the single raw file
      result = self._process_single_raw_file(raw_file_key, year, None, refresh)

      # Extract the list of processed files from the result
      if result and result.get("status") == "success":
        return result.get("processed_files", [])

      return None

    except Exception as e:
      logger.error(f"Failed to process raw filing {raw_file_key}: {e}")
      return None

  def _process_single_raw_file(
    self,
    raw_file_key: str,
    year: int,
    xbrl_pipeline,  # noqa: ARG002 - Not used, processor created in _process_raw_zip_file
    refresh: bool,
  ) -> Dict:
    """Process a single raw file using the XBRL pipeline."""
    try:
      # Extract CIK and accession number from S3 key structure
      # Expected format: raw/year=2025/CIK/ACCESSION.zip
      key_parts = raw_file_key.split("/")
      if len(key_parts) < 3:
        raise ValueError(f"Invalid raw file key structure: {raw_file_key}")

      cik = key_parts[-2]
      accession_number = key_parts[-1].replace(".zip", "")

      logger.info(f"Processing raw file: {cik}/{accession_number}")

      # Download raw file from S3 to temp location
      with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as temp_file:
        temp_zip_path = temp_file.name

      try:
        self.s3_client.download_file(self.raw_bucket, raw_file_key, temp_zip_path)

        # Fetch actual filing metadata from SEC
        filing = self._get_filing_metadata(cik, accession_number)

        if not filing:
          # Fallback: Create basic filing dict
          logger.warning(
            f"Could not fetch filing metadata for {cik}/{accession_number}, using defaults"
          )
          filing = {
            "accessionNumber": accession_number,
            "filingDate": f"{year}-01-01",  # Default to year start
            "isInlineXBRL": True,  # Most modern filings are inline XBRL
          }

        # Process using the existing XBRL processing logic
        result = self._process_raw_zip_file(temp_zip_path, cik, filing, year, refresh)

        # Map the result to expected format
        if result.get("status") == "completed":
          result["status"] = "success"
          result["processed_files"] = result.get("parquet_files", [])

        return result

      finally:
        # Cleanup temp file
        if Path(temp_zip_path).exists():
          Path(temp_zip_path).unlink()

    except Exception as e:
      logger.error(f"Failed to process raw file {raw_file_key}: {e}")
      return {"status": "failed", "error": str(e)}

  def _process_raw_zip_file(
    self, zip_path: str, cik: str, filing: Dict, year: int, refresh: bool
  ) -> Dict:
    """Process a raw XBRL ZIP file to parquet format."""
    accession_number = filing.get("accessionNumber", "").replace("-", "")

    try:
      # Check if parquet files already exist (unless refresh=True)
      if not refresh:
        existing_files = self._check_parquet_files_exist_by_year(
          cik, accession_number, year
        )
        if existing_files:
          logger.info(
            f"Parquet files already exist for {cik}/{accession_number}: {len(existing_files)} files"
          )
          return {
            "status": "skipped",
            "reason": "parquet_files_exist",
            "cik": cik,
            "accession_number": accession_number,
            "existing_parquet_files": existing_files,
          }

      # Create a temporary directory to extract XBRL ZIP contents
      with tempfile.TemporaryDirectory() as xbrl_extract_dir:
        # Extract ZIP contents
        from zipfile import ZipFile

        logger.info(f"Extracting XBRL ZIP from: {zip_path}")

        with ZipFile(zip_path) as zip_file:
          zip_file.extractall(xbrl_extract_dir)
          logger.info(f"Extracted {len(zip_file.namelist())} files from XBRL ZIP")

        # Find primary document
        primary_file_path = self._find_primary_document(xbrl_extract_dir, filing)
        if not primary_file_path:
          raise Exception("No primary document found in extracted files")

        logger.info(f"Primary document: {primary_file_path.name}")

        # Construct the SEC URL for metadata
        xbrl_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_number}/{primary_file_path.name}"

        # Process XBRL with temporary output directory
        with tempfile.TemporaryDirectory() as output_dir:
          # Schema configuration
          schema_config = {
            "name": "SEC Database Schema",
            "description": "Complete financial reporting schema with XBRL taxonomy support",
            "version": "1.0.0",
            "base_schema": "base",
            "extensions": ["roboledger"],
          }

          # Get entity metadata
          entity_metadata = self._get_entity_metadata(cik)

          # Initialize XBRL processor
          xbrl_processor = XBRLGraphProcessor(
            report_uri=xbrl_url,
            entityId=cik,
            sec_filer=entity_metadata or {"cik": cik},
            sec_report=filing,
            output_dir=output_dir,
            schema_config=schema_config,
            local_file_path=str(primary_file_path),
          )

          # Process XBRL
          xbrl_processor.process()

          # Collect generated parquet files from subdirectories
          # XBRLGraphProcessor saves to nodes/ and relationships/ subdirectories
          parquet_files = []

          # Collect from nodes subdirectory
          nodes_dir = Path(output_dir) / "nodes"
          if nodes_dir.exists():
            for parquet_file in nodes_dir.glob("*.parquet"):
              parquet_files.append(parquet_file)

          # Collect from relationships subdirectory
          rels_dir = Path(output_dir) / "relationships"
          if rels_dir.exists():
            for parquet_file in rels_dir.glob("*.parquet"):
              parquet_files.append(parquet_file)

          # Also check root directory for backwards compatibility
          for parquet_file in Path(output_dir).glob("*.parquet"):
            parquet_files.append(parquet_file)

          if not parquet_files:
            raise Exception("No parquet files generated from XBRL")

          # Upload to S3 with year partitioning
          s3_parquet_files = self._save_parquet_files_to_s3_by_year(
            parquet_files, cik, accession_number, year
          )

          return {
            "status": "completed",
            "cik": cik,
            "accession_number": accession_number,
            "parquet_files": s3_parquet_files,
            "parquet_files_count": len(s3_parquet_files),
          }

    except Exception as e:
      logger.error(f"Failed to process raw ZIP {cik}/{accession_number}: {e}")
      return {
        "status": "failed",
        "cik": cik,
        "accession_number": accession_number,
        "error": str(e),
      }

  def _find_primary_document(
    self, xbrl_extract_dir: str, filing: Dict
  ) -> Optional[Path]:
    """Find the primary XBRL document in the extracted directory."""
    if filing.get("isInlineXBRL", False):
      # For inline XBRL, find the primary HTML document
      primary_doc = filing.get("primaryDocument", "")
      if primary_doc:
        # Look for exact match first
        for file_path in Path(xbrl_extract_dir).rglob(primary_doc):
          return file_path

      # Fallback: look for the largest HTML file (likely the main document, not exhibits)
      html_files = list(Path(xbrl_extract_dir).rglob("*.htm")) + list(
        Path(xbrl_extract_dir).rglob("*.html")
      )
      if html_files:
        # Sort by file size to get the main document (exhibits are typically smaller)
        sorted_files = sorted(html_files, key=lambda p: p.stat().st_size, reverse=True)
        largest_file = sorted_files[0]

        # Log file sizes for debugging
        logger.info(
          f"Found {len(html_files)} HTML files. Largest: {largest_file.name} ({largest_file.stat().st_size:,} bytes)"
        )
        if len(sorted_files) > 1:
          second_largest = sorted_files[1]
          logger.info(
            f"Second largest: {second_largest.name} ({second_largest.stat().st_size:,} bytes)"
          )

        return largest_file
    else:
      # For traditional XBRL, find XML instance document
      # First try the XSD approach - find the schema and replace .xsd with .xml
      xsd_files = list(Path(xbrl_extract_dir).rglob("*.xsd"))
      if xsd_files:
        # Usually there's only one XSD file
        xsd_file = xsd_files[0]
        xml_filename = xsd_file.name.replace(".xsd", ".xml")

        # Look for the corresponding XML file
        for file_path in Path(xbrl_extract_dir).rglob(xml_filename):
          logger.info(f"Found instance document via XSD: {file_path.name}")
          return file_path

      # Fallback: find the largest XML file (instance documents are typically the largest)
      xml_files = list(Path(xbrl_extract_dir).rglob("*.xml"))
      if xml_files:
        # Exclude files that are clearly not instance documents
        instance_candidates = [
          f
          for f in xml_files
          if not any(
            suffix in f.name.lower()
            for suffix in ["_cal.xml", "_def.xml", "_lab.xml", "_pre.xml"]
          )
        ]

        if instance_candidates:
          largest_xml = max(instance_candidates, key=lambda p: p.stat().st_size)
          logger.info(
            f"Found instance document as largest XML: {largest_xml.name} ({largest_xml.stat().st_size:,} bytes)"
          )
          return largest_xml
        elif xml_files:
          # If no good candidates, just use the largest XML
          largest_xml = max(xml_files, key=lambda p: p.stat().st_size)
          logger.warning(f"Using largest XML file as fallback: {largest_xml.name}")
          return largest_xml

    logger.error(f"Could not find primary document in {xbrl_extract_dir}")
    return None

  def _get_entity_metadata(self, cik: str) -> Optional[Dict]:
    """Fetch entity metadata from SEC API."""
    try:
      # Fetch entity submissions from SEC
      sec_client = SECClient(cik=cik)
      submissions = sec_client.get_submissions()

      if not submissions:
        return None

      # Extract entity metadata
      entity_data = {
        "cik": submissions.get("cik", cik),
        "name": submissions.get("name"),
        "entity_name": submissions.get("name"),  # Alias for processor
        "ticker": submissions.get("tickers", [None])[0]
        if submissions.get("tickers")
        else None,
        "exchange": submissions.get("exchanges", [None])[0]
        if submissions.get("exchanges")
        else None,
        "sic": submissions.get("sic"),
        "sicDescription": submissions.get("sicDescription"),
        "category": submissions.get("category"),
        "entityType": submissions.get("entityType"),
        "stateOfIncorporation": submissions.get("stateOfIncorporation"),
        "fiscalYearEnd": submissions.get("fiscalYearEnd"),
        "ein": submissions.get("ein"),
        "phone": submissions.get("phone"),
      }

      # Add address information if available
      if "addresses" in submissions:
        if "business" in submissions["addresses"]:
          business_addr = submissions["addresses"]["business"]
          entity_data["business_address"] = {
            "street1": business_addr.get("street1"),
            "street2": business_addr.get("street2"),
            "city": business_addr.get("city"),
            "state": business_addr.get("stateOrCountry"),
            "zipCode": business_addr.get("zipCode"),
          }

        if "mailing" in submissions["addresses"]:
          mailing_addr = submissions["addresses"]["mailing"]
          entity_data["mailing_address"] = {
            "street1": mailing_addr.get("street1"),
            "street2": mailing_addr.get("street2"),
            "city": mailing_addr.get("city"),
            "state": mailing_addr.get("stateOrCountry"),
            "zipCode": mailing_addr.get("zipCode"),
          }

      logger.info(
        f"Found entity metadata: name={entity_data.get('name')}, "
        f"ticker={entity_data.get('ticker')}, "
        f"sic={entity_data.get('sic')}"
      )

      return entity_data

    except Exception as e:
      logger.error(f"Failed to fetch entity metadata for CIK {cik}: {e}")
      return None

  def _get_filing_metadata(
    self, cik: str, accession_number_no_dash: str
  ) -> Optional[Dict]:
    """Fetch filing metadata from stored entity submissions snapshot or SEC API as fallback."""
    try:
      # Format accession number with dashes
      accession_with_dash = f"{accession_number_no_dash[:10]}-{accession_number_no_dash[10:12]}-{accession_number_no_dash[12:]}"

      # First try to load entity submissions snapshot from S3
      submissions = self._load_entity_submissions_snapshot(cik)

      if not submissions:
        # Fallback to SEC API if no snapshot exists
        logger.warning(
          f"No entity submissions snapshot for {cik}, falling back to SEC API"
        )
        sec_client = SECClient(cik=cik)
        submissions = sec_client.get_submissions()

      if not submissions or "filings" not in submissions:
        return None

      recent_filings = submissions["filings"].get("recent", {})

      # Find the specific filing
      accession_numbers = recent_filings.get("accessionNumber", [])

      for idx, acc_num in enumerate(accession_numbers):
        if acc_num == accession_with_dash:
          # Found the filing, extract its metadata
          # Safely get values from lists with bounds checking
          def safe_get(list_name: str, index: int, default=None):
            lst = recent_filings.get(list_name, [])
            return lst[index] if index < len(lst) else default

          filing_data = {
            "accessionNumber": acc_num,
            "filingDate": safe_get("filingDate", idx),
            "primaryDocument": safe_get("primaryDocument", idx),
            "isInlineXBRL": bool(safe_get("isInlineXBRL", idx, False)),
            "isXBRL": bool(safe_get("isXBRL", idx, False)),
            "form": safe_get("form", idx),
            "reportDate": safe_get("reportDate", idx),
            "acceptanceDateTime": safe_get("acceptanceDateTime", idx),
            "periodOfReport": safe_get("periodOfReport", idx),
            "fileNumber": safe_get("fileNumber", idx),
            "filmNumber": safe_get("filmNumber", idx),
          }

          logger.info(
            f"Found filing metadata: form={filing_data['form']}, "
            f"isInlineXBRL={filing_data['isInlineXBRL']}, "
            f"primaryDocument={filing_data.get('primaryDocument', 'N/A')}"
          )

          return filing_data

      # If not found in recent, check additional pages
      if "additional_pages" in submissions:
        logger.debug(
          f"Checking {len(submissions['additional_pages'])} additional pages for filing"
        )
        for page_data in submissions["additional_pages"]:
          # Additional pages have a different structure - they're just arrays of filings
          if isinstance(page_data, list):
            # Convert list to dict format similar to recent filings
            for filing in page_data:
              if filing.get("accessionNumber") == accession_with_dash:
                # Found the filing in additional pages
                filing_data = {
                  "accessionNumber": filing.get("accessionNumber"),
                  "filingDate": filing.get("filingDate"),
                  "primaryDocument": filing.get("primaryDocument"),
                  "isInlineXBRL": bool(filing.get("isInlineXBRL", False)),
                  "isXBRL": bool(filing.get("isXBRL", False)),
                  "form": filing.get("form"),
                  "reportDate": filing.get("reportDate"),
                  "acceptanceDateTime": filing.get("acceptanceDateTime"),
                  "periodOfReport": filing.get("periodOfReport"),
                  "fileNumber": filing.get("fileNumber"),
                  "filmNumber": filing.get("filmNumber"),
                }
                logger.info(f"Found filing in additional pages: {filing_data['form']}")
                return filing_data

      logger.warning(
        f"Filing {accession_with_dash} not found in SEC submissions for CIK {cik}"
      )
      return None

    except Exception as e:
      logger.error(
        f"Failed to fetch filing metadata for {cik}/{accession_number_no_dash}: {e}"
      )
      return None

  def _check_parquet_files_exist_by_year(
    self, cik: str, accession_number: str, year: int
  ) -> List[str]:
    """Check if parquet files already exist for this filing in the year partition.

    Checks for files in the new structure:
    - processed/year={year}/nodes/{NodeType}/{cik}_{accession}.parquet
    - processed/year={year}/relationships/{RelType}/{cik}_{accession}.parquet
    """
    try:
      existing_files = []

      # Check for node files
      nodes_prefix = f"processed/year={year}/nodes/"
      response = self.s3_client.list_objects_v2(
        Bucket=self.processed_bucket, Prefix=nodes_prefix, MaxKeys=1000
      )

      if "Contents" in response:
        # Look for files matching this CIK and accession
        pattern = f"{cik}_{accession_number}.parquet"
        for obj in response["Contents"]:
          if obj["Key"].endswith(pattern):
            existing_files.append(obj["Key"])

      # Check for relationship files
      rels_prefix = f"processed/year={year}/relationships/"
      response = self.s3_client.list_objects_v2(
        Bucket=self.processed_bucket, Prefix=rels_prefix, MaxKeys=1000
      )

      if "Contents" in response:
        # Look for files matching this CIK and accession
        pattern = f"{cik}_{accession_number}.parquet"
        for obj in response["Contents"]:
          if obj["Key"].endswith(pattern):
            existing_files.append(obj["Key"])

      return existing_files

    except Exception as e:
      logger.warning(
        f"Error checking existing parquet files for {cik}/{accession_number}: {e}"
      )
      return []

  def _save_parquet_files_to_s3_by_year(
    self, parquet_files: List[Path], cik: str, accession_number: str, year: int
  ) -> List[str]:
    """Save parquet files to S3 with type-centric partitioning for bulk loading.

    Files are saved to:
    - processed/year={year}/nodes/{NodeType}/{cik}_{accession}.parquet
    - processed/year={year}/relationships/{RelType}/{cik}_{accession}.parquet

    This structure:
    - Removes timestamps for easier existence checking
    - Partitions by type for efficient wildcard ingestion
    - Allows simple overwrite on refresh
    - Maintains traceability via CIK and accession in filename
    """
    s3_keys = []

    # Node types and relationship types we expect from XBRLGraphProcessor
    # Use exact table names from schema
    node_types = {
      "Entity",
      "Report",
      "Element",
      "Unit",
      "Fact",
      "FactDimension",
      "FactSet",
      "Structure",
      "Association",
      "Period",
      "Label",
      "Reference",
      "Taxonomy",
    }

    relationship_types = {
      "ENTITY_HAS_REPORT",
      "REPORT_HAS_FACT",
      "REPORT_HAS_FACT_SET",
      "REPORT_USES_TAXONOMY",
      "FACT_HAS_UNIT",
      "FACT_HAS_DIMENSION",
      "FACT_HAS_ENTITY",
      "FACT_HAS_ELEMENT",
      "FACT_HAS_PERIOD",
      "FACT_SET_CONTAINS_FACT",
      "ELEMENT_HAS_LABEL",
      "ELEMENT_HAS_REFERENCE",
      "STRUCTURE_HAS_TAXONOMY",
      "TAXONOMY_HAS_LABEL",
      "TAXONOMY_HAS_REFERENCE",
      "STRUCTURE_HAS_ASSOCIATION",
      "ASSOCIATION_HAS_FROM_ELEMENT",
      "ASSOCIATION_HAS_TO_ELEMENT",
      "FACT_DIMENSION_AXIS_ELEMENT",
      "FACT_DIMENSION_MEMBER_ELEMENT",
    }

    # Upload each parquet file to the appropriate type-centric location
    for parquet_file in parquet_files:
      # Keep exact filename case for table names (e.g., FactDimension not factdimension)
      filename = parquet_file.stem  # Remove .parquet extension, keep exact case

      # Check if file is already in a subdirectory (nodes/ or relationships/)
      parent_dir = parquet_file.parent.name

      # Determine if this is a node or relationship file
      is_node = False
      is_relationship = False
      file_type = filename  # Use the filename as the type (exact case)

      # If file is already in nodes/ or relationships/ directory, use that
      if parent_dir == "nodes":
        is_node = True
      elif parent_dir == "relationships":
        is_relationship = True
      else:
        # File is in root, determine type from filename
        # Check if it's a node type (case-insensitive)
        for node_type in node_types:
          if (
            filename.lower() == node_type.lower()
            or filename.lower() == f"{node_type.lower()}s"
          ):
            is_node = True
            file_type = node_type
            break

        # Check if it's a relationship type (case-insensitive)
        if not is_node:
          for rel_type in relationship_types:
            if (
              filename.lower() == rel_type.lower()
              or filename.lower() == f"{rel_type.lower()}s"
            ):
              is_relationship = True
              file_type = rel_type
              break

      # Build the S3 key based on type
      # Key insight: For Entity nodes, we only need one per CIK (no accession)
      # since entity data is the same across all filings
      if is_node:
        if file_type == "Entity":
          # Entity is per-CIK, not per-filing
          s3_key = f"processed/year={year}/nodes/{file_type}/{cik}.parquet"
        else:
          # Other nodes are per-filing
          s3_key = (
            f"processed/year={year}/nodes/{file_type}/{cik}_{accession_number}.parquet"
          )
      elif is_relationship:
        # All relationships are per-filing
        s3_key = f"processed/year={year}/relationships/{file_type}/{cik}_{accession_number}.parquet"
      else:
        # Unknown file type, save to misc directory
        logger.warning(f"Unknown file type for {parquet_file.name}, saving to misc")
        s3_key = (
          f"processed/year={year}/misc/{filename}_{cik}_{accession_number}.parquet"
        )

      # Upload the file (will overwrite if exists - this is desired for refresh)
      self.s3_client.upload_file(str(parquet_file), self.processed_bucket, s3_key)
      s3_keys.append(s3_key)
      logger.debug(
        f"Uploaded {parquet_file.name} to s3://{self.processed_bucket}/{s3_key}"
      )

    logger.info(f"Uploaded {len(s3_keys)} parquet files for {cik}/{accession_number}")
    return s3_keys

  def _init_s3_client(self):
    """Initialize S3 client."""
    s3_config = env.get_s3_config()
    endpoint_url = s3_config.get("endpoint_url")

    if endpoint_url:
      return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
      )
    else:
      return boto3.client(
        "s3",
        aws_access_key_id=s3_config.get("aws_access_key_id"),
        aws_secret_access_key=s3_config.get("aws_secret_access_key"),
        region_name=s3_config.get("region_name"),
      )
