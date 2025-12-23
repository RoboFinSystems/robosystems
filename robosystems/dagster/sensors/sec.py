"""SEC pipeline sensors for parallel processing.

This sensor watches for raw XBRL filings in S3 and triggers parallel
processing jobs for each unprocessed filing.

Architecture:
- Phase 1 (Downloads): sec_raw_filings downloads ZIPs to S3 (2 concurrent max)
- Phase 2 (Processing): This sensor triggers sec_process_job for each filing (20 concurrent max)
- Phase 3 (Materialization): sec_materialize_job ingests to graph (sequential)

The sensor polls S3 every 60 seconds, finds raw ZIPs without corresponding
parquet output, registers dynamic partitions, and triggers parallel processing.
"""

import boto3
from botocore.exceptions import ClientError
from dagster import (
  DefaultSensorStatus,
  RunRequest,
  SensorEvaluationContext,
  SkipReason,
  sensor,
)

from robosystems.config import env
from robosystems.dagster.jobs.sec import sec_process_job


def _get_s3_client():
  """Create S3 client with LocalStack support for dev."""
  kwargs = {
    "region_name": env.AWS_REGION or "us-east-1",
  }
  if env.AWS_ENDPOINT_URL:
    kwargs["endpoint_url"] = env.AWS_ENDPOINT_URL
  return boto3.client("s3", **kwargs)


def _parse_raw_s3_key(key: str) -> tuple[str, str, str] | None:
  """Parse S3 key to extract year, cik, accession.

  Expected format: raw/year=2024/320193/0000320193-24-000081.zip

  Returns:
      Tuple of (year, cik, accession) or None if invalid format
  """
  parts = key.split("/")
  if len(parts) < 4 or not parts[-1].endswith(".zip"):
    return None

  # Extract year from "year=2024"
  year_part = parts[1]
  if not year_part.startswith("year="):
    return None
  year = year_part.replace("year=", "")

  # CIK is the third part
  cik = parts[2]

  # Accession is filename without .zip
  accession = parts[-1].replace(".zip", "")

  return year, cik, accession


def _check_processed_exists(
  s3_client, bucket: str, year: str, cik: str, accession: str
) -> bool:
  """Check if parquet output exists for this filing.

  We check for the Entity parquet file as a proxy for "fully processed".
  """
  processed_key = f"processed/year={year}/nodes/Entity/{cik}_{accession}.parquet"
  try:
    s3_client.head_object(Bucket=bucket, Key=processed_key)
    return True
  except Exception:
    return False


# Sensor status controlled by environment variable
SEC_PARALLEL_SENSOR_STATUS = (
  DefaultSensorStatus.RUNNING
  if env.SEC_PARALLEL_SENSOR_ENABLED
  else DefaultSensorStatus.STOPPED
)


@sensor(
  job=sec_process_job,
  minimum_interval_seconds=60,
  default_status=SEC_PARALLEL_SENSOR_STATUS,
  description="Watch for raw SEC filings and trigger parallel processing",
)
def sec_processing_sensor(context: SensorEvaluationContext):
  """Watch for raw SEC filings in S3 and trigger parallel processing.

  This sensor:
  1. Lists raw XBRL ZIPs in S3 (raw/year=*/cik/*.zip)
  2. Checks which don't have corresponding parquet output
  3. Registers dynamic partitions for unprocessed filings
  4. Yields RunRequest for each to trigger sec_process_job

  The QueuedRunCoordinator limits concurrent runs (default 20).

  Partition key format: {year}_{cik}_{accession}
  """
  # Skip in dev environment to avoid S3 connection issues
  if env.ENVIRONMENT == "dev":
    yield SkipReason(
      "Skipped in dev environment - use sec-process-parallel for local testing"
    )
    return

  raw_bucket = env.SEC_RAW_BUCKET
  processed_bucket = env.SEC_PROCESSED_BUCKET

  # Validate required S3 bucket configuration
  if not raw_bucket or not processed_bucket:
    yield SkipReason(
      "Missing required S3 bucket configuration (SEC_RAW_BUCKET or SEC_PROCESSED_BUCKET)"
    )
    return

  s3_client = _get_s3_client()

  try:
    # List all raw ZIPs
    paginator = s3_client.get_paginator("list_objects_v2")
    raw_files = []

    for page in paginator.paginate(Bucket=raw_bucket, Prefix="raw/"):
      for obj in page.get("Contents", []):
        key = obj["Key"]
        if key.endswith(".zip"):
          raw_files.append(key)

    if not raw_files:
      return

    context.log.info(f"Found {len(raw_files)} raw filings to check")

    # Track new partitions to register
    new_partitions = []
    run_requests = []

    for raw_key in raw_files:
      parsed = _parse_raw_s3_key(raw_key)
      if not parsed:
        continue

      year, cik, accession = parsed
      partition_key = f"{year}_{cik}_{accession}"

      # Check if already processed
      if _check_processed_exists(s3_client, processed_bucket, year, cik, accession):
        continue

      # Add to new partitions list
      new_partitions.append(partition_key)

      # Create run request with idempotent run_key
      run_requests.append(
        RunRequest(
          run_key=f"sec-process-{partition_key}",
          partition_key=partition_key,
        )
      )

    if not new_partitions:
      context.log.info("All filings already processed")
      return

    # Register dynamic partitions in batch
    context.log.info(f"Registering {len(new_partitions)} dynamic partitions")
    context.instance.add_dynamic_partitions(
      partitions_def_name="sec_filings",
      partition_keys=new_partitions,
    )

    # Yield run requests
    context.log.info(f"Triggering {len(run_requests)} processing jobs")
    yield from run_requests

  except ClientError as e:
    error_code = e.response.get("Error", {}).get("Code", "Unknown")
    if error_code == "NoSuchBucket":
      context.log.error(f"S3 bucket does not exist: {raw_bucket}")
    elif error_code == "AccessDenied":
      context.log.error(f"Access denied to S3 bucket: {raw_bucket}")
    else:
      context.log.error(f"S3 error ({error_code}): {e}")
    # Re-raise to mark sensor run as failed - Dagster will retry
    raise
  except Exception as e:
    context.log.error(f"Error in SEC processing sensor: {type(e).__name__}: {e}")
    # Re-raise to mark sensor run as failed - Dagster will retry
    raise
