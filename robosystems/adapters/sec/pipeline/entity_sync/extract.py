"""SEC Entity Sync Extract Asset.

Lists raw XBRL ZIPs in the shared raw S3 bucket for a single CIK.
No downloading — the shared SEC pipeline populates the raw bucket.
"""

import json

from dagster import AssetExecutionContext, MaterializeResult, asset

from .configs import SECEntitySyncConfig
from .utils import get_pipeline_work_dir


@asset(
  group_name="sec_entity_pipeline",
  description="Discover SEC filings for a CIK in the shared raw S3 bucket",
  kinds={"s3"},
  metadata={
    "pipeline": "sec_entity",
    "stage": "extract",
  },
)
def sec_entity_extract(
  context: AssetExecutionContext,
  config: SECEntitySyncConfig,
) -> MaterializeResult:
  """Discover available filings for a CIK in the shared raw bucket.

  Lists all raw XBRL ZIPs matching the CIK across all year partitions.
  Also loads the submissions metadata for the CIK (used by the
  processor for filing metadata like form type, filing date, etc.).

  The shared SEC pipeline is the sole source — if a filing isn't in
  the raw bucket, it's skipped.

  Returns:
      MaterializeResult with filing discovery metadata
  """
  import boto3

  from robosystems.config import env
  from robosystems.config.storage.shared import DataSourceType, get_raw_key

  context.log.info(f"Discovering SEC filings for CIK={config.cik} in shared raw bucket")

  s3_client = boto3.client("s3")
  bucket = env.SHARED_RAW_BUCKET

  # List all year partitions for this CIK
  # Raw files are at: sec/year={year}/{cik}/{accession}.zip
  prefix = get_raw_key(DataSourceType.SEC, "")
  filings: list[dict[str, str]] = []

  # Use paginator since there could be many year partitions
  paginator = s3_client.get_paginator("list_objects_v2")

  # We need to scan all year= partitions for this CIK's files
  # List top-level year= prefixes first
  year_response = paginator.paginate(
    Bucket=bucket,
    Prefix=f"{prefix}year=",
    Delimiter="/",
  )

  year_prefixes = []
  for page in year_response:
    for cp in page.get("CommonPrefixes", []):
      year_prefixes.append(cp["Prefix"])

  context.log.info(f"Found {len(year_prefixes)} year partitions to scan")

  # Scan each year partition for this CIK
  for year_prefix in year_prefixes:
    cik_prefix = f"{year_prefix}{config.cik}/"

    file_pages = paginator.paginate(
      Bucket=bucket,
      Prefix=cik_prefix,
    )

    for page in file_pages:
      for obj in page.get("Contents", []):
        key = obj["Key"]
        if key.endswith(".zip"):
          # Extract year and accession from the key
          # Key format: sec/year={year}/{cik}/{accession}.zip
          parts = key.split("/")
          year_part = parts[1]  # "year=2024"
          filename = parts[-1]  # "{accession}.zip"
          accession = filename.replace(".zip", "")

          filings.append(
            {
              "storage_key": key,
              "year": year_part.replace("year=", ""),
              "accession": accession,
              "size_bytes": obj["Size"],
            }
          )

  context.log.info(f"Found {len(filings)} raw XBRL ZIPs for CIK={config.cik}")

  if not filings:
    context.log.warning(
      f"No filings found for CIK={config.cik} in shared raw bucket. "
      "Ensure the shared SEC download pipeline has run."
    )

  # Check for submissions metadata
  submissions_key = get_raw_key(DataSourceType.SEC, "submissions", f"{config.cik}.json")
  has_submissions = False
  try:
    s3_client.head_object(Bucket=bucket, Key=submissions_key)
    has_submissions = True
    context.log.info(f"Submissions metadata found at {submissions_key}")
  except Exception:
    context.log.warning(
      f"No submissions metadata for CIK={config.cik}. "
      "Filing form types will be determined from XBRL content."
    )

  # Write manifest to shared work directory for transform to read
  work_dir = get_pipeline_work_dir(config.graph_id)
  manifest_path = work_dir / "filing_manifest.json"
  manifest = {
    "cik": config.cik,
    "filings": filings,
    "has_submissions": has_submissions,
    "submissions_key": submissions_key if has_submissions else None,
    "bucket": bucket,
    "form_types": config.form_types,
  }
  manifest_path.parent.mkdir(parents=True, exist_ok=True)
  manifest_path.write_text(json.dumps(manifest, indent=2))

  context.log.info(f"Wrote filing manifest → {manifest_path}")

  return MaterializeResult(
    metadata={
      "graph_id": config.graph_id,
      "cik": config.cik,
      "filings_found": len(filings),
      "has_submissions": has_submissions,
      "years": sorted({f["year"] for f in filings}),
    }
  )
