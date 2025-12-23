"""SEC XBRL pipeline Dagster assets.

Pipeline stages (run independently via separate jobs):

1. DOWNLOAD (sec_download_only job):
   - sec_companies_list - Fetch company list from SEC EDGAR
   - sec_raw_filings - Download XBRL ZIPs (year-partitioned)

2. PROCESS (sec_process job, sensor-triggered):
   - sec_process_filing - Process single filing to parquet (dynamic partitions)

3. MATERIALIZE (sec_materialize job):
   - sec_duckdb_staging - Discover processed parquet files
   - sec_graph_materialized - Materialize to LadybugDB graph

The pipeline leverages existing adapters:
- robosystems.adapters.sec.SECClient - EDGAR API client
- robosystems.adapters.sec.XBRLGraphProcessor - XBRL processing
- robosystems.adapters.sec.XBRLDuckDBGraphProcessor - DuckDB staging/materialization

Architecture Notes:
- Year partitioning for downloads
- Dynamic partitioning for processing (one partition per filing, parallel)
- Sensor discovers unprocessed filings and triggers processing
- Graph materialization always rebuilds from all processed data
"""

from datetime import UTC
from typing import Any

from dagster import (
  AssetExecutionContext,
  Config,
  DynamicPartitionsDefinition,
  MaterializeResult,
  MetadataValue,
  Output,
  RetryPolicy,
  StaticPartitionsDefinition,
  asset,
)

from robosystems.config import env
from robosystems.dagster.resources import S3Resource

# In-memory cache for SEC submissions during a single run
_sec_submissions_cache: dict[str, dict] = {}


def _store_entity_submissions_snapshot(
  s3_client, bucket: str, cik: str, submissions_data: dict
) -> str | None:
  """Store entity submissions snapshot to S3.

  Submissions are stored at the CIK level (not year-partitioned) since they
  contain cumulative data spanning all years.

  Args:
      s3_client: boto3 S3 client
      bucket: S3 bucket name
      cik: Company CIK
      submissions_data: Complete submissions data from SEC API

  Returns:
      S3 key where data was stored, or None on failure
  """
  import json
  from datetime import datetime

  try:
    timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    submissions_json = json.dumps(submissions_data, default=str)

    # Store as latest (primary location for quick retrieval)
    latest_s3_key = f"submissions/{cik}/latest.json"
    s3_client.put_object(
      Bucket=bucket,
      Key=latest_s3_key,
      Body=submissions_json.encode("utf-8"),
      ContentType="application/json",
    )

    # Store versioned copy for history/audit
    version_s3_key = f"submissions/{cik}/versions/v{timestamp}.json"
    s3_client.put_object(
      Bucket=bucket,
      Key=version_s3_key,
      Body=submissions_json.encode("utf-8"),
      ContentType="application/json",
    )

    return latest_s3_key

  except Exception as e:
    # Don't fail the pipeline if snapshot storage fails
    import logging

    logging.getLogger(__name__).warning(
      f"Failed to store submissions snapshot for {cik}: {e}"
    )
    return None


def _load_entity_submissions_snapshot(s3_client, bucket: str, cik: str) -> dict | None:
  """Load entity submissions snapshot from S3.

  Args:
      s3_client: boto3 S3 client
      bucket: S3 bucket name
      cik: Company CIK

  Returns:
      Submissions data dict, or None if not found
  """
  import json

  try:
    s3_key = f"submissions/{cik}/latest.json"
    response = s3_client.get_object(Bucket=bucket, Key=s3_key)
    return json.loads(response["Body"].read().decode("utf-8"))
  except Exception:
    return None


def _get_sec_metadata(
  cik: str, accession: str, s3_client=None, bucket: str | None = None
) -> tuple[dict, dict]:
  """Fetch SEC filer and report metadata for a given CIK and accession number.

  Attempts to load from S3 snapshot first (stored during download phase),
  falling back to SEC API only if no snapshot exists.

  Args:
      cik: Company CIK
      accession: Accession number (with dashes)
      s3_client: Optional boto3 S3 client for loading snapshots
      bucket: Optional S3 bucket name for snapshots

  Returns:
      Tuple of (sec_filer dict, sec_report dict) with full metadata.
  """
  from robosystems.adapters.sec import SECClient

  submissions = None

  # Check in-memory cache first
  if cik in _sec_submissions_cache:
    submissions = _sec_submissions_cache[cik]

  # Try loading from S3 snapshot
  if submissions is None and s3_client is not None and bucket is not None:
    submissions = _load_entity_submissions_snapshot(s3_client, bucket, cik)
    if submissions:
      _sec_submissions_cache[cik] = submissions

  # Fallback to SEC API if no snapshot
  if submissions is None:
    import logging

    logging.getLogger(__name__).warning(
      f"No S3 snapshot for CIK {cik}, falling back to SEC API"
    )
    client = SECClient(cik=cik)
    submissions = client.get_submissions()
    _sec_submissions_cache[cik] = submissions

  # Build sec_filer from company-level data
  sec_filer = {
    "cik": cik,
    "name": submissions.get("name"),
    "entity_name": submissions.get("name"),  # Alternative key used by processor
    "ticker": submissions.get("tickers", [None])[0]
    if submissions.get("tickers")
    else None,
    "exchange": submissions.get("exchanges", [None])[0]
    if submissions.get("exchanges")
    else None,
    "sic": submissions.get("sic"),
    "sicDescription": submissions.get("sicDescription"),
    "stateOfIncorporation": submissions.get("stateOfIncorporation"),
    "fiscalYearEnd": submissions.get("fiscalYearEnd"),
    "ein": submissions.get("ein"),
    "entityType": submissions.get("entityType"),
    "category": submissions.get("category"),
    "website": submissions.get("website") or submissions.get("investorWebsite"),
    "phone": submissions.get("phone"),
  }

  # Find the specific filing in recent filings
  sec_report: dict = {"accessionNumber": accession}
  filings = submissions.get("filings", {}).get("recent", {})

  def safe_get(field: str, idx: int, default=None):
    """Safely get value from filings list with bounds checking."""
    lst = filings.get(field, [])
    return lst[idx] if idx < len(lst) else default

  if filings and "accessionNumber" in filings:
    accession_numbers = filings["accessionNumber"]
    for i, acc_num in enumerate(accession_numbers):
      if acc_num == accession:
        # Found the filing - extract all metadata
        sec_report = {
          "accessionNumber": accession,
          "form": safe_get("form", i),
          "filingDate": safe_get("filingDate", i),
          "reportDate": safe_get("reportDate", i),
          "acceptanceDateTime": safe_get("acceptanceDateTime", i),
          "primaryDocument": safe_get("primaryDocument", i),
          "periodOfReport": safe_get("periodOfReport", i),
          "isXBRL": bool(safe_get("isXBRL", i, False)),
          "isInlineXBRL": bool(safe_get("isInlineXBRL", i, False)),
        }
        break

  return sec_filer, sec_report


# Year partitions for SEC data (2019-2025)
SEC_YEARS = [str(y) for y in range(2019, 2026)]
sec_year_partitions = StaticPartitionsDefinition(SEC_YEARS)

# Dynamic partitions for individual filing processing
# Partition key format: {year}_{cik}_{accession}
sec_filing_partitions = DynamicPartitionsDefinition(name="sec_filings")


# ============================================================================
# Configuration Classes
# ============================================================================


class SECCompaniesConfig(Config):
  """Configuration for SEC companies list asset."""

  ticker_filter: list[str] = []  # Filter to specific tickers (e.g., ["NVDA", "AAPL"])
  cik_filter: list[str] = []  # Filter to specific CIKs
  max_companies: int = 0  # Limit number of companies (0 = unlimited)


class SECDownloadConfig(Config):
  """Configuration for SEC raw filings download."""

  skip_existing: bool = True  # Skip already downloaded filings
  form_types: list[str] = ["10-K", "10-Q"]  # Form types to download
  tickers: list[str] = []  # Optional ticker filter (empty = all companies)
  ciks: list[str] = []  # Optional CIK filter


class SECSingleFilingConfig(Config):
  """Configuration for single filing processing."""

  # No config needed - partition key contains all info
  pass


class SECDuckDBConfig(Config):
  """Configuration for DuckDB staging - discovers all processed files."""

  pass  # No config needed - always discovers all years


class SECMaterializeConfig(Config):
  """Configuration for graph materialization."""

  graph_id: str = "sec"  # Target graph ID
  ignore_errors: bool = True  # Continue on individual table errors
  # Note: Always rebuilds graph from scratch - incremental not yet supported


# ============================================================================
# Assets
# ============================================================================


@asset(
  group_name="sec_pipeline",
  description="Fetch list of SEC-registered companies from EDGAR",
  compute_kind="download",
  metadata={
    "pipeline": "sec",
    "stage": "discovery",
  },
)
def sec_companies_list(
  context: AssetExecutionContext,
  config: SECCompaniesConfig,
) -> Output[dict[str, Any]]:
  """Fetch list of SEC-registered companies.

  Downloads the company tickers list from SEC EDGAR and optionally
  filters by ticker or CIK.

  Returns:
      Dictionary with company data keyed by CIK
  """
  from robosystems.adapters.sec import SECClient

  context.log.info("Fetching SEC companies list from EDGAR")

  sec_client = SECClient()
  companies_raw = sec_client.get_companies()

  # Convert to dict keyed by CIK for easier lookups
  companies = {}
  for idx, company in companies_raw.items():
    cik = str(company.get("cik_str", company.get("cik", "")))
    ticker = company.get("ticker", "")

    # Apply filters if specified
    if config.ticker_filter and ticker not in config.ticker_filter:
      continue
    if config.cik_filter and cik not in config.cik_filter:
      continue

    companies[cik] = {
      "cik": cik,
      "ticker": ticker,
      "title": company.get("title", ""),
    }

    # Apply max limit if specified
    if config.max_companies > 0 and len(companies) >= config.max_companies:
      break

  context.log.info(f"Retrieved {len(companies)} companies from SEC")

  return Output(
    companies,
    metadata={
      "company_count": len(companies),
      "filtered_by_ticker": len(config.ticker_filter) > 0,
      "filtered_by_cik": len(config.cik_filter) > 0,
      "sample_companies": MetadataValue.json(
        list(companies.values())[:5] if companies else []
      ),
    },
  )


@asset(
  group_name="sec_pipeline",
  description="Download SEC XBRL filings for a specific year",
  compute_kind="download",
  partitions_def=sec_year_partitions,
  deps=[sec_companies_list],
  metadata={
    "pipeline": "sec_download",
    "stage": "extraction",
  },
  # Limit concurrent SEC downloads to avoid rate limiting
  # Max 2 partitions (years) download at a time
  op_tags={"dagster/concurrency_key": "sec_download", "dagster/max_concurrent": "2"},
)
def sec_raw_filings(
  context: AssetExecutionContext,
  config: SECDownloadConfig,
  s3: S3Resource,
) -> MaterializeResult:
  """Download SEC XBRL filings for a specific year.

  Downloads 10-K and 10-Q filings for the partition year,
  storing ZIPs in S3 for subsequent processing.

  Concurrency limited to 2 via dagster/concurrency_key to avoid SEC rate limiting.

  Returns:
      MaterializeResult with download statistics
  """
  from robosystems.adapters.sec import SECClient

  year = int(context.partition_key)
  context.log.info(f"Downloading SEC filings for year {year}")

  # Get bucket for raw filings
  bucket = env.SEC_RAW_BUCKET or "robosystems-sec-raw"

  # Initialize counters
  total_downloaded = 0
  total_skipped = 0
  total_errors = 0
  companies_processed = 0

  # Get companies to process - use filters from config or fetch all
  sec_client = SECClient()
  if config.tickers or config.ciks:
    # Filtered mode - process specific companies
    companies_raw = sec_client.get_companies()
    companies = []
    for _, company in companies_raw.items():
      ticker = company.get("ticker", "")
      cik = str(company.get("cik_str", company.get("cik", "")))
      if (config.tickers and ticker in config.tickers) or (
        config.ciks and cik in config.ciks
      ):
        companies.append({"cik": cik, "ticker": ticker})
  else:
    # Full mode - get all companies
    companies_raw = sec_client.get_companies()
    companies = [
      {"cik": str(c.get("cik_str", c.get("cik", ""))), "ticker": c.get("ticker", "")}
      for _, c in companies_raw.items()
    ]

  context.log.info(f"Processing {len(companies)} companies for year {year}")

  for company in companies:
    cik = company["cik"]
    ticker = company["ticker"]

    try:
      client = SECClient(cik=cik)

      # Fetch full submissions and store snapshot in S3 for later processing
      submissions_raw = client.get_submissions()
      _store_entity_submissions_snapshot(s3.client, bucket, cik, submissions_raw)

      # Get DataFrame for filtering
      submissions = client.submissions_df()

      # Filter to target year and form types
      year_mask = submissions["reportDate"].str.startswith(str(year))
      form_mask = submissions["form"].isin(config.form_types)
      xbrl_mask = submissions["isXBRL"] | submissions["isInlineXBRL"]

      filings = submissions[year_mask & form_mask & xbrl_mask]

      for _, filing in filings.iterrows():
        accession = filing["accessionNumber"]
        s3_key = f"raw/year={year}/{cik}/{accession}.zip"

        # Skip if exists and configured to do so
        if config.skip_existing:
          existing = s3.list_objects(s3_key)
          if existing:
            total_skipped += 1
            continue

        # Download the filing
        try:
          report_url = client.get_report_url(filing)
          if report_url:
            # Download XBRL ZIP
            xbrlzip_url = client.get_xbrlzip_url(filing)
            xbrl_zip = client.download_xbrlzip(xbrlzip_url)

            if xbrl_zip:
              # Save to S3 as bytes
              import zipfile
              from io import BytesIO

              buffer = BytesIO()
              with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
                for name in xbrl_zip.namelist():
                  zf.writestr(name, xbrl_zip.read(name))
              buffer.seek(0)

              # Upload to S3
              s3.client.upload_fileobj(buffer, bucket, s3_key)
              total_downloaded += 1
              context.log.debug(f"Downloaded {ticker} ({cik}) {accession} for {year}")

        except Exception as e:
          context.log.warning(f"Failed to download {ticker} {accession}: {e}")
          total_errors += 1

      companies_processed += 1

      # Progress log every 100 companies
      if companies_processed % 100 == 0:
        context.log.info(
          f"Progress: {companies_processed}/{len(companies)} companies, "
          f"{total_downloaded} downloaded, {total_skipped} skipped"
        )

    except Exception as e:
      context.log.warning(f"Failed to process company {ticker} ({cik}): {e}")
      total_errors += 1

  context.log.info(
    f"Download complete for year {year}: "
    f"{total_downloaded} downloaded, {total_skipped} skipped, {total_errors} errors"
  )

  return MaterializeResult(
    metadata={
      "year": year,
      "companies_processed": companies_processed,
      "filings_downloaded": total_downloaded,
      "filings_skipped": total_skipped,
      "errors": total_errors,
    }
  )


# ============================================================================
# Dynamic Partition Assets (per-filing processing)
# ============================================================================


@asset(
  group_name="sec_pipeline",
  description="Process a single SEC filing to parquet format",
  compute_kind="transform",
  partitions_def=sec_filing_partitions,
  # No deps - sensor handles discovery and triggers runs directly
  metadata={
    "pipeline": "sec",
    "stage": "processing",
  },
  # Retry once on failure (handles transient OOM on large filings)
  retry_policy=RetryPolicy(max_retries=1),
  # No concurrency limit - scales with infrastructure
  # Each filing processes independently
)
def sec_process_filing(
  context: AssetExecutionContext,
  config: SECSingleFilingConfig,
  s3: S3Resource,
) -> MaterializeResult:
  """Process a single SEC filing to parquet format.

  Takes partition key in format {year}_{cik}_{accession} and processes
  the corresponding raw ZIP file to parquet output.

  Returns:
      MaterializeResult with processing statistics
  """
  from robosystems.adapters.sec import XBRLGraphProcessor

  # Parse partition key: {year}_{cik}_{accession}
  partition_key = context.partition_key
  parts = partition_key.split("_", 2)  # Split into 3 parts max
  if len(parts) != 3:
    context.log.error(f"Invalid partition key format: {partition_key}")
    return MaterializeResult(
      metadata={"status": "error", "reason": f"Invalid partition key: {partition_key}"}
    )

  year, cik, accession = parts
  context.log.info(f"Processing filing: year={year}, cik={cik}, accession={accession}")

  raw_bucket = env.SEC_RAW_BUCKET or "robosystems-sec-raw"
  processed_bucket = env.SEC_PROCESSED_BUCKET or "robosystems-sec-processed"

  # Download raw ZIP
  raw_key = f"raw/year={year}/{cik}/{accession}.zip"

  import os
  import tempfile
  import zipfile
  from io import BytesIO

  try:
    buffer = BytesIO()
    s3.client.download_fileobj(raw_bucket, raw_key, buffer)
    buffer.seek(0)
  except Exception as e:
    context.log.error(f"Failed to download {raw_key}: {e}")
    return MaterializeResult(
      metadata={"status": "error", "reason": f"Download failed: {e}"}
    )

  # Extract and process
  try:
    with tempfile.TemporaryDirectory() as tmpdir:
      with zipfile.ZipFile(buffer, "r") as zf:
        zf.extractall(tmpdir)

      # Find main XBRL instance file
      exclude_suffixes = ("_def.xml", "_lab.xml", "_pre.xml", "_cal.xml", ".xsd")
      all_files = os.listdir(tmpdir)
      xbrl_files = [
        f
        for f in all_files
        if f.endswith((".xml", ".htm", ".html"))
        and not any(f.endswith(suffix) for suffix in exclude_suffixes)
      ]

      # Prefer .htm files for inline XBRL
      htm_files = [f for f in xbrl_files if f.endswith((".htm", ".html"))]
      if htm_files:
        xbrl_files = sorted(
          htm_files,
          key=lambda f: os.path.getsize(os.path.join(tmpdir, f)),
          reverse=True,
        )

      if not xbrl_files:
        context.log.warning(f"No XBRL instance files found in {raw_key}")
        return MaterializeResult(
          metadata={"status": "error", "reason": "No XBRL files found"}
        )

      # Build report URL
      from robosystems.adapters.sec import SEC_BASE_URL

      report_url = f"{SEC_BASE_URL}/Archives/edgar/data/{int(cik)}/{accession.replace('-', '')}/{xbrl_files[0]}"

      # Schema config
      schema_config = {
        "name": "SEC Database Schema",
        "description": "Complete financial reporting schema with XBRL taxonomy support",
        "base_schema": "base",
        "extensions": ["roboledger"],
      }

      # Fetch full SEC metadata from S3 snapshot (stored during download)
      sec_filer, sec_report = _get_sec_metadata(
        cik, accession, s3_client=s3.client, bucket=raw_bucket
      )
      # Ensure primaryDocument is set from local files if not in API response
      if not sec_report.get("primaryDocument"):
        sec_report["primaryDocument"] = xbrl_files[0]

      # Process with XBRLGraphProcessor
      processor = XBRLGraphProcessor(
        report_uri=report_url,
        entityId=cik,
        sec_filer=sec_filer,
        sec_report=sec_report,
        output_dir=tmpdir,
        local_file_path=os.path.join(tmpdir, xbrl_files[0]),
        schema_config=schema_config,
      )

      processor.process()

      # Upload parquet files to S3
      files_uploaded = 0
      for entity_type in ["nodes", "relationships"]:
        entity_dir = os.path.join(tmpdir, entity_type)
        if os.path.exists(entity_dir):
          for parquet_file in os.listdir(entity_dir):
            if parquet_file.endswith(".parquet"):
              local_path = os.path.join(entity_dir, parquet_file)
              table_name = parquet_file.replace(".parquet", "")
              s3_key = f"processed/year={year}/{entity_type}/{table_name}/{cik}_{accession}.parquet"

              with open(local_path, "rb") as f:
                s3.client.upload_fileobj(f, processed_bucket, s3_key)
              files_uploaded += 1

      context.log.info(f"Processed {partition_key}: {files_uploaded} files uploaded")

      return MaterializeResult(
        metadata={
          "partition_key": partition_key,
          "year": year,
          "cik": cik,
          "accession": accession,
          "files_uploaded": files_uploaded,
          "status": "success",
        }
      )

  except Exception as e:
    context.log.error(f"Processing failed for {partition_key}: {e}")
    return MaterializeResult(
      metadata={
        "partition_key": partition_key,
        "status": "error",
        "reason": str(e),
      }
    )


# ============================================================================
# Staging & Materialization Assets
# ============================================================================


@asset(
  group_name="sec_pipeline",
  description="Stage processed parquet files and create DuckDB tables",
  compute_kind="load",
  # No deps - triggered manually via sec_materialize job after processing completes
  metadata={
    "pipeline": "sec",
    "stage": "staging",
  },
  # NOT partitioned - runs once to discover all processed parquet files
)
def sec_duckdb_staging(
  context: AssetExecutionContext,
  config: SECDuckDBConfig,
) -> MaterializeResult:
  """Stage processed parquet files and create DuckDB tables.

  Discovers all processed parquet files across years and creates
  DuckDB virtual tables pointing to S3 data.

  Note: This is a synchronous wrapper around the async processor.

  Returns:
      MaterializeResult with staging statistics
  """
  import asyncio

  from robosystems.adapters.sec import XBRLDuckDBGraphProcessor

  context.log.info("Creating DuckDB staging tables from processed files")

  # Use the existing processor's discovery and staging logic
  processor = XBRLDuckDBGraphProcessor(graph_id="sec", source_prefix="processed")

  async def run_staging():
    # Discover all processed files (no year filter - ingest everything)
    context.log.info("Discovering processed parquet files...")
    tables_info = await processor._discover_processed_files(year=None)
    return tables_info

  # Run async code in sync context
  tables_info = asyncio.run(run_staging())

  if not tables_info:
    context.log.warning("No processed files found")
    return MaterializeResult(
      metadata={
        "status": "no_data",
        "reason": "No processed files found",
      }
    )

  total_files = sum(len(files) for files in tables_info.values())
  context.log.info(f"Found {len(tables_info)} tables with {total_files} files")

  return MaterializeResult(
    metadata={
      "tables_discovered": len(tables_info),
      "total_files": total_files,
      "table_names": MetadataValue.json(list(tables_info.keys())),
    }
  )


@asset(
  group_name="sec_pipeline",
  description="Materialize staged data to LadybugDB graph",
  compute_kind="load",
  deps=[sec_duckdb_staging],
  metadata={
    "pipeline": "sec",
    "stage": "materialization",
  },
  # NOT partitioned - runs once after staging complete
  # Single-threaded graph ingestion (LadybugDB constraint)
)
def sec_graph_materialized(
  context: AssetExecutionContext,
  config: SECMaterializeConfig,
) -> MaterializeResult:
  """Materialize staged data to LadybugDB graph.

  Uses XBRLDuckDBGraphProcessor.process_files() which handles:
  - Database rebuild (if requested)
  - DuckDB staging table creation
  - Graph ingestion

  Note: This is a synchronous wrapper around the async processor.

  Returns:
      MaterializeResult with materialization statistics
  """
  import asyncio

  from robosystems.adapters.sec import XBRLDuckDBGraphProcessor
  from robosystems.operations.graph.shared_repository_service import (
    ensure_shared_repository_exists,
  )

  context.log.info(f"Materializing to graph: {config.graph_id}")

  # Use the existing processor for full pipeline
  processor = XBRLDuckDBGraphProcessor(
    graph_id=config.graph_id, source_prefix="processed"
  )

  async def run_materialization():
    # Ensure the SEC repository metadata exists in PostgreSQL
    # This creates the Graph record, GraphSchema, and DuckDB staging tables
    context.log.info("Ensuring SEC repository metadata exists...")
    repo_result = await ensure_shared_repository_exists(
      repository_name="sec",
      created_by="system",
      instance_id="local-dev" if env.ENVIRONMENT == "dev" else "ladybug-shared-prod",
    )
    context.log.info(f"SEC repository status: {repo_result.get('status', 'unknown')}")

    # Use the high-level process_files method which handles everything
    # Always rebuild - incremental ingestion not yet supported
    result = await processor.process_files(
      rebuild=True,
      year=None,  # Process all years
    )
    return result

  # Run async code in sync context
  result = asyncio.run(run_materialization())

  total_rows = result.get("total_rows_ingested", 0)
  total_time = result.get("total_time_ms", 0)
  tables_count = result.get("tables_processed", 0)

  context.log.info(f"Materialization complete: {total_rows} rows in {total_time:.2f}ms")

  return MaterializeResult(
    metadata={
      "graph_id": config.graph_id,
      "rows_ingested": total_rows,
      "tables_processed": tables_count,
      "execution_time_ms": total_time,
      "status": result.get("status", "unknown"),
    }
  )
