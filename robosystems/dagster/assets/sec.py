"""SEC XBRL pipeline Dagster assets.

This module defines the SEC data pipeline with two-level partitioning:

1. sec_companies_list - Fetch company list from SEC EDGAR
2. sec_raw_filings - Download XBRL ZIPs (year-partitioned, 2 concurrent max)
3. sec_filings_to_process - Discover raw filings, register as dynamic partitions
4. sec_process_filing - Process single filing to parquet (dynamic partitions, unlimited scale)
5. sec_duckdb_staging - Create DuckDB staging tables (runs once)
6. sec_graph_materialized - Materialize to LadybugDB graph (runs once)

The pipeline leverages existing adapters:
- robosystems.adapters.sec.SECClient - EDGAR API client
- robosystems.adapters.sec.XBRLGraphProcessor - XBRL processing
- robosystems.adapters.sec.XBRLDuckDBGraphProcessor - DuckDB staging/materialization

Architecture Notes:
- Year partitioning for downloads (rate-limited to 2 concurrent)
- Dynamic partitioning for processing (one partition per filing, unlimited scale)
- Each filing tracked independently for retry/visibility
- DuckDB staging creates virtual tables from S3 parquet files
- Graph materialization rebuilds the entire graph from staged data
"""

from typing import Any

from dagster import (
  AssetExecutionContext,
  Config,
  DynamicPartitionsDefinition,
  MaterializeResult,
  MetadataValue,
  Output,
  StaticPartitionsDefinition,
  asset,
)

from robosystems.config import env
from robosystems.dagster.resources import S3Resource

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


class SECBatchProcessConfig(Config):
  """Configuration for batch processing all filings in a year partition."""

  refresh: bool = False  # Re-process existing files
  tickers: list[str] = []  # Optional ticker filter
  ciks: list[str] = []  # Optional CIK filter


class SECFilingDiscoveryConfig(Config):
  """Configuration for filing discovery and partition registration."""

  year_filter: list[int] = []  # Optional year filter (empty = all years)
  skip_processed: bool = True  # Skip filings that already have parquet output


class SECSingleFilingConfig(Config):
  """Configuration for single filing processing."""

  # No config needed - partition key contains all info
  pass


class SECDuckDBConfig(Config):
  """Configuration for DuckDB staging."""

  rebuild: bool = True  # Rebuild staging tables from scratch
  year_filter: list[int] = []  # Optional year filter (empty = all years)


class SECMaterializeConfig(Config):
  """Configuration for graph materialization."""

  graph_id: str = "sec"  # Target graph ID
  ignore_errors: bool = True  # Continue on individual table errors
  rebuild: bool = True  # Rebuild graph database before materialization


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
      if config.tickers and ticker in config.tickers:
        companies.append({"cik": cik, "ticker": ticker})
      elif config.ciks and cik in config.ciks:
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
              from io import BytesIO
              import zipfile

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


@asset(
  group_name="sec_pipeline",
  description="Process all XBRL filings for a year to parquet format",
  compute_kind="transform",
  partitions_def=sec_year_partitions,
  deps=[sec_raw_filings],
  metadata={
    "pipeline": "sec",
    "stage": "processing",
  },
)
def sec_batch_process(
  context: AssetExecutionContext,
  config: SECBatchProcessConfig,
  s3: S3Resource,
) -> MaterializeResult:
  """Process all downloaded XBRL filings for a year into parquet format.

  Batch processes all raw XBRL ZIPs for a year partition using XBRLGraphProcessor.
  Use this for CLI workflows. For per-filing visibility, use sec_process_filing instead.

  Returns:
      MaterializeResult with processing statistics
  """
  from robosystems.adapters.sec import SECClient, XBRLGraphProcessor

  year = int(context.partition_key)
  context.log.info(f"Batch processing SEC filings for year {year}")

  raw_bucket = env.SEC_RAW_BUCKET or "robosystems-sec-raw"
  processed_bucket = env.SEC_PROCESSED_BUCKET or "robosystems-sec-processed"

  # List raw filings for this year
  raw_prefix = f"raw/year={year}/"
  paginator = s3.client.get_paginator("list_objects_v2")
  raw_files = []
  for page in paginator.paginate(Bucket=raw_bucket, Prefix=raw_prefix):
    for obj in page.get("Contents", []):
      raw_files.append({"key": obj["Key"], "size": obj["Size"]})

  if not raw_files:
    context.log.warning(f"No raw filings found for year {year}")
    return MaterializeResult(metadata={"year": year, "status": "no_data"})

  # Group by CIK and apply filters
  filings_by_cik: dict[str, list[dict]] = {}
  for file_info in raw_files:
    key = file_info["key"]
    parts = key.split("/")
    if len(parts) >= 3:
      cik = parts[2]
      if cik not in filings_by_cik:
        filings_by_cik[cik] = []
      filings_by_cik[cik].append(file_info)

  # Apply CIK filter
  if config.ciks:
    filings_by_cik = {k: v for k, v in filings_by_cik.items() if k in config.ciks}

  # Apply ticker filter (resolve tickers to CIKs)
  if config.tickers:
    sec_client = SECClient()
    companies = sec_client.get_companies()
    ticker_to_cik = {
      company.get("ticker", ""): str(company.get("cik_str", company.get("cik", "")))
      for _, company in companies.items()
    }
    allowed_ciks = {ticker_to_cik.get(t, "") for t in config.tickers}
    filings_by_cik = {k: v for k, v in filings_by_cik.items() if k in allowed_ciks}

  total_processed = 0
  total_files = 0
  total_errors = 0

  import tempfile
  from io import BytesIO
  import zipfile
  import os

  for cik, filings in filings_by_cik.items():
    for file_info in filings:
      key = file_info["key"]
      accession = key.split("/")[-1].replace(".zip", "")

      # Check if already processed
      processed_check = f"processed/year={year}/nodes/Entity/{cik}_{accession}.parquet"
      if not config.refresh:
        try:
          s3.client.head_object(Bucket=processed_bucket, Key=processed_check)
          continue  # Already processed
        except Exception:
          pass

      try:
        # Download raw ZIP
        buffer = BytesIO()
        s3.client.download_fileobj(raw_bucket, key, buffer)
        buffer.seek(0)

        with tempfile.TemporaryDirectory() as tmpdir:
          with zipfile.ZipFile(buffer, "r") as zf:
            zf.extractall(tmpdir)

          # Find main XBRL instance file
          exclude_suffixes = ("_def.xml", "_lab.xml", "_pre.xml", "_cal.xml", ".xsd")
          all_files = os.listdir(tmpdir)
          xbrl_files = [
            f for f in all_files
            if f.endswith((".xml", ".htm", ".html"))
            and not any(f.endswith(suffix) for suffix in exclude_suffixes)
          ]

          htm_files = [f for f in xbrl_files if f.endswith((".htm", ".html"))]
          if htm_files:
            xbrl_files = sorted(
              htm_files,
              key=lambda f: os.path.getsize(os.path.join(tmpdir, f)),
              reverse=True,
            )

          if not xbrl_files:
            context.log.warning(f"No XBRL files in {key}")
            total_errors += 1
            continue

          # Build report URL
          from robosystems.adapters.sec import SEC_BASE_URL
          report_url = f"{SEC_BASE_URL}/Archives/edgar/data/{int(cik)}/{accession.replace('-', '')}/{xbrl_files[0]}"

          schema_config = {
            "name": "SEC Database Schema",
            "description": "Complete financial reporting schema with XBRL taxonomy support",
            "base_schema": "base",
            "extensions": ["roboledger"],
          }

          processor = XBRLGraphProcessor(
            report_uri=report_url,
            entityId=cik,
            sec_filer={"cik": cik},
            sec_report={"accessionNumber": accession, "primaryDocument": xbrl_files[0]},
            output_dir=tmpdir,
            local_file_path=os.path.join(tmpdir, xbrl_files[0]),
            schema_config=schema_config,
          )

          processor.process()

          # Upload parquet files
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
                  total_files += 1

          total_processed += 1

      except Exception as e:
        context.log.warning(f"Failed to process {key}: {e}")
        total_errors += 1

    # Progress log
    if list(filings_by_cik.keys()).index(cik) % 10 == 0:
      context.log.info(f"Progress: {total_processed} filings, {total_errors} errors")

  context.log.info(f"Batch processing complete for {year}: {total_processed} filings, {total_files} files")

  return MaterializeResult(
    metadata={
      "year": year,
      "filings_processed": total_processed,
      "files_created": total_files,
      "errors": total_errors,
    }
  )


# ============================================================================
# Dynamic Partition Assets (per-filing processing)
# ============================================================================


@asset(
  group_name="sec_pipeline",
  description="Discover raw filings and register as dynamic partitions",
  compute_kind="discovery",
  deps=[sec_raw_filings],
  metadata={
    "pipeline": "sec",
    "stage": "discovery",
  },
  # Runs once after downloads complete - discovers all raw ZIPs in S3
)
def sec_filings_to_process(
  context: AssetExecutionContext,
  config: SECFilingDiscoveryConfig,
  s3: S3Resource,
) -> Output[list[str]]:
  """Discover raw filings in S3 and register as dynamic partitions.

  Scans the raw filings bucket for downloaded ZIPs and registers each
  as a dynamic partition for parallel processing.

  Partition key format: {year}_{cik}_{accession}

  Returns:
      List of partition keys registered
  """
  raw_bucket = env.SEC_RAW_BUCKET or "robosystems-sec-raw"
  processed_bucket = env.SEC_PROCESSED_BUCKET or "robosystems-sec-processed"

  context.log.info("Discovering raw filings for partition registration...")

  # Determine years to scan
  years_to_scan = config.year_filter if config.year_filter else [int(y) for y in SEC_YEARS]

  all_filings = []
  for year in years_to_scan:
    raw_prefix = f"raw/year={year}/"
    paginator = s3.client.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=raw_bucket, Prefix=raw_prefix):
      for obj in page.get("Contents", []):
        key = obj["Key"]
        # Parse: raw/year=2024/320193/0000320193-24-000081.zip
        parts = key.split("/")
        if len(parts) >= 4 and parts[-1].endswith(".zip"):
          cik = parts[2]
          accession = parts[-1].replace(".zip", "")
          partition_key = f"{year}_{cik}_{accession}"

          # Check if already processed (if skip_processed is enabled)
          if config.skip_processed:
            processed_prefix = f"processed/year={year}/nodes/Entity/{cik}_{accession}.parquet"
            try:
              s3.client.head_object(Bucket=processed_bucket, Key=processed_prefix)
              context.log.debug(f"Skipping already processed: {partition_key}")
              continue
            except Exception:
              pass  # Not processed yet

          all_filings.append(partition_key)

  context.log.info(f"Discovered {len(all_filings)} filings to process")

  # Register dynamic partitions
  if all_filings:
    context.instance.add_dynamic_partitions(
      partitions_def_name="sec_filings",
      partition_keys=all_filings,
    )
    context.log.info(f"Registered {len(all_filings)} dynamic partitions")

  return Output(
    all_filings,
    metadata={
      "filings_discovered": len(all_filings),
      "years_scanned": len(years_to_scan),
      "sample_partitions": MetadataValue.json(all_filings[:10] if all_filings else []),
    },
  )


@asset(
  group_name="sec_pipeline",
  description="Process a single SEC filing to parquet format",
  compute_kind="transform",
  partitions_def=sec_filing_partitions,
  deps=[sec_filings_to_process],
  metadata={
    "pipeline": "sec",
    "stage": "processing",
  },
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

  import tempfile
  from io import BytesIO
  import zipfile
  import os

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
        f for f in all_files
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

      # Process with XBRLGraphProcessor
      processor = XBRLGraphProcessor(
        report_uri=report_url,
        entityId=cik,
        sec_filer={"cik": cik},
        sec_report={"accessionNumber": accession, "primaryDocument": xbrl_files[0]},
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
  deps=[sec_batch_process],
  metadata={
    "pipeline": "sec",
    "stage": "staging",
  },
  # NOT partitioned - runs once after batch processing completes
  # Discovers parquet files and creates DuckDB virtual tables (metadata only)
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

  # Determine year filter
  year = config.year_filter[0] if config.year_filter else None

  async def run_staging():
    # Discover processed files
    context.log.info("Discovering processed parquet files...")
    tables_info = await processor._discover_processed_files(year)
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

  context.log.info(f"Materializing to graph: {config.graph_id}")

  # Use the existing processor for full pipeline
  processor = XBRLDuckDBGraphProcessor(
    graph_id=config.graph_id, source_prefix="processed"
  )

  async def run_materialization():
    # Use the high-level process_files method which handles everything
    result = await processor.process_files(
      rebuild=config.rebuild,
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
      "rebuild": config.rebuild,
      "status": result.get("status", "unknown"),
    }
  )
