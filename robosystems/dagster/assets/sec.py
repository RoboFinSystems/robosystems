"""SEC XBRL pipeline Dagster assets.

This module defines the SEC data pipeline as Dagster assets with year partitioning:

1. sec_companies_list - Fetch company list from SEC EDGAR
2. sec_raw_filings - Download XBRL ZIPs (year-partitioned)
3. sec_processed_filings - Process XBRL to parquet (year-partitioned)
4. sec_duckdb_staging - Create DuckDB staging tables (all years)
5. sec_graph_materialized - Materialize to LadybugDB graph

The pipeline leverages existing adapters:
- robosystems.adapters.sec.SECClient - EDGAR API client
- robosystems.adapters.sec.XBRLGraphProcessor - XBRL processing
- robosystems.adapters.sec.XBRLDuckDBGraphProcessor - DuckDB staging/materialization

Architecture Notes:
- Year partitioning allows parallel processing of different years
- DuckDB staging creates virtual tables from S3 parquet files
- Graph materialization rebuilds the entire graph from staged data
- Rate limiting (2 concurrent) is configured at the job level via tags
"""

from typing import Any

from dagster import (
  AssetExecutionContext,
  Config,
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


class SECProcessConfig(Config):
  """Configuration for SEC filings processing."""

  refresh: bool = False  # Re-process existing files
  tickers: list[str] = []  # Optional ticker filter
  ciks: list[str] = []  # Optional CIK filter


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
    "pipeline": "sec_download",  # Tag for rate limiting
    "stage": "extraction",
  },
)
def sec_raw_filings(
  context: AssetExecutionContext,
  config: SECDownloadConfig,
  s3: S3Resource,
) -> MaterializeResult:
  """Download SEC XBRL filings for a specific year.

  Downloads 10-K and 10-Q filings for the partition year,
  storing ZIPs in S3 for subsequent processing.

  Rate-limited to 2 concurrent downloads to avoid SEC throttling.

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
  description="Process XBRL filings into parquet format",
  compute_kind="transform",
  partitions_def=sec_year_partitions,
  deps=[sec_raw_filings],
  metadata={
    "pipeline": "sec",
    "stage": "processing",
  },
)
def sec_processed_filings(
  context: AssetExecutionContext,
  config: SECProcessConfig,
  s3: S3Resource,
) -> MaterializeResult:
  """Process downloaded XBRL filings into parquet format.

  Processes raw XBRL ZIPs using Arelle and XBRLGraphProcessor,
  outputting structured parquet files for each node/relationship type.

  Returns:
      MaterializeResult with processing statistics
  """
  from robosystems.adapters.sec import SECClient, XBRLGraphProcessor

  year = int(context.partition_key)
  context.log.info(f"Processing SEC filings for year {year}")

  raw_bucket = env.SEC_RAW_BUCKET or "robosystems-sec-raw"
  processed_bucket = env.SEC_PROCESSED_BUCKET or "robosystems-sec-processed"

  # List raw filings for this year (from the raw bucket, not the default S3 bucket)
  raw_prefix = f"raw/year={year}/"
  paginator = s3.client.get_paginator("list_objects_v2")
  raw_files = []
  for page in paginator.paginate(Bucket=raw_bucket, Prefix=raw_prefix):
    for obj in page.get("Contents", []):
      raw_files.append(
        {
          "key": obj["Key"],
          "size": obj["Size"],
          "last_modified": obj["LastModified"],
        }
      )

  if not raw_files:
    context.log.warning(f"No raw filings found for year {year}")
    return MaterializeResult(
      metadata={
        "year": year,
        "status": "no_data",
        "reason": "No raw filings found",
      }
    )

  # Group files by CIK
  filings_by_cik: dict[str, list[dict]] = {}
  for file_info in raw_files:
    key = file_info["key"]
    parts = key.split("/")
    if len(parts) >= 3:
      cik = parts[2]  # raw/year=YYYY/CIK/accession.zip
      if cik not in filings_by_cik:
        filings_by_cik[cik] = []
      filings_by_cik[cik].append(file_info)

  # Apply CIK filter if specified
  if config.ciks:
    filings_by_cik = {k: v for k, v in filings_by_cik.items() if k in config.ciks}

  # Apply ticker filter if specified (need to resolve tickers to CIKs)
  if config.tickers:
    sec_client = SECClient()
    companies = sec_client.get_companies()
    ticker_to_cik = {}
    for _, company in companies.items():
      ticker_to_cik[company.get("ticker", "")] = str(
        company.get("cik_str", company.get("cik", ""))
      )
    allowed_ciks = {ticker_to_cik.get(t, "") for t in config.tickers}
    filings_by_cik = {k: v for k, v in filings_by_cik.items() if k in allowed_ciks}

  total_processed = 0
  total_records = 0
  total_errors = 0

  import tempfile
  from io import BytesIO
  import zipfile

  for cik, filings in filings_by_cik.items():
    for file_info in filings:
      key = file_info["key"]
      accession = key.split("/")[-1].replace(".zip", "")

      # Check if already processed
      processed_prefix = f"processed/year={year}/nodes/Entity/{cik}_{accession}.parquet"
      if not config.refresh:
        # Check if already processed (in processed bucket)
        try:
          s3.client.head_object(Bucket=processed_bucket, Key=processed_prefix)
          context.log.debug(f"Skipping already processed: {cik}/{accession}")
          continue
        except Exception:
          pass  # File doesn't exist, proceed with processing

      try:
        # Download raw ZIP from raw bucket
        buffer = BytesIO()
        s3.client.download_fileobj(raw_bucket, key, buffer)
        buffer.seek(0)

        # Extract to temp directory
        with tempfile.TemporaryDirectory() as tmpdir:
          with zipfile.ZipFile(buffer, "r") as zf:
            zf.extractall(tmpdir)

          # Find the main XBRL instance file (exclude taxonomy/metadata files)
          import os

          # Patterns to exclude (taxonomy definition files)
          exclude_suffixes = ("_def.xml", "_lab.xml", "_pre.xml", "_cal.xml", ".xsd")

          all_files = os.listdir(tmpdir)
          xbrl_files = [
            f
            for f in all_files
            if f.endswith((".xml", ".htm", ".html"))
            and not any(f.endswith(suffix) for suffix in exclude_suffixes)
          ]

          # Prefer .htm files for inline XBRL, then .xml
          htm_files = [f for f in xbrl_files if f.endswith((".htm", ".html"))]
          if htm_files:
            # Pick the largest .htm file (usually the main document)
            xbrl_files = sorted(
              htm_files,
              key=lambda f: os.path.getsize(os.path.join(tmpdir, f)),
              reverse=True,
            )

          if not xbrl_files:
            context.log.warning(f"No XBRL instance files found in {key}")
            total_errors += 1
            continue

          # Get report URL for metadata
          from robosystems.adapters.sec import SEC_BASE_URL

          report_url = f"{SEC_BASE_URL}/Archives/edgar/data/{int(cik)}/{accession.replace('-', '')}/{xbrl_files[0]}"

          # Schema config for XBRL processing (with roboledger extension)
          schema_config = {
            "name": "SEC Database Schema",
            "description": "Complete financial reporting schema with XBRL taxonomy support",
            "base_schema": "base",
            "extensions": ["roboledger"],
          }

          # Get company info from SEC (ticker not available in processing loop)
          sec_filer = {"cik": cik}

          # Build SEC report metadata
          sec_report = {
            "accessionNumber": accession,
            "form": None,  # Will be determined from file
            "filingDate": None,
            "primaryDocument": xbrl_files[0],
          }

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

          # Run the processing - process() returns None, raises on error
          try:
            processor.process()

            # Upload parquet files to S3 if any were created
            # Processor outputs files directly in nodes/ and relationships/ directories
            for entity_type in ["nodes", "relationships"]:
              entity_dir = os.path.join(tmpdir, entity_type)
              if os.path.exists(entity_dir):
                for parquet_file in os.listdir(entity_dir):
                  if parquet_file.endswith(".parquet"):
                    local_path = os.path.join(entity_dir, parquet_file)
                    # Extract table name from filename (e.g., Entity.parquet -> Entity)
                    table_name = parquet_file.replace(".parquet", "")
                    s3_key = f"processed/year={year}/{entity_type}/{table_name}/{cik}_{accession}.parquet"

                    with open(local_path, "rb") as f:
                      s3.client.upload_fileobj(f, processed_bucket, s3_key)

                    total_records += 1

            total_processed += 1
            context.log.debug(f"Processed {cik}/{accession}")

          except Exception as e:
            context.log.warning(f"Processing failed for {cik}/{accession}: {e}")
            total_errors += 1

      except Exception as e:
        context.log.warning(f"Failed to process {key}: {e}")
        total_errors += 1

    # Progress log every 10 companies
    if list(filings_by_cik.keys()).index(cik) % 10 == 0:
      context.log.info(
        f"Progress: {total_processed} filings processed, {total_errors} errors"
      )

  context.log.info(
    f"Processing complete for year {year}: "
    f"{total_processed} filings, {total_records} files, {total_errors} errors"
  )

  return MaterializeResult(
    metadata={
      "year": year,
      "filings_processed": total_processed,
      "files_created": total_records,
      "errors": total_errors,
    }
  )


@asset(
  group_name="sec_pipeline",
  description="Stage and materialize processed parquet files to LadybugDB graph",
  compute_kind="load",
  deps=[sec_processed_filings],
  metadata={
    "pipeline": "sec",
    "stage": "staging",
  },
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
