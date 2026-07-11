"""
SEC Filing Processing.

This module contains functions for processing individual SEC XBRL filings
into parquet format for downstream consolidation and ingestion.
"""

import gc
import os
import tempfile
import zipfile
from dataclasses import dataclass
from io import BytesIO
from typing import TYPE_CHECKING

if TYPE_CHECKING:
  from robosystems.adapters.sec.processors.metadata import SECMetadataLoader

# Import from specific modules to avoid circular imports
from arelle.UrlUtil import IXDS_DOC_SEPARATOR, IXDS_SURROGATE

from robosystems.adapters.sec.client.edgar import SEC_BASE_URL
from robosystems.adapters.sec.processors.xbrl_graph import XBRLGraphProcessor

INLINE_XBRL_NAMESPACE = b"http://www.xbrl.org/2013/inlineXBRL"


def _inline_xbrl_documents(tmpdir: str, htm_files: list[str]) -> list[str]:
  """Return the subset of .htm files that are inline XBRL documents.

  Multi-document filings (common for 40-F/20-F) split one logical instance
  across several files: contexts, units, and schema references live in the
  primary document while tagged facts live in continuation documents. Each
  member declares the inline XBRL namespace on its root element, so a head
  scan is sufficient to identify them.
  """
  members = []
  for f in htm_files:
    try:
      with open(os.path.join(tmpdir, f), "rb") as fh:
        head = fh.read(65536)
    except OSError:
      continue
    if INLINE_XBRL_NAMESPACE in head:
      members.append(f)
  return sorted(members)


@dataclass
class ProcessedFilingResult:
  """Result from processing a single filing."""

  success: bool
  source_file_id: str
  partition_key: str
  tables: dict[str, bytes]  # table_key -> parquet bytes (e.g., "nodes/Entity")
  filing_date: str | None = None  # YYYY-MM-DD from SEC metadata
  error: str | None = None
  skipped_reason: str | None = None  # Set when filing is filtered out (e.g., form type)


def process_single_filing_to_memory(
  storage_key: str,
  partition_key: str,
  source_file_id: str,
  s3_client,
  raw_bucket: str,
  metadata_loader: "SECMetadataLoader",
  allowed_form_types: list[str] | None = None,
  enricher=None,
) -> ProcessedFilingResult:
  """Process a single filing and return parquet data in memory.

  This function processes a filing but does NOT write to S3 or update SourceFile status.
  The caller is responsible for:
  - Consolidating results from multiple filings
  - Writing consolidated parquet files
  - Updating SourceFile status

  Args:
      storage_key: S3 key of the raw file
      partition_key: Partition key (year_cik_accession)
      source_file_id: SourceFile ID for tracking
      s3_client: boto3 S3 client
      raw_bucket: S3 bucket for raw files
      metadata_loader: SECMetadataLoader instance for fetching SEC metadata
      allowed_form_types: If set, only process filings with these form types.
          Filings with non-matching types return success with empty tables
          and skipped_reason set.
      enricher: Shared SemanticEnricher instance. If provided, reuses the model
          across filings instead of loading a new one per filing.

  Returns:
      ProcessedFilingResult with parquet data or error
  """
  # Parse partition key to get year, cik, accession
  parts = partition_key.split("_", 2)
  if len(parts) != 3:
    return ProcessedFilingResult(
      success=False,
      source_file_id=source_file_id,
      partition_key=partition_key,
      tables={},
      error=f"Invalid partition key: {partition_key}",
    )

  year, cik, accession = parts

  # Download raw ZIP
  try:
    buffer = BytesIO()
    s3_client.download_fileobj(raw_bucket, storage_key, buffer)
    buffer.seek(0)
  except Exception as e:
    return ProcessedFilingResult(
      success=False,
      source_file_id=source_file_id,
      partition_key=partition_key,
      tables={},
      error=f"Download failed: {e}",
    )

  # Extract and process
  processor = None
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
        return ProcessedFilingResult(
          success=False,
          source_file_id=source_file_id,
          partition_key=partition_key,
          tables={},
          error="No XBRL files found",
        )

      # Build report URL
      cik_int = int(cik)
      accno_clean = accession.replace("-", "")
      report_url = (
        f"{SEC_BASE_URL}/Archives/edgar/data/{cik_int}/{accno_clean}/{xbrl_files[0]}"
      )

      # Multi-document inline XBRL filings (IXDS) split one logical instance
      # across several .htm files; they must be loaded together so contexts
      # defined in the primary document resolve for facts tagged in the others.
      instance_file_path = os.path.join(tmpdir, xbrl_files[0])
      ixds_members = _inline_xbrl_documents(tmpdir, htm_files)
      if len(ixds_members) > 1:
        instance_file_path = os.path.join(
          tmpdir, IXDS_SURROGATE
        ) + IXDS_DOC_SEPARATOR.join(os.path.join(tmpdir, f) for f in ixds_members)
      elif len(ixds_members) == 1:
        instance_file_path = os.path.join(tmpdir, ixds_members[0])

      # Schema config
      schema_config = {
        "name": "SEC Database Schema",
        "description": "Complete financial reporting schema with XBRL taxonomy support",
        "base_schema": "base",
        "extensions": ["roboledger"],
      }

      # Fetch full SEC metadata from S3 snapshot
      sec_filer, sec_report = metadata_loader.get_metadata(
        cik, accession, s3_client=s3_client, bucket=raw_bucket
      )
      if not sec_report.get("primaryDocument"):
        sec_report["primaryDocument"] = xbrl_files[0]

      # Check form type filter before heavy XBRL processing
      if allowed_form_types:
        form_type = sec_report.get("form", "")
        if form_type not in allowed_form_types:
          return ProcessedFilingResult(
            success=True,
            source_file_id=source_file_id,
            partition_key=partition_key,
            tables={},
            skipped_reason=f"form_type={form_type}",
          )

      # Process with XBRLGraphProcessor
      processor = XBRLGraphProcessor(
        report_uri=report_url,
        entityId=cik,
        sec_filer=sec_filer,
        sec_report=sec_report,
        output_dir=tmpdir,
        local_file_path=instance_file_path,
        schema_config=schema_config,
        enricher=enricher,
      )

      processor.process()

      # Extract filing date from SEC metadata for output partitioning
      filing_date = sec_report.get("filingDate")

      # Collect parquet files as bytes (don't write to S3 yet)
      tables: dict[str, bytes] = {}

      for entity_type in ["nodes", "relationships"]:
        entity_dir = os.path.join(tmpdir, entity_type)
        if os.path.exists(entity_dir):
          for parquet_file in os.listdir(entity_dir):
            if parquet_file.endswith(".parquet"):
              local_path = os.path.join(entity_dir, parquet_file)
              table_name = parquet_file.replace(".parquet", "")
              # Key format: "nodes/TableName" or "relationships/RelName"
              key = f"{entity_type}/{table_name}"
              with open(local_path, "rb") as f:
                tables[key] = f.read()

      # A processed filing with no facts means extraction silently failed
      # (e.g. wrong instance document selected). Surface it as an error so
      # the SourceFile lands in a retryable state instead of masking data
      # loss as success.
      if "nodes/Fact" not in tables:
        return ProcessedFilingResult(
          success=False,
          source_file_id=source_file_id,
          partition_key=partition_key,
          tables={},
          filing_date=filing_date,
          error="XBRL processing produced zero facts",
        )

      return ProcessedFilingResult(
        success=True,
        source_file_id=source_file_id,
        partition_key=partition_key,
        tables=tables,
        filing_date=filing_date,
        error=None,
      )

  except Exception as e:
    return ProcessedFilingResult(
      success=False,
      source_file_id=source_file_id,
      partition_key=partition_key,
      tables={},
      filing_date=None,
      error=str(e),
    )
  finally:
    # Always close the buffer to release memory
    buffer.close()
    # Clean up processor if it was created (releases DataFrames, etc.)
    if processor is not None:
      del processor
    gc.collect()
