"""Process one SEC XBRL filing into in-memory parquet tables."""

import gc
import os
import tempfile
import zipfile
from dataclasses import dataclass
from io import BytesIO
from typing import TYPE_CHECKING

if TYPE_CHECKING:
  from robosystems.adapters.sec.processors.metadata import SECMetadataLoader

from arelle.UrlUtil import IXDS_DOC_SEPARATOR, IXDS_SURROGATE

from robosystems.adapters.sec.client.edgar import SEC_BASE_URL
from robosystems.adapters.sec.processors.xbrl_graph import XBRLGraphProcessor

INLINE_XBRL_NAMESPACE = b"http://www.xbrl.org/2013/inlineXBRL"


def _inline_xbrl_documents(tmpdir: str, htm_files: list[str]) -> list[str]:
  """The .htm files that declare the inline XBRL namespace (a head scan suffices)."""
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
  success: bool
  source_file_id: str
  partition_key: str
  tables: dict[str, bytes]  # table_key -> parquet bytes (e.g., "nodes/Entity")
  filing_date: str | None = None  # YYYY-MM-DD from SEC metadata
  error: str | None = None
  skipped_reason: str | None = None  # set when filtered out, e.g. by form type


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
  """Process one filing into parquet bytes in memory.

  Writes nothing to S3 and leaves SourceFile status to the caller.
  ``partition_key`` is ``year_cik_accession``. A form type outside
  ``allowed_form_types`` returns success with empty tables and
  ``skipped_reason`` set. Pass a shared ``enricher`` to reuse its model.
  """
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

  processor = None
  try:
    with tempfile.TemporaryDirectory() as tmpdir:
      with zipfile.ZipFile(buffer, "r") as zf:
        zf.extractall(tmpdir)

      exclude_suffixes = ("_def.xml", "_lab.xml", "_pre.xml", "_cal.xml", ".xsd")
      all_files = os.listdir(tmpdir)
      xbrl_files = [
        f
        for f in all_files
        if f.endswith((".xml", ".htm", ".html"))
        and not any(f.endswith(suffix) for suffix in exclude_suffixes)
      ]

      # Prefer inline XBRL, largest file first.
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

      cik_int = int(cik)
      accno_clean = accession.replace("-", "")
      report_url = (
        f"{SEC_BASE_URL}/Archives/edgar/data/{cik_int}/{accno_clean}/{xbrl_files[0]}"
      )

      # A multi-document inline filing (IXDS) loads as one instance so the
      # primary document's contexts resolve for facts in the other members.
      instance_file_path = os.path.join(tmpdir, xbrl_files[0])
      ixds_members = _inline_xbrl_documents(tmpdir, htm_files)
      if len(ixds_members) > 1:
        instance_file_path = os.path.join(
          tmpdir, IXDS_SURROGATE
        ) + IXDS_DOC_SEPARATOR.join(os.path.join(tmpdir, f) for f in ixds_members)
      elif len(ixds_members) == 1:
        instance_file_path = os.path.join(tmpdir, ixds_members[0])

      schema_config = {
        "name": "SEC Database Schema",
        "description": "Complete financial reporting schema with XBRL taxonomy support",
        "base_schema": "base",
        "extensions": ["roboledger"],
      }

      sec_filer, sec_report = metadata_loader.get_metadata(
        cik, accession, s3_client=s3_client, bucket=raw_bucket
      )
      if not sec_report.get("primaryDocument"):
        sec_report["primaryDocument"] = xbrl_files[0]

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

      filing_date = sec_report.get("filingDate")

      tables: dict[str, bytes] = {}

      for entity_type in ["nodes", "relationships"]:
        entity_dir = os.path.join(tmpdir, entity_type)
        if os.path.exists(entity_dir):
          for parquet_file in os.listdir(entity_dir):
            if parquet_file.endswith(".parquet"):
              local_path = os.path.join(entity_dir, parquet_file)
              table_name = parquet_file.replace(".parquet", "")
              key = f"{entity_type}/{table_name}"
              with open(local_path, "rb") as f:
                tables[key] = f.read()

      # No facts means extraction silently failed (e.g. the wrong instance
      # document); an error leaves the SourceFile retryable.
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
    buffer.close()
    if processor is not None:
      del processor
    gc.collect()
