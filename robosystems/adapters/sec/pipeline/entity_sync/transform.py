"""SEC Entity Sync Transform Asset.

Processes raw XBRL ZIPs for a single CIK using XBRLGraphProcessor,
consolidates parquet tables across filings, and writes output for loading.
"""

import gc
import json

from dagster import AssetExecutionContext, MaterializeResult, asset

from .configs import SECEntitySyncConfig
from .utils import get_pipeline_work_dir


@asset(
  group_name="sec_entity_pipeline",
  description="Process XBRL filings for a CIK into graph parquet tables",
  deps=["sec_entity_extract"],
  kinds={"xbrl", "parquet"},
  metadata={
    "pipeline": "sec_entity",
    "stage": "transform",
  },
)
def sec_entity_transform(
  context: AssetExecutionContext,
  config: SECEntitySyncConfig,
) -> MaterializeResult:
  """Process XBRL filings and produce consolidated parquet tables.

  Reads the filing manifest from the extract step, processes each
  filing with XBRLGraphProcessor, consolidates parquet tables across
  filings (deduplicating shared tables like Element, Label, etc.),
  and writes the final output parquet files.

  Returns:
      MaterializeResult with processing statistics
  """
  from io import BytesIO

  import boto3
  import pyarrow as pa
  import pyarrow.parquet as pq

  from robosystems.adapters.sec.processors.constants import SHARED_NODE_TABLES
  from robosystems.adapters.sec.processors.metadata import SECMetadataLoader
  from robosystems.adapters.sec.processors.processing import (
    process_single_filing_to_memory,
  )

  work_dir = get_pipeline_work_dir(config.graph_id)
  manifest_path = work_dir / "filing_manifest.json"

  if not manifest_path.exists():
    raise ValueError(
      f"Filing manifest not found at {manifest_path}. Run sec_entity_extract first."
    )

  manifest = json.loads(manifest_path.read_text())
  filings = manifest["filings"]
  bucket = manifest["bucket"]
  form_types = manifest.get("form_types", config.form_types)

  context.log.info(
    f"Processing {len(filings)} filings for CIK={config.cik}, form_types={form_types}"
  )

  if not filings:
    context.log.warning("No filings to process")
    return MaterializeResult(
      metadata={
        "graph_id": config.graph_id,
        "cik": config.cik,
        "filings_processed": 0,
        "filings_skipped": 0,
        "filings_failed": 0,
        "tables_output": 0,
      }
    )

  s3_client = boto3.client("s3")
  metadata_loader = SECMetadataLoader()

  # Initialize enricher only if enrichment is enabled
  enricher = None
  if not config.skip_enrichment:
    try:
      from robosystems.adapters.sec.enrichment import SemanticEnricher

      enricher = SemanticEnricher()
      context.log.info("Semantic enrichment enabled")
    except Exception as e:
      context.log.warning(f"Failed to initialize enricher, skipping: {e}")

  # Process each filing and collect results
  # Accumulate Arrow tables by key for consolidation
  tables_by_key: dict[str, list[pa.Table]] = {}
  processed = 0
  skipped = 0
  failed = 0

  for i, filing in enumerate(filings):
    storage_key = filing["storage_key"]
    accession = filing["accession"]
    year = filing["year"]

    # Build partition key matching shared pipeline format
    partition_key = f"{year}_{config.cik}_{accession}"

    context.log.info(f"Processing filing {i + 1}/{len(filings)}: {accession} ({year})")

    try:
      result = process_single_filing_to_memory(
        storage_key=storage_key,
        partition_key=partition_key,
        source_file_id=f"entity_sync_{accession}",
        s3_client=s3_client,
        raw_bucket=bucket,
        metadata_loader=metadata_loader,
        allowed_form_types=form_types,
        enricher=enricher,
      )

      if not result.success:
        context.log.warning(f"Filing {accession} failed: {result.error}")
        failed += 1
        continue

      if result.skipped_reason:
        context.log.debug(f"Filing {accession} skipped: {result.skipped_reason}")
        skipped += 1
        continue

      if not result.tables:
        context.log.debug(f"Filing {accession} produced no tables")
        skipped += 1
        continue

      # Accumulate tables
      for key, parquet_bytes in result.tables.items():
        if key not in tables_by_key:
          tables_by_key[key] = []
        reader = pq.ParquetFile(BytesIO(parquet_bytes))
        table = reader.read()
        tables_by_key[key].append(table)

      processed += 1

    except Exception as e:
      context.log.error(f"Filing {accession} error: {e}")
      failed += 1

    # Periodic GC to manage memory
    if (i + 1) % 10 == 0:
      gc.collect()
      context.log.info(
        f"Progress: {i + 1}/{len(filings)} "
        f"({processed} processed, {skipped} skipped, {failed} failed)"
      )

  context.log.info(
    f"Processing complete: {processed} processed, {skipped} skipped, {failed} failed"
  )

  # Consolidate tables: concat and dedup shared tables
  output_dir = work_dir / "output"
  output_dir.mkdir(parents=True, exist_ok=True)
  tables_output = 0

  for key, tables in tables_by_key.items():
    if not tables:
      continue

    combined = pa.concat_tables(tables, promote_options="permissive")
    del tables

    # Dedup shared node tables on identifier column
    if key in SHARED_NODE_TABLES and "identifier" in combined.column_names:
      original = combined.num_rows
      identifiers = combined.column("identifier")
      seen: set[str] = set()
      keep: list[int] = []
      for idx, val in enumerate(identifiers.to_pylist()):
        if val not in seen:
          seen.add(val)
          keep.append(idx)
      if len(keep) < original:
        combined = combined.take(keep)
        context.log.info(f"Deduped {key}: {original} → {combined.num_rows} rows")

    # Write output parquet
    # key format: "nodes/Entity" or "relationships/ENTITY_HAS_REPORT"
    entity_type, table_name = key.split("/", 1)
    out_file = output_dir / f"sec_{table_name}.parquet"
    pq.write_table(combined, str(out_file))
    tables_output += 1

    context.log.info(f"Wrote {key}: {combined.num_rows} rows → {out_file.name}")
    del combined

  gc.collect()

  # Write table metadata for the load step
  table_meta: dict[str, dict[str, str]] = {}
  for key in tables_by_key:
    entity_type, table_name = key.split("/", 1)
    table_meta[table_name] = {
      "entity_type": entity_type,
      "table_name": table_name,
    }

  meta_path = work_dir / "table_metadata.json"
  meta_path.write_text(json.dumps(table_meta, indent=2))

  return MaterializeResult(
    metadata={
      "graph_id": config.graph_id,
      "cik": config.cik,
      "filings_processed": processed,
      "filings_skipped": skipped,
      "filings_failed": failed,
      "tables_output": tables_output,
      "table_names": sorted(table_meta.keys()),
    }
  )
