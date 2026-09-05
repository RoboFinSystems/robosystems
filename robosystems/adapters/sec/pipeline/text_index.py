"""SEC Text Search Indexing Assets.

Two assets for indexing SEC filing text content into OpenSearch:

1. sec_narratives_indexed — Extract and index narrative sections from raw filings
   Reads raw ZIP files, extracts 10-K/10-Q HTML, detects Item sections,
   externalizes clean text to S3/CDN, indexes into OpenSearch.

2. sec_ixbrl_disclosures_indexed — Extract iXBRL disclosure sections with XBRL element metadata
   Reads raw ZIP files, parses iXBRL text blocks (ix:nonNumeric TextBlock elements),
   extracts nested XBRL element qnames, indexes with element metadata for
   bidirectional navigation between knowledge graph and document search.
   Also resolves CDN content_url from externalized Fact data for full HTML retrieval.

Both parsers come from ``xbrlkit.text``. A section longer than ``part_size``
is indexed as consecutive parts: each part is its own document carrying
``part`` / ``part_count``, a ``parent_document_id`` shared by the section's
parts, and the ``next_document_id`` to read on. An unsplit section keeps the
document id it always had.

All depend on sec_processed_filings (need Entity/Report metadata from parquets).
All run parallel to the DuckDB staging branch.
"""

import gc
import hashlib
import io
import math
import re
import zipfile
from typing import Any

import boto3
from dagster import AssetExecutionContext, BackfillPolicy, MaterializeResult, asset

from robosystems.config import env
from robosystems.config.storage.shared import (
  DataSourceType,
  get_processed_key,
  get_public_data_url,
  get_raw_key,
)
from robosystems.logger import logger

from .configs import (
  SECiXBRLIndexConfig,
  SECNarrativeIndexConfig,
  sec_quarter_partitions,
)

EMBEDDING_MODEL_NAME = "bge-small-en-v1.5"

# bge-small-en-v1.5 has a 512-token context window (~2000 chars).
# Text beyond this is silently truncated by the tokenizer, so passing
# full 50-100KB textblocks wastes memory on tokenizer buffers without
# improving embedding quality.
_EMBEDDING_MAX_CHARS = 2000

# Smaller batches reduce peak memory from tokenizer/ONNX intermediate
# buffers. 200 docs x 2000 chars = 400KB vs 1000 x 50KB = 50MB.
_EMBEDDING_BATCH_SIZE = 200


def _embed_document_batch(enricher, documents: list[dict]) -> bool:
  """Add embeddings to documents in-place using fastembed.

  Called before bulk_index to generate embeddings for hybrid search.
  Returns True if embeddings were added, False on failure (documents
  are still indexed without embeddings).
  """
  try:
    texts = [doc.get("content", "")[:_EMBEDDING_MAX_CHARS] for doc in documents]
    embeddings = enricher.embed_batch(texts)
    for doc, emb in zip(documents, embeddings, strict=True):
      doc["embedding"] = emb
      doc["embedding_model"] = EMBEDDING_MODEL_NAME
    return True
  except Exception as e:
    logger.warning(f"Embedding generation failed, indexing without vectors: {e}")
    return False


def _part_document_ids(
  graph_id: str, source: str, accession: str, section: Any
) -> tuple[str, str | None, str | None]:
  """(document_id, parent_document_id, next_document_id) for one section part.

  An unsplit section's id is ``sha256(graph:source:accession:section_id)``,
  as it always was, with no parent and no next. A part's id adds its part
  number; every part of the section shares the unsplit id as its parent.
  """

  def _id(*keys: str) -> str:
    return hashlib.sha256(":".join(keys).encode()).hexdigest()[:16]

  section_key = (graph_id, source, accession, section.section_id)
  if section.part_count <= 1:
    return _id(*section_key), None, None
  own = _id(*section_key, str(section.part))
  next_id = (
    _id(*section_key, str(section.part + 1))
    if section.part < section.part_count
    else None
  )
  return own, _id(*section_key), next_id


def _get_s3_client():
  """Get S3 client (handles LocalStack for dev)."""
  kwargs: dict[str, Any] = {"region_name": env.AWS_REGION}
  if env.ENVIRONMENT == "dev":
    endpoint = env.AWS_ENDPOINT_URL
    if endpoint:
      kwargs["endpoint_url"] = endpoint
  return boto3.client("s3", **kwargs)


def _get_processed_bucket() -> str:
  """Get the processed data S3 bucket name."""
  return env.SHARED_PROCESSED_BUCKET


def _get_raw_bucket() -> str:
  """Get the raw data S3 bucket name."""
  return env.SHARED_RAW_BUCKET


def _get_public_data_bucket() -> str:
  """Get the public data bucket for externalized content."""
  return env.PUBLIC_DATA_BUCKET


def _get_public_data_cdn_url() -> str:
  """Get the CDN URL for public data."""
  return env.PUBLIC_DATA_CDN_URL


def _get_indexed_accessions(
  os_client, graph_id: str, source_type: str | None = None
) -> set[str]:
  """Get accession numbers already indexed for a graph_id and source_type.

  Uses a composite aggregation to paginate through all unique accession numbers
  without an upper bound. Returns empty set if index doesn't exist.

  Args:
    os_client: OpenSearch client instance
    graph_id: Tenant filter
    source_type: Optional source type filter (e.g., "xbrl_textblock",
      "narrative_section", "ixbrl_disclosure"). When set, only returns
      accessions indexed for that specific source type, allowing each
      asset to track its own progress independently.
  """
  accessions: set[str] = set()
  after: dict | None = None

  try:
    # Build query with mandatory graph_id + optional source_type filter
    query_filter: list[dict] = [{"term": {"graph_id": graph_id}}]
    if source_type:
      query_filter.append({"term": {"source_type": source_type}})

    while True:
      agg: dict = {
        "composite": {
          "sources": [{"accession": {"terms": {"field": "accession_number"}}}],
          "size": 10000,
        }
      }
      if after:
        agg["composite"]["after"] = after

      result = os_client.client.search(
        index=os_client.index_name,
        body={
          "size": 0,
          "query": {"bool": {"filter": query_filter}},
          "aggs": {"accessions": agg},
        },
      )

      buckets = result["aggregations"]["accessions"]["buckets"]
      if not buckets:
        break

      accessions.update(b["key"]["accession"] for b in buckets)
      after = buckets[-1]["key"]

      if len(buckets) < 10000:
        break

  except Exception as e:
    logger.warning(f"Failed to query indexed accessions: {e}")

  return accessions


def _list_s3_parquet_keys(s3, bucket: str, prefix: str) -> list[str]:
  """List all .parquet keys under a prefix."""
  keys: list[str] = []
  paginator = s3.get_paginator("list_objects_v2")
  for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
    for obj in page.get("Contents", []):
      if obj["Key"].endswith(".parquet"):
        keys.append(obj["Key"])
  return keys


def _read_parquets_from_s3(
  s3, bucket: str, keys: list[str], columns: list[str] | None = None
):
  """Read multiple parquet files from S3 and concatenate into a single PyArrow table.

  Args:
    columns: If specified, only read these columns from each parquet file.
      This avoids loading large columns (e.g. embeddings) that aren't needed.
  """
  import pyarrow.parquet as pq

  tables = []
  for key in keys:
    try:
      response = s3.get_object(Bucket=bucket, Key=key)
      buf = io.BytesIO(response["Body"].read())
      table = pq.read_table(buf, columns=columns)
      tables.append(table)
    except Exception as e:
      logger.warning(f"Failed to read parquet {key}: {e}")
  if not tables:
    return None

  import pyarrow as pa

  return pa.concat_tables(tables, promote_options="default")


def _extract_html_from_zip(zip_bytes: bytes) -> str | None:
  """Extract the main filing HTML from a ZIP file.

  Strategy: prefer the file containing a dei:DocumentType iXBRL tag (the actual
  filing), falling back to the largest non-exhibit HTM file. The iXBRL check
  handles cases where exhibits (e.g., credit card agreements) are larger than
  the filing itself (seen with COST, where 'costex102...' exhibit is 1.4MB vs
  the 832KB filing).
  """
  with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
    htm_candidates: list[tuple[str, int]] = []
    for info in zf.infolist():
      name_lower = info.filename.lower()
      if not name_lower.endswith((".htm", ".html")):
        continue
      # Skip known non-filing files
      if any(
        skip in name_lower
        for skip in [
          "filingsummary",
          "metalinks",
          "defnref",
          "_cal.",
          "_def.",
          "_lab.",
          "_pre.",
          ".xsd",
        ]
      ):
        continue
      # Skip R-files (viewer artifacts)
      basename = name_lower.rsplit("/", 1)[-1]
      if re.match(r"^r\d+\.htm", basename):
        continue
      htm_candidates.append((info.filename, info.file_size))

    if not htm_candidates:
      return None

    # Strategy 1: Find the file with dei:DocumentType iXBRL tag (the actual filing)
    # Check candidates from largest to smallest (filing is usually large)
    # Read only first 2MB for the iXBRL check to avoid decoding large exhibits
    for filename, _ in sorted(htm_candidates, key=lambda x: -x[1]):
      raw = zf.read(filename)
      prefix = raw[:2_000_000].decode("utf-8", errors="replace")
      if _extract_ixbrl_doc_type(prefix) is not None:
        return raw.decode("utf-8", errors="replace")

    # Strategy 2: Fall back to largest non-exhibit file (pre-iXBRL filings)
    fallback = [
      (f, s)
      for f, s in htm_candidates
      if not any(
        pat in f.lower().rsplit("/", 1)[-1]
        for pat in ["ex1", "ex2", "ex3", "ex4", "consent", "subsidiar"]
      )
    ]
    candidates = fallback or htm_candidates
    main_file = max(candidates, key=lambda x: x[1])[0]
    return zf.read(main_file).decode("utf-8", errors="replace")


def _extract_ixbrl_doc_type(html: str) -> str | None:
  """Extract dei:DocumentType from iXBRL header.

  Returns the document type (e.g., '10-K', '10-Q', 'DEF 14A', '20-F')
  or None if not found. This is more reliable than regex on visible text.

  Handles two common iXBRL patterns:
  1. Direct value: <ix:nonNumeric name="dei:DocumentType">10-K</ix:nonNumeric>
  2. Nested span: <ix:nonNumeric name="dei:DocumentType"><span>10-K</span></ix:nonNumeric>
  """
  match = re.search(
    r'name=["\']dei:DocumentType["\'][^>]*>(.*?)</ix:non',
    html[:2000000],
    re.IGNORECASE | re.DOTALL,
  )
  if match:
    # Strip any nested HTML tags to get the plain text value
    value = re.sub(r"<[^>]+>", "", match.group(1)).strip()
    if value:
      return value
  return None


@asset(
  group_name="sec_pipeline",
  description="Extract and index narrative sections from SEC filings into OpenSearch",
  kinds={"opensearch", "s3"},
  deps=["sec_processed_filings"],
  partitions_def=sec_quarter_partitions,
  backfill_policy=BackfillPolicy.single_run(),
  metadata={
    "pipeline": "sec",
    "stage": "text_index",
    "source": "narrative_sections",
  },
)
def sec_narratives_indexed(
  context: AssetExecutionContext,
  config: SECNarrativeIndexConfig,
) -> MaterializeResult:
  """Extract narrative sections from raw 10-K/10-Q filings and index into OpenSearch.

  Partitioned by quarter (e.g. 2026-Q1). For each filing ZIP in the raw bucket:
  1. Extract HTML document from ZIP
  2. Run NarrativeExtractor to detect and extract Item sections
  3. Upload clean text to public data S3 bucket (CDN-served)
  4. Index into OpenSearch with content + content_url
  """
  from xbrlkit.text import NarrativeExtractor

  from robosystems.operations.search.client import OpenSearchClient

  s3 = _get_s3_client()
  raw_bucket = _get_raw_bucket()
  processed_bucket = _get_processed_bucket()
  public_bucket = _get_public_data_bucket()
  cdn_url = _get_public_data_cdn_url()

  extractor = NarrativeExtractor(part_size=config.part_size or None)

  os_client = OpenSearchClient(env.OPENSEARCH_URL, env.OPENSEARCH_INDEX)
  os_client.create_index_if_not_exists()

  # Load fastembed for embedding generation (required for hybrid search)
  from robosystems.adapters.sec.enrichment import SemanticEnricher

  enricher = SemanticEnricher()
  context.log.info("Loaded SemanticEnricher for embedding generation")

  # Get already-indexed accessions for incremental skip (scoped to this source type)
  if config.force_reindex:
    indexed_accessions: set[str] = set()
    context.log.info("Force reindex enabled — will re-index all documents")
  else:
    indexed_accessions = _get_indexed_accessions(
      os_client, config.graph_id, source_type="narrative_section"
    )
  if indexed_accessions:
    context.log.info(
      f"Found {len(indexed_accessions)} already-indexed accessions, will skip"
    )

  # Use partition key to scope to a single quarter
  partition_key = context.partition_key  # e.g. "2026-Q1"
  prefix_base = get_processed_key(DataSourceType.SEC, "processed")
  partition_prefix = f"{prefix_base}/filed={partition_key}"

  context.log.info(
    f"Scanning parquets from s3://{processed_bucket}/{partition_prefix}/"
  )

  all_parquet_keys = _list_s3_parquet_keys(s3, processed_bucket, partition_prefix)
  context.log.info(f"Found {len(all_parquet_keys)} parquets for {partition_key}")

  report_keys = [k for k in all_parquet_keys if "/nodes/Report/" in k]
  entity_keys = [k for k in all_parquet_keys if "/nodes/Entity/" in k]
  ehr_keys = [k for k in all_parquet_keys if "/relationships/ENTITY_HAS_REPORT/" in k]

  context.log.info(f"Reading {len(report_keys)} Report parquets for metadata...")
  report_table = _read_parquets_from_s3(
    s3,
    processed_bucket,
    report_keys,
    columns=[
      "identifier",
      "accession_number",
      "form",
      "filing_date",
      "fiscal_year_focus",
      "fiscal_period_focus",
    ],
  )
  entity_table = _read_parquets_from_s3(
    s3,
    processed_bucket,
    entity_keys,
    columns=["identifier", "ticker", "name", "cik"],
  )
  ehr_table = _read_parquets_from_s3(s3, processed_bucket, ehr_keys)

  if report_table is None:
    context.log.warning("No Report parquets found")
    return MaterializeResult(
      metadata={"status": "no_data", "graph_id": config.graph_id}
    )

  reports_df = report_table.to_pandas()
  context.log.info(f"Loaded {len(reports_df)} reports")

  # Filter to target form types
  form_types_upper = [ft.upper() for ft in config.form_types]
  target_reports = reports_df[
    reports_df["form"].str.upper().isin(form_types_upper)
  ].copy()
  context.log.info(
    f"Found {len(target_reports)} reports matching form types {form_types_upper}"
  )

  # Build entity lookup via relationship
  entity_lookup: dict[str, dict[str, str]] = {}
  report_to_entity: dict[str, str] = {}
  if entity_table is not None:
    entities_df = entity_table.to_pandas()
    for _, row in entities_df.iterrows():
      entity_lookup[row.get("identifier")] = {
        "ticker": str(row.get("ticker", "")),
        "name": str(row.get("name", "")),
        "cik": str(row.get("cik", "")),
      }
  if ehr_table is not None:
    ehr_df = ehr_table.to_pandas()
    for _, row in ehr_df.iterrows():
      report_to_entity[row.get("to")] = row.get("from")

  # Build accession → report metadata + entity lookup
  accession_metadata: dict[str, dict[str, Any]] = {}
  for _, report in target_reports.iterrows():
    accession = report.get("accession_number", "")
    if not accession:
      continue
    entity_id = report_to_entity.get(report.get("identifier"))
    entity_info = entity_lookup.get(entity_id, {}) if entity_id else {}
    fy = report.get("fiscal_year_focus")
    accession_metadata[accession] = {
      "form_type": report.get("form", ""),
      "filing_date": str(report.get("filing_date", "")),
      "fiscal_year": int(fy) if fy is not None and not math.isnan(fy) else None,
      "fiscal_period": report.get("fiscal_period_focus", ""),
      "cik": entity_info.get("cik", ""),
      "ticker": entity_info.get("ticker", ""),
      "entity_name": entity_info.get("name", ""),
    }

  context.log.info(f"Built metadata for {len(accession_metadata)} accessions")

  # Free intermediate data before potentially loading fastembed model
  del reports_df, report_table, target_reports, entity_lookup, report_to_entity
  del all_parquet_keys, report_keys, entity_keys, ehr_keys
  gc.collect()

  # List raw ZIPs and process those matching our target accessions
  # A section part is about part_size chars (25K by default, at most ~1.3x that).
  # Batch of 100 x ~32KB = ~3MB, safely under OpenSearch's 10MB bulk request limit.
  batch_size = 100
  documents: list[dict[str, Any]] = []
  total_indexed = 0
  filings_processed = 0
  sections_extracted = 0
  errors = 0

  # Scan raw bucket for ZIPs scoped to this partition's year
  # Raw ZIPs use year= partitions (annual), but accession_metadata
  # from the quarterly processed parquets handles fine-grained filtering
  partition_year = partition_key.split("-Q")[0]  # "2026-Q1" → "2026"
  raw_prefix = f"{get_raw_key(DataSourceType.SEC)}/year={partition_year}"
  paginator = s3.get_paginator("list_objects_v2")

  zip_keys: list[str] = []
  for page in paginator.paginate(Bucket=raw_bucket, Prefix=raw_prefix):
    for obj in page.get("Contents", []):
      if obj["Key"].endswith(".zip"):
        zip_keys.append(obj["Key"])

  context.log.info(f"Found {len(zip_keys)} raw ZIP files for year={partition_year}")

  with os_client.bulk_write_mode(write_interval="5s"):
    for zip_key in zip_keys:
      # Extract accession from key: sec/year=2026/CIK/ACCESSION.zip
      filename = zip_key.rsplit("/", 1)[-1]
      accession = filename.replace(".zip", "")

      # Skip if not in our target metadata
      if accession not in accession_metadata:
        continue

      # Skip already-indexed accessions (incremental)
      if accession in indexed_accessions:
        continue

      meta = accession_metadata[accession]

      # Extract CIK and year from key path
      parts = zip_key.split("/")
      year_part = next((p for p in parts if p.startswith("year=")), "")
      year = year_part.replace("year=", "") if year_part else ""
      cik = meta.get("cik", "")

      try:
        response = s3.get_object(Bucket=raw_bucket, Key=zip_key)
        zip_bytes = response["Body"].read()

        html_content = _extract_html_from_zip(zip_bytes)
        if not html_content:
          context.log.debug(f"No HTML found in {zip_key}")
          errors += 1
          continue

        # Verify actual document type via iXBRL tag (catches proxy statements
        # that are larger than the actual filing in the same accession ZIP)
        ixbrl_doc_type = _extract_ixbrl_doc_type(html_content)
        if ixbrl_doc_type and ixbrl_doc_type.upper() not in form_types_upper:
          context.log.debug(
            f"Skipping {accession}: iXBRL doc type '{ixbrl_doc_type}' "
            f"not in {form_types_upper}"
          )
          continue

        # A re-index replaces the accession's documents: the part count of a
        # section, and so its document ids, can change between parser versions.
        if config.force_reindex:
          os_client.delete_by_accession(config.graph_id, "narrative_section", accession)

        # Extract narrative sections
        sections = extractor.extract(html_content, ixbrl_doc_type or meta["form_type"])
        filings_processed += 1

        if not sections:
          continue

        for section in sections:
          sections_extracted += 1

          # Externalize clean text to public S3 bucket, one object per part
          part_suffix = f"_part{section.part}" if section.part_count > 1 else ""
          narrative_key = (
            f"{year}/{cik}/{accession}/narrative_{section.section_id}{part_suffix}.txt"
          )
          content_url_value = ""

          if public_bucket:
            try:
              s3.put_object(
                Bucket=public_bucket,
                Key=narrative_key,
                Body=section.content.encode("utf-8"),
                ContentType="text/plain; charset=utf-8",
              )
              content_url_value = get_public_data_url(
                public_bucket, narrative_key, cdn_url
              )
            except Exception as e:
              context.log.debug(f"Failed to externalize {narrative_key}: {e}")

          doc_id, parent_id, next_id = _part_document_ids(
            config.graph_id, "narr", accession, section
          )

          documents.append(
            {
              "graph_id": config.graph_id,
              "document_id": doc_id,
              "source_type": "narrative_section",
              "entity_ticker": meta.get("ticker") or cik,
              "entity_name": meta.get("entity_name"),
              "entity_cik": cik,
              "section_id": section.section_id,
              "section_label": section.label,
              "part": section.part,
              "part_count": section.part_count,
              "parent_document_id": parent_id,
              "next_document_id": next_id,
              "content": section.content,
              "content_url": content_url_value,
              "content_length": len(section.content),
              "filing_date": meta.get("filing_date"),
              "fiscal_year": meta.get("fiscal_year"),
              "fiscal_period": meta.get("fiscal_period"),
              "form_type": meta.get("form_type"),
              "accession_number": accession,
            }
          )

      except Exception as e:
        context.log.warning(f"Error processing {zip_key}: {e}")
        errors += 1
        continue

      # Batch index to limit memory
      if len(documents) >= batch_size:
        if not config.skip_embeddings:
          _embed_document_batch(enricher, documents)
        batch_result = os_client.bulk_index(documents)
        total_indexed += batch_result["indexed"]
        errors += batch_result["errors"]
        context.log.info(
          f"Batch indexed {batch_result['indexed']} sections ({total_indexed} total)"
        )
        documents.clear()

    # Index remaining documents
    if documents:
      if not config.skip_embeddings:
        _embed_document_batch(enricher, documents)
      batch_result = os_client.bulk_index(documents)
      total_indexed += batch_result["indexed"]
      errors += batch_result["errors"]
      context.log.info(
        f"Final batch indexed {batch_result['indexed']} sections "
        f"({batch_result['errors']} errors)"
      )

  # Release enricher memory
  del enricher
  gc.collect()

  context.log.info(
    f"Narrative indexing complete: {filings_processed} filings, "
    f"{sections_extracted} sections, {total_indexed} indexed, {errors} errors"
  )

  return MaterializeResult(
    metadata={
      "graph_id": config.graph_id,
      "source_type": "narrative_section",
      "filings_processed": filings_processed,
      "sections_extracted": sections_extracted,
      "documents_indexed": total_indexed,
      "errors": errors,
    }
  )


@asset(
  group_name="sec_pipeline",
  description="Extract iXBRL disclosure sections with XBRL element metadata into OpenSearch",
  kinds={"opensearch"},
  deps=["sec_processed_filings"],
  partitions_def=sec_quarter_partitions,
  backfill_policy=BackfillPolicy.single_run(),
  metadata={
    "pipeline": "sec",
    "stage": "text_index",
    "source": "ixbrl_disclosures",
  },
)
def sec_ixbrl_disclosures_indexed(
  context: AssetExecutionContext,
  config: SECiXBRLIndexConfig,
) -> MaterializeResult:
  """Extract iXBRL disclosure sections and index with XBRL element metadata.

  Partitioned by quarter (e.g. 2026-Q1). For each filing ZIP:
  1. Extract iXBRL HTML (largest HTM file)
  2. Verify document type via dei:DocumentType iXBRL tag
  3. Parse ix:nonNumeric TextBlock elements as disclosure sections
  4. Extract nested ix:nonFraction element qnames per section
  5. Resolve CDN content_url from externalized Fact data (value_type=external)
  6. Index into OpenSearch with source_type="ixbrl_disclosure" + xbrl_elements metadata

  Enables bidirectional navigation: search → graph (find facts in a disclosure)
  and graph → search (find the disclosure discussing a fact).
  """
  from xbrlkit.text import iXBRLParser

  from robosystems.operations.search.client import OpenSearchClient

  s3 = _get_s3_client()
  raw_bucket = _get_raw_bucket()
  processed_bucket = _get_processed_bucket()

  parser = iXBRLParser(part_size=config.part_size or None)

  os_client = OpenSearchClient(env.OPENSEARCH_URL, env.OPENSEARCH_INDEX)
  os_client.create_index_if_not_exists()

  # Load fastembed for embedding generation (required for hybrid search)
  from robosystems.adapters.sec.enrichment import SemanticEnricher

  enricher = SemanticEnricher()
  context.log.info("Loaded SemanticEnricher for embedding generation")

  # Get already-indexed accessions for incremental skip (scoped to this source type)
  if config.force_reindex:
    indexed_accessions: set[str] = set()
    context.log.info("Force reindex enabled — will re-index all documents")
  else:
    indexed_accessions = _get_indexed_accessions(
      os_client, config.graph_id, source_type="ixbrl_disclosure"
    )
  if indexed_accessions:
    context.log.info(
      f"Found {len(indexed_accessions)} already-indexed accessions, will skip"
    )

  # Use partition key to scope to a single quarter
  partition_key = context.partition_key  # e.g. "2026-Q1"
  prefix_base = get_processed_key(DataSourceType.SEC, "processed")
  partition_prefix = f"{prefix_base}/filed={partition_key}"

  context.log.info(
    f"Scanning parquets from s3://{processed_bucket}/{partition_prefix}/"
  )

  all_parquet_keys = _list_s3_parquet_keys(s3, processed_bucket, partition_prefix)
  context.log.info(f"Found {len(all_parquet_keys)} parquets for {partition_key}")

  report_keys = [k for k in all_parquet_keys if "/nodes/Report/" in k]
  entity_keys = [k for k in all_parquet_keys if "/nodes/Entity/" in k]
  ehr_keys = [k for k in all_parquet_keys if "/relationships/ENTITY_HAS_REPORT/" in k]
  element_keys = [k for k in all_parquet_keys if "/nodes/Element/" in k]
  fact_keys = [k for k in all_parquet_keys if "/nodes/Fact/" in k]
  fhe_keys = [k for k in all_parquet_keys if "/relationships/FACT_HAS_ELEMENT/" in k]
  rhf_keys = [k for k in all_parquet_keys if "/relationships/REPORT_HAS_FACT/" in k]

  context.log.info(f"Reading {len(report_keys)} Report parquets for metadata...")
  report_table = _read_parquets_from_s3(
    s3,
    processed_bucket,
    report_keys,
    columns=[
      "identifier",
      "accession_number",
      "form",
      "filing_date",
      "fiscal_year_focus",
      "fiscal_period_focus",
    ],
  )
  entity_table = _read_parquets_from_s3(
    s3,
    processed_bucket,
    entity_keys,
    columns=["identifier", "ticker", "name", "cik"],
  )
  ehr_table = _read_parquets_from_s3(s3, processed_bucket, ehr_keys)

  if report_table is None:
    context.log.warning("No Report parquets found")
    return MaterializeResult(
      metadata={"status": "no_data", "graph_id": config.graph_id}
    )

  reports_df = report_table.to_pandas()
  form_types_upper = [ft.upper() for ft in config.form_types]
  target_reports = reports_df[
    reports_df["form"].str.upper().isin(form_types_upper)
  ].copy()

  # Build report_id → accession lookup before target_reports is deleted
  report_id_to_accession: dict[str, str] = {}
  for _, report in target_reports.iterrows():
    rid = report.get("identifier", "")
    acc = report.get("accession_number", "")
    if rid and acc:
      report_id_to_accession[rid] = acc

  # Build entity lookup
  entity_lookup: dict[str, dict[str, str]] = {}
  report_to_entity: dict[str, str] = {}
  if entity_table is not None:
    entities_df = entity_table.to_pandas()
    for _, row in entities_df.iterrows():
      entity_lookup[row.get("identifier")] = {
        "ticker": str(row.get("ticker", "")),
        "name": str(row.get("name", "")),
        "cik": str(row.get("cik", "")),
      }
  if ehr_table is not None:
    ehr_df = ehr_table.to_pandas()
    for _, row in ehr_df.iterrows():
      report_to_entity[row.get("to")] = row.get("from")

  # Build accession → metadata
  accession_metadata: dict[str, dict[str, Any]] = {}
  for _, report in target_reports.iterrows():
    accession = report.get("accession_number", "")
    if not accession:
      continue
    entity_id = report_to_entity.get(report.get("identifier"))
    entity_info = entity_lookup.get(entity_id, {}) if entity_id else {}
    fy = report.get("fiscal_year_focus")
    accession_metadata[accession] = {
      "form_type": report.get("form", ""),
      "filing_date": str(report.get("filing_date", "")),
      "fiscal_year": int(fy) if fy is not None and not math.isnan(fy) else None,
      "fiscal_period": report.get("fiscal_period_focus", ""),
      "cik": entity_info.get("cik", ""),
      "ticker": entity_info.get("ticker", ""),
      "entity_name": entity_info.get("name", ""),
    }

  context.log.info(f"Built metadata for {len(accession_metadata)} accessions")

  # Build CDN URL lookup: (accession_number, element_qname) → content_url
  # Resolves externalized Fact URLs by joining Element, Fact, FACT_HAS_ELEMENT,
  # and REPORT_HAS_FACT parquets. Streams one file at a time to limit memory.
  cdn_url_lookup: dict[tuple[str, str], str] = {}

  # Step 1: Read Element parquets, filter to textblocks, build element_id → qname
  context.log.info(
    f"Reading {len(element_keys)} Element parquets for textblock qnames..."
  )
  element_id_to_qname: dict[str, str] = {}
  element_table = _read_parquets_from_s3(
    s3,
    processed_bucket,
    element_keys,
    columns=["identifier", "qname", "is_textblock"],
  )
  if element_table is not None:
    elements_df = element_table.to_pandas()
    elements_df = elements_df.fillna({"qname": "", "is_textblock": False})
    tb_df = elements_df[elements_df["is_textblock"].astype(bool)]
    element_id_to_qname = dict(zip(tb_df["identifier"], tb_df["qname"], strict=False))
    del elements_df, tb_df, element_table
  context.log.info(f"Found {len(element_id_to_qname)} textblock elements")

  # Step 2: Stream FACT_HAS_ELEMENT to find textblock fact IDs and their element qnames
  textblock_fact_to_qname: dict[str, str] = {}
  textblock_element_ids = set(element_id_to_qname.keys())
  context.log.info(f"Streaming {len(fhe_keys)} FACT_HAS_ELEMENT parquets...")
  for key in fhe_keys:
    table = _read_parquets_from_s3(s3, processed_bucket, [key])
    if table is None:
      continue
    df = table.to_pandas()
    mask = df["to"].isin(textblock_element_ids)
    matched = df.loc[mask]
    textblock_fact_to_qname.update(
      dict(zip(matched["from"], matched["to"].map(element_id_to_qname), strict=False))
    )
    del df, table, matched
  context.log.info(f"Found {len(textblock_fact_to_qname)} textblock facts")
  del element_id_to_qname, textblock_element_ids

  # Step 3: Stream Fact parquets for value_type=external to get fact_id → URL
  textblock_fact_ids = set(textblock_fact_to_qname.keys())
  fact_id_to_url: dict[str, str] = {}
  context.log.info(f"Streaming {len(fact_keys)} Fact parquets for external URLs...")
  for key in fact_keys:
    table = _read_parquets_from_s3(
      s3, processed_bucket, [key], columns=["identifier", "value", "value_type"]
    )
    if table is None:
      continue
    df = table.to_pandas()
    mask = df["identifier"].isin(textblock_fact_ids) & (df["value_type"] == "external")
    matched = df.loc[mask, ["identifier", "value"]].fillna("")
    fact_id_to_url.update(
      dict(zip(matched["identifier"], matched["value"], strict=False))
    )
    del df, table, matched
  context.log.info(f"Found {len(fact_id_to_url)} external textblock fact URLs")

  # Step 4: Stream REPORT_HAS_FACT to map fact_id → report_id
  fact_id_to_report: dict[str, str] = {}
  context.log.info(f"Streaming {len(rhf_keys)} REPORT_HAS_FACT parquets...")
  for key in rhf_keys:
    table = _read_parquets_from_s3(s3, processed_bucket, [key])
    if table is None:
      continue
    df = table.to_pandas()
    mask = df["to"].isin(textblock_fact_ids)
    matched = df.loc[mask]
    fact_id_to_report.update(dict(zip(matched["to"], matched["from"], strict=False)))
    del df, table, matched
  context.log.info(f"Mapped {len(fact_id_to_report)} facts to reports")

  # Step 5: Combine — for each fact, resolve accession and qname, store URL
  for fact_id, url in fact_id_to_url.items():
    report_id = fact_id_to_report.get(fact_id)
    if not report_id:
      continue
    accession = report_id_to_accession.get(report_id)
    if not accession:
      continue
    qname = textblock_fact_to_qname.get(fact_id)
    if qname:
      cdn_url_lookup[(accession, qname)] = url

  context.log.info(f"Built CDN URL lookup with {len(cdn_url_lookup)} entries")
  del fact_id_to_url, fact_id_to_report, textblock_fact_to_qname, textblock_fact_ids

  # Free intermediate data before potentially loading fastembed model
  del reports_df, report_table, target_reports, entity_lookup, report_to_entity
  del report_id_to_accession
  del all_parquet_keys, report_keys, entity_keys, ehr_keys
  del element_keys, fact_keys, fhe_keys, rhf_keys
  gc.collect()

  # Scan raw ZIPs
  # A section part is about part_size chars (25K by default, at most ~1.3x that).
  # Batch of 100 x ~32KB = ~3MB, safely under OpenSearch's 10MB bulk request limit.
  batch_size = 100
  documents: list[dict[str, Any]] = []
  total_indexed = 0
  filings_processed = 0
  sections_extracted = 0
  total_elements = 0
  errors = 0

  # Scan raw ZIPs scoped to this partition's year.
  # Raw ZIPs use year= partitions (annual), but accession_metadata
  # from the quarterly processed parquets handles fine-grained filtering.
  partition_year = partition_key.split("-Q")[0]  # "2026-Q1" → "2026"
  raw_prefix = f"{get_raw_key(DataSourceType.SEC)}/year={partition_year}"
  paginator = s3.get_paginator("list_objects_v2")

  zip_keys: list[str] = []
  for page in paginator.paginate(Bucket=raw_bucket, Prefix=raw_prefix):
    for obj in page.get("Contents", []):
      if obj["Key"].endswith(".zip"):
        zip_keys.append(obj["Key"])

  context.log.info(f"Found {len(zip_keys)} raw ZIP files for year={partition_year}")

  with os_client.bulk_write_mode(write_interval="5s"):
    for zip_key in zip_keys:
      filename = zip_key.rsplit("/", 1)[-1]
      accession = filename.replace(".zip", "")

      if accession not in accession_metadata:
        continue

      if accession in indexed_accessions:
        continue

      meta = accession_metadata[accession]
      cik = meta.get("cik", "")

      try:
        response = s3.get_object(Bucket=raw_bucket, Key=zip_key)
        zip_bytes = response["Body"].read()

        html_content = _extract_html_from_zip(zip_bytes)
        if not html_content:
          context.log.debug(f"No HTML found in {zip_key}")
          errors += 1
          continue

        # Verify document type via iXBRL tag
        ixbrl_doc_type = _extract_ixbrl_doc_type(html_content)
        if ixbrl_doc_type and ixbrl_doc_type.upper() not in form_types_upper:
          context.log.debug(
            f"Skipping {accession}: iXBRL doc type '{ixbrl_doc_type}' "
            f"not in {form_types_upper}"
          )
          continue

        # A re-index replaces the accession's documents: the part count of a
        # section, and so its document ids, can change between parser versions.
        if config.force_reindex:
          os_client.delete_by_accession(config.graph_id, "ixbrl_disclosure", accession)

        # Parse iXBRL disclosure sections
        sections = parser.parse(html_content)
        filings_processed += 1

        if not sections:
          continue

        for section in sections:
          sections_extracted += 1
          if section.part == 1:
            total_elements += section.element_count

          doc_id, parent_id, next_id = _part_document_ids(
            config.graph_id, "ixbrl", accession, section
          )

          documents.append(
            {
              "graph_id": config.graph_id,
              "document_id": doc_id,
              "source_type": "ixbrl_disclosure",
              "entity_ticker": meta.get("ticker") or cik,
              "entity_name": meta.get("entity_name"),
              "entity_cik": cik,
              "section_id": section.section_id,
              "section_label": section.label,
              "part": section.part,
              "part_count": section.part_count,
              "parent_document_id": parent_id,
              "next_document_id": next_id,
              "content": section.content,
              "content_url": cdn_url_lookup.get((accession, section.section_id), ""),
              "content_length": len(section.content),
              "xbrl_elements": section.xbrl_elements,
              "xbrl_element_count": section.element_count,
              "filing_date": meta.get("filing_date"),
              "fiscal_year": meta.get("fiscal_year"),
              "fiscal_period": meta.get("fiscal_period"),
              "form_type": meta.get("form_type"),
              "accession_number": accession,
            }
          )

      except Exception as e:
        context.log.warning(f"Error processing {zip_key}: {e}")
        errors += 1
        continue

      # Batch index to limit memory
      if len(documents) >= batch_size:
        if not config.skip_embeddings:
          _embed_document_batch(enricher, documents)
        batch_result = os_client.bulk_index(documents)
        total_indexed += batch_result["indexed"]
        errors += batch_result["errors"]
        context.log.info(
          f"Batch indexed {batch_result['indexed']} disclosures ({total_indexed} total)"
        )
        documents.clear()

    # Index remaining
    if documents:
      if not config.skip_embeddings:
        _embed_document_batch(enricher, documents)
      batch_result = os_client.bulk_index(documents)
      total_indexed += batch_result["indexed"]
      errors += batch_result["errors"]
      context.log.info(
        f"Final batch indexed {batch_result['indexed']} disclosures "
        f"({batch_result['errors']} errors)"
      )

  # Release enricher and lookup memory
  del enricher, cdn_url_lookup
  gc.collect()

  context.log.info(
    f"iXBRL indexing complete: {filings_processed} filings, "
    f"{sections_extracted} sections, {total_elements} elements, "
    f"{total_indexed} indexed, {errors} errors"
  )

  return MaterializeResult(
    metadata={
      "graph_id": config.graph_id,
      "source_type": "ixbrl_disclosure",
      "filings_processed": filings_processed,
      "sections_extracted": sections_extracted,
      "total_elements": total_elements,
      "documents_indexed": total_indexed,
      "errors": errors,
    }
  )
