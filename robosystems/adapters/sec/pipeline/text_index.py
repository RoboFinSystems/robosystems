"""SEC Text Search Indexing Assets.

Three assets for indexing SEC filing text content into OpenSearch:

1. sec_textblocks_indexed — Index XBRL text blocks (already externalized to S3)
   Reads processed parquets (Fact + Entity + Report + Element), fetches externalized
   HTML from S3, strips to plain text, indexes into OpenSearch.

2. sec_narratives_indexed — Extract and index narrative sections from raw filings
   Reads raw ZIP files, extracts 10-K/10-Q HTML, detects Item sections,
   externalizes clean text to S3/CDN, indexes into OpenSearch.

3. sec_ixbrl_disclosures_indexed — Extract iXBRL disclosure sections with XBRL element metadata
   Reads raw ZIP files, parses iXBRL text blocks (ix:nonNumeric TextBlock elements),
   extracts nested XBRL element qnames, indexes with element metadata for
   bidirectional navigation between knowledge graph and document search.

All depend on sec_processed_filings (need Entity/Report metadata from parquets).
All run parallel to the DuckDB staging branch.
"""

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
  get_raw_key,
)
from robosystems.logger import logger

from .configs import (
  SECiXBRLIndexConfig,
  SECNarrativeIndexConfig,
  SECTextBlockIndexConfig,
  sec_quarter_partitions,
)

EMBEDDING_MODEL_NAME = "bge-small-en-v1.5"


def _embed_document_batch(enricher, documents: list[dict], context) -> None:
  """Add embeddings to documents in-place using fastembed.

  Called before bulk_index when enable_embeddings is True.
  """
  texts = [doc.get("content", "") for doc in documents]
  embeddings = enricher.embed_batch(texts)
  for doc, emb in zip(documents, embeddings, strict=True):
    doc["embedding"] = emb
    doc["embedding_model"] = EMBEDDING_MODEL_NAME


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


def _derive_section_label(element_name: str) -> str:
  """Derive a human-readable section label from an XBRL element name.

  Examples:
      'Risk Factors [Text Block]' → 'Risk Factors'
      'ManagementDiscussionAndAnalysisTextBlock' → 'Management Discussion And Analysis'
  """
  label = element_name
  for suffix in ["[Text Block]", "Text Block", "TextBlock", "[text Block]"]:
    if label.endswith(suffix):
      label = label[: -len(suffix)].strip()
      break

  # If it's camelCase, add spaces
  if " " not in label and any(c.isupper() for c in label[1:]):
    label = re.sub(r"([a-z])([A-Z])", r"\1 \2", label)
    label = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1 \2", label)

  return label.strip()


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

  return pa.concat_tables(tables)


def _fetch_s3_text(s3, bucket: str, key: str) -> str | None:
  """Fetch text content from S3."""
  try:
    response = s3.get_object(Bucket=bucket, Key=key)
    content = response["Body"].read()
    return content.decode("utf-8", errors="replace")
  except Exception as e:
    logger.debug(f"Failed to fetch {key}: {e}")
    return None


def _strip_html(html: str) -> str:
  """Simple HTML stripping for text block content."""
  from robosystems.utils.html_parser import extract_structured_content

  try:
    return extract_structured_content(html)
  except Exception:
    # Fallback: regex strip
    text = re.sub(r"<[^>]+>", " ", html)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


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


def _url_to_s3_key(url: str) -> tuple[str, str] | None:
  """Parse an externalized text block URL back to bucket + key.

  Handles both CDN URLs and direct S3 URLs:
    https://cdn.example.com/2026/0000005513/... → key from CDN prefix
    https://bucket.s3.amazonaws.com/key → bucket + key
  """
  cdn_url = _get_public_data_cdn_url()
  if cdn_url and url.startswith(cdn_url):
    key = url[len(cdn_url) :].lstrip("/")
    return (_get_public_data_bucket(), key)

  # Handle direct S3 URL: https://bucket.s3.amazonaws.com/key
  # or https://bucket.s3.region.amazonaws.com/key
  if ".s3." in url and "amazonaws.com/" in url:
    parts = url.split(".s3.", 1)
    bucket = parts[0].split("//", 1)[1]
    key = parts[1].split("amazonaws.com/", 1)[1]
    return (bucket, key)

  return None


@asset(
  group_name="sec_pipeline",
  description="Index XBRL text blocks into OpenSearch for full-text search",
  kinds={"opensearch"},
  deps=["sec_processed_filings"],
  partitions_def=sec_quarter_partitions,
  backfill_policy=BackfillPolicy.single_run(),
  metadata={
    "pipeline": "sec",
    "stage": "text_index",
    "source": "xbrl_textblocks",
  },
)
def sec_textblocks_indexed(
  context: AssetExecutionContext,
  config: SECTextBlockIndexConfig,
) -> MaterializeResult:
  """Index externalized XBRL text blocks into OpenSearch.

  Partitioned by quarter (e.g. 2026-Q1). Each run processes one quarter's
  parquets, making backfills trivial and memory bounded.

  Joins Fact (value_type=external) + Element (is_textblock=true) + Entity + Report.
  """
  from robosystems.operations.search.client import OpenSearchClient

  s3 = _get_s3_client()
  processed_bucket = _get_processed_bucket()

  os_client = OpenSearchClient(env.OPENSEARCH_URL, env.OPENSEARCH_INDEX)
  os_client.create_index_if_not_exists()

  # Optionally load fastembed for embedding generation
  enricher = None
  if config.enable_embeddings:
    from robosystems.adapters.sec.enrichment import SemanticEnricher

    enricher = SemanticEnricher()
    context.log.info("Loaded SemanticEnricher for embedding generation")

  # Get already-indexed accessions for incremental skip (scoped to this source type)
  if config.force_reindex:
    indexed_accessions: set[str] = set()
    context.log.info("Force reindex enabled — will re-index all documents")
  else:
    indexed_accessions = _get_indexed_accessions(
      os_client, config.graph_id, source_type="xbrl_textblock"
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
  node_keys = {
    "Entity": [k for k in all_parquet_keys if "/nodes/Entity/" in k],
    "Report": [k for k in all_parquet_keys if "/nodes/Report/" in k],
    "Element": [k for k in all_parquet_keys if "/nodes/Element/" in k],
    "Fact": [k for k in all_parquet_keys if "/nodes/Fact/" in k],
  }

  for table, keys in node_keys.items():
    context.log.info(f"Found {len(keys)} parquet files for {table}")

  if not node_keys["Fact"]:
    context.log.warning("No Fact parquet files found")
    return MaterializeResult(
      metadata={"status": "no_data", "graph_id": config.graph_id}
    )

  # --- Phase 1: Load small lookup tables (Entity, Report, Element) ---
  # Entity and Report are small (~5K rows). Element is large (~1M rows) but
  # we only need 4 small columns (no embeddings) so it's manageable.

  context.log.info("Reading Entity table...")
  entity_table = _read_parquets_from_s3(
    s3,
    processed_bucket,
    node_keys["Entity"],
    columns=["identifier", "ticker", "name", "cik"],
  )
  context.log.info("Reading Report table...")
  report_table = _read_parquets_from_s3(
    s3,
    processed_bucket,
    node_keys["Report"],
    columns=[
      "identifier",
      "accession_number",
      "form",
      "filing_date",
      "fiscal_year_focus",
      "fiscal_period_focus",
      "cik",
    ],
  )
  context.log.info("Reading Element table...")
  element_table = _read_parquets_from_s3(
    s3,
    processed_bucket,
    node_keys["Element"],
    columns=["identifier", "qname", "name", "is_textblock"],
  )

  if any(t is None for t in [entity_table, report_table, element_table]):
    context.log.warning("Failed to read one or more lookup tables")
    return MaterializeResult(
      metadata={"status": "read_error", "graph_id": config.graph_id}
    )

  # Build lookup dicts
  entities_df = entity_table.to_pandas()
  reports_df = report_table.to_pandas()
  elements_df = element_table.to_pandas()

  context.log.info(
    f"Loaded {len(entities_df)} entities, "
    f"{len(reports_df)} reports, {len(elements_df)} elements"
  )

  entities_df = entities_df.fillna("")
  entities_df["cik"] = entities_df["cik"].astype(str)
  entity_lookup: dict[str, dict[str, str]] = entities_df.set_index("identifier")[
    ["ticker", "name", "cik"]
  ].to_dict("index")
  del entities_df, entity_table

  reports_df["filing_date"] = reports_df["filing_date"].astype(str)
  reports_df["fiscal_year"] = reports_df["fiscal_year_focus"].apply(
    lambda fy: (
      int(fy)
      if fy is not None and not (isinstance(fy, float) and math.isnan(fy))
      else None
    )
  )
  reports_df = reports_df.rename(columns={"fiscal_period_focus": "fiscal_period"})
  reports_df = reports_df.fillna("")
  report_lookup: dict[str, dict[str, Any]] = reports_df.set_index("identifier")[
    ["filing_date", "form", "fiscal_year", "fiscal_period", "accession_number"]
  ].to_dict("index")
  del reports_df, report_table

  # Build element lookup and extract textblock element IDs for early filtering
  elements_df = elements_df.fillna({"qname": "", "name": "", "is_textblock": False})
  element_lookup: dict[str, dict[str, Any]] = elements_df.set_index("identifier")[
    ["qname", "name", "is_textblock"]
  ].to_dict("index")
  textblock_element_ids: set[str] = set(
    elements_df.loc[elements_df["is_textblock"], "identifier"]
  )
  del elements_df, element_table

  context.log.info(f"Found {len(textblock_element_ids)} textblock element types")

  if not textblock_element_ids:
    context.log.info("No textblock elements found")
    return MaterializeResult(
      metadata={
        "graph_id": config.graph_id,
        "source_type": "xbrl_textblock",
        "documents_indexed": 0,
      }
    )

  # --- Phase 2: Stream relationship parquets with early filtering ---
  # Instead of loading all 7M+ edges, stream one file at a time and
  # filter to only the edges we need (textblock facts).

  rel_keys = [k for k in all_parquet_keys if "/relationships/" in k]
  fhe_keys = [k for k in rel_keys if "/FACT_HAS_ELEMENT/" in k]
  rhf_keys = [k for k in rel_keys if "/REPORT_HAS_FACT/" in k]
  ehr_keys = [k for k in rel_keys if "/ENTITY_HAS_REPORT/" in k]

  # Step 2a: Stream FACT_HAS_ELEMENT to find fact_ids that map to textblock elements
  context.log.info(
    f"Streaming {len(fhe_keys)} FACT_HAS_ELEMENT parquets "
    f"(filtering to {len(textblock_element_ids)} textblock elements)..."
  )
  fact_to_element: dict[str, str] = {}
  textblock_fact_ids: set[str] = set()
  for key in fhe_keys:
    table = _read_parquets_from_s3(s3, processed_bucket, [key])
    if table is None:
      continue
    df = table.to_pandas()
    # Filter to only edges pointing at textblock elements
    mask = df["to"].isin(textblock_element_ids)
    matched = df.loc[mask]
    fact_to_element.update(dict(zip(matched["from"], matched["to"], strict=False)))
    textblock_fact_ids.update(matched["from"])
    del df, table, matched

  context.log.info(
    f"Found {len(textblock_fact_ids)} facts linked to textblock elements"
  )

  if not textblock_fact_ids:
    context.log.info("No textblock facts found")
    return MaterializeResult(
      metadata={
        "graph_id": config.graph_id,
        "source_type": "xbrl_textblock",
        "documents_indexed": 0,
      }
    )

  # Step 2b: Stream Fact parquets to get external textblock facts only
  context.log.info(
    f"Streaming {len(node_keys['Fact'])} Fact parquets "
    f"(filtering to {len(textblock_fact_ids)} textblock facts)..."
  )
  external_textblock_facts: list[dict[str, str]] = []
  for key in node_keys["Fact"]:
    table = _read_parquets_from_s3(
      s3, processed_bucket, [key], columns=["identifier", "value", "value_type"]
    )
    if table is None:
      continue
    df = table.to_pandas()
    mask = df["identifier"].isin(textblock_fact_ids) & (df["value_type"] == "external")
    matched = df.loc[mask, ["identifier", "value"]].fillna("")
    external_textblock_facts.extend(matched.to_dict("records"))
    del df, table, matched

  context.log.info(f"Found {len(external_textblock_facts)} external textblock facts")

  if not external_textblock_facts:
    context.log.info("No externalized textblock facts to index")
    return MaterializeResult(
      metadata={
        "graph_id": config.graph_id,
        "source_type": "xbrl_textblock",
        "documents_indexed": 0,
      }
    )

  # Step 2c: Stream REPORT_HAS_FACT to map textblock facts to reports
  context.log.info(
    f"Streaming {len(rhf_keys)} REPORT_HAS_FACT parquets "
    f"(filtering to {len(textblock_fact_ids)} textblock facts)..."
  )
  fact_to_report: dict[str, str] = {}
  for key in rhf_keys:
    table = _read_parquets_from_s3(s3, processed_bucket, [key])
    if table is None:
      continue
    df = table.to_pandas()
    mask = df["to"].isin(textblock_fact_ids)
    matched = df.loc[mask]
    fact_to_report.update(dict(zip(matched["to"], matched["from"], strict=False)))
    del df, table, matched

  context.log.info(f"Mapped {len(fact_to_report)} textblock facts to reports")

  # Step 2d: ENTITY_HAS_REPORT is small (~5K edges), load normally
  context.log.info(f"Reading {len(ehr_keys)} ENTITY_HAS_REPORT parquets...")
  report_to_entity: dict[str, str] = {}
  ehr_table = _read_parquets_from_s3(s3, processed_bucket, ehr_keys)
  if ehr_table is not None:
    ehr_df = ehr_table.to_pandas()
    report_to_entity = dict(zip(ehr_df["to"], ehr_df["from"], strict=False))
    del ehr_df, ehr_table

  context.log.info(f"Mapped {len(report_to_entity)} reports to entities")

  # --- Phase 3: Build and index OpenSearch documents ---
  documents: list[dict[str, Any]] = []
  total_indexed = 0
  errors = 0
  skipped = 0

  for fact in external_textblock_facts:
    fact_id = fact["identifier"]
    value_url = fact["value"]

    # Resolve report for accession check
    report_id = fact_to_report.get(fact_id)
    report_info = report_lookup.get(report_id, {}) if report_id else {}

    # Skip already-indexed accessions (incremental)
    accession = report_info.get("accession_number", "")
    if accession and accession in indexed_accessions:
      skipped += 1
      continue

    # Resolve element
    element_id = fact_to_element.get(fact_id)
    element_info = element_lookup.get(element_id, {}) if element_id else {}

    entity_id = report_to_entity.get(report_id) if report_id else None
    entity_info = entity_lookup.get(entity_id, {}) if entity_id else {}

    # Fetch content from S3
    s3_info = _url_to_s3_key(str(value_url))
    if not s3_info:
      skipped += 1
      continue

    content_bucket, content_key = s3_info
    html_content = _fetch_s3_text(s3, content_bucket, content_key)
    if not html_content:
      errors += 1
      continue

    # Strip HTML
    plain_text = _strip_html(html_content)
    if len(plain_text) < config.min_content_length:
      skipped += 1
      continue

    section_label = _derive_section_label(element_info.get("name", ""))
    doc_id = hashlib.sha256(f"{config.graph_id}:tb:{fact_id}".encode()).hexdigest()[:16]

    documents.append(
      {
        "graph_id": config.graph_id,
        "document_id": doc_id,
        "source_type": "xbrl_textblock",
        "entity_ticker": entity_info.get("ticker"),
        "entity_name": entity_info.get("name"),
        "entity_cik": entity_info.get("cik"),
        "element_qname": element_info.get("qname"),
        "section_label": section_label,
        "content": plain_text,
        "content_url": str(value_url),
        "content_length": len(plain_text),
        "filing_date": report_info.get("filing_date"),
        "fiscal_year": report_info.get("fiscal_year"),
        "fiscal_period": report_info.get("fiscal_period"),
        "form_type": report_info.get("form"),
        "accession_number": report_info.get("accession_number"),
      }
    )

    # Bulk index in batches to limit memory from accumulated documents
    if len(documents) >= 1000:
      if enricher:
        _embed_document_batch(enricher, documents, context)
      batch_result = os_client.bulk_index(documents)
      total_indexed += batch_result["indexed"]
      errors += batch_result["errors"]
      context.log.info(
        f"Batch indexed {batch_result['indexed']} docs "
        f"({batch_result['errors']} errors, {total_indexed} total)"
      )
      documents.clear()

  context.log.info(f"Processing complete ({skipped} skipped, {errors} errors so far)")

  # Index remaining documents
  if documents:
    if enricher:
      _embed_document_batch(enricher, documents, context)
    batch_result = os_client.bulk_index(documents)
    total_indexed += batch_result["indexed"]
    errors += batch_result["errors"]
    context.log.info(
      f"Final batch indexed {batch_result['indexed']} docs "
      f"({batch_result['errors']} errors)"
    )

  # Release enricher memory
  if enricher:
    del enricher
    import gc

    gc.collect()

  return MaterializeResult(
    metadata={
      "graph_id": config.graph_id,
      "source_type": "xbrl_textblock",
      "documents_indexed": total_indexed,
      "documents_skipped": skipped,
      "errors": errors,
      "total_textblock_facts": len(external_textblock_facts),
    }
  )


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
  from robosystems.adapters.sec.narrative_extractor import NarrativeExtractor
  from robosystems.operations.search.client import OpenSearchClient

  s3 = _get_s3_client()
  raw_bucket = _get_raw_bucket()
  processed_bucket = _get_processed_bucket()
  public_bucket = _get_public_data_bucket()
  cdn_url = _get_public_data_cdn_url()

  extractor = NarrativeExtractor(max_section_length=config.max_section_length)

  os_client = OpenSearchClient(env.OPENSEARCH_URL, env.OPENSEARCH_INDEX)
  os_client.create_index_if_not_exists()

  # Optionally load fastembed for embedding generation
  enricher = None
  if config.enable_embeddings:
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
      "cik",
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
      "cik": entity_info.get("cik", str(report.get("cik", ""))),
      "ticker": entity_info.get("ticker", ""),
      "entity_name": entity_info.get("name", ""),
    }

  context.log.info(f"Built metadata for {len(accession_metadata)} accessions")

  # List raw ZIPs and process those matching our target accessions
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

      # Extract narrative sections
      sections = extractor.extract(html_content, ixbrl_doc_type or meta["form_type"])
      filings_processed += 1

      if not sections:
        continue

      for section in sections:
        sections_extracted += 1

        # Externalize clean text to public S3 bucket
        narrative_key = f"{year}/{cik}/{accession}/narrative_{section.section_id}.txt"
        content_url_value = ""

        if public_bucket:
          try:
            s3.put_object(
              Bucket=public_bucket,
              Key=narrative_key,
              Body=section.content.encode("utf-8"),
              ContentType="text/plain; charset=utf-8",
            )
            if cdn_url:
              content_url_value = f"{cdn_url}/{narrative_key}"
            else:
              content_url_value = (
                f"https://{public_bucket}.s3.amazonaws.com/{narrative_key}"
              )
          except Exception as e:
            context.log.debug(f"Failed to externalize {narrative_key}: {e}")

        doc_id = hashlib.sha256(
          f"{config.graph_id}:narr:{accession}:{section.section_id}".encode()
        ).hexdigest()[:16]

        documents.append(
          {
            "graph_id": config.graph_id,
            "document_id": doc_id,
            "source_type": "narrative_section",
            "entity_ticker": meta.get("ticker"),
            "entity_name": meta.get("entity_name"),
            "entity_cik": cik,
            "section_id": section.section_id,
            "section_label": section.section_label,
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
    if len(documents) >= 500:
      if enricher:
        _embed_document_batch(enricher, documents, context)
      batch_result = os_client.bulk_index(documents)
      total_indexed += batch_result["indexed"]
      errors += batch_result["errors"]
      context.log.info(
        f"Batch indexed {batch_result['indexed']} sections ({total_indexed} total)"
      )
      documents.clear()

  # Index remaining documents
  if documents:
    if enricher:
      _embed_document_batch(enricher, documents, context)
    batch_result = os_client.bulk_index(documents)
    total_indexed += batch_result["indexed"]
    errors += batch_result["errors"]
    context.log.info(
      f"Final batch indexed {batch_result['indexed']} sections "
      f"({batch_result['errors']} errors)"
    )

  # Release enricher memory
  if enricher:
    del enricher
    import gc

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
  5. Index into OpenSearch with source_type="ixbrl_disclosure" + xbrl_elements metadata

  Enables bidirectional navigation: search → graph (find facts in a disclosure)
  and graph → search (find the disclosure discussing a fact).
  """
  from robosystems.adapters.sec.ixbrl_parser import iXBRLParser
  from robosystems.operations.search.client import OpenSearchClient

  s3 = _get_s3_client()
  raw_bucket = _get_raw_bucket()
  processed_bucket = _get_processed_bucket()

  parser = iXBRLParser(max_section_length=config.max_section_length)

  os_client = OpenSearchClient(env.OPENSEARCH_URL, env.OPENSEARCH_INDEX)
  os_client.create_index_if_not_exists()

  # Optionally load fastembed for embedding generation
  enricher = None
  if config.enable_embeddings:
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
      "cik",
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
      "cik": entity_info.get("cik", str(report.get("cik", ""))),
      "ticker": entity_info.get("ticker", ""),
      "entity_name": entity_info.get("name", ""),
    }

  context.log.info(f"Built metadata for {len(accession_metadata)} accessions")

  # Scan raw ZIPs
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
        errors += 1
        continue

      # Verify document type via iXBRL tag
      ixbrl_doc_type = _extract_ixbrl_doc_type(html_content)
      if ixbrl_doc_type and ixbrl_doc_type.upper() not in form_types_upper:
        continue

      # Parse iXBRL disclosure sections
      sections = parser.parse(html_content)
      filings_processed += 1

      if not sections:
        continue

      for section in sections:
        sections_extracted += 1
        total_elements += section.element_count

        doc_id = hashlib.sha256(
          f"{config.graph_id}:ixbrl:{accession}:{section.section_id}".encode()
        ).hexdigest()[:16]

        documents.append(
          {
            "graph_id": config.graph_id,
            "document_id": doc_id,
            "source_type": "ixbrl_disclosure",
            "entity_ticker": meta.get("ticker"),
            "entity_name": meta.get("entity_name"),
            "entity_cik": cik,
            "section_id": section.section_id,
            "section_label": section.section_label,
            "content": section.content,
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
    if len(documents) >= 500:
      if enricher:
        _embed_document_batch(enricher, documents, context)
      batch_result = os_client.bulk_index(documents)
      total_indexed += batch_result["indexed"]
      errors += batch_result["errors"]
      context.log.info(
        f"Batch indexed {batch_result['indexed']} disclosures ({total_indexed} total)"
      )
      documents.clear()

  # Index remaining
  if documents:
    if enricher:
      _embed_document_batch(enricher, documents, context)
    batch_result = os_client.bulk_index(documents)
    total_indexed += batch_result["indexed"]
    errors += batch_result["errors"]
    context.log.info(
      f"Final batch indexed {batch_result['indexed']} disclosures "
      f"({batch_result['errors']} errors)"
    )

  # Release enricher memory
  if enricher:
    del enricher
    import gc

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
