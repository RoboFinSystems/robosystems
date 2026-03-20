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
from dagster import AssetExecutionContext, MaterializeResult, asset

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
)


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


def _partition_year(key: str) -> int:
  """Extract year from a filed= partition key (e.g., 'filed=2025-Q1' → 2025)."""
  for part in key.split("/"):
    if part.startswith("filed="):
      return int(part.split("=")[1][:4])
    if part.startswith("year="):
      return int(part.split("=")[1][:4])
  return 0


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


def _read_parquets_from_s3(s3, bucket: str, keys: list[str]):
  """Read multiple parquet files from S3 and concatenate into a single PyArrow table."""
  import pyarrow.parquet as pq

  tables = []
  for key in keys:
    try:
      response = s3.get_object(Bucket=bucket, Key=key)
      buf = io.BytesIO(response["Body"].read())
      table = pq.read_table(buf)
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

  Picks the largest non-exhibit HTM file, filtering out XBRL taxonomy files,
  viewer artifacts, and common exhibit patterns. Returns None if no valid
  HTML file is found.
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
      # Skip exhibits (ex, consent, subsidiary, certification patterns)
      if any(
        pat in basename for pat in ["ex1", "ex2", "ex3", "ex4", "consent", "subsidiar"]
      ):
        continue
      htm_candidates.append((info.filename, info.file_size))

    if not htm_candidates:
      return None

    # Pick the largest file — the main filing document is always the biggest
    main_file = max(htm_candidates, key=lambda x: x[1])[0]
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

  Reads processed parquet files to find externalized text block facts,
  fetches content from S3, strips HTML, and bulk indexes into OpenSearch.

  Joins Fact (value_type=external) + Element (is_textblock=true) + Entity + Report.
  """
  from robosystems.operations.search.client import OpenSearchClient

  s3 = _get_s3_client()
  processed_bucket = _get_processed_bucket()

  os_client = OpenSearchClient(env.OPENSEARCH_URL, env.OPENSEARCH_INDEX)
  os_client.create_index_if_not_exists()

  # Get already-indexed accessions for incremental skip (scoped to this source type)
  indexed_accessions = _get_indexed_accessions(
    os_client, config.graph_id, source_type="xbrl_textblock"
  )
  if indexed_accessions:
    context.log.info(
      f"Found {len(indexed_accessions)} already-indexed accessions, will skip"
    )

  # Discover parquet files for each table
  prefix_base = get_processed_key(DataSourceType.SEC, "processed")

  if config.start_year:
    context.log.info(
      f"Scanning parquets from s3://{processed_bucket}/{prefix_base}/ "
      f"(start_year={config.start_year})"
    )
  else:
    context.log.info(f"Scanning parquets from s3://{processed_bucket}/{prefix_base}/")

  # Read Entity, Report, Element, Fact parquets
  all_parquet_keys = _list_s3_parquet_keys(
    s3, processed_bucket, f"{prefix_base}/filed="
  )

  # Filter by start_year if specified (partition format: filed=2025-Q1)
  if config.start_year:
    all_parquet_keys = [
      k for k in all_parquet_keys if _partition_year(k) >= config.start_year
    ]
    context.log.info(
      f"Filtered to {len(all_parquet_keys)} parquets (>= {config.start_year})"
    )

  entity_keys = all_parquet_keys
  node_keys = {
    "Entity": [k for k in entity_keys if "/nodes/Entity/" in k],
    "Report": [k for k in entity_keys if "/nodes/Report/" in k],
    "Element": [k for k in entity_keys if "/nodes/Element/" in k],
    "Fact": [k for k in entity_keys if "/nodes/Fact/" in k],
  }

  for table, keys in node_keys.items():
    context.log.info(f"Found {len(keys)} parquet files for {table}")

  if not node_keys["Fact"]:
    context.log.warning("No Fact parquet files found")
    return MaterializeResult(
      metadata={"status": "no_data", "graph_id": config.graph_id}
    )

  # Read tables
  context.log.info("Reading Entity table...")
  entity_table = _read_parquets_from_s3(s3, processed_bucket, node_keys["Entity"])
  context.log.info("Reading Report table...")
  report_table = _read_parquets_from_s3(s3, processed_bucket, node_keys["Report"])
  context.log.info("Reading Element table...")
  element_table = _read_parquets_from_s3(s3, processed_bucket, node_keys["Element"])
  context.log.info("Reading Fact table...")
  fact_table = _read_parquets_from_s3(s3, processed_bucket, node_keys["Fact"])

  if any(t is None for t in [entity_table, report_table, element_table, fact_table]):
    context.log.warning("Failed to read one or more parquet tables")
    return MaterializeResult(
      metadata={"status": "read_error", "graph_id": config.graph_id}
    )

  # Convert to pandas for joins

  facts_df = fact_table.to_pandas()
  entities_df = entity_table.to_pandas()
  reports_df = report_table.to_pandas()
  elements_df = element_table.to_pandas()

  context.log.info(
    f"Loaded {len(facts_df)} facts, {len(entities_df)} entities, "
    f"{len(reports_df)} reports, {len(elements_df)} elements"
  )

  # Filter to external text block facts
  external_facts = facts_df[facts_df["value_type"] == "external"].copy()
  context.log.info(f"Found {len(external_facts)} externalized facts")

  if external_facts.empty:
    context.log.info("No externalized facts to index")
    return MaterializeResult(
      metadata={
        "graph_id": config.graph_id,
        "source_type": "xbrl_textblock",
        "documents_indexed": 0,
      }
    )

  # Build lookup dicts from entities, reports, elements
  # Entity: identifier → {ticker, name, cik}
  entity_lookup = {}
  for _, row in entities_df.iterrows():
    entity_lookup[row.get("identifier")] = {
      "ticker": row.get("ticker", ""),
      "name": row.get("name", ""),
      "cik": str(row.get("cik", "")),
    }

  # Report: identifier → {filing_date, form, fiscal_year, fiscal_period, accession}
  report_lookup = {}
  for _, row in reports_df.iterrows():
    fy = row.get("fiscal_year_focus")
    report_lookup[row.get("identifier")] = {
      "filing_date": str(row.get("filing_date", "")),
      "form": row.get("form", ""),
      "fiscal_year": int(fy) if fy is not None and not math.isnan(fy) else None,
      "fiscal_period": row.get("fiscal_period_focus", ""),
      "accession_number": row.get("accession_number", ""),
    }

  # Element: identifier → {qname, name, is_textblock}
  element_lookup = {}
  for _, row in elements_df.iterrows():
    element_lookup[row.get("identifier")] = {
      "qname": row.get("qname", ""),
      "name": row.get("name", ""),
      "is_textblock": row.get("is_textblock", False),
    }

  # Now read the relationship parquets to resolve Fact → Entity, Report, Element
  # REPORT_HAS_FACT: source=report_id, target=fact_id
  # FACT_HAS_ELEMENT: source=fact_id, target=element_id
  # ENTITY_HAS_REPORT: source=entity_id, target=report_id
  rel_keys = [k for k in entity_keys if "/relationships/" in k]
  rhf_keys = [k for k in rel_keys if "/REPORT_HAS_FACT/" in k]
  fhe_keys = [k for k in rel_keys if "/FACT_HAS_ELEMENT/" in k]
  ehr_keys = [k for k in rel_keys if "/ENTITY_HAS_REPORT/" in k]

  context.log.info("Reading relationship parquets...")
  rhf_table = _read_parquets_from_s3(s3, processed_bucket, rhf_keys)
  fhe_table = _read_parquets_from_s3(s3, processed_bucket, fhe_keys)
  ehr_table = _read_parquets_from_s3(s3, processed_bucket, ehr_keys)

  # Build mappings: fact_id → report_id, fact_id → element_id, report_id → entity_id
  fact_to_report: dict[str, str] = {}
  if rhf_table is not None:
    rhf_df = rhf_table.to_pandas()
    for _, row in rhf_df.iterrows():
      fact_to_report[row.get("to")] = row.get("from")

  fact_to_element: dict[str, str] = {}
  if fhe_table is not None:
    fhe_df = fhe_table.to_pandas()
    for _, row in fhe_df.iterrows():
      fact_to_element[row.get("from")] = row.get("to")

  report_to_entity: dict[str, str] = {}
  if ehr_table is not None:
    ehr_df = ehr_table.to_pandas()
    for _, row in ehr_df.iterrows():
      report_to_entity[row.get("to")] = row.get("from")

  context.log.info(
    f"Relationship mappings: {len(fact_to_report)} fact→report, "
    f"{len(fact_to_element)} fact→element, {len(report_to_entity)} report→entity"
  )

  # Build OpenSearch documents
  documents: list[dict[str, Any]] = []
  errors = 0
  skipped = 0

  for _, fact in external_facts.iterrows():
    fact_id = fact.get("identifier")
    value_url = fact.get("value", "")

    # Resolve report early for accession check
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

    # Only index text blocks
    if not element_info.get("is_textblock", False):
      skipped += 1
      continue

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

  context.log.info(
    f"Built {len(documents)} documents ({skipped} skipped, {errors} errors)"
  )

  # Bulk index
  result = {"indexed": 0, "errors": 0}
  if documents:
    result = os_client.bulk_index(documents)
    context.log.info(
      f"Indexed {result['indexed']} text blocks ({result['errors']} errors)"
    )

  return MaterializeResult(
    metadata={
      "graph_id": config.graph_id,
      "source_type": "xbrl_textblock",
      "documents_indexed": result["indexed"],
      "documents_skipped": skipped,
      "errors": errors + result["errors"],
      "total_facts_scanned": len(external_facts),
    }
  )


@asset(
  group_name="sec_pipeline",
  description="Extract and index narrative sections from SEC filings into OpenSearch",
  kinds={"opensearch", "s3"},
  deps=["sec_processed_filings"],
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

  For each filing ZIP in the raw bucket:
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

  # Get already-indexed accessions for incremental skip (scoped to this source type)
  indexed_accessions = _get_indexed_accessions(
    os_client, config.graph_id, source_type="narrative_section"
  )
  if indexed_accessions:
    context.log.info(
      f"Found {len(indexed_accessions)} already-indexed accessions, will skip"
    )

  # Read Report parquets to get filing metadata (accession → form, filing_date, etc.)
  prefix_base = get_processed_key(DataSourceType.SEC, "processed")
  all_parquet_keys = _list_s3_parquet_keys(
    s3, processed_bucket, f"{prefix_base}/filed="
  )

  # Filter by start_year if specified
  if config.start_year:
    all_parquet_keys = [
      k for k in all_parquet_keys if _partition_year(k) >= config.start_year
    ]
    context.log.info(
      f"Filtered to {len(all_parquet_keys)} parquets (>= {config.start_year})"
    )

  report_keys = [k for k in all_parquet_keys if "/nodes/Report/" in k]
  entity_keys = [k for k in all_parquet_keys if "/nodes/Entity/" in k]
  ehr_keys = [k for k in all_parquet_keys if "/relationships/ENTITY_HAS_REPORT/" in k]

  context.log.info(f"Reading {len(report_keys)} Report parquets for metadata...")
  report_table = _read_parquets_from_s3(s3, processed_bucket, report_keys)
  entity_table = _read_parquets_from_s3(s3, processed_bucket, entity_keys)
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
  filings_processed = 0
  sections_extracted = 0
  errors = 0

  # Scan raw bucket for ZIPs
  raw_prefix = get_raw_key(DataSourceType.SEC)
  paginator = s3.get_paginator("list_objects_v2")

  zip_keys: list[str] = []
  for page in paginator.paginate(Bucket=raw_bucket, Prefix=raw_prefix):
    for obj in page.get("Contents", []):
      if obj["Key"].endswith(".zip"):
        zip_keys.append(obj["Key"])

  # Filter ZIPs by start_year if specified (key format: sec/year=2025/CIK/ACCESSION.zip)
  if config.start_year:
    zip_keys = [k for k in zip_keys if _partition_year(k) >= config.start_year]

  context.log.info(f"Found {len(zip_keys)} raw ZIP files")

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

    # Batch index every 100 filings to limit memory
    if len(documents) >= 500:
      result = os_client.bulk_index(documents)
      context.log.info(f"Batch indexed {result['indexed']} sections")
      documents = []

  # Index remaining documents
  result = {"indexed": 0, "errors": 0}
  if documents:
    result = os_client.bulk_index(documents)
    context.log.info(
      f"Indexed {result['indexed']} narrative sections ({result['errors']} errors)"
    )

  context.log.info(
    f"Narrative indexing complete: {filings_processed} filings, "
    f"{sections_extracted} sections, {errors} errors"
  )

  return MaterializeResult(
    metadata={
      "graph_id": config.graph_id,
      "source_type": "narrative_section",
      "filings_processed": filings_processed,
      "sections_extracted": sections_extracted,
      "documents_indexed": result["indexed"],
      "errors": errors + result["errors"],
    }
  )


@asset(
  group_name="sec_pipeline",
  description="Extract iXBRL disclosure sections with XBRL element metadata into OpenSearch",
  kinds={"opensearch"},
  deps=["sec_processed_filings"],
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

  For each filing ZIP:
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

  # Get already-indexed accessions for incremental skip (scoped to this source type)
  indexed_accessions = _get_indexed_accessions(
    os_client, config.graph_id, source_type="ixbrl_disclosure"
  )
  if indexed_accessions:
    context.log.info(
      f"Found {len(indexed_accessions)} already-indexed accessions, will skip"
    )

  # Read Report parquets for filing metadata
  prefix_base = get_processed_key(DataSourceType.SEC, "processed")
  all_parquet_keys = _list_s3_parquet_keys(
    s3, processed_bucket, f"{prefix_base}/filed="
  )

  if config.start_year:
    all_parquet_keys = [
      k for k in all_parquet_keys if _partition_year(k) >= config.start_year
    ]
    context.log.info(
      f"Filtered to {len(all_parquet_keys)} parquets (>= {config.start_year})"
    )

  report_keys = [k for k in all_parquet_keys if "/nodes/Report/" in k]
  entity_keys = [k for k in all_parquet_keys if "/nodes/Entity/" in k]
  ehr_keys = [k for k in all_parquet_keys if "/relationships/ENTITY_HAS_REPORT/" in k]

  context.log.info(f"Reading {len(report_keys)} Report parquets for metadata...")
  report_table = _read_parquets_from_s3(s3, processed_bucket, report_keys)
  entity_table = _read_parquets_from_s3(s3, processed_bucket, entity_keys)
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
  filings_processed = 0
  sections_extracted = 0
  total_elements = 0
  errors = 0

  raw_prefix = get_raw_key(DataSourceType.SEC)
  paginator = s3.get_paginator("list_objects_v2")

  zip_keys: list[str] = []
  for page in paginator.paginate(Bucket=raw_bucket, Prefix=raw_prefix):
    for obj in page.get("Contents", []):
      if obj["Key"].endswith(".zip"):
        zip_keys.append(obj["Key"])

  if config.start_year:
    zip_keys = [k for k in zip_keys if _partition_year(k) >= config.start_year]

  context.log.info(f"Found {len(zip_keys)} raw ZIP files")

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
      result = os_client.bulk_index(documents)
      context.log.info(f"Batch indexed {result['indexed']} disclosures")
      documents = []

  # Index remaining
  result = {"indexed": 0, "errors": 0}
  if documents:
    result = os_client.bulk_index(documents)
    context.log.info(
      f"Indexed {result['indexed']} disclosures ({result['errors']} errors)"
    )

  context.log.info(
    f"iXBRL indexing complete: {filings_processed} filings, "
    f"{sections_extracted} sections, {total_elements} elements, {errors} errors"
  )

  return MaterializeResult(
    metadata={
      "graph_id": config.graph_id,
      "source_type": "ixbrl_disclosure",
      "filings_processed": filings_processed,
      "sections_extracted": sections_extracted,
      "total_elements": total_elements,
      "documents_indexed": result["indexed"],
      "errors": errors + result["errors"],
    }
  )
