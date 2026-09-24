"""SEC full-text indexing into OpenSearch: narrative Item sections and iXBRL
text-block disclosures, both parsed from the raw filing ZIPs by ``xbrlkit.text``.

A section longer than ``part_size`` is indexed as consecutive parts: each part
is its own document carrying ``part`` / ``part_count``, a ``parent_document_id``
shared by the section's parts, and the ``next_document_id`` to read on. An
unsplit section keeps its unsplit document id.
"""

import gc
import hashlib
import io
import math
import re
import zipfile
from typing import Any

import boto3
import pandas as pd
from dagster import AssetExecutionContext, BackfillPolicy, MaterializeResult, asset

from robosystems.config import env
from robosystems.config.storage.shared import (
  PUBLIC_DATA_STORAGE_CLASS,
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

# The model's 512-token window is ~2000 chars; the tokenizer truncates the
# rest anyway, so longer input only costs memory.
_EMBEDDING_MAX_CHARS = 2000

# Small batches bound tokenizer/ONNX intermediate buffers.
_EMBEDDING_BATCH_SIZE = 200


def _embed_document_batch(enricher, documents: list[dict]) -> bool:
  """Add embeddings in place. False on failure; the batch is then indexed
  without vectors."""
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


def _narrative_keys(
  year: str, cik: str, accession: str, section: Any
) -> tuple[str, str | None]:
  """(the key this section part is written to, the stale key it replaces).

  A section long enough to split is written as ``_part{n}`` objects. The
  unsplit object a run before the split left beside them is no longer what the
  index points at, so the first part names it for deletion. An unsplit section
  replaces nothing.
  """
  unsplit = f"{year}/{cik}/{accession}/narrative_{section.section_id}.txt"
  if section.part_count <= 1:
    return unsplit, None
  key = unsplit.removesuffix(".txt") + f"_part{section.part}.txt"
  return key, unsplit if section.part == 1 else None


def _cell(value: Any) -> str:
  """A parquet cell as text; a missing value is an empty string.

  ``str()`` on a pandas missing value is the truthy ``"<NA>"``, which once
  reached the index as the ticker of every filer without one.
  """
  if value is None or pd.isna(value):
    return ""
  return str(value)


def _get_s3_client():
  """S3 client; LocalStack endpoint in dev."""
  kwargs: dict[str, Any] = {"region_name": env.AWS_REGION}
  if env.ENVIRONMENT == "dev":
    endpoint = env.AWS_ENDPOINT_URL
    if endpoint:
      kwargs["endpoint_url"] = endpoint
  return boto3.client("s3", **kwargs)


def _get_processed_bucket() -> str:
  return env.SHARED_PROCESSED_BUCKET


def _get_raw_bucket() -> str:
  return env.SHARED_RAW_BUCKET


def _get_public_data_bucket() -> str:
  return env.PUBLIC_DATA_BUCKET


def _get_public_data_cdn_url() -> str:
  return env.PUBLIC_DATA_CDN_URL


def _get_indexed_accessions(
  os_client, graph_id: str, source_type: str | None = None
) -> set[str]:
  """Accessions already indexed, paged through a composite aggregation.

  ``source_type`` scopes it so each asset tracks its own progress. Any query
  failure (including a missing index) yields an empty set.
  """
  accessions: set[str] = set()
  after: dict | None = None

  try:
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
  """Concatenate parquet files into one Arrow table; None if none could be read.

  Unreadable files are logged and skipped.
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
  """The main filing HTML from a filing ZIP.

  Prefers the file carrying a dei:DocumentType tag, since an exhibit can be
  larger than the filing itself; falls back to the largest non-exhibit HTM
  (pre-iXBRL filings).
  """
  with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
    htm_candidates: list[tuple[str, int]] = []
    for info in zf.infolist():
      name_lower = info.filename.lower()
      if not name_lower.endswith((".htm", ".html")):
        continue
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
      # R-files are viewer artifacts.
      basename = name_lower.rsplit("/", 1)[-1]
      if re.match(r"^r\d+\.htm", basename):
        continue
      htm_candidates.append((info.filename, info.file_size))

    if not htm_candidates:
      return None

    # Largest first; decode only the first 2 MB for the tag check.
    for filename, _ in sorted(htm_candidates, key=lambda x: -x[1]):
      raw = zf.read(filename)
      prefix = raw[:2_000_000].decode("utf-8", errors="replace")
      if _extract_ixbrl_doc_type(prefix) is not None:
        return raw.decode("utf-8", errors="replace")

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
  """The dei:DocumentType value (e.g. '10-K'), or None; nested tags are stripped."""
  match = re.search(
    r'name=["\']dei:DocumentType["\'][^>]*>(.*?)</ix:non',
    html[:2000000],
    re.IGNORECASE | re.DOTALL,
  )
  if match:
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
  """Extract Item sections from one quarter's filings, write each part's text to
  the public CDN bucket, and index it with its content_url."""
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

  from robosystems.adapters.sec.enrichment import SemanticEnricher

  enricher = SemanticEnricher()
  context.log.info("Loaded SemanticEnricher for embedding generation")

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

  partition_key = context.partition_key
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

  form_types_upper = [ft.upper() for ft in config.form_types]
  target_reports = reports_df[
    reports_df["form"].str.upper().isin(form_types_upper)
  ].copy()
  context.log.info(
    f"Found {len(target_reports)} reports matching form types {form_types_upper}"
  )

  entity_lookup: dict[str, dict[str, str]] = {}
  report_to_entity: dict[str, str] = {}
  if entity_table is not None:
    entities_df = entity_table.to_pandas()
    for _, row in entities_df.iterrows():
      entity_lookup[row.get("identifier")] = {
        "ticker": _cell(row.get("ticker")),
        "name": _cell(row.get("name")),
        "cik": _cell(row.get("cik")),
      }
  if ehr_table is not None:
    ehr_df = ehr_table.to_pandas()
    for _, row in ehr_df.iterrows():
      report_to_entity[row.get("to")] = row.get("from")

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
      "filing_date": _cell(report.get("filing_date")),
      "fiscal_year": int(fy) if fy is not None and not math.isnan(fy) else None,
      "fiscal_period": report.get("fiscal_period_focus", ""),
      "cik": entity_info.get("cik", ""),
      "ticker": entity_info.get("ticker", ""),
      "entity_name": entity_info.get("name", ""),
    }

  context.log.info(f"Built metadata for {len(accession_metadata)} accessions")

  del reports_df, report_table, target_reports, entity_lookup, report_to_entity
  del all_parquet_keys, report_keys, entity_keys, ehr_keys
  gc.collect()

  # Parts are at most ~1.3x part_size (~32 KB at the default), so 100 stay well
  # under OpenSearch's 10 MB bulk limit.
  batch_size = 100
  documents: list[dict[str, Any]] = []
  total_indexed = 0
  filings_processed = 0
  sections_extracted = 0
  errors = 0

  # Raw ZIPs are partitioned by year; accession_metadata narrows to the quarter.
  partition_year = partition_key.split("-Q")[0]
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
      # Key shape: sec/year=YYYY/CIK/ACCESSION.zip
      filename = zip_key.rsplit("/", 1)[-1]
      accession = filename.replace(".zip", "")

      if accession not in accession_metadata:
        continue

      if accession in indexed_accessions:
        continue

      meta = accession_metadata[accession]

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

        # Catches a proxy statement picked in place of the filing.
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

        sections = extractor.extract(html_content, ixbrl_doc_type or meta["form_type"])
        filings_processed += 1

        if not sections:
          continue

        for section in sections:
          sections_extracted += 1

          narrative_key, stale_key = _narrative_keys(year, cik, accession, section)
          content_url_value = ""

          if public_bucket:
            try:
              s3.put_object(
                Bucket=public_bucket,
                Key=narrative_key,
                Body=section.content.encode("utf-8"),
                ContentType="text/plain; charset=utf-8",
                StorageClass=PUBLIC_DATA_STORAGE_CLASS,
              )
              content_url_value = get_public_data_url(
                public_bucket, narrative_key, cdn_url
              )
            except Exception as e:
              context.log.debug(f"Failed to externalize {narrative_key}: {e}")

            if stale_key and content_url_value:
              try:
                s3.delete_object(Bucket=public_bucket, Key=stale_key)
              except Exception as e:
                context.log.debug(f"Failed to delete stale {stale_key}: {e}")

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
  """Index one quarter's iXBRL text-block disclosures with the element qnames
  nested in each, so search and graph can navigate to each other.

  content_url is the CDN copy of the matching externalized textblock Fact.
  """
  from xbrlkit.text import iXBRLParser

  from robosystems.operations.search.client import OpenSearchClient

  s3 = _get_s3_client()
  raw_bucket = _get_raw_bucket()
  processed_bucket = _get_processed_bucket()

  parser = iXBRLParser(part_size=config.part_size or None)

  os_client = OpenSearchClient(env.OPENSEARCH_URL, env.OPENSEARCH_INDEX)
  os_client.create_index_if_not_exists()

  from robosystems.adapters.sec.enrichment import SemanticEnricher

  enricher = SemanticEnricher()
  context.log.info("Loaded SemanticEnricher for embedding generation")

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

  partition_key = context.partition_key
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

  report_id_to_accession: dict[str, str] = {}
  for _, report in target_reports.iterrows():
    rid = report.get("identifier", "")
    acc = report.get("accession_number", "")
    if rid and acc:
      report_id_to_accession[rid] = acc

  entity_lookup: dict[str, dict[str, str]] = {}
  report_to_entity: dict[str, str] = {}
  if entity_table is not None:
    entities_df = entity_table.to_pandas()
    for _, row in entities_df.iterrows():
      entity_lookup[row.get("identifier")] = {
        "ticker": _cell(row.get("ticker")),
        "name": _cell(row.get("name")),
        "cik": _cell(row.get("cik")),
      }
  if ehr_table is not None:
    ehr_df = ehr_table.to_pandas()
    for _, row in ehr_df.iterrows():
      report_to_entity[row.get("to")] = row.get("from")

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
      "filing_date": _cell(report.get("filing_date")),
      "fiscal_year": int(fy) if fy is not None and not math.isnan(fy) else None,
      "fiscal_period": report.get("fiscal_period_focus", ""),
      "cik": entity_info.get("cik", ""),
      "ticker": entity_info.get("ticker", ""),
      "entity_name": entity_info.get("name", ""),
    }

  context.log.info(f"Built metadata for {len(accession_metadata)} accessions")

  # (accession, textblock qname) → content_url, joined across Element,
  # FACT_HAS_ELEMENT, Fact and REPORT_HAS_FACT one file at a time to bound memory.
  cdn_url_lookup: dict[tuple[str, str], str] = {}

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

  del reports_df, report_table, target_reports, entity_lookup, report_to_entity
  del report_id_to_accession
  del all_parquet_keys, report_keys, entity_keys, ehr_keys
  del element_keys, fact_keys, fhe_keys, rhf_keys
  gc.collect()

  # Parts are at most ~1.3x part_size (~32 KB at the default), so 100 stay well
  # under OpenSearch's 10 MB bulk limit.
  batch_size = 100
  documents: list[dict[str, Any]] = []
  total_indexed = 0
  filings_processed = 0
  sections_extracted = 0
  total_elements = 0
  errors = 0

  # Raw ZIPs are partitioned by year; accession_metadata narrows to the quarter.
  partition_year = partition_key.split("-Q")[0]
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
