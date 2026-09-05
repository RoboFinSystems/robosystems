"""The per-filer catalog on the public CDN — the public pages' index, without a database.

``companies/{ticker}.json`` lists one filer's filings with their public
representations; ``companies/index.json`` lists every filer with its latest
filing. Both are a fold over the processed Report and Entity tables — the
same parquet the graph is built from — joined to each filing's
``manifest.json``, which the processor wrote beside the artifacts. They are
regenerated whole: a run rewrites the file of every filer it touched (the
filers with a filing in its partitions, or all of them on ``full_rebuild``)
and the index always. Nothing is patched in place, so two overlapping runs
cannot corrupt a file: the later write is a complete view as of its read,
stale by at most one cycle.

Reads: the Entity, Report and ENTITY_HAS_REPORT parquet of every partition
from ``start_year`` on (three small tables), and one manifest per filing of
a touched filer. Writes: one object per touched filer, the index, and
``robots.txt`` when it is missing.
"""

import json
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from typing import Any
from urllib.parse import quote

import pandas as pd
from dagster import AssetExecutionContext, BackfillPolicy, MaterializeResult, asset

from robosystems.config import env
from robosystems.config.storage.shared import (
  FILING_ARTIFACT_MANIFEST,
  FILING_CATALOG_INDEX_KEY,
  FILING_ROBOTS_KEY,
  DataSourceType,
  get_filing_artifact_key,
  get_filing_catalog_key,
  get_processed_key,
)
from robosystems.logger import logger
from robosystems.operations.aws.s3 import S3Client

from .configs import SEC_QUARTERS, SECFilingCatalogConfig, sec_quarter_partitions
from .text_index import _get_s3_client

CATALOG_VERSION = 1
CATALOG_MEDIA_TYPE = "application/json"
CATALOG_CACHE_CONTROL = "public, max-age=60"
ROBOTS_CACHE_CONTROL = "public, max-age=3600"

# Filed documents, the text-block fragments and the narrative extracts are
# served, never indexed — the CDN adds the X-Robots-Tag on the same extensions
# (cloudformation/s3.yaml). The holon, the Tavi and the catalog stay indexable.
ROBOTS_TXT = "User-agent: *\nDisallow: /*.htm$\nDisallow: /*.html$\nDisallow: /*.txt$\n"

REPORT_COLUMNS = [
  "identifier",
  "accession_number",
  "form",
  "filing_date",
  "report_date",
  "fiscal_year_focus",
  "fiscal_period_focus",
  "updated_at",
]
ENTITY_COLUMNS = [
  "identifier",
  "cik",
  "ticker",
  "name",
  "exchange",
  "sic",
  "sic_description",
  "updated_at",
]
RELATIONSHIP_COLUMNS = ["from", "to"]

TABLE_PATHS = {
  "Report": ("nodes", "Report"),
  "Entity": ("nodes", "Entity"),
  "ENTITY_HAS_REPORT": ("relationships", "ENTITY_HAS_REPORT"),
}


# ── the fold (pure) ─────────────────────────────────────────────────────────


def corpus_partitions(start_year: int) -> list[str]:
  """The quarter partitions the catalog spans."""
  return [q for q in SEC_QUARTERS if int(q.split("-")[0]) >= start_year]


def _text(value: Any) -> str | None:
  if value is None or (isinstance(value, float) and pd.isna(value)):
    return None
  text = str(value).strip()
  return text or None


def _int(value: Any) -> int | None:
  text = _text(value)
  if text is None:
    return None
  try:
    return int(float(text))
  except ValueError:
    return None


def filers(entities: pd.DataFrame) -> dict[str, dict[str, Any]]:
  """One row per CIK — the most recently written Entity row, so a ticker or
  name change on a later filing wins."""
  out: dict[str, dict[str, Any]] = {}
  if entities.empty:
    return out
  ordered = entities.sort_values("updated_at", na_position="first")
  for row in ordered.to_dict("records"):
    cik = _text(row.get("cik"))
    if not cik:
      continue
    out[cik] = {
      "cik": cik,
      "ticker": _text(row.get("ticker")),
      "name": _text(row.get("name")),
      "exchange": _text(row.get("exchange")),
      "sic": _text(row.get("sic")),
      "sic_description": _text(row.get("sic_description")),
    }
  return out


def filings_by_cik(
  reports: pd.DataFrame,
  relationships: pd.DataFrame,
  entities: pd.DataFrame,
  form_types: list[str],
) -> dict[str, list[dict[str, Any]]]:
  """Each filer's listed filings, newest first, one per accession.

  A filing reprocessed into a later batch appears twice in the parquet; the
  row written last wins. Forms outside ``form_types`` carry no statements
  and are dropped.
  """
  out: dict[str, list[dict[str, Any]]] = {}
  if reports.empty or relationships.empty or entities.empty:
    return out
  entity_cik = {
    _text(r["identifier"]): _text(r["cik"])
    for r in entities[["identifier", "cik"]].to_dict("records")
  }
  report_entity = {
    _text(r["to"]): _text(r["from"])
    for r in relationships[["from", "to"]].to_dict("records")
  }
  wanted = {f.upper() for f in form_types}
  latest: dict[str, dict[str, Any]] = {}
  for row in reports.sort_values("updated_at", na_position="first").to_dict("records"):
    accession = _text(row.get("accession_number"))
    form = (_text(row.get("form")) or "").upper()
    if not accession or form not in wanted:
      continue
    cik = entity_cik.get(report_entity.get(_text(row.get("identifier"))))
    if not cik:
      continue
    latest[accession] = {
      "cik": cik,
      "accession": accession,
      "form": form,
      "filing_date": _text(row.get("filing_date")),
      "report_date": _text(row.get("report_date")),
      "fiscal_year": _int(row.get("fiscal_year_focus")),
      "fiscal_period": _text(row.get("fiscal_period_focus")),
      "report_id": _text(row.get("identifier")),
    }
  for filing in latest.values():
    out.setdefault(filing["cik"], []).append(filing)
  for filings in out.values():
    filings.sort(key=lambda f: (f["filing_date"] or "", f["accession"]), reverse=True)
  return out


def filing_folder(filing: dict[str, Any]) -> tuple[str, str, str] | None:
  """``(year, cik, accession)`` of a filing's artifact folder, or None."""
  filing_date = filing.get("filing_date")
  if not filing_date:
    return None
  return filing_date[:4], filing["cik"], filing["accession"]


def viewer_link(viewer_url: str, url: str) -> str:
  return f"{viewer_url.rstrip('/')}/?url={quote(url, safe='')}"


def build_company(
  filer: dict[str, Any],
  filings: list[dict[str, Any]],
  manifests: dict[str, dict[str, Any] | None],
  *,
  viewer_url: str,
) -> dict[str, Any]:
  """One filer's catalog file: its filings, each with the representations its
  manifest lists (none when the filing predates the artifacts), a viewer link
  per openable representation, and the latest filing per form."""
  entries = []
  latest: dict[str, str] = {}
  for filing in filings:
    manifest = manifests.get(filing["accession"]) or {}
    representations = list(manifest.get("representations") or [])
    viewer = {
      r["kind"]: viewer_link(viewer_url, r["url"])
      for r in representations
      if r.get("kind") in ("holon", "tavi") and r.get("url")
    }
    entries.append(
      {
        "accession": filing["accession"],
        "form": filing["form"],
        "filing_date": filing["filing_date"],
        "report_date": filing["report_date"],
        "fiscal_year": filing["fiscal_year"],
        "fiscal_period": filing["fiscal_period"],
        "report_id": filing["report_id"],
        "folder": manifest.get("folder"),
        "representations": representations,
        "viewer": viewer,
      }
    )
    if representations:
      latest.setdefault(filing["form"], filing["accession"])
  return {
    "version": CATALOG_VERSION,
    "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
    "source": "SEC EDGAR",
    **filer,
    "filings": entries,
    "latest": latest,
  }


def index_row(filer: dict[str, Any], filings: list[dict[str, Any]]) -> dict[str, Any]:
  newest = filings[0]
  return {
    "ticker": filer["ticker"],
    "cik": filer["cik"],
    "name": filer["name"],
    "exchange": filer["exchange"],
    "sic_description": filer["sic_description"],
    "filings": len(filings),
    "latest": {
      "accession": newest["accession"],
      "form": newest["form"],
      "filing_date": newest["filing_date"],
      "report_date": newest["report_date"],
      "fiscal_year": newest["fiscal_year"],
      "fiscal_period": newest["fiscal_period"],
    },
  }


def build_index(rows: list[dict[str, Any]]) -> dict[str, Any]:
  companies = sorted(rows, key=lambda r: (r["ticker"] or "", r["cik"]))
  return {
    "version": CATALOG_VERSION,
    "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
    "source": "SEC EDGAR",
    "count": len(companies),
    "companies": companies,
  }


# ── reads ────────────────────────────────────────────────────────────────────


def _table_keys(s3: Any, bucket: str, partition: str, table: str) -> list[str]:
  """Parquet keys of one table in one partition, in either layout the process
  stage has written (``nodes/Report/part.parquet`` or ``nodes/Report.parquet``)."""
  kind, name = TABLE_PATHS[table]
  base = get_processed_key(DataSourceType.SEC, "processed", f"filed={partition}", kind)
  keys: list[str] = []
  paginator = s3.get_paginator("list_objects_v2")
  for page in paginator.paginate(Bucket=bucket, Prefix=f"{base}/{name}"):
    for obj in page.get("Contents", []):
      key = obj["Key"]
      if key.endswith(".parquet") and (
        f"/{name}/" in key or key.endswith(f"/{name}.parquet")
      ):
        keys.append(key)
  return keys


def _read_table(
  s3: Any, bucket: str, keys: list[str], columns: list[str], partition: str
) -> pd.DataFrame:
  import io

  import pyarrow.parquet as pq

  frames = []
  for key in keys:
    try:
      body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
      table = pq.read_table(io.BytesIO(body))
      present = [c for c in columns if c in table.column_names]
      frame = table.select(present).to_pandas()
      for missing in columns:
        if missing not in frame.columns:
          frame[missing] = None
      frame["partition"] = partition
      frames.append(frame)
    except Exception as e:
      logger.warning(f"Failed to read parquet {key}: {e}")
  if not frames:
    return pd.DataFrame(columns=[*columns, "partition"])
  return pd.concat(frames, ignore_index=True)


def read_corpus(
  s3: Any, bucket: str, partitions: list[str]
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
  """The Report, Entity and ENTITY_HAS_REPORT rows of every partition, each row
  tagged with its partition."""
  reports, entities, relationships = [], [], []
  for partition in partitions:
    reports.append(
      _read_table(
        s3,
        bucket,
        _table_keys(s3, bucket, partition, "Report"),
        REPORT_COLUMNS,
        partition,
      )
    )
    entities.append(
      _read_table(
        s3,
        bucket,
        _table_keys(s3, bucket, partition, "Entity"),
        ENTITY_COLUMNS,
        partition,
      )
    )
    relationships.append(
      _read_table(
        s3,
        bucket,
        _table_keys(s3, bucket, partition, "ENTITY_HAS_REPORT"),
        RELATIONSHIP_COLUMNS,
        partition,
      )
    )
  return (
    _concat(reports, REPORT_COLUMNS),
    _concat(entities, ENTITY_COLUMNS),
    _concat(relationships, RELATIONSHIP_COLUMNS),
  )


def _concat(frames: list[pd.DataFrame], columns: list[str]) -> pd.DataFrame:
  """Concatenate the non-empty frames (an empty one would only warn), else an
  empty frame with the expected columns."""
  present = [f for f in frames if not f.empty]
  if not present:
    return pd.DataFrame(columns=[*columns, "partition"])
  return pd.concat(present, ignore_index=True)


def read_manifests(
  s3: Any,
  bucket: str,
  filings: list[dict[str, Any]],
  workers: int,
) -> dict[str, dict[str, Any] | None]:
  """Each filing's manifest, or None when the filing has none yet."""

  def one(filing: dict[str, Any]) -> tuple[str, dict[str, Any] | None]:
    folder = filing_folder(filing)
    if folder is None:
      return filing["accession"], None
    key = get_filing_artifact_key(*folder, FILING_ARTIFACT_MANIFEST)
    try:
      body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
      return filing["accession"], json.loads(body)
    except s3.exceptions.NoSuchKey:
      return filing["accession"], None
    except Exception as e:
      logger.warning(f"Manifest read failed for {key}: {e}")
      return filing["accession"], None

  with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
    return dict(pool.map(one, filings))


# ── the asset ────────────────────────────────────────────────────────────────


def _run_partitions(context: AssetExecutionContext) -> set[str]:
  try:
    return set(context.partition_keys)
  except Exception:
    return {context.partition_key}


def _dump(document: dict[str, Any]) -> str:
  return json.dumps(document, separators=(",", ":"), ensure_ascii=False)


@asset(
  group_name="sec_pipeline",
  description="Regenerate the per-filer catalog and corpus index on the public CDN",
  kinds={"s3"},
  deps=["sec_processed_filings"],
  partitions_def=sec_quarter_partitions,
  backfill_policy=BackfillPolicy.single_run(),
  metadata={
    "pipeline": "sec",
    "stage": "catalog",
  },
)
def sec_filing_catalog(
  context: AssetExecutionContext,
  config: SECFilingCatalogConfig,
) -> MaterializeResult:
  """Fold the processed Report/Entity tables and the filings' manifests into
  the filer catalog and corpus index on the public CDN.

  Partitioned by quarter. Rewrites the catalog file of every filer with a
  filing in the run's partitions (every filer on ``full_rebuild``), and the
  corpus index always.
  """
  public_bucket = env.PUBLIC_DATA_BUCKET
  if not public_bucket:
    context.log.warning("No public data bucket configured; catalog skipped")
    return MaterializeResult(metadata={"status": "skipped", "reason": "no_bucket"})

  s3 = _get_s3_client()
  writer = S3Client()
  processed_bucket = env.SHARED_PROCESSED_BUCKET
  run_partitions = _run_partitions(context)
  partitions = corpus_partitions(config.start_year)
  context.log.info(
    f"Catalog: folding {len(partitions)} partitions from {config.start_year}; "
    f"run partitions {sorted(run_partitions)}"
  )

  reports, entities, relationships = read_corpus(s3, processed_bucket, partitions)
  context.log.info(
    f"Read {len(reports)} report rows, {len(entities)} entity rows, "
    f"{len(relationships)} entity-report rows"
  )
  by_cik = filings_by_cik(reports, relationships, entities, config.form_types)
  filer_rows = filers(entities)

  listed: dict[str, tuple[dict[str, Any], list[dict[str, Any]]]] = {}
  for cik, filer in filer_rows.items():
    if config.require_ticker and not filer["ticker"]:
      continue
    filings = by_cik.get(cik)
    if filings:
      listed[cik] = (filer, filings)

  if config.full_rebuild:
    touched = set(listed)
  else:
    in_run = entities[entities["partition"].isin(run_partitions)]
    touched = {c for c in (_text(v) for v in in_run["cik"]) if c} & set(listed)
  context.log.info(f"{len(listed)} filers listed; rewriting {len(touched)}")

  written = 0
  failed = 0
  for cik in sorted(touched):
    filer, filings = listed[cik]
    manifests = read_manifests(s3, public_bucket, filings, config.manifest_workers)
    document = build_company(filer, filings, manifests, viewer_url=config.viewer_url)
    ok = writer.upload_string(
      _dump(document),
      public_bucket,
      get_filing_catalog_key(filer["ticker"]),
      content_type=CATALOG_MEDIA_TYPE,
      cache_control=CATALOG_CACHE_CONTROL,
    )
    written += int(ok)
    failed += int(not ok)

  index = build_index([index_row(f, fs) for f, fs in listed.values()])
  index_ok = writer.upload_string(
    _dump(index),
    public_bucket,
    FILING_CATALOG_INDEX_KEY,
    content_type=CATALOG_MEDIA_TYPE,
    cache_control=CATALOG_CACHE_CONTROL,
  )

  if not writer.object_exists(public_bucket, FILING_ROBOTS_KEY):
    writer.upload_string(
      ROBOTS_TXT,
      public_bucket,
      FILING_ROBOTS_KEY,
      content_type="text/plain",
      cache_control=ROBOTS_CACHE_CONTROL,
    )

  context.log.info(
    f"Catalog: {written} filer files written, {failed} failed, "
    f"index of {index['count']} {'written' if index_ok else 'FAILED'}"
  )
  return MaterializeResult(
    metadata={
      "status": "success" if failed == 0 and index_ok else "partial",
      "graph_id": config.graph_id,
      "filers_listed": len(listed),
      "filers_written": written,
      "filers_failed": failed,
      "index_written": index_ok,
      "partitions": len(partitions),
    }
  )
