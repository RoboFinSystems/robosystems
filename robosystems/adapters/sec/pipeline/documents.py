"""The filed documents the XBRL path does not bring: 8-K earnings releases,
and the primary documents of filings processed before inline XBRL.

Both are fetched from EDGAR once, kept, and served from the public bucket,
so nothing downstream (the filing-text tools) ever goes back to EDGAR, and
neither goes into the search index: a filing is read from its folder whole.
Jobs that pull from EDGAR carry the ``edgar`` tag, which the run queue limits
to one at a time, so these never add their rate to the XBRL download's.

- ``sec_current_reports`` — EFTS finds the quarter's 8-Ks with the items
  each reports; one request fetches a wanted filing's ``-xbrl.zip`` (its
  exhibits ride inside) into the raw bucket, the 8-K with its EX-99 exhibits
  is published to the filing's public folder with a manifest, and the filer's
  releases list (``current-reports/{cik}.json``) gains it — the 8-K's way in,
  since it is not in the graph or the catalog.
- ``sec_filing_documents`` — the primary document a pre-inline filing's
  manifest names but its folder lacks, fetched and added to the manifest.
"""

import asyncio
import gc
import json
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime, timedelta
from typing import Any

from dagster import (
  AssetExecutionContext,
  BackfillPolicy,
  DagsterRunStatus,
  Failure,
  MaterializeResult,
  RunsFilter,
  asset,
)

from robosystems.config import env
from robosystems.config.storage.shared import (
  FILING_ARTIFACT_MANIFEST,
  DataSourceType,
  get_current_report_raw_key,
  get_current_reports_list_key,
  get_filing_artifact_key,
  get_filing_artifact_prefix,
  get_public_data_url,
  get_raw_key,
)

from .configs import (
  SECCurrentReportsConfig,
  SECFilingDocumentsConfig,
  sec_quarter_partitions,
)

# Concurrent read-merge-writes of the filers' releases lists.
LIST_WORKERS = 16

_QUARTER_MONTHS = {1: (1, 3), 2: (4, 6), 3: (7, 9), 4: (10, 12)}


def quarter_bounds(partition: str) -> tuple[date, date]:
  """First and last day of a ``YYYY-QN`` partition."""
  year, quarter = partition.split("-Q")
  first, last = _QUARTER_MONTHS[int(quarter)]
  start = date(int(year), first, 1)
  end = (
    date(int(year) + 1, 1, 1) if last == 12 else date(int(year), last + 1, 1)
  ) - timedelta(days=1)
  return start, end


def quarter_of(day: date) -> str:
  """The ``YYYY-QN`` partition a filing date falls in."""
  return f"{day.year}-Q{(day.month - 1) // 3 + 1}"


def discovery_window(
  partition: str,
  since_days: int | None,
  today: date,
  *,
  within_quarter: bool = False,
) -> tuple[date, date] | None:
  """The dates to discover in, or None when the window is empty (a future
  quarter, or a nightly run whose quarter ended before its look-back begins).

  A look-back reaches into the previous quarter when it has to: a nightly run
  on October 3 covers September 26 on, so a night missed at a quarter's end is
  caught up, and what it finds is kept under its own filing quarter.
  ``within_quarter`` stops it at the quarter's first day, for a caller that
  files everything it finds under the run's quarter.
  """
  first, end = quarter_bounds(partition)
  start, end = first, min(end, today)
  if since_days is not None:
    start = today - timedelta(days=since_days)
    if within_quarter:
      start = max(start, first)
  return (start, end) if start <= end else None


# Jobs that pull from EDGAR, which the `edgar` run tag keeps to one at a time
# in the run queue. A run launched past the queue (the asset's Materialize
# button, the CLI) carries no tag, so the assets check for themselves.
EDGAR_PULL_JOBS = (
  "sec_download",
  "sec_current_reports_capture",
  "sec_filing_documents_fetch",
)


def refuse_concurrent_edgar_pull(context: AssetExecutionContext) -> None:
  """Fail before the first request when another EDGAR pull is running."""
  filters = [RunsFilter(tags={"edgar": "pull"}, statuses=[DagsterRunStatus.STARTED])]
  filters += [
    RunsFilter(job_name=job, statuses=[DagsterRunStatus.STARTED])
    for job in EDGAR_PULL_JOBS
  ]
  for run_filter in filters:
    for run in context.instance.get_runs(filters=run_filter, limit=5):
      if run.run_id != context.run_id:
        raise Failure(
          f"Another EDGAR pull is running ({run.job_name} {run.run_id[:8]}); "
          "two at once would exceed EDGAR's rate limit. Wait for it, or launch "
          "through the job so the run queue orders them."
        )


def _run_partitions(context: AssetExecutionContext) -> list[str]:
  try:
    return sorted(context.partition_keys)
  except Exception:
    return [context.partition_key]


def _corpus_ciks(s3: Any, raw_bucket: str) -> set[str]:
  """Every filer the XBRL download has seen: one submissions file per CIK."""
  prefix = get_raw_key(DataSourceType.SEC, "submissions") + "/"
  ciks: set[str] = set()
  paginator = s3.get_paginator("list_objects_v2")
  for page in paginator.paginate(Bucket=raw_bucket, Prefix=prefix):
    for obj in page.get("Contents", []):
      name = obj["Key"].rsplit("/", 1)[-1]
      if name.endswith(".json"):
        ciks.add(name.removesuffix(".json").zfill(10))
  return ciks


def _read_object(s3: Any, bucket: str, key: str) -> bytes | None:
  try:
    return s3.get_object(Bucket=bucket, Key=key)["Body"].read()
  except s3.exceptions.NoSuchKey:
    return None


def _read_manifest(s3: Any, bucket: str, key: str) -> dict[str, Any] | None:
  from robosystems.operations.aws.s3 import gunzip_if_gzipped

  body = _read_object(s3, bucket, key)
  return json.loads(gunzip_if_gzipped(body)) if body else None


def _published_files(s3: Any, bucket: str, key: str) -> list[dict[str, Any]] | None:
  """The files a published filing's manifest names, or None when the filing
  is not published: no manifest, one that cannot be read, or one that names
  nothing. The caller publishes those again, which is what repairs them — a
  broken manifest left in place would be skipped by every later run."""
  try:
    manifest = _read_manifest(s3, bucket, key)
  except Exception:
    return None
  files = manifest.get("representations") if isinstance(manifest, dict) else None
  return files if isinstance(files, list) and files else None


def _write_manifest(
  writer: Any, bucket: str, key: str, manifest: dict[str, Any]
) -> bool:
  from robosystems.adapters.sec.processors.artifacts import (
    JSON_MEDIA_TYPE,
    MANIFEST_CACHE_CONTROL,
    put_public_artifact,
  )

  return put_public_artifact(
    writer,
    bucket,
    key,
    json.dumps(manifest, indent=2, default=str).encode("utf-8"),
    JSON_MEDIA_TYPE,
    cache_control=MANIFEST_CACHE_CONTROL,
  )


def _publish_document(
  writer: Any, bucket: str, cdn_url: str, key: str, name: str, data: bytes
) -> dict[str, Any] | None:
  """One filed document into the public folder; its representation, or None
  when the upload failed."""
  from robosystems.adapters.sec.processors.artifacts import (
    GZIP_DOCUMENT_EXTENSIONS,
    document_media_type,
    put_public_artifact,
  )

  media_type = document_media_type(name)
  ok = put_public_artifact(
    writer,
    bucket,
    key,
    data,
    media_type,
    compress=name.lower().endswith(GZIP_DOCUMENT_EXTENSIONS),
  )
  if not ok:
    return None
  return {
    "name": name,
    "media_type": media_type,
    "bytes": len(data),
    "url": get_public_data_url(bucket, key, cdn_url),
  }


async def _fetch(session: Any, url: str, log: Any) -> tuple[int, bytes]:
  from .download import _get_with_429_retry

  return await _get_with_429_retry(session, url, log)


async def _drain(tasks: list[Any], stats: Counter, label: str, log: Any) -> None:
  """Await every task; one that raises counts as failed rather than ending a
  run that may be hours into a backfill."""
  for done, coro in enumerate(asyncio.as_completed(tasks), start=1):
    try:
      await coro
    except Exception as e:
      log.warning(f"{label}: {e}")
      stats["failed"] += 1
    if done % 250 == 0:
      log.info(f"  {label}: {done}/{len(tasks)} handled ({dict(stats)})")


# ── 8-K earnings releases ──────────────────────────────────────────────────


async def _capture_current_reports(
  hits: list[Any],
  partition: str,
  config: SECCurrentReportsConfig,
  log: Any,
) -> tuple[Counter, list[Any]]:
  """Fetch (or read back from raw) and publish each wanted 8-K. Returns the
  counts and the 8-Ks now in their public folders, published before or now."""
  import aiohttp

  from robosystems.adapters.sec.client.current_reports import filing_zip_url
  from robosystems.adapters.sec.client.rate_limiter import AsyncRateLimiter
  from robosystems.adapters.sec.config import SEC_CONFIG
  from robosystems.adapters.sec.processors.current_reports import (
    current_report_manifest,
    documents_in_zip,
  )
  from robosystems.operations.aws.s3 import S3Client

  from .text_index import _get_s3_client

  s3 = _get_s3_client()
  writer = S3Client()
  raw_bucket = env.SHARED_RAW_BUCKET
  public_bucket = env.PUBLIC_DATA_BUCKET
  cdn_url = env.PUBLIC_DATA_CDN_URL
  limiter = AsyncRateLimiter(rate=config.download_rate)
  semaphore = asyncio.Semaphore(config.download_concurrency)
  stats: Counter = Counter()
  # (hit, the files its manifest names)
  published: list[tuple[Any, list[dict[str, Any]] | None]] = []

  async with aiohttp.ClientSession(headers=SEC_CONFIG["headers"]) as session:

    async def one(hit: Any) -> None:
      year = hit.filing_date[:4]
      manifest_key = get_filing_artifact_key(
        year, hit.cik, hit.accession, FILING_ARTIFACT_MANIFEST
      )
      if not config.republish:
        # Read rather than checked for: the manifest names the filing's files,
        # which its releases-list entry needs when the run that published it
        # ended before the list was written.
        files = await asyncio.to_thread(
          _published_files, s3, public_bucket, manifest_key
        )
        if files is not None:
          stats["already_published"] += 1
          published.append((hit, files))
          return

      raw_key = get_current_report_raw_key(
        quarter_of(date.fromisoformat(hit.filing_date)), hit.cik, hit.accession
      )
      zip_bytes = await asyncio.to_thread(_read_object, s3, raw_bucket, raw_key)
      if zip_bytes is None:
        async with semaphore:
          async with limiter:
            try:
              status, zip_bytes = await _fetch(
                session, filing_zip_url(hit.cik, hit.accession), log
              )
            except Exception as e:
              log.warning(f"8-K fetch failed for {hit.accession}: {e}")
              stats["failed"] += 1
              return
        if status == 404:
          # Filed before inline XBRL reached 8-K cover pages: no zip.
          stats["no_zip"] += 1
          return
        if not zip_bytes:
          stats["failed"] += 1
          return
        stats["fetched"] += 1
        await asyncio.to_thread(
          writer.upload_bytes,
          zip_bytes,
          raw_bucket,
          raw_key,
          content_type="application/zip",
        )
      else:
        stats["from_raw"] += 1

      try:
        documents = documents_in_zip(zip_bytes, hit.primary_document)
      except Exception as e:
        log.warning(f"8-K zip unreadable for {hit.accession}: {e}")
        stats["failed"] += 1
        return

      representations: list[dict[str, Any]] = []
      errors: list[str] = []
      for document in documents:
        key = get_filing_artifact_key(year, hit.cik, hit.accession, document.name)
        rep = await asyncio.to_thread(
          _publish_document,
          writer,
          public_bucket,
          cdn_url,
          key,
          document.name,
          document.data,
        )
        if rep is None:
          errors.append(f"{document.name}: upload failed")
          continue
        rep = {"kind": document.kind, **rep}
        if document.exhibit:
          rep["exhibit"] = document.exhibit
        representations.append(rep)

      folder = get_public_data_url(
        public_bucket,
        get_filing_artifact_prefix(year, hit.cik, hit.accession) + "/",
        cdn_url,
      )
      manifest = current_report_manifest(hit, representations, errors, folder)
      if await asyncio.to_thread(
        _write_manifest, writer, public_bucket, manifest_key, manifest
      ):
        stats["published"] += 1
        stats["exhibits"] += sum(1 for r in representations if r["kind"] == "exhibit")
        published.append((hit, representations))
      else:
        stats["failed"] += 1

    await _drain([one(hit) for hit in hits], stats, f"{partition} 8-Ks", log)

  return stats, published


def _update_release_lists(
  hits: list[tuple[Any, list[dict[str, Any]] | None]],
  corpus: set[str] | None,
  log: Any,
) -> Counter:
  """Merge the published 8-Ks into each registrant's releases list, newest
  first — a combined filing under every registrant the corpus holds.

  Whole-file rewrites, one per filer whose list changes: an intraday run
  finds mostly filings it has already listed, and leaves those lists alone.
  Runs that pull are serialized by the ``edgar`` run-queue limit and
  :func:`refuse_concurrent_edgar_pull`, so no two write the same list at once.
  """
  from robosystems.adapters.sec.processors.artifacts import (
    JSON_MEDIA_TYPE,
    MANIFEST_CACHE_CONTROL,
    put_public_artifact,
  )
  from robosystems.adapters.sec.processors.current_reports import merge_releases
  from robosystems.operations.aws.s3 import S3Client, gunzip_if_gzipped

  from .text_index import _get_s3_client

  s3 = _get_s3_client()
  writer = S3Client()
  bucket = env.PUBLIC_DATA_BUCKET
  cdn_url = env.PUBLIC_DATA_CDN_URL
  by_cik: dict[str, list[Any]] = {}
  for hit, representations in hits:
    for cik in hit.registrants(corpus):
      by_cik.setdefault(cik, []).append((hit, representations))

  def one(cik: str) -> str:
    key = get_current_reports_list_key(cik)
    body = _read_object(s3, bucket, key)
    existing = json.loads(gunzip_if_gzipped(body)).get("releases", []) if body else []
    releases = merge_releases(existing, by_cik[cik], bucket, cdn_url)
    if releases == existing:
      return "lists_unchanged"
    document = {"cik": cik, "releases": releases}
    written = put_public_artifact(
      writer,
      bucket,
      key,
      json.dumps(document, separators=(",", ":")).encode("utf-8"),
      JSON_MEDIA_TYPE,
      cache_control=MANIFEST_CACHE_CONTROL,
    )
    return "lists_written" if written else "lists_failed"

  stats: Counter = Counter()
  with ThreadPoolExecutor(max_workers=LIST_WORKERS) as pool:
    for outcome in pool.map(one, sorted(by_cik)):
      stats[outcome] += 1
  if stats["lists_failed"]:
    log.warning(f"{stats['lists_failed']} releases lists failed to write")
  return stats


@asset(
  group_name="sec_pipeline",
  description="Fetch and publish 8-K earnings releases (items 2.02 / 7.01) with their exhibits",
  kinds={"download", "s3"},
  partitions_def=sec_quarter_partitions,
  backfill_policy=BackfillPolicy.single_run(),
  metadata={"pipeline": "sec", "graph_id": "sec", "stage": "current_reports"},
)
def sec_current_reports(
  context: AssetExecutionContext, config: SECCurrentReportsConfig
) -> MaterializeResult:
  """Discover each selected quarter's 8-Ks through EFTS, keep those reporting
  a wanted item from a filer in the corpus, and publish them.

  A range of quarters runs in one run, one quarter after another, so a
  backfill is one launch and never two pulls at once.
  """
  from xbrlkit.edgar import EftsClient

  from robosystems.adapters.sec.client.current_reports import (
    discover_current_reports,
  )
  from robosystems.adapters.sec.config import xbrlkit_config

  from .text_index import _get_s3_client

  refuse_concurrent_edgar_pull(context)
  partitions = _run_partitions(context)
  today = datetime.now(UTC).date()
  efts = EftsClient(xbrlkit_config(), per_sec=config.efts_rate)
  corpus = (
    _corpus_ciks(_get_s3_client(), env.SHARED_RAW_BUCKET)
    if config.in_corpus_only
    else None
  )
  if corpus is not None:
    context.log.info(f"Corpus holds {len(corpus)} filers")

  totals: Counter = Counter()
  for partition in partitions:
    window = discovery_window(partition, config.since_days, today)
    if window is None:
      context.log.info(f"{partition}: nothing to discover yet")
      continue
    seen, hits = discover_current_reports(
      efts, window[0], window[1], config.items, context.log.info
    )
    in_corpus = [h for h in hits if h.registrants(corpus)]
    totals["eight_ks_seen"] += seen
    totals["wanted"] += len(in_corpus)
    context.log.info(
      f"{partition} ({window[0]}..{window[1]}): {seen} 8-Ks, {len(hits)} report "
      f"{config.items}, {len(in_corpus)} from filers in the corpus"
    )
    if config.dry_run or not in_corpus:
      continue
    stats, published = asyncio.run(
      _capture_current_reports(in_corpus, partition, config, context.log)
    )
    stats.update(_update_release_lists(published, corpus, context.log))
    context.log.info(f"{partition}: {dict(stats)}")
    totals.update(stats)

  return MaterializeResult(
    metadata={
      "partitions": len(partitions),
      "dry_run": config.dry_run,
      **{k: int(v) for k, v in totals.items()},
    }
  )


# ── documents of pre-inline filings ────────────────────────────────────────


async def _fetch_filing_documents(
  todo: list[tuple[dict[str, Any], dict[str, Any], str]],
  config: SECFilingDocumentsConfig,
  log: Any,
) -> Counter:
  import aiohttp

  from robosystems.adapters.sec.client.current_reports import filing_file_url
  from robosystems.adapters.sec.client.rate_limiter import AsyncRateLimiter
  from robosystems.adapters.sec.config import SEC_CONFIG
  from robosystems.operations.aws.s3 import S3Client

  writer = S3Client()
  public_bucket = env.PUBLIC_DATA_BUCKET
  cdn_url = env.PUBLIC_DATA_CDN_URL
  limiter = AsyncRateLimiter(rate=config.download_rate)
  semaphore = asyncio.Semaphore(config.download_concurrency)
  stats: Counter = Counter()

  async with aiohttp.ClientSession(headers=SEC_CONFIG["headers"]) as session:

    async def one(filing: dict[str, Any], manifest: dict[str, Any], name: str) -> None:
      cik, accession = filing["cik"], filing["accession"]
      year = filing["filing_date"][:4]
      async with semaphore:
        async with limiter:
          try:
            status, data = await _fetch(
              session, filing_file_url(cik, accession, name), log
            )
          except Exception as e:
            log.warning(f"Document fetch failed for {accession}: {e}")
            stats["failed"] += 1
            return
      if status == 404 or not data:
        stats["missing_on_edgar" if status == 404 else "failed"] += 1
        return
      key = get_filing_artifact_key(year, cik, accession, name)
      rep = await asyncio.to_thread(
        _publish_document, writer, public_bucket, cdn_url, key, name, data
      )
      if rep is None:
        stats["failed"] += 1
        return
      manifest = {
        **manifest,
        "representations": [
          *(manifest.get("representations") or []),
          {"kind": "document", **rep},
        ],
      }
      manifest_key = get_filing_artifact_key(
        year, cik, accession, FILING_ARTIFACT_MANIFEST
      )
      if await asyncio.to_thread(
        _write_manifest, writer, public_bucket, manifest_key, manifest
      ):
        stats["fetched"] += 1
      else:
        stats["failed"] += 1

    await _drain([one(*entry) for entry in todo], stats, "documents", log)

  return stats


@asset(
  group_name="sec_pipeline",
  description="Fetch the primary documents of filings processed before inline XBRL",
  kinds={"download", "s3"},
  partitions_def=sec_quarter_partitions,
  backfill_policy=BackfillPolicy.single_run(),
  metadata={"pipeline": "sec", "graph_id": "sec", "stage": "filing_documents"},
)
def sec_filing_documents(
  context: AssetExecutionContext, config: SECFilingDocumentsConfig
) -> MaterializeResult:
  """For each processed filing whose manifest names a primary document its
  folder does not hold, fetch that one file and list it in the manifest.

  The work list comes from storage (the processed Report rows and each
  filing's manifest), so EDGAR sees only the document requests themselves.
  """
  from robosystems.adapters.sec.processors.current_reports import missing_document

  from .catalog import filings_by_cik, read_corpus, read_manifests
  from .text_index import _get_s3_client

  refuse_concurrent_edgar_pull(context)
  s3 = _get_s3_client()
  partitions = _run_partitions(context)
  totals: Counter = Counter()
  for partition in partitions:
    reports, entities, relationships = read_corpus(
      s3, env.SHARED_PROCESSED_BUCKET, [partition]
    )
    by_cik = filings_by_cik(reports, relationships, entities, config.form_types)
    filings = [f for fs in by_cik.values() for f in fs if f.get("filing_date")]
    manifests = read_manifests(
      s3, env.PUBLIC_DATA_BUCKET, filings, config.manifest_workers
    )
    todo = []
    for filing in filings:
      manifest = manifests.get(filing["accession"])
      name = missing_document(manifest)
      if manifest and name:
        todo.append((filing, manifest, name))
    totals["filings"] += len(filings)
    totals["missing"] += len(todo)
    context.log.info(
      f"{partition}: {len(filings)} filings, {len(todo)} without their document"
    )
    del reports, entities, relationships, manifests
    gc.collect()
    if config.dry_run or not todo:
      continue
    stats = asyncio.run(_fetch_filing_documents(todo, config, context.log))
    context.log.info(f"{partition}: {dict(stats)}")
    totals.update(stats)

  return MaterializeResult(
    metadata={
      "partitions": len(partitions),
      "dry_run": config.dry_run,
      **{k: int(v) for k, v in totals.items()},
    }
  )
