"""One-off: gzip the public filing artifacts written before the writer did.

The artifact writer stores a filing's holon, Tavi model and filed document
gzipped (``processors/artifacts.py``). Everything it wrote before that sits in
the public bucket uncompressed, and a reprocess is a needlessly expensive way
to rewrite bytes that are already there. This job rewrites them in place: same
key, same headers, plus ``Content-Encoding: gzip``, compressed by the writer's
own function so a later reprocess produces the same bytes.

It runs in the bucket's region — in-region transfer is free, and the same pass
from outside AWS would cost more in egress than the reprocess it replaces.

The public bucket is unversioned, so every write here is final. Three things
keep that safe: a rewrite is conditional on the ETag it read (an object the
nightly rewrote in between is left alone), the compressed bytes are decoded and
compared before they replace anything, and ``restore`` is the inverse of
``compress``. The narrative sweep deletes, so it is its own mode with its own
dry run, and it removes an unsplit narrative only when the ``_part1`` that
superseded it is in the same folder and is the newer of the two.
"""

import gzip
import re
from collections import Counter
from collections.abc import Iterable, Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from itertools import batched, groupby
from typing import Any

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError
from dagster import Failure, OpExecutionContext, op

from robosystems.adapters.sec.processors.artifacts import (
  GZIP_DOCUMENT_EXTENSIONS,
  GZIP_ENCODING,
  gzip_artifact,
)
from robosystems.config import env
from robosystems.config.storage.shared import (
  FILING_ARTIFACT_HOLON,
  FILING_ARTIFACT_TAVI,
  PUBLIC_DATA_STORAGE_CLASS,
)
from robosystems.operations.aws.s3 import GZIP_MAGIC

from .configs import SECPublicGzipBackfillConfig

# A year, or anything beneath one. Keeps a run out of ``companies/`` and off
# the bucket root.
PREFIX_PATTERN = re.compile(r"^\d{4}/")
# ``{year}/{cik}/{accession}/{name}`` — a file directly in a filing's folder.
FILING_KEY_PATTERN = re.compile(r"^\d{4}/[^/]+/[^/]+/([^/]+)$")
NARRATIVE_PART_PATTERN = re.compile(r"_part\d+\.txt$")

# The text blocks and the narrative extracts share the folder and two of the
# document extensions; neither is an artifact.
NON_ARTIFACT_PREFIXES = ("fact_", "narrative_")
COPIED_HEADERS = (
  "ContentType",
  "CacheControl",
  "ContentDisposition",
  "ContentLanguage",
)
# S3's answers to a conditional PUT whose object changed underneath it.
CHANGED_ERROR_CODES = {"PreconditionFailed", "ConditionalRequestConflict"}

REWRITE_BATCH = 2000
DELETE_BATCH = 1000
LOGGED_ORPHANS = 50


@dataclass(frozen=True)
class Outcome:
  """What happened to one object: rewritten, skipped, changed or failed."""

  status: str
  bytes_before: int = 0
  bytes_after: int = 0


def is_target(key: str) -> bool:
  """Whether ``key`` is a holon, a Tavi model or a filed document.

  By inclusion, so the manifest — which the catalog reads with a client that
  does not decode — and everything else in the folder can never match.
  """
  match = FILING_KEY_PATTERN.match(key)
  if not match:
    return False
  name = match.group(1)
  if name in (FILING_ARTIFACT_HOLON, FILING_ARTIFACT_TAVI):
    return True
  if name.startswith(NON_ARTIFACT_PREFIXES):
    return False
  return name.lower().endswith(GZIP_DOCUMENT_EXTENSIONS)


def _replace(
  s3: Any,
  bucket: str,
  key: str,
  body: bytes,
  read: dict[str, Any],
  encoding: str | None,
) -> bool:
  """PUT ``body`` over ``key`` with the headers it was read with.

  False when the object changed since it was read.
  """
  args: dict[str, Any] = {
    "Bucket": bucket,
    "Key": key,
    "Body": body,
    "StorageClass": PUBLIC_DATA_STORAGE_CLASS,
    "IfMatch": read["ETag"],
  }
  for header in COPIED_HEADERS:
    if read.get(header):
      args[header] = read[header]
  if read.get("Metadata"):
    args["Metadata"] = read["Metadata"]
  if encoding:
    args["ContentEncoding"] = encoding
  try:
    s3.put_object(**args)
  except ClientError as e:
    if e.response.get("Error", {}).get("Code") in CHANGED_ERROR_CODES:
      return False
    raise
  return True


def compress_one(s3: Any, bucket: str, key: str, dry_run: bool) -> Outcome:
  read = s3.get_object(Bucket=bucket, Key=key)
  if read.get("ContentEncoding") == GZIP_ENCODING:
    read["Body"].close()
    return Outcome("skipped")
  raw = read["Body"].read()
  if raw[:2] == GZIP_MAGIC:
    return Outcome("skipped")
  packed = gzip_artifact(raw)
  if gzip.decompress(packed) != raw:
    raise ValueError(f"{key}: compressed bytes do not decode to the original")
  if not dry_run and not _replace(s3, bucket, key, packed, read, GZIP_ENCODING):
    return Outcome("changed")
  return Outcome("rewritten", len(raw), len(packed))


def restore_one(s3: Any, bucket: str, key: str, dry_run: bool) -> Outcome:
  read = s3.get_object(Bucket=bucket, Key=key)
  if read.get("ContentEncoding") != GZIP_ENCODING:
    read["Body"].close()
    return Outcome("skipped")
  packed = read["Body"].read()
  raw = gzip.decompress(packed)
  if not dry_run and not _replace(s3, bucket, key, raw, read, None):
    return Outcome("changed")
  return Outcome("rewritten", len(packed), len(raw))


def find_orphan_narratives(folder: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
  """The unsplit narrative objects in one filing folder that parts superseded.

  ``narrative_{section}.txt`` is an orphan only when ``narrative_{section}_part1.txt``
  is listed beside it and was written after it. A part is never a candidate,
  and an unsplit object newer than the parts is the live one.
  """
  by_name = {obj["Key"].rpartition("/")[2]: obj for obj in folder}
  orphans = []
  for name, obj in by_name.items():
    if not name.startswith("narrative_") or not name.endswith(".txt"):
      continue
    if NARRATIVE_PART_PATTERN.search(name):
      continue
    first_part = by_name.get(f"{name.removesuffix('.txt')}_part1.txt")
    if first_part is None or obj["LastModified"] >= first_part["LastModified"]:
      continue
    orphans.append(obj)
  return orphans


def iter_objects(s3: Any, bucket: str, prefix: str) -> Iterator[dict[str, Any]]:
  """Every object under ``prefix``, in key order. Raises on a failed listing."""
  paginator = s3.get_paginator("list_objects_v2")
  for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
    yield from page.get("Contents") or []


def rewrite_prefix(
  s3: Any,
  bucket: str,
  prefix: str,
  mode: str,
  workers: int,
  dry_run: bool,
  log: Any,
) -> Counter[str]:
  """Compress or restore every artifact under ``prefix``."""
  one = compress_one if mode == "compress" else restore_one
  counts: Counter[str] = Counter()

  def attempt(key: str) -> Outcome:
    try:
      return one(s3, bucket, key, dry_run)
    except Exception as e:
      log.error(f"{key}: {e}")
      return Outcome("failed")

  targets = (
    obj["Key"] for obj in iter_objects(s3, bucket, prefix) if is_target(obj["Key"])
  )
  with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
    for batch in batched(targets, REWRITE_BATCH, strict=False):
      for outcome in pool.map(attempt, batch):
        counts["seen"] += 1
        counts[outcome.status] += 1
        counts["bytes_before"] += outcome.bytes_before
        counts["bytes_after"] += outcome.bytes_after
      log.info(f"{prefix}: {dict(counts)}")
  return counts


def sweep_prefix(
  s3: Any, bucket: str, prefix: str, dry_run: bool, log: Any
) -> Counter[str]:
  """Delete the superseded unsplit narratives under ``prefix``."""
  counts: Counter[str] = Counter()
  orphans: list[dict[str, Any]] = []
  filing_objects = (
    obj
    for obj in iter_objects(s3, bucket, prefix)
    if FILING_KEY_PATTERN.match(obj["Key"])
  )
  for _folder, objects in groupby(
    filing_objects, key=lambda o: o["Key"].rpartition("/")[0]
  ):
    counts["folders"] += 1
    orphans.extend(find_orphan_narratives(objects))

  counts["orphans"] = len(orphans)
  counts["bytes"] = sum(obj["Size"] for obj in orphans)
  for obj in orphans[:LOGGED_ORPHANS]:
    log.info(f"orphan: {obj['Key']} ({obj['Size']} bytes, {obj['LastModified']})")

  if dry_run:
    return counts
  for batch in batched(orphans, DELETE_BATCH, strict=False):
    response = s3.delete_objects(
      Bucket=bucket,
      Delete={"Objects": [{"Key": obj["Key"]} for obj in batch], "Quiet": True},
    )
    errors = response.get("Errors") or []
    for error in errors:
      log.error(f"{error.get('Key')}: {error.get('Code')} {error.get('Message')}")
    counts["failed"] += len(errors)
    counts["deleted"] += len(batch) - len(errors)
  return counts


def _client(workers: int) -> Any:
  kwargs: dict[str, Any] = {"region_name": env.AWS_REGION}
  if env.ENVIRONMENT == "dev" and env.AWS_ENDPOINT_URL:
    kwargs["endpoint_url"] = env.AWS_ENDPOINT_URL
  return boto3.client(
    "s3",
    config=BotoConfig(
      # One connection per worker: botocore's default pool of 10 would queue them.
      max_pool_connections=workers + 8,
      retries={"mode": "adaptive", "max_attempts": 8},
    ),
    **kwargs,
  )


@op
def public_gzip_backfill(
  context: OpExecutionContext,
  config: SECPublicGzipBackfillConfig,
) -> dict[str, Any]:
  """Walk each prefix in the configured mode, continuing past a failed object."""
  bucket = env.PUBLIC_DATA_BUCKET
  if not bucket:
    raise Failure(description="No public data bucket configured")
  invalid = [prefix for prefix in config.prefixes if not PREFIX_PATTERN.match(prefix)]
  if invalid or not config.prefixes:
    raise Failure(description=f"Prefixes must start with a filing year: {invalid}")

  s3 = _client(config.workers)
  totals: Counter[str] = Counter()
  for prefix in config.prefixes:
    if config.mode == "sweep_narratives":
      counts = sweep_prefix(s3, bucket, prefix, config.dry_run, context.log)
    else:
      counts = rewrite_prefix(
        s3, bucket, prefix, config.mode, config.workers, config.dry_run, context.log
      )
    context.log.info(f"{prefix} done: {dict(counts)}")
    totals.update(counts)

  result = {"mode": config.mode, "dry_run": config.dry_run, **totals}
  context.log.info(f"Public gzip backfill: {result}")
  if totals["failed"]:
    raise Failure(
      description=f"{totals['failed']} objects failed; re-run the same prefixes",
      metadata={name: int(count) for name, count in totals.items()},
    )
  return result
