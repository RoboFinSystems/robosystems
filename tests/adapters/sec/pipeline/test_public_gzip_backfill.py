"""The one-off public-artifact rewrite: what it touches, and what it never does."""

import gzip
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError
from dagster import Failure, build_op_context

from robosystems.adapters.sec.pipeline.configs import SECPublicGzipBackfillConfig
from robosystems.adapters.sec.pipeline.public_gzip_backfill import (
  compress_one,
  find_orphan_narratives,
  is_target,
  public_gzip_backfill,
  restore_one,
  rewrite_prefix,
  sweep_prefix,
)
from robosystems.adapters.sec.processors.artifacts import gzip_artifact

FOLDER = "2026/0000008670/0000008670-26-000030"
HOLON = f"{FOLDER}/holon.jsonld"
RAW = b'{"@context": "https://example.com"}' * 50
WRITTEN = datetime(2026, 8, 6, tzinfo=UTC)
MODULE = "robosystems.adapters.sec.pipeline.public_gzip_backfill"


class FakeBucket:
  """An S3 client over a dict: enough of get/put/list/delete to run the job."""

  def __init__(self):
    self.objects: dict[str, dict] = {}
    self.puts: list[dict] = []
    self.deleted: list[str] = []
    self.delete_errors: set[str] = set()

  def add(self, key, body=RAW, modified=WRITTEN, **headers):
    self.objects[key] = {
      "body": body,
      "etag": f'"{len(self.puts)}-{len(body)}"',
      "modified": modified,
      "headers": headers,
    }

  def get_object(self, Bucket, Key):
    obj = self.objects[Key]
    return {"Body": _Body(obj["body"]), "ETag": obj["etag"], **obj["headers"]}

  def put_object(self, **args):
    obj = self.objects[args["Key"]]
    if args["IfMatch"] != obj["etag"]:
      raise ClientError({"Error": {"Code": "PreconditionFailed"}}, "PutObject")
    self.puts.append(args)
    headers = {
      name: args[name]
      for name in ("ContentType", "CacheControl", "ContentEncoding", "StorageClass")
      if name in args
    }
    self.add(args["Key"], args["Body"], obj["modified"], **headers)

  def get_paginator(self, _operation):
    paginator = MagicMock()
    paginator.paginate.side_effect = lambda Bucket, Prefix: [
      {
        "Contents": [
          {"Key": key, "Size": len(obj["body"]), "LastModified": obj["modified"]}
          for key, obj in sorted(self.objects.items())
          if key.startswith(Prefix)
        ]
      }
    ]
    return paginator

  def delete_objects(self, Bucket, Delete):
    errors = []
    for entry in Delete["Objects"]:
      if entry["Key"] in self.delete_errors:
        errors.append({"Key": entry["Key"], "Code": "InternalError"})
        continue
      self.deleted.append(entry["Key"])
      del self.objects[entry["Key"]]
    return {"Errors": errors}


class _Body:
  def __init__(self, data):
    self.data = data
    self.was_read = False

  def read(self):
    self.was_read = True
    return self.data

  def close(self):
    pass


@pytest.fixture
def s3():
  return FakeBucket()


@pytest.mark.unit
class TestIsTarget:
  @pytest.mark.parametrize(
    "name",
    [
      "holon.jsonld",
      "tavi.json",
      "adp-20260630.htm",
      "d10k.HTML",
      "filing.txt",
      "r.xml",
    ],
  )
  def test_the_representations_are_targets(self, name):
    assert is_target(f"{FOLDER}/{name}")

  @pytest.mark.parametrize(
    "key",
    [
      # The catalog reads the manifest with a client that does not decode.
      f"{FOLDER}/manifest.json",
      f"{FOLDER}/tavi.gaps.json",
      f"{FOLDER}/fact_6bb004681d86.html",
      f"{FOLDER}/fact_6bb004681d86.txt",
      f"{FOLDER}/narrative_item_1.txt",
      f"{FOLDER}/narrative_item_1_part1.txt",
      f"{FOLDER}/report.pdf",
      f"{FOLDER}/nested/holon.jsonld",
      "companies/adp.json",
      "companies/index.json",
      "robots.txt",
      "2026/holon.jsonld",
    ],
  )
  def test_nothing_else_is(self, key):
    assert not is_target(key)


@pytest.mark.unit
class TestCompressOne:
  def test_rewrites_in_place_with_the_headers_it_read(self, s3):
    s3.add(
      HOLON, ContentType="application/ld+json", CacheControl="public, max-age=86400"
    )

    outcome = compress_one(s3, "public", HOLON, dry_run=False)

    assert outcome.status == "rewritten"
    assert outcome.bytes_before == len(RAW)
    assert outcome.bytes_after < len(RAW)
    (put,) = s3.puts
    assert put["Key"] == HOLON
    assert put["Body"] == gzip_artifact(RAW)
    assert put["ContentEncoding"] == "gzip"
    assert put["ContentType"] == "application/ld+json"
    assert put["CacheControl"] == "public, max-age=86400"
    assert put["StorageClass"] == "INTELLIGENT_TIERING"
    assert put["IfMatch"] == '"0-1750"'

  def test_stores_the_bytes_the_writer_would(self, s3):
    s3.add(HOLON)

    compress_one(s3, "public", HOLON, dry_run=False)

    assert s3.objects[HOLON]["body"] == gzip_artifact(RAW)
    assert gzip.decompress(s3.objects[HOLON]["body"]) == RAW

  def test_an_object_already_gzipped_is_skipped_unread(self, s3):
    s3.add(HOLON, gzip_artifact(RAW), ContentEncoding="gzip")
    body = _Body(gzip_artifact(RAW))
    s3.get_object = lambda Bucket, Key: {
      "Body": body,
      "ETag": '"e"',
      "ContentEncoding": "gzip",
    }

    assert compress_one(s3, "public", HOLON, dry_run=False).status == "skipped"
    assert not body.was_read
    assert s3.puts == []

  def test_gzip_bytes_are_never_compressed_twice(self, s3):
    s3.add(HOLON, gzip_artifact(RAW))

    assert compress_one(s3, "public", HOLON, dry_run=False).status == "skipped"
    assert s3.puts == []

  def test_an_object_rewritten_since_it_was_read_is_left_alone(self, s3):
    s3.add(HOLON)
    read = s3.get_object("public", HOLON)
    s3.get_object = lambda Bucket, Key: {**read, "ETag": '"stale"'}

    assert compress_one(s3, "public", HOLON, dry_run=False).status == "changed"
    assert s3.objects[HOLON]["body"] == RAW

  def test_any_other_put_failure_raises(self, s3):
    s3.add(HOLON)
    s3.put_object = MagicMock(
      side_effect=ClientError({"Error": {"Code": "AccessDenied"}}, "PutObject")
    )

    with pytest.raises(ClientError):
      compress_one(s3, "public", HOLON, dry_run=False)

  def test_a_dry_run_counts_and_writes_nothing(self, s3):
    s3.add(HOLON)

    outcome = compress_one(s3, "public", HOLON, dry_run=True)

    assert outcome.status == "rewritten"
    assert outcome.bytes_after < outcome.bytes_before
    assert s3.puts == []


@pytest.mark.unit
class TestRestoreOne:
  def test_restore_is_the_inverse_of_compress(self, s3):
    s3.add(
      HOLON, ContentType="application/ld+json", CacheControl="public, max-age=86400"
    )

    compress_one(s3, "public", HOLON, dry_run=False)
    outcome = restore_one(s3, "public", HOLON, dry_run=False)

    assert outcome.status == "rewritten"
    assert s3.objects[HOLON]["body"] == RAW
    restored = s3.puts[-1]
    assert "ContentEncoding" not in restored
    assert restored["ContentType"] == "application/ld+json"
    assert restored["CacheControl"] == "public, max-age=86400"

  def test_a_plain_object_is_skipped(self, s3):
    s3.add(HOLON)

    assert restore_one(s3, "public", HOLON, dry_run=False).status == "skipped"
    assert s3.puts == []


@pytest.mark.unit
class TestRewritePrefix:
  def test_only_the_artifacts_under_the_prefix_are_rewritten(self, s3):
    for name in (
      "holon.jsonld",
      "tavi.json",
      "adp.htm",
      "manifest.json",
      "fact_1.html",
    ):
      s3.add(f"{FOLDER}/{name}")
    s3.add("2025/0000008670/0000008670-25-000007/holon.jsonld")
    s3.add("companies/adp.json")

    counts = rewrite_prefix(s3, "public", "2026/", "compress", 4, False, MagicMock())

    assert counts["seen"] == counts["rewritten"] == 3
    assert {put["Key"] for put in s3.puts} == {
      f"{FOLDER}/holon.jsonld",
      f"{FOLDER}/tavi.json",
      f"{FOLDER}/adp.htm",
    }
    assert counts["bytes_after"] < counts["bytes_before"]

  def test_a_second_run_skips_everything(self, s3):
    s3.add(HOLON)
    rewrite_prefix(s3, "public", "2026/", "compress", 2, False, MagicMock())

    counts = rewrite_prefix(s3, "public", "2026/", "compress", 2, False, MagicMock())

    assert counts["skipped"] == 1
    assert counts["rewritten"] == 0
    assert len(s3.puts) == 1

  def test_one_failed_object_does_not_stop_the_rest(self, s3):
    s3.add(HOLON)
    s3.add(f"{FOLDER}/tavi.json")
    get_object = s3.get_object

    def flaky(Bucket, Key):
      if Key == HOLON:
        raise RuntimeError("reset")
      return get_object(Bucket, Key)

    s3.get_object = flaky
    log = MagicMock()

    counts = rewrite_prefix(s3, "public", "2026/", "compress", 2, False, log)

    assert counts["failed"] == 1
    assert counts["rewritten"] == 1
    log.error.assert_called_once()


def _narrative(name, modified):
  return {"Key": f"{FOLDER}/{name}", "Size": 100, "LastModified": modified}


@pytest.mark.unit
class TestFindOrphanNarratives:
  SPLIT = WRITTEN + timedelta(days=41)

  def test_an_unsplit_narrative_older_than_its_parts_is_an_orphan(self):
    folder = [
      _narrative("narrative_item_1.txt", WRITTEN),
      _narrative("narrative_item_1_part1.txt", self.SPLIT),
      _narrative("narrative_item_1_part2.txt", self.SPLIT),
    ]

    assert [o["Key"] for o in find_orphan_narratives(folder)] == [
      f"{FOLDER}/narrative_item_1.txt"
    ]

  def test_an_unsplit_narrative_with_no_parts_is_live(self):
    folder = [
      _narrative("narrative_item_1c.txt", WRITTEN),
      _narrative("narrative_item_1_part1.txt", self.SPLIT),
    ]

    assert find_orphan_narratives(folder) == []

  def test_an_unsplit_narrative_newer_than_the_parts_is_live(self):
    folder = [
      _narrative("narrative_item_7.txt", self.SPLIT),
      _narrative("narrative_item_7_part1.txt", WRITTEN),
    ]

    assert find_orphan_narratives(folder) == []

  def test_written_in_the_same_second_is_not_older(self):
    folder = [
      _narrative("narrative_item_7.txt", WRITTEN),
      _narrative("narrative_item_7_part1.txt", WRITTEN),
    ]

    assert find_orphan_narratives(folder) == []

  def test_a_part_is_never_a_candidate(self):
    # Even one whose own name, read as a section, has a part 1 beside it.
    folder = [
      _narrative("narrative_item_1_part1.txt", WRITTEN),
      _narrative("narrative_item_1_part1_part1.txt", self.SPLIT),
      _narrative("narrative_item_1_part2.txt", WRITTEN),
    ]

    assert find_orphan_narratives(folder) == []

  def test_a_later_part_alone_does_not_make_an_orphan(self):
    folder = [
      _narrative("narrative_item_1.txt", WRITTEN),
      _narrative("narrative_item_1_part2.txt", self.SPLIT),
    ]

    assert find_orphan_narratives(folder) == []

  def test_only_narratives_are_considered(self):
    folder = [
      _narrative("fact_abc.txt", WRITTEN),
      _narrative("fact_abc_part1.txt", self.SPLIT),
      _narrative("holon.jsonld", WRITTEN),
    ]

    assert find_orphan_narratives(folder) == []


@pytest.mark.unit
class TestSweepPrefix:
  SPLIT = WRITTEN + timedelta(days=41)

  def _seed(self, s3):
    s3.add(f"{FOLDER}/narrative_item_1.txt", b"old", WRITTEN)
    s3.add(f"{FOLDER}/narrative_item_1_part1.txt", b"new", self.SPLIT)
    s3.add(f"{FOLDER}/narrative_item_2.txt", b"live", self.SPLIT)
    s3.add(f"{FOLDER}/holon.jsonld")
    # Another filing's part 1 never makes this filing's narrative an orphan.
    other = "2026/0000008670/0000008670-26-000012"
    s3.add(f"{other}/narrative_item_7.txt", b"live", WRITTEN)
    s3.add(f"{FOLDER}/narrative_item_7_part1.txt", b"new", self.SPLIT)

  def test_only_the_orphans_are_deleted(self, s3):
    self._seed(s3)

    counts = sweep_prefix(s3, "public", "2026/", False, MagicMock())

    assert s3.deleted == [f"{FOLDER}/narrative_item_1.txt"]
    assert counts["folders"] == 2
    assert counts["orphans"] == counts["deleted"] == 1
    assert counts["bytes"] == 3

  def test_a_dry_run_deletes_nothing(self, s3):
    self._seed(s3)
    before = set(s3.objects)

    counts = sweep_prefix(s3, "public", "2026/", True, MagicMock())

    assert counts["orphans"] == 1
    assert counts["deleted"] == 0
    assert set(s3.objects) == before

  def test_a_delete_s3_refuses_is_counted_as_failed(self, s3):
    self._seed(s3)
    s3.delete_errors = {f"{FOLDER}/narrative_item_1.txt"}

    counts = sweep_prefix(s3, "public", "2026/", False, MagicMock())

    assert counts["failed"] == 1
    assert counts["deleted"] == 0


@pytest.mark.unit
class TestOp:
  def _run(self, s3, **config):
    with patch(f"{MODULE}._client", return_value=s3), patch(f"{MODULE}.env") as env:
      env.PUBLIC_DATA_BUCKET = "public"
      return public_gzip_backfill(
        build_op_context(), SECPublicGzipBackfillConfig(**config)
      )

  @pytest.mark.parametrize("prefix", ["", "companies/", "/2026/", "robots.txt", "26/"])
  def test_a_prefix_outside_a_filing_year_is_refused_before_any_request(
    self, s3, prefix
  ):
    s3.get_paginator = MagicMock()

    with pytest.raises(Failure):
      self._run(s3, prefixes=["2026/", prefix])

    s3.get_paginator.assert_not_called()

  def test_no_prefixes_is_refused(self, s3):
    with pytest.raises(Failure):
      self._run(s3, prefixes=[])

  def test_totals_span_the_prefixes(self, s3):
    s3.add(HOLON)
    s3.add("2025/0000008670/0000008670-25-000007/tavi.json")

    result = self._run(s3, prefixes=["2025/", f"{FOLDER}/"])

    assert result["mode"] == "compress"
    assert result["rewritten"] == 2
    assert len(s3.puts) == 2

  def test_the_default_mode_never_deletes(self, s3):
    s3.add(f"{FOLDER}/narrative_item_1.txt", b"old", WRITTEN)
    s3.add(f"{FOLDER}/narrative_item_1_part1.txt", b"new", WRITTEN + timedelta(days=1))

    self._run(s3, prefixes=["2026/"])

    assert s3.deleted == []

  def test_a_failed_object_fails_the_run(self, s3):
    s3.add(HOLON)
    s3.get_object = MagicMock(side_effect=RuntimeError("reset"))

    with pytest.raises(Failure, match="1 objects failed"):
      self._run(s3, prefixes=["2026/"])
