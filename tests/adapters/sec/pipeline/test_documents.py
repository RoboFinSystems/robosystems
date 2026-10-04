"""The document assets: 8-K capture and publish, pre-inline documents, the 8-K index."""

import asyncio
import gzip
import io
import json
import zipfile
from collections import Counter
from datetime import date
from unittest.mock import MagicMock, patch

import pytest
from dagster import build_asset_context

from robosystems.adapters.sec.pipeline import documents as module
from robosystems.adapters.sec.pipeline.configs import (
  SECCurrentReportsConfig,
  SECFilingDocumentsConfig,
)
from robosystems.adapters.sec.pipeline.documents import (
  discovery_window,
  quarter_bounds,
  quarter_of,
  refuse_concurrent_edgar_pull,
  sec_filing_documents,
)
from robosystems.adapters.sec.processors.current_reports import CurrentReportHit

COVER = (
  b"<html><body><ix:nonNumeric name='dei:DocumentType'>8-K</ix:nonNumeric>"
  b"<p>Item 2.02 Results of Operations and Financial Condition. The company "
  b"issued a press release announcing results for the quarter ended September "
  b"30, furnished as Exhibit 99.1 to this report.</p></body></html>"
)
RELEASE = (
  b"<html><body><p>Exhibit 99.1</p><p>Acme reports record fourth quarter revenue "
  b"of $10 billion, up 8 percent year over year, and raises its full year "
  b"guidance for operating margin to thirty percent.</p></body></html>"
)
HIT = CurrentReportHit(
  accession="0000320193-25-000077",
  cik="0000320193",
  filing_date="2025-10-30",
  items=("2.02", "9.01"),
  primary_document="aapl-20251030.htm",
  entity_name="Apple Inc.",
  ticker="AAPL",
)


def _zip() -> bytes:
  buf = io.BytesIO()
  with zipfile.ZipFile(buf, "w") as zf:
    zf.writestr("aapl-20251030.htm", COVER)
    zf.writestr("ex991.htm", RELEASE)
    zf.writestr("aapl-20251030.xsd", b"<schema/>")
  return buf.getvalue()


class _Writer:
  """S3Client stand-in recording what is published."""

  def __init__(self, existing: set[str] | None = None):
    self.objects: dict[str, tuple[bytes, dict]] = {}
    self.existing = existing or set()

  def object_exists(self, bucket, key):
    return key in self.existing

  def upload_bytes(self, data, bucket, key, **kwargs):
    self.objects[key] = (data, kwargs)
    return True

  def body(self, key) -> bytes:
    data, kwargs = self.objects[key]
    return gzip.decompress(data) if kwargs.get("content_encoding") == "gzip" else data


@pytest.fixture
def env():
  fake = MagicMock()
  fake.SHARED_RAW_BUCKET = "raw"
  fake.PUBLIC_DATA_BUCKET = "public"
  fake.PUBLIC_DATA_CDN_URL = "https://cdn.example.com"
  with patch.object(module, "env", fake):
    yield fake


@pytest.mark.unit
class TestWindows:
  def test_quarter_bounds(self):
    assert quarter_bounds("2025-Q4") == (date(2025, 10, 1), date(2025, 12, 31))
    assert quarter_bounds("2024-Q1") == (date(2024, 1, 1), date(2024, 3, 31))

  def test_whole_quarter_to_today(self):
    assert discovery_window("2025-Q4", None, date(2025, 11, 5)) == (
      date(2025, 10, 1),
      date(2025, 11, 5),
    )

  def test_nightly_look_back(self):
    assert discovery_window("2025-Q4", 7, date(2025, 11, 5)) == (
      date(2025, 10, 29),
      date(2025, 11, 5),
    )

  def test_the_look_back_reaches_into_the_previous_quarter(self):
    # A night missed on September 30 is caught up by the October 3 run.
    assert discovery_window("2025-Q4", 7, date(2025, 10, 3)) == (
      date(2025, 9, 26),
      date(2025, 10, 3),
    )

  def test_empty_windows(self):
    assert discovery_window("2026-Q1", None, date(2025, 11, 5)) is None
    assert discovery_window("2025-Q3", 7, date(2025, 11, 5)) is None

  def test_a_look_back_kept_inside_its_quarter(self):
    # The XBRL download files what it finds under the run's quarter, so its
    # look-back stops at the quarter's first day.
    assert discovery_window("2025-Q4", 2, date(2025, 10, 1), within_quarter=True) == (
      date(2025, 10, 1),
      date(2025, 10, 1),
    )
    assert discovery_window("2025-Q4", 2, date(2025, 11, 5), within_quarter=True) == (
      date(2025, 11, 3),
      date(2025, 11, 5),
    )
    assert (
      discovery_window("2025-Q3", 2, date(2025, 11, 5), within_quarter=True) is None
    )

  def test_quarter_of(self):
    assert quarter_of(date(2025, 9, 30)) == "2025-Q3"
    assert quarter_of(date(2025, 10, 1)) == "2025-Q4"
    assert quarter_of(date(2026, 1, 15)) == "2026-Q1"


@pytest.mark.unit
def test_corpus_ciks_from_submissions():
  s3 = MagicMock()
  s3.get_paginator.return_value.paginate.return_value = [
    {
      "Contents": [
        {"Key": "sec/submissions/0000320193.json"},
        {"Key": "sec/submissions/66740.json"},
        {"Key": "sec/submissions/readme.txt"},
      ]
    }
  ]
  assert module._corpus_ciks(s3, "raw") == {"0000320193", "0000066740"}


@pytest.mark.unit
class TestCaptureCurrentReports:
  def _run(self, writer, raw_zip, fetched=(200, b""), published=(False, None)):
    fetch = MagicMock()

    async def _fetch(session, url, log):
      fetch(url)
      return fetched

    with (
      patch("robosystems.operations.aws.s3.S3Client", return_value=writer),
      patch.object(module, "_read_object", return_value=raw_zip),
      patch.object(module, "_published_files", return_value=published),
      patch.object(module, "_fetch", _fetch),
      patch(
        "robosystems.adapters.sec.pipeline.text_index._get_s3_client",
        return_value=MagicMock(),
      ),
    ):
      stats, published = asyncio.run(
        module._capture_current_reports(
          [HIT], "2025-Q4", SECCurrentReportsConfig(), MagicMock()
        )
      )
    self.published = published
    return stats, fetch

  def test_publishes_from_raw_without_edgar(self, env):
    writer = _Writer()
    stats, fetch = self._run(writer, _zip())
    assert stats == Counter({"from_raw": 1, "published": 1, "exhibits": 1})
    [(hit, representations)] = self.published
    assert hit == HIT
    assert [r["kind"] for r in representations] == ["document", "exhibit"]
    fetch.assert_not_called()
    folder = "2025/0000320193/0000320193-25-000077"
    assert set(writer.objects) == {
      f"{folder}/aapl-20251030.htm",
      f"{folder}/ex991.htm",
      f"{folder}/manifest.json",
    }
    assert writer.body(f"{folder}/ex991.htm") == RELEASE
    manifest = json.loads(writer.body(f"{folder}/manifest.json"))
    assert manifest["form"] == "8-K"
    assert manifest["items"] == ["2.02", "9.01"]
    assert [(r["kind"], r.get("exhibit")) for r in manifest["representations"]] == [
      ("document", None),
      ("exhibit", "EX-99.1"),
    ]
    assert manifest["representations"][1]["url"] == (
      f"https://cdn.example.com/{folder}/ex991.htm"
    )

  def test_fetches_once_and_keeps_the_zip_raw_under_its_filing_quarter(self, env):
    # The run's partition is 2025-Q4 either way; the key follows the filing.
    writer = _Writer()
    stats, fetch = self._run(writer, None, fetched=(200, _zip()))
    assert stats["fetched"] == 1 and stats["published"] == 1
    fetch.assert_called_once()
    assert "-xbrl.zip" in fetch.call_args.args[0]
    raw_key = "sec/8k/filed=2025-Q4/0000320193/0000320193-25-000077.zip"
    assert writer.objects[raw_key][0] == _zip()
    assert quarter_of(date.fromisoformat(HIT.filing_date)) == "2025-Q4"

  def test_no_zip_on_edgar(self, env):
    writer = _Writer()
    stats, _fetch = self._run(writer, None, fetched=(404, b""))
    assert stats == Counter({"no_zip": 1})
    assert writer.objects == {}
    assert self.published == []

  def test_already_published_is_left_alone(self, env):
    files = [
      {"kind": "document", "name": "aapl-20251030.htm"},
      {"kind": "exhibit", "name": "ex991.htm", "exhibit": "EX-99.1"},
    ]
    writer = _Writer()
    stats, fetch = self._run(writer, _zip(), published=(True, files))
    assert stats == Counter({"already_published": 1})
    fetch.assert_not_called()
    assert writer.objects == {}
    # Still listed, with the files its manifest names: a run that published
    # the filing and ended before the list was written is repaired by this one.
    assert self.published == [(HIT, files)]

  def test_republish_does_not_ask_what_is_there(self, env):
    writer = _Writer()
    fetch = MagicMock()

    async def _fetch(session, url, log):
      fetch(url)
      return 200, b""

    with (
      patch("robosystems.operations.aws.s3.S3Client", return_value=writer),
      patch.object(module, "_read_object", return_value=_zip()),
      patch.object(module, "_published_files") as published_files,
      patch.object(module, "_fetch", _fetch),
      patch(
        "robosystems.adapters.sec.pipeline.text_index._get_s3_client",
        return_value=MagicMock(),
      ),
    ):
      stats, _published = asyncio.run(
        module._capture_current_reports(
          [HIT], "2025-Q4", SECCurrentReportsConfig(republish=True), MagicMock()
        )
      )
    published_files.assert_not_called()
    assert stats["published"] == 1


@pytest.mark.unit
class TestPublishedFiles:
  """What a filing's manifest says is already in its public folder."""

  class _S3:
    class exceptions:
      class NoSuchKey(Exception):
        pass

    def __init__(self, body=None):
      self.body = body

    def get_object(self, Bucket, Key):
      if self.body is None:
        raise self.exceptions.NoSuchKey()
      return {"Body": io.BytesIO(self.body)}

  def test_no_manifest_means_not_published(self):
    assert module._published_files(self._S3(), "public", "k") == (False, None)

  def test_a_manifest_names_the_files(self):
    files = [{"kind": "document", "name": "a.htm"}]
    body = json.dumps({"representations": files}).encode()
    assert module._published_files(self._S3(body), "public", "k") == (True, files)

  def test_an_unreadable_manifest_is_still_published(self):
    assert module._published_files(self._S3(b"not json"), "public", "k") == (
      True,
      None,
    )


@pytest.mark.unit
class TestFilingDocuments:
  FILING = {
    "cik": "0000320193",
    "accession": "0000320193-17-000070",
    "filing_date": "2017-11-03",
    "form": "10-K",
  }
  MANIFEST = {
    "accession": "0000320193-17-000070",
    "primary_document": "a10-k20179302017.htm",
    "is_inline_xbrl": False,
    "representations": [{"kind": "holon", "url": "h"}],
  }

  def test_fetches_the_document_and_lists_it(self, env):
    writer = _Writer()
    urls: list[str] = []

    async def _fetch(session, url, log):
      urls.append(url)
      return 200, b"<html>Item 7. Management's Discussion</html>"

    with (
      patch("robosystems.operations.aws.s3.S3Client", return_value=writer),
      patch.object(module, "_fetch", _fetch),
    ):
      stats = asyncio.run(
        module._fetch_filing_documents(
          [(self.FILING, self.MANIFEST, "a10-k20179302017.htm")],
          SECFilingDocumentsConfig(),
          MagicMock(),
        )
      )
    assert stats == Counter({"fetched": 1})
    assert urls == [
      "https://www.sec.gov/Archives/edgar/data/320193/000032019317000070/"
      "a10-k20179302017.htm"
    ]
    folder = "2017/0000320193/0000320193-17-000070"
    manifest = json.loads(writer.body(f"{folder}/manifest.json"))
    assert [r["kind"] for r in manifest["representations"]] == ["holon", "document"]
    assert manifest["representations"][1]["name"] == "a10-k20179302017.htm"

  def test_asset_lists_only_filings_missing_their_document(self, env):
    inline = {**self.FILING, "accession": "0000320193-21-000105"}
    inline_manifest = {**self.MANIFEST, "is_inline_xbrl": True}
    captured: list = []

    async def _fetch_docs(todo, config, log):
      captured.extend(todo)
      return Counter({"fetched": len(todo)})

    with (
      patch(
        "robosystems.adapters.sec.pipeline.catalog.read_corpus",
        return_value=(MagicMock(), MagicMock(), MagicMock()),
      ),
      patch(
        "robosystems.adapters.sec.pipeline.catalog.filings_by_cik",
        return_value={"0000320193": [self.FILING, inline]},
      ),
      patch(
        "robosystems.adapters.sec.pipeline.catalog.read_manifests",
        return_value={
          self.FILING["accession"]: self.MANIFEST,
          inline["accession"]: inline_manifest,
        },
      ),
      patch(
        "robosystems.adapters.sec.pipeline.text_index._get_s3_client",
        return_value=MagicMock(),
      ),
      patch.object(module, "_fetch_filing_documents", _fetch_docs),
    ):
      result = sec_filing_documents(
        build_asset_context(partition_key="2017-Q4"), SECFilingDocumentsConfig()
      )
    assert [entry[0]["accession"] for entry in captured] == [self.FILING["accession"]]
    assert result.metadata["missing"] == 1
    assert result.metadata["fetched"] == 1


@pytest.mark.unit
class TestEdgarPullGuard:
  def _context(self, runs):
    context = MagicMock()
    context.run_id = "self-run"
    context.instance.get_runs.return_value = runs
    return context

  def test_another_started_pull_fails_fast(self):
    from dagster import Failure

    other = MagicMock(run_id="other-run-1", job_name="sec_download")
    with pytest.raises(Failure, match="sec_download"):
      refuse_concurrent_edgar_pull(self._context([other]))

  def test_only_itself_running_is_fine(self):
    me = MagicMock(run_id="self-run", job_name="sec_current_reports_capture")
    refuse_concurrent_edgar_pull(self._context([me]))


@pytest.mark.unit
def test_release_lists_fold_a_combined_filing_under_each_registrant(env):
  combined = CurrentReportHit(
    accession="0000092122-25-000050",
    cik="0000092122",
    ciks=("0000092122", "0001000000", "0002000000"),
    filing_date="2025-07-31",
    items=("2.02",),
  )
  writer = _Writer()
  with (
    patch("robosystems.operations.aws.s3.S3Client", return_value=writer),
    patch(
      "robosystems.adapters.sec.pipeline.text_index._get_s3_client",
      return_value=MagicMock(),
    ),
    patch.object(module, "_read_object", return_value=None),
  ):
    stats = module._update_release_lists(
      [(combined, None)], {"0000092122", "0001000000"}, MagicMock()
    )
  assert stats == Counter({"lists_written": 2})
  assert set(writer.objects) == {
    "current-reports/0000092122.json",
    "current-reports/0001000000.json",
  }
  listed = json.loads(writer.body("current-reports/0001000000.json"))
  assert listed["releases"][0]["folder"].endswith(
    "/2025/0000092122/0000092122-25-000050/"
  )


@pytest.mark.unit
def test_release_list_write_failure_is_counted(env):
  writer = _Writer()
  writer.upload_bytes = lambda *a, **k: False
  with (
    patch("robosystems.operations.aws.s3.S3Client", return_value=writer),
    patch(
      "robosystems.adapters.sec.pipeline.text_index._get_s3_client",
      return_value=MagicMock(),
    ),
    patch.object(module, "_read_object", return_value=None),
  ):
    stats = module._update_release_lists([(HIT, None)], None, MagicMock())
  assert stats == Counter({"lists_failed": 1})


@pytest.mark.unit
def test_release_lists_merge_newest_first(env):
  older = {
    "accession": "0000320193-25-000050",
    "filing_date": "2025-07-31",
    "items": ["2.02"],
    "folder": "f",
  }
  writer = _Writer()
  s3 = MagicMock()
  existing = json.dumps({"cik": HIT.cik, "releases": [older]}).encode()
  with (
    patch("robosystems.operations.aws.s3.S3Client", return_value=writer),
    patch(
      "robosystems.adapters.sec.pipeline.text_index._get_s3_client", return_value=s3
    ),
    patch.object(module, "_read_object", return_value=existing),
  ):
    representations = [
      {"kind": "document", "name": "aapl-20251030.htm"},
      {"kind": "exhibit", "name": "ex991.htm", "exhibit": "EX-99.1"},
    ]
    stats = module._update_release_lists([(HIT, representations)], None, MagicMock())
  assert stats == Counter({"lists_written": 1})
  listed = json.loads(writer.body("current-reports/0000320193.json"))
  assert [r["accession"] for r in listed["releases"]] == [
    HIT.accession,
    older["accession"],
  ]
  newest = listed["releases"][0]
  assert newest["items"] == ["2.02", "9.01"]
  assert newest["folder"] == (
    "https://cdn.example.com/2025/0000320193/0000320193-25-000077/"
  )
  assert newest["document"] == "aapl-20251030.htm"
  assert newest["exhibits"] == {"EX-99.1": "ex991.htm"}


@pytest.mark.unit
def test_a_release_republished_without_its_files_keeps_their_names(env):
  from robosystems.adapters.sec.processors.current_reports import merge_releases

  first = merge_releases(
    [],
    [(HIT, [{"kind": "document", "name": "aapl-20251030.htm"}])],
    "public",
    "https://cdn.example.com",
  )
  again = merge_releases(first, [(HIT, None)], "public", "https://cdn.example.com")
  assert again[0]["document"] == "aapl-20251030.htm"
