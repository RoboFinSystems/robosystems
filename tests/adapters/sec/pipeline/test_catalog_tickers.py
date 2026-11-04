"""The catalog job when tickers move: renames, new listings, and a shared ticker."""

import json
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd
import pytest
from dagster import build_asset_context

from robosystems.adapters.sec.pipeline import catalog as catalog_module
from robosystems.adapters.sec.pipeline.catalog import sec_filing_catalog
from robosystems.adapters.sec.pipeline.configs import SECFilingCatalogConfig
from robosystems.adapters.sec.tickers import Listing
from robosystems.config.storage.shared import (
  FILING_CATALOG_ALIASES_KEY,
  FILING_CATALOG_INDEX_KEY,
)

RENAMED = "0000000001"
UNTOUCHED = "0000000002"
NEWLY_LISTED = "0000000003"
OLD_HOLDER = "0000000004"
NEW_HOLDER = "0000000005"
VIEWER_ROWS = (RENAMED, UNTOUCHED, NEWLY_LISTED, OLD_HOLDER, NEW_HOLDER)
PUBLIC = "public"


class _NoSuchKey(Exception):
  pass


class _Bucket:
  """An in-memory public bucket with the boto calls the job makes."""

  exceptions = SimpleNamespace(NoSuchKey=_NoSuchKey)

  def __init__(self):
    self.objects: dict[str, str] = {}
    self.writes: list[str] = []

  def get_object(self, Bucket, Key):
    if Key not in self.objects:
      raise _NoSuchKey(Key)
    body = self.objects[Key].encode()
    return {"Body": SimpleNamespace(read=lambda: body)}

  def upload_string(self, content, bucket, key, **_):
    self.objects[key] = content
    self.writes.append(key)
    return True

  def json(self, key):
    return json.loads(self.objects[key])

  def seed_index(self, *rows):
    self.objects[FILING_CATALOG_INDEX_KEY] = json.dumps({"companies": list(rows)})


def _prior(cik, ticker, renderable=True):
  return {
    "cik": cik,
    "ticker": ticker,
    "renderable": renderable,
    "latest_renderable": {"accession": f"{cik}-carried"} if renderable else None,
  }


def _corpus():
  ciks = (
    (RENAMED, "BK", "2026-Q4"),
    (UNTOUCHED, "UNT", "2026-Q3"),
    (NEWLY_LISTED, None, "2026-Q3"),
    (OLD_HOLDER, "XOM", "2026-Q3"),
    (NEW_HOLDER, "XOM", "2026-Q3"),
  )
  entities = pd.DataFrame(
    [
      {
        "identifier": f"e-{cik}",
        "cik": cik,
        "ticker": ticker,
        "name": f"FILER {cik[-1]}",
        "exchange": "NYSE",
        "sic": "1",
        "sic_description": "S",
        "partition": partition,
      }
      for cik, ticker, partition in ciks
    ]
  )
  reports = pd.DataFrame(
    [
      {
        "identifier": f"r-{cik}",
        "accession_number": f"{cik}-26-000001",
        "form": "10-Q",
        "filing_date": "2026-08-01",
        "report_date": "2026-06-30",
        "fiscal_year_focus": 2026,
        "fiscal_period_focus": "Q2",
        "partition": partition,
      }
      for cik, _, partition in ciks
    ]
  )
  links = pd.DataFrame(
    [
      {"from": f"e-{cik}", "to": f"r-{cik}", "partition": partition}
      for cik, _, partition in ciks
    ]
  )
  return reports, entities, links


TICKER_MAP = {
  RENAMED: Listing(("BNY",), "FILER 1"),
  UNTOUCHED: Listing(("UNT",), "FILER 2"),
  NEWLY_LISTED: Listing(("NEWL",), "FILER 3"),
  NEW_HOLDER: Listing(("XOM",), "FILER 5"),
}


def _run(bucket, *, ticker_map=TICKER_MAP, corpus=None, full_rebuild=False):
  context = build_asset_context(partition_key="2026-Q4")
  with (
    patch.object(catalog_module, "_get_s3_client", return_value=bucket),
    patch.object(catalog_module, "S3Client", return_value=bucket),
    patch.object(catalog_module, "read_corpus", return_value=corpus or _corpus()),
    patch.object(catalog_module, "current_ticker_map", return_value=ticker_map),
    patch.object(catalog_module.env, "PUBLIC_DATA_BUCKET", PUBLIC),
  ):
    return sec_filing_catalog(
      context, SECFilingCatalogConfig(graph_id="sec", full_rebuild=full_rebuild)
    )


@pytest.fixture
def bucket():
  return _Bucket()


@pytest.mark.unit
class TestCatalogWhenTickersMove:
  def _seeded(self, bucket):
    bucket.seed_index(
      _prior(RENAMED, "BK"),
      _prior(UNTOUCHED, "UNT"),
      _prior(NEW_HOLDER, "XOM"),
    )
    return bucket

  def test_a_renamed_filer_moves_to_its_new_ticker(self, bucket):
    _run(self._seeded(bucket))

    index = {r["cik"]: r for r in bucket.json(FILING_CATALOG_INDEX_KEY)["companies"]}
    assert index[RENAMED]["ticker"] == "BNY"
    document = bucket.json("companies/bny.json")
    assert document["cik"] == RENAMED
    assert document["former_tickers"] == ["BK"]
    assert "moved_to" not in document

  def test_the_old_ticker_stays_a_whole_document_that_says_where_it_went(self, bucket):
    _run(self._seeded(bucket))

    old = bucket.json("companies/bk.json")
    assert old["moved_to"] == "BNY"
    assert old["ticker"] == "BNY"
    assert old["cik"] == RENAMED
    assert [f["accession"] for f in old["filings"]] == [f"{RENAMED}-26-000001"]

  def test_the_alias_is_published(self, bucket):
    _run(self._seeded(bucket))

    aliases = bucket.json(FILING_CATALOG_ALIASES_KEY)["aliases"]
    assert aliases["BK"]["cik"] == RENAMED
    assert aliases["BK"]["ticker"] == "BNY"

  def test_a_rename_rewrites_a_filer_that_did_not_file_this_partition(self, bucket):
    bucket.seed_index(_prior(UNTOUCHED, "OLD"), _prior(RENAMED, "BNY"))

    _run(bucket)

    assert bucket.json("companies/unt.json")["cik"] == UNTOUCHED
    assert bucket.json("companies/old.json")["moved_to"] == "UNT"

  def test_a_new_listing_gets_a_file_whatever_partition_it_filed_in(self, bucket):
    _run(self._seeded(bucket))

    assert bucket.json("companies/newl.json")["cik"] == NEWLY_LISTED

  def test_an_untouched_filer_carries_its_renderability_forward(self, bucket):
    _run(self._seeded(bucket))

    assert "companies/unt.json" not in bucket.writes
    index = {r["cik"]: r for r in bucket.json(FILING_CATALOG_INDEX_KEY)["companies"]}
    assert index[UNTOUCHED]["renderable"] is True
    assert index[UNTOUCHED]["latest_renderable"]["accession"] == f"{UNTOUCHED}-carried"

  def test_a_second_run_changes_no_ticker_and_rewrites_only_the_partition(self, bucket):
    _run(self._seeded(bucket))
    first_aliases = bucket.json(FILING_CATALOG_ALIASES_KEY)["aliases"]
    bucket.writes.clear()

    _run(bucket)

    assert bucket.json(FILING_CATALOG_ALIASES_KEY)["aliases"] == first_aliases
    assert "companies/newl.json" not in bucket.writes
    # The renamed filer files in this partition, so both its files stay current.
    assert {"companies/bny.json", "companies/bk.json"} <= set(bucket.writes)

  def test_a_shared_ticker_is_not_taken_from_its_old_holder(self, bucket):
    _run(self._seeded(bucket))

    index = {r["cik"]: r for r in bucket.json(FILING_CATALOG_INDEX_KEY)["companies"]}
    assert index[NEW_HOLDER]["ticker"] == "XOM"
    assert "XOM" not in bucket.json(FILING_CATALOG_ALIASES_KEY)["aliases"]

  def test_no_map_keeps_the_published_tickers_rather_than_the_rows(self, bucket):
    """The rows still say BK. Going back to them would undo the rename and
    publish an alias pointing the wrong way."""
    _run(self._seeded(bucket))
    bucket.writes.clear()

    _run(bucket, ticker_map=None)

    index = {r["cik"]: r for r in bucket.json(FILING_CATALOG_INDEX_KEY)["companies"]}
    assert index[RENAMED]["ticker"] == "BNY"
    aliases = bucket.json(FILING_CATALOG_ALIASES_KEY)["aliases"]
    assert aliases["BK"]["ticker"] == "BNY"
    assert "BNY" not in aliases
    assert "moved_to" not in bucket.json("companies/bny.json")

  def test_a_full_rebuild_replaces_a_corrupt_index(self, bucket):
    bucket.objects[FILING_CATALOG_INDEX_KEY] = "not json"

    result = _run(bucket, full_rebuild=True)

    assert result.metadata["index_written"] is True
    index = {r["cik"]: r for r in bucket.json(FILING_CATALOG_INDEX_KEY)["companies"]}
    assert index[RENAMED]["ticker"] == "BNY"

  def test_a_full_rebuild_replaces_corrupt_aliases(self, bucket):
    self._seeded(bucket)
    bucket.objects[FILING_CATALOG_ALIASES_KEY] = "not json"

    result = _run(bucket, full_rebuild=True)

    assert result.metadata["index_written"] is True
    assert isinstance(bucket.json(FILING_CATALOG_ALIASES_KEY)["aliases"], dict)

  def test_an_ordinary_run_on_a_corrupt_index_fails_and_writes_nothing(self, bucket):
    bucket.objects[FILING_CATALOG_INDEX_KEY] = "not json"

    with pytest.raises(ValueError):
      _run(bucket)
    assert bucket.writes == []

  def test_a_filer_leaving_the_index_is_reported(self, bucket):
    bucket.seed_index(_prior("0000000099", "GONE"), _prior(UNTOUCHED, "UNT"))

    result = _run(bucket)

    assert result.metadata["filers_dropped"] == 1

  def test_an_index_that_cannot_be_read_fails_the_run(self, bucket):
    def fail(**_):
      raise RuntimeError("AccessDenied")

    bucket.get_object = fail

    with pytest.raises(RuntimeError):
      _run(bucket)
    assert bucket.writes == []

  def test_a_failed_alias_write_leaves_the_index_for_the_next_run(self, bucket):
    self._seeded(bucket)
    before = bucket.objects[FILING_CATALOG_INDEX_KEY]
    original = bucket.upload_string

    def upload(content, target, key, **kwargs):
      if key == FILING_CATALOG_ALIASES_KEY:
        return False
      return original(content, target, key, **kwargs)

    bucket.upload_string = upload

    result = _run(bucket)

    assert bucket.objects[FILING_CATALOG_INDEX_KEY] == before
    assert result.metadata["status"] == "partial"
