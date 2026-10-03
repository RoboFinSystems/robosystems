"""EFTS 8-K discovery: items kept, windows halved under the 10k ceiling."""

from datetime import date
from urllib.parse import parse_qs

import pytest
import requests

from robosystems.adapters.sec.client.current_reports import (
  discover_current_reports,
  filing_file_url,
  filing_zip_url,
)


class _FakeEfts:
  """EFTS over a fixed list of (file_date, accession, items) filings, answering
  as EFTS does: a count past the ceiling comes back as the ceiling with
  ``relation: gte``, never the real number, and a probe's ``size`` is ignored."""

  def __init__(self, filings: list[tuple[str, str, list[str]]], ceiling: int = 10_000):
    self.filings = filings
    self.ceiling = ceiling
    self.calls = 0

  def _fetch_page(self, params: dict, offset: int = 0, size: int = 100) -> dict:
    self.calls += 1
    start, end = params["startdt"], params["enddt"]
    rows = [f for f in self.filings if start <= f[0] <= end]
    page = rows[offset : offset + max(size, 100)]
    capped = len(rows) > self.ceiling
    return {
      "hits": {
        "total": {
          "value": self.ceiling if capped else len(rows),
          "relation": "gte" if capped else "eq",
        },
        "hits": [
          {
            "_id": f"{acc}:doc.htm",
            "_source": {"ciks": ["1"], "file_date": day, "items": items},
          }
          for day, acc, items in page
        ],
      }
    }


@pytest.mark.unit
class TestDiscoverCurrentReports:
  def test_keeps_wanted_items_once_per_accession(self):
    efts = _FakeEfts(
      [
        ("2025-10-01", "0000000001-25-000001", ["2.02", "9.01"]),
        ("2025-10-02", "0000000001-25-000002", ["5.02"]),
        ("2025-10-03", "0000000001-25-000003", ["7.01"]),
        ("2025-10-03", "0000000001-25-000003", ["7.01"]),
      ]
    )
    seen, hits = discover_current_reports(
      efts, date(2025, 10, 1), date(2025, 12, 31), ["2.02", "7.01"]
    )
    assert seen == 4
    assert [h.accession for h in hits] == [
      "0000000001-25-000001",
      "0000000001-25-000003",
    ]

  def test_halves_a_window_over_the_ceiling(self, monkeypatch):
    # EFTS reports a capped window as exactly the ceiling with relation
    # "gte"; a window that is read as fitting would silently lose the rest.
    from robosystems.adapters.sec.client import current_reports as module

    monkeypatch.setattr(module, "EFTS_MAX_RESULTS", 3)
    days = ["2025-10-01", "2025-10-02", "2025-10-20", "2025-11-15", "2025-12-30"]
    efts = _FakeEfts(
      [(day, f"0000000001-25-00000{i}", ["2.02"]) for i, day in enumerate(days)],
      ceiling=3,
    )
    seen, hits = discover_current_reports(
      efts, date(2025, 10, 1), date(2025, 12, 31), ["2.02"]
    )
    assert seen == 5
    assert len(hits) == 5

  def test_a_single_day_over_the_ceiling_is_read_once_and_logged(self, monkeypatch):
    from robosystems.adapters.sec.client import current_reports as module

    monkeypatch.setattr(module, "EFTS_MAX_RESULTS", 2)
    efts = _FakeEfts(
      [("2025-10-01", f"0000000001-25-00000{i}", ["2.02"]) for i in range(4)],
      ceiling=2,
    )
    logged: list[str] = []
    seen, hits = discover_current_reports(
      efts, date(2025, 10, 1), date(2025, 10, 1), ["2.02"], logged.append
    )
    assert seen == 2 and len(hits) == 2
    assert logged and "over 2" in logged[0]

  def test_query_excludes_amendments(self):
    captured: list[dict] = []

    class _Recorder(_FakeEfts):
      def _fetch_page(self, params, offset=0, size=100):
        captured.append(params)
        return super()._fetch_page(params, offset, size)

    discover_current_reports(
      _Recorder([]), date(2025, 1, 1), date(2025, 1, 2), ["2.02"]
    )
    forms = parse_qs(f"forms={captured[0]['forms']}")["forms"][0]
    assert forms == "8-K,-8-K/A"


@pytest.mark.unit
def test_filing_urls():
  assert filing_zip_url("0000320193", "0000320193-25-000077") == (
    "https://www.sec.gov/Archives/edgar/data/320193/000032019325000077/"
    "0000320193-25-000077-xbrl.zip"
  )
  assert filing_file_url("0000320193", "0000320193-17-000070", "a10-k.htm").endswith(
    "/320193/000032019317000070/a10-k.htm"
  )


class _DroppingEfts(_FakeEfts):
  """An EFTS whose connection is dropped on the first ``drops`` requests, as a
  session left idle through a quarter's fetches finds on its next use."""

  def __init__(self, filings, drops: int):
    super().__init__(filings)
    self.drops = drops

  def _fetch_page(self, params: dict, offset: int = 0, size: int = 100) -> dict:
    if self.drops:
      self.drops -= 1
      raise requests.ConnectionError("Connection reset by peer")
    return super()._fetch_page(params, offset=offset, size=size)


@pytest.mark.unit
class TestDroppedConnection:
  FILINGS = [("2025-10-01", "0000000001-25-000001", ["2.02"])]

  @pytest.fixture(autouse=True)
  def _no_waiting(self, monkeypatch):
    from robosystems.adapters.sec.client import current_reports as module

    self.waits: list[float] = []
    monkeypatch.setattr(module.time, "sleep", self.waits.append)

  def test_a_dropped_connection_is_retried(self):
    efts = _DroppingEfts(self.FILINGS, drops=2)
    seen, hits = discover_current_reports(
      efts, date(2025, 10, 1), date(2025, 12, 31), ["2.02"]
    )
    assert (seen, len(hits)) == (1, 1)
    assert self.waits == [2.0, 4.0]

  def test_a_connection_that_stays_down_is_raised(self):
    efts = _DroppingEfts(self.FILINGS, drops=10)
    with pytest.raises(requests.ConnectionError):
      discover_current_reports(efts, date(2025, 10, 1), date(2025, 12, 31), ["2.02"])
    assert self.waits == [2.0, 4.0, 8.0]
