"""A JournalReport Intuit cuts at its cell cap must never import as complete."""

import itertools
from datetime import date, timedelta

import pytest
import requests

from robosystems.adapters.quickbooks.pipeline.extract import fetch_journal_report
from robosystems.adapters.quickbooks.pipeline.utils import (
  JournalReportTruncatedError,
  journal_report_truncated,
  parse_journal_report,
)

NOTICE = "Unable to display more data. Please reduce the date range."


def _line(day: str, tx_id: str, account: str, dr: str = "", cr: str = "") -> dict:
  return {
    "ColData": [
      {"value": day},
      {"value": "Deposit" if day else "", "id": tx_id if day else ""},
      {"value": ""},
      {"value": ""},
      {"value": ""},
      {"value": account, "id": account},
      {"value": dr},
      {"value": cr},
    ]
  }


def _transaction(day: date) -> list[dict]:
  tx_id = day.strftime("%Y%m%d")
  return [
    _line(day.isoformat(), tx_id, "10", dr="100.00"),
    _line("", "", "20", cr="100.00"),
    {"Summary": {"ColData": [{"value": "Total"}]}},
  ]


class _CappedIntuit:
  """One transaction a day. A window with more than ``cap`` transactions is
  cut mid-transaction and ends with the notice, as Intuit does."""

  def __init__(self, cap: int) -> None:
    self.cap = cap
    self.windows: list[tuple[str, str]] = []

  def get_transactions(self, start_date: str, end_date: str) -> dict:
    self.windows.append((start_date, end_date))
    lo, hi = date.fromisoformat(start_date), date.fromisoformat(end_date)
    days = [lo + timedelta(days=n) for n in range((hi - lo).days + 1)]
    rows: list[dict] = []
    for n, day in enumerate(days):
      if n == self.cap:
        rows.append(_transaction(day)[0])
        rows.append({"ColData": [{"value": NOTICE}]})
        break
      rows.extend(_transaction(day))
    return {"Header": {"ReportName": "JournalReport"}, "Rows": {"Row": rows}}


@pytest.mark.unit
class TestTruncationSignal:
  def test_a_complete_report_is_not_truncated(self):
    report = {"Rows": {"Row": _transaction(date(2024, 1, 2)) * 3}}
    assert journal_report_truncated(report) is False

  def test_an_empty_report_is_not_truncated(self):
    assert journal_report_truncated({}) is False
    assert journal_report_truncated({"Rows": {}}) is False

  def test_the_notice_row_is_truncation(self):
    report = {
      "Rows": {
        "Row": [*_transaction(date(2024, 1, 2)), {"ColData": [{"value": NOTICE}]}]
      }
    }
    assert journal_report_truncated(report) is True

  def test_a_small_open_group_alone_is_not_truncation(self):
    """Far below the cap an open final group is an unfamiliar row, not a cut;
    only the notice or a near-cap size decides."""
    rows = [*_transaction(date(2024, 1, 2)), _line("2024-01-03", "9", "10", dr="5")]
    assert journal_report_truncated({"Rows": {"Row": rows}}) is False

  def test_the_notice_in_the_header_is_truncation(self):
    report = {
      "Header": {"Option": [{"Name": "note", "Value": NOTICE}]},
      "Rows": {"Row": []},
    }
    assert journal_report_truncated(report) is True

  def test_the_parser_alone_does_not_notice(self):
    """Why the check exists: parsing a cut report returns rows, no error."""
    report = _CappedIntuit(cap=2).get_transactions("2024-01-01", "2024-01-10")
    entries, _lines = parse_journal_report(report)
    assert len(entries) == 3


@pytest.mark.unit
class TestWindowedFetch:
  def test_a_long_history_imports_every_transaction(self):
    intuit = _CappedIntuit(cap=100)

    report = fetch_journal_report(intuit, "2021-01-01", "2023-12-31")
    entries, _lines = parse_journal_report(report)

    expected = (date(2023, 12, 31) - date(2021, 1, 1)).days + 1
    assert len(entries) == expected
    assert len({e["Id"] for e in entries}) == expected
    assert journal_report_truncated(report) is False
    assert len(intuit.windows) > 3  # year windows were split

  def test_windows_cover_the_range_without_overlap(self):
    intuit = _CappedIntuit(cap=100)

    fetch_journal_report(intuit, "2022-03-15", "2023-06-01")

    kept = sorted(
      (date.fromisoformat(a), date.fromisoformat(b))
      for a, b in intuit.windows
      if (date.fromisoformat(b) - date.fromisoformat(a)).days + 1 <= 100
    )
    assert kept[0][0] == date(2022, 3, 15)
    assert kept[-1][1] == date(2023, 6, 1)
    for (_, prev_end), (start, _) in itertools.pairwise(kept):
      assert start == prev_end + timedelta(days=1)

  def test_a_window_that_cannot_be_narrowed_enough_fails_loudly(self):
    with pytest.raises(JournalReportTruncatedError):
      fetch_journal_report(_CappedIntuit(cap=0), "2024-01-01", "2024-03-31")

  def test_a_short_incremental_window_is_one_call(self):
    intuit = _CappedIntuit(cap=100)
    fetch_journal_report(intuit, "2024-01-01", "2024-02-29")
    assert intuit.windows == [("2024-01-01", "2024-02-29")]


def _big_cut_report() -> dict:
  """Near the cell cap, cut mid-transaction, with no notice at all."""
  rows: list[dict] = []
  day = date(2020, 1, 1)
  while sum(len(r.get("ColData", [])) for r in rows) < 310_000:
    rows.extend(_transaction(day))
    day += timedelta(days=1)
  rows.append(_line(day.isoformat(), "cut", "10", dr="1"))
  return {"Rows": {"Row": rows}}


@pytest.mark.unit
class TestTruncationSignalEdges:
  def test_the_notice_in_a_summary_row_is_truncation(self):
    rows = [
      *_transaction(date(2024, 1, 2)),
      {"Summary": {"ColData": [{"value": NOTICE}]}},
    ]
    assert journal_report_truncated({"Rows": {"Row": rows}}) is True

  def test_the_notice_in_a_section_header_is_truncation(self):
    rows = [
      *_transaction(date(2024, 1, 2)),
      {"type": "Section", "Header": {"ColData": [{"value": NOTICE}]}},
    ]
    assert journal_report_truncated({"Rows": {"Row": rows}}) is True

  def test_a_small_report_ending_in_a_total_row_is_complete(self):
    """An unfamiliar final row far below the cap must not fail every sync."""
    rows = [*_transaction(date(2024, 1, 2)), _line("", "", "TOTAL", dr="100", cr="100")]
    report = {"Rows": {"Row": rows}}
    assert journal_report_truncated(report) is False

  def test_an_open_group_near_the_cap_is_truncation(self):
    assert journal_report_truncated(_big_cut_report()) is True


class _SlowIntuit(_CappedIntuit):
  """Times out on any window longer than ``slow_over`` days."""

  def __init__(self, slow_over: int) -> None:
    super().__init__(cap=10_000)
    self.slow_over = slow_over

  def get_transactions(self, start_date: str, end_date: str) -> dict:
    span = (date.fromisoformat(end_date) - date.fromisoformat(start_date)).days + 1
    if span > self.slow_over:
      self.windows.append((start_date, end_date))
      raise requests.exceptions.ReadTimeout("report still generating")
    return super().get_transactions(start_date, end_date)


@pytest.mark.unit
class TestSlowReports:
  def test_a_report_too_slow_to_generate_is_split(self):
    report = fetch_journal_report(_SlowIntuit(slow_over=60), "2023-01-01", "2023-12-31")
    entries, _lines = parse_journal_report(report)
    assert len(entries) == 365

  def test_a_week_too_slow_to_generate_raises(self):
    with pytest.raises(requests.exceptions.ReadTimeout):
      fetch_journal_report(_SlowIntuit(slow_over=0), "2023-01-01", "2023-01-31")
