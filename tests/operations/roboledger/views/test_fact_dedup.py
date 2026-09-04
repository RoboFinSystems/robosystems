"""Tests for the precision-aware dedup shared by the graph-backed views.

The case that motivated it: 3M FY2024 research and development expense is
reported twice in the 10-K under the same element and period — 1,085 (in
millions, ``decimals=-6``) on the income statement and 1,100
(``decimals=-8``) in the narrative. Both are consolidated facts with the
same period identity; the statement figure must be the survivor
regardless of the order the engine returns the rows.
"""

import pytest

from robosystems.operations.roboledger.views.fact_dedup import (
  UNKNOWN_PRECISION,
  keep_most_precise,
  precision_rank,
)

MMM_RD_STATEMENT = {
  "qname": "us-gaap:ResearchAndDevelopmentExpense",
  "start_date": "2024-01-01",
  "end_date": "2024-12-31",
  "value": 1_085_000_000,
  "decimals": "-6",
}
MMM_RD_NARRATIVE = {
  "qname": "us-gaap:ResearchAndDevelopmentExpense",
  "start_date": "2024-01-01",
  "end_date": "2024-12-31",
  "value": 1_100_000_000,
  "decimals": "-8",
}


def _period_key(row):
  return (row["qname"], row["start_date"], row["end_date"])


@pytest.mark.unit
class TestPrecisionRank:
  def test_millions_beats_hundred_millions(self):
    assert precision_rank("-6") > precision_rank("-8")

  def test_inf_beats_any_finite_precision(self):
    assert precision_rank("INF") > precision_rank("6")
    assert precision_rank("inf") > precision_rank("6")

  def test_integer_input_is_accepted(self):
    assert precision_rank(-6) == precision_rank("-6")

  @pytest.mark.parametrize("value", [None, "", "   ", "n/a"])
  def test_missing_or_unparseable_is_least_precise(self, value):
    assert precision_rank(value) == UNKNOWN_PRECISION
    assert precision_rank(value) < precision_rank("-8")


@pytest.mark.unit
class TestKeepMostPrecise:
  def test_statement_figure_survives_when_listed_first(self):
    rows = [MMM_RD_STATEMENT, MMM_RD_NARRATIVE]
    assert [r["value"] for r in keep_most_precise(rows, _period_key)] == [1_085_000_000]

  def test_statement_figure_survives_when_listed_second(self):
    rows = [MMM_RD_NARRATIVE, MMM_RD_STATEMENT]
    assert [r["value"] for r in keep_most_precise(rows, _period_key)] == [1_085_000_000]

  def test_stated_precision_beats_missing_precision(self):
    unknown = {**MMM_RD_NARRATIVE, "decimals": None}
    rows = [unknown, MMM_RD_STATEMENT]
    assert [r["value"] for r in keep_most_precise(rows, _period_key)] == [1_085_000_000]

  def test_equal_precision_keeps_the_first_row(self):
    first = {**MMM_RD_STATEMENT, "value": 1}
    second = {**MMM_RD_STATEMENT, "value": 2}
    assert [r["value"] for r in keep_most_precise([first, second], _period_key)] == [1]

  def test_no_decimals_column_degrades_to_first_seen(self):
    """Rows from a query that never projected decimals behave as before."""
    a = {"qname": "x", "start_date": "", "end_date": "2024-12-31", "value": 1}
    b = {"qname": "x", "start_date": "", "end_date": "2024-12-31", "value": 2}
    assert [r["value"] for r in keep_most_precise([a, b], _period_key)] == [1]

  def test_output_keeps_first_seen_key_order(self):
    """A replaced survivor takes the slot its key was first seen in, so the
    query's ORDER BY is not disturbed by which duplicate was more precise."""
    other = {
      "qname": "us-gaap:Revenues",
      "start_date": "2024-01-01",
      "end_date": "2024-12-31",
      "value": 24_575_000_000,
      "decimals": "-6",
    }
    rows = [MMM_RD_NARRATIVE, other, MMM_RD_STATEMENT]
    assert [r["value"] for r in keep_most_precise(rows, _period_key)] == [
      1_085_000_000,
      24_575_000_000,
    ]

  def test_empty_input(self):
    assert keep_most_precise([], _period_key) == []
