"""How the noncontrolling interest's roll-forward classifies a consolidation
entity's untagged lines: the reversal of a former member's elimination is
the interest leaving; the elimination of a member that joined within the
column is the interest it brought (0065); a tagged line, or one for a
member in since before the column, is read off its flow tag instead."""

from __future__ import annotations

from datetime import date
from types import SimpleNamespace

import pytest

from robosystems.operations.roboledger.reports import fact_grid

pytestmark = pytest.mark.unit

JULY = (date(2026, 7, 1), date(2026, 7, 31))


def _line(kind: str, *, tag: str | None = None, joined=None, left=None):
  return SimpleNamespace(
    adjustment_kind=kind, tag_qname=tag, adjusts_joined_on=joined, adjusts_left_on=left
  )


def test_an_untagged_elimination_for_a_member_joining_in_the_column_is_its_arrival():
  line = _line("elimination_investment", joined=date(2026, 7, 1))
  assert fact_grid._joins_the_group(line, *JULY)
  assert fact_grid._NCI_MOVEMENTS["acquisition"] == (
    "rs-gaap:NoncontrollingInterestIncreaseFromBusinessCombination"
  )


def test_a_member_formed_in_the_group_keeps_its_contribution_tag():
  tagged = _line(
    "elimination_investment",
    tag="rs-gaap:ProceedsFromMinorityShareholders",
    joined=date(2026, 7, 1),
  )
  assert not fact_grid._joins_the_group(tagged, *JULY)
  assert fact_grid._NCI_FLOW_KIND[tagged.tag_qname] == "contribution"


def test_a_member_in_since_before_the_column_did_not_arrive_in_it():
  earlier = _line("elimination_investment", joined=date(2026, 3, 1))
  since_the_start = _line("elimination_investment")
  assert not fact_grid._joins_the_group(earlier, *JULY)
  assert not fact_grid._joins_the_group(since_the_start, *JULY)


def test_only_the_investment_elimination_carries_the_arrival():
  intercompany = _line("elimination_intercompany", joined=date(2026, 7, 1))
  assert not fact_grid._joins_the_group(intercompany, *JULY)


def test_a_former_members_reversal_is_its_leaving_whatever_its_dates():
  reversal = _line(
    "elimination_investment", joined=date(2026, 7, 1), left=date(2026, 7, 31)
  )
  assert fact_grid._leaves_the_group(reversal, date(2026, 8, 1))
  assert not fact_grid._leaves_the_group(reversal, JULY[0])
  assert fact_grid._leaves_the_group(_line("deconsolidation"), JULY[0])
