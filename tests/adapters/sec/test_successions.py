"""The curated table of registrants that took a ticker over from another."""

import re

import pytest

from robosystems.adapters.sec.successions import (
  REASSIGNMENTS,
  SUCCESSIONS,
  Succession,
  predecessors_of,
  recorded_pairs,
)

pytestmark = pytest.mark.unit

_CIK = re.compile(r"^\d{10}$")
_ACCESSION = re.compile(r"^\d{10}-\d{2}-\d{6}$")


def test_every_row_is_well_formed():
  for row in SUCCESSIONS:
    assert _CIK.match(row.predecessor) and _CIK.match(row.successor), row
    assert row.predecessor != row.successor, row
    assert _ACCESSION.match(row.evidence), row
    assert re.match(r"^\d{4}-\d{2}-\d{2}$", row.effective), row
    assert row.ticker == row.ticker.upper(), row
  for row in REASSIGNMENTS:
    assert _CIK.match(row.former) and _CIK.match(row.holder), row
    assert row.former != row.holder and row.basis, row
    assert row.ticker == row.ticker.upper(), row


def test_a_registrant_has_one_successor_and_one_finding():
  """A filer hands its ticker to one registrant, and a pair is either a
  succession or a reassignment, never both."""
  predecessors = [row.predecessor for row in SUCCESSIONS]
  assert len(predecessors) == len(set(predecessors))
  successions = {(r.predecessor, r.successor) for r in SUCCESSIONS}
  reassignments = {(r.former, r.holder) for r in REASSIGNMENTS}
  assert not successions & reassignments
  assert recorded_pairs() == successions | reassignments


def test_predecessors_follow_every_link_nearest_first():
  chain = (
    Succession("T", "0000000001", "0000000002", "2020-01-01", "0000000002-20-000001"),
    Succession("T", "0000000002", "0000000003", "2024-01-01", "0000000003-24-000001"),
  )
  assert predecessors_of("0000000003", chain) == ["0000000002", "0000000001"]
  assert predecessors_of("0000000002", chain) == ["0000000001"]
  assert predecessors_of("0000000001", chain) == []


def test_a_loop_in_the_table_ends():
  loop = (
    Succession("T", "0000000001", "0000000002", "2020-01-01", "0000000002-20-000001"),
    Succession("T", "0000000002", "0000000001", "2024-01-01", "0000000001-24-000001"),
  )
  assert predecessors_of("0000000001", loop) == ["0000000002"]
