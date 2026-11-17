"""The trial balance reads over an inverted window, and the mapping existence
check, against a real Postgres: a window that ends before it starts holds no
posting, so it answers no balance rather than an error."""

from __future__ import annotations

import pytest

from robosystems.models.extensions import Structure
from robosystems.operations.roboledger.reads.taxonomies import (
  get_mapped_trial_balance,
  mapping_exists,
)
from robosystems.operations.roboledger.reads.trial_balance import get_trial_balance
from tests.operations.roboledger.reads.test_account_rollups import (
  JUNE_END,
  JUNE_START,
  ext_session,
  seeded_mapping,
)

__all__ = ["ext_session", "seeded_mapping"]

pytestmark = pytest.mark.unit


def test_an_inverted_window_has_no_trial_balance(ext_session, seeded_mapping):
  tb = get_trial_balance(ext_session, start_date=JUNE_END, end_date=JUNE_START)

  assert all(r.total_debits == r.total_credits == 0.0 for r in tb.rows)
  assert tb.total_debits == tb.total_credits == 0.0


def test_an_inverted_window_has_no_mapped_trial_balance(ext_session, seeded_mapping):
  mapped = get_mapped_trial_balance(
    ext_session, str(seeded_mapping.id), start_date=JUNE_END, end_date=JUNE_START
  )

  assert mapped.mapping_id == str(seeded_mapping.id)
  assert all(r.total_debits == r.total_credits == 0.0 for r in mapped.rows)


def test_the_same_window_the_right_way_round_has_june(ext_session, seeded_mapping):
  """The control: the fixture's June activity is there to be missed."""
  mapped = get_mapped_trial_balance(
    ext_session, str(seeded_mapping.id), start_date=JUNE_START, end_date=JUNE_END
  )

  assert any(r.total_credits == 8_500.0 for r in mapped.rows)


def test_a_mapping_exists_only_as_a_chart_mapping(ext_session, seeded_mapping):
  other = Structure(
    name="Balance sheet",
    block_type="balance_sheet",
    taxonomy_id=seeded_mapping.taxonomy_id,
  )
  ext_session.add(other)
  ext_session.flush()

  assert mapping_exists(ext_session, str(seeded_mapping.id))
  assert not mapping_exists(ext_session, "str_missing")
  assert not mapping_exists(ext_session, str(other.id))
