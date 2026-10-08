"""Under a shadow connection the ledger side of an account-scope
reconciliation is what has landed: no draft will ever post, so none is
counted as awaiting the close."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from robosystems.operations.roboledger.reconciliations.engine import (
  _compute_account_scope,
  reconciliation_window,
)
from robosystems.operations.roboledger.reconciliations.resolvers import (
  IndependentSide,
)

MODULE = "robosystems.operations.roboledger.reconciliations.engine"


def _side():
  return IndependentSide(
    method="schedule_register",
    source="schedules",
    balances={},
    covered_element_ids=frozenset({"el_1"}),
    scope="account",
  )


def _session():
  session = MagicMock()
  session.execute.return_value.scalars.return_value = []
  return session


@pytest.mark.unit
class TestShadowWindow:
  def test_the_window_carries_shadow(self):
    window = reconciliation_window("2026-03", 1, "ent_p", shadow=True)
    assert window.shadow is True and window.period_end == date(2026, 3, 31)
    assert reconciliation_window("2026-03", 1, "ent_p").shadow is False

  def test_a_shadow_window_counts_nothing_as_awaiting_the_close(self):
    with (
      patch(f"{MODULE}.get_net_balances_cents", return_value={}),
      patch(f"{MODULE}._draft_balances") as drafts,
      patch(f"{MODULE}._undrafted_schedule_balances") as undrafted,
    ):
      _compute_account_scope(
        _session(),
        window=reconciliation_window("2026-03", 1, "ent_p", shadow=True),
        side=_side(),
        include_tied=False,
      )
    drafts.assert_not_called()
    undrafted.assert_not_called()

  def test_a_live_window_still_counts_the_drafts(self):
    with (
      patch(f"{MODULE}.get_net_balances_cents", return_value={}),
      patch(f"{MODULE}._draft_balances", return_value={}) as drafts,
      patch(f"{MODULE}._undrafted_schedule_balances", return_value={}),
    ):
      _compute_account_scope(
        _session(),
        window=reconciliation_window("2026-03", 1, "ent_p"),
        side=_side(),
        include_tied=False,
      )
    drafts.assert_called_once()
