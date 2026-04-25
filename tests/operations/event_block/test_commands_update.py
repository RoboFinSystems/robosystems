"""Tests for update_event_block — status transitions, supersede chain, late-binding."""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from robosystems.models.api.event_block import UpdateEventBlockRequest
from robosystems.operations.event_block.commands import (
  EventNotFoundError,
  InvalidEventTransitionError,
  update_event_block,
)


def _event(event_id: str, status: str = "classified") -> SimpleNamespace:
  """Build a minimal stand-in for an Event row that satisfies _to_envelope."""
  return SimpleNamespace(
    id=event_id,
    event_type="invoice_issued",
    event_category="sales",
    event_class="economic",
    status=status,
    occurred_at=datetime(2026, 3, 1, tzinfo=UTC),
    effective_at=None,
    source="native",
    external_id=None,
    external_url=None,
    amount=None,
    currency="USD",
    description=None,
    metadata_={},
    agent_id=None,
    resource_type=None,
    resource_element_id=None,
    replaced_by_event_id=None,
    replaces_event_id=None,
    obligated_by_event_id=None,
    discharges_event_id=None,
    created_at=datetime(2026, 3, 1, tzinfo=UTC),
    created_by="usr_test",
  )


def _session_with_events(*events: SimpleNamespace) -> MagicMock:
  """MagicMock session whose .get(Event, id) returns the matching event."""
  by_id = {e.id: e for e in events}
  session = MagicMock()
  session.get.side_effect = lambda _cls, eid: by_id.get(eid)
  # _load_dimension_ids issues a select via session.execute → return [].
  session.execute.return_value.scalars.return_value.all.return_value = []
  return session


class TestSupersedeChain:
  def test_supersede_sets_both_sides_atomically(self) -> None:
    """When A is superseded by B, both forward and backward links are set."""
    predecessor = _event("evt_a", status="classified")
    successor = _event("evt_b", status="classified")
    session = _session_with_events(predecessor, successor)

    body = UpdateEventBlockRequest(
      event_id="evt_a",
      transition_to="superseded",
      superseded_by_id="evt_b",
    )
    envelope = update_event_block(session, body, created_by="usr_test")

    assert predecessor.status == "superseded"
    assert predecessor.replaced_by_event_id == "evt_b"
    assert successor.replaces_event_id == "evt_a"
    assert envelope.replaced_by_event_id == "evt_b"
    session.commit.assert_called_once()

  def test_supersede_requires_superseded_by_id(self) -> None:
    """transition_to='superseded' with no superseded_by_id is rejected."""
    predecessor = _event("evt_a")
    session = _session_with_events(predecessor)

    body = UpdateEventBlockRequest(event_id="evt_a", transition_to="superseded")
    with pytest.raises(InvalidEventTransitionError, match="superseded_by_id"):
      update_event_block(session, body, created_by="usr_test")

    assert predecessor.status == "classified"
    assert predecessor.replaced_by_event_id is None
    session.commit.assert_not_called()

  def test_supersede_self_reference_rejected(self) -> None:
    """An event cannot supersede itself."""
    predecessor = _event("evt_a")
    session = _session_with_events(predecessor)

    body = UpdateEventBlockRequest(
      event_id="evt_a",
      transition_to="superseded",
      superseded_by_id="evt_a",
    )
    with pytest.raises(InvalidEventTransitionError, match="cannot supersede itself"):
      update_event_block(session, body, created_by="usr_test")

    assert predecessor.status == "classified"
    session.commit.assert_not_called()

  def test_supersede_missing_successor_raises(self) -> None:
    """Unknown superseded_by_id raises EventNotFoundError before any mutation."""
    predecessor = _event("evt_a")
    session = _session_with_events(predecessor)

    body = UpdateEventBlockRequest(
      event_id="evt_a",
      transition_to="superseded",
      superseded_by_id="evt_missing",
    )
    with pytest.raises(EventNotFoundError, match="evt_missing"):
      update_event_block(session, body, created_by="usr_test")

    assert predecessor.status == "classified"
    assert predecessor.replaced_by_event_id is None
    session.commit.assert_not_called()

  def test_supersede_from_terminal_state_rejected(self) -> None:
    """Terminal states (fulfilled, voided, superseded) cannot be superseded."""
    predecessor = _event("evt_a", status="fulfilled")
    successor = _event("evt_b")
    session = _session_with_events(predecessor, successor)

    body = UpdateEventBlockRequest(
      event_id="evt_a",
      transition_to="superseded",
      superseded_by_id="evt_b",
    )
    with pytest.raises(InvalidEventTransitionError, match="terminal"):
      update_event_block(session, body, created_by="usr_test")

    assert predecessor.status == "fulfilled"
    assert predecessor.replaced_by_event_id is None
    assert successor.replaces_event_id is None
    session.commit.assert_not_called()

  def test_supersede_allowed_from_non_terminal_states(self) -> None:
    """captured / classified / committed / pending all permit supersede."""
    for source_status in ("captured", "classified", "committed", "pending"):
      predecessor = _event("evt_a", status=source_status)
      successor = _event("evt_b")
      session = _session_with_events(predecessor, successor)

      body = UpdateEventBlockRequest(
        event_id="evt_a",
        transition_to="superseded",
        superseded_by_id="evt_b",
      )
      update_event_block(session, body, created_by="usr_test")

      assert predecessor.status == "superseded", f"failed from {source_status}"
      assert predecessor.replaced_by_event_id == "evt_b"
      assert successor.replaces_event_id == "evt_a"


class TestDualityLateBinding:
  def test_discharges_event_id_can_be_set_via_update(self) -> None:
    """Late-binding: mark a payment as discharging an invoice after the fact."""
    payment = _event("evt_payment", status="classified")
    session = _session_with_events(payment)

    body = UpdateEventBlockRequest(
      event_id="evt_payment",
      discharges_event_id="evt_invoice",
    )
    envelope = update_event_block(session, body, created_by="usr_test")

    assert payment.discharges_event_id == "evt_invoice"
    assert envelope.discharges_event_id == "evt_invoice"

  def test_obligated_by_event_id_can_be_set_via_update(self) -> None:
    """Late-binding: link a depreciation entry back to its asset_acquired event."""
    schedule_entry = _event("evt_dep", status="captured")
    session = _session_with_events(schedule_entry)

    body = UpdateEventBlockRequest(
      event_id="evt_dep",
      obligated_by_event_id="evt_asset",
    )
    envelope = update_event_block(session, body, created_by="usr_test")

    assert schedule_entry.obligated_by_event_id == "evt_asset"
    assert envelope.obligated_by_event_id == "evt_asset"
