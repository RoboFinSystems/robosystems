"""Tests for update_event_block — status transitions, supersede chain, late-binding."""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from robosystems.models.api.event_block import UpdateEventBlockRequest
from robosystems.operations.event_block.commands import (
  EventNotFoundError,
  HandlerMetadataValidationError,
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


class TestApproveFiresHandler:
  """captured → committed (and classified → committed) fires the
  registered Python handler against the captured metadata. This is what
  makes the inbox approve action actually post GL rows."""

  def _journal_event(
    self, *, event_id: str = "evt_qb_001", status: str = "captured"
  ) -> SimpleNamespace:
    """An event_type='journal_entry_recorded' event whose captured metadata
    matches the handler's flat-shape schema. The dispatch mock stands in
    for actual GL writes."""
    e = _event(event_id, status=status)
    e.event_type = "journal_entry_recorded"
    e.event_category = "adjustment"
    e.metadata_ = {
      "posting_date": "2026-03-31",
      "memo": "Sale",
      "type": "standard",
      "status": "draft",
      "line_items": [
        {"element_id": "elem_a", "debit_amount": 100, "credit_amount": 0},
        {"element_id": "elem_b", "debit_amount": 0, "credit_amount": 100},
      ],
    }
    return e

  def test_captured_to_committed_fires_handler(self) -> None:
    event = self._journal_event(status="captured")
    session = _session_with_events(event)

    body = UpdateEventBlockRequest(event_id="evt_qb_001", transition_to="committed")
    fake_handler = MagicMock()
    fake_handler.metadata_schema.model_validate.return_value = "validated_metadata"

    with patch(
      "robosystems.operations.event_block.commands.get_python_handler",
      return_value=fake_handler,
    ) as get_handler:
      update_event_block(session, body, created_by="usr_test")

    get_handler.assert_called_once_with("journal_entry_recorded")
    fake_handler.dispatch.assert_called_once_with(
      session, event, "validated_metadata", "usr_test"
    )
    assert event.status == "committed"
    session.commit.assert_called_once()

  def test_classified_to_committed_fires_handler(self) -> None:
    event = self._journal_event(status="classified")
    session = _session_with_events(event)

    body = UpdateEventBlockRequest(event_id="evt_qb_001", transition_to="committed")
    fake_handler = MagicMock()
    fake_handler.metadata_schema.model_validate.return_value = "ok"

    with patch(
      "robosystems.operations.event_block.commands.get_python_handler",
      return_value=fake_handler,
    ):
      update_event_block(session, body, created_by="usr_test")

    fake_handler.dispatch.assert_called_once()

  def test_committed_to_fulfilled_does_not_fire_handler(self) -> None:
    """Handler fires only on the initial commit, not subsequent transitions."""
    event = self._journal_event(status="committed")
    session = _session_with_events(event)

    body = UpdateEventBlockRequest(event_id="evt_qb_001", transition_to="fulfilled")
    fake_handler = MagicMock()

    with patch(
      "robosystems.operations.event_block.commands.get_python_handler",
      return_value=fake_handler,
    ):
      update_event_block(session, body, created_by="usr_test")

    fake_handler.dispatch.assert_not_called()
    assert event.status == "fulfilled"

  def test_void_does_not_fire_handler(self) -> None:
    """Rejecting a captured event must not write GL rows."""
    event = self._journal_event(status="captured")
    session = _session_with_events(event)

    body = UpdateEventBlockRequest(event_id="evt_qb_001", transition_to="voided")
    fake_handler = MagicMock()

    with patch(
      "robosystems.operations.event_block.commands.get_python_handler",
      return_value=fake_handler,
    ):
      update_event_block(session, body, created_by="usr_test")

    fake_handler.dispatch.assert_not_called()
    assert event.status == "voided"

  def test_no_handler_registered_falls_through(self) -> None:
    """An event_type with no Python handler still transitions cleanly —
    e.g., support events with no GL impact."""
    event = self._journal_event(status="captured")
    event.event_type = "audit_review_completed"  # no registered handler
    session = _session_with_events(event)

    body = UpdateEventBlockRequest(event_id="evt_qb_001", transition_to="committed")
    with patch(
      "robosystems.operations.event_block.commands.get_python_handler",
      return_value=None,
    ):
      update_event_block(session, body, created_by="usr_test")

    assert event.status == "committed"
    session.commit.assert_called_once()

  def test_handler_validation_failure_raises_and_blocks_commit(self) -> None:
    """Bad captured metadata fails validation; the wrapper exception lets
    the caller surface a useful error. Status mutation happens before the
    raise — caller is expected to roll back on exception."""
    event = self._journal_event(status="captured")
    session = _session_with_events(event)

    body = UpdateEventBlockRequest(event_id="evt_qb_001", transition_to="committed")
    fake_handler = MagicMock()
    # Construct a real ValidationError via a Pydantic model
    from pydantic import BaseModel
    from pydantic import ValidationError as PydanticValidationError

    class _Tiny(BaseModel):
      x: int

    try:
      _Tiny(x="not_an_int")
    except PydanticValidationError as e:
      validation_err = e
    fake_handler.metadata_schema.model_validate.side_effect = validation_err

    with patch(
      "robosystems.operations.event_block.commands.get_python_handler",
      return_value=fake_handler,
    ):
      with pytest.raises(HandlerMetadataValidationError) as exc:
        update_event_block(session, body, created_by="usr_test")

    assert "evt_qb_001" in str(exc.value)
    assert "journal_entry_recorded" in str(exc.value)
    fake_handler.dispatch.assert_not_called()
    # Caller's transaction must roll back — we never reached commit.
    session.commit.assert_not_called()
