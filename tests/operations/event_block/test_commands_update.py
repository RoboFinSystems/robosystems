"""Tests for update_event_block — status transitions, supersede chain, late-binding."""

from __future__ import annotations

from datetime import UTC, date, datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.exc import OperationalError

from robosystems.models.api.event_block import UpdateEventBlockRequest
from robosystems.operations.event_block.commands import (
  EventNotFoundError,
  HandlerMetadataValidationError,
  InvalidEventTransitionError,
  update_event_block,
)
from robosystems.operations.locking import RowLockedError


def _event(event_id: str, status: str = "classified") -> SimpleNamespace:
  """Build a minimal stand-in for an Event row that satisfies _to_envelope."""
  return SimpleNamespace(
    id=event_id,
    event_type="invoice_issued",
    event_category="sales",
    event_class="economic",
    event_action=None,
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
    payload_drift=False,
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
  session.get.side_effect = lambda _cls, eid, **_kwargs: by_id.get(eid)
  # The locked read is one ordered batch query covering the event and, on a
  # supersede, its successor — `.filter(...).order_by(id).populate_existing()
  # .with_for_update().all()`. Self-returning links keep the stub independent
  # of the chain's order; the command keys the result by id, so handing back
  # every event is equivalent to a matching IN clause.
  locked_q = session.query.return_value.filter.return_value
  locked_q.order_by.return_value = locked_q
  locked_q.populate_existing.return_value = locked_q
  locked_q.with_for_update.return_value = locked_q
  locked_q.all.return_value = list(events)
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

  def test_commit_takes_period_fence_before_row_lock(self) -> None:
    """Approval must fence the period before locking the event row.

    Close holds the exclusive fence then the event lock. Taking the
    event first and the fence in the handler was the inversion that
    failed close mid-publish.
    """
    event = self._journal_event(status="captured")
    session = _session_with_events(event)
    body = UpdateEventBlockRequest(event_id="evt_qb_001", transition_to="committed")

    order: list[str] = []
    session.query.return_value.filter.return_value.with_for_update.side_effect = (
      lambda *a, **k: (
        order.append("row_lock"),
        session.query.return_value.filter.return_value,
      )[1]
    )

    with (
      patch(
        "robosystems.operations.event_block.commands.assert_period_not_closed",
        side_effect=lambda *a, **k: order.append("fence"),
      ) as fence,
      patch(
        "robosystems.operations.event_block.commands.get_python_handler",
        return_value=None,
      ),
    ):
      update_event_block(session, body, created_by="usr_test")

    fence.assert_called_once()
    assert order == ["fence", "row_lock"], order
    # Fenced on the event's current posting date (occurred_at, no effective_at).
    assert fence.call_args.args[1:] == (date(2026, 3, 1),)
    assert event.status == "committed"

  def test_commit_with_effective_at_patch_fences_both_dates(self) -> None:
    """An approval that also moves ``effective_at`` posts against the new
    date, so the pre-lock fence must cover the period it is moving *to*
    as well as the one it is leaving — as ``update_journal_entry`` does."""
    event = self._journal_event(status="captured")
    session = _session_with_events(event)
    body = UpdateEventBlockRequest(
      event_id="evt_qb_001",
      transition_to="committed",
      effective_at=datetime(2026, 4, 30, 23, 59, 59, tzinfo=UTC),
    )

    with (
      patch(
        "robosystems.operations.event_block.commands.assert_period_not_closed"
      ) as fence,
      patch(
        "robosystems.operations.event_block.commands.get_python_handler",
        return_value=None,
      ),
    ):
      update_event_block(session, body, created_by="usr_test")

    fence.assert_called_once()
    assert fence.call_args.args[1:] == (date(2026, 3, 1), date(2026, 4, 30))

  def test_void_does_not_take_period_fence(self) -> None:
    event = self._journal_event(status="captured")
    session = _session_with_events(event)
    body = UpdateEventBlockRequest(event_id="evt_qb_001", transition_to="voided")

    with patch(
      "robosystems.operations.event_block.commands.assert_period_not_closed"
    ) as fence:
      update_event_block(session, body, created_by="usr_test")

    fence.assert_not_called()
    assert event.status == "voided"

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


class TestTransitionRowLock:
  """The status check is read-decide-write, so the row must be locked for
  the life of the transaction. Without the lock an inbox approval racing
  the sync's auto-commit pass fires the same handler twice and leaves one
  event with two sets of GL rows — books that still foot and are wrong."""

  def test_event_is_loaded_for_update(self) -> None:
    event = _event("evt_a", status="classified")
    session = _session_with_events(event)

    body = UpdateEventBlockRequest(event_id="evt_a", description="corrected")
    update_event_block(session, body, created_by="usr_test")

    locked_q = session.query.return_value.filter.return_value
    locked_q.with_for_update.assert_called()
    locked_q.populate_existing.assert_called()

  def test_supersede_locks_both_rows_in_id_order(self) -> None:
    """Both rows the supersede path writes are locked in the same statement.

    Locking only the predecessor leaves two callers superseding each other in
    opposite directions each holding what the other needs; the deadlock then
    surfaces at commit, outside any wrapper that would translate it. Ordering
    makes the cycle impossible rather than translating it after the fact.
    """
    predecessor = _event("evt_a", status="classified")
    successor = _event("evt_b", status="classified")
    session = _session_with_events(predecessor, successor)

    update_event_block(
      session,
      UpdateEventBlockRequest(
        event_id="evt_a", transition_to="superseded", superseded_by_id="evt_b"
      ),
      created_by="usr_test",
    )

    locked_q = session.query.return_value.filter.return_value
    locked_q.with_for_update.assert_called()
    from robosystems.operations.locking import ordered_lock_column

    ordered = [a for c in locked_q.order_by.call_args_list for a in c.args]
    assert len(ordered) == 1
    assert ordered[0] is ordered_lock_column()
    # The successor came from the locked batch, not a second unlocked fetch.
    assert not [c for c in session.get.call_args_list if c.args[1:2] == ("evt_b",)]


class TestLockContention:
  """The lock is bounded. A sync holds the batch for the life of its
  transaction (minutes), so an approval that waited would pin an HTTP
  request and its pooled connection for that whole time — enough of them
  exhaust the extensions pool. Postgres raises 55P03 when lock_timeout
  expires; that becomes a retryable 409, not a 500."""

  def test_lock_timeout_is_set_before_the_locking_read(self) -> None:
    event = _event("evt_a", status="classified")
    session = _session_with_events(event)

    body = UpdateEventBlockRequest(event_id="evt_a", description="corrected")
    update_event_block(session, body, created_by="usr_test")

    statements = [str(c.args[0]) for c in session.execute.call_args_list if c.args]
    assert any("lock_timeout" in s for s in statements)

  def test_lock_not_available_becomes_event_locked_error(self) -> None:
    session = _session_with_events(_event("evt_a"))
    session.query.side_effect = OperationalError(
      "SELECT ...", {}, SimpleNamespace(pgcode="55P03")
    )

    body = UpdateEventBlockRequest(event_id="evt_a", transition_to="committed")
    with pytest.raises(RowLockedError, match="evt_a"):
      update_event_block(session, body, created_by="usr_test")

    session.commit.assert_not_called()

  def test_deadlock_also_becomes_event_locked_error(self) -> None:
    """40P01 is retryable in exactly the sense 55P03 is.

    Postgres has already aborted the transaction by the time it surfaces, so
    there is nothing to salvage — but it must not reach the inbox as a 500.
    """
    session = _session_with_events(_event("evt_a"))
    session.query.side_effect = OperationalError(
      "SELECT ...", {}, SimpleNamespace(pgcode="40P01")
    )

    body = UpdateEventBlockRequest(event_id="evt_a", transition_to="committed")
    with pytest.raises(RowLockedError, match="evt_a"):
      update_event_block(session, body, created_by="usr_test")

    session.commit.assert_not_called()

  def test_other_operational_errors_are_not_swallowed(self) -> None:
    """Only 55P03 is a lock conflict. A connection fault must keep its
    identity rather than surfacing to the inbox as 'retry in a moment'."""
    session = _session_with_events(_event("evt_a"))
    session.query.side_effect = OperationalError(
      "SELECT ...", {}, SimpleNamespace(pgcode="08006")
    )

    body = UpdateEventBlockRequest(event_id="evt_a", transition_to="committed")
    with pytest.raises(OperationalError):
      update_event_block(session, body, created_by="usr_test")
