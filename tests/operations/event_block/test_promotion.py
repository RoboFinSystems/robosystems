"""Tests for the period-boundary obligation promoter (Stream 2.B core)."""

from __future__ import annotations

from datetime import UTC, datetime, time
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from robosystems.operations.event_block.promotion import (
  PromotionResult,
  promote_pending_obligations,
)


def _pending_event(
  event_id: str,
  *,
  schedule_id: str = "struct_a",
  period_end: str = "2026-01-31",
  status: str = "pending",
) -> SimpleNamespace:
  """Build a stand-in pending schedule_entry_due event row."""
  return SimpleNamespace(
    id=event_id,
    event_type="schedule_entry_due",
    status=status,
    occurred_at=datetime.combine(
      datetime.fromisoformat(period_end).date(), time(23, 59, 59), tzinfo=UTC
    ),
    metadata_={
      "schedule_id": schedule_id,
      "posting_date": period_end,
      "period_start": period_end[:7] + "-01",
      "period_end": period_end,
    },
  )


def _session_returning(events: list[SimpleNamespace]) -> MagicMock:
  """MagicMock session whose .query(Event).filter(...).all() returns events."""
  session = MagicMock()
  query = MagicMock()
  query.filter.return_value = query
  query.all.return_value = events
  session.query.return_value = query
  return session


class TestCoPilotMode:
  """dispatch_handlers=False — flip status only, no GL writes."""

  def test_no_pending_returns_empty_result(self) -> None:
    session = _session_returning([])
    result = promote_pending_obligations(
      session, "kg_test", as_of=datetime(2026, 2, 1, tzinfo=UTC)
    )
    assert isinstance(result, PromotionResult)
    assert result.classified_count == 0
    assert result.dispatched_count == 0
    assert result.error_count == 0

  def test_flips_pending_to_classified(self) -> None:
    """Co-pilot path uses a bulk UPDATE — verify the result tracks every event id.

    The bulk path doesn't mutate the in-memory ORM rows (the UPDATE goes
    straight through SQLAlchemy's bulk machinery), so we check the
    PromotionResult and the bulk update call instead of `event.status`.
    """
    e1 = _pending_event("evt_1", period_end="2026-01-31")
    e2 = _pending_event("evt_2", period_end="2026-01-31")
    session = _session_returning([e1, e2])

    result = promote_pending_obligations(
      session, "kg_test", as_of=datetime(2026, 2, 1, tzinfo=UTC)
    )

    assert set(result.classified_event_ids) == {"evt_1", "evt_2"}
    assert result.dispatched_count == 0  # co-pilot — no dispatch
    # Two .query() calls: one to load candidates, one to bulk-update by id
    assert session.query.call_count == 2
    bulk_update_call = session.query.return_value.filter.return_value.update
    bulk_update_call.assert_called_once_with(
      {"status": "classified"}, synchronize_session="fetch"
    )

  def test_does_not_call_handler_dispatch(self) -> None:
    e1 = _pending_event("evt_1")
    session = _session_returning([e1])

    with patch(
      "robosystems.operations.event_block.promotion.get_python_handler"
    ) as get_handler:
      promote_pending_obligations(
        session, "kg_test", as_of=datetime(2026, 2, 1, tzinfo=UTC)
      )

    # Co-pilot mode never even resolves the handler.
    get_handler.assert_not_called()


class TestAutopilotMode:
  """dispatch_handlers=True — flip status AND fire the registered handler."""

  def test_dispatches_for_each_classified_event(self) -> None:
    e1 = _pending_event("evt_1", schedule_id="struct_a")
    e2 = _pending_event("evt_2", schedule_id="struct_b")
    session = _session_returning([e1, e2])

    handler = MagicMock()
    handler.metadata_schema.model_validate.return_value = MagicMock()
    handler.dispatch.return_value = MagicMock()

    with patch(
      "robosystems.operations.event_block.promotion.get_python_handler",
      return_value=handler,
    ):
      result = promote_pending_obligations(
        session,
        "kg_test",
        as_of=datetime(2026, 2, 1, tzinfo=UTC),
        dispatch_handlers=True,
      )

    assert handler.dispatch.call_count == 2
    assert result.dispatched_count == 2
    assert result.error_count == 0
    # Status flips happened too — autopilot is co-pilot + dispatch
    assert e1.status == "classified"
    assert e2.status == "classified"

  def test_per_event_validation_error_does_not_sink_sweep(self) -> None:
    """A single bad metadata payload is captured; other events still process."""
    from pydantic import ValidationError

    e_bad = _pending_event("evt_bad")
    e_good = _pending_event("evt_good")
    session = _session_returning([e_bad, e_good])

    handler = MagicMock()
    handler.dispatch.return_value = MagicMock()

    def validate(payload):
      if payload.get("schedule_id") == e_bad.metadata_["schedule_id"] and (
        # Distinguish bad vs good by event id smuggled through period_end
        payload.get("posting_date") == e_bad.metadata_["posting_date"]
        and payload.get("period_end") == e_bad.metadata_["period_end"]
        and payload is e_bad.metadata_
      ):
        raise ValidationError.from_exception_data(
          title="ScheduleEntryDueMetadata", line_errors=[]
        )
      return MagicMock()

    handler.metadata_schema.model_validate.side_effect = validate

    with patch(
      "robosystems.operations.event_block.promotion.get_python_handler",
      return_value=handler,
    ):
      result = promote_pending_obligations(
        session,
        "kg_test",
        as_of=datetime(2026, 2, 1, tzinfo=UTC),
        dispatch_handlers=True,
      )

    # Both classified; only the good one dispatched.
    assert e_bad.status == "classified"
    assert e_good.status == "classified"
    assert "evt_bad" in result.classified_event_ids
    assert "evt_good" in result.classified_event_ids
    assert result.dispatched_event_ids == ["evt_good"]
    assert len(result.errors) == 1
    assert result.errors[0][0] == "evt_bad"

  def test_handler_dispatch_exception_is_collected_not_raised(self) -> None:
    """If handler.dispatch raises, the error is captured but the sweep continues."""
    e1 = _pending_event("evt_1")
    e2 = _pending_event("evt_2")
    session = _session_returning([e1, e2])

    handler = MagicMock()
    handler.metadata_schema.model_validate.return_value = MagicMock()
    handler.dispatch.side_effect = [
      RuntimeError("fact missing"),
      MagicMock(),  # second event succeeds
    ]

    with patch(
      "robosystems.operations.event_block.promotion.get_python_handler",
      return_value=handler,
    ):
      result = promote_pending_obligations(
        session,
        "kg_test",
        as_of=datetime(2026, 2, 1, tzinfo=UTC),
        dispatch_handlers=True,
      )

    # No raise. Both classified (the flip already happened).
    assert e1.status == "classified"
    assert e2.status == "classified"
    assert result.dispatched_event_ids == ["evt_2"]
    assert result.error_count == 1
    assert "RuntimeError" in result.errors[0][1]
    assert "fact missing" in result.errors[0][1]

  def test_dispatched_handler_receives_typed_metadata_and_created_by(self) -> None:
    e1 = _pending_event("evt_1")
    session = _session_returning([e1])

    typed = MagicMock(name="typed_metadata")
    handler = MagicMock()
    handler.metadata_schema.model_validate.return_value = typed
    handler.dispatch.return_value = MagicMock()

    with patch(
      "robosystems.operations.event_block.promotion.get_python_handler",
      return_value=handler,
    ):
      promote_pending_obligations(
        session,
        "kg_test",
        as_of=datetime(2026, 2, 1, tzinfo=UTC),
        dispatch_handlers=True,
        created_by="usr_admin",
      )

    handler.dispatch.assert_called_once_with(session, e1, typed, "usr_admin")


class TestQueryShape:
  """The candidate query targets event_type, status, and the as_of cutoff."""

  def test_filters_by_event_type_status_and_as_of(self) -> None:
    e1 = _pending_event("evt_1")
    session = _session_returning([e1])

    promote_pending_obligations(
      session, "kg_test", as_of=datetime(2026, 2, 1, tzinfo=UTC)
    )

    # The first .filter() call is the candidate load:
    # session.query(Event).filter(event_type, status, occurred_at).all()
    # (The second .filter() is the bulk-update by id list — different shape.)
    query = session.query.return_value
    candidate_filter_args = query.filter.call_args_list[0].args
    rendered = " | ".join(str(c) for c in candidate_filter_args)
    assert "events.event_type" in rendered
    assert "events.status" in rendered
    assert "events.occurred_at" in rendered
