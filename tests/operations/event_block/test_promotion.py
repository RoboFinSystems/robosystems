"""Tests for the period-boundary obligation promoter (Stream 2.B core)."""

from __future__ import annotations

from datetime import UTC, date, datetime, time
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from robosystems.models.extensions.roboledger.entry import Entry
from robosystems.models.extensions.roboledger.event import Event
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


def _entry_row(schedule_id: str, posting_date: str) -> SimpleNamespace:
  """Build a stand-in entries row for the stranded-obligation draft check."""
  return SimpleNamespace(
    source_structure_id=schedule_id,
    posting_date=date.fromisoformat(posting_date),
  )


def _session_returning(
  events: list[SimpleNamespace],
  *,
  live_schedule_ids: set[str] | None = None,
  entries: list[SimpleNamespace] | None = None,
) -> MagicMock:
  """MagicMock session that dispatches by query target.

  - ``query(Event)`` → candidate load (``.all()`` → events) + bulk updates.
  - ``query(Entry)`` → the stranded check's draft-existence lookup
    (``.all()`` → ``entries``).
  - ``query(Structure.id)`` → the orphan guard's existence check
    (``.all()`` → ``[(sid,), ...]`` for the live schedule ids).

  ``live_schedule_ids`` defaults to every schedule_id present on ``events``
  (i.e. no orphans), so tests that don't exercise the guard behave as before.
  The Event query mock is exposed as ``session.event_query`` for introspection.
  """
  if live_schedule_ids is None:
    live_schedule_ids = {
      e.metadata_.get("schedule_id")
      for e in events
      if e.metadata_ and e.metadata_.get("schedule_id")
    }
  event_query = MagicMock()
  event_query.filter.return_value = event_query
  # The candidate load is `.filter(...).order_by(id).with_for_update().all()` —
  # locked because everything the sweep does after it is read-decide-write
  # against these rows, ordered so it cannot deadlock against the other batch
  # locks. Self-returning like `.filter` so the chain stays
  # position-independent.
  event_query.order_by.return_value = event_query
  event_query.with_for_update.return_value = event_query
  event_query.all.return_value = events
  entry_query = MagicMock()
  entry_query.filter.return_value = entry_query
  entry_query.all.return_value = entries or []
  struct_query = MagicMock()
  struct_query.filter.return_value = struct_query
  struct_query.all.return_value = [(sid,) for sid in live_schedule_ids]
  session = MagicMock()
  session.query.side_effect = lambda target: (
    event_query if target is Event else entry_query if target is Entry else struct_query
  )
  session.event_query = event_query
  session.entry_query = entry_query
  session.struct_query = struct_query
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
    # Three .query() calls now: load candidates, the orphan-guard structure
    # existence check, and the bulk classify-update by id.
    assert session.query.call_count == 3
    bulk_update_call = session.event_query.filter.return_value.update
    bulk_update_call.assert_called_once_with(
      {"status": "classified"}, synchronize_session="fetch"
    )

  def test_classify_update_is_guarded_on_pending(self) -> None:
    """The bulk UPDATE carries `status = 'pending'` alongside the id list.

    Without it the statement writes `classified` over whatever the row now
    holds, so an obligation voided or committed since the candidate read would
    be silently reverted — a lost update, not a redundant write.

    The candidate read's `FOR UPDATE` is what makes that race unreachable, so
    this cannot be provoked end-to-end; it is pinned here instead, at the
    statement that relies on it, against a refactor that moves the read.
    """
    session = _session_returning([_pending_event("evt_1", period_end="2026-01-31")])

    promote_pending_obligations(
      session, "kg_test", as_of=datetime(2026, 2, 1, tzinfo=UTC)
    )

    predicates = [
      str(arg)
      for call in session.event_query.filter.call_args_list
      for arg in call.args
    ]
    assert any("events.status = " in p for p in predicates), predicates

  def test_candidate_load_is_ordered_for_deadlock_freedom(self) -> None:
    """The candidate load orders by `id`.

    `supersede_pending_obligations` locks an overlapping set of pending
    obligations. Two transactions that take overlapping row locks in different
    orders deadlock — each holding what the other waits for — and Postgres
    resolves that by killing one with SQLSTATE 40P01. Without a shared ORDER BY
    the acquisition sequence is whatever each query plan produces, which is not
    a property either query controls. Pinned here because nothing else would
    notice it going away until it happened in production.
    """
    session = _session_returning([_pending_event("evt_1", period_end="2026-01-31")])

    promote_pending_obligations(
      session, "kg_test", as_of=datetime(2026, 2, 1, tzinfo=UTC)
    )

    ordered_by = [
      str(arg)
      for call in session.event_query.order_by.call_args_list
      for arg in call.args
    ]
    assert ordered_by == ["Event.id"], ordered_by

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
    query = session.event_query
    candidate_filter_args = query.filter.call_args_list[0].args
    rendered = " | ".join(str(c) for c in candidate_filter_args)
    assert "events.event_type" in rendered
    assert "events.status" in rendered
    assert "events.occurred_at" in rendered


class TestOrphanGuard:
  """Obligations whose schedule structure was deleted are voided in place,
  never drafted — the catch-all that stops a deleted schedule from
  double-posting at promotion.
  """

  def test_orphan_voided_not_classified_copilot(self) -> None:
    live = _pending_event("evt_live", schedule_id="struct_live")
    orphan = _pending_event("evt_orphan", schedule_id="struct_deleted")
    # Only struct_live still exists as a structure row.
    session = _session_returning([live, orphan], live_schedule_ids={"struct_live"})

    result = promote_pending_obligations(
      session, "kg_test", as_of=datetime(2026, 2, 1, tzinfo=UTC)
    )

    assert result.voided_orphan_event_ids == ["evt_orphan"]
    assert result.voided_orphan_count == 1
    # The live obligation still classifies; the orphan never does.
    assert result.classified_event_ids == ["evt_live"]
    assert "evt_orphan" not in result.classified_event_ids

  def test_orphan_not_dispatched_autopilot(self) -> None:
    live = _pending_event("evt_live", schedule_id="struct_live")
    orphan = _pending_event("evt_orphan", schedule_id="struct_deleted")
    session = _session_returning([live, orphan], live_schedule_ids={"struct_live"})

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

    # Only the live obligation is dispatched into a draft; the orphan is voided.
    assert result.dispatched_event_ids == ["evt_live"]
    assert result.voided_orphan_event_ids == ["evt_orphan"]
    assert handler.dispatch.call_count == 1
    assert orphan.status != "classified"

  def test_no_orphans_when_all_schedules_live(self) -> None:
    e1 = _pending_event("evt_1", schedule_id="struct_a")
    e2 = _pending_event("evt_2", schedule_id="struct_b")
    session = _session_returning([e1, e2], live_schedule_ids={"struct_a", "struct_b"})

    result = promote_pending_obligations(
      session, "kg_test", as_of=datetime(2026, 2, 1, tzinfo=UTC)
    )

    assert result.voided_orphan_count == 0
    assert set(result.classified_event_ids) == {"evt_1", "evt_2"}


class TestStrandedObligations:
  """F9: matured `classified` obligations with no drafted closing entry are
  reachable by the sweep — dispatched in autopilot, surfaced in co-pilot.
  A co-pilot background sweep flips pending → classified without dispatch,
  so these are the adjusting entries a close would otherwise silently omit.
  """

  @staticmethod
  def _handler() -> MagicMock:
    handler = MagicMock()
    handler.metadata_schema.model_validate.return_value = MagicMock()
    handler.dispatch.return_value = MagicMock()
    return handler

  def test_copilot_surfaces_stranded_without_dispatch(self) -> None:
    stranded = _pending_event("evt_stranded", status="classified")
    session = _session_returning([stranded])

    result = promote_pending_obligations(
      session, "kg_test", as_of=datetime(2026, 2, 1, tzinfo=UTC)
    )

    assert result.stranded_event_ids == ["evt_stranded"]
    assert result.stranded_count == 1
    assert result.dispatched_count == 0
    # Already classified — the co-pilot flip has nothing to do.
    assert result.classified_event_ids == []

  def test_autopilot_dispatches_stranded(self) -> None:
    stranded = _pending_event("evt_stranded", status="classified")
    session = _session_returning([stranded])
    handler = self._handler()

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

    assert result.stranded_event_ids == ["evt_stranded"]
    assert result.dispatched_event_ids == ["evt_stranded"]
    # Not flipped this sweep — it was already classified.
    assert result.classified_event_ids == []
    assert handler.dispatch.call_count == 1

  def test_classified_with_draft_is_not_stranded(self) -> None:
    """An entries row for the (schedule, period) — regardless of which event
    triggered it — means the obligation has its draft. Mirrors the reconcile
    key in ScheduleService.create_closing_entry, so posted twins (like a
    manually re-dispatched close) are never re-dispatched.
    """
    settled = _pending_event("evt_settled", status="classified")
    session = _session_returning(
      [settled], entries=[_entry_row("struct_a", "2026-01-31")]
    )
    handler = self._handler()

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

    assert result.stranded_event_ids == []
    assert result.dispatched_event_ids == []
    handler.dispatch.assert_not_called()

  def test_entry_outside_period_window_still_stranded(self) -> None:
    """A draft for a *different* month doesn't settle this obligation."""
    stranded = _pending_event(
      "evt_stranded", status="classified", period_end="2026-01-31"
    )
    session = _session_returning(
      [stranded], entries=[_entry_row("struct_a", "2025-12-31")]
    )

    result = promote_pending_obligations(
      session, "kg_test", as_of=datetime(2026, 2, 1, tzinfo=UTC)
    )

    assert result.stranded_event_ids == ["evt_stranded"]

  def test_classified_orphan_is_voided_not_dispatched(self) -> None:
    """A stranded obligation whose schedule was deleted voids in place
    (Layer 2), instead of erroring at dispatch every sweep."""
    orphan = _pending_event(
      "evt_orphan", schedule_id="struct_deleted", status="classified"
    )
    session = _session_returning([orphan], live_schedule_ids=set())
    handler = self._handler()

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

    assert result.voided_orphan_event_ids == ["evt_orphan"]
    assert result.stranded_event_ids == []
    handler.dispatch.assert_not_called()

  def test_pending_and_stranded_both_dispatch_in_one_sweep(self) -> None:
    fresh = _pending_event("evt_fresh", schedule_id="struct_a")
    stranded = _pending_event(
      "evt_stranded", schedule_id="struct_b", status="classified"
    )
    session = _session_returning([fresh, stranded])
    handler = self._handler()

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

    assert result.classified_event_ids == ["evt_fresh"]
    assert result.stranded_event_ids == ["evt_stranded"]
    assert set(result.dispatched_event_ids) == {"evt_fresh", "evt_stranded"}
    assert handler.dispatch.call_count == 2

  def test_classified_without_window_metadata_is_skipped(self) -> None:
    """No schedule_id / period metadata → the draft check can't run; the
    event is left alone (dispatch would reject its metadata anyway)."""
    opaque = SimpleNamespace(
      id="evt_opaque",
      event_type="schedule_entry_due",
      status="classified",
      occurred_at=datetime(2026, 1, 31, 23, 59, 59, tzinfo=UTC),
      metadata_=None,
    )
    session = _session_returning([opaque])

    result = promote_pending_obligations(
      session, "kg_test", as_of=datetime(2026, 2, 1, tzinfo=UTC)
    )

    assert result.stranded_event_ids == []
    assert result.dispatched_count == 0
