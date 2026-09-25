"""Event Block commands: create, update, preview, and execute (publish to source)."""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any

from pydantic import ValidationError
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from robosystems.db.integrity import violates
from robosystems.logger import logger
from robosystems.models.api.event_block import (
  CreateEventBlockRequest,
  EventBlockEnvelope,
  ExecuteEventBlockRequest,
  ExecuteEventBlockResponse,
  UpdateEventBlockRequest,
)
from robosystems.models.api.event_handler import (
  PreviewEventBlockResponse,
  TransactionPreview,
)
from robosystems.models.extensions.roboledger.dimension_junctions import (
  event_dimensions,
)
from robosystems.models.extensions.roboledger.entry import Entry
from robosystems.models.extensions.roboledger.event import Event
from robosystems.models.extensions.roboledger.transaction import Transaction
from robosystems.operations.locking import (
  RowLockedError,
  bounded_lock_wait,
  ordered_lock_column,
)
from robosystems.operations.roboledger.commands._guards import (
  ClosedPeriodError,
  assert_period_not_closed,
)
from robosystems.operations.roboledger.entry_status import (
  LANDED_ENTRY_STATUSES,
)
from robosystems.operations.roboledger.reads.event_block import (
  _load_dimension_ids,
  _to_envelope,
)

from .engine import (
  EngineValidationError,
  apply_handler,
  posting_date_for_event,
  resolve_agent_type,
)
from .python_handlers import get_python_handler
from .python_handlers.types import (
  EventBlockPythonHandler,
  HandlerMetadataValidationError,
)
from .registry import (
  HandlerAmbiguousError,
  HandlerNotFoundError,
  resolve_handler,
)
from .template import (
  TemplateInterpolationError,
  build_handler_context,
  interpolate,
)


class EventNotFoundError(Exception):
  pass


class InvalidEventTransitionError(Exception):
  pass


class EventNotPublishableError(Exception):
  """Raised when execute is asked to publish a retracted event.

  Terminal states have no outbound transitions. Publishing one would
  overwrite ``voided`` / ``superseded`` with ``fulfilled`` and post a
  JournalEntry for work the books already retracted.
  """

  def __init__(self, event_id: str, status: str, reason: str | None = None) -> None:
    super().__init__(
      f"Event {event_id} cannot be published: {reason}."
      if reason
      else f"Event {event_id} is {status!r} and cannot be published."
    )
    self.event_id = event_id
    self.status = status


class EventEffectsAlreadyLandedError(Exception):
  """Raised when a retraction would orphan effects the books already hold.

  Retracting drops the event from `is_live`, which every downstream read
  filters on. Once a row has posted or published to QuickBooks, the effect
  would outlive the event that explains it, so the correction is a reversal.
  """

  def __init__(self, event_id: str, status: str, reason: str) -> None:
    super().__init__(
      f"Event {event_id} cannot be retracted from {status!r}: {reason}. "
      "Reverse the posted entries instead — `create-event-block("
      "event_type='journal_entry_reversed', metadata={entry_id: ...})`."
    )
    self.event_id = event_id
    self.status = status
    self.reason = reason


class DuplicateEventError(Exception):
  """Raised when (source, external_id) already names an event on this graph.

  `idx_events_source_external` is the dedup key that makes re-delivering an
  upstream record safe; this lets a retrying connector tell "already
  ingested" from a real fault.
  """

  def __init__(self, source: str, external_id: str) -> None:
    super().__init__(
      f"Event already exists for source={source} external_id={external_id}"
    )
    self.source = source
    self.external_id = external_id


# Valid outbound transitions from each status. The retracted states
# (`voided`, `superseded`) are final. `fulfilled` can still be retracted
# because a handler may reach it with its rows still draft (they post at
# close). This table decides reachability; `_assert_retractable` decides
# whether a retraction is safe. `captured → classified` records an inbox
# choice ahead of the commit that fires the handler.
_VALID_TRANSITIONS: dict[str, frozenset[str]] = {
  "captured": frozenset({"classified", "committed", "voided", "superseded"}),
  "classified": frozenset(
    {"committed", "pending", "fulfilled", "voided", "superseded"}
  ),
  "committed": frozenset({"pending", "fulfilled", "voided", "superseded"}),
  "pending": frozenset({"fulfilled", "voided", "superseded"}),
  "fulfilled": frozenset({"voided", "superseded"}),
  "voided": frozenset(),
  "superseded": frozenset(),
}

# Retracted statuses — `Event.is_live` is `status NOT IN` this set.
_RETRACTED_STATUSES = frozenset({"voided", "superseded"})

# Entry/Transaction statuses that mean the effect is in the books to stay
# (`reversed` included: retracting would strand both halves of the pair).
# Shared with the balance reads so the two can't disagree.
_LANDED_ENTRY_STATUSES = LANDED_ENTRY_STATUSES
_LANDED_TRANSACTION_STATUSES = frozenset({"posted"})


def _has_linked_entries(session: Session, event_id: str) -> bool:
  """Whether any journal entry, draft or posted, already points at the event."""
  return bool(
    session.execute(
      select(func.count())
      .select_from(Entry)
      .where(Entry.triggered_by_event_id == event_id)
    ).scalar_one()
  )


def _retraction_fence_dates(session: Session, event_id: str) -> list[date]:
  """Posting dates of every ledger row a retraction of this event would strand.

  Read before the event row lock so the caller can take the shared period
  fence in the order the rest of the module uses — fence, then rows.
  """
  entry_dates = session.execute(
    select(Entry.posting_date).where(Entry.triggered_by_event_id == event_id).distinct()
  ).scalars()
  transaction_dates = session.execute(
    select(Transaction.date)
    .where(Transaction.triggered_by_event_id == event_id)
    .distinct()
  ).scalars()
  return sorted({d for d in (*entry_dates, *transaction_dates) if d is not None})


def _assert_retractable(session: Session, event: Event) -> None:
  """Refuse a retraction that would orphan posted or published effects.

  Decided by what landed, never by status: a `committed` event can sit
  across a close and collect posted entries.

  The caller must hold the shared period fence (`_retraction_fence_dates` →
  `assert_period_not_closed`) before the event row lock. Close promotes
  drafts with a bulk `UPDATE` that never locks the `Event` row, so the fence
  is the only thing serializing the two.
  """
  if event.metadata_ and event.metadata_.get("qb_external_id"):
    raise EventEffectsAlreadyLandedError(
      str(event.id),
      str(event.status),
      "it has published to QuickBooks",
    )

  posted_entries = session.execute(
    select(func.count())
    .select_from(Entry)
    .where(
      Entry.triggered_by_event_id == event.id,
      Entry.status.in_(_LANDED_ENTRY_STATUSES),
    )
  ).scalar_one()
  if posted_entries:
    raise EventEffectsAlreadyLandedError(
      str(event.id),
      str(event.status),
      f"{posted_entries} of its journal entries have posted",
    )

  posted_transactions = session.execute(
    select(func.count())
    .select_from(Transaction)
    .where(
      Transaction.triggered_by_event_id == event.id,
      Transaction.status.in_(_LANDED_TRANSACTION_STATUSES),
    )
  ).scalar_one()
  if posted_transactions:
    raise EventEffectsAlreadyLandedError(
      str(event.id),
      str(event.status),
      f"{posted_transactions} of its transactions have posted",
    )


# Execute refuses these; a repeat execute of a ``fulfilled`` event is an
# idempotent no-op instead.
_UNPUBLISHABLE_STATUSES = frozenset({"voided", "superseded"})


# Platform-emitted sources — always valid, no registration involved. Adapter
# and external sources validate against the graph's registered Connections
# instead: registering a connection is what opens a source name.
_STATIC_EVENT_SOURCES = frozenset({"manual", "system", "schedule"})


def _assert_not_duplicate(session: Session, body: CreateEventBlockRequest) -> None:
  """Reject a re-post of an already-ingested (source, external_id) pair.

  Mirrors the partial unique index, which only applies when external_id is
  not null — events without one are not deduplicated and are skipped here too.
  """
  if not body.external_id:
    return
  existing = (
    session.query(Event.id)
    .filter(Event.source == body.source, Event.external_id == body.external_id)
    .first()
  )
  if existing is not None:
    raise DuplicateEventError(body.source, body.external_id)


def _flush_new_event(
  session: Session, event: Event, body: CreateEventBlockRequest
) -> None:
  """Flush a new Event, mapping the ``(source, external_id)`` unique index to
  ``DuplicateEventError`` for posts that race past ``_assert_not_duplicate``.
  """
  try:
    session.flush()
  except IntegrityError as exc:
    if body.external_id and violates(exc, "idx_events_source_external"):
      raise DuplicateEventError(body.source, body.external_id) from exc
    raise


class ConnectionNotOnGraphError(ValueError):
  """A ``connection_id`` that is not one of this graph's connections.

  Connection ids are platform-wide; every place an event names one must
  resolve on the calling graph, or a publish would post into another
  tenant's source-of-truth system.
  """


def _require_connection_on_graph(connection_id: str, graph_id: str) -> None:
  from robosystems.database import SessionFactory as _PlatformSessionFactory
  from robosystems.models.core.connection.connection import Connection

  with _PlatformSessionFactory() as platform_session:
    connection = Connection.get_by_id(connection_id, platform_session)
    if connection is None or str(connection.graph_id) != str(graph_id):
      raise ConnectionNotOnGraphError(
        f"Connection {connection_id!r} is not registered on this graph."
      )


def _validate_routed_connection(metadata: dict | None, graph_id: str) -> None:
  """Validate ``metadata.connection_id`` when an event carries one."""
  connection_id = (metadata or {}).get("connection_id")
  if connection_id:
    _require_connection_on_graph(str(connection_id), graph_id)


def _validate_event_source(source: str, graph_id: str) -> None:
  """A source is valid iff it's platform-emitted or registered on the graph.

  Registered means a Connection whose ``provider`` matches, or an
  ``external`` connection whose ``source_name`` matches.
  """
  if source in _STATIC_EVENT_SOURCES:
    return

  from robosystems.database import SessionFactory as _PlatformSessionFactory
  from robosystems.models.core.connection.connection import Connection

  with _PlatformSessionFactory() as platform_session:
    connections = Connection.get_all_for_graph(graph_id, platform_session)
    for connection in connections:
      if connection.provider == source or (
        connection.provider == "external" and connection.source_name == source
      ):
        return
    registered = sorted(
      {
        c.source_name if c.provider == "external" else c.provider
        for c in connections
        if c.provider != "external" or c.source_name
      }
    )
  raise ValueError(
    f"Unknown event source {source!r}. Valid sources are "
    f"{sorted(_STATIC_EVENT_SOURCES)} plus this graph's registered "
    f"connections ({registered or 'none registered'}). Register an external "
    "source via POST /v1/graphs/{graph_id}/connections with "
    "provider='external'."
  )


def _build_event_row(
  body: CreateEventBlockRequest,
  created_by: str,
  status: str,
) -> Event:
  return Event(
    event_type=body.event_type,
    event_category=body.event_category,
    event_class=body.event_class,
    event_action=body.event_action,
    agent_id=body.agent_id,
    resource_type=body.resource_type,
    resource_element_id=body.resource_element_id,
    occurred_at=body.occurred_at,
    effective_at=body.effective_at,
    source=body.source,
    external_id=body.external_id,
    external_url=body.external_url,
    amount=body.amount,
    currency=body.currency,
    description=body.description,
    metadata_=body.metadata,
    status=status,
    obligated_by_event_id=body.obligated_by_event_id,
    discharges_event_id=body.discharges_event_id,
    created_at=datetime.now(UTC),
    created_by=created_by,
  )


def create_event_block(
  session: Session,
  body: CreateEventBlockRequest,
  created_by: str,
  *,
  graph_id: str,
) -> EventBlockEnvelope:
  """Persist an event block and commit, optionally firing its handler.

  ``apply_handlers=False`` captures the row (``status='captured'``, no GL
  rows). ``apply_handlers=True`` resolves a handler (Python registry first,
  then the DSL ``event_handlers`` table) and fires it atomically with the
  row. Any validation failure persists nothing.

  Use :func:`create_event_block_in_session` when the event is one step of a
  larger unit of work.
  """
  # The keys write-back and the sync maintain; the in-session path is for
  # internal callers that set their own.
  _refuse_system_metadata(body.metadata)
  _event, envelope = create_event_block_in_session(
    session, body, created_by, graph_id=graph_id
  )
  session.commit()
  return envelope


def create_event_block_in_session(
  session: Session,
  body: CreateEventBlockRequest,
  created_by: str,
  *,
  graph_id: str,
) -> tuple[Event, EventBlockEnvelope]:
  """:func:`create_event_block` without the commit; returns row and envelope.

  The envelope is built here because ``commit()`` expires the instance, and
  reading attributes afterwards can raise on a write that succeeded.
  """
  _validate_event_source(body.source, graph_id)
  _validate_routed_connection(body.metadata, graph_id)
  _assert_not_duplicate(session, body)

  if body.apply_handlers:
    python_handler = get_python_handler(body.event_type)
    if python_handler is not None:
      try:
        typed_metadata = python_handler.metadata_schema.model_validate(body.metadata)
      except ValidationError as e:
        raise HandlerMetadataValidationError(
          f"event_type='{body.event_type}' metadata validation failed: {e}"
        )

      event = _build_event_row(body, created_by, status=python_handler.target_status)
      session.add(event)
      _flush_new_event(session, event, body)

      if body.dimension_ids:
        session.execute(
          event_dimensions.insert(),
          [{"event_id": event.id, "dimension_id": d} for d in body.dimension_ids],
        )

      python_handler.dispatch(session, event, typed_metadata, created_by)

      envelope = _to_envelope(event, body.dimension_ids)
      return event, envelope

    agent_type = resolve_agent_type(session, body.agent_id)
    handler = resolve_handler(
      session,
      event_type=body.event_type,
      event_category=body.event_category,
      source=body.source,
      agent_type=agent_type,
      resource_type=body.resource_type,
      metadata=body.metadata,
    )

    event = _build_event_row(body, created_by, status="classified")
    session.add(event)
    _flush_new_event(session, event, body)

    if body.dimension_ids:
      session.execute(
        event_dimensions.insert(),
        [{"event_id": event.id, "dimension_id": d} for d in body.dimension_ids],
      )

    apply_handler(session, event, handler, created_by=created_by)

    envelope = _to_envelope(event, body.dimension_ids)
    return event, envelope

  event = _build_event_row(body, created_by, status="captured")
  session.add(event)
  _flush_new_event(session, event, body)

  if body.dimension_ids:
    session.execute(
      event_dimensions.insert(),
      [{"event_id": event.id, "dimension_id": d} for d in body.dimension_ids],
    )

  envelope = _to_envelope(event, body.dimension_ids)
  return event, envelope


def fire_handler_on_commit(
  session: Session,
  event: Event,
  created_by: str,
) -> None:
  """Fire the event's Python handler against its captured metadata.

  Called on ``captured``/``classified`` → ``committed``. Event types with no
  Python handler are a silent no-op. Errors propagate so the transaction
  rolls back rather than leave a ``committed`` event with no GL rows.
  """
  python_handler = get_python_handler(event.event_type)
  if python_handler is None:
    return

  raw_metadata = dict(event.metadata_ or {})
  try:
    typed_metadata = python_handler.metadata_schema.model_validate(raw_metadata)
  except ValidationError as e:
    raise HandlerMetadataValidationError(
      f"Event {event.id} (type={event.event_type}): captured metadata fails "
      f"handler validation — cannot commit. {e}"
    )

  python_handler.dispatch(session, event, typed_metadata, created_by)


def _validate_classification(event: Event) -> None:
  """Let the event's Python handler refuse a ``classified`` it could not post.

  Runs after the metadata patch, so the handler sees the caller's choice.
  Handlers without the hook (most) accept the transition as before.
  """
  python_handler = get_python_handler(event.event_type)
  if python_handler is None or python_handler.validate_classification is None:
    return
  raw_metadata = dict(event.metadata_ or {})
  try:
    typed_metadata = python_handler.metadata_schema.model_validate(raw_metadata)
  except ValidationError as e:
    raise HandlerMetadataValidationError(
      f"Event {event.id} (type={event.event_type}): metadata fails handler "
      f"validation — cannot classify. {e}"
    )
  python_handler.validate_classification(event, typed_metadata)


def _refuse_system_metadata(patch: dict | None) -> None:
  """Written by write-back and the sync; a caller-supplied value would misstate
  what reached QuickBooks (a fake `qb_external_id` stops write-back). Applies
  to a create's metadata and an update's metadata_patch alike."""
  from .qb_writeback import QB_ENTRY_IDS_KEY

  system_keys = {
    QB_ENTRY_IDS_KEY,
    "qb_external_id",
    "routed_via",
    "last_outbound_error",
    "qb_sync_token",
    "drift_detected_at",
    "drift_payload",
    "reconciliation_history",
  }
  reserved = sorted(
    key for key in (patch or {}) if key in system_keys or key.startswith("dispatch_")
  )
  if reserved:
    raise InvalidEventTransitionError(
      f"metadata cannot set system-maintained keys: {', '.join(reserved)}."
    )


def _has_field_corrections(body: UpdateEventBlockRequest) -> bool:
  return any(
    value is not None
    for value in (
      body.description,
      body.effective_at,
      body.metadata_patch or None,
      body.obligated_by_event_id,
      body.discharges_event_id,
      body.event_action,
    )
  )


def update_event_block(
  session: Session,
  body: UpdateEventBlockRequest,
  created_by: str,
  *,
  graph_id: str,
) -> EventBlockEnvelope:
  """Apply a status transition and/or field corrections to an event block.

  ``captured``/``classified`` → ``committed`` fires the event's Python
  handler unless it already wrote rows at creation; handler errors roll back
  the whole update. ``captured → classified`` gives the handler a veto.
  """
  # The transition check is read-decide-write and several paths (inbox
  # approval, the sync's auto-commit) can advance the same event, so it is
  # decided under a row lock.
  session.flush()  # pairs with populate_existing below; autoflush is off here
  peek = session.get(Event, body.event_id)
  if peek is None:
    raise EventNotFoundError(f"Event not found: {body.event_id}")
  _refuse_system_metadata(body.metadata_patch)
  # Period fence before the event row lock, matching close's order. A commit
  # fences its current date; a re-date fences the date it moves to and the
  # rows it already wrote, but not the date it leaves, so an event with no rows
  # captured in a closed month stays movable out of it.
  fence_dates: set[date] = set()
  if body.transition_to == "committed":
    fence_dates.add(
      posting_date_for_event(
        effective_at=peek.effective_at,
        occurred_at=peek.occurred_at,
      )
    )
  if body.effective_at is not None:
    fence_dates.add(
      posting_date_for_event(
        effective_at=body.effective_at,
        occurred_at=peek.occurred_at,
      )
    )
    fence_dates.update(_retraction_fence_dates(session, peek.id))
  if fence_dates:
    assert_period_not_closed(session, *sorted(fence_dates))
  # Retraction fence (see `_assert_retractable`), on the rows' own posting
  # dates: an event with no ledger rows stays retractable in a closed period.
  if body.transition_to in _RETRACTED_STATUSES:
    retraction_dates = _retraction_fence_dates(session, peek.id)
    if retraction_dates:
      assert_period_not_closed(session, *retraction_dates)
  with bounded_lock_wait(
    session,
    f"Event {body.event_id} is being written by another process "
    "(most likely a running sync). Retry in a moment.",
  ):
    # Lock every row this operation writes (the supersede successor too) in
    # one ordered statement, so opposing supersedes cannot deadlock.
    # `populate_existing` so a reused session can't hold the lock over a
    # stale status.
    wanted = {body.event_id}
    if body.transition_to == "superseded" and body.superseded_by_id:
      wanted.add(body.superseded_by_id)
    locked = {
      row.id: row
      for row in session.query(Event)
      .filter(Event.id.in_(wanted))
      .order_by(ordered_lock_column())
      .populate_existing()
      .with_for_update()
      .all()
    }
  event = locked.get(body.event_id)
  if event is None:
    raise EventNotFoundError(f"Event not found: {body.event_id}")

  if event.status in _RETRACTED_STATUSES and _has_field_corrections(body):
    raise InvalidEventTransitionError(
      f"Event is {event.status}; its fields can no longer be corrected."
    )

  fire_handler = False
  if body.transition_to is not None:
    allowed = _VALID_TRANSITIONS.get(event.status, frozenset())
    if body.transition_to not in allowed:
      raise InvalidEventTransitionError(
        f"Cannot transition event from '{event.status}' to '{body.transition_to}'. "
        f"Allowed transitions: {sorted(allowed) if allowed else 'none (terminal state)'}."
      )

    if body.transition_to in _RETRACTED_STATUSES:
      _assert_retractable(session, event)

    if body.transition_to == "superseded":
      if body.superseded_by_id is None:
        raise InvalidEventTransitionError(
          "transition_to='superseded' requires superseded_by_id."
        )
      if body.superseded_by_id == event.id:
        raise InvalidEventTransitionError("An event cannot supersede itself.")
      successor = locked.get(body.superseded_by_id)
      if successor is None:
        raise EventNotFoundError(
          f"Superseding event not found: {body.superseded_by_id}"
        )
      # Set both sides of the correction chain atomically so the backward
      # link query ("which event does B replace?") resolves.
      event.replaced_by_event_id = successor.id
      successor.replaces_event_id = event.id

    # A handler that ran at creation already wrote its entry (often a draft,
    # arriving `classified`); firing again would duplicate it. Drafts must
    # count here.
    fire_handler = (
      body.transition_to == "committed"
      and event.status in ("captured", "classified")
      and not _has_linked_entries(session, str(event.id))
    )

    event.status = body.transition_to

  if body.description is not None:
    event.description = body.description

  if body.effective_at is not None:
    event.effective_at = body.effective_at

  if body.metadata_patch:
    _validate_routed_connection(body.metadata_patch, graph_id)
    merged = dict(event.metadata_ or {})
    merged.update(body.metadata_patch)
    event.metadata_ = merged

  if body.obligated_by_event_id is not None:
    event.obligated_by_event_id = body.obligated_by_event_id

  if body.discharges_event_id is not None:
    event.discharges_event_id = body.discharges_event_id

  if body.event_action is not None:
    event.event_action = body.event_action

  if body.transition_to == "classified":
    _validate_classification(event)

  if fire_handler:
    # After the metadata patch so the handler sees the final shape.
    fire_handler_on_commit(session, event, created_by)

  # Before commit, which expires `event`.
  envelope = _to_envelope(event, _load_dimension_ids(session, event.id))
  session.commit()
  return envelope


def _python_preview_to_response(
  preview, handler: EventBlockPythonHandler
) -> PreviewEventBlockResponse:
  """Map a Python HandlerPreview to the public PreviewEventBlockResponse shape.

  ``matched_handler`` is the DSL row shape and stays empty for a Python
  handler; the handler's name rides in ``handler_metadata`` instead so a
  reader can see that one matched.
  """

  def _line_element_ref(li: dict) -> str:
    # Each line carries exactly one of element_id or element_external_id.
    return li.get("element_id") or li.get("element_external_id") or ""

  planned: list[TransactionPreview] = []
  for entry_idx, entry in enumerate(preview.planned_entries):
    line_items = entry.get("line_items", [])
    # Multi-leg entries are summarized by their first debit and first credit.
    first_debit = next((li for li in line_items if li.get("debit_amount", 0) > 0), None)
    first_credit = next(
      (li for li in line_items if li.get("credit_amount", 0) > 0), None
    )
    if first_debit and first_credit:
      debit_amount = first_debit.get("debit_amount", 0)
      credit_amount = first_credit.get("credit_amount", 0)
      planned.append(
        TransactionPreview(
          entry_index=entry_idx,
          debit_element_id=_line_element_ref(first_debit),
          credit_element_id=_line_element_ref(first_credit),
          amount_cents=debit_amount,
          interpolated_debit_amount=str(debit_amount),
          interpolated_credit_amount=str(credit_amount),
        )
      )
  return PreviewEventBlockResponse(
    matched_handler=None,  # Python handlers aren't rows in event_handlers
    planned_transactions=planned,
    validation_errors=preview.validation_errors,
    would_succeed=preview.would_succeed,
    handler_metadata={
      "handler": handler.display_name,
      **(preview.computed_values or {}),
    },
  )


def preview_event_block(
  session: Session,
  body: CreateEventBlockRequest,
  created_by: str,
) -> PreviewEventBlockResponse:
  """Dry-run handler resolution and template evaluation; writes nothing."""
  from robosystems.operations.roboledger.reads.event_handler import handler_to_response

  python_handler = get_python_handler(body.event_type)
  if python_handler is not None:
    try:
      typed_metadata = python_handler.metadata_schema.model_validate(body.metadata)
    except ValidationError as e:
      return PreviewEventBlockResponse(
        matched_handler=None,
        planned_transactions=[],
        validation_errors=[f"metadata validation: {e}"],
        would_succeed=False,
      )
    preview = python_handler.dispatch_preview(session, body, typed_metadata)
    return _python_preview_to_response(preview, python_handler)

  errors: list[str] = []
  matched_handler_response = None
  planned: list[TransactionPreview] = []
  agent_type = resolve_agent_type(session, body.agent_id)

  try:
    handler = resolve_handler(
      session,
      event_type=body.event_type,
      event_category=body.event_category,
      source=body.source,
      agent_type=agent_type,
      resource_type=body.resource_type,
      metadata=body.metadata,
    )
    matched_handler_response = handler_to_response(handler)
  except HandlerNotFoundError as e:
    errors.append(str(e))
    return PreviewEventBlockResponse(
      matched_handler=None,
      planned_transactions=[],
      validation_errors=errors,
      would_succeed=False,
    )
  except HandlerAmbiguousError as e:
    errors.append(str(e))
    return PreviewEventBlockResponse(
      matched_handler=None,
      planned_transactions=[],
      validation_errors=errors,
      would_succeed=False,
    )

  try:
    assert_period_not_closed(
      session,
      posting_date_for_event(
        effective_at=body.effective_at,
        occurred_at=body.occurred_at,
      ),
    )
  except (ClosedPeriodError, RowLockedError) as e:
    errors.append(str(e))

  template = handler.transaction_template or {}
  event_ctx = {
    "id": "preview",
    "event_type": body.event_type,
    "event_category": body.event_category,
    "agent_id": body.agent_id,
    "resource_type": body.resource_type,
    "resource_element_id": body.resource_element_id,
    "occurred_at": str(body.occurred_at),
    "effective_at": str(body.effective_at) if body.effective_at else None,
    "source": body.source,
    "external_id": body.external_id,
    "amount": body.amount,
    "currency": body.currency,
    "description": body.description,
    "metadata": body.metadata,
  }
  context = {
    "event": event_ctx,
    "handler": build_handler_context(handler),
  }

  for i, entry_spec in enumerate(template.get("transactions", [])):
    et = entry_spec.get("entry_template", {})
    try:
      debit_amount_str = et.get("debit", {}).get("amount", "")
      credit_amount_str = et.get("credit", {}).get("amount", "")
      debit_raw = interpolate(debit_amount_str, context)
      credit_raw = interpolate(credit_amount_str, context)
      try:
        debit_cents = int(debit_raw)
        credit_cents = int(credit_raw)
      except (TypeError, ValueError):
        errors.append(
          f"Entry {i}: amount expression resolved to non-numeric value "
          f"(debit={debit_raw!r}, credit={credit_raw!r})"
        )
        continue

      if debit_cents != credit_cents:
        errors.append(
          f"Entry {i}: unbalanced — debit {debit_cents} ≠ credit {credit_cents}"
        )

      planned.append(
        TransactionPreview(
          entry_index=i,
          debit_element_id=et.get("debit", {}).get("element_id", ""),
          credit_element_id=et.get("credit", {}).get("element_id", ""),
          amount_cents=debit_cents,
          interpolated_debit_amount=str(debit_cents),
          interpolated_credit_amount=str(credit_cents),
        )
      )
    except (TemplateInterpolationError, EngineValidationError) as e:
      errors.append(f"Entry {i}: {e}")

  return PreviewEventBlockResponse(
    matched_handler=matched_handler_response,
    planned_transactions=planned,
    validation_errors=errors,
    would_succeed=len(errors) == 0,
  )


def execute_event_block(
  session: Session,
  body: ExecuteEventBlockRequest,
  created_by: str,
  *,
  graph_id: str,
  acquire_period_fence: bool = True,
  entry_ids: list[str] | None = None,
  qb_clients: dict[str, Any] | None = None,
) -> ExecuteEventBlockResponse:
  """Publish an event to its connection's source-of-truth system.

  ``entry_ids`` limits the publish to those draft entries; close passes the
  ones in the period it is closing, so an event whose entries span periods
  publishes each with its own period.

  A ``native`` connection (or none) is a no-op. Otherwise each unpublished
  draft Entry posts as its own QB JournalEntry, recorded in
  ``metadata.qb_entry_ids`` and promoted to ``posted``; once no draft
  remains the event goes ``fulfilled``. On rejection what landed is kept,
  ``metadata.last_outbound_error`` is stamped, and the event moves to
  ``pending`` where legal.

  Idempotent: each POST carries the entry id as its RequestId, and an entry
  already in ``qb_entry_ids`` is never re-posted. ``voided``/``superseded``
  raise :class:`EventNotPublishableError` before any external write.

  Close passes ``acquire_period_fence=False``: it already holds the
  exclusive fence, and taking the shared side would deadlock on it.

  ``qb_clients`` is a per-caller cache keyed by connection id. Close passes
  one dict for the whole run, so it builds one client (one token refresh)
  rather than one per entry; a failed build is cached and re-raised too.
  """
  # Local imports keep the QB SDK and platform DB out of create/update callers.
  from robosystems.adapters.quickbooks.client.api import QBAuthFailedError, QBClient
  from robosystems.database import SessionFactory as _PlatformSessionFactory
  from robosystems.models.core.connection.connection import Connection
  from robosystems.models.core.connection.connection_credentials import (
    ConnectionCredentials,
  )
  from robosystems.models.extensions.roboledger.entry import Entry
  from robosystems.models.extensions.roboledger.transaction import Transaction
  from robosystems.operations.roboledger.fiscal_calendar.qb_writeback import (
    PUBLISH_TO_SOURCE_KEY,
    WRITEBACK_EVENT_SOURCES,
  )

  from .qb_writeback import (
    QB_ENTRY_IDS_KEY,
    QBWritebackError,
    post_event_to_qb,
    published_entry_ids,
  )

  # Locked: a QB POST sits between this read and the status write, and a
  # concurrent void must not be overwritten by `fulfilled` afterwards.
  # `populate_existing` is required because close calls this on a shared
  # session that already loaded the event; the flush first keeps the re-read
  # from discarding unflushed changes (autoflush is off).
  session.flush()
  peek = session.get(Event, body.event_id)
  if peek is None:
    raise EventNotFoundError(f"Event {body.event_id} not found")
  if acquire_period_fence:
    assert_period_not_closed(
      session,
      posting_date_for_event(
        effective_at=peek.effective_at,
        occurred_at=peek.occurred_at,
      ),
    )
  with bounded_lock_wait(
    session,
    f"Event {body.event_id} is being written by another process. Retry in a moment.",
  ):
    event = (
      session.query(Event)
      .filter(Event.id == body.event_id)
      .populate_existing()
      .with_for_update()
      .first()
    )
  if event is None:
    raise EventNotFoundError(f"Event {body.event_id} not found")

  metadata = dict(event.metadata_ or {})
  existing_qb_id = metadata.get("qb_external_id")
  # Published before per-entry tracking: the whole event is in QuickBooks.
  if existing_qb_id and QB_ENTRY_IDS_KEY not in metadata:
    primary = str(existing_qb_id).split(",", 1)[0]
    return ExecuteEventBlockResponse(
      event_id=str(event.id),
      status=str(event.status),
      qb_external_id=primary,
      qb_error=None,
    )
  if event.status in _UNPUBLISHABLE_STATUSES:
    raise EventNotPublishableError(str(event.id), str(event.status))
  if event.status == "fulfilled":
    return ExecuteEventBlockResponse(
      event_id=str(event.id),
      status="fulfilled",
      qb_external_id=None,
      qb_error=None,
    )
  # Same rule as close's `writeback_source_clause`: an explicit
  # publish_to_source decides, else only RL-originated sources publish. It
  # outranks the caller's connection, or a synced-in QB event would be sent
  # back to QB.
  publish_flag = metadata.get(PUBLISH_TO_SOURCE_KEY)
  publishes = (
    publish_flag is True
    if publish_flag is not None
    else event.source in WRITEBACK_EVENT_SOURCES
  )
  if not publishes:
    logger.debug(
      f"Event {event.id} (source={event.source}, publish_to_source="
      f"{publish_flag}) stays local — no QB write."
    )
    return ExecuteEventBlockResponse(
      event_id=str(event.id),
      status=str(event.status),
      qb_external_id=None,
      qb_error=None,
    )

  # Caller can override the connection (used by the close-period batch
  # where schedule events don't carry connection_id in metadata).
  connection_id = body.connection_id or metadata.get("connection_id")

  if not connection_id:
    logger.debug(
      f"Event {event.id} has no connection_id in metadata — native path, no QB write."
    )
    return ExecuteEventBlockResponse(
      event_id=str(event.id),
      status=str(event.status),
      qb_external_id=None,
      qb_error=None,
    )

  with _PlatformSessionFactory() as platform_session:
    connection = Connection.get_by_id(connection_id, platform_session)
    if connection is None:
      logger.warning(
        f"Event {event.id} references connection {connection_id} which is "
        f"missing or soft-deleted — skipping QB write."
      )
      return ExecuteEventBlockResponse(
        event_id=str(event.id),
        status=str(event.status),
        qb_external_id=None,
        qb_error=None,
      )
    # The id came from the request or from client-editable metadata; only a
    # connection registered on *this* graph may be published to.
    if str(connection.graph_id) != str(graph_id):
      raise ConnectionNotOnGraphError(
        f"Connection {connection_id!r} is not registered on this graph."
      )

    if connection.write_policy == "native":
      return ExecuteEventBlockResponse(
        event_id=str(event.id),
        status=str(event.status),
        qb_external_id=None,
        qb_error=None,
      )

    if connection.provider != "quickbooks":
      logger.debug(
        f"Event {event.id} on non-QB provider {connection.provider} — "
        f"write-back not implemented; status unchanged."
      )
      return ExecuteEventBlockResponse(
        event_id=str(event.id),
        status=str(event.status),
        qb_external_id=None,
        qb_error=None,
      )

    cred = ConnectionCredentials.get_by_connection_id(connection_id, platform_session)
    if cred is None:
      raise ValueError(
        f"Connection {connection_id} has no credentials — cannot write to QB."
      )
    realm_id = connection.realm_id
    credentials = cred.get_credentials()

  if not realm_id:
    raise ValueError(
      f"Connection {connection_id} has no realm_id — cannot write to QB."
    )

  # Constructing QBClient runs the auth path (persists rotated tokens, flags
  # needs_reauth on auth failure).
  qb_client = qb_clients.get(str(connection_id)) if qb_clients is not None else None
  if isinstance(qb_client, QBAuthFailedError):
    # This run already failed to authenticate; asking Intuit again per entry
    # would repeat the same refresh (and, for invalid_grant, a dead token).
    # Fresh traceback: re-raising one object keeps growing its frames.
    raise qb_client.with_traceback(None)
  if qb_client is None:
    try:
      qb_client = QBClient(
        realm_id=str(realm_id),
        qb_credentials=credentials,
        connection_id=str(connection_id),
      )
    except QBAuthFailedError as e:
      if qb_clients is not None:
        qb_clients[str(connection_id)] = e
      raise
    if qb_clients is not None:
      qb_clients[str(connection_id)] = qb_client

  # Only ledger rows publish; without them QB would get an entry the ledger
  # never holds.
  has_rows = (
    session.query(Entry.id).filter(Entry.triggered_by_event_id == event.id).first()
    is not None
  )
  if not has_rows:
    raise EventNotPublishableError(
      str(event.id),
      str(event.status),
      reason="it has no drafted ledger entries; commit it first",
    )

  error_payload: dict[str, Any] | None = None
  try:
    newly_published = post_event_to_qb(
      session, event, qb_client.client, entry_ids=entry_ids
    )
  except QBWritebackError as e:
    newly_published = e.published
    error_payload = e.payload

  now = datetime.now(UTC)
  published = {**published_entry_ids(event.metadata_), **newly_published}
  new_meta = dict(event.metadata_ or {})
  if published:
    new_meta[QB_ENTRY_IDS_KEY] = published
    # Comma-joined for the cross-source matcher in the extensions loader,
    # which compares incoming external_ids against this key.
    new_meta["qb_external_id"] = ",".join(published.values())
  if newly_published:
    new_meta["routed_via"] = {
      "connection_id": str(connection_id),
      "sent_at": now.isoformat(),
    }
    session.query(Entry).filter(Entry.id.in_(list(newly_published))).update(
      {Entry.status: "posted", Entry.posted_at: now},
      synchronize_session=False,
    )

  if error_payload is not None:
    new_meta["last_outbound_error"] = error_payload
    event.metadata_ = new_meta
    if "pending" in _VALID_TRANSITIONS.get(str(event.status), frozenset()):
      event.status = "pending"
    session.flush()
    return ExecuteEventBlockResponse(
      event_id=str(event.id),
      status=str(event.status),
      qb_external_id=None,
      qb_entry_ids=published or None,
      qb_error=error_payload,
    )

  new_meta.pop("last_outbound_error", None)
  event.metadata_ = new_meta

  # Fulfilled once no draft of the event is left to publish; an entry in a
  # later period keeps it open until that period closes.
  remaining = (
    session.query(Entry.id)
    .filter(Entry.triggered_by_event_id == event.id, Entry.status == "draft")
    .first()
  )
  if remaining is None:
    event.status = "fulfilled"
    session.query(Transaction).filter(
      Transaction.triggered_by_event_id == event.id
    ).update(
      {Transaction.status: "posted", Transaction.posted_at: now},
      synchronize_session=False,
    )

  session.flush()
  logger.info(
    f"Event {event.id} published to QB via connection {connection_id}: "
    f"{len(newly_published)} entr(y/ies), status={event.status}"
  )

  return ExecuteEventBlockResponse(
    event_id=str(event.id),
    status=str(event.status),
    qb_external_id=next(iter(newly_published.values()), None),
    qb_entry_ids=published or None,
    qb_error=None,
  )
