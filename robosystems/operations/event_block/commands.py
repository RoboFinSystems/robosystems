"""Event Block commands — create, update, and preview operations.

Functions are pure: ``(Session, RequestModel, created_by) → ResponseModel``.

- ``create_event_block``: persists an event row. With ``apply_handlers=False``
  the row lands in ``status='captured'`` and nothing else is written. With
  ``apply_handlers=True`` the event is resolved against the Python registry
  first, falling back to the DSL registry; the matched handler fires and
  produces GL rows atomically with the event row.
- ``update_event_block``: applies status transitions and field corrections to
  an existing event.
- ``preview_event_block``: dry-runs handler resolution + template evaluation
  and returns the plan without persisting anything.
"""

from __future__ import annotations

from datetime import UTC, date, datetime

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
from robosystems.models.extensions.roboledger.agent import Agent
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
from robosystems.operations.roboledger.reads.event_block import (
  _load_dimension_ids,
  _to_envelope,
)

from .engine import EngineValidationError, apply_handler, posting_date_for_event
from .python_handlers import get_python_handler
from .python_handlers.types import HandlerMetadataValidationError
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

  def __init__(self, event_id: str, status: str) -> None:
    super().__init__(f"Event {event_id} is {status!r} and cannot be published.")
    self.event_id = event_id
    self.status = status


class EventEffectsAlreadyLandedError(Exception):
  """Raised when a retraction would orphan effects the books already hold.

  Retracting an event drops it from `is_live`, which is what materialization,
  QuickBooks write-back, and every reporting read filter on. That is the right
  answer while the event's ledger rows are still drafts — nothing has hit the
  books, so removing the event removes the whole story. It is the wrong answer
  once a row has posted or the event has published to QuickBooks: the effect
  stays where it landed while the event that explains it disappears, leaving a
  GL balance no query can attribute and a QuickBooks entry with no local
  counterpart.

  Past that line the correction is a reversal, not a retraction — the same
  split `JournalEntryNotDraftError` draws for entries.
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

  `idx_events_source_external` is the deduplication key for external
  integrations: it is what makes re-delivering the same upstream record safe.
  Without this check the insert reached the database and the IntegrityError
  escaped as an opaque 500, so a connector retrying a delivery — or a demo
  re-run — could not tell "already ingested" from a real fault, which is the
  one distinction the index exists to provide.
  """

  def __init__(self, source: str, external_id: str) -> None:
    super().__init__(
      f"Event already exists for source={source} external_id={external_id}"
    )
    self.source = source
    self.external_id = external_id


# Valid outbound transitions from each status.
# `voided` and `superseded` are the retracted states and have empty sets — a
# retraction is final. `superseded` is reachable from every other state so
# corrections can replace an event regardless of how far it has progressed.
#
# `fulfilled` is the *end of the work*, not the end of the record. A handler
# that targets it (`asset_disposed`, `journal_entry_reversed`) lands the event
# there with its ledger rows still `draft`, because drafts post at close. That
# left the pair unretractable: `delete_journal_entry` refuses to delete the
# last draft of a live event and tells the caller to void or supersede it,
# while an empty transition set made that impossible — so a disposal draft
# could never be discarded, only posted and then reversed.
#
# What gates a retraction is `_assert_retractable`, which asks whether the
# event's rows have landed — never which status it is being retracted from.
# The table decides reachability; the guard decides safety.
_VALID_TRANSITIONS: dict[str, frozenset[str]] = {
  "captured": frozenset({"committed", "voided", "superseded"}),
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

# Entry/Transaction statuses that mean the effect is in the books to stay.
# `reversed` counts: the original posted, and a reversing entry stands against
# it. Retracting the event would strand both halves of that pair.
_LANDED_ENTRY_STATUSES = frozenset({"posted", "reversed"})
_LANDED_TRANSACTION_STATUSES = frozenset({"posted"})


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

  Consulted for every retraction, not just from `fulfilled`. An earlier
  status is not evidence that nothing is behind the event: `classified` and
  `committed` handlers (`schedule_entry_due`, `journal_entry_recorded`,
  `schedule_created`) write their ledger rows when they fire, and close
  promotes any draft in the period whose event is not already retracted —
  it filters on `voided`/`superseded`, not on `fulfilled`. So a `committed`
  event can sit across a close, collect posted entries, and reach here
  looking retractable. What decides is whether anything landed, which is
  what this reads; the status never did.

  Correctness depends on the caller having taken the shared period fence
  (`_retraction_fence_dates` → `assert_period_not_closed`) *before* the
  event row lock. The row lock alone is not enough: close promotes drafts
  with a bulk `UPDATE` over `entries` filtered by posting date, and never
  touches the owning `Event` row — so it does not contend on that lock, and
  a count taken under it can read zero while close is mid-flight. The fence
  is the only thing that serializes the two.
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


# Execute must not post these. A repeat execute of an already-``fulfilled``
# event is an idempotent no-op instead (see ``qb_external_id`` / status
# checks in ``execute_event_block``), so it stays out of this set.
_UNPUBLISHABLE_STATUSES = frozenset({"voided", "superseded"})


def _resolve_agent_type(session: Session, agent_id: str | None) -> str | None:
  """Load the counterparty's agent_type for DSL handler matching."""
  if agent_id is None:
    return None
  agent = session.get(Agent, agent_id)
  if agent is None:
    return None
  return agent.agent_type


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
  """Flush a freshly added Event, translating the partial unique index on
  ``(source, external_id)`` into ``DuplicateEventError``.

  ``_assert_not_duplicate`` ran first, but two identical posts racing past
  it both reach the insert; the index is the truth and its answer is the
  same one the pre-check gives — not a 500.
  """
  try:
    session.flush()
  except IntegrityError as exc:
    if body.external_id and violates(exc, "idx_events_source_external"):
      raise DuplicateEventError(body.source, body.external_id) from exc
    raise


class ConnectionNotOnGraphError(ValueError):
  """A ``connection_id`` that is not one of this graph's connections.

  Connection ids are platform-wide, so an id alone says nothing about which
  tenant owns it. Every place an event names a connection — the routing
  ``metadata.connection_id`` at capture, a patch to it, the override on
  execute — must resolve to a connection registered on the *calling* graph,
  or the publish would post into (and refresh the tokens of) another
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

  Registered means a live platform Connection whose ``provider`` matches
  (adapter sources: quickbooks/xero/plaid) or an ``external`` connection
  whose ``source_name`` matches. Static sources short-circuit before any
  platform-DB access. A platform-DB fault here surfaces through the
  registrar's generic error path, not as a validation failure.
  """
  if source in _STATIC_EVENT_SOURCES:
    return

  # Platform-DB lookup — local imports for the same reason as
  # `execute_event_block`: capture/preview callers shouldn't pull the
  # platform DB into their import graph.
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
  """Persist an event block, optionally firing the handler engine.

  ``apply_handlers=False``: capture-only — persists the event row in
  ``status='captured'`` and writes no GL rows.
  ``apply_handlers=True``: resolves the event_type to a handler and fires
  it atomically alongside the event row.

  Handler resolution order:
    1. Python registry (hub-defined complex workflows, e.g., asset_disposed)
    2. DSL registry (event_handlers table, tenant-configurable simple templates)

  ``body.source`` must be platform-emitted or registered on the graph
  (``_validate_event_source``) — validated before anything is persisted.

  On validation failure (no handler, ambiguous, template error, engine error)
  nothing is persisted — the exception propagates and the caller's session
  rolls back.

  Commits. An operation that creates an event as one step of a larger unit
  of work wants :func:`create_event_block_in_session` instead, so a later
  failure takes the event down with it.
  """
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
  """Persist an event block without committing; return the row and envelope.

  The body of :func:`create_event_block`, minus the commit, for callers
  that compose event creation with other writes in one transaction —
  ``resolve_reconciling_item`` posts a catch-up entry and clears the
  originating event's flag together, and a half-applied version of that
  pair is a worse state than either whole.

  The envelope is built before the caller commits for the reason the
  committing wrapper documents: ``commit()`` expires the instance, so
  reading attributes afterwards can raise on a write that succeeded.
  """
  _validate_event_source(body.source, graph_id)
  _validate_routed_connection(body.metadata, graph_id)
  _assert_not_duplicate(session, body)

  if body.apply_handlers:
    # 1. Python registry wins over DSL registry
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

      # Handler executes; errors roll back the whole unit of work.
      python_handler.dispatch(session, event, typed_metadata, created_by)

      # Read the row into the envelope BEFORE committing. `commit()` expires
      # every attribute on the instance, so a later `event.id` triggers a
      # reload — and if that reload comes back empty the ORM raises
      # ObjectDeletedError, which surfaces as a 500 for a write that already
      # succeeded. That is the worst failure shape available: the caller is
      # told the write failed, so a retry duplicates a committed event.
      envelope = _to_envelope(event, body.dimension_ids)
      return event, envelope

    # 2. Fall through to the DSL registry
    agent_type = _resolve_agent_type(session, body.agent_id)
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

    # Fire handler — flushes Transaction + Entry + LineItems inside
    apply_handler(session, event, handler, created_by=created_by)

    # Envelope before commit — see the python-handler path above.
    envelope = _to_envelope(event, body.dimension_ids)
    return event, envelope

  # Capture-only path (apply_handlers=False)
  event = _build_event_row(body, created_by, status="captured")
  session.add(event)
  _flush_new_event(session, event, body)

  if body.dimension_ids:
    session.execute(
      event_dimensions.insert(),
      [{"event_id": event.id, "dimension_id": d} for d in body.dimension_ids],
    )

  # Envelope before commit — see the python-handler path above.
  envelope = _to_envelope(event, body.dimension_ids)
  return event, envelope


def fire_handler_on_commit(
  session: Session,
  event: Event,
  created_by: str,
) -> None:
  """Resolve and fire a Python handler against the captured event metadata.

  Called when an event transitions to ``committed`` from a pre-handler
  state (``captured`` or ``classified``). For events whose ``event_type``
  has a registered Python handler, this validates the captured metadata
  against the handler's schema and dispatches — producing the GL rows
  the handler is responsible for. Events whose ``event_type`` has no
  handler fall through silently (e.g., support events with no GL
  impact, or future event types not yet wired).

  Errors propagate to the caller so the surrounding transaction rolls
  back — a bad approval cannot leave the event in ``committed`` with
  no GL rows behind it.

  Public (no underscore) because the loader's auto-commit path also
  calls this from outside this module.
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


def update_event_block(
  session: Session,
  body: UpdateEventBlockRequest,
  created_by: str,
  *,
  graph_id: str,
) -> EventBlockEnvelope:
  """Apply a status transition and/or field corrections to an event block.

  When the requested transition is ``captured → committed`` or
  ``classified → committed``, the event's Python handler (if any) fires
  against the captured metadata to produce the corresponding GL rows.
  Handler errors roll back the entire update, including the status
  change — a failed commit leaves the event in its pre-approval state.
  """
  # Lock the row for the life of the transaction. The transition check below
  # is read-decide-write, and the decision is only sound if nothing else can
  # move the event in between. More than one path can advance the same event —
  # inbox approval and the sync's auto-commit pass (`extensions/loader.py`,
  # which locks its batch for the same reason) — and each fires the handler
  # once, so the transition must be decided under the lock. Under READ
  # COMMITTED the blocked reader re-reads the committed row once the lock
  # releases, sees the new status, and raises InvalidEventTransitionError as
  # it should.
  #
  # Bounded, because the conflicting writer is usually a sync or a promotion
  # sweep that runs long — see `locking.bounded_lock_wait`.
  session.flush()  # pairs with populate_existing below; autoflush is off here
  peek = session.get(Event, body.event_id)
  if peek is None:
    raise EventNotFoundError(f"Event not found: {body.event_id}")
  # Fence before the event row lock — same order as journal update/delete
  # and as close's exclusive fence. Approving (event lock, then handler
  # fence) against a closer (exclusive fence, then event lock) used to
  # sit until lock_timeout and fail close mid-publish. Both the current
  # posting date and the one ``body.effective_at`` would move it to, as
  # ``update_journal_entry`` does — the handler posts against the patched
  # date, and its own fence runs after the lock.
  if body.transition_to == "committed":
    fence_dates = {
      posting_date_for_event(
        effective_at=peek.effective_at,
        occurred_at=peek.occurred_at,
      )
    }
    if body.effective_at is not None:
      fence_dates.add(
        posting_date_for_event(
          effective_at=body.effective_at,
          occurred_at=peek.occurred_at,
        )
      )
    assert_period_not_closed(session, *sorted(fence_dates))
  # A retraction drops the event from `is_live` while close is separately
  # promoting its drafts to posted. Close excludes only events already
  # retracted, and reaches the entries through a bulk `UPDATE` that never
  # locks the `Event` row — so nothing but this fence orders the two, and
  # without it the guard below can read zero posted rows in the window where
  # close is about to post them. Fenced on the rows' own posting dates, not
  # the event's: an event with no ledger rows has nothing to strand and
  # stays retractable even where its date falls in a closed period.
  if body.transition_to in _RETRACTED_STATUSES:
    retraction_dates = _retraction_fence_dates(session, peek.id)
    if retraction_dates:
      assert_period_not_closed(session, *retraction_dates)
  with bounded_lock_wait(
    session,
    f"Event {body.event_id} is being written by another process "
    "(most likely a running sync). Retry in a moment.",
  ):
    # Lock **every row this operation will write**, in one ordered statement.
    # The supersede path mutates the successor as well, so taking only this
    # event's lock leaves two callers superseding each other in opposite
    # directions (A←B here, B←A there) each holding what the other needs, and
    # the resulting deadlock surfaces at commit rather than here. Ordering by
    # `ordered_lock_column()` makes that cycle impossible instead of
    # translating it afterwards.
    #
    # `populate_existing` for the same reason as `execute_event_block`: every
    # production caller opens a fresh session per request, so the identity map
    # is empty today, but a caller that reused a session would otherwise get
    # the lock and a stale status — the one combination this guard exists to
    # prevent. The flush that pairs with it is there too, one line up.
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

  fire_handler = False
  if body.transition_to is not None:
    allowed = _VALID_TRANSITIONS.get(event.status, frozenset())
    if body.transition_to not in allowed:
      raise InvalidEventTransitionError(
        f"Cannot transition event from '{event.status}' to '{body.transition_to}'. "
        f"Allowed transitions: {sorted(allowed) if allowed else 'none (terminal state)'}."
      )

    # A retraction is allowed only while nothing the event wrote has landed —
    # see `_assert_retractable`, which also explains why the status it is
    # retracted *from* does not narrow this. Runs under the row lock taken
    # above and the period fence taken before it, ahead of any field write.
    if body.transition_to in _RETRACTED_STATUSES:
      _assert_retractable(session, event)

    if body.transition_to == "superseded":
      if body.superseded_by_id is None:
        raise InvalidEventTransitionError(
          "transition_to='superseded' requires superseded_by_id."
        )
      if body.superseded_by_id == event.id:
        raise InvalidEventTransitionError("An event cannot supersede itself.")
      # Locked above alongside `event`, in id order.
      successor = locked.get(body.superseded_by_id)
      if successor is None:
        raise EventNotFoundError(
          f"Superseding event not found: {body.superseded_by_id}"
        )
      # Set both sides of the correction chain atomically so the backward
      # link query ("which event does B replace?") resolves.
      event.replaced_by_event_id = successor.id
      successor.replaces_event_id = event.id

    fire_handler = body.transition_to == "committed" and event.status in (
      "captured",
      "classified",
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

  if fire_handler:
    # Handler runs after metadata patches so it sees the final shape.
    # Errors propagate; the surrounding transaction rolls back.
    fire_handler_on_commit(session, event, created_by)

  # Envelope before commit — see `create_event_block`. The dimension read has
  # to happen here too: it reads `event.id`, which is expired by the commit.
  envelope = _to_envelope(event, _load_dimension_ids(session, event.id))
  session.commit()
  return envelope


def _python_preview_to_response(preview) -> PreviewEventBlockResponse:
  """Map a Python HandlerPreview to the public PreviewEventBlockResponse shape."""

  def _line_element_ref(li: dict) -> str:
    # The metadata schema requires exactly one of element_id or
    # element_external_id per line. QB-captured lines carry only
    # element_external_id; manual/native lines may carry element_id
    # already. Either is human-meaningful for preview display.
    return li.get("element_id") or li.get("element_external_id") or ""

  planned: list[TransactionPreview] = []
  for entry_idx, entry in enumerate(preview.planned_entries):
    line_items = entry.get("line_items", [])
    # Python handlers emit multi-leg entries; expose them as a simple list
    # with (entry_index, debit_element_id, credit_element_id) where possible.
    # For entries with >2 legs (disposal), pick the first debit + first credit
    # pair as a summary — full detail is available in handler_metadata /
    # the echoed planned_entries via the underlying preview data.
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
    handler_metadata=preview.computed_values,
  )


def preview_event_block(
  session: Session,
  body: CreateEventBlockRequest,
  created_by: str,
) -> PreviewEventBlockResponse:
  """Dry-run: resolve handler + evaluate template, return plan without persisting.

  Python registry wins over DSL registry, same as create_event_block.
  No rows are written.
  """
  from robosystems.operations.roboledger.reads.event_handler import handler_to_response

  # 1. Python registry
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
    return _python_preview_to_response(preview)

  # 2. DSL registry fallback
  errors: list[str] = []
  matched_handler_response = None
  planned: list[TransactionPreview] = []
  agent_type = _resolve_agent_type(session, body.agent_id)

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

  # Dry-evaluate template without writing
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


# ───────────────────────────────────────────────────────────────────────────
# execute-event-block — publish to source-of-truth
# ───────────────────────────────────────────────────────────────────────────


def execute_event_block(
  session: Session,
  body: ExecuteEventBlockRequest,
  created_by: str,
  *,
  graph_id: str,
  acquire_period_fence: bool = True,
) -> ExecuteEventBlockResponse:
  """Publish an event to its connection's source-of-truth system.

  Flow:
  1. Load Event by id. Read `metadata.connection_id`.
  2. Resolve the connection's `write_policy` from the platform DB.
  3. If `'native'`: fast-path return — no QB write, status unchanged.
  4. If `'qb_authoritative'` / `'hybrid'`:
     - Build a `quickbooks.objects.JournalEntry` from `event.metadata`
       via `qb_writeback.post_event_to_qb`.
     - On success: stamp `metadata.qb_external_id`,
       `metadata.routed_via`, transition status to `'fulfilled'` (QB
       acknowledged synchronously), promote linked draft Entry +
       Transaction rows to `'posted'`.
     - On rejection (`QBWritebackError`): stamp
       `metadata.last_outbound_error`, transition status to
       `'pending'`. Drafts stay draft for retry.

  Idempotency: the QB POST carries `request_id=event.id`. QB's
  ~5-minute RequestId dedup window means our retry-after-network-blip
  path is safe at the API layer. An event that already carries
  ``qb_external_id`` (or is already ``fulfilled``) is returned as-is.
  ``voided`` / ``superseded`` raise :class:`EventNotPublishableError`
  before any external write.

  ``acquire_period_fence`` is the request-facing default. Close already
  holds the exclusive fence on a dedicated connection; taking the
  shared side here would wait on that exclusive lock and time out.
  """
  # Local imports to avoid pulling QB SDK + platform-DB session into
  # callers that only need create/update/preview.
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
  )

  from .qb_writeback import QBWritebackError, post_event_to_qb

  # Locked, and for a sharper reason than the other paths: between this read
  # and the status write below sits a POST to QuickBooks. A concurrent void or
  # supersede would otherwise be clobbered by the `fulfilled`/`pending` stamp
  # *after* the external write already happened — QB's request_id dedup covers
  # a repeated post, not an event that was voided and published anyway.
  #
  # `populate_existing` is load-bearing, not belt-and-braces: close_service's
  # QB pre-publish loop calls this on a **shared** session that already loaded
  # these events, so without it the lock would be taken while `event.status`
  # kept the value it held before blocking.
  #
  # The flush pairs with it and is not optional. This factory is
  # `autoflush=False` (`db/extensions.py`), so a re-read would otherwise
  # overwrite any in-flight change to this row that the caller had not yet
  # written — silently, since the discarded value simply stops existing. Flush
  # first and the re-read returns our own pending write along with anything
  # another transaction committed. No caller has pending event writes here
  # today; this makes that a property of the function rather than of its
  # callers.
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
  if existing_qb_id:
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
  # An explicit publish_to_source=False pins the event to the local lane,
  # and outranks the caller-supplied connection: close passes its
  # writeback connection to every event in the batch, so checking this
  # after resolving connection_id would publish the very entries the flag
  # exists to hold back.
  if metadata.get(PUBLISH_TO_SOURCE_KEY) is False:
    logger.debug(
      f"Event {event.id} carries publish_to_source=False — local lane, no QB write."
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

  # Native fast-path: event isn't bound to a source-of-truth connection
  # OR the connection's policy is native (RoboSystems is system of
  # record). No QB write fires.
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

  # Platform-DB lookup for the connection's write_policy + credentials.
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
      # Only QB write-back is implemented. Other providers fast-path.
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

  # Instantiating QBClient runs the auth path: rotated tokens get persisted,
  # AuthClientError flips the connection to needs_reauth, and transient
  # errors raise QBAuthFailedError.
  try:
    qb_client = QBClient(
      realm_id=str(realm_id),
      qb_credentials=credentials,
      connection_id=str(connection_id),
    )
  except QBAuthFailedError:
    # Surface to the caller — the operator must reconnect via OAuth.
    raise

  # Build + post. On QB rejection, stamp the error onto event.metadata
  # and transition status='pending' for retry without raising.
  try:
    qb_txn_ids = post_event_to_qb(session, event, qb_client.client)
  except QBWritebackError as e:
    new_meta = dict(event.metadata_ or {})
    new_meta["last_outbound_error"] = e.payload
    event.metadata_ = new_meta
    event.status = "pending"
    session.flush()
    return ExecuteEventBlockResponse(
      event_id=str(event.id),
      status="pending",
      qb_external_id=None,
      qb_error=e.payload,
    )

  # Success path. Multi-entry events get a list of qb_txn_ids; flatten
  # to a single string (comma-joined) for the metadata stamp and
  # take the first for the response field — the multi-entry case is
  # rare today (QB ingest only) and the comma-joined form preserves
  # the round-trip mapping for the cross-source matcher.
  primary_qb_id = qb_txn_ids[0]
  joined_qb_id = ",".join(qb_txn_ids)
  new_meta = dict(event.metadata_ or {})
  new_meta["qb_external_id"] = joined_qb_id
  new_meta["routed_via"] = {
    "connection_id": str(connection_id),
    "qb_request_id": str(event.id),
    "sent_at": datetime.now(UTC).isoformat(),
  }
  # Clear any prior error from a retry succeeding.
  new_meta.pop("last_outbound_error", None)
  event.metadata_ = new_meta
  event.status = "fulfilled"

  # Promote linked draft Entry + Transaction rows to posted. The split
  # between Event lifecycle and Entry/Transaction status means we promote
  # ledger-row status here (not in the handler — the handler stamps
  # Entry.status='draft' at create, and execute promotes after QB
  # confirms).
  session.query(Entry).filter(Entry.triggered_by_event_id == event.id).update(
    {Entry.status: "posted", Entry.posted_at: datetime.now(UTC)},
    synchronize_session=False,
  )
  session.query(Transaction).filter(
    Transaction.triggered_by_event_id == event.id
  ).update(
    {Transaction.status: "posted", Transaction.posted_at: datetime.now(UTC)},
    synchronize_session=False,
  )

  session.flush()
  logger.info(
    f"Event {event.id} published to QB via connection {connection_id}: "
    f"qb_external_id={joined_qb_id}, status=fulfilled, "
    f"drafts promoted"
  )

  return ExecuteEventBlockResponse(
    event_id=str(event.id),
    status="fulfilled",
    qb_external_id=primary_qb_id,
    qb_error=None,
  )
