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

from datetime import UTC, datetime

from pydantic import ValidationError
from sqlalchemy import text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

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
from robosystems.models.extensions.roboledger.event import Event
from robosystems.operations.roboledger.reads.event_block import (
  _load_dimension_ids,
  _to_envelope,
)

from .engine import EngineValidationError, apply_handler
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

# How long an approval waits for a conflicting writer before giving up. Long
# enough to absorb another approval or a short write — those resolve in
# milliseconds — and short enough that a multi-minute sync returns an error
# instead of holding the connection. Postgres raises SQLSTATE 55P03 on expiry,
# the same code `NOWAIT` raises, so one handler covers both if we ever switch.
_LOCK_TIMEOUT_MS = 3000
_LOCK_NOT_AVAILABLE = "55P03"


class EventNotFoundError(Exception):
  pass


class InvalidEventTransitionError(Exception):
  pass


class EventLockedError(Exception):
  """Raised when the event row is held by a concurrent writer.

  In practice that writer is a running sync: `_capture_transactions_as_events`
  locks its whole batch for the life of the sync transaction, which can be
  minutes. Waiting it out would pin an HTTP request and its pooled connection
  for that whole time, and enough concurrent approvals would exhaust the
  extensions pool. Failing fast with a retryable error is the better trade.
  """


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
# Terminal states (fulfilled, voided, superseded) have empty sets — no further
# transitions allowed. `superseded` is reachable from any non-terminal state
# so corrections can replace an event regardless of how far it has progressed.
_VALID_TRANSITIONS: dict[str, frozenset[str]] = {
  "captured": frozenset({"committed", "voided", "superseded"}),
  "classified": frozenset(
    {"committed", "pending", "fulfilled", "voided", "superseded"}
  ),
  "committed": frozenset({"pending", "fulfilled", "voided", "superseded"}),
  "pending": frozenset({"fulfilled", "voided", "superseded"}),
  "fulfilled": frozenset(),
  "voided": frozenset(),
  "superseded": frozenset(),
}


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
  """
  _validate_event_source(body.source, graph_id)
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
      session.flush()

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
      session.commit()
      return envelope

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
    session.flush()

    if body.dimension_ids:
      session.execute(
        event_dimensions.insert(),
        [{"event_id": event.id, "dimension_id": d} for d in body.dimension_ids],
      )

    # Fire handler — flushes Transaction + Entry + LineItems inside
    apply_handler(session, event, handler, created_by=created_by)

    # Envelope before commit — see the python-handler path above.
    envelope = _to_envelope(event, body.dimension_ids)
    session.commit()
    return envelope

  # Capture-only path (apply_handlers=False)
  event = _build_event_row(body, created_by, status="captured")
  session.add(event)
  session.flush()

  if body.dimension_ids:
    session.execute(
      event_dimensions.insert(),
      [{"event_id": event.id, "dimension_id": d} for d in body.dimension_ids],
    )

  # Envelope before commit — see the python-handler path above.
  envelope = _to_envelope(event, body.dimension_ids)
  session.commit()
  return envelope


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
  # move the event in between. The live race is an inbox approval against the
  # sync's auto-commit pass (`extensions/loader.py`, which locks its batch for
  # the same reason): both read `captured`, both fire the handler, and the
  # event ends up with two sets of GL rows — a ledger that still foots and is
  # still wrong. Under READ COMMITTED the blocked reader re-reads the committed
  # row once the lock releases, sees the new status, and raises
  # InvalidEventTransitionError as it should.
  #
  # Bounded, because the conflicting writer is usually a sync that runs for
  # minutes — see EventLockedError. `SET LOCAL` reverts at transaction end.
  session.execute(text(f"SET LOCAL lock_timeout = '{_LOCK_TIMEOUT_MS}ms'"))
  try:
    event = session.get(Event, body.event_id, with_for_update=True)
  except OperationalError as exc:
    if getattr(exc.orig, "pgcode", None) == _LOCK_NOT_AVAILABLE:
      raise EventLockedError(
        f"Event {body.event_id} is being written by another process "
        "(most likely a running sync). Retry in a moment."
      ) from exc
    raise
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

    if body.transition_to == "superseded":
      if body.superseded_by_id is None:
        raise InvalidEventTransitionError(
          "transition_to='superseded' requires superseded_by_id."
        )
      if body.superseded_by_id == event.id:
        raise InvalidEventTransitionError("An event cannot supersede itself.")
      successor = session.get(Event, body.superseded_by_id)
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
  path is safe at the API layer.
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

  from .qb_writeback import QBWritebackError, post_event_to_qb

  event = session.query(Event).filter(Event.id == body.event_id).first()
  if event is None:
    raise EventNotFoundError(f"Event {body.event_id} not found")

  metadata = dict(event.metadata_ or {})
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
