"""The event-driven ledger — agents, events, handlers, and journal entries.

The REA layer: `Agent` counterparty records, the event blocks that capture
real-world business events, the handler registry that transforms them into
GL postings, the reconciling items a changed source payload produces, and
the update/delete surface for journal entries already posted.
"""

from __future__ import annotations

from fastapi import APIRouter

from robosystems.adapters.quickbooks.client.api import (
  QBAuthFailedError,
  QBAuthUnavailableError,
)
from robosystems.middleware.extensions import OperationSpec
from robosystems.models.api.common import DeleteResult
from robosystems.models.api.event_block import (
  CreateEventBlockRequest,
  EventBlockEnvelope,
  ExecuteEventBlockRequest,
  ExecuteEventBlockResponse,
  UpdateEventBlockRequest,
)
from robosystems.models.api.event_handler import (
  CreateEventHandlerRequest,
  EventHandlerResponse,
  PreviewEventBlockResponse,
  UpdateEventHandlerRequest,
)
from robosystems.models.api.extensions.agent import (
  CreateAgentRequest,
  LedgerAgentResponse,
  UpdateAgentRequest,
)
from robosystems.models.api.extensions.journal_entries import (
  DeleteJournalEntryRequest,
  JournalEntryResponse,
  UpdateJournalEntryRequest,
)
from robosystems.models.api.extensions.reconciling_items import (
  PreviewReconcilingItemRequest,
  ReconcilingItemPlan,
  ResolveReconcilingItemRequest,
  ResolveReconcilingItemResponse,
)
from robosystems.operations.event_block import (
  DuplicateEventError,
  EventEffectsAlreadyLandedError,
  EventNotFoundError,
  EventNotPublishableError,
  InvalidEventTransitionError,
)
from robosystems.operations.event_block import (
  create_event_block as cmd_create_event_block,
)
from robosystems.operations.event_block import (
  execute_event_block as cmd_execute_event_block,
)
from robosystems.operations.event_block import (
  preview_event_block as cmd_preview_event_block,
)
from robosystems.operations.event_block import (
  update_event_block as cmd_update_event_block,
)
from robosystems.operations.event_block.engine import EngineValidationError
from robosystems.operations.event_block.python_handlers._disposal_plan import (
  ScheduleNotFoundError as DisposalScheduleNotFoundError,
)
from robosystems.operations.event_block.python_handlers.journal_entry_recorded import (
  ElementResolutionError,
)
from robosystems.operations.event_block.python_handlers.types import (
  HandlerMetadataValidationError,
)
from robosystems.operations.event_block.registry import (
  HandlerAmbiguousError,
  HandlerNotFoundError,
)
from robosystems.operations.event_block.template import TemplateInterpolationError
from robosystems.operations.locking import RowLockedError
from robosystems.operations.roboledger.commands._guards import ClosedPeriodError
from robosystems.operations.roboledger.commands.agent import (
  AgentNotFoundError,
  DuplicateExternalIdError,
)
from robosystems.operations.roboledger.commands.agent import (
  create_agent as cmd_create_agent,
)
from robosystems.operations.roboledger.commands.agent import (
  update_agent as cmd_update_agent,
)
from robosystems.operations.roboledger.commands.event_handler import (
  EventHandlerNotFoundError,
  TemplateValidationError,
)
from robosystems.operations.roboledger.commands.event_handler import (
  create_event_handler as cmd_create_event_handler,
)
from robosystems.operations.roboledger.commands.event_handler import (
  update_event_handler as cmd_update_event_handler,
)
from robosystems.operations.roboledger.commands.journal_entries import (
  JournalEntryAlreadyReversedError,
  JournalEntryNotDraftError,
  JournalEntryNotFoundError,
  JournalEntryNotPostedError,
  JournalEntryOwnedByEventError,
  UnbalancedJournalEntryError,
)
from robosystems.operations.roboledger.commands.journal_entries import (
  delete_journal_entry as cmd_delete_journal_entry,
)
from robosystems.operations.roboledger.commands.journal_entries import (
  update_journal_entry as cmd_update_journal_entry,
)
from robosystems.operations.roboledger.commands.reconciling_items import (
  NotAReconcilingItemError,
  ReconcilingItemNotFoundError,
  RestateBlockedError,
)
from robosystems.operations.roboledger.commands.reconciling_items import (
  preview_reconciling_item as cmd_preview_reconciling_item,
)
from robosystems.operations.roboledger.commands.reconciling_items import (
  resolve_reconciling_item as cmd_resolve_reconciling_item,
)
from robosystems.routers.extensions.roboledger._common import make_registrar

router = APIRouter()

_OP_TAG = "RoboLedger: Ledger & Events"
_registrar = make_registrar(router, _OP_TAG)


# ── Agents: counterparty records (events.agent_id references them) ────────────

create_agent_op = _registrar.register(
  OperationSpec(
    name="create-agent",
    summary="Create Agent",
    description=(
      "Create a counterparty record (customer, vendor, employee, etc.). "
      "The (source, external_id) pair is a dedup key — a second insert with "
      "the same pair returns 409. Use update-agent to patch fields."
    ),
    command=cmd_create_agent,
    request_model=CreateAgentRequest,
    result_type=LedgerAgentResponse,
    error_map={DuplicateExternalIdError: 409, ValueError: 422},
    mark_stale_reason="agent_created",
  )
)

update_agent_op = _registrar.register(
  OperationSpec(
    name="update-agent",
    summary="Update Agent",
    description=(
      "Patch counterparty fields. Only supplied fields are updated. "
      "Set is_active=false to deactivate (agents are never deleted — they are "
      "reference data referenced by events and transactions)."
    ),
    command=cmd_update_agent,
    request_model=UpdateAgentRequest,
    result_type=LedgerAgentResponse,
    error_map={AgentNotFoundError: 404, RowLockedError: 409, ValueError: 422},
    mark_stale_reason="agent_updated",
  )
)


# ── Event Blocks ─────────────────────────────────────────────────────────────

create_event_block_op = _registrar.register(
  OperationSpec(
    name="create-event-block",
    summary="Create Event Block",
    description=(
      "Persist a real-world business event. "
      "apply_handlers=False (default): capture-only, status='captured'. "
      "apply_handlers=True: resolves an event_handler, fires the template, "
      "creates GL entries atomically, status='classified'. "
      "Use preview-event-block to dry-run before committing. "
      "For journal_entry_recorded, whether the entry writes back to a "
      "connected source system follows `source` (schedule/manual publish; "
      "system does not) unless metadata.publish_to_source says otherwise — "
      "set it false for an alignment entry mirroring a change already made "
      "upstream, which would otherwise be applied twice."
    ),
    command=cmd_create_event_block,
    request_model=CreateEventBlockRequest,
    result_type=EventBlockEnvelope,
    error_map={
      # Ahead of the broad `ValueError: 422` so a repeat delivery is a conflict.
      DuplicateEventError: (
        409,
        lambda _e: "Event already ingested for this source and external_id",
      ),
      HandlerNotFoundError: 404,
      HandlerAmbiguousError: 409,
      TemplateInterpolationError: 422,
      EngineValidationError: 422,
      HandlerMetadataValidationError: 422,
      # e.g. `journal_entry_reversed` locks the entry it reverses. Retryable.
      RowLockedError: 409,
      # Reversed at most once: a fixable request, not a retryable conflict.
      JournalEntryAlreadyReversedError: 422,
      DisposalScheduleNotFoundError: 404,
      JournalEntryNotFoundError: 404,
      JournalEntryNotPostedError: 422,
      ClosedPeriodError: 422,
      UnbalancedJournalEntryError: 422,
      ValueError: 422,
    },
    # Source validation reads the graph's Connections (platform DB).
    requires_graph_id=True,
    mark_stale_reason="event_block_created",
  )
)

update_event_block_op = _registrar.register(
  OperationSpec(
    name="update-event-block",
    summary="Update Event Block",
    description=(
      "Apply a status transition (captured → classified | committed | voided) "
      "and/or field corrections (description, effective_at, metadata_patch) "
      "to an existing event block. Only supplied fields are updated. "
      "captured → classified records an account choice without posting — "
      "for a bank-feed line, patch metadata.classified_element_id (or "
      "accept_suggestion: true) in the same call. When the transition is "
      "captured/classified → committed, the registered Python handler fires "
      "against the captured metadata to produce the GL rows, unless it "
      "already wrote them when the event was created; a bank-feed "
      "line with no account chosen and no matching rule is refused. Errors "
      "from the handler (validation, element resolution, closed period, "
      "unbalanced lines) surface as 422 here so the inbox UI can display "
      "the failure reason without retry."
    ),
    command=cmd_update_event_block,
    request_model=UpdateEventBlockRequest,
    result_type=EventBlockEnvelope,
    error_map={
      EventNotFoundError: 404,
      InvalidEventTransitionError: 422,
      # The event's rows already posted (or published to QB): reverse instead.
      EventEffectsAlreadyLandedError: 422,
      HandlerMetadataValidationError: 422,
      ElementResolutionError: 422,
      ClosedPeriodError: 422,
      UnbalancedJournalEntryError: 422,
      # Subclasses ValueError; registered explicitly so it isn't mapped to 404.
      JournalEntryAlreadyReversedError: 422,
      # A running sync holds the row lock: the one retryable error here.
      RowLockedError: 409,
      # The rest mirrors create-event-block: approve fires the same handler.
      DisposalScheduleNotFoundError: 404,
      JournalEntryNotFoundError: 404,
      JournalEntryNotPostedError: 422,
      ValueError: 422,
    },
    # `metadata_patch.connection_id` must name one of this graph's connections.
    requires_graph_id=True,
    mark_stale_reason="event_block_updated",
  )
)


# QB publish uses `request_id=event.id` for idempotency; the loader's
# cross-source matcher recognises the round-tripped entry on the next sync.
execute_event_block_op = _registrar.register(
  OperationSpec(
    name="execute-event-block",
    summary="Execute Event Block",
    description=(
      "For events on a connection with write_policy='qb_authoritative' "
      "or 'hybrid', publish the captured GL plan to the source-of-truth "
      "system (QuickBooks). Captures qb_txn_id on "
      "event.metadata.qb_external_id, transitions status to 'fulfilled' "
      "(or 'pending' on rejection), and promotes draft GL rows to "
      "'posted'. Native-policy events fast-path through with no QB "
      "write — RoboSystems is the system of record."
    ),
    command=cmd_execute_event_block,
    request_model=ExecuteEventBlockRequest,
    result_type=ExecuteEventBlockResponse,
    error_map={
      EventNotFoundError: 404,
      # Intuit unreachable or busy; the connection is fine. Before the base.
      QBAuthUnavailableError: 503,
      # The QBClient has already flipped the connection to needs_reauth; 401
      # tells the UI the operator must reconnect.
      QBAuthFailedError: 401,
      # Retryable; the publish did not reach QuickBooks.
      RowLockedError: 409,
      # Voided / superseded: publishing would un-retract the event.
      EventNotPublishableError: 409,
      ClosedPeriodError: 422,
      ValueError: 422,
    },
    # Both the body override and the event-metadata routing id are caller-set.
    requires_graph_id=True,
    mark_stale_reason="event_published",
  )
)


# ── Event Handlers: the rule registry that drives event → GL ─────────────────

create_event_handler_op = _registrar.register(
  OperationSpec(
    name="create-event-handler",
    summary="Create Event Handler",
    description=(
      "Define a rule that fires GL transactions when a matching event block "
      "is created with apply_handlers=True. Match criteria (event_type, "
      "event_category, match_source, match_agent_type, etc.) act as AND-joined "
      "filters — null fields match anything. The highest-priority matching handler "
      "wins. AI-suggested handlers (suggested_by='ai') require approval before "
      "they are eligible for matching."
    ),
    command=cmd_create_event_handler,
    request_model=CreateEventHandlerRequest,
    result_type=EventHandlerResponse,
    error_map={TemplateValidationError: 422, ValueError: 422},
  )
)

update_event_handler_op = _registrar.register(
  OperationSpec(
    name="update-event-handler",
    summary="Update Event Handler",
    description=(
      "Patch an event handler's match criteria, template, priority, or active "
      "state. Pass approve=true to approve an AI-suggested handler; "
      "approve=false to revoke approval. Only supplied fields are updated."
    ),
    command=cmd_update_event_handler,
    request_model=UpdateEventHandlerRequest,
    result_type=EventHandlerResponse,
    error_map={
      EventHandlerNotFoundError: 404,
      TemplateValidationError: 422,
      RowLockedError: 409,
      ValueError: 422,
    },
  )
)

preview_event_block_op = _registrar.register(
  OperationSpec(
    name="preview-event-block",
    summary="Preview Event Block",
    description=(
      "Dry-run: resolve the matching handler and evaluate the transaction "
      "template without writing any rows. Returns the matched handler + planned "
      "debit/credit lines + any validation errors. Use this before "
      "create-event-block(apply_handlers=True) to confirm the GL plan."
    ),
    command=cmd_preview_event_block,
    request_model=CreateEventBlockRequest,
    result_type=PreviewEventBlockResponse,
    error_map={ValueError: 422},
  )
)

# ── Reconciling Items ────────────────────────────────────────────────────────
# A posted event whose source payload changed afterwards; the sync flags it
# rather than overwriting approved books. Preview first: the disposition is an
# accounting judgement the operator should agree before anything is written.

preview_reconciling_item_op = _registrar.register(
  OperationSpec(
    name="preview-reconciling-item",
    summary="Preview Reconciling Item",
    description=(
      "Read what changed on a reconciling item — an event whose source-system "
      "payload changed after it was posted (list them with list-event-blocks "
      "is_reconciling_item=true). Returns the posted entries against the "
      "accepted payload, the per-account net difference, which disposition "
      "applies by default, and anything blocking the others. Writes nothing. "
      "Run this before resolve-reconciling-item and agree the treatment with "
      "the user — restate moves prior months' figures, catch_up does not."
    ),
    command=cmd_preview_reconciling_item,
    request_model=PreviewReconcilingItemRequest,
    result_type=ReconcilingItemPlan,
    requires_created_by=False,
    requires_graph_id=True,
    error_map={
      ReconcilingItemNotFoundError: 404,
      NotAReconcilingItemError: 409,
      ValueError: 422,
    },
  )
)

resolve_reconciling_item_op = _registrar.register(
  OperationSpec(
    name="resolve-reconciling-item",
    summary="Resolve Reconciling Item",
    description=(
      "Dispose of one reconciling item and clear its flag. Three treatments: "
      "'restate' regenerates the event's entries from the accepted payload in "
      "place (prior months' figures change — right when nothing external binds "
      "them); 'catch_up' leaves history alone and posts the difference as an "
      "alignment entry in an open period, local-only so it cannot travel back "
      "to the source system and apply the change twice; 'acknowledge' records "
      "that the difference was handled elsewhere and clears the flag without "
      "touching the ledger (a note is required, and reference_event_id should "
      "name the entry that handled it). Omit disposition to take the default "
      "from preview-reconciling-item. Clearing the flag means the item stays "
      "cleared: the event's payload is set to the accepted one, so the next "
      "sync no longer sees a difference."
    ),
    command=cmd_resolve_reconciling_item,
    request_model=ResolveReconcilingItemRequest,
    result_type=ResolveReconcilingItemResponse,
    requires_graph_id=True,
    error_map={
      ReconcilingItemNotFoundError: 404,
      NotAReconcilingItemError: 409,
      RestateBlockedError: 422,
      ClosedPeriodError: 422,
      ElementResolutionError: 422,
      UnbalancedJournalEntryError: 422,
      HandlerMetadataValidationError: 422,
      RowLockedError: 409,
      ValueError: 422,
    },
    mark_stale_reason="reconciling_item_resolved",
  )
)

# ── Journal Entries ──────────────────────────────────────────────────────────
# Update/delete only: all creation goes through
# `create-event-block(event_type='journal_entry_recorded')`.

update_journal_entry_op = _registrar.register(
  OperationSpec(
    name="update-journal-entry",
    summary="Update Journal Entry",
    description=(
      "Update a draft journal entry. Posted entries are immutable and "
      "must be corrected via "
      "`create-event-block(event_type='journal_entry_reversed')`. If "
      "line_items is provided, existing line items are replaced "
      "atomically, the new set must balance, and a line on a retired "
      "(`is_active=false`) account is refused."
    ),
    command=cmd_update_journal_entry,
    request_model=UpdateJournalEntryRequest,
    result_type=JournalEntryResponse,
    error_map={
      JournalEntryNotFoundError: 404,
      JournalEntryNotDraftError: 422,
      ClosedPeriodError: 422,
      RowLockedError: 409,
      UnbalancedJournalEntryError: 422,
      ValueError: 422,
    },
    requires_created_by=False,
    mark_stale_reason="journal_entry_updated",
  )
)

delete_journal_entry_op = _registrar.register(
  OperationSpec(
    name="delete-journal-entry",
    summary="Delete Journal Entry",
    description=(
      "Hard-delete a draft journal entry. Posted entries are immutable "
      "and must be reversed instead."
    ),
    command=cmd_delete_journal_entry,
    request_model=DeleteJournalEntryRequest,
    result_type=DeleteResult,
    error_map={
      JournalEntryNotFoundError: 404,
      JournalEntryNotDraftError: 422,
      # The last draft of a live event: retract the event, not the draft.
      JournalEntryOwnedByEventError: 422,
      ClosedPeriodError: 422,
      RowLockedError: 409,
    },
    requires_created_by=False,
    mark_stale_reason="journal_entry_deleted",
  )
)
