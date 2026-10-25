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
from robosystems.adapters.quickbooks.reports import TrialBalanceReportError
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
  AcceptSuggestionsRequest,
  AcceptSuggestionsResponse,
  CreateAgentRequest,
  LearnClassificationDefaultsRequest,
  LearnClassificationDefaultsResponse,
  LedgerAgentResponse,
  UpdateAgentRequest,
)
from robosystems.models.api.extensions.consolidation import (
  AcquisitionResponse,
  AllocationOverrideResponse,
  DeconsolidationResponse,
  OwnershipChangeResponse,
  ProposeConsolidationEntriesRequest,
  ProposeConsolidationEntriesResponse,
  RecordAcquisitionRequest,
  RecordDeconsolidationRequest,
  RecordOwnershipChangeRequest,
  SetAllocationOverrideRequest,
)
from robosystems.models.api.extensions.dimensions import (
  CreateDimensionRequest,
  DimensionResponse,
  UpdateDimensionRequest,
)
from robosystems.models.api.extensions.journal_entries import (
  DeleteJournalEntryRequest,
  JournalEntryResponse,
  RecordOpeningBalanceRequest,
  UpdateJournalEntryRequest,
)
from robosystems.models.api.extensions.reconciliations import (
  PreviewReconciliationsRequest,
  ReconciliationListResponse,
  ReconciliationPolicyResponse,
  ReconciliationPreviewResponse,
  ReconciliationSummary,
  RecordStatementBalanceRequest,
  RefreshReconciliationsRequest,
  SetReconciliationPolicyRequest,
  SignOffReconciliationRequest,
)
from robosystems.models.api.extensions.reconciling_items import (
  PreviewReconcilingItemRequest,
  ReconcilingItemPlan,
  ResolveReconcilingItemRequest,
  ResolveReconcilingItemResponse,
)
from robosystems.operations.event_block import (
  DuplicateEventError,
  EventDocumentNotFoundError,
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
from robosystems.operations.event_block.discharges import DischargeRefusedError
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
from robosystems.operations.event_block.reserved import ReservedEventTypeError
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
  learn_classification_defaults as cmd_learn_classification_defaults,
)
from robosystems.operations.roboledger.commands.agent import (
  update_agent as cmd_update_agent,
)
from robosystems.operations.roboledger.commands.consolidation import (
  AcquisitionError,
  AllocationOverrideError,
  DeconsolidationError,
  OwnershipChangeError,
)
from robosystems.operations.roboledger.commands.consolidation import (
  propose_consolidation_entries as cmd_propose_consolidation_entries,
)
from robosystems.operations.roboledger.commands.consolidation import (
  record_acquisition as cmd_record_acquisition,
)
from robosystems.operations.roboledger.commands.consolidation import (
  record_deconsolidation as cmd_record_deconsolidation,
)
from robosystems.operations.roboledger.commands.consolidation import (
  record_ownership_change as cmd_record_ownership_change,
)
from robosystems.operations.roboledger.commands.consolidation import (
  set_allocation_override as cmd_set_allocation_override,
)
from robosystems.operations.roboledger.commands.dimensions import (
  DuplicateDimensionError,
)
from robosystems.operations.roboledger.commands.dimensions import (
  create_dimension as cmd_create_dimension,
)
from robosystems.operations.roboledger.commands.dimensions import (
  update_dimension as cmd_update_dimension,
)
from robosystems.operations.roboledger.commands.event_handler import (
  ApprovalNotAllowedError,
  EventHandlerNotFoundError,
  StaleApprovalError,
  TemplateValidationError,
)
from robosystems.operations.roboledger.commands.event_handler import (
  create_event_handler as cmd_create_event_handler,
)
from robosystems.operations.roboledger.commands.event_handler import (
  update_event_handler as cmd_update_event_handler,
)
from robosystems.operations.roboledger.commands.journal_entries import (
  ConsolidationAdjustmentError,
  JournalEntryAlreadyReversedError,
  JournalEntryNotDraftError,
  JournalEntryNotFoundError,
  JournalEntryNotPostedError,
  JournalEntryOwnedByEventError,
  JournalEntrySettlesItemsError,
  UnbalancedJournalEntryError,
)
from robosystems.operations.roboledger.commands.journal_entries import (
  delete_journal_entry as cmd_delete_journal_entry,
)
from robosystems.operations.roboledger.commands.journal_entries import (
  update_journal_entry as cmd_update_journal_entry,
)
from robosystems.operations.roboledger.commands.opening_balances import (
  OpeningBalanceError,
  OpeningBalanceLockedError,
)
from robosystems.operations.roboledger.commands.opening_balances import (
  record_opening_balance as cmd_record_opening_balance,
)
from robosystems.operations.roboledger.commands.reconciliations import (
  NotAGraphMemberError,
  ReconciliationNotFoundError,
  ReconciliationNotReconciledError,
  SeparateReviewerError,
  StatementAccountNotFoundError,
  StatementDocumentNotFoundError,
)
from robosystems.operations.roboledger.commands.reconciliations import (
  preview_reconciliations as cmd_preview_reconciliations,
)
from robosystems.operations.roboledger.commands.reconciliations import (
  record_statement_balance as cmd_record_statement_balance,
)
from robosystems.operations.roboledger.commands.reconciliations import (
  refresh_reconciliations as cmd_refresh_reconciliations,
)
from robosystems.operations.roboledger.commands.reconciliations import (
  set_reconciliation_policy as cmd_set_reconciliation_policy,
)
from robosystems.operations.roboledger.commands.reconciliations import (
  sign_off_reconciliation as cmd_sign_off_reconciliation,
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
from robosystems.operations.roboledger.commands.suggestions import (
  accept_suggestions as cmd_accept_suggestions,
)
from robosystems.operations.roboledger.entity_scope import EntityNotInGraphError
from robosystems.operations.roboledger.line_dimensions import DimensionNotFoundError
from robosystems.operations.roboledger.reads.consolidation import (
  NoConsolidationEntityError,
)
from robosystems.operations.roboledger.reconciliations import (
  NoSourceLedgerError,
  SourceLedgerUnavailableError,
)
from robosystems.operations.roboledger.reports.statement_sets import (
  StatementStampError,
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
      "reference data referenced by events and transactions). "
      "`classification_element_id` sets the account this counterparty's bank "
      "lines are suggested (an empty string clears it), and "
      "`classification_mode='always_ask'` stops suggesting one for a "
      "counterparty whose lines go to different accounts; its still-open "
      "lines are re-suggested at once. Each entity of a group keeps its own "
      "default, on its own chart: `classification_entity_id` names the one "
      "these fields are for, omitted the group parent. A default is also "
      "learned as lines are committed, so set one by hand only to correct it."
    ),
    command=cmd_update_agent,
    request_model=UpdateAgentRequest,
    result_type=LedgerAgentResponse,
    error_map={AgentNotFoundError: 404, RowLockedError: 409, ValueError: 422},
    mark_stale_reason="agent_updated",
  )
)

# ── Management dimensions: departments, classes, locations, projects ───────────

create_dimension_op = _registrar.register(
  OperationSpec(
    name="create-dimension",
    summary="Create Dimension",
    description=(
      "Add a member to a management dimension: a department, class, "
      "location or project (or an axis of your own) that ledger lines can "
      "carry, one member per axis per line. `value` is the member's stable "
      "key within its axis, a slug of the name when omitted; a second "
      "member with the same key returns 409. Tag lines with its id through "
      "`dimension_ids` on a journal line, and read the income statement by "
      "it with `live-financial-statement`'s `dimension`."
    ),
    command=cmd_create_dimension,
    request_model=CreateDimensionRequest,
    result_type=DimensionResponse,
    error_map={DuplicateDimensionError: 409, ValueError: 422},
    mark_stale_reason="dimension_created",
  )
)

update_dimension_op = _registrar.register(
  OperationSpec(
    name="update-dimension",
    summary="Update Dimension",
    description=(
      "Rename a management dimension member, or retire it with "
      "`is_active=false`. Members are never deleted: a retired member stays "
      "on the lines that carry it and goes on no new ones. A member synced "
      "from QuickBooks keeps QuickBooks's name."
    ),
    command=cmd_update_dimension,
    request_model=UpdateDimensionRequest,
    result_type=DimensionResponse,
    error_map={DimensionNotFoundError: 404, RowLockedError: 409, ValueError: 422},
    mark_stale_reason="dimension_updated",
  )
)

learn_classification_defaults_op = _registrar.register(
  OperationSpec(
    name="learn-classification-defaults",
    summary="Learn Classification Defaults",
    description=(
      "Seed each counterparty's default account from the bank lines already "
      "committed to the books, one for each entity whose lines it has: the "
      "account most of those lines went to becomes the default, with the lines "
      "that agreed counted as confirmations. A default already there is kept. Its still-open lines "
      "are then suggested from the default. Run it once on books that were "
      "classified before defaults were learned; after that, every commit "
      "teaches the default by itself. `dry_run` reports what would be learned "
      "without writing it."
    ),
    command=cmd_learn_classification_defaults,
    request_model=LearnClassificationDefaultsRequest,
    result_type=LearnClassificationDefaultsResponse,
    error_map={RowLockedError: 409},
    mark_stale_reason="classification_defaults_learned",
  )
)


accept_suggestions_op = _registrar.register(
  OperationSpec(
    name="accept-suggestions",
    summary="Accept Suggestions",
    description=(
      "Take the suggestion on every open bank line the filters select, "
      "oldest first: each is classified to its `suggested_element_id` "
      "through the same update a single line goes through, and posted too "
      "with `commit: true`. Select by `source`, `agent_id`, "
      "`suggestion_source` (`agent_default` for the learned ones), a date "
      "range, or explicit `event_ids` from a multi-select. A line it cannot "
      "take — no suggestion to take, already classified or posted, a closed "
      "month, a choice the handler refuses — comes back `skipped` with the "
      "reason, never forced. `dry_run: true` reports the counts without "
      "writing; `limit` bounds a call and `truncated` says more matched."
    ),
    command=cmd_accept_suggestions,
    request_model=AcceptSuggestionsRequest,
    result_type=AcceptSuggestionsResponse,
    error_map={EntityNotInGraphError: 404, RowLockedError: 409},
    requires_graph_id=True,
    mark_stale_reason="event_block_updated",
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
      "upstream, which would otherwise be applied twice. "
      "An invoice_issued or bill_received takes due_date, or terms_days "
      "(net N from occurred_at); with neither it is due on receipt. A later "
      "due date is a field correction on update-event-block."
    ),
    command=cmd_create_event_block,
    request_model=CreateEventBlockRequest,
    result_type=EventBlockEnvelope,
    error_map={
      EventDocumentNotFoundError: 404,
      # Ahead of the broad `ValueError: 422` so a repeat delivery is a conflict.
      DuplicateEventError: (
        409,
        lambda _e: "Event already ingested for this source and external_id",
      ),
      ReservedEventTypeError: 422,
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
      DischargeRefusedError: 422,
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
      "captured/classified → committed, the registered Python handler (else "
      "the matching event handler's template) fires against the captured "
      "metadata to produce the GL rows, unless it already wrote them when "
      "the event was created; an event with a handler cannot skip to "
      "pending or fulfilled without rows. A bank-feed "
      "line with no account chosen and no matching rule is refused. Errors "
      "from the handler (validation, element resolution, closed period, "
      "unbalanced lines) surface as 422 here so the inbox UI can display "
      "the failure reason without retry. A bank line's flow is a second "
      "classification beside its account, overriding the account's default "
      "flow on the cash flow and equity statements: patch "
      "`classified_flow_qname` (or `flow_qname` on each "
      "`classified_allocations` part) with an rs-gaap flow concept such as "
      "`rs-gaap:ProceedsFromIssuanceOfLongTermDebt`. On a line that has already posted, "
      "this re-tags its lines in place, in a closed month too, since no "
      "balance moves; an empty string clears it. A payment or bank line "
      "records the invoices or bills it settles with `discharge_allocations` "
      "(positive cents per document, together at most its amount) or one "
      "`discharges_event_id`; each is checked: an invoice for money in, a "
      "bill for money out, on its own entity, not voided."
    ),
    command=cmd_update_event_block,
    request_model=UpdateEventBlockRequest,
    result_type=EventBlockEnvelope,
    error_map={
      # A re-tag in a closed month could not re-stamp its statements.
      StatementStampError: 422,
      EventDocumentNotFoundError: 404,
      EventNotFoundError: 404,
      ReservedEventTypeError: 422,
      InvalidEventTransitionError: 422,
      DischargeRefusedError: 422,
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
      HandlerAmbiguousError: 409,
      TemplateInterpolationError: 422,
      EngineValidationError: 422,
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


# Each draft entry posts with its entry id as the QB RequestId for idempotency;
# the loader's cross-source matcher recognises the round-tripped entries.
execute_event_block_op = _registrar.register(
  OperationSpec(
    name="execute-event-block",
    summary="Execute Event Block",
    description=(
      "For events on a connection with write_policy='qb_authoritative' "
      "or 'hybrid', publish the event's draft GL entries to the "
      "source-of-truth system (QuickBooks), each as its own JournalEntry. "
      "Records the QuickBooks ids per entry on event.metadata.qb_entry_ids "
      "and promotes the published entries to 'posted'; the event goes "
      "'fulfilled' once no draft remains, or 'pending' on rejection (what "
      "landed is kept). Native-policy events fast-path through with no QB "
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
      "wins. A handler created through an AI client is the AI's suggestion "
      "(suggested_by='ai'): it takes no part in matching until a person "
      "approves it in the app or the API."
    ),
    command=cmd_create_event_handler,
    request_model=CreateEventHandlerRequest,
    result_type=EventHandlerResponse,
    error_map={TemplateValidationError: 422, ValueError: 422},
    requires_surface=True,
  )
)

update_event_handler_op = _registrar.register(
  OperationSpec(
    name="update-event-handler",
    summary="Update Event Handler",
    description=(
      "Patch an event handler's match criteria, template, priority, or active "
      "state. Pass approve=true to approve an AI-suggested handler; "
      "approve=false to revoke approval. Only supplied fields are updated. "
      "Through an AI client, approve=true is refused, and a change to what the "
      "handler matches or books makes it a suggestion again, out of matching "
      "until a person approves it."
    ),
    command=cmd_update_event_handler,
    request_model=UpdateEventHandlerRequest,
    result_type=EventHandlerResponse,
    error_map={
      EventHandlerNotFoundError: 404,
      ApprovalNotAllowedError: 403,
      StaleApprovalError: 409,
      TemplateValidationError: 422,
      RowLockedError: 409,
      ValueError: 422,
    },
    requires_surface=True,
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
      "name the entry that handled it: a standing entry on the item's books, "
      "other than the item, and not already counted for something else such "
      "as a catch-up or another settlement). An alignment entry authored "
      "by hand "
      "can name the items it settles in metadata.resolves_reconciling_items "
      "instead, each as {event_id, expected_drift_detected_at} carrying the "
      "preview's stamp, and each is acknowledged against it as it is "
      "recorded — an item left flagged would be caught up a second time. "
      "What an acknowledged entry booked counts toward the item's next "
      "change; voiding or reversing that entry, or a catch-up, flags the item "
      "again, and superseding it by a correction that books exactly the item's "
      "difference hands the settlement to the correction. An entry that "
      "settles a QuickBooks change has to stay local, as a catch-up does: "
      "create it with metadata.publish_to_source false, booking only the "
      "differences it settles; one that would publish or books more is "
      "refused, and so is a later patch setting it to publish. Omit "
      "disposition to take the default from preview-reconciling-item. "
      "Clearing the flag means the item stays "
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

# ── Reconciliations ──────────────────────────────────────────────────────────

preview_reconciliations_op = _registrar.register(
  OperationSpec(
    name="preview-reconciliations",
    summary="Preview Reconciliations",
    description=(
      "Compare the ledger at a period end with something outside it, without "
      "recording anything. `method` picks the check. `source_ledger` (the "
      "default) reads QuickBooks' own trial balance for the period end and "
      "sets it beside the ledger's, account by account: balance-sheet "
      "accounts cumulatively, income and expense accounts from the start of "
      "the fiscal year. A difference there means the ledger's copy of the "
      "books has drifted from the source (a transaction deleted or back-dated "
      "there after it was synced, or activity not yet synced), so run it "
      "before trusting any other figure on a synced ledger; it needs a "
      "connected QuickBooks ledger. `schedule_register` compares each asset "
      "account a schedule carries a balance on (a prepaid, accumulated "
      "depreciation, a fixed-asset cost account) with what its schedules say "
      "it holds, and lists the schedules behind each figure. A difference "
      "there is a balance with no schedule behind it, or a scheduled amount "
      "the ledger does not hold. Each row carries both balances, the "
      "difference, and whether the account ties."
    ),
    command=cmd_preview_reconciliations,
    request_model=PreviewReconciliationsRequest,
    result_type=ReconciliationPreviewResponse,
    requires_created_by=False,
    requires_graph_id=True,
    error_map={
      EntityNotInGraphError: 404,
      NoSourceLedgerError: 409,
      SourceLedgerUnavailableError: 503,
      # Intuit unreachable or busy; the connection is fine. Before the base.
      QBAuthUnavailableError: 503,
      QBAuthFailedError: 401,
      TrialBalanceReportError: 502,
      ValueError: 422,
    },
  )
)

refresh_reconciliations_op = _registrar.register(
  OperationSpec(
    name="refresh-reconciliations",
    summary="Refresh Reconciliations",
    description=(
      "Run every reconciliation that applies at a period end and record each "
      "result on its block. A ledger synced from QuickBooks is compared with "
      "QuickBooks' own trial balance, on one block for the whole ledger. Each "
      "asset account a schedule carries a balance on is compared with what "
      "its schedules say it holds, on a block of its own. Each account with a "
      "statement covering the period (record-statement-balance; the latest "
      "within its statement cycle) is compared with that balance. A block "
      "reconciles "
      "for the period when its difference is within its materiality. Running "
      "it again replaces the period's comparison, so the answer is always as "
      "of the last run. A block is created the first time its check applies, "
      "and from then on the period's close waits on it until "
      "set-reconciliation-policy releases it. The one exception is an account "
      "block whose first comparison does not tie: it is created without "
      "holding the close, so a difference found on first contact is reported "
      "and becomes a close requirement only when you turn it on. Returns "
      "every reconciliation's standing for the period. Use "
      "preview-reconciliations to see a comparison without recording it."
    ),
    command=cmd_refresh_reconciliations,
    request_model=RefreshReconciliationsRequest,
    result_type=ReconciliationListResponse,
    requires_graph_id=True,
    error_map={
      EntityNotInGraphError: 404,
      NoSourceLedgerError: 409,
      RowLockedError: 409,
      SourceLedgerUnavailableError: 503,
      # Intuit unreachable or busy; the connection is fine. Before the base.
      QBAuthUnavailableError: 503,
      QBAuthFailedError: 401,
      TrialBalanceReportError: 502,
      ValueError: 422,
    },
    mark_stale_reason="reconciliations_refreshed",
  )
)

record_statement_balance_op = _registrar.register(
  OperationSpec(
    name="record-statement-balance",
    summary="Record Statement Balance",
    description=(
      "Record the ending balance of a statement (a bank, card or loan "
      "statement) for one balance-sheet account, and reconcile the account "
      "to it. Give the balance as the statement shows it, as a positive "
      "number in the account's normal direction, with the statement's ending "
      "date. The ledger's balance at that date is set beside it, counting "
      "the drafts the close will post, and the result is recorded on the "
      "account's statement reconciliation for the period the statement ends "
      "in. On an account a bank feed keeps, ledger lines that did not come "
      "from the feed and are dated on or before the statement are listed as "
      "outstanding and adjust the statement balance, since the bank had not "
      "cleared them; one dated before the statement's own cycle is listed as "
      "`stale_outstanding` and does not, since the bank should have cleared "
      "it by now, so the difference stays until it clears, is voided or is "
      "re-dated; a statement ending before the period's last day is "
      "carried to it by the feed's own lines (`roll_forward`). Attach the statement as evidence by passing the `document_id` of "
      "its stored file (create-document-upload, then complete-document-upload); "
      "recording it again with another document lapses a sign-off, and "
      "recording it again without one keeps the document already attached "
      "(an empty string detaches it). Writes no "
      "books. The first "
      "statement recorded for an account creates its reconciliation, which "
      "does not hold the close: turn `required_for_close` on with "
      "set-reconciliation-policy to make every period's close wait for a "
      "statement on that account. Recording the same account and date again "
      "replaces the earlier balance; when more than one statement ends in a "
      "period, the one with the latest date stands. Recording a balance that "
      "differs from one already signed off lapses that sign-off. A "
      "difference left after the outstanding lines is activity one side has "
      "and the other does not yet, or an error on either. Returns the "
      "reconciliation's standing for the period."
    ),
    command=cmd_record_statement_balance,
    request_model=RecordStatementBalanceRequest,
    result_type=ReconciliationSummary,
    requires_graph_id=True,
    error_map={
      EntityNotInGraphError: 404,
      StatementAccountNotFoundError: 404,
      StatementDocumentNotFoundError: 404,
      RowLockedError: 409,
      ValueError: 422,
    },
    mark_stale_reason="statement_balance_recorded",
  )
)

set_reconciliation_policy_op = _registrar.register(
  OperationSpec(
    name="set-reconciliation-policy",
    summary="Set Reconciliation Policy",
    description=(
      "Change how much the close cares about one reconciliation: whether the "
      "period's close waits on it (`required_for_close`); its `materiality`, "
      "the difference up to which it still counts as reconciled; whether the "
      "close also waits for a sign-off (`review_required`); and whether the "
      "reviewer must be someone other than the person who ran the comparison "
      "(`separate_reviewer`, which can only be turned on while the graph has "
      "at least two members who can write; if it later has one, turn it off "
      "or add a member). For a statement reconciliation, `statement_cycle` "
      "says how often the statement is issued (monthly, quarterly or "
      "annual): a period is covered by the latest statement ending within "
      "the cycle that ends with it, so a quarterly statement still covers "
      "the months between statements. Omitted fields keep their value. The next "
      "refresh-reconciliations uses the new materiality; comparisons already "
      "recorded are not re-judged."
    ),
    command=cmd_set_reconciliation_policy,
    request_model=SetReconciliationPolicyRequest,
    result_type=ReconciliationPolicyResponse,
    requires_graph_id=True,
    error_map={
      ReconciliationNotFoundError: 404,
      SeparateReviewerError: 409,
      RowLockedError: 409,
      ValueError: 422,
    },
    mark_stale_reason="reconciliation_policy_changed",
  )
)

sign_off_reconciliation_op = _registrar.register(
  OperationSpec(
    name="sign-off-reconciliation",
    summary="Sign Off Reconciliation",
    description=(
      "Record your review of a reconciliation for a period. Only a period "
      "that reconciles can be signed off, and only by a member of the graph: "
      "access that comes from an organization role is not enough. The "
      "sign-off records who ran the comparison and who reviewed it, and says "
      "so when they are the same person; a reconciliation with "
      "`separate_reviewer` set refuses that case instead. The sign-off stands "
      "for the balances as they were compared: if any of them changes "
      "afterwards, the period goes back to reconciled or unreconciled and "
      "needs signing off again, unless the balances return to the ones "
      "already signed. Signing off a period already reviewed "
      "changes nothing. Returns the reconciliation's standing for the period."
    ),
    command=cmd_sign_off_reconciliation,
    request_model=SignOffReconciliationRequest,
    result_type=ReconciliationSummary,
    requires_graph_id=True,
    error_map={
      ReconciliationNotFoundError: 404,
      NotAGraphMemberError: 403,
      ReconciliationNotReconciledError: 409,
      SeparateReviewerError: 409,
      RowLockedError: 409,
      ValueError: 422,
    },
    mark_stale_reason="reconciliation_signed_off",
  )
)

# ── Journal Entries ──────────────────────────────────────────────────────────
# Update/delete only: all creation goes through
# `create-event-block(event_type='journal_entry_recorded')`, except the
# opening balance, which has an operation of its own.

record_opening_balance_op = _registrar.register(
  OperationSpec(
    name="record-opening-balance",
    summary="Record Opening Balance",
    description=(
      "Record the balance sheet an entity's books start from, when its "
      "history does not come with them: one balance per balance-sheet "
      "account (assets, liabilities, equity), in cents, debit or credit in "
      "the account's normal direction. It posts as one `opening` entry dated "
      "the day before the entity's first fiscal month, so every month's "
      "opening balances include it and no month's activity does: the cash "
      "flow and the equity statement start from it rather than counting it. "
      "Name the equity account that takes any difference between debits and "
      "credits (`equity_element_id`, usually Opening Balance Equity); "
      "unbalanced balances without one are refused, and an income or expense "
      "account is refused, since income before the books begin is in "
      "retained earnings. One per entity: recording it again replaces it, "
      "until the entity's first close, after which a correction is an "
      "adjusting entry in an open month. Set up the fiscal calendar first; "
      "moving its start (change-calendar-start) moves the opening with it."
    ),
    command=cmd_record_opening_balance,
    request_model=RecordOpeningBalanceRequest,
    result_type=JournalEntryResponse,
    error_map={
      EntityNotInGraphError: 404,
      OpeningBalanceLockedError: 409,
      RowLockedError: 409,
      OpeningBalanceError: 422,
      ValueError: 422,
    },
    mark_stale_reason="opening_balance_recorded",
  )
)

propose_consolidation_entries_op = _registrar.register(
  OperationSpec(
    name="propose-consolidation-entries",
    summary="Propose Consolidation Entries",
    description=(
      "Draft a month's consolidating entries in the group's consolidation "
      "entity: what preview-consolidation-entries computes, written as "
      "drafts that the consolidation entity's close posts. Proposing the "
      "month again replaces the drafts it wrote before, so it can be run "
      "after any member's books change; a person's own entries are never "
      "touched. Lines that eliminate a cash flow carry the row they offset. "
      "A residual or a disagreement is returned beside the drafts, never "
      "plugged; a month the consolidation entity has closed is left as "
      "posted."
    ),
    command=cmd_propose_consolidation_entries,
    request_model=ProposeConsolidationEntriesRequest,
    result_type=ProposeConsolidationEntriesResponse,
    error_map={
      NoConsolidationEntityError: 422,
      HandlerMetadataValidationError: 422,
      ClosedPeriodError: 422,
      ConsolidationAdjustmentError: 422,
      UnbalancedJournalEntryError: 422,
      RowLockedError: 409,
      ValueError: 422,
    },
    mark_stale_reason="consolidation_entries_proposed",
  )
)

set_allocation_override_op = _registrar.register(
  OperationSpec(
    name="set-allocation-override",
    summary="Set Allocation Override",
    description=(
      "Set the outside owners' share of a member's result for one month in "
      "place of the pro-rata share its ownership percentage gives: a "
      "preferred return, a waterfall, a special allocation. Give the share "
      "as an amount in cents (negative for a share of a loss) or as a "
      "percentage of the month's result, with the basis; `clear` removes it. "
      "preview- and propose-consolidation-entries then use it for that "
      "month, and their basis says so. Only for a member consolidated in "
      "full, and refused in a month the consolidation entity has closed."
    ),
    command=cmd_set_allocation_override,
    request_model=SetAllocationOverrideRequest,
    result_type=AllocationOverrideResponse,
    error_map={
      EntityNotInGraphError: 404,
      AllocationOverrideError: 422,
      ClosedPeriodError: 422,
      RowLockedError: 409,
      ValueError: 422,
    },
    mark_stale_reason="allocation_override_set",
  )
)

record_acquisition_op = _registrar.register(
  OperationSpec(
    name="record-acquisition",
    summary="Record Acquisition",
    description=(
      "Record a member the parent bought from its outside owners, for a member "
      "that joined mid-life (`joined_on` set) with its balance sheet at "
      "acquisition recorded by record-opening-balance. Give the consideration "
      "paid to the sellers in cents and the basis (the purchase agreement, a "
      "valuation); optionally the fair value of the outside owners' interest "
      "(`nci_fair_value`, for goodwill on the whole business) and fair-value "
      "step-ups (`allocations`). It writes a draft in the consolidation "
      "entity, dated the join, that its close posts: the excess of the "
      "consideration over the parent's share of the member's equity at "
      "acquisition goes to the step-ups and the rest to goodwill. "
      "preview-consolidation-entries then reads the consideration as the "
      "parent's investment, so the pre-acquisition residual clears. A bargain "
      "purchase is refused until `confirm_bargain_purchase` is set after the "
      "reassessment ASC 805 asks for. Recording again replaces the draft; a "
      "posted acquisition is reversed first."
    ),
    command=cmd_record_acquisition,
    request_model=RecordAcquisitionRequest,
    result_type=AcquisitionResponse,
    error_map={
      EntityNotInGraphError: 404,
      AcquisitionError: 422,
      ClosedPeriodError: 422,
      ConsolidationAdjustmentError: 422,
      UnbalancedJournalEntryError: 422,
      RowLockedError: 409,
      ValueError: 422,
    },
    mark_stale_reason="acquisition_recorded",
  )
)

record_ownership_change_op = _registrar.register(
  OperationSpec(
    name="record-ownership-change",
    summary="Record Ownership Change",
    description=(
      "Record the parent buying more of a member consolidated in full from "
      "its outside owners, or selling some of it to them while keeping "
      "control: the share it holds from a month (`ownership_pct`, staying 50 "
      "or more) and the price in cents, with the basis. It sets the member's "
      "share from that month and writes a draft in the consolidation entity, "
      "dated the month's first day, that its close posts: ASC 810 makes it an "
      "equity transaction with no gain or loss. Buying, the price above the "
      "book value of the capital bought goes against the investment, the "
      "share bought of the member's results since it joined comes out of the "
      "outside owners' interest, and the difference is the parent's equity. "
      "Selling, the parent's own entry takes the proceeds against the "
      "carrying amount of the shares sold and books its gain or loss; the "
      "draft reverses that gain, releases the parent's cost above book on "
      "those shares, credits the outside owners their larger share of the "
      "member's results, and takes the difference to the parent's equity. "
      "preview-consolidation-entries then reads the price paid, net of what a "
      "sale released, as part of the parent's investment, and the new share "
      "from that month. Changes are recorded in order; recording a month "
      "again replaces its draft, and a posted one is reversed first. A change "
      "across consolidation methods is not supported yet."
    ),
    command=cmd_record_ownership_change,
    request_model=RecordOwnershipChangeRequest,
    result_type=OwnershipChangeResponse,
    error_map={
      EntityNotInGraphError: 404,
      OwnershipChangeError: 422,
      ClosedPeriodError: 422,
      ConsolidationAdjustmentError: 422,
      UnbalancedJournalEntryError: 422,
      RowLockedError: 409,
      ValueError: 422,
    },
    mark_stale_reason="ownership_change_recorded",
  )
)

record_deconsolidation_op = _registrar.register(
  OperationSpec(
    name="record-deconsolidation",
    summary="Record Deconsolidation",
    description=(
      "Record a member's leaving the group, once `left_on` is set on it: what "
      "the parent received for its shares in cents (0 for a dissolution) and "
      "the basis. It writes a draft in the consolidation entity, dated the "
      "first day of the month after the member's last, that its close posts: "
      "what the consolidation entity still holds for the member on its "
      "balance-sheet accounts and as the outside owners' interest is taken "
      "off (goodwill, step-ups, the price above book of the investment, the "
      "outside owners' accumulated share), a member in full has the results "
      "it earned in the group put back into the group's equity as they leave "
      "with its books, and the difference is the gain or loss on its leaving: "
      "the group's adjustment to the gain the parent's own books took, so the "
      "consolidated gain is the consideration less the carrying amount of "
      "what left (ASC 810). The investment and intercompany eliminations go "
      "back on their own in that month's proposals. The parent's own sale "
      "entry stays on its books. Recording again replaces the draft; a posted "
      "deconsolidation is reversed first. A retained interest is not "
      "supported yet."
    ),
    command=cmd_record_deconsolidation,
    request_model=RecordDeconsolidationRequest,
    result_type=DeconsolidationResponse,
    error_map={
      EntityNotInGraphError: 404,
      DeconsolidationError: 422,
      ClosedPeriodError: 422,
      ConsolidationAdjustmentError: 422,
      UnbalancedJournalEntryError: 422,
      RowLockedError: 409,
      ValueError: 422,
    },
    mark_stale_reason="deconsolidation_recorded",
  )
)

update_journal_entry_op = _registrar.register(
  OperationSpec(
    name="update-journal-entry",
    summary="Update Journal Entry",
    description=(
      "Update a draft journal entry, or re-tag any entry's flows or "
      "management dimensions. If "
      "line_items is provided, existing line items are replaced "
      "atomically, the new set must balance, and a line on a retired "
      "(`is_active=false`) account is refused. `line_flows` sets the cash "
      "flow row of the named lines in place, on a posted entry too, closed "
      "month included, since a flow moves no balance; the lines on the "
      "other side of the entry that were in step with it follow. "
      "`line_dimensions` sets the department, class, location or project "
      "members the named lines carry, in place, on a posted entry and a "
      "closed month too, since a dimension moves no balance either. "
      "Posted entries accept nothing else and are corrected via "
      "`create-event-block(event_type='journal_entry_reversed')`."
    ),
    command=cmd_update_journal_entry,
    request_model=UpdateJournalEntryRequest,
    result_type=JournalEntryResponse,
    error_map={
      # A re-tag in a closed month could not re-stamp its statements.
      StatementStampError: 422,
      JournalEntryNotFoundError: 404,
      JournalEntryNotDraftError: 422,
      ClosedPeriodError: 422,
      RowLockedError: 409,
      UnbalancedJournalEntryError: 422,
      ValueError: 422,
    },
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
      # One draft of an event that settled reconciling items: void the event.
      JournalEntrySettlesItemsError: 422,
      ClosedPeriodError: 422,
      RowLockedError: 409,
    },
    requires_created_by=False,
    mark_stale_reason="journal_entry_deleted",
  )
)
