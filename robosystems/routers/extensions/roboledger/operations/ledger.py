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
  CreateAgentRequest,
  LedgerAgentResponse,
  UpdateAgentRequest,
)
from robosystems.models.api.extensions.journal_entries import (
  DeleteJournalEntryRequest,
  JournalEntryResponse,
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
from robosystems.operations.roboledger.entity_scope import EntityNotInGraphError
from robosystems.operations.roboledger.reconciliations import (
  NoSourceLedgerError,
  SourceLedgerUnavailableError,
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
      ReservedEventTypeError: 422,
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
      "name the entry that handled it). An alignment entry authored by hand "
      "can name the items it settles in metadata.resolves_reconciling_items "
      "instead, and each is acknowledged against it as it is recorded — an "
      "item left flagged would be caught up a second time. Omit disposition "
      "to take the default "
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
      "statement balance recorded in the period (record-statement-balance) is "
      "compared with that balance. A block reconciles "
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
      "in. Attach the statement as evidence by passing the `document_id` of "
      "its stored file (create-document-upload, then complete-document-upload); "
      "recording it again with another document lapses a sign-off. Writes no "
      "books. The first "
      "statement recorded for an account creates its reconciliation, which "
      "does not hold the close: turn `required_for_close` on with "
      "set-reconciliation-policy to make every period's close wait for a "
      "statement on that account. Recording the same account and date again "
      "replaces the earlier balance; when more than one statement ends in a "
      "period, the one with the latest date stands. Recording a balance that "
      "differs from one already signed off lapses that sign-off. A "
      "difference is not explained here: it "
      "is activity one side has and the other does not yet, or an error on "
      "either. Returns the reconciliation's standing for the period."
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
      "or add a member). Omitted fields keep their value. The next "
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
