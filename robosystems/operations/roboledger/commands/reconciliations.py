"""Reconciliation commands."""

from __future__ import annotations

from decimal import ROUND_HALF_UP, Decimal

from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import flag_modified

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
from robosystems.models.api.information_block import ReconciliationMechanics
from robosystems.models.extensions import Element, Structure
from robosystems.models.extensions.roboledger import COA_SOURCES, FactSet
from robosystems.operations.information_block.reconciliation import (
  RECONCILIATION_BLOCK_TYPE,
)
from robosystems.operations.locking import lock_by_id
from robosystems.operations.roboledger.fiscal_calendar import (
  FiscalCalendarService,
  next_period,
)
from robosystems.operations.roboledger.reads.fiscal_calendar import (
  get_fiscal_year_start_month,
)
from robosystems.operations.roboledger.reads.reconciliations import (
  list_reconciliations,
)
from robosystems.operations.roboledger.reconciliations import (
  NoSourceLedgerError,
  NothingToReconcileError,
  ReconciliationWindow,
  ScheduleRegisterResolver,
  SourceLedgerResolver,
  StatementResolver,
  compute_reconciliations,
  reconciliation_window,
)
from robosystems.operations.roboledger.reconciliations.blocks import (
  ComparedVia,
  account_comparison,
  account_reconciliations,
  create_account_reconciliation,
  ensure_ledger_reconciliation,
  find_ledger_reconciliation,
  has_reconciliations,
  lock_reconciliation_writes,
  preparers,
  reconciliation_rule,
  record_policy_change,
  record_reconciliation,
  record_sign_off,
)
from robosystems.operations.roboledger.reconciliations.observations import (
  record_statement_observation,
)


class ReconciliationNotFoundError(Exception):
  def __init__(self, structure_id: str) -> None:
    super().__init__(f"Reconciliation {structure_id!r} not found.")
    self.structure_id = structure_id


class ReconciliationNotReconciledError(Exception):
  """Only a comparison that reconciles can be signed off."""

  def __init__(self, name: str, period: str, status: str) -> None:
    reason = {
      "not_started": (
        "it has not been compared for that period; run refresh-reconciliations"
      ),
      "unpinned": (
        "its comparison was recorded before sign-off existed and cannot be "
        "pinned; run refresh-reconciliations again first"
      ),
    }.get(
      status,
      "the two sides do not reconcile; clear what refresh-reconciliations reports",
    )
    super().__init__(f"Cannot sign off {name!r} for {period}: {reason}.")
    self.status = status


class StatementAccountNotFoundError(Exception):
  def __init__(self, element_id: str) -> None:
    super().__init__(f"Account {element_id!r} not found.")
    self.element_id = element_id


class StatementAccountError(ValueError):
  """The account cannot be reconciled to a statement."""


class StatementDocumentNotFoundError(Exception):
  def __init__(self, document_id: str) -> None:
    super().__init__(f"Document {document_id!r} not found on this graph.")
    self.document_id = document_id


class NotAGraphMemberError(Exception):
  """The reviewer has no membership of their own on the graph."""

  def __init__(self) -> None:
    super().__init__(
      "Only a member of this graph can sign off a reconciliation. Access that "
      "comes from an organization role is not a membership; add the reviewer "
      "to the graph as a member or admin."
    )


class SeparateReviewerError(Exception):
  """The block's separate-reviewer policy is not, or cannot be, satisfied."""


def _explicit_write_members(graph_id: str) -> set[str]:
  from robosystems.database import SessionFactory
  from robosystems.models.core import GraphUser

  with SessionFactory() as platform_session:
    return GraphUser.explicit_write_member_ids(graph_id, platform_session)


def _load_reconciliation(session: Session, structure_id: str) -> Structure:
  structure = lock_by_id(
    session,
    Structure,
    structure_id,
    f"Reconciliation {structure_id} is being written by another process. "
    "Retry in a moment.",
  )
  if structure is None or structure.block_type != RECONCILIATION_BLOCK_TYPE:
    raise ReconciliationNotFoundError(structure_id)
  return structure


def _summary(session: Session, structure_id: str, period: str) -> ReconciliationSummary:
  for rec in list_reconciliations(session, period).reconciliations:
    if rec.structure_id == structure_id:
      return rec
  raise ReconciliationNotFoundError(structure_id)


def preview_reconciliations(
  session: Session, body: PreviewReconciliationsRequest, *, graph_id: str
) -> ReconciliationPreviewResponse:
  """Compare the ledger at a period end by one method. Writes nothing.

  Raises ``ValueError`` on a malformed period. For ``source_ledger`` it also
  raises `NoSourceLedgerError` when the graph has no synced ledger, and the
  QuickBooks client's own errors.
  """
  window = reconciliation_window(body.period, get_fiscal_year_start_month(session))
  if body.method == "schedule_register":
    side = _schedule_register_side(session, window)
  elif body.method == "statement":
    side = _statement_side(session, window)
  else:
    side = SourceLedgerResolver(graph_id).resolve(session, window)
  return compute_reconciliations(
    session, window=window, side=side, include_tied=body.include_tied
  )


def _schedule_register_side(session: Session, window: ReconciliationWindow):
  # An account that once had a schedule keeps its block, and compares against
  # zero once no schedule reaches it.
  reconciled = frozenset(account_reconciliations(session, "schedule_register"))
  return ScheduleRegisterResolver(also_cover=reconciled).resolve(session, window)


def _statement_side(session: Session, window: ReconciliationWindow):
  reconciled = frozenset(account_reconciliations(session, "statement"))
  return StatementResolver(reconciled).resolve(session, window)


def _record_account_side(
  session: Session,
  side,
  *,
  window: ReconciliationWindow,
  created_by: str,
  compared_via: ComparedVia,
  create: bool,
) -> None:
  """Compare an account-scope side and record each account on its block.

  With ``create``, an account with no block gets one, required for close
  only if it ties: a difference found on first contact is reported without
  holding the close. Without it, an account with no block is skipped.
  """
  if not side.covered_element_ids:
    return
  comparison = compute_reconciliations(
    session, window=window, side=side, include_tied=True
  )
  blocks = account_reconciliations(session, side.method)
  for row in sorted(
    comparison.rows, key=lambda r: (r.account_code or "", r.account_name)
  ):
    structure = blocks.get(str(row.element_id))
    if structure is None:
      if not create:
        continue
      structure = create_account_reconciliation(
        session,
        method=side.method,
        element_id=str(row.element_id),
        account_name=row.account_name,
        required_for_close=row.status == "tied",
        created_by=created_by,
      )
    record_reconciliation(
      session,
      structure,
      window=window,
      side=side,
      comparison=account_comparison(comparison, row),
      created_by=created_by,
      compared_via=compared_via,
    )


def refresh_reconciliations(
  session: Session,
  body: RefreshReconciliationsRequest,
  *,
  graph_id: str,
  created_by: str,
  compared_via: ComparedVia = "operation",
) -> ReconciliationListResponse:
  """Run every check that applies at the period end and record each result
  on its block. Flushes; the caller owns the commit.

  A synced ledger is compared with its source, on one ledger-wide block.
  Each asset account a schedule carries a balance on is compared with its
  schedules, and each account with a statement recorded in the period with
  that statement, on a block of its own. Run as an ``operation``, a missing
  schedule block is created; it starts out required for close only if it
  ties, so a difference found on first contact is reported without holding
  the close. Run by a ``sync``, only existing blocks are refreshed.

  Raises ``ValueError`` on a malformed period, `NothingToReconcileError`
  when no check applies, the QuickBooks client's own errors, and
  ``RowLockedError`` when another refresh of this graph is in flight.
  """
  create = compared_via == "operation"
  window = reconciliation_window(body.period, get_fiscal_year_start_month(session))

  # The source is read before the write lock, so a slow report holds nothing.
  mirror = None
  skipped: list[str] = []
  mirror_block = find_ledger_reconciliation(session, "source_ledger")
  if create or mirror_block is not None:
    try:
      mirror = SourceLedgerResolver(graph_id).resolve(session, window)
    except NoSourceLedgerError:
      if mirror_block is not None:
        skipped.append(
          f"{mirror_block.name} was not compared: this graph no longer has a "
          "connected source ledger. Its earlier comparisons stand, and a "
          "period it was never compared for stays not started."
        )
  mirror_comparison = (
    compute_reconciliations(session, window=window, side=mirror, include_tied=True)
    if mirror is not None
    else None
  )

  lock_reconciliation_writes(session, graph_id)
  register = _schedule_register_side(session, window)
  statements = _statement_side(session, window)
  if (
    mirror is None
    and not register.covered_element_ids
    and not statements.covered_element_ids
  ):
    raise NothingToReconcileError()

  if mirror is not None and mirror_comparison is not None:
    record_reconciliation(
      session,
      ensure_ledger_reconciliation(
        session, method=mirror.method, source=mirror.source, created_by=created_by
      ),
      window=window,
      side=mirror,
      comparison=mirror_comparison,
      created_by=created_by,
      compared_via=compared_via,
    )

  _record_account_side(
    session,
    register,
    window=window,
    created_by=created_by,
    compared_via=compared_via,
    create=create,
  )
  # A statement block exists only once a statement has been recorded for the
  # account, so there is nothing for a refresh to create.
  _record_account_side(
    session,
    statements,
    window=window,
    created_by=created_by,
    compared_via=compared_via,
    create=False,
  )
  response = list_reconciliations(session, body.period)
  response.notes = skipped
  return response


def _statement_document_exists(graph_id: str, document_id: str) -> bool:
  from robosystems.database import SessionFactory
  from robosystems.models.core import Document

  with SessionFactory() as platform_session:
    return (
      Document.get_by_id_and_graph(document_id, graph_id, platform_session) is not None
    )


def record_statement_balance(
  session: Session,
  body: RecordStatementBalanceRequest,
  *,
  graph_id: str,
  created_by: str,
) -> ReconciliationSummary:
  """Record a statement's ending balance for an account and reconcile the
  account to it for the period the statement ends in. Flushes; the caller
  owns the commit.

  The first statement recorded for an account creates its ``statement``
  block, which does not hold the close until `set_reconciliation_policy`
  says so: a required statement is a statement owed every period. Recording
  the same account and date again replaces the earlier balance.

  Raises `StatementAccountNotFoundError`, `StatementAccountError` when the
  account is not a balance-sheet chart account, `StatementDocumentNotFoundError`,
  and ``RowLockedError`` when another reconciliation write is in flight.
  """
  element = session.get(Element, body.element_id)
  if element is None:
    raise StatementAccountNotFoundError(body.element_id)
  if element.source not in COA_SOURCES or element.period_type != "instant":
    raise StatementAccountError(
      f"{element.name!r} is not a balance-sheet account in the chart of "
      "accounts. A statement states a balance, so only an asset, liability "
      "or equity account can be reconciled to one."
    )
  if body.document_id and not _statement_document_exists(graph_id, body.document_id):
    raise StatementDocumentNotFoundError(body.document_id)

  period = body.as_of.strftime("%Y-%m")
  window = reconciliation_window(period, get_fiscal_year_start_month(session))
  lock_reconciliation_writes(session, graph_id)
  record_statement_observation(
    session,
    element=element,
    as_of=body.as_of,
    # Through the decimal text, so the cents are the ones the caller typed.
    stated_cents=int(
      (Decimal(str(body.balance)) * 100).to_integral_value(rounding=ROUND_HALF_UP)
    ),
    document_id=body.document_id,
    note=body.note,
    created_by=created_by,
  )

  element_id = str(element.id)
  structure = account_reconciliations(session, "statement").get(element_id)
  if structure is None:
    structure = create_account_reconciliation(
      session,
      method="statement",
      element_id=element_id,
      account_name=element.name,
      required_for_close=False,
      created_by=created_by,
    )
  side = StatementResolver(frozenset({element_id})).resolve(session, window)
  comparison = compute_reconciliations(
    session, window=window, side=side, include_tied=True
  )
  (row,) = comparison.rows
  record_reconciliation(
    session,
    structure,
    window=window,
    side=side,
    comparison=account_comparison(comparison, row),
    created_by=created_by,
  )
  return _summary(session, str(structure.id), period)


def refresh_next_period(
  session: Session, *, graph_id: str, created_by: str
) -> str | None:
  """Refresh the next period to close, on a ledger that already reconciles.

  Called after a sync so the close gate is not a step someone has to
  remember. A ledger with no reconciliation block is left alone (returns
  ``None``): the first refresh is a person's decision. Raises as
  `refresh_reconciliations` does.
  """
  if not has_reconciliations(session):
    return None
  calendar = FiscalCalendarService().get(session, graph_id)
  if calendar is None or not calendar.closed_through_period:
    return None
  period = next_period(calendar.closed_through_period)
  refresh_reconciliations(
    session,
    RefreshReconciliationsRequest(period=period),
    graph_id=graph_id,
    created_by=created_by,
    compared_via="sync",
  )
  return period


def sign_off_reconciliation(
  session: Session,
  body: SignOffReconciliationRequest,
  *,
  graph_id: str,
  created_by: str,
) -> ReconciliationSummary:
  """Record the caller's review of a reconciled period. Flushes; the caller
  owns the commit.

  The sign-off stands for the comparison as it is now: a later change to any
  balance at that period end lapses it. Signing off a period already reviewed
  returns it unchanged.

  Raises `ReconciliationNotFoundError`, `ReconciliationNotReconciledError`,
  `NotAGraphMemberError`, `SeparateReviewerError`, and ``ValueError`` on a
  malformed period.
  """
  window = reconciliation_window(body.period, get_fiscal_year_start_month(session))
  # A refresh in flight would replace the comparison being signed.
  lock_reconciliation_writes(session, graph_id)
  structure = _load_reconciliation(session, body.structure_id)
  members = _explicit_write_members(graph_id)
  if created_by not in members:
    raise NotAGraphMemberError()

  rec = _summary(session, str(structure.id), body.period)
  if rec.status == "reviewed":
    return rec
  if rec.status != "reconciled":
    raise ReconciliationNotReconciledError(structure.name, body.period, rec.status)

  fact_set = session.get(FactSet, rec.fact_set_id)
  if fact_set is None:
    raise ReconciliationNotReconciledError(structure.name, body.period, "not_started")
  compared = fact_set.metadata_ or {}
  if rec.separate_reviewer and created_by in preparers(compared):
    supplied = compared.get("prepared_by") == created_by
    if len(members) < 2:
      way_out = (
        "This graph no longer has another member who can write: add one, or "
        "turn `separate_reviewer` off with set-reconciliation-policy."
      )
    elif supplied:
      way_out = "Ask another member to sign off."
    else:
      way_out = (
        "Ask another member to sign off, or to run refresh-reconciliations "
        "so that you can."
      )
    raise SeparateReviewerError(
      f"{structure.name!r} requires a reviewer other than the person who ran "
      "the comparison or recorded its balance, and "
      f"{'you recorded its balance' if supplied else 'you ran this one'}. "
      f"{way_out}"
    )

  if not (fact_set.metadata_ or {}).get("balance_digest"):
    raise ReconciliationNotReconciledError(structure.name, body.period, "unpinned")
  record_sign_off(
    session,
    structure=structure,
    window=window,
    fact_set=fact_set,
    reviewer_id=created_by,
    note=body.note,
  )
  return _summary(session, str(structure.id), body.period)


def set_reconciliation_policy(
  session: Session,
  body: SetReconciliationPolicyRequest,
  *,
  graph_id: str,
  created_by: str,
) -> ReconciliationPolicyResponse:
  """Change a reconciliation's policy: whether the close waits on it, its
  materiality, and whether it needs a review and a separate reviewer.

  The next comparison uses the new materiality; results already recorded are
  not re-judged. Raises `ReconciliationNotFoundError`, and
  `SeparateReviewerError` when a separate reviewer is asked for on a graph
  with fewer than two members who can write.
  """
  structure = _load_reconciliation(session, body.structure_id)

  mechanics = ReconciliationMechanics.model_validate(structure.artifact_mechanics)
  before = mechanics.model_dump(mode="json")
  if body.required_for_close is not None:
    mechanics.required_for_close = body.required_for_close
  if body.review_required is not None:
    mechanics.review_required = body.review_required
  if body.separate_reviewer is not None:
    if body.separate_reviewer and not mechanics.separate_reviewer:
      members = len(_explicit_write_members(graph_id))
      if members < 2:
        raise SeparateReviewerError(
          "A separate reviewer needs at least two members of the graph who "
          f"can write; this graph has {members}. Add a member first."
        )
    mechanics.separate_reviewer = body.separate_reviewer
  if body.materiality is not None:
    mechanics.materiality = body.materiality
    rule = reconciliation_rule(session, str(structure.id))
    if rule is not None:
      rule.metadata_ = {**(rule.metadata_ or {}), "tolerance": body.materiality}
      flag_modified(rule, "metadata_")
  after = mechanics.model_dump(mode="json")
  structure.artifact_mechanics = after
  session.flush()
  changes = {
    field: {"from": before[field], "to": after[field]}
    for field in after
    if before[field] != after[field]
  }
  if changes:
    record_policy_change(
      session, structure=structure, changes=changes, changed_by=created_by
    )

  return ReconciliationPolicyResponse(
    structure_id=str(structure.id),
    required_for_close=mechanics.required_for_close,
    materiality=mechanics.materiality,
    review_required=mechanics.review_required,
    separate_reviewer=mechanics.separate_reviewer,
  )
