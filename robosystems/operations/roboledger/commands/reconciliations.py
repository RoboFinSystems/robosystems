"""Reconciliation commands."""

from __future__ import annotations

from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import flag_modified

from robosystems.models.api.extensions.reconciliations import (
  PreviewReconciliationsRequest,
  ReconciliationListResponse,
  ReconciliationPolicyResponse,
  ReconciliationPreviewResponse,
  ReconciliationSummary,
  RefreshReconciliationsRequest,
  SetReconciliationPolicyRequest,
  SignOffReconciliationRequest,
)
from robosystems.models.api.information_block import ReconciliationMechanics
from robosystems.models.extensions import Structure
from robosystems.models.extensions.roboledger import FactSet
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
  SourceLedgerResolver,
  compute_reconciliations,
  reconciliation_window,
)
from robosystems.operations.roboledger.reconciliations.blocks import (
  ComparedVia,
  ensure_ledger_reconciliation,
  find_ledger_reconciliation,
  lock_reconciliation_writes,
  reconciliation_rule,
  record_ledger_reconciliation,
  record_sign_off,
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
  """Compare the ledger at a period end with its synced source. Writes nothing.

  Raises ``ValueError`` on a malformed period, `NoSourceLedgerError` when the
  graph has no synced ledger, and the QuickBooks client's own errors.
  """
  window = reconciliation_window(body.period, get_fiscal_year_start_month(session))
  side = SourceLedgerResolver(graph_id).resolve(session, window)
  return compute_reconciliations(
    session, window=window, side=side, include_tied=body.include_tied
  )


def refresh_reconciliations(
  session: Session,
  body: RefreshReconciliationsRequest,
  *,
  graph_id: str,
  created_by: str,
  compared_via: ComparedVia = "operation",
) -> ReconciliationListResponse:
  """Compare at the period end and record the result on the block, creating
  the block on first use. Flushes; the caller owns the commit.

  Raises as `preview_reconciliations` does, and ``RowLockedError`` when
  another refresh of this graph is in flight.
  """
  window = reconciliation_window(body.period, get_fiscal_year_start_month(session))
  # The source is read before the write lock, so a slow report holds nothing.
  side = SourceLedgerResolver(graph_id).resolve(session, window)
  comparison = compute_reconciliations(
    session, window=window, side=side, include_tied=True
  )

  lock_reconciliation_writes(session, graph_id)
  structure = ensure_ledger_reconciliation(
    session, method=side.method, source=side.source, created_by=created_by
  )
  record_ledger_reconciliation(
    session,
    structure,
    window=window,
    side=side,
    comparison=comparison,
    created_by=created_by,
    compared_via=compared_via,
  )
  return list_reconciliations(session, body.period)


def refresh_next_period(
  session: Session, *, graph_id: str, created_by: str
) -> str | None:
  """Refresh the next period to close, on a ledger that already reconciles.

  Called after a sync so the close gate is not a step someone has to
  remember. A ledger with no reconciliation block is left alone (returns
  ``None``): the first refresh is a person's decision. Raises as
  `refresh_reconciliations` does.
  """
  if find_ledger_reconciliation(session, "source_ledger") is None:
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

  if (
    rec.separate_reviewer
    and rec.compared_via == "operation"
    and rec.compared_by == created_by
  ):
    way_out = (
      "Ask another member to sign off, or to run refresh-reconciliations so "
      "that you can."
      if len(members) > 1
      else "This graph no longer has another member who can write: add one, "
      "or turn `separate_reviewer` off with set-reconciliation-policy."
    )
    raise SeparateReviewerError(
      f"{structure.name!r} requires a reviewer other than the person who ran "
      f"the comparison, and you ran this one. {way_out}"
    )

  fact_set = session.get(FactSet, rec.fact_set_id)
  if fact_set is None:
    raise ReconciliationNotReconciledError(structure.name, body.period, "not_started")
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
  structure.artifact_mechanics = mechanics.model_dump(mode="json")
  session.flush()

  return ReconciliationPolicyResponse(
    structure_id=str(structure.id),
    required_for_close=mechanics.required_for_close,
    materiality=mechanics.materiality,
    review_required=mechanics.review_required,
    separate_reviewer=mechanics.separate_reviewer,
  )
