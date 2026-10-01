"""Reconciliation commands."""

from __future__ import annotations

from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import flag_modified

from robosystems.models.api.extensions.reconciliations import (
  PreviewReconciliationsRequest,
  ReconciliationListResponse,
  ReconciliationPolicyResponse,
  ReconciliationPreviewResponse,
  RefreshReconciliationsRequest,
  SetReconciliationPolicyRequest,
)
from robosystems.models.api.information_block import ReconciliationMechanics
from robosystems.models.extensions import Structure
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
  ensure_ledger_reconciliation,
  find_ledger_reconciliation,
  lock_reconciliation_writes,
  reconciliation_rule,
  record_ledger_reconciliation,
)


class ReconciliationNotFoundError(Exception):
  def __init__(self, structure_id: str) -> None:
    super().__init__(f"Reconciliation {structure_id!r} not found.")
    self.structure_id = structure_id


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
) -> ReconciliationListResponse:
  """Compare at the period end and record the result on the block, creating
  the block on first use. Flushes; the caller owns the commit.

  Raises as `preview_reconciliations` does, and ``RowLockedError`` when
  another refresh of this graph is in flight.
  """
  window = reconciliation_window(body.period, get_fiscal_year_start_month(session))
  # The source is read before the write lock, so a slow report holds nothing.
  side = SourceLedgerResolver(graph_id).resolve(session, window)
  comparison = compute_reconciliations(session, window=window, side=side)

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
  )
  return period


def set_reconciliation_policy(
  session: Session, body: SetReconciliationPolicyRequest, *, created_by: str
) -> ReconciliationPolicyResponse:
  """Change whether the close waits on a reconciliation, and its materiality.

  The next comparison uses the new materiality; results already recorded are
  not re-judged. Raises `ReconciliationNotFoundError`.
  """
  structure = lock_by_id(
    session,
    Structure,
    body.structure_id,
    f"Reconciliation {body.structure_id} is being written by another "
    "process. Retry in a moment.",
  )
  if structure is None or structure.block_type != RECONCILIATION_BLOCK_TYPE:
    raise ReconciliationNotFoundError(body.structure_id)

  mechanics = ReconciliationMechanics.model_validate(structure.artifact_mechanics)
  if body.required_for_close is not None:
    mechanics.required_for_close = body.required_for_close
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
  )
