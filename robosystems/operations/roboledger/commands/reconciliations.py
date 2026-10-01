"""Reconciliation commands."""

from __future__ import annotations

from sqlalchemy.orm import Session

from robosystems.models.api.extensions.reconciliations import (
  PreviewReconciliationsRequest,
  ReconciliationPreviewResponse,
)
from robosystems.operations.roboledger.reads.fiscal_calendar import (
  get_fiscal_year_start_month,
)
from robosystems.operations.roboledger.reconciliations import (
  SourceLedgerResolver,
  compute_reconciliations,
  reconciliation_window,
)


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
