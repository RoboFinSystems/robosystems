"""Ledger summary endpoint."""

import logging

from fastapi import APIRouter, Depends, HTTPException, Path
from sqlalchemy import func, select
from sqlalchemy.exc import ProgrammingError

from robosystems.db.extensions import extensions_session
from robosystems.db.platform import get_db_session
from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.middleware.graph.types import GRAPH_OR_SUBGRAPH_ID_PATTERN
from robosystems.middleware.rate_limits import subscription_aware_rate_limit_dependency
from robosystems.models.api.extensions.summary import LedgerSummaryResponse
from robosystems.models.extensions import Element, Entry, LineItem, Transaction
from robosystems.models.iam import User
from robosystems.models.iam.connection import Connection

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get(
  "/summary",
  response_model=LedgerSummaryResponse,
  operation_id="getLedgerSummary",
  summary="Ledger Summary",
  tags=["Ledger"],
)
async def get_summary(
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
):
  try:
    with extensions_session(graph_id) as session:
      account_count = (
        session.execute(select(func.count()).select_from(Element)).scalar() or 0
      )
      transaction_count = (
        session.execute(select(func.count()).select_from(Transaction)).scalar() or 0
      )
      entry_count = (
        session.execute(select(func.count()).select_from(Entry)).scalar() or 0
      )
      line_item_count = (
        session.execute(select(func.count()).select_from(LineItem)).scalar() or 0
      )

      date_range = session.execute(
        select(func.min(Transaction.date), func.max(Transaction.date))
      ).one()
      earliest = date_range[0]
      latest = date_range[1]
  except ValueError:
    raise HTTPException(
      status_code=404,
      detail="Ledger not initialized. Connect a data source first.",
    )
  except ProgrammingError:
    raise HTTPException(
      status_code=404,
      detail="Ledger not initialized. Connect a data source first.",
    )

  # Connection info from platform DB
  connection_count = 0
  last_sync_at = None
  gen = None
  try:
    gen = get_db_session()
    db = next(gen)
    conn_result = db.execute(
      select(func.count(), func.max(Connection.last_sync)).where(
        Connection.graph_id == graph_id
      )
    ).one()
    connection_count = conn_result[0] or 0
    last_sync_at = conn_result[1]
  except Exception:
    logger.warning(
      "Failed to fetch connection metadata for %s", graph_id, exc_info=True
    )
  finally:
    if gen is not None and hasattr(gen, "close"):
      gen.close()

  return LedgerSummaryResponse(
    graph_id=graph_id,
    account_count=account_count,
    transaction_count=transaction_count,
    entry_count=entry_count,
    line_item_count=line_item_count,
    earliest_transaction_date=earliest,
    latest_transaction_date=latest,
    connection_count=connection_count,
    last_sync_at=last_sync_at,
  )
