"""Ledger account endpoints."""

import json

from fastapi import APIRouter, Depends, HTTPException, Path, Query
from sqlalchemy import func, select
from sqlalchemy.exc import ProgrammingError

from robosystems.db.extensions import extensions_session
from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.middleware.graph.types import GRAPH_OR_SUBGRAPH_ID_PATTERN
from robosystems.middleware.rate_limits import subscription_aware_rate_limit_dependency
from robosystems.models.api.common import create_pagination_info
from robosystems.models.api.extensions.accounts import (
  AccountListResponse,
  AccountResponse,
  AccountTreeNode,
  AccountTreeResponse,
)
from robosystems.models.core import User
from robosystems.models.extensions import Element

router = APIRouter()


def _parse_meta(raw) -> dict:
  if isinstance(raw, dict):
    return raw
  if isinstance(raw, str):
    try:
      return json.loads(raw)
    except (ValueError, TypeError):
      return {}
  return {}


def _account_to_response(row: Element) -> AccountResponse:
  meta = _parse_meta(row.metadata_)
  return AccountResponse(
    id=row.id,
    code=row.code,
    name=row.name,
    description=row.description,
    classification=row.classification,
    sub_classification=row.sub_classification,
    balance_type=row.balance_type,
    parent_id=row.parent_id,
    depth=row.depth,
    currency=row.currency,
    is_active=row.is_active,
    is_placeholder=row.is_placeholder,
    account_type=meta.get("account_type"),
    external_id=row.external_id,
    external_source=row.external_source,
  )


@router.get(
  "/accounts",
  response_model=AccountListResponse,
  operation_id="listLedgerAccounts",
  summary="List Accounts",
  tags=["Ledger"],
)
async def list_accounts(
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  classification: str | None = Query(None, description="Filter by classification"),
  is_active: bool | None = Query(None, description="Filter by active status"),
  limit: int = Query(100, ge=1, le=1000),
  offset: int = Query(0, ge=0),
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
):
  try:
    with extensions_session(graph_id) as session:
      # Only return company CoA elements, not seed taxonomy elements
      from robosystems.models.extensions.roboledger import COA_SOURCES

      query = select(Element).where(Element.source.in_(COA_SOURCES))
      count_query = (
        select(func.count()).select_from(Element).where(Element.source.in_(COA_SOURCES))
      )

      if classification is not None:
        query = query.where(Element.classification == classification)
        count_query = count_query.where(Element.classification == classification)
      if is_active is not None:
        query = query.where(Element.is_active == is_active)
        count_query = count_query.where(Element.is_active == is_active)

      total = session.execute(count_query).scalar() or 0
      rows = (
        session.execute(query.order_by(Element.code).offset(offset).limit(limit))
        .scalars()
        .all()
      )

      return AccountListResponse(
        accounts=[_account_to_response(r) for r in rows],
        pagination=create_pagination_info(total, limit, offset),
      )
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


@router.get(
  "/accounts/tree",
  response_model=AccountTreeResponse,
  operation_id="getLedgerAccountTree",
  summary="Account Tree",
  tags=["Ledger"],
)
async def get_account_tree(
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
):
  try:
    with extensions_session(graph_id) as session:
      # Only return company CoA elements, not seed taxonomy elements
      from robosystems.models.extensions.roboledger import COA_SOURCES

      rows = (
        session.execute(
          select(Element).where(Element.source.in_(COA_SOURCES)).order_by(Element.code)
        )
        .scalars()
        .all()
      )

      nodes: dict[str, AccountTreeNode] = {}
      roots: list[AccountTreeNode] = []

      for r in rows:
        meta = _parse_meta(r.metadata_)
        node = AccountTreeNode(
          id=r.id,
          code=r.code,
          name=r.name,
          classification=r.classification,
          account_type=meta.get("account_type"),
          balance_type=r.balance_type,
          depth=r.depth,
          is_active=r.is_active,
        )
        nodes[r.id] = node

      for r in rows:
        node = nodes[r.id]
        if r.parent_id and r.parent_id in nodes:
          nodes[r.parent_id].children.append(node)
        else:
          roots.append(node)

      return AccountTreeResponse(roots=roots, total_accounts=len(rows))
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
