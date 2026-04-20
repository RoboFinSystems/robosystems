"""Account (Chart of Accounts) read operations."""

from __future__ import annotations

import json
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from robosystems.models.api.common import create_pagination_info
from robosystems.models.api.extensions.accounts import (
  AccountListResponse,
  AccountResponse,
  AccountTreeNode,
  AccountTreeResponse,
)
from robosystems.models.extensions import Element
from robosystems.models.extensions.roboledger import COA_SOURCES


def _parse_meta(raw: Any) -> dict[str, Any]:
  if isinstance(raw, dict):
    return raw
  if isinstance(raw, str):
    try:
      return json.loads(raw)
    except (ValueError, TypeError):
      return {}
  return {}


def account_to_response(row: Element) -> AccountResponse:
  """Map an Element row to the wire-facing AccountResponse."""
  meta = _parse_meta(row.metadata_)
  return AccountResponse(
    id=row.id,
    code=row.code,
    name=row.name,
    description=row.description,
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


def list_accounts(
  session: Session,
  *,
  classification: str | None = None,
  is_active: bool | None = None,
  limit: int = 100,
  offset: int = 0,
) -> AccountListResponse:
  """List Chart of Accounts elements filtered by classification + is_active."""
  query = select(Element).where(Element.source.in_(COA_SOURCES))
  count_query = (
    select(func.count()).select_from(Element).where(Element.source.in_(COA_SOURCES))
  )

  if classification is not None:
    pass  # classification filter removed; column no longer on Element
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
    accounts=[account_to_response(r) for r in rows],
    pagination=create_pagination_info(total, limit, offset),
  )


def get_account_tree(session: Session) -> AccountTreeResponse:
  """Return the Chart of Accounts as a parent/child tree."""
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
