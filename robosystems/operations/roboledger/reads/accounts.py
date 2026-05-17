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
from robosystems.models.extensions import (
  Element,
  ElementTrait,
  Trait,
)
from robosystems.models.extensions.roboledger import COA_SOURCES
from robosystems.operations.library.reads import efs_trait_by_element


def _parse_meta(raw: Any) -> dict[str, Any]:
  if isinstance(raw, dict):
    return raw
  if isinstance(raw, str):
    try:
      return json.loads(raw)
    except (ValueError, TypeError):
      return {}
  return {}


# Local alias for the shared library helper.
_efs_by_element = efs_trait_by_element


def account_to_response(row: Element, trait: str | None = None) -> AccountResponse:
  """Map an Element row to the wire-facing AccountResponse.

  Callers that batch-load elements should also batch-load the FASB EFS
  trait via :func:`_efs_by_element` and pass it through, so
  list endpoints avoid N+1 lookups.
  """
  meta = _parse_meta(row.metadata_)
  return AccountResponse(
    id=row.id,
    code=row.code,
    name=row.name,
    description=row.description,
    trait=trait,
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
  trait: str | None = None,
  is_active: bool | None = None,
  limit: int = 100,
  offset: int = 0,
) -> AccountListResponse:
  """List Chart of Accounts elements filtered by trait + is_active.

  ``trait`` filters on the FASB elementsOfFinancialStatements
  trait via the element_traits junction table.
  """
  query = select(Element).where(Element.source.in_(COA_SOURCES))
  count_query = (
    select(func.count()).select_from(Element).where(Element.source.in_(COA_SOURCES))
  )

  if trait is not None:
    subquery = (
      select(ElementTrait.element_id)
      .join(Trait, Trait.id == ElementTrait.trait_id)
      .where(
        Trait.category == "elementsOfFinancialStatements",
        Trait.identifier == trait,
      )
    )
    query = query.where(Element.id.in_(subquery))
    count_query = count_query.where(Element.id.in_(subquery))
  if is_active is not None:
    query = query.where(Element.is_active == is_active)
    count_query = count_query.where(Element.is_active == is_active)

  total = session.execute(count_query).scalar() or 0
  rows = (
    session.execute(query.order_by(Element.code).offset(offset).limit(limit))
    .scalars()
    .all()
  )

  efs_map = _efs_by_element(session, [r.id for r in rows])
  return AccountListResponse(
    accounts=[account_to_response(r, efs_map.get(r.id)) for r in rows],
    pagination=create_pagination_info(total, limit, offset),
  )


def get_account_tree(
  session: Session, *, include_inactive: bool = False
) -> AccountTreeResponse:
  """Return the Chart of Accounts as a parent/child tree.

  Filters to ``is_active=True`` by default. Inactive accounts (deleted
  in the source system but still referenced by historical journal
  lines — see the QB adapter's ``Active IN (true, false)`` fetch) are
  load-bearing for the materializer's foreign-key integrity but clutter
  every CoA-facing view. Pass ``include_inactive=True`` to surface them
  (admin / cleanup contexts only).
  """
  query = select(Element).where(Element.source.in_(COA_SOURCES))
  if not include_inactive:
    query = query.where(Element.is_active.is_(True))
  rows = session.execute(query.order_by(Element.code)).scalars().all()

  efs_map = _efs_by_element(session, [r.id for r in rows])
  nodes: dict[str, AccountTreeNode] = {}
  roots: list[AccountTreeNode] = []

  for r in rows:
    meta = _parse_meta(r.metadata_)
    node = AccountTreeNode(
      id=r.id,
      code=r.code,
      name=r.name,
      trait=efs_map.get(r.id),
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
