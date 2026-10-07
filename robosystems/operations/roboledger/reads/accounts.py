"""Account (Chart of Accounts) read operations. Each entity has its own
chart, so every read here is one entity's accounts."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from sqlalchemy import ColumnElement, and_, func, or_, select
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
  EntityTaxonomy,
  Taxonomy,
  Trait,
)
from robosystems.models.extensions.roboledger import COA_SOURCES
from robosystems.operations.library.reads import efs_trait_by_element
from robosystems.operations.roboledger.entity_scope import (
  find_entity_id,
  is_group_parent,
  resolve_entity_id,
)
from robosystems.operations.taxonomy_block.coa_mappings import (
  CHART_LINK_BASIS,
  mapping_owner_id,
)


def coa_element_clause():
  """Predicate for a Chart-of-Accounts element.

  ``source`` alone is not enough: envelope-authored framework concepts also
  carry ``source='native'``. Adapter-imported accounts have no
  ``taxonomy_id``, so NULL-taxonomy rows stay in.
  """
  return and_(
    Element.source.in_(COA_SOURCES),
    or_(
      Element.taxonomy_id.is_(None),
      Element.taxonomy_id.in_(
        select(Taxonomy.id).where(Taxonomy.taxonomy_type == "chart_of_accounts")
      ),
    ),
  )


@dataclass(frozen=True)
class AccountScope:
  """Whose chart accounts a read returns.

  ``with_unowned`` adds the accounts in no entity's chart: a chart from before
  charts were linked to an entity, and synced accounts not yet filed under
  one. Those are the group parent's.
  """

  entity_id: str | None
  with_unowned: bool

  def params(self) -> dict[str, Any]:
    """Bind values for `OWNED_ACCOUNT_SQL`."""
    return {
      "scope_entity_id": self.entity_id,
      "scope_with_unowned": self.with_unowned,
    }


def account_scope(session: Session, entity_id: str | None = None) -> AccountScope:
  """The scope for one entity's accounts, default the group parent's. On a
  graph with no entity yet it is the unowned accounts alone."""
  if entity_id is None:
    return AccountScope(find_entity_id(session), True)
  entity_id = resolve_entity_id(session, entity_id)
  return AccountScope(entity_id, is_group_parent(session, entity_id))


def mapping_account_scope(session: Session, mapping_id: str) -> AccountScope:
  """The scope of the accounts a mapping is for: those of the entity whose
  chart it maps from."""
  owner_id = mapping_owner_id(session, mapping_id)
  if owner_id is None:
    return AccountScope(find_entity_id(session), True)
  return AccountScope(owner_id, is_group_parent(session, owner_id))


def _owned_charts():
  return select(EntityTaxonomy.taxonomy_id).where(
    EntityTaxonomy.basis == CHART_LINK_BASIS
  )


def entity_accounts_clause(scope: AccountScope) -> ColumnElement[bool]:
  """Predicate for the Chart-of-Accounts elements in ``scope``."""
  owned = Element.taxonomy_id.in_(
    _owned_charts().where(EntityTaxonomy.entity_id == scope.entity_id)
  )
  if not scope.with_unowned:
    return and_(coa_element_clause(), owned)
  unowned = or_(
    Element.taxonomy_id.is_(None), ~Element.taxonomy_id.in_(_owned_charts())
  )
  return and_(coa_element_clause(), or_(owned, unowned))


# The same ownership test for raw SQL over an ``elements`` alias, bound with
# `AccountScope.params`. Pair it with the caller's own chart-account test.
OWNED_ACCOUNT_SQL = """(
    {alias}.taxonomy_id IN (
      SELECT taxonomy_id FROM entity_taxonomies
      WHERE basis = 'chart_of_accounts' AND entity_id = :scope_entity_id
    )
    OR (:scope_with_unowned AND (
      {alias}.taxonomy_id IS NULL
      OR {alias}.taxonomy_id NOT IN (
        SELECT taxonomy_id FROM entity_taxonomies
        WHERE basis = 'chart_of_accounts'
      )
    ))
  )"""


def _parse_meta(raw: Any) -> dict[str, Any]:
  if isinstance(raw, dict):
    return raw
  if isinstance(raw, str):
    try:
      return json.loads(raw)
    except (ValueError, TypeError):
      return {}
  return {}


_efs_by_element = efs_trait_by_element


def account_to_response(row: Element, trait: str | None = None) -> AccountResponse:
  """Batch callers should load EFS traits once and pass ``trait`` in."""
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
  entity_id: str | None = None,
) -> AccountListResponse:
  """List one entity's CoA elements, default the group parent's; ``trait`` is
  the FASB elementsOfFinancialStatements trait."""
  accounts = entity_accounts_clause(account_scope(session, entity_id))
  query = select(Element).where(accounts)
  count_query = select(func.count()).select_from(Element).where(accounts)

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
  session: Session,
  *,
  include_inactive: bool = False,
  entity_id: str | None = None,
) -> AccountTreeResponse:
  """Return one entity's Chart of Accounts as a parent/child tree, default
  the group parent's.

  Inactive accounts (retired upstream, kept for historical lines) are
  excluded unless ``include_inactive``.
  """
  query = select(Element).where(
    entity_accounts_clause(account_scope(session, entity_id))
  )
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
