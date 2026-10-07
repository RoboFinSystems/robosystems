"""The group's bank and card accounts: every chart account a feed books to
or a source system calls a bank account, with the entity it belongs to and
the health of whatever writes to it."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from robosystems.adapters.bank_feed.accounts import BANK_FEED_KEY, account_entities
from robosystems.models.api.extensions.bank_accounts import (
  BankAccountListResponse,
  BankAccountResponse,
)
from robosystems.models.extensions import Element, Entity
from robosystems.operations.roboledger.entity_scope import (
  find_entity_id,
  resolve_entity_id,
)
from robosystems.operations.roboledger.reads.accounts import coa_element_clause

# QuickBooks' own account types for the accounts a bank statement covers.
QB_BANK_ACCOUNT_TYPES = ("Bank", "Credit Card")


@dataclass(frozen=True)
class ConnectionHealth:
  """What the platform knows about a connection, keyed by its id. Read on
  the platform database by the caller; this module never opens it."""

  status: str | None
  institution: str | None = None
  last_sync_at: datetime | None = None
  last_sync_status: str | None = None


def bank_account_clause():
  """Predicate for a chart account a bank statement covers: one a feed
  books to, or one its source system types as a bank or card account."""
  return or_(
    Element.metadata_.has_key(BANK_FEED_KEY),
    Element.metadata_["account_type"].astext.in_(QB_BANK_ACCOUNT_TYPES),
  )


def list_bank_accounts(
  session: Session,
  *,
  entity_id: str | None = None,
  connections: Mapping[str, ConnectionHealth] | None = None,
) -> BankAccountListResponse:
  """Every bank and card account across the group, or ``entity_id``'s."""
  parent_id = find_entity_id(session)
  rows = (
    session.execute(
      select(Element)
      .where(coa_element_clause(), bank_account_clause())
      .order_by(Element.code, Element.name, Element.id)
    )
    .scalars()
    .all()
  )
  owners = account_entities(session, [row.id for row in rows], parent_id=parent_id)
  if entity_id is not None:
    wanted = resolve_entity_id(session, entity_id)
    rows = [row for row in rows if owners.get(str(row.id)) == wanted]
  names = {
    str(eid): str(name)
    for eid, name in session.execute(select(Entity.id, Entity.name)).all()
  }
  health = connections or {}
  accounts = [_row(row, owners.get(str(row.id)), names, health) for row in rows]
  return BankAccountListResponse(accounts=accounts, total=len(accounts))


def _row(
  element: Element,
  entity_id: str | None,
  names: Mapping[str, str],
  health: Mapping[str, ConnectionHealth],
) -> BankAccountResponse:
  raw = element.metadata_
  meta: dict[str, Any] = raw if isinstance(raw, dict) else {}
  link: dict[str, Any] = meta.get(BANK_FEED_KEY) or {}
  source: str | None = None
  connection_id: str | None = None
  institution: str | None = None
  if link:
    source = link.get("provider")
    connection_id = link.get("connection_id")
    institution = link.get("institution")
  elif element.external_source:
    source = str(element.external_source)
    connection_id = str(element.connection_id) if element.connection_id else None
  conn = health.get(connection_id) if connection_id else None
  return BankAccountResponse(
    id=str(element.id),
    code=element.code,
    name=str(element.name),
    kind="credit" if element.balance_type == "credit" else "bank",
    balance_type=str(element.balance_type),
    is_active=bool(element.is_active),
    entity_id=entity_id,
    entity_name=names.get(entity_id) if entity_id else None,
    source=source,
    connection_id=connection_id,
    institution=institution or (conn.institution if conn else None),
    feed_account_id=link.get("account_id"),
    feed_account_name=link.get("account_name"),
    feed_account_kind=link.get("kind"),
    connection_status=conn.status if conn else None,
    last_sync_at=conn.last_sync_at if conn else None,
    last_sync_status=conn.last_sync_status if conn else None,
  )
