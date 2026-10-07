"""Link or create — one chart account per bank account the feed exposes.

Each account is **linked** to a matching chart account or, when nothing
matches, **created** as an ordinary tenant ``coa:*`` element through the
TaxonomyBlock envelope, as if the customer had added it.

The link lives in the element's ``metadata.bank_feed`` — never in its
``source`` or its qname — so the chart stays the tenant's. A created element
also carries ``external_source`` + ``external_id`` for provenance.

The chart the account is in is the entity whose books the feed's lines go
into. A new account lands in the group parent's chart; ``link-bank-account``
moves its link to an account in a subsidiary's chart, and from then on the
feed books there. A link is found wherever it is, so nothing is created twice.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from robosystems.adapters.bank_feed.chart import BankAccount, ChartIndex, name_key
from robosystems.adapters.bank_feed.hints import slug
from robosystems.logger import logger
from robosystems.models.api.taxonomy_block import (
  TaxonomyBlockElementRequest,
  UpdateTaxonomyBlockRequest,
)
from robosystems.models.extensions import Element, Taxonomy
from robosystems.operations.roboledger.commands.chart_of_accounts import (
  active_chart_id,
)
from robosystems.operations.roboledger.reads.accounts import coa_element_clause
from robosystems.operations.taxonomy_block.chart_of_accounts import (
  update as update_chart_block,
)

BANK_FEED_KEY = "bank_feed"
# Code ranges for accounts the feed has to create, when the chart uses codes:
# cash-side assets in the 10xx block, cards and credit lines in the 21xx block.
ASSET_CODE_BASE = 1010
LIABILITY_CODE_BASE = 2110
CODE_STEP = 10


class ChartRequiredError(RuntimeError):
  """The graph has no chart of accounts; the provider guard should have
  refused the connection (``CHART_REQUIRED``)."""


@dataclass(frozen=True)
class AccountLinkResult:
  links: dict[str, str]
  created: int
  linked: int


def build_chart_index(session: Session, entity_id: str | None = None) -> ChartIndex:
  """Every account on the entity's chart, default the group parent's, by
  normalized name and by code."""
  chart_id = active_chart_id(session, entity_id)
  if chart_id is None:
    return ChartIndex()
  rows = session.execute(
    select(Element.id, Element.name, Element.code).where(
      Element.taxonomy_id == chart_id, Element.is_active.is_(True)
    )
  ).all()
  index = ChartIndex()
  for element_id, name, code in rows:
    if name:
      index.by_name.setdefault(name_key(str(name)), str(element_id))
    if code:
      index.by_code.setdefault(str(code).strip(), str(element_id))
  return index


def chart_indexes(session: Session, entity_ids: Iterable[str]) -> dict[str, ChartIndex]:
  """One chart index per entity, for the suggestions on each entity's lines."""
  return {
    entity_id: build_chart_index(session, entity_id)
    for entity_id in sorted({eid for eid in entity_ids if eid})
  }


_ENTITY_OF_ACCOUNT_SQL = text("""
  SELECT e.id AS element_id, owner.entity_id
  FROM elements e
  LEFT JOIN entity_taxonomies owner
    ON owner.taxonomy_id = e.taxonomy_id AND owner.basis = 'chart_of_accounts'
  WHERE e.id = ANY(:element_ids)
""")


def account_entities(
  session: Session, element_ids: Iterable[str | None], *, parent_id: str | None
) -> dict[str, str | None]:
  """The entity each chart account's lines belong to: the owner of its chart,
  else the group parent (an account in no entity's chart is the parent's)."""
  ids = sorted({str(eid) for eid in element_ids if eid})
  if not ids:
    return {}
  rows = session.execute(_ENTITY_OF_ACCOUNT_SQL, {"element_ids": ids}).all()
  return {
    str(row.element_id): (str(row.entity_id) if row.entity_id else parent_id)
    for row in rows
  }


def chart_accounts(session: Session) -> list[Element]:
  """Every account on every chart of the graph."""
  return list(
    session.execute(select(Element).where(coa_element_clause())).scalars().all()
  )


def link_bank_accounts(
  session: Session,
  accounts: list[BankAccount],
  *,
  provider: str,
  connection_id: str,
  created_by: str,
  entity_id: str | None = None,
) -> AccountLinkResult:
  """Return ``{feed_account_id: element_id}`` for every account, creating
  the ones nothing on the chart matches. Flushes; the caller commits.

  A link is honoured in whichever entity's chart it sits; a name match and a
  new account are on the chart of ``entity_id``, the entity the feed was
  connected for (default the group parent).
  """
  chart_id = active_chart_id(session, entity_id)
  if chart_id is None:
    raise ChartRequiredError(
      f"Entity {entity_id!r} has no chart of accounts; initialize one before "
      "syncing a bank feed."
      if entity_id
      else "This graph has no chart of accounts; initialize one before syncing a "
      "bank feed."
    )

  elements = chart_accounts(session)
  home = [element for element in elements if element.taxonomy_id == chart_id]
  by_feed: dict[str, Element] = {}
  by_name: dict[str, Element] = {}
  for element in elements:
    link = (element.metadata_ or {}).get(BANK_FEED_KEY) or {}
    if link.get("provider") == provider and link.get("account_id"):
      by_feed[str(link["account_id"])] = element
  for element in home:
    link = (element.metadata_ or {}).get(BANK_FEED_KEY) or {}
    # An account another connection feeds is never claimed by name: two
    # feeds sharing one chart account would overwrite each other's link on
    # every sync. The same connection may re-claim its own (a replaced Plaid
    # Item brings new account ids for the same accounts).
    owned_elsewhere = bool(link) and (
      link.get("provider") != provider or link.get("connection_id") != connection_id
    )
    if element.name and element.is_active and not owned_elsewhere:
      by_name.setdefault(name_key(str(element.name)), element)

  links: dict[str, str] = {}
  linked = 0
  to_create: list[BankAccount] = []
  for account in accounts:
    element = by_feed.get(account.account_id) or by_name.get(name_key(account.name))
    if element is None:
      to_create.append(account)
      continue
    links[account.account_id] = str(element.id)
    linked += 1
    if account.account_id not in by_feed:
      element.metadata_ = {
        **(element.metadata_ or {}),
        BANK_FEED_KEY: _link(account, provider, connection_id),
      }

  if to_create:
    created = create_chart_accounts(
      session,
      chart_id,
      home,
      to_create,
      provider=provider,
      connection_id=connection_id,
      created_by=created_by,
    )
    links.update(created)

  session.flush()
  return AccountLinkResult(links=links, created=len(to_create), linked=linked)


def _link(account: BankAccount, provider: str, connection_id: str) -> dict[str, str]:
  return {
    "provider": provider,
    "account_id": account.account_id,
    "account_name": account.name,
    "institution": account.institution,
    "kind": account.kind,
    "connection_id": connection_id,
  }


def feed_account(link: dict[str, Any], *, balance_type: str) -> BankAccount:
  """The feed's account as its link records it, for creating its chart
  account again in another entity's chart."""
  return BankAccount(
    account_id=str(link["account_id"]),
    name=str(link.get("account_name") or link["account_id"]),
    kind=str(link.get("kind") or "account"),
    trait="liability" if balance_type == "credit" else "asset",
    balance_type=balance_type,
    institution=str(link.get("institution") or "Bank"),
  )


def create_chart_accounts(
  session: Session,
  chart_id: str,
  existing: list[Element],
  accounts: list[BankAccount],
  *,
  provider: str,
  connection_id: str,
  created_by: str,
) -> dict[str, str]:
  """Create one account per feed account on ``chart_id`` through the
  envelope, codes allocated against ``existing`` (that chart's accounts), and
  return ``{feed_account_id: element_id}``.

  Qnames take the chart's own prefix (a subsidiary's chart has one of its
  own) and are unique across the graph, not the chart.
  """
  uses_codes = any(element.code for element in existing)
  taken_codes = {str(element.code).strip() for element in existing if element.code}
  prefix = chart_qname_prefix(session, chart_id)
  taken = taken_qnames(session) | {
    str(element.qname) for element in existing if element.qname
  }

  requests: list[TaxonomyBlockElementRequest] = []
  qname_by_account: dict[str, str] = {}
  for account in accounts:
    code = (
      _next_code(
        taken_codes,
        LIABILITY_CODE_BASE if account.is_credit else ASSET_CODE_BASE,
      )
      if uses_codes
      else None
    )
    qname = _unique_qname(taken, code or slug(account.name), prefix)
    qname_by_account[account.account_id] = qname
    requests.append(
      TaxonomyBlockElementRequest(
        qname=qname,
        name=account.name,
        trait=account.trait,
        balance_type=account.balance_type,
        period_type="instant",
        code=code,
        description=(
          f"{account.institution} {account.kind} account, added by the bank feed."
        ),
        metadata={BANK_FEED_KEY: _link(account, provider, connection_id)},
      )
    )

  update_chart_block(
    session,
    UpdateTaxonomyBlockRequest(taxonomy_id=chart_id, elements_to_add=requests),
    created_by,
  )

  rows = session.execute(
    select(Element).where(
      Element.taxonomy_id == chart_id,
      Element.qname.in_(list(qname_by_account.values())),
    )
  ).scalars()
  by_qname = {str(element.qname): element for element in rows}
  links: dict[str, str] = {}
  for account in accounts:
    element = by_qname.get(qname_by_account[account.account_id])
    if element is None:
      raise RuntimeError(
        f"Chart account for {provider} account {account.account_id} was not created"
      )
    element.external_source = provider
    element.external_id = account.account_id
    element.connection_id = connection_id
    links[account.account_id] = str(element.id)
    logger.info(
      "Bank feed created chart account %s (%s) for %s account %s",
      element.qname,
      account.name,
      provider,
      account.account_id,
    )
  return links


def _next_code(taken: set[str], base: int) -> str:
  code = base
  while str(code) in taken:
    code += CODE_STEP
  taken.add(str(code))
  return str(code)


def chart_qname_prefix(session: Session, chart_id: str) -> str:
  """The qname prefix the chart's accounts carry: its own namespace, else
  the plain ``coa`` of the group parent's chart."""
  standard = session.execute(
    select(Taxonomy.standard).where(Taxonomy.id == chart_id)
  ).scalar_one_or_none()
  return str(standard) if standard else "coa"


def taken_qnames(session: Session) -> set[str]:
  """Every qname on the graph; the index is unique across it."""
  return {
    str(qname)
    for qname in session.execute(
      select(Element.qname).where(Element.qname.is_not(None))
    ).scalars()
  }


def _unique_qname(taken: set[str], token: str, prefix: str = "coa") -> str:
  candidate = f"{prefix}:{token}"
  suffix = 2
  while candidate in taken:
    candidate = f"{prefix}:{token}{suffix}"
    suffix += 1
  taken.add(candidate)
  return candidate
