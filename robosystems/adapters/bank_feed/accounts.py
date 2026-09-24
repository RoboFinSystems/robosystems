"""Link or create — one chart account per bank account the feed exposes.

Each account is **linked** to a matching chart account or, when nothing
matches, **created** as an ordinary tenant ``coa:*`` element through the
TaxonomyBlock envelope, as if the customer had added it.

The link lives in the element's ``metadata.bank_feed`` — never in its
``source`` or its qname — so the chart stays the tenant's. A created element
also carries ``external_source`` + ``external_id`` for provenance.
"""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.orm import Session

from robosystems.adapters.bank_feed.chart import BankAccount, ChartIndex, name_key
from robosystems.adapters.bank_feed.hints import slug
from robosystems.logger import logger
from robosystems.models.api.taxonomy_block import (
  TaxonomyBlockElementRequest,
  UpdateTaxonomyBlockRequest,
)
from robosystems.models.extensions import Element
from robosystems.operations.roboledger.commands.chart_of_accounts import (
  active_chart_id,
)
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


def build_chart_index(session: Session) -> ChartIndex:
  """Every account on the graph's chart, by normalized name and by code."""
  chart_id = active_chart_id(session)
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


def link_bank_accounts(
  session: Session,
  accounts: list[BankAccount],
  *,
  provider: str,
  connection_id: str,
  created_by: str,
) -> AccountLinkResult:
  """Return ``{feed_account_id: element_id}`` for every account, creating
  the ones nothing on the chart matches. Flushes; the caller commits."""
  chart_id = active_chart_id(session)
  if chart_id is None:
    raise ChartRequiredError(
      "This graph has no chart of accounts; initialize one before syncing a bank feed."
    )

  elements = list(
    session.execute(select(Element).where(Element.taxonomy_id == chart_id))
    .scalars()
    .all()
  )
  by_feed: dict[str, Element] = {}
  by_name: dict[str, Element] = {}
  for element in elements:
    link = (element.metadata_ or {}).get(BANK_FEED_KEY) or {}
    if link.get("provider") == provider and link.get("account_id"):
      by_feed[str(link["account_id"])] = element
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
    created = _create_accounts(
      session,
      chart_id,
      elements,
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


def _create_accounts(
  session: Session,
  chart_id: str,
  existing: list[Element],
  accounts: list[BankAccount],
  *,
  provider: str,
  connection_id: str,
  created_by: str,
) -> dict[str, str]:
  uses_codes = any(element.code for element in existing)
  taken_codes = {str(element.code).strip() for element in existing if element.code}
  taken_qnames = {str(element.qname) for element in existing if element.qname}

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
    qname = _unique_qname(taken_qnames, code or slug(account.name))
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


def _unique_qname(taken: set[str], token: str) -> str:
  candidate = f"coa:{token}"
  suffix = 2
  while candidate in taken:
    candidate = f"coa:{token}{suffix}"
    suffix += 1
  taken.add(candidate)
  return candidate
