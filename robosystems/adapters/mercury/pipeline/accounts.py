"""Link or create — one chart account per bank account the feed exposes.

The bank feed owns no chart (``ref/adapters.md`` §2.9). A company cannot be
assumed to have added a chart account for every bank account it brings —
checking, savings, every card — so at sync each account the feed exposes is
either **linked** to an existing chart account (the customer initialized a
template, authored a chart through the taxonomy block, or kept the chart a
severed QuickBooks connection created) or, when nothing matches, **created**
as an ordinary tenant ``coa:*`` element through the TaxonomyBlock envelope,
exactly as if the customer had added it with ``update-taxonomy-block``.

The link lives on the element as ``metadata.bank_feed`` — never as an
element ``source`` (which stays ``native`` / ``quickbooks``) and never as a
``mercury:`` qname, so the loader's vocabulary learns nothing and the chart
stays the tenant's. An element the feed created additionally carries
``external_source='mercury'`` + ``external_id`` for provenance.
"""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.orm import Session

from robosystems.adapters.mercury.pipeline.tier0 import slug
from robosystems.adapters.mercury.pipeline.transform import (
  BankAccount,
  ChartIndex,
  name_key,
)
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
PROVIDER = "mercury"
# Code ranges for accounts the feed has to create, when the chart uses codes:
# cash-side assets in the 10xx block, the IO card in the 21xx block.
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
  connection_id: str,
  created_by: str,
) -> AccountLinkResult:
  """Return ``{mercury_account_id: element_id}`` for every account, creating
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
    if link.get("provider") == PROVIDER and link.get("account_id"):
      by_feed[str(link["account_id"])] = element
    if element.name and element.is_active:
      by_name.setdefault(name_key(str(element.name)), element)

  links: dict[str, str] = {}
  linked = 0
  to_create: list[BankAccount] = []
  for account in accounts:
    element = by_feed.get(account.mercury_id) or by_name.get(name_key(account.name))
    if element is None:
      to_create.append(account)
      continue
    links[account.mercury_id] = str(element.id)
    linked += 1
    if account.mercury_id not in by_feed:
      _stamp_link(element, account, connection_id)

  if to_create:
    created = _create_accounts(
      session,
      chart_id,
      elements,
      to_create,
      connection_id=connection_id,
      created_by=created_by,
    )
    links.update(created)

  session.flush()
  return AccountLinkResult(links=links, created=len(to_create), linked=linked)


def _stamp_link(element: Element, account: BankAccount, connection_id: str) -> None:
  metadata = dict(element.metadata_ or {})
  metadata[BANK_FEED_KEY] = {
    "provider": PROVIDER,
    "account_id": account.mercury_id,
    "account_name": account.name,
    "kind": account.kind,
    "connection_id": connection_id,
  }
  element.metadata_ = metadata


def _create_accounts(
  session: Session,
  chart_id: str,
  existing: list[Element],
  accounts: list[BankAccount],
  *,
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
    qname_by_account[account.mercury_id] = qname
    requests.append(
      TaxonomyBlockElementRequest(
        qname=qname,
        name=account.name,
        trait=account.trait,
        balance_type=account.balance_type,
        period_type="instant",
        code=code,
        description=f"Mercury {account.kind} account, added by the bank feed.",
        metadata={
          BANK_FEED_KEY: {
            "provider": PROVIDER,
            "account_id": account.mercury_id,
            "account_name": account.name,
            "kind": account.kind,
            "connection_id": connection_id,
          }
        },
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
    element = by_qname.get(qname_by_account[account.mercury_id])
    if element is None:
      raise RuntimeError(
        f"Chart account for Mercury account {account.mercury_id} was not created"
      )
    element.external_source = PROVIDER
    element.external_id = account.mercury_id
    element.connection_id = connection_id
    links[account.mercury_id] = str(element.id)
    logger.info(
      "Bank feed created chart account %s (%s) for Mercury account %s",
      element.qname,
      account.name,
      account.mercury_id,
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
