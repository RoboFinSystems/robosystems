"""Move a bank feed's account to another chart account.

The chart an account is in is the entity whose books its lines go into, so
pointing a feed account at an account in a subsidiary's chart binds the feed
to that subsidiary: the lines still in the inbox follow it, and every line
the feed captures from then on lands there. Posted entries stay where they
were posted — the old account keeps its history.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from robosystems.adapters.bank_feed.accounts import (
  BANK_FEED_KEY,
  ChartRequiredError,
  account_entities,
  build_chart_index,
  create_chart_accounts,
  feed_account,
)
from robosystems.logger import logger
from robosystems.models.api.extensions.bank_accounts import (
  LinkBankAccountRequest,
  LinkBankAccountResponse,
)
from robosystems.models.extensions import Element
from robosystems.models.extensions.roboledger import Event
from robosystems.operations.connection_service import synced_ledger_live
from robosystems.operations.roboledger.entity_scope import (
  ensure_entity_id,
  is_group_parent,
  resolve_entity_id,
)
from robosystems.operations.roboledger.reads.accounts import coa_element_clause
from robosystems.operations.taxonomy_block.coa_mappings import entity_chart_id

__all__ = [
  "AccountAlreadyFedError",
  "ChartAccountNotFoundError",
  "ChartRequiredError",
  "FeedAccountNotFoundError",
  "NotAChartAccountError",
  "QuickBooksKeptEntityError",
  "link_bank_account",
]

# A line's classification, dropped when it names an account in the chart the
# line just left.
CLASSIFICATION_KEYS = (
  "classified_element_id",
  "classified_allocations",
  "accept_suggestion",
  "classified_by",
  "basis",
)
OPEN_STATUSES = ("captured", "classified")


class FeedAccountNotFoundError(LookupError):
  """No chart account carries this feed account's link."""

  def __init__(self, connection_id: str, account_id: str) -> None:
    super().__init__(
      f"Connection {connection_id!r} has no linked account {account_id!r}; "
      "sync the feed first."
    )


class ChartAccountNotFoundError(LookupError):
  def __init__(self, element_id: str) -> None:
    super().__init__(f"Account {element_id!r} not found on this graph.")


class NotAChartAccountError(ValueError):
  """The target is not an active account of a chart, or not of the entity named."""


class QuickBooksKeptEntityError(ValueError):
  """The target entity's books are kept by a synced ledger."""

  def __init__(self, entity_id: str) -> None:
    super().__init__(
      f"QuickBooks keeps entity {entity_id!r}'s books; a bank feed cannot book "
      "there. Move the account to a subsidiary, or sever QuickBooks first."
    )


class AccountAlreadyFedError(ValueError):
  """Another connection's feed already books to the target account."""

  def __init__(self, element_id: str, link: dict[str, Any]) -> None:
    super().__init__(
      f"Account {element_id!r} is already fed by {link.get('provider')} account "
      f"{link.get('account_name') or link.get('account_id')} (connection "
      f"{link.get('connection_id')}); two feeds cannot book to one account."
    )


def link_bank_account(
  session: Session,
  body: LinkBankAccountRequest,
  created_by: str,
  *,
  graph_id: str,
) -> LinkBankAccountResponse:
  current = _linked_element(session, body.connection_id, body.account_id)
  if current is None:
    raise FeedAccountNotFoundError(body.connection_id, body.account_id)
  link = dict((current.metadata_ or {}).get(BANK_FEED_KEY) or {})
  provider = str(link.get("provider") or "")
  parent_id = ensure_entity_id(session)
  previous_entity = account_entities(session, [current.id], parent_id=parent_id).get(
    str(current.id)
  )

  created = False
  if body.element_id:
    target = session.get(Element, body.element_id)
    if target is None:
      raise ChartAccountNotFoundError(body.element_id)
    _assert_linkable(session, target, body)
    target_entity = account_entities(session, [target.id], parent_id=parent_id)[
      str(target.id)
    ]
    if body.entity_id and resolve_entity_id(session, body.entity_id) != target_entity:
      raise NotAChartAccountError(
        f"Account {body.element_id!r} is not in entity {body.entity_id!r}'s chart."
      )
    # Checked after `_assert_linkable`, which the current account passes by
    # construction (its own link is this one): a no-op is still a valid ask.
    if str(target.id) == str(current.id):
      return _response(
        body, provider, current, current, str(target_entity), changed=False
      )
    _assert_not_synced(session, graph_id, str(target_entity))
  else:
    target_entity = resolve_entity_id(session, body.entity_id)
    _assert_not_synced(session, graph_id, target_entity)
    chart_id = entity_chart_id(session, target_entity)
    if chart_id is None:
      raise ChartRequiredError(
        f"Entity {target_entity!r} has no chart of accounts; initialize one first."
      )
    # The created account carries the feed's provenance, so the old one
    # must give it up before the row exists (the triple is unique).
    _release_provenance(current, provider, body.account_id)
    session.flush()
    home = list(
      session.execute(select(Element).where(Element.taxonomy_id == chart_id))
      .scalars()
      .all()
    )
    ids = create_chart_accounts(
      session,
      chart_id,
      home,
      [feed_account(link, balance_type=str(current.balance_type or "debit"))],
      provider=provider,
      connection_id=body.connection_id,
      created_by=created_by,
    )
    target = session.get(Element, ids[body.account_id])
    if target is None:
      raise RuntimeError(
        f"Chart account for {provider} account {body.account_id} was not created"
      )
    created = True

  if not created:
    # The create path gave it up before the new row existed.
    _release_provenance(current, provider, body.account_id)
  current.metadata_ = {
    key: value
    for key, value in (current.metadata_ or {}).items()
    if key != BANK_FEED_KEY
  }
  if not created:
    target.metadata_ = {**(target.metadata_ or {}), BANK_FEED_KEY: link}

  repointed, unclassified, split = _repoint_open_lines(
    session,
    provider=provider,
    connection_id=body.connection_id,
    old=str(current.id),
    new=str(target.id),
    entity_id=str(target_entity),
    entity_changed=previous_entity != target_entity,
    parent_id=parent_id,
  )
  session.flush()
  logger.info(
    "Bank feed account %s (%s) moved from element %s to %s in entity %s: "
    "%d open lines moved, %d unclassified",
    body.account_id,
    provider,
    current.id,
    target.id,
    target_entity,
    repointed,
    unclassified,
  )
  return _response(
    body,
    provider,
    current,
    target,
    str(target_entity),
    created=created,
    repointed=repointed,
    unclassified=unclassified,
    split=split,
  )


def _linked_element(session: Session, connection_id: str, account_id: str):
  link = Element.metadata_[BANK_FEED_KEY]
  return session.execute(
    select(Element).where(
      link["connection_id"].astext == connection_id,
      link["account_id"].astext == account_id,
    )
  ).scalar_one_or_none()


def _assert_not_synced(session: Session, graph_id: str, entity_id: str) -> None:
  """A feed account never books where a synced ledger keeps the books: the
  group parent, while QuickBooks is connected."""
  if is_group_parent(session, entity_id) and synced_ledger_live(graph_id):
    raise QuickBooksKeptEntityError(entity_id)


def _assert_linkable(session: Session, target: Element, body: LinkBankAccountRequest):
  is_account = session.execute(
    select(Element.id).where(Element.id == target.id, coa_element_clause())
  ).scalar_one_or_none()
  if is_account is None:
    raise NotAChartAccountError(f"{target.id!r} is not a chart of accounts element.")
  if not target.is_active:
    raise NotAChartAccountError(f"Account {target.id!r} is retired.")
  other = (target.metadata_ or {}).get(BANK_FEED_KEY) or {}
  if other and (other.get("connection_id"), other.get("account_id")) != (
    body.connection_id,
    body.account_id,
  ):
    raise AccountAlreadyFedError(str(target.id), other)


def _release_provenance(element: Element, provider: str, account_id: str) -> None:
  """A feed-created account gives up the feed's provenance when its link
  leaves; the tenant's own account never carried it."""
  if element.external_source == provider and element.external_id == account_id:
    element.external_source = None
    element.external_id = None
    element.connection_id = None


def _repoint_open_lines(
  session: Session,
  *,
  provider: str,
  connection_id: str,
  old: str,
  new: str,
  entity_id: str,
  entity_changed: bool,
  parent_id: str,
) -> tuple[int, int, int]:
  """Move the feed's still-open lines to the new account and entity.

  Across an entity change the suggestion is resolved again, by name, on the
  new entity's chart, and a classification that named an account in the old
  chart is dropped: the line goes back to the inbox rather than post into
  another entity's books. Returns the lines moved, the lines unclassified,
  and the pairs whose two legs now sit on two entities.
  """
  link = Event.metadata_
  rows = (
    session.execute(
      select(Event)
      .where(
        Event.source == provider,
        Event.status.in_(OPEN_STATUSES),
        link["connection_id"].astext == connection_id,
        or_(
          Event.resource_element_id == old,
          link["from_element_id"].astext == old,
          link["to_element_id"].astext == old,
        ),
      )
      .order_by(Event.id)
      .with_for_update()
    )
    .scalars()
    .all()
  )
  chart = build_chart_index(session, entity_id) if entity_changed else None
  repointed = unclassified = split = 0
  for event in rows:
    meta = dict(event.metadata_ or {})
    if event.resource_element_id == old:
      event.resource_element_id = new
    for key in ("from_element_id", "to_element_id"):
      if meta.get(key) == old:
        meta[key] = new
    if meta.get("kind") == "internal_transfer":
      # A pair books on the receiving side. When only one leg moved, the
      # pair now crosses two entities — intercompany, which the books do
      # not tie yet; it is counted so the caller can say so, and the commit
      # guard refuses it until its other leg follows or it is dissolved.
      legs = [str(meta.get("from_element_id")), str(meta.get("to_element_id"))]
      owners = account_entities(session, legs, parent_id=parent_id)
      event.entity_id = owners.get(legs[1]) or entity_id
      if owners.get(legs[0]) != owners.get(legs[1]):
        split += 1
    else:
      event.entity_id = entity_id
      if chart is not None and _reclassify(meta, chart, session, entity_id, parent_id):
        event.status = "captured"
        unclassified += 1
    event.metadata_ = meta
    repointed += 1
  return repointed, unclassified, split


def _reclassify(
  meta: dict[str, Any],
  chart: Any,
  session: Session,
  entity_id: str,
  parent_id: str,
) -> bool:
  """Re-resolve the suggestion on the new chart; return whether the line's
  classification had to be dropped."""
  name = meta.get("suggested_account_name")
  resolved = chart.resolve(name) if name else None
  if resolved:
    meta["suggested_element_id"] = resolved
  else:
    meta.pop("suggested_element_id", None)
  contras = [meta.get("classified_element_id")] + [
    allocation.get("element_id")
    for allocation in meta.get("classified_allocations") or []
    if isinstance(allocation, dict)
  ]
  contras = [str(c) for c in contras if c]
  classified = bool(contras) or bool(meta.get("accept_suggestion"))
  if not classified:
    return False
  owners = account_entities(session, contras, parent_id=parent_id)
  foreign = any(owners.get(c) != entity_id for c in contras)
  if meta.get("accept_suggestion") and not resolved:
    foreign = True
  if foreign:
    for key in CLASSIFICATION_KEYS:
      meta.pop(key, None)
  return foreign


def _response(
  body: LinkBankAccountRequest,
  provider: str,
  current: Element,
  target: Element,
  entity_id: str,
  *,
  changed: bool = True,
  created: bool = False,
  repointed: int = 0,
  unclassified: int = 0,
  split: int = 0,
) -> LinkBankAccountResponse:
  return LinkBankAccountResponse(
    connection_id=body.connection_id,
    provider=provider,
    account_id=body.account_id,
    element_id=str(target.id),
    previous_element_id=str(current.id),
    entity_id=entity_id,
    account_created=created,
    events_repointed=repointed,
    events_unclassified=unclassified,
    pairs_across_entities=split,
    changed=changed,
  )
