"""Graph-side effects of a connection lifecycle event.

The sever half of the native accounting cutover
(``specs/ledger/native-accounting-cutover.md`` §3): when a QuickBooks
tenant goes native, the chart QuickBooks created becomes the tenant's own.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import update
from sqlalchemy.orm import Session

from robosystems.models.extensions import Element

SEVERABLE_SOURCES: frozenset[str] = frozenset({"quickbooks"})


def sever_synced_chart(
  session: Session, connection_id: str, *, source: str = "quickbooks"
) -> int:
  """Stamp the elements a synced connection created as native-owned.

  One statement does three jobs. Nulling ``external_source`` /
  ``connection_id`` / ``external_id`` takes the rows out of the loader's
  upsert key (``idx_elements_upsert_key``), so a later sync of the same
  provider can neither overwrite nor re-adopt them and the trait heal never
  sees them. ``source='native'`` keeps them visible as chart accounts
  (``native`` is in ``COA_SOURCES``) and marks them editable. The qname is
  kept: facts, associations, line items and reports reference element ids,
  and a rename would reopen the adapter's qname-collision path.

  Returns the number of elements stamped.
  """
  if source not in SEVERABLE_SOURCES:
    raise ValueError(f"{source!r} connections cannot be severed")
  result = session.execute(
    update(Element)
    .where(
      Element.external_source == source,
      Element.connection_id == connection_id,
    )
    .values(
      source="native",
      external_source=None,
      connection_id=None,
      external_id=None,
    )
  )
  return int(result.rowcount or 0)


# ---------------------------------------------------------------------------
# Bank feed purge — the disconnect half of a bank-feed connection
# ---------------------------------------------------------------------------

# Event metadata a bank feed wrote from its source's payload (Mercury's or
# Plaid's). Scrubbed from accepted (posted) events on disconnect; the
# accounting keys — the amount, the accounts, the classification — are the
# tenant's own and stay.
BANK_FEED_PAYLOAD_KEYS: frozenset[str] = frozenset(
  {
    "account_id",
    "account_name",
    "authorized_date",
    "bank_description",
    "card_id",
    "check_number",
    "counterparty_external_id",
    "counterparty_id",
    "counterparty_name",
    "counterparty_type",
    "custom_category",
    "dashboard_link",
    "external_memo",
    "from_account_id",
    "from_account_name",
    "from_date",
    "gl_allocations",
    "item_id",
    "legs",
    "logo_url",
    "merchant_category",
    "merchant_category_code",
    "merchant_entity_id",
    "merchant_name",
    "mercury_category",
    "note",
    "payment_channel",
    "plaid_category_confidence",
    "rekeyed_from",
    "released_legs",
    "source_legs",
    "source_removed_transaction_ids",
    "plaid_category_detailed",
    "plaid_category_primary",
    "split",
    "suggested_account_key",
    "suggested_account_name",
    "to_account_id",
    "to_account_name",
    "to_date",
    "transaction_code",
    "transaction_id",
    "website",
  }
)
_UNPOSTED_STATUSES: tuple[str, ...] = ("captured", "classified", "voided")


def scrub_payload_keys(value: Any) -> Any:
  """Drop the feed's payload keys wherever they sit in the metadata.

  A reconciling item stashes a whole payload under ``drift_payload`` and a
  resolution writes the trail under ``reconciliation_history``, so the keys
  live nested as well as at the top level — and a resolution after the purge
  would copy a nested payload back into the live metadata.
  """
  if isinstance(value, dict):
    return {
      k: scrub_payload_keys(v)
      for k, v in value.items()
      if k not in BANK_FEED_PAYLOAD_KEYS
    }
  if isinstance(value, list):
    return [scrub_payload_keys(v) for v in value]
  return value


def purge_bank_feed(
  session: Session, *, source: str, connection_id: str
) -> dict[str, int]:
  """Delete what a bank feed captured and scrub what it left on posted rows.

  The deletion a bank partnership's data agreement asks for on disconnect:
  events the feed captured that were never posted are hard-deleted (their
  dimension junctions first); events that were accepted into the books keep
  their accounting content but lose the provider's payload keys — at the top
  level and inside any stashed payload or trail — and the deep link back to
  it; counterparties the feed created and nothing
  references are deleted; the link on the chart accounts the feed linked or
  created is cleared. Flushes; the caller commits.

  Everything is scoped to ``connection_id``: events by the
  ``metadata.connection_id`` the feed stamps on every row it captures, chart
  links by the connection recorded in ``metadata.bank_feed``. The create path
  allows one live connection per provider per graph, but a disconnect must
  never reach past its own connection even if that ever changes.
  """
  from sqlalchemy import delete, select

  from robosystems.models.extensions import Agent, Event
  from robosystems.models.extensions.roboledger.dimension_junctions import (
    event_dimensions,
  )

  feed_events = list(
    session.execute(
      select(Event).where(
        Event.source == source,
        Event.metadata_["connection_id"].astext == connection_id,
      )
    )
    .scalars()
    .all()
  )
  unposted_ids = [
    str(event.id) for event in feed_events if event.status in _UNPOSTED_STATUSES
  ]
  if unposted_ids:
    session.execute(
      delete(event_dimensions).where(event_dimensions.c.event_id.in_(unposted_ids))
    )
    session.execute(delete(Event).where(Event.id.in_(unposted_ids)))

  scrubbed = 0
  for event in feed_events:
    if str(event.id) in unposted_ids:
      continue
    metadata = dict(event.metadata_ or {})
    kept = scrub_payload_keys(metadata)
    if kept != metadata or event.external_url:
      event.metadata_ = kept
      event.external_url = None
      scrubbed += 1
  session.flush()

  referenced = {
    str(agent_id)
    for (agent_id,) in session.execute(
      select(Event.agent_id).where(Event.agent_id.is_not(None)).distinct()
    ).all()
  }
  feed_agents = list(
    session.execute(
      select(Agent).where(Agent.source == source, Agent.connection_id == connection_id)
    )
    .scalars()
    .all()
  )
  removable = [
    str(agent.id) for agent in feed_agents if str(agent.id) not in referenced
  ]
  if removable:
    session.execute(delete(Agent).where(Agent.id.in_(removable)))

  linked = list(
    session.execute(
      select(Element).where(
        Element.metadata_["bank_feed"]["provider"].astext == source,
        Element.metadata_["bank_feed"]["connection_id"].astext == connection_id,
      )
    )
    .scalars()
    .all()
  )
  for element in linked:
    metadata = dict(element.metadata_ or {})
    metadata.pop("bank_feed", None)
    element.metadata_ = metadata
    if element.external_source == source and element.connection_id == connection_id:
      element.external_source = None
      element.external_id = None
      element.connection_id = None
  session.flush()

  return {
    "events_deleted": len(unposted_ids),
    "events_scrubbed": scrubbed,
    "agents_deleted": len(removable),
    "accounts_unlinked": len(linked),
  }
