"""Graph-side effects of a connection lifecycle event: severing a synced chart
when a tenant goes native, and purging a bank feed on disconnect."""

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

  Nulling the external keys takes the rows out of the loader's upsert key, so a
  later sync can neither overwrite nor re-adopt them; ``source='native'``
  keeps them chart accounts and makes them editable. The qname is kept (a
  rename would reopen the adapter's qname-collision path).

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


# Provider payload keys scrubbed from accepted events on disconnect; the
# accounting keys (amount, accounts, classification) are the tenant's and stay.
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

  Recursive because ``drift_payload`` and ``reconciliation_history`` nest whole
  payloads, which a later resolution would copy back into the live metadata.
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


def _release_unposted_effects(
  session: Session, event_ids: list[str]
) -> tuple[list[str], int]:
  """Delete the draft entries and unposted transactions these events left.

  A voided line keeps its draft, and the close skips it only while the event
  exists; tenant schemas have no FK to refuse deleting the event first.
  Returns the ids that now own no ledger rows, and the drafts deleted.
  """
  from sqlalchemy import delete, select

  from robosystems.models.extensions.roboledger.entry import Entry
  from robosystems.models.extensions.roboledger.transaction import Transaction

  if not event_ids:
    return [], 0
  drafts = session.execute(
    delete(Entry)
    .where(Entry.triggered_by_event_id.in_(event_ids), Entry.status == "draft")
    .execution_options(synchronize_session=False)
  )
  session.execute(
    delete(Transaction)
    .where(
      Transaction.triggered_by_event_id.in_(event_ids),
      Transaction.status != "posted",
      Transaction.id.not_in(
        select(Entry.transaction_id).where(Entry.transaction_id.is_not(None))
      ),
    )
    .execution_options(synchronize_session=False)
  )
  owners = set(
    session.execute(
      select(Entry.triggered_by_event_id)
      .where(Entry.triggered_by_event_id.in_(event_ids))
      .union(
        select(Transaction.triggered_by_event_id).where(
          Transaction.triggered_by_event_id.in_(event_ids)
        )
      )
    )
    .scalars()
    .all()
  )
  return [i for i in event_ids if i not in owners], int(drafts.rowcount or 0)


def purge_bank_feed(
  session: Session, *, source: str, connection_id: str
) -> dict[str, int]:
  """Delete what a bank feed captured and scrub what it left on posted rows.

  What the bank's data agreement requires on disconnect: unposted captured
  events are hard-deleted with any draft a voided line left; accepted events
  keep their accounting content but lose the provider's payload keys and deep
  link; unreferenced counterparties the feed created are deleted; chart-account
  links are cleared. Flushes; the caller commits.

  Strictly scoped to ``connection_id``, even though only one live connection
  per provider is allowed today.
  """
  from sqlalchemy import delete, select

  from robosystems.models.extensions import Agent, Event
  from robosystems.models.extensions.roboledger.dimension_junctions import (
    event_dimensions,
  )
  from robosystems.operations.locking import bounded_lock_wait, ordered_lock_column

  # Locked until the purge commits, so the statuses decided on are the ones
  # deleted on: a commit in flight lands first and its line is kept, one
  # arriving later finds the line gone. Ordered by id, as every batch lock
  # over events is.
  session.flush()
  with bounded_lock_wait(
    session,
    f"The {source} feed's lines are being written by another process. "
    "Retry the disconnect in a moment.",
  ):
    feed_events = list(
      session.execute(
        select(Event)
        .where(
          Event.source == source,
          Event.metadata_["connection_id"].astext == connection_id,
        )
        .order_by(ordered_lock_column())
        .with_for_update()
        .execution_options(populate_existing=True)
      )
      .scalars()
      .all()
    )
  # An event that still owns ledger rows once its drafts are gone has landed
  # history: it is scrubbed below like any posted line, never deleted.
  unposted_ids, drafts_deleted = _release_unposted_effects(
    session,
    [str(event.id) for event in feed_events if event.status in _UNPOSTED_STATUSES],
  )
  deleted: set[str] = set()
  if unposted_ids:
    still_unposted = (
      Event.id.in_(unposted_ids),
      Event.status.in_(_UNPOSTED_STATUSES),
    )
    session.execute(
      delete(event_dimensions).where(
        event_dimensions.c.event_id.in_(select(Event.id).where(*still_unposted))
      )
    )
    deleted = {
      str(event_id)
      for event_id in session.execute(
        delete(Event).where(*still_unposted).returning(Event.id)
      ).scalars()
    }

  scrubbed = 0
  for event in feed_events:
    if str(event.id) in deleted:
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
    "events_deleted": len(deleted),
    "drafts_deleted": drafts_deleted,
    "events_scrubbed": scrubbed,
    "agents_deleted": len(removable),
    "accounts_unlinked": len(linked),
  }
