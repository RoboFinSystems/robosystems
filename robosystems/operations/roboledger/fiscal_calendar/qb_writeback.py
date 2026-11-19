"""Which RoboLedger-originated drafts publish to QuickBooks on period close.

Shared by the close path (which publishes) and the outbox read (which
previews), so the preview can't drift from the write. A draft publishes iff
the entity's books have a write-back keeper (platform DB) and the draft is
eligible (extensions DB): in period, its event not retracted, not already in
QB, and the event publishes. ``metadata.publish_to_source`` decides that
when present; otherwise ``Event.source`` does. The explicit flag exists so an
entry mirroring a change already made upstream can stay local instead of
applying twice.

A ``shadow`` connection is the third answer, read here too: nothing
publishes and the close posts nothing locally. QuickBooks keeps the books;
the close's drafts become ``shadowed`` expectations, and the gates it would
have refused on are findings. Each answer is per entity, read from the
connection that keeps that entity's books (`books_keeper`).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import TYPE_CHECKING

from sqlalchemy import ColumnElement, Row, and_, not_, or_
from sqlalchemy.orm import Session

from robosystems.models.extensions.roboledger.entry import Entry
from robosystems.models.extensions.roboledger.event import Event

if TYPE_CHECKING:
  from robosystems.models.core.connection.connection import Connection

# Synced-in QB transactions already live in QB, so they are not here.
WRITEBACK_EVENT_SOURCES = ("schedule", "manual")

# Retracted events can keep leftover draft rows; close neither publishes
# nor posts them, and the schedule commands' period fence skips them.
WRITEBACK_EXCLUDED_EVENT_STATUSES = ("voided", "superseded")

# ``native`` is absent: there RoboSystems is the system of record. So is
# ``shadow``: there QuickBooks is, and RoboSystems never writes to it.
WRITEBACK_WRITE_POLICIES = ("qb_authoritative", "hybrid")
SHADOW_WRITE_POLICY = "shadow"

# Explicit publish decision on the event, overriding the source default
# either way.
PUBLISH_TO_SOURCE_KEY = "publish_to_source"


def writeback_source_clause() -> ColumnElement[bool]:
  """Explicit flag, else source default.

  ``->>`` yields SQL NULL for both an absent key and JSON ``null``, so
  ``IS NULL`` is exactly "no explicit answer".
  """
  flag = Event.metadata_[PUBLISH_TO_SOURCE_KEY].astext
  return or_(
    flag == "true",
    and_(flag.is_(None), Event.source.in_(WRITEBACK_EVENT_SOURCES)),
  )


@dataclass(frozen=True)
class WritebackConnection:
  """The QB connection (platform DB) that close will publish drafts to."""

  connection_id: str
  write_policy: str


# The ledgers a keeper can be written back to. A keeper of any other
# provider is read-only whatever its policy says.
WRITEBACK_PROVIDERS = frozenset({"quickbooks"})

_KEEPER_MEMO = "books_keepers"


def books_keeper(
  platform_session: Session, graph_id: str, entity_id: str | None
) -> Connection | None:
  """The connection that keeps ``entity_id``'s books, or None when the entity
  keeps its own: the live row flagged as its ledger of record.

  Live is not deleted, past OAuth, and neither disconnected nor severed. A
  row still in ``pending_oauth`` is a connect attempt, not a connection: it
  has no credentials and carries the provider's default policy. Every policy
  question about an entity reads this one row, so the policy the close
  publishes under and the policy that shadows it cannot come from different
  connections.
  """
  from robosystems.models.core.connection.connection import (
    Connection,
    ConnectionStatus,
  )

  if not entity_id:
    return None
  return (
    platform_session.query(Connection)
    .filter(
      Connection.graph_id == graph_id,
      Connection.entity_id == entity_id,
      Connection.keeps_books.is_(True),
      Connection.deleted_at.is_(None),
      Connection.status.notin_(
        [
          ConnectionStatus.PENDING_OAUTH.value,
          ConnectionStatus.DISCONNECTED.value,
          ConnectionStatus.SEVERED.value,
        ]
      ),
    )
    .first()
  )


def resolve_writeback_connection(
  platform_session: Session, graph_id: str, entity_id: str | None
) -> WritebackConnection | None:
  """The connection the entity's close publishes to, or None: its keeper,
  when the keeper's ledger can be written to and its policy writes back."""
  keeper = books_keeper(platform_session, graph_id, entity_id)
  if (
    keeper is None
    or (keeper.provider or "").lower() not in WRITEBACK_PROVIDERS
    or keeper.write_policy not in WRITEBACK_WRITE_POLICIES
  ):
    return None
  return WritebackConnection(
    connection_id=str(keeper.id),
    write_policy=str(keeper.write_policy),
  )


def shadow_close_for_entity(
  platform_session: Session, graph_id: str, entity_id: str | None
) -> bool:
  """Whether this entity's close runs in shadow: its books have a keeper,
  and the keeper's policy is ``shadow``."""
  keeper = books_keeper(platform_session, graph_id, entity_id)
  return keeper is not None and keeper.write_policy == SHADOW_WRITE_POLICY


def publish_posture(
  platform_session: Session, graph_id: str, entity_id: str | None
) -> tuple[WritebackConnection | None, bool]:
  """`(writeback, shadow)` for an entity's drafts: where its close would
  publish them, and whether it would shadow them instead. Never both."""
  return (
    resolve_writeback_connection(platform_session, graph_id, entity_id),
    shadow_close_for_entity(platform_session, graph_id, entity_id),
  )


class ShadowLedgerPostingError(ValueError):
  """Something would post to books a shadow close keeps."""

  def __init__(self, action: str) -> None:
    super().__init__(
      f"{action} would post to books that run a shadow close beside the "
      "ledger that keeps them, where nothing posts here: these books mirror "
      "that ledger. Record it as a draft, which the close keeps as an "
      "expectation, or make the change in the ledger itself."
    )


def keeper_event_source(keeper: Connection) -> str | None:
  """The `Event.source` a keeper's own entries arrive under: the name an
  external ledger registered, the provider for a synced one."""
  provider = (keeper.provider or "").lower()
  if provider == "external":
    return str(keeper.source_name).lower() if keeper.source_name else None
  return provider or None


def shadow_books(session: Session, entity_id: str | None) -> bool:
  """Whether this entity's books run a shadow close, for a session opened on
  a graph (`extensions_session` records which). A session that names no
  graph has no connection to consult, so it is never shadow.

  Remembered for the session: a posting path asks once per line, and the
  answer is a read of another database."""
  graph_id = session.info.get("graph_id")
  if not isinstance(graph_id, str) or not graph_id or not entity_id:
    return False
  memo: dict[str, bool] = session.info.setdefault(_KEEPER_MEMO, {})
  if entity_id not in memo:
    from robosystems.database import SessionFactory

    with SessionFactory() as platform_session:
      memo[entity_id] = shadow_close_for_entity(platform_session, graph_id, entity_id)
  return memo[entity_id]


def closed_under_shadow(
  session: Session, period: str, entity_id: str | None
) -> bool | None:
  """Whether a closed period's close ran in shadow, as its receipt records.
  None while the period is open, or for a receipt from before shadow existed:
  the live policy decides then."""
  from robosystems.models.extensions.roboledger.fiscal_period import FiscalPeriod

  if entity_id is None:
    return None
  receipt = (
    session.query(FiscalPeriod.close_receipt)
    .filter(
      FiscalPeriod.entity_id == entity_id,
      FiscalPeriod.name == period,
      FiscalPeriod.status == "closed",
    )
    .limit(1)
    .scalar()
  )
  if not receipt or "shadow" not in receipt:
    return None
  return bool(receipt["shadow"])


_KEEPER_SOURCE_MEMO = "books_keeper_sources"


def books_keeper_source(session: Session, entity_id: str | None) -> str | None:
  """The source whose entries are this entity's books, for a session opened
  on a graph: its keeper's, or None when it keeps its own. Remembered for
  the session, like `shadow_books`."""
  graph_id = session.info.get("graph_id")
  if not isinstance(graph_id, str) or not graph_id or not entity_id:
    return None
  memo: dict[str, str | None] = session.info.setdefault(_KEEPER_SOURCE_MEMO, {})
  if entity_id not in memo:
    from robosystems.database import SessionFactory

    with SessionFactory() as platform_session:
      keeper = books_keeper(platform_session, graph_id, entity_id)
      memo[entity_id] = keeper_event_source(keeper) if keeper is not None else None
  return memo[entity_id]


def assert_local_posting_allowed(
  session: Session,
  entity_id: str | None,
  action: str,
  *,
  source: str | None = None,
) -> None:
  """Refuse to post to books a shadow close keeps. Under shadow the ledger
  mirrors the one that keeps it: only that ledger's own entries land
  (``source`` names where this one came from), and the close keeps every
  draft as an expectation rather than posting it."""
  if not shadow_books(session, entity_id):
    return
  if source and source.lower() == books_keeper_source(session, entity_id):
    return
  raise ShadowLedgerPostingError(action)


def assert_opening_balance_allowed(session: Session, entity_id: str | None) -> None:
  """Refuse an opening balance on shadow books a synced ledger keeps: its
  sync brings the balances it starts from, and an opening beside them would
  count twice. An external ledger sends no such baseline, so the opening
  balance is where its mirror starts."""
  from robosystems.operations.roboledger.commands.connections import (
    SEVERABLE_SOURCES,
  )

  if (
    shadow_books(session, entity_id)
    and books_keeper_source(session, entity_id) in SEVERABLE_SOURCES
  ):
    raise ShadowLedgerPostingError("Recording an opening balance")


class MirroredGroupError(ValueError):
  """Every company of the group is kept by a ledger elsewhere."""

  def __init__(self) -> None:
    super().__init__(
      "Every company of this group is mirrored from a ledger kept elsewhere, "
      "so the group is consolidated there and nothing is drafted here. Read "
      "the group combined, or send the consolidating entries with the "
      "ledger that holds them."
    )


def assert_group_consolidates_here(session: Session, member_ids: list[str]) -> None:
  """Refuse to draft consolidating entries for a group whose members are all
  shadow books: nothing we originate posts to any of them, and the entries
  would be a second consolidation beside the source's own."""
  if member_ids and all(shadow_books(session, member) for member in member_ids):
    raise MirroredGroupError()


def _entry_not_yet_in_qb() -> ColumnElement[bool]:
  """The entry has no recorded QuickBooks id.

  Published entries are recorded per entry in ``metadata.qb_entry_ids``. An
  event published before that map existed carries only ``qb_external_id``,
  and all of its entries went together.
  """
  recorded = Event.metadata_["qb_entry_ids"]
  return or_(
    and_(recorded.is_(None), Event.metadata_["qb_external_id"].astext.is_(None)),
    and_(recorded.isnot(None), not_(recorded.has_key(Entry.id))),
  )


def select_writeback_eligible_entries(
  session: Session, period_start: date, period_end: date, *, entity_id: str
) -> list[Row[tuple[Entry, Event]]]:
  """The entity's entries that publish on close *if* a write-back connection
  exists."""
  return (
    session.query(Entry, Event)
    .join(Event, Event.id == Entry.triggered_by_event_id)
    .filter(
      Entry.entity_id == entity_id,
      Entry.posting_date >= period_start,
      Entry.posting_date <= period_end,
      Entry.status == "draft",
      writeback_source_clause(),
      Event.status.notin_(WRITEBACK_EXCLUDED_EVENT_STATUSES),
      _entry_not_yet_in_qb(),
    )
    .all()
  )


def writeback_eligible_entry_ids(
  session: Session, period_start: date, period_end: date, *, entity_id: str
) -> set[str]:
  return {
    str(entry.id)
    for entry, _event in select_writeback_eligible_entries(
      session, period_start, period_end, entity_id=entity_id
    )
  }
