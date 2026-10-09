"""Data source connections (platform DB metadata + encrypted credentials).

The module-level functions are the sync dispatch kernel shared by the REST
sync endpoint and the `sync-connection` MCP tool.
"""

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import date
from typing import Any

from sqlalchemy.exc import ProgrammingError
from sqlalchemy.orm import Session

from robosystems.database import SessionFactory
from robosystems.logger import logger
from robosystems.models.core.connection.connection import (
  Connection,
  ConnectionStatus,
  WritePolicy,
)
from robosystems.models.core.connection.connection_credentials import (
  ConnectionCredentials,
)
from robosystems.operations.roboledger.commands.connections import SEVERABLE_SOURCES

# Internal callers (Dagster, background tasks); bypasses the creator check.
SYSTEM_USER_ID = "system"


class ConnectionService:
  """Manages data source connections in PostgreSQL."""

  @staticmethod
  async def create_connection(
    entity_id: str,
    provider: str,
    user_id: str,
    credentials: dict[str, Any] | None = None,
    metadata: dict[str, Any] | None = None,
    graph_id: str | None = None,
    expires_at: Any = None,
    db_session: Session | None = None,
  ) -> dict[str, Any]:
    """Create a connection row and, if given, its encrypted credentials.

    `graph_id` defaults to `entity_id`. `metadata` carries the
    provider-specific fields (realm_id, item_id, cik, ...). `write_policy` is
    defaulted per provider by `Connection.create`.
    """
    metadata = metadata or {}
    target_graph_id = graph_id or entity_id

    session = db_session or SessionFactory()
    session_created = db_session is None

    try:
      conn = Connection.create(
        graph_id=target_graph_id,
        user_id=user_id,
        provider=provider,
        session=session,
        status=metadata.get("status", "pending_oauth"),
        realm_id=metadata.get("realm_id"),
        item_id=metadata.get("item_id"),
        cik=metadata.get("cik"),
        entity_name=metadata.get("entity_name"),
        institution_name=metadata.get("institution_name"),
        source_name=metadata.get("source_name"),
        auto_sync_enabled=metadata.get("auto_sync_enabled", True),
      )

      if credentials:
        ConnectionCredentials.create(
          connection_id=conn.id,
          provider=provider,
          user_id=user_id,
          credentials=credentials,
          session=session,
          expires_at=expires_at,
        )

      logger.info(
        f"Created connection {conn.id} for provider={provider}, "
        f"graph={target_graph_id}, user={user_id}"
      )

      return conn.to_dict()

    except Exception:
      logger.error(
        "Failed to create connection for entity %s", entity_id, exc_info=True
      )
      raise
    finally:
      if session_created:
        session.close()

  @staticmethod
  async def get_connection(
    connection_id: str,
    user_id: str | None = None,
    graph_id: str | None = None,
    db_session: Session | None = None,
  ) -> dict[str, Any] | None:
    """Fetch a connection with decrypted credentials, or None.

    Pass `graph_id` (the authorized URL scope) whenever it is known: without
    it a guessed `connection_id` reaches another graph's connection. With it,
    any graph member resolves the connection (the caller enforces role);
    without it the legacy creator check applies, bypassed by `SYSTEM_USER_ID`.
    """
    session = db_session or SessionFactory()
    session_created = db_session is None

    try:
      conn = Connection.get_by_id(connection_id, session)
      if not conn:
        logger.warning("Connection not found: %s", connection_id)
        return None

      if graph_id:
        if conn.graph_id != graph_id:
          logger.warning("Connection %s not in requested graph scope", connection_id)
          return None
      elif user_id and user_id != SYSTEM_USER_ID and conn.user_id != user_id:
        logger.warning("User not authorized for connection %s", connection_id)
        return None

      result = conn.to_dict()

      cred = ConnectionCredentials.get_by_connection_id(connection_id, session)
      if cred:
        result["credentials"] = cred.get_credentials()
        try:
          result["is_expired"] = cred.is_expired()
        except Exception:
          result["is_expired"] = False
        result["expires_at"] = cred.expires_at
      else:
        result["credentials"] = {}
        result["is_expired"] = False
        result["expires_at"] = None

      return result

    except Exception:
      logger.error("Failed to get connection %s", connection_id, exc_info=True)
      return None
    finally:
      if session_created:
        session.close()

  @staticmethod
  async def list_connections(
    entity_id: str | None = None,
    provider: str | None = None,
    user_id: str | None = None,
    graph_id: str | None = None,
    db_session: Session | None = None,
  ) -> list[dict[str, Any]]:
    """List connections, without decrypting credentials.

    `graph_id` takes precedence over `entity_id`. Only the scope-less call
    filters by creator (`SYSTEM_USER_ID` sees all).
    """
    session = db_session or SessionFactory()
    session_created = db_session is None

    try:
      target_graph_id = graph_id or entity_id
      filter_user_id = None if (graph_id or user_id == SYSTEM_USER_ID) else user_id

      connections = Connection.list_filtered(
        session=session,
        graph_id=target_graph_id,
        user_id=filter_user_id,
        provider=provider,
      )

      result = []
      for conn in connections:
        conn_dict = conn.to_dict()

        cred = ConnectionCredentials.get_by_connection_id(conn.id, session)
        conn_dict["has_credentials"] = cred is not None
        try:
          conn_dict["is_expired"] = cred.is_expired() if cred else False
        except Exception:
          conn_dict["is_expired"] = False

        result.append(conn_dict)

      # A failed read raises: an empty list would read as "no connections".
      return result

    finally:
      if session_created:
        session.close()

  @staticmethod
  async def delete_connection(
    connection_id: str,
    user_id: str,
    graph_id: str | None = None,
    db_session: Session | None = None,
  ) -> bool:
    """Soft-delete a connection and deactivate its credentials.

    The row is kept so tenant-side rows keyed by its ``connection_id`` stay
    attached; re-OAuthing the same QB realm revives it (unless severed).
    ``write_policy`` drops to ``native`` since no external GL is active;
    `Connection.restore` re-applies the provider default. Pass `graph_id` so a
    guessed `connection_id` can't delete another graph's connection.
    """
    session = db_session or SessionFactory()
    session_created = db_session is None

    try:
      conn = Connection.get_by_id(connection_id, session)
      if not conn:
        logger.warning(f"Connection {connection_id} not found for deletion")
        return False

      if graph_id and conn.graph_id != graph_id:
        logger.warning("Connection %s not in requested graph scope", connection_id)
        return False

      cred = ConnectionCredentials.get_by_connection_id(connection_id, session)
      if cred:
        cred.deactivate(session)

      if conn.write_policy != WritePolicy.NATIVE.value:
        conn.write_policy = WritePolicy.NATIVE.value

      conn.soft_delete(session)
      logger.info(f"Soft-deleted connection {connection_id}")
      return True

    except Exception:
      logger.error("Failed to delete connection %s", connection_id, exc_info=True)
      return False
    finally:
      if session_created:
        session.close()

  @staticmethod
  async def sever_connection(
    connection_id: str,
    user_id: str,
    graph_id: str | None = None,
    db_session: Session | None = None,
  ) -> dict[str, Any]:
    """The native-accounting cutover for a synced-ledger connection.

    Stamps the provider's chart native-owned, drops ``write_policy`` to
    ``native`` and marks the row ``severed`` so a re-OAuth never revives it.
    Does not delete the row: the caller runs provider cleanup and
    `delete_connection` afterwards, so a failed stamp changes nothing.

    Raises `ConnectionNotFoundError` (also for a wrong graph scope) and
    `SeverNotSupportedError` for providers that are not a synced GL.
    """
    session = db_session or SessionFactory()
    session_created = db_session is None

    try:
      conn = Connection.get_by_id(connection_id, session)
      if not conn or (graph_id and conn.graph_id != graph_id):
        raise ConnectionNotFoundError(connection_id)

      provider = (conn.provider or "").lower()
      if provider not in SYNCED_LEDGER_PROVIDERS:
        raise SeverNotSupportedError(provider)

      from robosystems.db.extensions import extensions_session
      from robosystems.operations.extensions.staleness import mark_graph_stale
      from robosystems.operations.roboledger.commands.connections import (
        sever_synced_chart,
      )

      target_graph_id = graph_id or conn.graph_id
      with extensions_session(target_graph_id) as ext:
        stamped = sever_synced_chart(ext, connection_id, source=provider)

      conn.set_write_policy(session, WritePolicy.NATIVE.value)
      conn.update_status(ConnectionStatus.SEVERED.value, session)
      # The stamp renames the severed accounts in the graph's projection.
      mark_graph_stale(target_graph_id, "connection_severed")
      logger.info(
        "Severed connection %s (%s) on graph %s: %d elements now native",
        connection_id,
        provider,
        target_graph_id,
        stamped,
      )
      return {
        "connection_id": connection_id,
        "provider": provider,
        "elements_severed": stamped,
      }
    finally:
      if session_created:
        session.close()

  @staticmethod
  def mark_connection_needs_reauth_sync(
    connection_id: str,
    db_session: Session | None = None,
  ) -> bool:
    """Mark connection as needing operator re-authorization (sync path).

    Surfaces a "reconnect" CTA rather than a generic error. Sync because its
    caller (the QB auth-refresh wrapper) runs inside a sync Dagster asset.
    Idempotent.
    """
    session = db_session or SessionFactory()
    session_created = db_session is None

    try:
      conn = Connection.get_by_id(connection_id, session)
      if conn:
        if conn.status != "needs_reauth":
          conn.update_status("needs_reauth", session)
          logger.warning(f"Marked connection {connection_id} as needs_reauth")
        return True
      return False
    except Exception:
      logger.error(
        "Failed to mark connection needs_reauth for %s",
        connection_id,
        exc_info=True,
      )
      return False
    finally:
      if session_created:
        session.close()

  @staticmethod
  async def update(
    connection_id: str,
    user_id: str,
    metadata: dict[str, Any] | None = None,
    credentials: dict[str, Any] | None = None,
    status: str | None = None,
    graph_id: str | None = None,
    db_session: Session | None = None,
  ) -> bool:
    """Update metadata fields, status, and/or credentials.

    Only a fixed set of `metadata` keys is applied; anything else is ignored.
    `graph_id` is accepted but unused — this path does no scope check.
    """
    session = db_session or SessionFactory()
    session_created = db_session is None

    try:
      conn = Connection.get_by_id(connection_id, session)
      if not conn:
        logger.warning(f"Connection {connection_id} not found for update")
        return False

      update_kwargs = {}
      if status:
        update_kwargs["status"] = status
      if metadata:
        if "realm_id" in metadata:
          update_kwargs["realm_id"] = metadata["realm_id"]
        if "item_id" in metadata:
          update_kwargs["item_id"] = metadata["item_id"]
        if "cik" in metadata:
          update_kwargs["cik"] = metadata["cik"]
        if "entity_name" in metadata:
          update_kwargs["entity_name"] = metadata["entity_name"]
        if "institution_name" in metadata:
          update_kwargs["institution_name"] = metadata["institution_name"]
        if "auto_sync_enabled" in metadata:
          update_kwargs["auto_sync_enabled"] = metadata["auto_sync_enabled"]

      if update_kwargs:
        conn.update_metadata(session, **update_kwargs)

      if credentials:
        cred = ConnectionCredentials.get_by_connection_id(connection_id, session)
        if cred:
          cred.update_credentials(credentials, session)
        else:
          ConnectionCredentials.create(
            connection_id=connection_id,
            provider=conn.provider,
            user_id=user_id,
            credentials=credentials,
            session=session,
          )

      logger.info(f"Updated connection {connection_id}")
      return True

    except Exception:
      logger.error("Failed to update connection %s", connection_id, exc_info=True)
      return False
    finally:
      if session_created:
        session.close()

  @staticmethod
  async def set_write_policy(
    connection_id: str,
    write_policy: str,
    user_id: str,
    graph_id: str,
    db_session: Session | None = None,
  ) -> dict[str, Any] | None:
    """Set a connection's source-of-truth `write_policy`.

    The connection must belong to `graph_id` (the authorized URL scope), so a
    guessed id can't flip another graph into write-back; any write-role member
    may set it (the router enforces role). Returns None when missing or out of
    scope. Raises ValueError for 'native', which only disconnect and sever
    set, or a value the model refuses.
    """
    session = db_session or SessionFactory()
    session_created = db_session is None

    try:
      conn = Connection.get_by_id(connection_id, session)
      if not conn:
        logger.warning("Connection not found: %s", connection_id)
        return None
      if conn.graph_id != graph_id:
        logger.warning(
          "Connection %s does not belong to graph %s", connection_id, graph_id
        )
        return None

      if write_policy == WritePolicy.NATIVE.value:
        raise ValueError(
          "'native' is what disconnecting or severing leaves; a live "
          "connection chooses 'qb_authoritative' or 'shadow'."
        )
      conn.set_write_policy(session, write_policy)
      logger.info("Set write_policy=%s on connection %s", write_policy, connection_id)
      return conn.to_dict()
    finally:
      if session_created:
        session.close()


# ---------------------------------------------------------------------------
# Provider compatibility — native and synced ledgers never mix.
# ---------------------------------------------------------------------------

# Providers that ARE the general ledger while connected; a bank feed cannot sit
# beside one. Also exactly what a cutover can sever.
SYNCED_LEDGER_PROVIDERS: frozenset[str] = SEVERABLE_SOURCES

# Bank activity into the inbox of native books: needs a chart, no synced GL.
BANK_FEED_PROVIDERS: frozenset[str] = frozenset({"mercury", "plaid"})


class ProviderConflictError(Exception):
  """A provider cannot be connected given the books the graph keeps.

  ``code`` is stable for clients: ``QUICKBOOKS_ACTIVE``, ``CHART_REQUIRED``,
  ``NATIVE_BOOKS_PRESENT``, ``ENTITY_NOT_FOUND``.
  """

  def __init__(self, code: str, message: str) -> None:
    super().__init__(message)
    self.code = code
    self.message = message

  @property
  def http_status(self) -> int:
    """What the routers answer: 404 for an entity the graph lacks, 409 for a
    conflict with the books it keeps."""
    return 404 if self.code == "ENTITY_NOT_FOUND" else 409


def assert_provider_compatible(
  graph_id: str, provider: str, session: Session, *, entity_id: str | None = None
) -> None:
  """Refuse a provider that would mix native and synced books of one entity.

  A graph is a reporting group; a synced ledger (QuickBooks) keeps the
  group parent's books and only those, so the rule is per entity:

  - a bank feed whose accounts would land on the parent while a synced GL
    is live → ``QUICKBOOKS_ACTIVE`` (a subsidiary's feed is fine);
  - a bank feed for an entity with no chart of accounts → ``CHART_REQUIRED``;
  - a synced GL when the parent's books are native (posted line items on
    elements it did not create, a feed account bound to the parent, or a
    live feed whose accounts land on the parent) → ``NATIVE_BOOKS_PRESENT``;
  - a feed naming an entity the graph does not have → ``ENTITY_NOT_FOUND``.

  ``entity_id`` is where the feed's accounts land: a subsidiary's id, or
  None for the group parent. Anything else (``external`` sources, a second
  SEC repo …) passes.
  """
  wanted = (provider or "").lower()
  connections = list(Connection.get_all_for_graph(graph_id, session))
  live = {(c.provider or "").lower() for c in connections}

  if wanted in BANK_FEED_PROVIDERS:
    # A named entity is resolved first, whatever else is live: one the graph
    # lacks is refused as such, never reached by a later probe.
    lands_on_parent = _is_group_parent(graph_id, entity_id)
    blocking = sorted(live & SYNCED_LEDGER_PROVIDERS)
    if blocking and lands_on_parent:
      raise ProviderConflictError(
        "QUICKBOOKS_ACTIVE",
        f"{blocking[0].capitalize()} keeps the group parent's books, so a bank "
        "feed cannot book there. Connect the bank for a subsidiary (name it "
        "when connecting), or sever the synced connection first.",
      )
    if not _graph_has_chart(graph_id, entity_id):
      raise ProviderConflictError(
        "CHART_REQUIRED",
        "Initialize a chart of accounts for the entity first (from a "
        "template, or by severing a synced QuickBooks connection to keep its "
        "chart).",
      )
    return

  if wanted in SYNCED_LEDGER_PROVIDERS:
    feeds = [
      c for c in connections if (c.provider or "").lower() in BANK_FEED_PROVIDERS
    ]
    if (
      _feed_lands_on_parent(graph_id, feeds, session)
      or _parent_has_feed_account(graph_id)
      or _graph_has_native_books(graph_id, synced_source=wanted)
    ):
      raise ProviderConflictError(
        "NATIVE_BOOKS_PRESENT",
        "The group parent keeps its books natively; a synced ledger cannot "
        "become the source of truth over them. A subsidiary's native books "
        "are no bar.",
      )


def synced_ledger_live(graph_id: str) -> bool:
  """Whether a synced ledger (QuickBooks) is connected to the graph, and so
  keeps the group parent's books. Read on the platform database."""
  with SessionFactory() as session:
    return any(
      (c.provider or "").lower() in SYNCED_LEDGER_PROVIDERS
      for c in Connection.get_all_for_graph(graph_id, session)
    )


def _is_group_parent(graph_id: str, entity_id: str | None) -> bool:
  """Whether a feed landing on ``entity_id`` lands on the group parent.
  None is the parent by definition; a named entity is looked up on the
  tenant, and one the graph does not have is refused."""
  from robosystems.operations.roboledger.entity_scope import (
    EntityNotInGraphError,
    NoEntityError,
    is_group_parent,
    resolve_entity_id,
  )

  if entity_id is None:
    return True

  def probe(ext: Session) -> bool:
    try:
      return is_group_parent(ext, resolve_entity_id(ext, entity_id))
    except (EntityNotInGraphError, NoEntityError) as exc:
      raise ProviderConflictError(
        "ENTITY_NOT_FOUND", f"Entity {entity_id!r} is not an entity of this graph."
      ) from exc

  return _probe_books(graph_id, probe)


def _feed_lands_on_parent(
  graph_id: str, feeds: list[Connection], session: Session
) -> bool:
  """Whether any live bank feed's accounts land on the group parent: the
  entity it was connected for, from its connect-time config. A stored
  entity the graph no longer has is nobody's, not the parent's."""
  targets: list[str | None] = []
  for feed in feeds:
    creds = ConnectionCredentials.get_by_connection_id(str(feed.id), session)
    stored = dict(creds.get_credentials()) if creds is not None else {}
    target = (stored.get("sync_config") or {}).get("entity_id")
    targets.append(str(target) if target else None)
  if None in targets:
    return True
  if not targets:
    return False
  return _any_is_group_parent(graph_id, [t for t in targets if t])


def _any_is_group_parent(graph_id: str, entity_ids: list[str]) -> bool:
  """One probe for several stored entities; one the graph lacks is skipped."""
  from robosystems.operations.roboledger.entity_scope import (
    EntityNotInGraphError,
    NoEntityError,
    find_entity_id,
  )

  def probe(ext: Session) -> bool:
    parent = find_entity_id(ext)
    for entity_id in entity_ids:
      try:
        if find_entity_id(ext, entity_id) == parent:
          return True
      except (EntityNotInGraphError, NoEntityError):
        continue
    return False

  return _probe_books(graph_id, probe)


def _parent_has_feed_account(graph_id: str) -> bool:
  from robosystems.operations.roboledger.reads.books import entity_has_feed_account

  return _probe_books(graph_id, lambda ext: entity_has_feed_account(ext, None))


def _graph_has_chart(graph_id: str, entity_id: str | None = None) -> bool:
  from robosystems.operations.roboledger.reads.books import graph_has_chart

  return _probe_books(graph_id, lambda ext: graph_has_chart(ext, entity_id))


def _graph_has_native_books(graph_id: str, *, synced_source: str) -> bool:
  """Native postings in the group parent's books (every book, on a graph
  with no entity yet)."""
  from robosystems.operations.roboledger.entity_scope import find_entity_id
  from robosystems.operations.roboledger.reads.books import (
    graph_has_native_line_items,
  )

  return _probe_books(
    graph_id,
    lambda ext: graph_has_native_line_items(
      ext, synced_source=synced_source, entity_id=find_entity_id(ext)
    ),
  )


def _probe_books(graph_id: str, predicate: Callable[[Session], bool]) -> bool:
  """Run a books predicate on the graph's extensions schema.

  A graph with no tenant schema yet (subgraphs get theirs on first sync) or
  already torn down reads as ``False``; any other `ProgrammingError` raises.
  """
  from robosystems.db.extensions import extensions_session
  from robosystems.middleware.extensions import is_schema_missing

  try:
    with extensions_session(graph_id) as ext:
      return predicate(ext)
  except ProgrammingError as exc:
    if is_schema_missing(exc):
      return False
    raise


class ConnectionSyncError(Exception):
  """Base for connection-sync dispatch failures."""


class ConnectionNotFoundError(ConnectionSyncError):
  """Connection missing, outside the graph scope, or not owned by the caller."""


class SeverNotSupportedError(ConnectionSyncError):
  """Only a synced-ledger connection (QuickBooks) can be severed."""

  def __init__(self, provider: str) -> None:
    super().__init__(
      f"Only a synced ledger connection can be severed; {provider!r} is not one. "
      "Disconnect it instead."
    )
    self.provider = provider


class ProviderUnavailableError(ConnectionSyncError):
  """The connection's provider is disabled or unknown."""


class SyncInProgressError(ConnectionSyncError):
  """A sync already holds the per-connection lock."""

  def __init__(
    self,
    connection_id: str,
    holder_id: str | None,
    ttl_remaining: int | None,
  ) -> None:
    self.connection_id = connection_id
    self.holder_id = holder_id
    self.ttl_remaining = ttl_remaining
    super().__init__(
      f"Sync already in progress for connection {connection_id} "
      f"(held by {holder_id}, expires in {ttl_remaining}s)"
    )


class NoSyncConnectionError(ConnectionSyncError):
  """The graph has no syncable connection to resolve."""


class AmbiguousSyncConnectionError(ConnectionSyncError):
  """More than one syncable connection matched; the caller must pick one."""

  def __init__(self, candidates: list[dict[str, Any]]) -> None:
    self.candidates = candidates
    listing = ", ".join(
      f"{c.get('connection_id')} ({c.get('provider')})" for c in candidates
    )
    super().__init__(
      f"Multiple sync connections found; specify connection_id: {listing}"
    )


async def resolve_sync_connection(
  graph_id: str,
  user_id: str,
  provider: str | None = None,
) -> dict[str, Any]:
  """Resolve a graph's single syncable connection.

  Only providers enabled in the registry count; connected rows win over
  others (the same preference the close gate applies). Raises
  `NoSyncConnectionError`, or `AmbiguousSyncConnectionError` (with
  `candidates`) rather than guess.
  """
  from robosystems.operations.providers.registry import provider_registry

  connections = await ConnectionService.list_connections(
    graph_id=graph_id, user_id=user_id, provider=provider
  )
  candidates = [
    c for c in connections if provider_registry.is_enabled(c.get("provider", ""))
  ]
  connected = [c for c in candidates if c.get("status") == "connected"]
  pool = connected or candidates
  if not pool:
    raise NoSyncConnectionError(
      f"Graph {graph_id} has no syncable connection"
      + (f" for provider '{provider}'" if provider else "")
    )
  if len(pool) > 1:
    raise AmbiguousSyncConnectionError(
      [
        {
          "connection_id": c.get("connection_id"),
          "provider": c.get("provider"),
          "status": c.get("status"),
        }
        for c in pool
      ]
    )
  return pool[0]


def _acquire_sync_lock(connection_id: str) -> str:
  """Take the per-connection sync lock; returns its id, or "" when Valkey is
  degraded (fails open). Raises `SyncInProgressError` while a sync holds it."""
  from robosystems.config.valkey_registry import ValkeyDatabase, create_redis_client
  from robosystems.middleware.auth.distributed_lock import DistributedLock

  try:
    redis_client = create_redis_client(ValkeyDatabase.LOCKS)
    sync_lock = DistributedLock(
      redis_client, f"qb_sync:{connection_id}", ttl_seconds=1800
    )
    lock_result = sync_lock.acquire(blocking=False)
    if not lock_result.acquired:
      # `acquire` reports Redis failures as `acquired=False`. A degraded
      # Valkey fails open (proceed unlocked) rather than 409 every sync.
      if isinstance(
        lock_result.error_message, str
      ) and lock_result.error_message.startswith("Redis error"):
        raise RuntimeError(f"lock backend degraded: {lock_result.error_message}")
      raise SyncInProgressError(
        connection_id, lock_result.holder_id, lock_result.ttl_remaining
      )
    return lock_result.lock_id or ""
  except SyncInProgressError:
    raise
  except Exception as e:
    # Fails open; dashboards watch `lock_skipped=true` for the race risk.
    logger.warning(
      "Could not acquire sync lock for connection %s: %s; "
      "proceeding without lock (concurrent-sync race still possible)",
      connection_id,
      e,
      extra={
        "connection_id": connection_id,
        "lock_skipped": True,
        "lock_skip_reason": type(e).__name__,
      },
    )
    return ""


@contextmanager
def sync_fence(connection_id: str) -> Iterator[None]:
  """Hold the per-connection sync lock across a disconnect, so no sync runs
  beside its cleanup and none starts until the row is gone. Raises
  `SyncInProgressError` while a sync runs."""
  lock_id = _acquire_sync_lock(connection_id)
  try:
    yield
  finally:
    if lock_id:
      _release_sync_lock(connection_id, lock_id)


def _release_sync_lock(connection_id: str, sync_lock_id: str) -> None:
  """Best-effort release; never raises (the lock's TTL is the fallback)."""
  try:
    from robosystems.config.valkey_registry import ValkeyDatabase, create_redis_client
    from robosystems.middleware.auth.distributed_lock import release_lock_by_id

    release_lock_by_id(
      create_redis_client(ValkeyDatabase.LOCKS),
      lock_key=f"qb_sync:{connection_id}",
      lock_id=sync_lock_id,
    )
  except Exception as release_exc:
    logger.warning(
      "Failed to release sync lock for connection %s (TTL is the fallback): %s",
      connection_id,
      release_exc,
    )


async def dispatch_first_sync(
  *, graph_id: str, connection_id: str, user_id: str, full_rebuild: bool
) -> str | None:
  """The sync a connect flow starts, under the per-connection lock.

  Goes through the sync lock so a callback never runs beside a sync the
  operator already started; that case returns ``None`` and the connect
  succeeds. Any other failure propagates. Returns the run id or ``None``.
  """
  try:
    result = await dispatch_connection_sync(
      graph_id=graph_id,
      connection_id=connection_id,
      user_id=user_id,
      full_rebuild=full_rebuild,
    )
  except SyncInProgressError as exc:
    logger.info(
      "First sync for connection %s not started: one is already running (%s)",
      connection_id,
      exc,
    )
    return None
  task_id = result.get("task_id")
  return str(task_id) if task_id else None


async def dispatch_connection_sync(
  *,
  graph_id: str,
  connection_id: str,
  user_id: str,
  full_rebuild: bool = False,
  since_date: date | None = None,
  sync_options: dict[str, object] | None = None,
  dispatch_timeout: float | None = None,
) -> dict[str, Any]:
  """Validate, lock, and dispatch a connection sync.

  `dispatched` in the result says whether a run started. If so, completion
  shows as `Connection.last_sync` advancing; if not, `task_id` is None and
  `message` says why. Raises `ConnectionNotFoundError`,
  `ProviderUnavailableError`, `SyncInProgressError`, or `TimeoutError` when
  `dispatch_timeout` elapses.
  """
  import asyncio

  from robosystems.operations.providers.registry import provider_registry

  connection = await ConnectionService.get_connection(
    connection_id, user_id, graph_id=graph_id
  )
  if not connection:
    raise ConnectionNotFoundError(f"Connection {connection_id} not found")

  provider = connection["provider"].lower()
  try:
    provider_registry.get_provider(provider)
  except ValueError as e:
    raise ProviderUnavailableError(str(e)) from e

  # Concurrent syncs of one connection race on the UPSERT path. The pipeline
  # releases the lock via `sync_lock_id` when the sync ends, failed or not;
  # the 30-min TTL only bounds a crashed run.
  sync_lock_id = _acquire_sync_lock(connection_id)

  effective_options: dict[str, object] = dict(sync_options or {})
  if full_rebuild:
    effective_options["full_rebuild"] = True
  if since_date is not None:
    effective_options["since_date"] = since_date.isoformat()
  # Plumbed into QBSyncConfig so `qb_load` can release the lock when done.
  if sync_lock_id:
    effective_options["sync_lock_id"] = sync_lock_id

  try:
    outcome = await asyncio.wait_for(
      provider_registry.sync_connection(
        provider, connection, effective_options or None, graph_id
      ),
      timeout=dispatch_timeout,
    )
  except BaseException:
    # A failed dispatch starts no run to release the lock.
    if sync_lock_id:
      _release_sync_lock(connection_id, sync_lock_id)
    raise

  # No run is coming to release the lock.
  if sync_lock_id and not outcome.dispatched:
    _release_sync_lock(connection_id, sync_lock_id)

  if outcome.dispatched:
    logger.info(
      f"Sync dispatched for connection {connection_id}: task_id={outcome.task_id}"
    )
  else:
    logger.info(
      f"Sync is a no-op for connection {connection_id} "
      f"(provider={provider}): {outcome.message}"
    )

  return {
    "connection_id": connection_id,
    "provider": provider,
    "dispatched": outcome.dispatched,
    "task_id": outcome.task_id,
    "message": outcome.message,
    "full_rebuild": bool(full_rebuild),
    "since_date": since_date.isoformat() if since_date else None,
    "last_sync_before_dispatch": connection.get("last_sync"),
  }
