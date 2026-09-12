"""Connection service for managing data source connections.

All connection metadata is stored in PostgreSQL (Connection model).
Encrypted credentials are stored in ConnectionCredentials.
No graph database operations — connections are platform metadata.

Module-level functions at the bottom form the connection-sync dispatch
kernel shared by the REST sync endpoint and the `sync-connection` MCP
tool, so both surfaces validate, lock, and dispatch identically.
"""

from collections.abc import Callable
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

# System user ID for internal operations (Dagster, background tasks)
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
    provider-specific fields (realm_id, item_id, cik, ...).

    `write_policy` governs the OUTBOUND (write-back) direction only and is
    defaulted per provider by `Connection.create`: QuickBooks connections are
    `qb_authoritative` (QB is the GL, so RoboLedger-originated entries write
    back), everything else is `native`. Inbound sync-down is decoupled — QB
    rows auto-commit to the GL via the loader's source-keyed rule whatever the
    policy says. Use `set_write_policy` to pause write-back.
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

    Pass `graph_id` (the URL scope the caller already authorized) whenever it
    is known: `connection_id` is caller-supplied, so without the scope check a
    guessed id reaches another graph's connection.

    Connections are graph assets that record their creator, not the
    creator's property: with `graph_id` given, the graph scope is the
    authorization and every member of the graph resolves the same
    connection (role is enforced by the caller). Without a graph scope the
    legacy creator check applies; `SYSTEM_USER_ID` bypasses it.
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

    `graph_id` takes precedence over `entity_id`. A graph scope lists the
    graph's connections for every member (they are graph assets — see
    `get_connection`); only the legacy scope-less call filters by creator, and
    `SYSTEM_USER_ID` sees every user's connections. Each dict carries
    `has_credentials` and `is_expired`.
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

      return result

    except Exception:
      logger.error("Failed to list connections", exc_info=True)
      return []
    finally:
      if session_created:
        session.close()

  @staticmethod
  def update_connection_credentials(
    connection_id: str,
    user_id: str,
    credentials: dict[str, Any],
    db_session: Session | None = None,
  ) -> bool:
    """Replace a connection's credentials, creating the row if absent."""
    session = db_session or SessionFactory()
    session_created = db_session is None

    try:
      cred = ConnectionCredentials.get_by_connection_id(connection_id, session)
      if cred:
        cred.update_credentials(credentials, session)
      else:
        ConnectionCredentials.create(
          connection_id=connection_id,
          provider="",
          user_id=user_id,
          credentials=credentials,
          session=session,
        )
      return True
    except Exception:
      logger.error("Failed to update credentials for %s", connection_id, exc_info=True)
      return False
    finally:
      if session_created:
        session.close()

  @staticmethod
  async def update_last_sync(
    connection_id: str,
    graph_id: str | None = None,
    db_session: Session | None = None,
  ) -> bool:
    """Stamp `last_sync` on the connection. `graph_id` is accepted but unused."""
    session = db_session or SessionFactory()
    session_created = db_session is None

    try:
      conn = Connection.get_by_id(connection_id, session)
      if conn:
        conn.update_last_sync(session)
        logger.info(f"Updated last_sync for connection {connection_id}")
        return True
      logger.warning(f"Connection {connection_id} not found for last_sync update")
      return False
    except Exception:
      logger.error("Failed to update last_sync for %s", connection_id, exc_info=True)
      return False
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

    The connection row is preserved with ``deleted_at`` stamped — the
    tenant-side events/agents/elements scoped to its ``connection_id``
    stay attached. Re-OAuthing to the same QB realm later revives this
    row in place via the OAuth callback's reuse path
    (`routers/graphs/connections/oauth.py`) — unless it was severed
    (`sever_connection`), which is the one-way cutover to native books.

    ``write_policy`` falls back to ``native`` on the way out: it describes
    a graph with an *active* authoritative external GL, and after this
    call there is none. `Connection.restore` re-applies the provider
    default on revival.

    Pass `graph_id` (the authorized URL scope) so a guessed `connection_id`
    can't delete another graph's connection.
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

    Stamps the chart the provider created as native-owned on the graph
    (`sever_synced_chart`), drops ``write_policy`` to ``native`` and marks
    the row ``severed`` so a later re-OAuth never revives it. Does NOT
    delete the row: the caller runs provider cleanup and
    `delete_connection` afterwards, so a failed stamp leaves the
    connection exactly as it was.

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
      from robosystems.operations.roboledger.commands.connections import (
        sever_synced_chart,
      )

      target_graph_id = graph_id or conn.graph_id
      with extensions_session(target_graph_id) as ext:
        stamped = sever_synced_chart(ext, connection_id, source=provider)

      conn.set_write_policy(session, WritePolicy.NATIVE.value)
      conn.update_status(ConnectionStatus.SEVERED.value, session)
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
  async def mark_connection_error(
    connection_id: str,
    graph_id: str | None = None,
    db_session: Session | None = None,
  ) -> bool:
    """Mark connection as having an error."""
    session = db_session or SessionFactory()
    session_created = db_session is None

    try:
      conn = Connection.get_by_id(connection_id, session)
      if conn:
        conn.update_status("error", session)
        logger.warning(f"Marked connection {connection_id} with error status")
        return True
      return False
    except Exception:
      logger.error(
        "Failed to mark connection error for %s", connection_id, exc_info=True
      )
      return False
    finally:
      if session_created:
        session.close()

  @staticmethod
  def mark_connection_needs_reauth_sync(
    connection_id: str,
    db_session: Session | None = None,
  ) -> bool:
    """Mark connection as needing operator re-authorization (sync path).

    Distinct from `mark_connection_error`: surfaces a "reconnect" CTA in
    the UI rather than a generic failure message. Called from the QB
    auth-refresh wrapper (`adapters/quickbooks/client/api.py`) which
    runs inside a sync Dagster asset and can't await the async
    `mark_connection_error` counterpart.

    Idempotent: a second call on an already-needs_reauth connection is
    a no-op.
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
  async def mark_connection_connected(
    connection_id: str,
    graph_id: str | None = None,
    db_session: Session | None = None,
  ) -> bool:
    """Mark connection as connected."""
    session = db_session or SessionFactory()
    session_created = db_session is None

    try:
      conn = Connection.get_by_id(connection_id, session)
      if conn:
        conn.update_status("connected", session)
        logger.info(f"Marked connection {connection_id} as connected")
        return True
      return False
    except Exception:
      logger.error(
        "Failed to mark connection connected for %s", connection_id, exc_info=True
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

    Graph-scoped on purpose: the connection must belong to `graph_id` (the
    URL scope the caller already authorized) — this prevents flipping
    another graph's connection into write-back via a guessed connection_id.
    Any write-role member of the graph may set the policy, not only the
    member who created the connection (the router enforces the role).
    Returns the updated connection dict, or None when the connection is
    missing or belongs to a different graph. Raises ValueError for an
    unsupported policy value (the model validates); valid values are
    'native' and 'qb_authoritative'.
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

      conn.set_write_policy(session, write_policy)
      logger.info("Set write_policy=%s on connection %s", write_policy, connection_id)
      return conn.to_dict()
    finally:
      if session_created:
        session.close()


# ---------------------------------------------------------------------------
# Provider compatibility — native and synced ledgers never mix
# (specs/ledger/native-accounting-cutover.md §2).
# ---------------------------------------------------------------------------

# Providers that ARE the general ledger while connected: their chart is the
# chart and their sync writes posted rows. A bank feed cannot sit beside one.
# The same set is what `sever_synced_chart` can stamp — a synced ledger is by
# definition the thing a cutover severs — so there is one definition.
SYNCED_LEDGER_PROVIDERS: frozenset[str] = SEVERABLE_SOURCES

# Providers that capture bank activity into the inbox of natively-kept
# books. They need a chart to resolve against and no synced GL in the way.
BANK_FEED_PROVIDERS: frozenset[str] = frozenset({"mercury"})


class ProviderConflictError(Exception):
  """A provider cannot be connected given the books the graph keeps.

  ``code`` is stable for clients: ``QUICKBOOKS_ACTIVE``, ``CHART_REQUIRED``,
  ``NATIVE_BOOKS_PRESENT``.
  """

  def __init__(self, code: str, message: str) -> None:
    super().__init__(message)
    self.code = code
    self.message = message


def assert_provider_compatible(graph_id: str, provider: str, session: Session) -> None:
  """Refuse a provider that would mix native and synced books.

  - a bank feed while a synced GL is live → ``QUICKBOOKS_ACTIVE``;
  - a bank feed on a graph with no chart of accounts → ``CHART_REQUIRED``;
  - a synced GL over native books (posted line items on elements it did not
    create, or a live bank feed) → ``NATIVE_BOOKS_PRESENT``.

  Anything else (``external`` sources, a second SEC repo …) passes.
  """
  wanted = (provider or "").lower()
  live = {
    (c.provider or "").lower() for c in Connection.get_all_for_graph(graph_id, session)
  }

  if wanted in BANK_FEED_PROVIDERS:
    blocking = sorted(live & SYNCED_LEDGER_PROVIDERS)
    if blocking:
      raise ProviderConflictError(
        "QUICKBOOKS_ACTIVE",
        f"Sever the {blocking[0]} connection first — a bank feed is native "
        "accounting, and while it is connected the synced ledger is the "
        "source of truth for bank transactions.",
      )
    if not _graph_has_chart(graph_id):
      raise ProviderConflictError(
        "CHART_REQUIRED",
        "Initialize a chart of accounts first (from a template, or by "
        "severing a synced QuickBooks connection to keep its chart).",
      )
    return

  if wanted in SYNCED_LEDGER_PROVIDERS:
    if live & BANK_FEED_PROVIDERS or _graph_has_native_books(
      graph_id, synced_source=wanted
    ):
      raise ProviderConflictError(
        "NATIVE_BOOKS_PRESENT",
        "This graph keeps its books natively; a synced ledger cannot become "
        "the source of truth over them.",
      )


def _graph_has_chart(graph_id: str) -> bool:
  from robosystems.operations.roboledger.reads.books import graph_has_chart

  return _probe_books(graph_id, graph_has_chart)


def _graph_has_native_books(graph_id: str, *, synced_source: str) -> bool:
  from robosystems.operations.roboledger.reads.books import (
    graph_has_native_line_items,
  )

  return _probe_books(
    graph_id, lambda ext: graph_has_native_line_items(ext, synced_source=synced_source)
  )


def _probe_books(graph_id: str, predicate: Callable[[Session], bool]) -> bool:
  """Run a books predicate on the graph's extensions schema.

  A graph with no tenant schema — never provisioned (subgraphs get theirs
  lazily from the loader's first sync), or already torn down — has no chart
  and no books, and reads as ``False``. `extensions_session` fails closed on
  that case with ``invalid_schema_name`` (SQLSTATE 3F000), which SQLAlchemy
  raises as `ProgrammingError`; every other programming error is a fault and
  surfaces.
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

  Considers only connections whose provider is registered (feature-flag
  enabled). When several rows exist, currently-connected ones win — the
  same preference `qb_sync_state` applies for the close gate. Refuses to
  guess between multiple live candidates.

  Raises:
      NoSyncConnectionError: no syncable connection for the graph.
      AmbiguousSyncConnectionError: more than one candidate; carries
          `candidates` so callers can present the choice.
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


def _release_sync_lock(connection_id: str, sync_lock_id: str) -> None:
  """Best-effort release of the per-connection sync lock.

  Never raises: the lock's TTL is the fallback, and a release failure must not
  turn a successful dispatch into an error.
  """
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

  The shared kernel behind `POST .../connections/{id}/sync` and the
  `sync-connection` MCP tool: graph- and user-scoped connection lookup,
  provider validation, the per-connection sync lock, and provider
  dispatch.

  `dispatched` in the returned dict says whether a run actually started.
  When true, `task_id` is that run and completion is observed via
  `Connection.last_sync` (the load asset updates it), surfaced through
  `get-fiscal-calendar` and the connections read surface. When false the
  provider had nothing to pull, `task_id` is None, `message` says why, and
  there is nothing to poll for.

  Raises:
      ConnectionNotFoundError, ProviderUnavailableError,
      SyncInProgressError. `TimeoutError` propagates when
      `dispatch_timeout` elapses before the dispatch call returns.
  """
  import asyncio

  from robosystems.config.valkey_registry import ValkeyDatabase, create_redis_client
  from robosystems.middleware.auth.distributed_lock import DistributedLock
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

  # Per-connection sync lock. Two concurrent qb_sync runs against the
  # same connection_id race on the UPSERT path; the lock serializes
  # them. 30-min TTL bounds a *stuck* job (Dagster crash mid-sync); the
  # normal completion path releases the lock explicitly from `qb_load`
  # via the `sync_lock_id` plumbed through `QBSyncConfig`. The 30-min
  # number is a safety-net for failed syncs, not an "intentional
  # cooldown."
  #
  # If the same operator mashes "Sync Now" or the OAuth callback races
  # a scheduler, the second attempt surfaces the holder's lock_id.
  sync_lock_id: str = ""
  try:
    redis_client = create_redis_client(ValkeyDatabase.LOCKS)
    sync_lock = DistributedLock(
      redis_client, f"qb_sync:{connection_id}", ttl_seconds=1800
    )
    lock_result = sync_lock.acquire(blocking=False)
    if not lock_result.acquired:
      # `acquire` swallows Redis failures into `acquired=False` — only a
      # genuinely-held lock is a sync-in-progress. A degraded Valkey
      # takes the same fail-open posture as the client-creation failure
      # below (proceed unlocked), instead of 409ing every sync attempt
      # for as long as Valkey stays unreachable.
      if isinstance(
        lock_result.error_message, str
      ) and lock_result.error_message.startswith("Redis error"):
        raise RuntimeError(f"lock backend degraded: {lock_result.error_message}")
      raise SyncInProgressError(
        connection_id, lock_result.holder_id, lock_result.ttl_remaining
      )
    # The Dagster job releases the lock on completion via this id;
    # `release_lock_by_id` compare-and-deletes against it.
    sync_lock_id = lock_result.lock_id or ""
  except SyncInProgressError:
    raise
  except Exception as e:
    # Dashboards watch for `lock_skipped=true` to detect Valkey-
    # degraded sync runs (we proceed unlocked rather than fail closed,
    # so the silent race risk is real and worth surfacing).
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
    # The Dagster job releases the lock on run completion — but a failed
    # DISPATCH never starts a run, so without this release the leaked
    # lock 409s every sync attempt on this connection for its full
    # 30-minute TTL. Best-effort, mirroring qb_load's release.
    if sync_lock_id:
      _release_sync_lock(connection_id, sync_lock_id)
    raise

  # A provider that did not dispatch a run has already done whatever it does —
  # there is no run coming that will release the lock, so holding it until the
  # TTL would 409 every later attempt on this connection for 30 minutes.
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
    # Only a dispatched run has something to poll; a provider with nothing to
    # pull must not be handed back an id that resolves to no work.
    "task_id": outcome.task_id,
    "message": outcome.message,
    "full_rebuild": bool(full_rebuild),
    "since_date": since_date.isoformat() if since_date else None,
    "last_sync_before_dispatch": connection.get("last_sync"),
  }
