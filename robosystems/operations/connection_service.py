"""Connection service for managing data source connections.

All connection metadata is stored in PostgreSQL (Connection model).
Encrypted credentials are stored in ConnectionCredentials.
No graph database operations — connections are platform metadata.

Module-level functions at the bottom form the connection-sync dispatch
kernel shared by the REST sync endpoint and the `sync-connection` MCP
tool, so both surfaces validate, lock, and dispatch identically.
"""

from datetime import date
from typing import Any

from sqlalchemy.orm import Session

from robosystems.database import SessionFactory
from robosystems.logger import logger
from robosystems.models.core.connection.connection import Connection
from robosystems.models.core.connection.connection_credentials import (
  ConnectionCredentials,
)

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
    """Create a new connection.

    Args:
        entity_id: Entity identifier (used as graph_id for backward compat)
        provider: Provider name (quickbooks, sec)
        user_id: User who owns the connection
        credentials: OAuth tokens or API keys to encrypt and store
        metadata: Provider-specific metadata (realm_id, item_id, cik, etc.)
        graph_id: Graph database ID (defaults to entity_id)
        expires_at: When credentials expire
        db_session: Optional existing database session

    Returns:
        Dict with connection details
    """
    metadata = metadata or {}
    target_graph_id = graph_id or entity_id

    session = db_session or SessionFactory()
    session_created = db_session is None

    try:
      # Create connection record
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

      # `write_policy` governs the OUTBOUND (write-back) direction only and
      # defaults per provider (`Connection.create` →
      # `default_write_policy_for_provider`): a QuickBooks connection is
      # `qb_authoritative` (QB is the GL, so RL-originated entries write
      # back), everything else is `native`. Inbound sync-down is decoupled —
      # QB rows auto-commit to GL via the loader's source-keyed rule
      # regardless of this policy. Flip to `native` via `set_write_policy`
      # to pause write-back (e.g. a setup/review window).

      # Store credentials if provided
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
    """Get connection details.

    Args:
        connection_id: Connection identifier
        user_id: Optional user filter (SYSTEM_USER_ID bypasses)
        graph_id: When set, the connection must belong to this graph — pass
            the URL scope the caller already authorized so a guessed
            connection_id can't reach another graph's connection.
        db_session: Optional existing database session

    Returns:
        Dict with connection details including credentials, or None
    """
    session = db_session or SessionFactory()
    session_created = db_session is None

    try:
      conn = Connection.get_by_id(connection_id, session)
      if not conn:
        logger.warning("Connection not found: %s", connection_id)
        return None

      # Enforce graph scope when provided (connection_id is caller-supplied)
      if graph_id and conn.graph_id != graph_id:
        logger.warning("Connection %s not in requested graph scope", connection_id)
        return None

      # Check user access (system user can access any connection)
      if user_id and user_id != SYSTEM_USER_ID and conn.user_id != user_id:
        logger.warning("User not authorized for connection %s", connection_id)
        return None

      result = conn.to_dict()

      # Include decrypted credentials
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
    """List connections with optional filters.

    Args:
        entity_id: Filter by graph_id (backward compat name)
        provider: Filter by provider type
        user_id: Filter by user (SYSTEM_USER_ID sees all)
        graph_id: Filter by graph_id (takes precedence over entity_id)
        db_session: Optional existing database session

    Returns:
        List of connection dicts
    """
    session = db_session or SessionFactory()
    session_created = db_session is None

    try:
      target_graph_id = graph_id or entity_id
      filter_user_id = None if user_id == SYSTEM_USER_ID else user_id

      connections = Connection.list_filtered(
        session=session,
        graph_id=target_graph_id,
        user_id=filter_user_id,
        provider=provider,
      )

      result = []
      for conn in connections:
        conn_dict = conn.to_dict()

        # Check if credentials exist (without decrypting)
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
    """Update credentials for a connection.

    Args:
        connection_id: Connection identifier
        user_id: User performing the update
        credentials: New credentials to encrypt and store
        db_session: Optional existing database session

    Returns:
        True if updated successfully
    """
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
    """Update last sync timestamp.

    Args:
        connection_id: Connection identifier
        graph_id: Unused (kept for backward compat)
        db_session: Optional existing database session

    Returns:
        True if updated successfully
    """
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
    (`routers/graphs/connections/oauth.py`).

    Args:
        connection_id: Connection identifier
        user_id: User performing the deletion
        graph_id: When set, the connection must belong to this graph — pass
            the URL scope the caller already authorized so a guessed
            connection_id can't delete another graph's connection.
        db_session: Optional existing database session

    Returns:
        True if deleted successfully
    """
    session = db_session or SessionFactory()
    session_created = db_session is None

    try:
      conn = Connection.get_by_id(connection_id, session)
      if not conn:
        logger.warning(f"Connection {connection_id} not found for deletion")
        return False

      # Enforce graph scope when provided (connection_id is caller-supplied)
      if graph_id and conn.graph_id != graph_id:
        logger.warning("Connection %s not in requested graph scope", connection_id)
        return False

      # Deactivate credentials (already soft-deletion via is_active=False)
      cred = ConnectionCredentials.get_by_connection_id(connection_id, session)
      if cred:
        cred.deactivate(session)

      # Soft-delete connection record. Hard-delete would orphan
      # connection_id-scoped tenant rows.
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
    """Update connection metadata and/or credentials.

    Args:
        connection_id: Connection identifier
        user_id: User performing the update
        metadata: Metadata fields to update
        credentials: New credentials to encrypt
        status: New status value
        graph_id: Unused (kept for backward compat)
        db_session: Optional existing database session

    Returns:
        True if updated successfully
    """
    session = db_session or SessionFactory()
    session_created = db_session is None

    try:
      conn = Connection.get_by_id(connection_id, session)
      if not conn:
        logger.warning(f"Connection {connection_id} not found for update")
        return False

      # Update metadata fields
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

      # Update credentials if provided
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
    Returns the updated connection dict, or None when the connection is
    missing, belongs to a different graph, or isn't owned by the user.
    Raises ValueError for an unsupported policy value (the model validates).

    Args:
        connection_id: Connection identifier
        write_policy: New policy ('native' | 'qb_authoritative')
        user_id: User performing the change (SYSTEM_USER_ID bypasses)
        graph_id: Graph the connection must belong to
        db_session: Optional existing database session
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
      if user_id and user_id != SYSTEM_USER_ID and conn.user_id != user_id:
        logger.warning("User not authorized for connection %s", connection_id)
        return None

      conn.set_write_policy(session, write_policy)
      logger.info("Set write_policy=%s on connection %s", write_policy, connection_id)
      return conn.to_dict()
    finally:
      if session_created:
        session.close()


class ConnectionSyncError(Exception):
  """Base for connection-sync dispatch failures."""


class ConnectionNotFoundError(ConnectionSyncError):
  """Connection missing, outside the graph scope, or not owned by the caller."""


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
  """Validate, lock, and dispatch a connection sync (async via Dagster).

  The shared kernel behind `POST .../connections/{id}/sync` and the
  `sync-connection` MCP tool: graph- and user-scoped connection lookup,
  provider validation, the per-connection sync lock, and provider
  dispatch. Completion is observed via `Connection.last_sync` (the load
  asset updates it), surfaced through `get-fiscal-calendar` and the
  connections read surface.

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
    # Capture the lock id so the Dagster job can release the lock on
    # completion. `lock_result.lock_id` is the same value the holder
    # stored under the lock key; `release_lock_by_id` does an atomic
    # compare-and-delete against it.
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
  # Lock release: pass the acquired lock_id through to QBSyncConfig
  # so `qb_load` can release it on completion.
  if sync_lock_id:
    effective_options["sync_lock_id"] = sync_lock_id

  try:
    task_id = await asyncio.wait_for(
      provider_registry.sync_connection(
        provider, connection, effective_options or None, graph_id
      ),
      timeout=dispatch_timeout,
    )
  except BaseException:
    # The Dagster job releases the lock on run completion — but a failed
    # DISPATCH never starts a run, so without this release the leaked
    # lock 409s every sync attempt on this connection for its full
    # 30-minute TTL (observed: a disabled-provider rejection locking the
    # connection out). Best-effort, mirroring qb_load's release.
    if sync_lock_id:
      try:
        from robosystems.middleware.auth.distributed_lock import release_lock_by_id

        release_lock_by_id(
          create_redis_client(ValkeyDatabase.LOCKS),
          lock_key=f"qb_sync:{connection_id}",
          lock_id=sync_lock_id,
        )
      except Exception as release_exc:
        logger.warning(
          "Failed to release sync lock for connection %s after dispatch "
          "failure (TTL is the fallback): %s",
          connection_id,
          release_exc,
        )
    raise

  logger.info(f"Sync initiated for connection {connection_id}: task_id={task_id}")

  return {
    "connection_id": connection_id,
    "provider": provider,
    "task_id": task_id,
    "full_rebuild": bool(full_rebuild),
    "since_date": since_date.isoformat() if since_date else None,
    "last_sync_before_dispatch": connection.get("last_sync"),
  }
