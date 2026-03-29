"""Connection service for managing data source connections.

All connection metadata is stored in PostgreSQL (Connection model).
Encrypted credentials are stored in ConnectionCredentials.
No graph database operations — connections are platform metadata.
"""

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
        auto_sync_enabled=metadata.get("auto_sync_enabled", True),
      )

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
        graph_id: Unused (kept for backward compat)
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
    """Delete a connection and deactivate its credentials.

    Args:
        connection_id: Connection identifier
        user_id: User performing the deletion
        graph_id: Unused (kept for backward compat)
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

      # Deactivate credentials (soft delete for audit trail)
      cred = ConnectionCredentials.get_by_connection_id(connection_id, session)
      if cred:
        cred.deactivate(session)

      # Delete connection record
      conn.delete(session)
      logger.info(f"Deleted connection {connection_id}")
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
