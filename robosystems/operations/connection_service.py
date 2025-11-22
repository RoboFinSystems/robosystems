"""
Connection service for managing connections across graph database (metadata) and PostgreSQL (credentials).
"""

# Standard library
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

# Third-party
from sqlalchemy.orm import Session

# Local imports
# Connection model removed - using direct Cypher queries instead
from ..models.iam.connection_credentials import ConnectionCredentials
from ..database import session
from ..logger import logger
from ..config import URIConstants
from ..middleware.graph import get_graph_repository
from ..middleware.graph.multitenant_utils import MultiTenantUtils

SYSTEM_USER_ID = "__system__"


def _safe_datetime_conversion(dt_value):
  """
  Safely convert datetime values from various formats to Python datetime objects.

  This function handles multiple datetime formats commonly encountered when working
  with different data sources and databases, providing a unified conversion approach.

  Type transformations handled:
  - None → None (passthrough)
  - datetime.datetime → datetime.datetime (passthrough)
  - Objects with .datetime attribute → extracts datetime attribute
  - Objects with .isoformat method → returns as-is (datetime-like)
  - ISO format strings → parses to datetime (handles 'Z' as UTC)
  - Unix timestamps (int/float) → converts from epoch time in UTC
  - Unsupported types → None (with debug logging)

  Args:
      dt_value: A datetime value in any of the following formats:
          - None
          - datetime.datetime object
          - Object with .datetime attribute (e.g., LadybugDB datetime)
          - Object with .isoformat method (datetime-like)
          - ISO 8601 string (e.g., "2024-01-01T12:00:00Z")
          - Unix timestamp as int or float (seconds since epoch)

  Returns:
      datetime or None: A standard Python datetime object with timezone info,
          or None if the input was None or could not be converted.

  Examples:
      >>> _safe_datetime_conversion(None)
      None
      >>> _safe_datetime_conversion(datetime.now())
      datetime.datetime(...)
      >>> _safe_datetime_conversion("2024-01-01T12:00:00Z")
      datetime.datetime(2024, 1, 1, 12, 0, tzinfo=datetime.timezone.utc)
      >>> _safe_datetime_conversion(1704110400.0)  # Unix timestamp
      datetime.datetime(2024, 1, 1, 12, 0, tzinfo=datetime.timezone.utc)
  """
  if dt_value is None:
    return None

  # If it's already a Python datetime, return as-is
  if isinstance(dt_value, datetime):
    return dt_value

  # If it has a datetime attribute, extract it
  if hasattr(dt_value, "datetime"):
    return dt_value.datetime

  # Try to convert if it has isoformat (datetime-like object)
  if hasattr(dt_value, "isoformat"):
    return dt_value

  # If it's a string, try to parse it
  if isinstance(dt_value, str):
    try:
      return datetime.fromisoformat(dt_value.replace("Z", "+00:00"))
    except ValueError:
      logger.debug(f"Could not parse datetime string: {dt_value}")
      return None

  # If it's a float or int, assume it's a Unix timestamp
  if isinstance(dt_value, (float, int)):
    try:
      return datetime.fromtimestamp(dt_value, tz=timezone.utc)
    except (ValueError, OSError):
      logger.debug(f"Could not parse timestamp: {dt_value}")
      return None

  # If we can't convert it, log a warning and return None
  logger.debug(f"Unable to convert datetime value of type {type(dt_value)}: {dt_value}")
  return None


class CredentialsNotFoundError(Exception):
  """Raised when credentials are not found for a connection."""

  pass


class UserAccessDeniedError(Exception):
  """Raised when a user is denied access to a connection."""

  pass


class ConnectionService:
  """Service for unified connection management across graph database and PostgreSQL."""

  @classmethod
  async def create_connection(
    cls,
    entity_id: str,
    provider: str,
    user_id: str,
    credentials: Dict[str, Any],
    metadata: Optional[Dict[str, Any]] = None,
    expires_at: Optional[datetime] = None,
    graph_id: Optional[str] = None,
    db_session: Optional[Session] = None,
  ) -> Dict[str, Any]:
    """
    Create a new connection with metadata in graph database and credentials in PostgreSQL.

    Args:
        entity_id: Entity identifier
        provider: Provider type (QuickBooks, Plaid, SEC)
        user_id: User who owns the connection
        credentials: Dict of auth credentials to encrypt and store
        metadata: Additional metadata for the connection
        expires_at: When credentials expire
        graph_id: Graph database identifier (for multitenant mode)
        db_session: Optional database session (if not provided, creates its own)

    Returns:
        Dict with connection details
    """
    try:
      # Validate database creation if in multi-tenant mode and graph_id is provided
      if MultiTenantUtils.is_multitenant_mode() and graph_id and graph_id != "default":
        MultiTenantUtils.validate_database_creation(graph_id)

      # Get appropriate database name using multi-tenant utilities
      database_name = MultiTenantUtils.get_database_name(graph_id)
      MultiTenantUtils.log_database_operation(
        "Creating connection", database_name, graph_id
      )

      # Get graph repository for the database
      repository = await get_graph_repository(
        graph_id or database_name, operation_type="write"
      )

      with repository:
        entity_query = """
        MATCH (c:Entity {identifier: $entity_id})
        RETURN c.identifier as identifier, c.name as name
        """
        entity_result = repository.execute_single(
          entity_query, {"entity_id": entity_id}
        )
        if not entity_result:
          raise ValueError(f"Entity {entity_id} not found")

        metadata = metadata or {}

        # Generate connection URI and ID from configurable base domain
        base_domain = URIConstants.ROBOSYSTEMS_BASE_URI
        connection_uri = f"{base_domain}/connection/{provider.lower()}/{entity_id}"
        connection_id = f"{provider.lower()}_{entity_id}_{user_id}"

        # Check if connection already exists and delete it to avoid duplicates
        existing_query = """
        MATCH (conn:Connection {connection_id: $connection_id})
        DETACH DELETE conn
        """
        existing_result = repository.execute_query(
          existing_query, {"connection_id": connection_id}
        )
        if existing_result:
          logger.info(
            f"Deleted existing connection {connection_id} before creating new one."
          )

        # Create graph connection node using Cypher
        connection_query = """
        CREATE (conn:Connection {
          provider: $provider,
          uri: $uri,
          connection_id: $connection_id,
          realm_id: $realm_id,
          item_id: $item_id,
          cik: $cik,
          status: 'connected',
          entity_name: $entity_name,
          institution_name: $institution_name,
          expires_at: $expires_at,
          auto_sync_enabled: $auto_sync_enabled
        })
        RETURN conn
        """
        repository.execute_query(
          connection_query,
          {
            "provider": provider,
            "uri": connection_uri,
            "connection_id": connection_id,
            "realm_id": metadata.get("realm_id"),
            "item_id": metadata.get("item_id"),
            "cik": metadata.get("cik"),
            "entity_name": metadata.get("entity_name"),
            "institution_name": metadata.get("institution_name"),
            "expires_at": expires_at,
            "auto_sync_enabled": metadata.get("auto_sync_enabled", True),
          },
        )

        # Connect to entity
        relationship_query = """
        MATCH (c:Entity {identifier: $entity_id})
        MATCH (conn:Connection {connection_id: $connection_id})
        MERGE (c)-[:HAS_CONNECTION]->(conn)
        """
        repository.execute_query(
          relationship_query, {"entity_id": entity_id, "connection_id": connection_id}
        )

      # Store encrypted credentials in PostgreSQL
      pg_session = db_session or session()
      session_created = db_session is None

      try:
        # Delete existing credentials if they exist to avoid duplicates
        existing_creds = ConnectionCredentials.get_by_connection_id(
          connection_id, pg_session
        )
        if existing_creds:
          logger.info(
            f"Deactivating existing credentials for connection {connection_id}."
          )
          existing_creds.deactivate(pg_session)

        ConnectionCredentials.create(
          connection_id=connection_id,
          provider=provider,
          user_id=user_id,
          credentials=credentials,
          expires_at=expires_at,
          session=pg_session,
        )

        logger.info(f"Successfully created connection {connection_id}.")

        return {
          "connection_id": connection_id,
          "provider": provider,
          "status": "connected",
          "entity_id": entity_id,
          "metadata": {
            "realm_id": metadata.get("realm_id"),
            "item_id": metadata.get("item_id"),
            "cik": metadata.get("cik"),
            "entity_name": metadata.get("entity_name"),
            "institution_name": metadata.get("institution_name"),
            "auto_sync_enabled": metadata.get("auto_sync_enabled", True),
          },
          "created_at": datetime.now(timezone.utc),
          "expires_at": expires_at,
        }

      finally:
        if session_created:
          session.remove()

    except Exception as e:
      logger.error(
        f"Failed to create connection for entity {entity_id}: {e}", exc_info=True
      )
      raise

  @classmethod
  async def get_connection(
    cls,
    connection_id: str,
    user_id: str,
    graph_id: Optional[str] = None,
    db_session: Optional[Session] = None,
  ) -> Optional[Dict[str, Any]]:
    """
    Get connection details including metadata and credentials.

    Args:
        connection_id: Connection identifier
        user_id: User ID for access control
        graph_id: Graph database identifier (for multitenant mode)
        db_session: Optional database session (if not provided, creates its own)

    Returns:
        Dict with connection details including decrypted credentials
    """
    try:
      # Get appropriate database name using multi-tenant utilities
      database_name = MultiTenantUtils.get_database_name(graph_id)
      MultiTenantUtils.log_database_operation(
        "Getting connection", database_name, graph_id
      )

      # Get graph repository for the database
      repository = await get_graph_repository(
        graph_id or database_name, operation_type="read"
      )

      with repository:
        connection_query = """
        MATCH (conn:Connection {connection_id: $connection_id})
        RETURN conn
        """
        result = repository.execute_single(
          connection_query, {"connection_id": connection_id}
        )

        if not result:
          logger.warning(
            f"Graph connection not found for connection_id: {connection_id} in database: {database_name}"
          )
          # Try to see what connections exist
          list_query = """
          MATCH (conn:Connection)
          RETURN conn.connection_id as id
          LIMIT 10
          """
          existing = repository.execute_query(list_query, {})
          logger.info(
            f"Existing connections in database {database_name}: {[r['id'] for r in existing]}"
          )
          return None

        # Extract connection properties from result
        # Handle both dict and node object formats
        conn_node = result["conn"]
        if hasattr(conn_node, "_properties"):
          conn_props = conn_node._properties
        else:
          conn_props = conn_node

      # Get PostgreSQL credentials
      pg_session = db_session or session()
      session_created = db_session is None

      try:
        pg_credentials = ConnectionCredentials.get_by_connection_id(
          connection_id, pg_session
        )
        if not pg_credentials:
          logger.warning(f"Credentials not found for connection_id: {connection_id}")
          raise CredentialsNotFoundError(f"Credentials not found for {connection_id}")

        # Check user access - system tasks can bypass user check
        # Otherwise, the user_id must match the one on the credential
        if user_id != SYSTEM_USER_ID and pg_credentials.user_id != user_id:
          logger.warning(
            f"Access denied for user {user_id} to connection {connection_id}"
          )
          raise UserAccessDeniedError(f"User {user_id} cannot access this connection")

        credentials = pg_credentials.get_credentials()

        return {
          "connection_id": connection_id,
          "provider": conn_props.get("provider"),
          "status": conn_props.get("status", "connected"),
          "metadata": {
            "realm_id": conn_props.get("realm_id"),
            "item_id": conn_props.get("item_id"),
            "cik": conn_props.get("cik"),
            "entity_name": conn_props.get("entity_name"),
            "institution_name": conn_props.get("institution_name"),
            "auto_sync_enabled": conn_props.get("auto_sync_enabled", True),
            "last_sync": _safe_datetime_conversion(conn_props.get("last_sync")),
          },
          "credentials": credentials,
          "created_at": _safe_datetime_conversion(conn_props.get("created_at")),
          "expires_at": pg_credentials.expires_at,
          "is_expired": pg_credentials.is_expired(),
        }

      finally:
        if session_created:
          session.remove()

    except Exception as e:
      logger.error(f"Failed to get connection {connection_id}: {e}", exc_info=True)
      raise

  @classmethod
  async def list_connections(
    cls,
    entity_id: str,
    provider: Optional[str] = None,
    user_id: Optional[str] = None,
    graph_id: Optional[str] = None,
  ) -> List[Dict[str, Any]]:
    """
    List connections for a entity, optionally filtered by provider and user.

    Args:
        entity_id: Entity identifier
        provider: Optional provider filter
        user_id: Optional user filter for access control
        graph_id: Graph database identifier (for multitenant mode)

    Returns:
        List of connection dicts (without credentials for security)
    """
    try:
      # Get appropriate database name using multi-tenant utilities
      database_name = MultiTenantUtils.get_database_name(graph_id)
      MultiTenantUtils.log_database_operation(
        "Listing connections", database_name, graph_id
      )

      logger.debug(f"Listing connections for entity {entity_id} in db {database_name}")
      # Get graph repository for the database
      repository = await get_graph_repository(
        graph_id or database_name, operation_type="read"
      )

      with repository:
        # Handle different query patterns based on filters
        if entity_id and entity_id != "":
          # Filter by specific entity
          if provider:
            cypher = """
            MATCH (c:Entity {identifier: $entity_id})-[:HAS_CONNECTION]->(conn:Connection {provider: $provider})
            RETURN conn, c.identifier as entity_id
            """
            results = repository.execute_query(
              cypher, {"entity_id": entity_id, "provider": provider}
            )
          else:
            cypher = """
            MATCH (c:Entity {identifier: $entity_id})-[:HAS_CONNECTION]->(conn:Connection)
            RETURN conn, c.identifier as entity_id
            """
            results = repository.execute_query(cypher, {"entity_id": entity_id})
        else:
          # No entity filter - get all connections with their companies
          if provider:
            cypher = """
            MATCH (c:Entity)-[:HAS_CONNECTION]->(conn:Connection {provider: $provider})
            RETURN conn, c.identifier as entity_id
            """
            results = repository.execute_query(cypher, {"provider": provider})
          else:
            cypher = """
            MATCH (c:Entity)-[:HAS_CONNECTION]->(conn:Connection)
            RETURN conn, c.identifier as entity_id
            """
            results = repository.execute_query(cypher, {})

        # Convert graph records to Connection-like objects
        connections = []
        entity_ids = []
        for record in results:
          conn_node = record["conn"]
          record_entity_id = record.get("entity_id", entity_id)

          # Create a simple object with the properties we need
          class SimpleConnection:
            def __init__(self, properties):
              for key, value in properties.items():
                setattr(self, key, value)

          # Handle both dict and node objects
          if isinstance(conn_node, dict):
            conn_obj = SimpleConnection(conn_node)
          else:
            conn_obj = SimpleConnection(conn_node._properties)
          connections.append(conn_obj)
          entity_ids.append(record_entity_id)

      result = []
      pg_session = session()

      try:
        for idx, conn in enumerate(connections):
          # Get credential info without decrypting
          pg_cred = ConnectionCredentials.get_by_connection_id(
            conn.connection_id, pg_session
          )

          # Apply user filter if specified (system tasks can see all)
          if (
            user_id
            and user_id != SYSTEM_USER_ID
            and pg_cred
            and pg_cred.user_id != user_id
          ):
            continue

          connection_dict = {
            "connection_id": getattr(conn, "connection_id", None),
            "provider": getattr(conn, "provider", None),
            "status": getattr(conn, "status", "connected"),
            "entity_id": entity_ids[idx],
            "metadata": {
              "realm_id": getattr(conn, "realm_id", None),
              "item_id": getattr(conn, "item_id", None),
              "cik": getattr(conn, "cik", None),
              "entity_name": getattr(conn, "entity_name", None),
              "institution_name": getattr(conn, "institution_name", None),
              "auto_sync_enabled": getattr(conn, "auto_sync_enabled", True),
              "last_sync": _safe_datetime_conversion(getattr(conn, "last_sync", None)),
            },
            "created_at": _safe_datetime_conversion(getattr(conn, "created_at", None)),
            "expires_at": pg_cred.expires_at if pg_cred else None,
            "is_expired": pg_cred.is_expired() if pg_cred else False,
            "user_id": pg_cred.user_id if pg_cred else None,
          }
          result.append(connection_dict)

        return result

      finally:
        session.remove()

    except Exception as e:
      logger.error(
        f"Failed to list connections for entity {entity_id}: {e}", exc_info=True
      )
      return []

  @classmethod
  def update_connection_credentials(
    cls,
    connection_id: str,
    user_id: str,
    credentials: Dict[str, Any],
    db_session: Optional[Session] = None,
  ) -> bool:
    """
    Update connection credentials.

    Args:
        connection_id: Connection identifier
        user_id: User ID for access control
        credentials: New credentials dict
        db_session: Optional database session (if not provided, creates its own)

    Returns:
        bool: True if successful, False otherwise
    """
    try:
      pg_session = db_session or session()
      session_created = db_session is None

      try:
        cred = ConnectionCredentials.get_by_connection_id(connection_id, pg_session)
        if not cred:
          raise CredentialsNotFoundError(
            f"Credentials not found for connection {connection_id}"
          )

        if cred.user_id != user_id:
          raise UserAccessDeniedError("User does not have permission to update")

        cred.update_credentials(credentials, pg_session)
        logger.info(f"Updated credentials for connection {connection_id}")
        return True
      finally:
        if session_created:
          session.remove()

    except Exception as e:
      logger.error(
        f"Failed to update credentials for connection {connection_id}: {e}",
        exc_info=True,
      )
      return False

  @classmethod
  async def update_last_sync(
    cls, connection_id: str, graph_id: Optional[str] = None
  ) -> bool:
    """
    Update the last sync timestamp for a connection.

    Args:
        connection_id: Connection identifier
        graph_id: Graph database identifier (for multitenant mode)

    Returns:
        True if successful
    """
    try:
      # Get appropriate database name using multi-tenant utilities
      database_name = MultiTenantUtils.get_database_name(graph_id)
      MultiTenantUtils.log_database_operation(
        "Updating last sync", database_name, graph_id
      )

      logger.debug(f"Updating last_sync for {connection_id} in db {database_name}")
      # Get graph repository for the database
      repository = await get_graph_repository(
        graph_id or database_name, operation_type="write"
      )

      with repository:
        update_query = """
        MATCH (conn:Connection {connection_id: $connection_id})
        SET conn.last_sync = $last_sync
        RETURN conn
        """
        result = repository.execute_single(
          update_query,
          {"connection_id": connection_id, "last_sync": datetime.now(timezone.utc)},
        )

        if result:
          logger.info(f"Updated last_sync for connection {connection_id}")
          return True
      return False

    except Exception as e:
      logger.error(
        f"Failed to update last_sync for {connection_id}: {e}", exc_info=True
      )
      return False

  @classmethod
  async def delete_connection(
    cls,
    connection_id: str,
    user_id: str,
    graph_id: Optional[str] = None,
    db_session: Optional[Session] = None,
  ) -> bool:
    """
    Delete a connection from graph database and deactivate credentials in PostgreSQL.

    Args:
        connection_id: Connection identifier
        user_id: User ID for access control
        graph_id: Graph database identifier
        db_session: Optional database session (if not provided, creates its own)

    Returns:
        bool: True if successful, False otherwise
    """
    try:
      # First, verify user access via PostgreSQL
      # This ensures a user can't delete a connection they don't own
      pg_session = db_session or session()
      session_created = db_session is None

      try:
        cred = ConnectionCredentials.get_by_connection_id(connection_id, pg_session)
        if cred and cred.user_id != user_id:
          raise UserAccessDeniedError("User does not have permission to delete")

        if cred:
          cred.deactivate(pg_session)
          logger.info(f"Deactivated credentials for connection {connection_id}")

        # Then, delete the connection from graph database
        database_name = MultiTenantUtils.get_database_name(graph_id)
        MultiTenantUtils.log_database_operation(
          "Deleting connection", database_name, graph_id
        )

        logger.debug(f"Deleting connection {connection_id} in db {database_name}")

        # Get graph repository for the database
        repository = await get_graph_repository(
          graph_id or database_name, operation_type="write"
        )

        with repository:
          delete_query = """
          MATCH (conn:Connection {connection_id: $connection_id})
          DETACH DELETE conn
          """
          repository.execute_query(delete_query, {"connection_id": connection_id})

        logger.info(f"Deleted connection {connection_id} from graph database")
        return True

      finally:
        if session_created:
          session.remove()

    except Exception as e:
      logger.error(f"Failed to delete connection {connection_id}: {e}", exc_info=True)
      return False

  @classmethod
  async def mark_connection_error(
    cls, connection_id: str, graph_id: Optional[str] = None
  ) -> bool:
    """
    Mark a connection as having an error.

    Args:
        connection_id: Connection identifier
        graph_id: Graph database identifier (for multitenant mode)

    Returns:
        True if successful
    """
    try:
      # Get appropriate database name using multi-tenant utilities
      database_name = MultiTenantUtils.get_database_name(graph_id)
      MultiTenantUtils.log_database_operation(
        "Marking connection error", database_name, graph_id
      )

      logger.debug(f"Marking error for {connection_id} in db {database_name}")
      # Get graph repository for the database
      repository = await get_graph_repository(
        graph_id or database_name, operation_type="write"
      )

      with repository:
        update_query = """
        MATCH (conn:Connection {connection_id: $connection_id})
        SET conn.status = 'error'
        RETURN conn
        """
        result = repository.execute_single(
          update_query, {"connection_id": connection_id}
        )

        if result:
          logger.warning(f"Marked connection {connection_id} with error status")
          return True
        return False

    except Exception as e:
      logger.error(
        f"Failed to mark connection {connection_id} as error: {e}", exc_info=True
      )
      return False

  @classmethod
  async def mark_connection_connected(
    cls, connection_id: str, graph_id: Optional[str] = None
  ) -> bool:
    """
    Mark a connection as connected/healthy.

    Args:
        connection_id: Connection identifier
        graph_id: Graph database identifier (for multitenant mode)

    Returns:
        True if successful
    """
    try:
      # Get appropriate database name using multi-tenant utilities
      database_name = MultiTenantUtils.get_database_name(graph_id)
      MultiTenantUtils.log_database_operation(
        "Marking connection active", database_name, graph_id
      )

      logger.debug(f"Marking connected for {connection_id} in db {database_name}")
      # Get graph repository for the database
      repository = await get_graph_repository(
        graph_id or database_name, operation_type="write"
      )

      with repository:
        update_query = """
        MATCH (conn:Connection {connection_id: $connection_id})
        SET conn.status = 'connected'
        RETURN conn
        """
        result = repository.execute_single(
          update_query, {"connection_id": connection_id}
        )

        if result:
          logger.info(f"Marked connection {connection_id} as connected")
          return True
        return False

    except Exception as e:
      logger.error(
        f"Failed to mark connection {connection_id} as connected: {e}",
        exc_info=True,
      )
      return False

  @classmethod
  async def update(
    cls,
    connection_id: str,
    user_id: str,
    metadata: Optional[Dict[str, Any]] = None,
    credentials: Optional[Dict[str, Any]] = None,
    status: Optional[str] = None,
    graph_id: Optional[str] = None,
    db_session: Optional[Session] = None,
  ) -> bool:
    """
    Update connection metadata and/or credentials.

    Args:
        connection_id: Connection identifier
        user_id: User ID for access control
        metadata: Optional metadata updates (merged with existing)
        credentials: Optional new credentials
        status: Optional new status
        graph_id: Graph database identifier (for multitenant mode)
        db_session: Optional database session

    Returns:
        bool: True if successful
    """
    try:
      # First update credentials if provided
      if credentials:
        success = cls.update_connection_credentials(
          connection_id, user_id, credentials, db_session
        )
        if not success:
          return False

      # Then update graph metadata if provided
      if metadata or status:
        # Get appropriate database name
        database_name = MultiTenantUtils.get_database_name(graph_id)
        MultiTenantUtils.log_database_operation(
          "Updating connection", database_name, graph_id
        )

        # Get graph repository
        repository = await get_graph_repository(
          graph_id or database_name, operation_type="write"
        )

        with repository:
          # Build SET clause dynamically
          set_clauses = []
          params = {"connection_id": connection_id}

          if status:
            set_clauses.append("conn.status = $status")
            params["status"] = status

          if metadata:
            for key, value in metadata.items():
              safe_key = key.replace("-", "_")
              set_clauses.append(f"conn.{safe_key} = ${safe_key}")
              params[safe_key] = value

          if set_clauses:
            update_query = f"""
            MATCH (conn:Connection {{connection_id: $connection_id}})
            SET {", ".join(set_clauses)}
            RETURN conn
            """

            result = repository.execute_single(update_query, params)

            if result:
              logger.info(f"Updated connection {connection_id} metadata")
              return True
            else:
              logger.warning(f"Connection {connection_id} not found for update")
              return False

      return True

    except Exception as e:
      logger.error(f"Failed to update connection {connection_id}: {e}", exc_info=True)
      return False
