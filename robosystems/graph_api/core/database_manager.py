"""
Kuzu Database Manager - Multi-database management for Kuzu API

This module provides database management capabilities for Kuzu clusters,
including creating, deleting, and managing multiple databases on a single node.

Key features:
- Create new databases with schema installation
- Delete databases and cleanup files
- List all databases on the node
- Health checking for multiple databases
- Schema management and validation
"""

import shutil
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any

import kuzu
from fastapi import HTTPException, status
from pydantic import BaseModel, Field
import re

from robosystems.config import env
from robosystems.logger import logger
from .connection_pool import initialize_connection_pool


def validate_database_path(base_path: Path, db_name: str) -> Path:
  """
  Safely construct and validate database path to prevent directory traversal.

  Args:
      base_path: Base directory for databases
      db_name: Database name to validate

  Returns:
      Safe database path

  Raises:
      HTTPException: If path is unsafe
  """
  if not db_name or not re.match(r"^[a-zA-Z0-9_-]+$", db_name):
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid database name"
    )

  # Construct safe path (Kuzu 0.11.x uses .kuzu files)
  db_path = base_path / f"{db_name}.kuzu"

  # Ensure the resolved path is still within base_path
  try:
    db_path.resolve().relative_to(base_path.resolve())
  except ValueError:
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail="Database path outside allowed directory",
    )

  return db_path


class DatabaseInfo(BaseModel):
  """Information about a database on this node."""

  graph_id: str = Field(..., description="Graph database identifier")
  database_path: str = Field(..., description="Full path to database files")
  created_at: str = Field(..., description="Database creation timestamp")
  size_bytes: int = Field(..., description="Database size in bytes")
  read_only: bool = Field(..., description="Whether database is read-only")
  is_healthy: bool = Field(..., description="Database health status")
  last_accessed: Optional[str] = Field(None, description="Last access timestamp")


class DatabaseCreateRequest(BaseModel):
  """Request to create a new database."""

  graph_id: str = Field(
    ...,
    description="Graph database identifier",
    pattern=r"^[a-zA-Z0-9_-]+$",
    max_length=64,
  )
  schema_type: str = Field(
    default="entity",
    description="Schema type (entity, shared, custom)",
    pattern=r"^(entity|shared|custom)$",
  )
  repository_name: Optional[str] = Field(
    None,
    description="Repository name for shared databases",
    pattern=r"^[a-zA-Z0-9_-]*$",
    max_length=32,
  )
  custom_schema_ddl: Optional[str] = Field(
    None,
    description="Custom schema DDL commands (required when schema_type='custom')",
  )
  is_subgraph: bool = Field(
    default=False,
    description="Whether this is a subgraph (bypasses max_databases check for Enterprise/Premium)",
  )
  read_only: bool = Field(default=False, description="Create as read-only database")

  class Config:
    extra = "forbid"


class DatabaseCreateResponse(BaseModel):
  """Response from database creation."""

  status: str = Field(..., description="Creation status")
  graph_id: str = Field(..., description="Graph database identifier")
  database_path: str = Field(..., description="Path to created database")
  schema_applied: bool = Field(..., description="Whether schema was applied")
  execution_time_ms: float = Field(..., description="Creation time in milliseconds")


class DatabaseListResponse(BaseModel):
  """Response listing all databases."""

  databases: List[DatabaseInfo] = Field(..., description="List of databases")
  total_databases: int = Field(..., description="Total number of databases")
  total_size_bytes: int = Field(..., description="Total size of all databases")
  node_capacity: Dict[str, Any] = Field(..., description="Node capacity information")


class DatabaseHealthResponse(BaseModel):
  """Health status for all databases."""

  healthy_databases: int = Field(..., description="Number of healthy databases")
  unhealthy_databases: int = Field(..., description="Number of unhealthy databases")
  databases: List[DatabaseInfo] = Field(..., description="Database health details")


class KuzuDatabaseManager:
  """
  Manages multiple Kuzu databases on a single node.

  This class handles the lifecycle of multiple databases including creation,
  deletion, schema management, and health monitoring. Now uses a thread-safe
  connection pool for improved performance and reliability.
  """

  def __init__(
    self,
    base_path: str,
    max_databases: int = 200,
    max_connections_per_db: int = 3,
    read_only: bool = False,
  ):
    """
    Initialize database manager with connection pooling.

    Args:
        base_path: Base directory for all databases
        max_databases: Maximum number of databases allowed on this node
        max_connections_per_db: Maximum connections per database in pool
        read_only: Whether this database manager operates in read-only mode
    """
    self.base_path = Path(base_path)
    self.max_databases = max_databases
    self.read_only = read_only

    # Initialize thread-safe connection pool
    self.connection_pool = initialize_connection_pool(
      base_path=str(base_path),
      max_connections_per_db=max_connections_per_db,
      connection_ttl_minutes=30,
    )

    # Note: All connections now managed by connection_pool for thread safety

    # Ensure base directory exists
    self.base_path.mkdir(parents=True, exist_ok=True)

    logger.info(
      f"Initialized Kuzu Database Manager with connection pool: {base_path} (max: {max_databases})"
    )

  def create_database(self, request: DatabaseCreateRequest) -> DatabaseCreateResponse:
    """
    Create a new Kuzu database with schema.

    Args:
        request: Database creation request

    Returns:
        Database creation response

    Raises:
        HTTPException: If creation fails or limits exceeded
    """
    start_time = time.time()

    # Validate request
    if not request.graph_id:
      raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST, detail="Graph ID is required"
      )

    # Check capacity (bypass for subgraphs on Enterprise/Premium instances)
    current_count = len(self.list_databases())
    if not request.is_subgraph and current_count >= self.max_databases:
      raise HTTPException(
        status_code=status.HTTP_507_INSUFFICIENT_STORAGE,
        detail=f"Maximum database capacity reached ({self.max_databases})",
      )
    elif request.is_subgraph:
      logger.info(
        f"Creating subgraph database {request.graph_id} (bypassing max_databases check)"
      )

    # Check if database already exists (using safe path construction)
    db_path = validate_database_path(self.base_path, request.graph_id)
    if db_path.exists():
      raise HTTPException(
        status_code=status.HTTP_409_CONFLICT,
        detail=f"Database {request.graph_id} already exists",
      )

    try:
      logger.info(f"Creating database: {request.graph_id}")

      # Get memory configuration from environment
      from robosystems.config import env

      max_memory_mb = env.KUZU_MAX_MEMORY_MB
      buffer_pool_size = max_memory_mb * 1024 * 1024

      # For SEC database, use explicit checkpoint threshold for large tables
      # SEC has huge tables (Fact, Association) that can exhaust memory
      database_name = request.graph_id
      if database_name == "sec":
        checkpoint_threshold = 134217728  # 128MB for SEC (more frequent checkpoints)
        logger.info("Using reduced checkpoint threshold (128MB) for SEC database")
      else:
        checkpoint_threshold = 536870912  # 512MB for regular databases

      # Create database with all optimizations
      db = kuzu.Database(
        str(db_path),
        read_only=False,
        buffer_pool_size=buffer_pool_size,
        compression=True,  # Safe: enabled by default in Kuzu
        max_num_threads=0,  # Use all available threads (Kuzu decides)
        auto_checkpoint=True,  # Enable automatic checkpointing
        checkpoint_threshold=checkpoint_threshold,  # Adaptive based on database
      )
      conn = kuzu.Connection(db)
      logger.info(
        f"Database created - buffer pool: {max_memory_mb} MB, "
        f"compression: enabled, auto_checkpoint: enabled, threshold: {checkpoint_threshold // (1024 * 1024)}MB"
      )

      # Apply schema based on type
      schema_applied = self._apply_schema(
        conn, request.schema_type, request.repository_name, request.custom_schema_ddl
      )

      # Close temporary connections (pool will manage connections going forward)
      conn.close()
      db.close()

      # Create DuckDB staging directory for this graph
      try:
        staging_base = Path("/app/data/staging")
        staging_dir = staging_base / request.graph_id
        staging_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Created DuckDB staging directory: {staging_dir}")
      except (OSError, PermissionError) as e:
        logger.warning(f"Could not create DuckDB staging directory: {e}")

      execution_time = (time.time() - start_time) * 1000

      logger.info(
        f"Database {request.graph_id} created successfully in {execution_time:.2f}ms"
      )

      # Update instance registry in DynamoDB
      if env.is_development():
        self._update_instance_registry()
      elif request.schema_type == "shared" and request.repository_name:
        # For shared repositories in production, register in DynamoDB
        self._register_shared_repository(request.graph_id)

      return DatabaseCreateResponse(
        status="success",
        graph_id=request.graph_id,
        database_path=str(db_path),
        schema_applied=schema_applied,
        execution_time_ms=execution_time,
      )

    except Exception as e:
      logger.error(f"Failed to create database {request.graph_id}: {e}")

      # Cleanup on failure
      if db_path.exists():
        try:
          if db_path.is_file():
            db_path.unlink()
          elif db_path.is_dir():
            shutil.rmtree(db_path)
        except Exception as cleanup_error:
          logger.error(
            f"Failed to cleanup after database creation failure: {cleanup_error}"
          )

      raise HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail=f"Database creation failed: {str(e)}",
      )

  def delete_database(self, graph_id: str) -> Dict[str, Any]:
    """
    Delete a database and cleanup resources.

    Args:
        graph_id: Graph database identifier to delete

    Returns:
        Deletion status
    """
    db_path = self.base_path / f"{graph_id}.kuzu"

    if not db_path.exists():
      raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"Database {graph_id} not found",
      )

    try:
      logger.info(f"Deleting database: {graph_id}")

      # Close any connections in the pool for this database
      self.connection_pool.close_database_connections(graph_id)

      # Remove database file (single file in Kuzu 0.11.x)
      if db_path.is_file():
        db_path.unlink()
      elif db_path.is_dir():
        # Legacy cleanup for old .db directories
        shutil.rmtree(db_path)

      logger.info(f"Database {graph_id} deleted successfully")

      return {
        "status": "success",
        "graph_id": graph_id,
        "message": "Database deleted successfully",
      }

    except Exception as e:
      logger.error(f"Failed to delete database {graph_id}: {e}")
      raise HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail=f"Database deletion failed: {str(e)}",
      )

  def get_connection(self, graph_id: str, read_only: bool = False):
    """
    Get a connection for a database from the connection pool.

    Args:
        graph_id: Database identifier
        read_only: Whether to open connection in read-only mode

    Returns:
        Kuzu connection from the pool
    """
    return self.connection_pool.get_connection(graph_id, read_only=read_only)

  def list_databases(self) -> List[str]:
    """
    List all databases on this node.

    Returns:
        List of database names
    """
    databases = []

    try:
      for item in self.base_path.iterdir():
        if item.is_file() and item.name.endswith(".kuzu"):
          db_name = item.name[:-5]  # Remove .kuzu extension
          databases.append(db_name)

      return sorted(databases)

    except Exception as e:
      logger.error(f"Failed to list databases: {e}")
      return []

  def get_database_path(self, graph_id: str) -> str:
    """
    Get the file path for a database.

    Args:
        graph_id: Graph database identifier

    Returns:
        Full path to the database file
    """
    db_path = validate_database_path(self.base_path, graph_id)
    return str(db_path)

  def get_database_info(self, graph_id: str) -> DatabaseInfo:
    """
    Get detailed information about a specific database.

    Args:
        graph_id: Graph database identifier to inspect

    Returns:
        Database information
    """
    db_path = self.base_path / f"{graph_id}.kuzu"

    if not db_path.exists():
      raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"Database {graph_id} not found",
      )

    try:
      # Calculate database size (single file in Kuzu 0.11.x)
      size_bytes = db_path.stat().st_size if db_path.is_file() else 0

      # Get creation time
      created_at = datetime.fromtimestamp(db_path.stat().st_ctime).isoformat()

      # Check if database is healthy
      is_healthy = self._check_database_health(graph_id)

      # Get last access time from connection pool
      last_accessed = None
      if self.connection_pool.has_active_connections(graph_id):
        last_accessed = datetime.now().isoformat()

      return DatabaseInfo(
        graph_id=graph_id,
        database_path=str(db_path),
        created_at=created_at,
        size_bytes=size_bytes,
        read_only=self.read_only,
        is_healthy=is_healthy,
        last_accessed=last_accessed,
      )

    except Exception as e:
      logger.error(f"Failed to get database info for {graph_id}: {e}")
      raise HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail=f"Failed to get database info: {str(e)}",
      )

  def get_all_databases_info(self) -> DatabaseListResponse:
    """
    Get information about all databases on this node.

    Returns:
        Complete database listing with metadata
    """
    databases = []
    total_size = 0

    for db_name in self.list_databases():
      try:
        db_info = self.get_database_info(db_name)
        databases.append(db_info)
        total_size += db_info.size_bytes
      except Exception as e:
        logger.error(f"Failed to get info for database {db_name}: {e}")

    # Calculate node capacity
    node_capacity = {
      "max_databases": self.max_databases,
      "current_databases": len(databases),
      "capacity_remaining": self.max_databases - len(databases),
      "utilization_percent": (len(databases) / self.max_databases) * 100,
    }

    return DatabaseListResponse(
      databases=databases,
      total_databases=len(databases),
      total_size_bytes=total_size,
      node_capacity=node_capacity,
    )

  def health_check_all(self) -> DatabaseHealthResponse:
    """
    Perform health checks on all databases.

    Returns:
        Health status for all databases
    """
    databases = []
    healthy_count = 0

    for db_name in self.list_databases():
      try:
        db_info = self.get_database_info(db_name)
        databases.append(db_info)
        if db_info.is_healthy:
          healthy_count += 1
      except Exception as e:
        logger.error(f"Health check failed for database {db_name}: {e}")

    return DatabaseHealthResponse(
      healthy_databases=healthy_count,
      unhealthy_databases=len(databases) - healthy_count,
      databases=databases,
    )

  def _apply_schema(
    self,
    conn: kuzu.Connection,
    schema_type: str,
    repository_name: Optional[str] = None,
    custom_ddl: Optional[str] = None,
  ) -> bool:
    """
    Apply schema to a new database based on type.

    Args:
        conn: Database connection
        schema_type: Type of schema to apply
        repository_name: Repository name for shared databases
        custom_ddl: Custom DDL commands for custom schema type

    Returns:
        True if schema was applied successfully
    """
    try:
      if schema_type == "entity":
        return self._apply_entity_schema(conn)
      elif schema_type == "shared":
        return self._apply_shared_schema(conn, repository_name)
      elif schema_type == "custom":
        return self._apply_custom_schema(conn, custom_ddl)
      else:
        logger.warning(f"Unknown schema type: {schema_type}")
        return False

    except Exception as e:
      logger.error(f"Failed to apply {schema_type} schema: {e}")
      return False

  def _apply_entity_schema(self, conn: kuzu.Connection) -> bool:
    """Apply entity graph schema using dynamic schema definitions."""
    try:
      from robosystems.schemas.loader import get_schema_loader

      # Use base schema only for maximum stability (align with direct file access)
      schema_loader = get_schema_loader(extensions=[])

      # Get all node types from the schema
      node_types = schema_loader.list_node_types()
      relationship_types = schema_loader.list_relationship_types()

      logger.info(
        f"Applying dynamic schema: {len(node_types)} node types, {len(relationship_types)} relationship types"
      )

      # Create node tables
      for node_name in node_types:
        node_schema = schema_loader.get_node_schema(node_name)
        if not node_schema:
          logger.warning(f"No schema found for node type: {node_name}")
          continue

        # Build column definitions from schema
        columns = []
        primary_key = None

        for prop in node_schema.properties:
          kuzu_type = self._map_schema_type_to_kuzu(prop.type)
          columns.append(f"{prop.name} {kuzu_type}")

          if prop.is_primary_key:
            primary_key = prop.name

        if not primary_key:
          logger.warning(f"No primary key defined for {node_name}, skipping")
          continue

        columns_str = ",\n        ".join(columns)

        create_sql = f"""
          CREATE NODE TABLE IF NOT EXISTS {node_name} (
              {columns_str},
              PRIMARY KEY ({primary_key})
          )"""

        try:
          conn.execute(create_sql.strip())
          logger.debug(f"Created node table: {node_name}")
        except Exception as e:
          if "already exists" not in str(e).lower():
            logger.warning(f"Failed to create node table {node_name}: {e}")

      # Create relationship tables
      for rel_name in relationship_types:
        rel_schema = schema_loader.get_relationship_schema(rel_name)
        if not rel_schema:
          logger.warning(f"No schema found for relationship type: {rel_name}")
          continue

        # Build relationship definition
        from_node = rel_schema.from_node
        to_node = rel_schema.to_node

        # Check if from/to nodes exist in our schema
        if from_node not in node_types or to_node not in node_types:
          logger.debug(
            f"Skipping relationship {rel_name}: missing nodes {from_node} or {to_node}"
          )
          continue

        # Build property definitions if any
        if rel_schema.properties:
          prop_definitions = []
          for prop in rel_schema.properties:
            kuzu_type = self._map_schema_type_to_kuzu(prop.type)
            prop_definitions.append(f"{prop.name} {kuzu_type}")

          props_str = ",\n            " + ",\n            ".join(prop_definitions)
          create_sql = f"""
            CREATE REL TABLE IF NOT EXISTS {rel_name} (
                FROM {from_node} TO {to_node}{props_str}
            )"""
        else:
          create_sql = f"""
            CREATE REL TABLE IF NOT EXISTS {rel_name} (
                FROM {from_node} TO {to_node}
            )"""

        try:
          conn.execute(create_sql.strip())
          logger.debug(f"Created relationship table: {rel_name}")
        except Exception as e:
          if "already exists" not in str(e).lower():
            logger.warning(f"Failed to create relationship table {rel_name}: {e}")

      logger.info(
        f"Dynamic schema applied successfully: {len(node_types)} nodes, {len(relationship_types)} relationships"
      )
      return True

    except Exception as e:
      logger.error(f"Failed to apply dynamic schema: {e}")
      # Fallback to minimal hardcoded schema for backward compatibility
      logger.warning("Falling back to minimal hardcoded schema")
      return self._apply_fallback_entity_schema(conn)

  def _apply_fallback_entity_schema(self, conn: kuzu.Connection) -> bool:
    """Fallback to minimal hardcoded schema if dynamic schema fails."""
    minimal_statements = [
      # Only create the absolute minimum for basic functionality
      """CREATE NODE TABLE IF NOT EXISTS Entity(
          identifier STRING,
          name STRING,
          PRIMARY KEY (identifier)
      )""",
      """CREATE NODE TABLE IF NOT EXISTS User(
          identifier STRING,
          name STRING,
          PRIMARY KEY (identifier)
      )""",
      "CREATE REL TABLE IF NOT EXISTS HAS_USER(FROM Entity TO User)",
    ]

    try:
      for statement in minimal_statements:
        conn.execute(statement.strip())
      logger.info("Fallback schema applied successfully")
      return True
    except Exception as e:
      logger.error(f"Failed to apply fallback schema: {e}")
      return False

  def _map_schema_type_to_kuzu(self, schema_type: str) -> str:
    """Map schema property types to Kuzu types."""
    type_mapping = {
      "STRING": "STRING",
      "INT64": "INT64",
      "INT32": "INT32",
      "DOUBLE": "DOUBLE",
      "FLOAT": "FLOAT",
      "BOOLEAN": "BOOLEAN",
      "TIMESTAMP": "TIMESTAMP",
      "DATE": "DATE",
      "BLOB": "BLOB",
    }

    return type_mapping.get(schema_type.upper(), "STRING")

  def _apply_custom_schema(
    self, conn: kuzu.Connection, custom_ddl: Optional[str]
  ) -> bool:
    """Apply custom schema DDL to database."""
    if not custom_ddl:
      logger.error("Custom DDL is required for custom schema type")
      return False

    try:
      # Split DDL into individual statements
      statements = [stmt.strip() for stmt in custom_ddl.split(";") if stmt.strip()]

      logger.info(f"Applying custom schema with {len(statements)} DDL statements")

      # Execute each statement
      for i, statement in enumerate(statements):
        try:
          conn.execute(statement)
          logger.debug(f"Executed custom DDL statement {i + 1}/{len(statements)}")
        except Exception as e:
          logger.error(
            f"Failed to execute DDL statement {i + 1}: {statement[:100]}... Error: {e}"
          )
          raise

      logger.info("Custom schema applied successfully")
      return True

    except Exception as e:
      logger.error(f"Failed to apply custom schema: {e}")
      return False

  def _apply_shared_schema(
    self, conn: kuzu.Connection, repository_name: Optional[str]
  ) -> bool:
    """Apply shared repository schema using appropriate extensions."""
    try:
      from robosystems.schemas.loader import (
        get_sec_schema_loader,
        get_schema_loader,
      )

      # Use repository-specific schema loaders
      if repository_name == "sec":
        # SEC repository: base + roboledger only
        schema_loader = get_sec_schema_loader()
        logger.info("Using SEC-specific schema (base + roboledger)")
      else:
        # Other repositories: use base schema only for stability
        schema_loader = get_schema_loader(extensions=[])
        logger.info(f"Using full schema for repository: {repository_name}")

      # Get schema types
      node_types = schema_loader.list_node_types()
      relationship_types = schema_loader.list_relationship_types()

      logger.info(
        f"Applying shared schema: {len(node_types)} node types, {len(relationship_types)} relationship types"
      )

      # Create node tables
      for node_name in node_types:
        node_schema = schema_loader.get_node_schema(node_name)
        if not node_schema:
          logger.warning(f"No schema found for node type: {node_name}")
          continue

        # Build column definitions from schema
        columns = []
        primary_key = None

        for prop in node_schema.properties:
          kuzu_type = self._map_schema_type_to_kuzu(prop.type)
          columns.append(f"{prop.name} {kuzu_type}")

          if prop.is_primary_key:
            primary_key = prop.name

        if not primary_key:
          logger.warning(f"No primary key defined for {node_name}, skipping")
          continue

        columns_str = ",\n        ".join(columns)

        create_sql = f"""
          CREATE NODE TABLE IF NOT EXISTS {node_name} (
              {columns_str},
              PRIMARY KEY ({primary_key})
          )"""

        try:
          conn.execute(create_sql.strip())
          logger.debug(f"Created shared node table: {node_name}")
        except Exception as e:
          if "already exists" not in str(e).lower():
            logger.warning(f"Failed to create shared node table {node_name}: {e}")

      # Create relationship tables
      for rel_name in relationship_types:
        rel_schema = schema_loader.get_relationship_schema(rel_name)
        if not rel_schema:
          logger.warning(f"No schema found for relationship type: {rel_name}")
          continue

        # Build relationship definition
        from_node = rel_schema.from_node
        to_node = rel_schema.to_node

        # Check if from/to nodes exist in our schema
        if from_node not in node_types or to_node not in node_types:
          logger.debug(
            f"Skipping relationship {rel_name}: missing nodes {from_node} or {to_node}"
          )
          continue

        # Build property definitions if any
        if rel_schema.properties:
          prop_definitions = []
          for prop in rel_schema.properties:
            kuzu_type = self._map_schema_type_to_kuzu(prop.type)
            prop_definitions.append(f"{prop.name} {kuzu_type}")

          props_str = ",\n            " + ",\n            ".join(prop_definitions)
          create_sql = f"""
            CREATE REL TABLE IF NOT EXISTS {rel_name} (
                FROM {from_node} TO {to_node}{props_str}
            )"""
        else:
          create_sql = f"""
            CREATE REL TABLE IF NOT EXISTS {rel_name} (
                FROM {from_node} TO {to_node}
            )"""

        try:
          conn.execute(create_sql.strip())
          logger.debug(f"Created shared relationship table: {rel_name}")
        except Exception as e:
          if "already exists" not in str(e).lower():
            logger.warning(
              f"Failed to create shared relationship table {rel_name}: {e}"
            )

      logger.info(
        f"Shared schema applied successfully for {repository_name}: {len(node_types)} nodes, {len(relationship_types)} relationships"
      )
      return True

    except Exception as e:
      logger.error(f"Failed to apply shared schema for {repository_name}: {e}")
      return False

  def _check_database_health(self, graph_id: str) -> bool:
    """
    Check if a database is healthy using connection pool.

    Args:
        graph_id: Graph database identifier to check

    Returns:
        True if database is healthy
    """
    try:
      db_path = self.base_path / f"{graph_id}.kuzu"

      # Basic file system check
      if not db_path.exists():
        return False

      # Try to get a connection from the pool and execute a simple query
      try:
        with self.connection_pool.get_connection(graph_id, read_only=True) as conn:
          # Execute a lightweight test query
          result = conn.execute("RETURN 1 AS health_check")
          # Consume the result to ensure query completed
          # Handle both single QueryResult and list[QueryResult] cases
          if isinstance(result, list):
            # If result is a list, close each QueryResult
            for r in result:
              if hasattr(r, "close"):
                r.close()
          elif hasattr(result, "close"):
            # If result is a single QueryResult, close it directly
            result.close()
          logger.debug(f"Health check passed for {graph_id}")
          return True
      except Exception as conn_error:
        # Log the specific error for debugging
        logger.warning(
          f"Health check connection failed for {graph_id}: {conn_error}. "
          "This may be due to Docker volume permissions or Kuzu recovery issues."
        )
        # For now, if we can't connect but file exists, consider it "unhealthy but present"
        # This allows us to track problematic databases without crashing
        return False

    except Exception as e:
      logger.error(f"Database health check failed for {graph_id}: {e}")
      return False

  def close_all_connections(self):
    """Close all open database connections using the connection pool."""
    try:
      # Close connections in the thread-safe pool
      self.connection_pool.close_all_connections()
      logger.info("Closed all connections in connection pool")
    except Exception as e:
      logger.error(f"Failed to close connection pool: {e}")

  def _update_instance_registry(self):
    """Update the instance registry in DynamoDB with current database count."""
    try:
      import boto3

      # Get DynamoDB client
      dynamodb = boto3.client(
        "dynamodb",
        endpoint_url=env.AWS_ENDPOINT_URL or "http://localstack:4566",
        region_name="us-east-1",
      )

      # Count databases (Kuzu 0.11.x uses .kuzu files)
      db_count = len([f for f in self.base_path.glob("*.kuzu") if f.is_file()])
      capacity_pct = int((db_count / self.max_databases) * 100)

      # Update the instance registry
      dynamodb.update_item(
        TableName=env.INSTANCE_REGISTRY_TABLE,
        Key={"instance_id": {"S": "local-kuzu-writer"}},
        UpdateExpression="SET database_count = :count, available_capacity_pct = :cap",
        ExpressionAttributeValues={
          ":count": {"N": str(db_count)},
          ":cap": {"N": str(100 - capacity_pct)},
        },
      )

      logger.debug(
        f"Updated instance registry: {db_count} databases, {capacity_pct}% used"
      )

    except Exception as e:
      # Don't fail database creation if registry update fails
      logger.warning(f"Failed to update instance registry: {e}")

  def _register_shared_repository(self, graph_id: str):
    """Register a shared repository in DynamoDB instance registry."""
    try:
      import boto3
      from datetime import datetime, timezone

      # Get instance ID from EC2 metadata or hostname
      from robosystems.config import env as config_env
      import re

      instance_id = config_env.EC2_INSTANCE_ID

      # Validate if we have a valid EC2 instance ID from environment
      if instance_id and not re.match(r"^i-[0-9a-f]{8,17}$", instance_id):
        logger.warning(
          f"EC2_INSTANCE_ID from environment '{instance_id}' is not a valid EC2 instance ID. "
          f"Skipping instance registry update."
        )
        return

      if not instance_id:
        try:
          # Try to get from EC2 metadata service
          import urllib.request

          response = urllib.request.urlopen(
            "http://169.254.169.254/latest/meta-data/instance-id", timeout=1
          )
          instance_id = response.read().decode("utf-8")

          # Validate that this is actually an EC2 instance ID
          import re

          if not re.match(r"^i-[0-9a-f]{8,17}$", instance_id):
            logger.warning(
              f"Retrieved instance ID '{instance_id}' is not a valid EC2 instance ID format. "
              f"Skipping instance registry update (likely running in container)."
            )
            return
        except Exception as e:
          # If we can't get EC2 metadata, we're likely in a container
          logger.info(
            f"Unable to retrieve EC2 instance ID (likely running in container): {e}. "
            f"Skipping instance registry update."
          )
          return

      # Get table name from centralized config
      table_name = config_env.INSTANCE_REGISTRY_TABLE

      # Get DynamoDB client
      dynamodb = boto3.client("dynamodb", region_name="us-east-1")

      # Check if instance exists in registry
      response = dynamodb.get_item(
        TableName=table_name, Key={"instance_id": {"S": instance_id}}
      )

      # Update or create the instance entry with the shared repository
      if "Item" in response:
        # Instance exists, add to allocated_databases
        current_dbs = response["Item"].get("allocated_databases", {}).get("SS", [])
        if graph_id not in current_dbs:
          current_dbs.append(graph_id)

        dynamodb.update_item(
          TableName=table_name,
          Key={"instance_id": {"S": instance_id}},
          UpdateExpression="SET allocated_databases = :dbs, last_allocation_time = :time",
          ExpressionAttributeValues={
            ":dbs": {"SS": current_dbs},
            ":time": {"S": datetime.now(timezone.utc).isoformat()},
          },
        )
      else:
        # New instance, create entry
        dynamodb.put_item(
          TableName=table_name,
          Item={
            "instance_id": {"S": instance_id},
            "allocated_databases": {"SS": [graph_id]},
            "status": {"S": "healthy"},
            "last_allocation_time": {"S": datetime.now(timezone.utc).isoformat()},
          },
        )

      logger.info(
        f"Registered shared repository {graph_id} on instance {instance_id} in {table_name}"
      )

    except Exception as e:
      # Don't fail database creation if registry update fails
      logger.warning(f"Failed to register shared repository {graph_id}: {e}")
