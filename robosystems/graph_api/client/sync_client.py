"""
Synchronous wrapper for GraphClient.

This module provides a synchronous wrapper around the async GraphClient
for use in CLI tools and other synchronous contexts.
"""

import asyncio
from typing import Any, Dict, List, Optional, cast
from .client import GraphClient


class GraphSyncClient:
  """Synchronous wrapper around the async GraphClient."""

  def __init__(
    self,
    base_url: Optional[str] = None,
    **kwargs,
  ):
    """Initialize sync client with async client underneath."""
    self._client = GraphClient(base_url=base_url, **kwargs)
    self._loop = None

  def __enter__(self):
    """Context manager entry."""
    return self

  def __exit__(self, exc_type, exc_val, exc_tb):
    """Context manager exit."""
    self.close()

  def _run_async(self, coro):
    """Run an async coroutine and return the result."""
    # Try to get the running loop, create one if needed
    try:
      self._loop = asyncio.get_running_loop()
      # If we're already in an async context, we need to run in a thread
      import concurrent.futures

      with concurrent.futures.ThreadPoolExecutor() as executor:
        future = executor.submit(asyncio.run, coro)
        return future.result()
    except RuntimeError:
      # No running loop, we can use asyncio.run directly
      return asyncio.run(coro)

  def close(self):
    """Close the client."""
    try:
      self._run_async(self._client.close())
    except RuntimeError as e:
      # Handle the case where the event loop is already closed
      if "Event loop is closed" in str(e):
        pass  # Ignore this error, client cleanup will happen on GC
      else:
        raise

  # Proxy all methods to async client with sync wrapper

  def health_check(self) -> Dict[str, Any]:
    """Check API health status."""
    return self._run_async(self._client.health_check())

  def query(
    self,
    cypher: str,
    graph_id: str = "sec",
    parameters: Optional[Dict[str, Any]] = None,
    streaming: bool = False,
  ) -> Dict[str, Any]:
    """Execute a Cypher query."""
    if streaming:
      raise NotImplementedError("Streaming not supported in sync client")
    return cast(
      Dict[str, Any],
      self._run_async(self._client.query(cypher, graph_id, parameters, streaming)),
    )

  def get_info(self) -> Dict[str, Any]:
    """Get database information."""
    return self._run_async(self._client.get_info())

  def list_databases(self) -> Dict[str, Any]:
    """List all databases."""
    return self._run_async(self._client.list_databases())

  def get_database(self, graph_id: str) -> Dict[str, Any]:
    """Get specific database information."""
    return self._run_async(self._client.get_database(graph_id))

  def create_database(
    self,
    graph_id: str,
    schema_type: str = "entity",
    repository_name: Optional[str] = None,
    custom_schema_ddl: Optional[str] = None,
    is_subgraph: bool = False,
  ) -> Dict[str, Any]:
    """Create a new database."""
    return self._run_async(
      self._client.create_database(
        graph_id, schema_type, repository_name, custom_schema_ddl, is_subgraph
      )
    )

  def delete_database(self, graph_id: str) -> Dict[str, Any]:
    """Delete a database."""
    return self._run_async(self._client.delete_database(graph_id))

  def ingest(
    self,
    graph_id: str,
    file_path: Optional[str] = None,
    table_name: Optional[str] = None,
    pipeline_run_id: Optional[str] = None,
    bucket: Optional[str] = None,
    files: Optional[List[str]] = None,
    mode: str = "sync",
    priority: int = 5,
    ignore_errors: bool = True,
  ) -> Dict[str, Any]:
    """Unified data ingestion with flexible execution modes."""
    return self._run_async(
      self._client.ingest(
        graph_id,
        file_path,
        table_name,
        pipeline_run_id,
        bucket,
        files,
        mode,
        priority,
        ignore_errors,
      )
    )

  def get_task_status(self, task_id: str) -> Dict[str, Any]:
    """Get background task status."""
    return self._run_async(self._client.get_task_status(task_id))

  def list_tasks(
    self, status: Optional[str] = None, limit: int = 100
  ) -> Dict[str, Any]:
    """List tasks with optional status filter."""
    return self._run_async(self._client.list_tasks(status, limit))

  def cancel_task(self, task_id: str) -> Dict[str, Any]:
    """Cancel a pending task."""
    return self._run_async(self._client.cancel_task(task_id))

  def get_queue_info(self) -> Dict[str, Any]:
    """Get ingestion queue information."""
    return self._run_async(self._client.get_queue_info())

  def execute_query(
    self, cypher: str, params: Optional[Dict[str, Any]] = None
  ) -> List[Dict[str, Any]]:
    """Execute a query and return data rows."""
    return self._run_async(self._client.execute_query(cypher, params))

  def execute_single(
    self, cypher: str, params: Optional[Dict[str, Any]] = None
  ) -> Optional[Dict[str, Any]]:
    """Execute a query expecting a single result."""
    return self._run_async(self._client.execute_single(cypher, params))

  def get_schema(self) -> List[Dict[str, Any]]:
    """Get database schema information."""
    return self._run_async(self._client.get_schema())

  def database_exists(self, graph_id: str) -> bool:
    """Check if a database exists."""
    return self._run_async(self._client.database_exists(graph_id))

  def ensure_database_exists(self, graph_id: str, schema_type: str = "entity") -> None:
    """Ensure a database exists, creating it if necessary."""
    return self._run_async(self._client.ensure_database_exists(graph_id, schema_type))

  def execute_ddl(self, ddl: str, graph_id: Optional[str] = None) -> Dict[str, Any]:
    """Execute DDL statements."""
    return self._run_async(self._client.execute_ddl(ddl, graph_id))

  def node_exists(self, label: str, filters: Optional[Dict[str, Any]] = None) -> bool:
    """Check if a node exists with the given label and filters."""
    return self._run_async(self._client.node_exists(label, filters))

  def create_backup(
    self,
    graph_id: str,
    backup_format: str = "full_dump",
    compression: bool = True,
    encryption: bool = False,
  ) -> Dict[str, Any]:
    """Create a backup of a database."""
    return self._run_async(
      self._client.create_backup(graph_id, backup_format, compression, encryption)
    )

  def download_backup(self, graph_id: str) -> Dict[str, Any]:
    """Download the current database as a backup."""
    return self._run_async(self._client.download_backup(graph_id))

  def restore_backup(
    self,
    graph_id: str,
    backup_data: bytes,
    create_system_backup: bool = True,
    force_overwrite: bool = False,
  ) -> Dict[str, Any]:
    """Restore a database from backup."""
    return self._run_async(
      self._client.restore_backup(
        graph_id, backup_data, create_system_backup, force_overwrite
      )
    )

  def get_database_info(self, graph_id: str) -> Dict[str, Any]:
    """Get comprehensive database information and statistics."""
    return self._run_async(self._client.get_database_info(graph_id))

  # DuckDB Table Management Methods

  def create_table(
    self,
    graph_id: str,
    table_name: str,
    s3_pattern: str,
  ) -> Dict[str, Any]:
    """Create a DuckDB staging table (sync wrapper)."""
    return self._run_async(self._client.create_table(graph_id, table_name, s3_pattern))

  def list_tables(self, graph_id: str) -> List[Dict[str, Any]]:
    """List all DuckDB staging tables for a graph (sync wrapper)."""
    return self._run_async(self._client.list_tables(graph_id))

  def query_table(self, graph_id: str, sql: str) -> Dict[str, Any]:
    """Execute SQL query on DuckDB staging tables (sync wrapper)."""
    return self._run_async(self._client.query_table(graph_id, sql))

  def delete_table(self, graph_id: str, table_name: str) -> Dict[str, Any]:
    """Delete a DuckDB staging table (sync wrapper)."""
    return self._run_async(self._client.delete_table(graph_id, table_name))

  def ingest_table_to_graph(
    self,
    graph_id: str,
    table_name: str,
    ignore_errors: bool = True,
  ) -> Dict[str, Any]:
    """Ingest a DuckDB staging table into the Kuzu graph (sync wrapper)."""
    return self._run_async(
      self._client.ingest_table_to_graph(graph_id, table_name, ignore_errors)
    )

  # Setters for compatibility
  @property
  def graph_id(self):
    """Get the default graph_id."""
    return self._client.graph_id

  @graph_id.setter
  def graph_id(self, value):
    """Set the default graph_id."""
    self._client.graph_id = value
