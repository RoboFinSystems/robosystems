"""
Repository Wrapper

This module provides a unified interface for both direct file repositories (synchronous)
and API-based repositories (asynchronous via GraphClient), automatically detecting
the repository type and conditionally awaiting methods as needed.

Key features:
- Automatic detection of sync vs async repository methods
- Conditional awaiting without code duplication
- Type-safe method proxying
- Backward compatibility with existing code
- Comprehensive method coverage
"""

import inspect
import asyncio
from typing import List, Dict, Any, Optional, Union, TYPE_CHECKING

from .base import GraphOperation
from robosystems.graph_api.client import GraphClient
from robosystems.logger import logger

if TYPE_CHECKING:
  from robosystems.graph_api.core.ladybug import Repository


class UniversalRepository:
  """
  Universal repository wrapper that handles both sync and async repositories.

  This wrapper automatically detects whether the underlying repository is
  synchronous (direct file) or asynchronous (API-based) and handles
  method calls accordingly.
  """

  def __init__(self, repository: Union["Repository", GraphClient]):
    """
    Initialize the universal repository wrapper.

    Args:
        repository: Either a Repository (sync) or GraphClient (async)
    """
    self._repository = repository
    # Check if it's async
    self._is_async = isinstance(repository, GraphClient)

    # Cache method inspection results for performance
    self._method_cache = {}

    logger.debug(
      f"Initialized UniversalRepository with {'async' if self._is_async else 'sync'} repository"
    )

  @property
  def is_async(self) -> bool:
    """Check if the underlying repository is asynchronous."""
    return self._is_async

  @property
  def repository_type(self) -> str:
    """Get the type of the underlying repository."""
    return "api" if self._is_async else "direct"

  def _is_method_async(self, method_name: str) -> bool:
    """
    Check if a repository method is asynchronous.

    Args:
        method_name: Name of the method to check

    Returns:
        True if the method is async, False otherwise
    """
    if method_name in self._method_cache:
      return self._method_cache[method_name]

    if not hasattr(self._repository, method_name):
      self._method_cache[method_name] = False
      return False

    method = getattr(self._repository, method_name)
    is_async = inspect.iscoroutinefunction(method)
    self._method_cache[method_name] = is_async

    return is_async

  async def _call_method(self, method_name: str, *args, **kwargs) -> Any:
    """
    Call a repository method, conditionally awaiting if it's async.

    Args:
        method_name: Name of the method to call
        *args: Positional arguments
        **kwargs: Keyword arguments

    Returns:
        Method result
    """
    if not hasattr(self._repository, method_name):
      raise AttributeError(f"Repository has no method '{method_name}'")

    method = getattr(self._repository, method_name)

    if self._is_method_async(method_name):
      return await method(*args, **kwargs)
    else:
      return method(*args, **kwargs)

  def _call_method_sync(self, method_name: str, *args, **kwargs) -> Any:
    """
    Call a repository method synchronously, using asyncio.run for async methods.

    Args:
        method_name: Name of the method to call
        *args: Positional arguments
        **kwargs: Keyword arguments

    Returns:
        Method result
    """
    if not hasattr(self._repository, method_name):
      raise AttributeError(f"Repository has no method '{method_name}'")

    method = getattr(self._repository, method_name)

    if self._is_method_async(method_name):
      return asyncio.run(method(*args, **kwargs))
    else:
      return method(*args, **kwargs)

  # Core database operations
  async def execute_query(
    self, cypher: str, params: Optional[Dict[str, Any]] = None
  ) -> List[Dict[str, Any]]:
    """Execute a single Cypher query and return results."""
    return await self._call_method("execute_query", cypher, params)

  async def execute_query_streaming(
    self, cypher: str, params: Optional[Dict[str, Any]] = None, chunk_size: int = 1000
  ):
    """
    Execute a query and yield results in chunks for streaming.

    This method checks if the underlying repository supports streaming,
    and falls back to chunking regular results if not.

    Args:
        cypher: The Cypher query to execute
        params: Optional query parameters
        chunk_size: Number of rows per chunk

    Yields:
        Dict containing chunk data
    """
    # Check if underlying repository supports streaming
    if hasattr(self._repository, "execute_query_streaming"):
      # Use native streaming support
      streaming_method = getattr(self._repository, "execute_query_streaming")
      async for chunk in streaming_method(cypher, params, chunk_size):
        yield chunk
    else:
      # Fallback: Execute normally and chunk results
      result = await self.execute_query(cypher, params)

      # Convert to streaming format
      columns = list(result[0].keys()) if result else []
      total_rows = len(result)

      for i in range(0, total_rows, chunk_size):
        chunk_data = result[i : i + chunk_size]
        chunk = {
          "chunk_index": i // chunk_size,
          "data": chunk_data,
          "columns": columns if i == 0 else [],
          "is_last_chunk": i + chunk_size >= total_rows,
          "row_count": len(chunk_data),
          "total_rows_sent": min(i + chunk_size, total_rows),
        }
        yield chunk

  async def execute_single(
    self, cypher: str, params: Optional[Dict[str, Any]] = None
  ) -> Optional[Dict[str, Any]]:
    """Execute a query expecting a single result."""
    return await self._call_method("execute_single", cypher, params)

  async def execute_transaction(
    self, operations: List[GraphOperation]
  ) -> List[List[Dict[str, Any]]]:
    """Execute multiple operations in a transaction."""
    return await self._call_method("execute_transaction", operations)

  async def count_nodes(
    self, label: str, filters: Optional[Dict[str, Any]] = None
  ) -> int:
    """Count nodes with optional filters."""
    return await self._call_method("count_nodes", label, filters)

  async def node_exists(self, label: str, filters: Dict[str, Any]) -> bool:
    """Check if a node exists with given filters."""
    logger.debug(f"UniversalRepository.node_exists called for label: {label}")
    try:
      result = await self._call_method("node_exists", label, filters)
      logger.debug(f"UniversalRepository.node_exists result: {result}")
      return result
    except Exception as e:
      logger.error(f"UniversalRepository.node_exists failed: {e}")
      raise

  async def health_check(self) -> Dict[str, Any]:
    """Perform a health check on the repository."""
    return await self._call_method("health_check")

  async def close(self) -> None:
    """Close the repository connection."""
    return await self._call_method("close")

  # Backward compatibility methods (synchronous versions)
  def execute_query_sync(
    self, cypher: str, params: Optional[Dict[str, Any]] = None
  ) -> List[Dict[str, Any]]:
    """Execute a single Cypher query and return results (sync)."""
    return self._call_method_sync("execute_query", cypher, params)

  def execute_single_sync(
    self, cypher: str, params: Optional[Dict[str, Any]] = None
  ) -> Optional[Dict[str, Any]]:
    """Execute a query expecting a single result (sync)."""
    return self._call_method_sync("execute_single", cypher, params)

  def execute_transaction_sync(
    self, operations: List[GraphOperation]
  ) -> List[List[Dict[str, Any]]]:
    """Execute multiple operations in a transaction (sync)."""
    return self._call_method_sync("execute_transaction", operations)

  def count_nodes_sync(
    self, label: str, filters: Optional[Dict[str, Any]] = None
  ) -> int:
    """Count nodes with optional filters (sync)."""
    return self._call_method_sync("count_nodes", label, filters)

  def node_exists_sync(self, label: str, filters: Dict[str, Any]) -> bool:
    """Check if a node exists with given filters (sync)."""
    return self._call_method_sync("node_exists", label, filters)

  def health_check_sync(self) -> Dict[str, Any]:
    """Perform a health check on the repository (sync)."""
    return self._call_method_sync("health_check")

  def close_sync(self) -> None:
    """Close the repository connection (sync)."""
    return self._call_method_sync("close")

  # Alias for backward compatibility
  def execute(
    self, cypher: str, params: Optional[Dict[str, Any]] = None
  ) -> List[Dict[str, Any]]:
    """Alias for execute_query_sync for backward compatibility."""
    return self.execute_query_sync(cypher, params)

  # Context manager support (async)
  async def __aenter__(self):
    """Async context manager entry."""
    if hasattr(self._repository, "__aenter__"):
      aenter_method = getattr(self._repository, "__aenter__")
      await aenter_method()
    return self

  async def __aexit__(self, exc_type, exc_val, exc_tb):
    """Async context manager exit."""
    if hasattr(self._repository, "__aexit__"):
      aexit_method = getattr(self._repository, "__aexit__")
      await aexit_method(exc_type, exc_val, exc_tb)
    elif hasattr(self._repository, "close"):
      await self.close()

  # Context manager support (sync)
  def __enter__(self):
    """Sync context manager entry."""
    if hasattr(self._repository, "__enter__"):
      enter_method = getattr(self._repository, "__enter__")
      enter_method()
    return self

  def __exit__(self, exc_type, exc_val, exc_tb):
    """Sync context manager exit."""
    if hasattr(self._repository, "__exit__"):
      exit_method = getattr(self._repository, "__exit__")
      exit_method(exc_type, exc_val, exc_tb)
    elif hasattr(self._repository, "close"):
      self.close_sync()

  # Property delegation
  @property
  def database_name(self) -> str:
    """Get the database name."""
    # Try different property names that might exist on different repository types
    db_name = getattr(self._repository, "database_name", None)
    if db_name is not None:
      return str(db_name)

    db_path = getattr(self._repository, "database_path", None)
    if db_path is not None:
      return str(db_path)

    return "unknown"

  @property
  def read_only(self) -> bool:
    """Check if the repository is read-only."""
    return getattr(self._repository, "read_only", False)

  # API-specific methods (only available for APIRepository)
  async def get_schema(self) -> List[Dict[str, Any]]:
    """Get database schema information (API repositories only)."""
    if not hasattr(self._repository, "get_schema"):
      raise NotImplementedError("get_schema is only available for API repositories")
    return await self._call_method("get_schema")

  # Direct access to underlying repository for advanced use cases
  def get_underlying_repository(self) -> Union["Repository", GraphClient]:
    """Get the underlying repository instance."""
    return self._repository

  def __repr__(self) -> str:
    """String representation of the universal repository."""
    repo_type = "GraphClient" if self._is_async else "Repository"
    return f"UniversalRepository({repo_type}({self.database_name}))"


# Convenience functions for creating universal repositories
def create_universal_repository(
  graph_id: str, operation_type: str = "write", tier=None
) -> UniversalRepository:
  """
  Create a universal repository for the specified graph.

  Args:
      graph_id: Database identifier
      operation_type: "read" or "write"
      tier: Instance tier for routing

  Returns:
      UniversalRepository instance
  """
  from .router import get_graph_repository

  if tier is None:
    from .types import GraphTier

    tier = GraphTier.LADYBUG_STANDARD

  # Note: get_graph_repository is async, but this is a sync function
  # This should ideally be an async function, but keeping for compatibility
  import asyncio

  repository = asyncio.run(get_graph_repository(graph_id, operation_type, tier))
  return UniversalRepository(repository)


async def create_universal_repository_with_auth(
  graph_id: str, current_user, operation_type: str = "write"
) -> UniversalRepository:
  """
  Create a universal repository with user authorization.

  Args:
      graph_id: Database identifier
      current_user: Authenticated user
      operation_type: "read" or "write"

  Returns:
      UniversalRepository instance
  """
  from .dependencies import get_graph_repository_with_auth

  repository = await get_graph_repository_with_auth(
    graph_id, current_user, operation_type
  )
  return UniversalRepository(repository)


# Utility functions for repository detection
def is_api_repository(
  repository: Union["Repository", GraphClient, UniversalRepository],
) -> bool:
  """Check if a repository is an API repository."""
  if isinstance(repository, UniversalRepository):
    return repository.is_async
  return isinstance(repository, GraphClient)


def is_direct_repository(
  repository: Union["Repository", GraphClient, UniversalRepository],
) -> bool:
  """Check if a repository is a direct file repository."""
  from robosystems.graph_api.core.ladybug import Repository

  if isinstance(repository, UniversalRepository):
    return not repository.is_async
  return isinstance(repository, Repository)


def get_repository_type(
  repository: Union["Repository", GraphClient, UniversalRepository],
) -> str:
  """Get the type of a repository."""
  from robosystems.graph_api.core.ladybug import Repository

  if isinstance(repository, UniversalRepository):
    return repository.repository_type
  elif isinstance(repository, GraphClient):
    return "api"
  elif isinstance(repository, Repository):
    return "direct"
  else:
    return "unknown"
