"""
Base interface for graph database engines.
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from dataclasses import dataclass


@dataclass
class GraphOperation:
  """Represents a single graph operation (reused from repository.py)."""

  cypher: str
  params: Dict[str, Any]
  description: Optional[str] = None


class GraphEngineInterface(ABC):
  """
  Abstract base class for graph database engines.

  This interface defines the contract that all graph database engines
  must implement to work with the RoboSystems middleware.
  """

  @abstractmethod
  def execute_query(
    self, cypher: str, params: Optional[Dict[str, Any]] = None
  ) -> List[Dict[str, Any]]:
    """
    Execute a single Cypher query and return results.

    Args:
        cypher: The Cypher query string
        params: Optional parameters for the query

    Returns:
        List of result records as dictionaries

    Raises:
        NotImplementedError: Must be implemented by subclasses
    """
    raise NotImplementedError("Subclasses must implement execute_query")

  @abstractmethod
  def execute_single(
    self, cypher: str, params: Optional[Dict[str, Any]] = None
  ) -> Optional[Dict[str, Any]]:
    """
    Execute a query expecting a single result.

    Args:
        cypher: The Cypher query string
        params: Optional parameters for the query

    Returns:
        Single result record or None

    Raises:
        NotImplementedError: Must be implemented by subclasses
    """
    raise NotImplementedError("Subclasses must implement execute_single")

  @abstractmethod
  def execute_transaction(
    self, operations: List[GraphOperation]
  ) -> List[List[Dict[str, Any]]]:
    """
    Execute multiple operations in a single transaction.

    Args:
        operations: List of GraphOperation objects

    Returns:
        List of result lists for each operation

    Raises:
        NotImplementedError: Must be implemented by subclasses
    """
    raise NotImplementedError("Subclasses must implement execute_transaction")

  @abstractmethod
  def health_check(self) -> Dict[str, Any]:
    """
    Perform a health check on the database connection.

    Returns:
        Health status information

    Raises:
        NotImplementedError: Must be implemented by subclasses
    """
    raise NotImplementedError("Subclasses must implement health_check")

  @abstractmethod
  def close(self) -> None:
    """
    Close the database connection.

    Raises:
        NotImplementedError: Must be implemented by subclasses
    """
    raise NotImplementedError("Subclasses must implement close")
