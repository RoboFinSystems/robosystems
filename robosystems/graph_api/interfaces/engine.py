"""
Base interface for graph database engines.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any


@dataclass
class GraphOperation:
  """One Cypher statement and its parameters, for batched execution."""

  cypher: str
  params: dict[str, Any]
  description: str | None = None


class GraphEngineInterface(ABC):
  """Contract every graph engine implements for the RoboSystems middleware.

  Implemented by :class:`robosystems.graph_api.core.ladybug.engine.Engine`.
  """

  @abstractmethod
  def execute_query(
    self, cypher: str, params: dict[str, Any] | None = None
  ) -> list[dict[str, Any]]:
    """Execute a Cypher query and return its rows as dictionaries."""
    raise NotImplementedError("Subclasses must implement execute_query")

  @abstractmethod
  def execute_single(
    self, cypher: str, params: dict[str, Any] | None = None
  ) -> dict[str, Any] | None:
    """Execute a query and return its first row, or None if empty."""
    raise NotImplementedError("Subclasses must implement execute_single")

  @abstractmethod
  def execute_transaction(
    self, operations: list[GraphOperation]
  ) -> list[list[dict[str, Any]]]:
    """Execute operations in order, returning one result list per operation.

    Implementations are not required to be atomic — see the LadybugDB engine,
    which has no rollback.
    """
    raise NotImplementedError("Subclasses must implement execute_transaction")

  @abstractmethod
  def health_check(self) -> dict[str, Any]:
    """Report connection health; implementations report status, not raise."""
    raise NotImplementedError("Subclasses must implement health_check")

  @abstractmethod
  def close(self) -> None:
    """Close the connection and release its resources."""
    raise NotImplementedError("Subclasses must implement close")
