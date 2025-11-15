"""
Graph API Client - Async client for graph database access.

This module provides an asynchronous client for interacting with graph database
backends (Kuzu and Neo4j) via the Graph API.
"""

from .client import GraphClient
from .exceptions import (
  GraphAPIError,
  GraphTransientError,
  GraphClientError,
  GraphServerError,
  GraphTimeoutError,
  GraphSyntaxError,
)
from .config import GraphClientConfig
from .factory import (
  GraphClientFactory,
  get_graph_client,
  get_graph_client_sync,
  get_graph_client_for_instance,
)

__all__ = [
  "GraphClient",
  "GraphClientConfig",
  "GraphAPIError",
  "GraphTransientError",
  "GraphClientError",
  "GraphServerError",
  "GraphTimeoutError",
  "GraphSyntaxError",
  "GraphClientFactory",
  "get_graph_client",
  "get_graph_client_sync",
  "get_graph_client_for_instance",
]
