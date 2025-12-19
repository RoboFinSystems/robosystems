"""
Graph API Client - Async client for graph database access.

This module provides an asynchronous client for interacting with graph database
backends via the Graph API with multi-backend support.
"""

from .client import GraphClient
from .config import GraphClientConfig
from .exceptions import (
  GraphAPIError,
  GraphClientError,
  GraphServerError,
  GraphSyntaxError,
  GraphTimeoutError,
  GraphTransientError,
)
from .factory import (
  GraphClientFactory,
  get_graph_client,
  get_graph_client_for_instance,
  get_graph_client_sync,
)

__all__ = [
  "GraphAPIError",
  "GraphClient",
  "GraphClientConfig",
  "GraphClientError",
  "GraphClientFactory",
  "GraphServerError",
  "GraphSyntaxError",
  "GraphTimeoutError",
  "GraphTransientError",
  "get_graph_client",
  "get_graph_client_for_instance",
  "get_graph_client_sync",
]
