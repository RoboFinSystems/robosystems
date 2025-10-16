"""
Kuzu API Client - Unified sync/async client for Kuzu API access.

This module provides both synchronous and asynchronous clients for interacting
with the Kuzu API cluster endpoints.
"""

from .client import KuzuClient
from .sync_client import KuzuSyncClient
from .exceptions import (
  KuzuAPIError,
  KuzuTransientError,
  KuzuClientError,
  KuzuServerError,
  KuzuTimeoutError,
  KuzuSyntaxError,
)
from .config import KuzuClientConfig
from .factory import (
  KuzuClientFactory,
  get_kuzu_client,
  get_kuzu_client_sync,
  get_kuzu_client_for_instance,
)

__all__ = [
  "KuzuClient",
  "KuzuSyncClient",
  "KuzuClientConfig",
  "KuzuAPIError",
  "KuzuTransientError",
  "KuzuClientError",
  "KuzuServerError",
  "KuzuTimeoutError",
  "KuzuSyntaxError",
  "KuzuClientFactory",
  "get_kuzu_client",
  "get_kuzu_client_sync",
  "get_kuzu_client_for_instance",
]
