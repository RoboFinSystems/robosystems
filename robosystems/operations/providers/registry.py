"""Registry mapping a connection provider name to its create/sync/cleanup trio.

`ConnectionService` and the connection routers dispatch through here rather than
importing providers directly, so a feature-flagged-off provider is simply absent.
"""

import logging
from typing import Any, Protocol

from sqlalchemy.orm import Session

from ...config import env
from ...middleware.otel.metrics import get_endpoint_metrics
from ...models.api.graphs.connections import (
  ExternalConnectionConfig,
  QuickBooksConnectionConfig,
)
from .external_provider import (
  cleanup_external_connection,
  create_external_connection,
  sync_external_connection,
)
from .quickbooks_provider import (
  cleanup_quickbooks_connection,
  create_quickbooks_connection,
  sync_quickbooks_connection,
)
from .types import SyncOutcome

logger = logging.getLogger(__name__)


class ConnectionProvider(Protocol):
  """The three lifecycle hooks every connection provider implements."""

  async def create_connection(
    self, entity_id: str | None, config: Any, user_id: str, graph_id: str, db: Session
  ) -> str:
    """Register the connection and return its id."""
    ...

  async def sync_connection(
    self,
    connection: dict[str, Any],
    sync_options: dict[str, Any] | None,
    graph_id: str,
  ) -> SyncOutcome:
    """Pull from the source; the outcome says whether a run was started."""
    ...

  async def cleanup_connection(self, connection: dict[str, Any], graph_id: str) -> None:
    """Release provider-side state before the connection row is deleted."""
    ...


class ProviderRegistry:
  """Providers enabled by feature flag, keyed by lowercase provider name."""

  def __init__(self):
    self._providers = {}

    self._record_feature_flag_status()

    if env.CONNECTION_QUICKBOOKS_ENABLED:
      self._providers["quickbooks"] = {
        "create": create_quickbooks_connection,
        "sync": sync_quickbooks_connection,
        "cleanup": cleanup_quickbooks_connection,
        "config_class": QuickBooksConnectionConfig,
      }

    # "external" is source-namespace registration only — see external_provider.py
    if env.CONNECTION_EXTERNAL_ENABLED:
      self._providers["external"] = {
        "create": create_external_connection,
        "sync": sync_external_connection,
        "cleanup": cleanup_external_connection,
        "config_class": ExternalConnectionConfig,
      }

  def _record_feature_flag_status(self):
    """Emit the flag state once at construction, so dashboards can tell a
    disabled provider from a broken one."""
    try:
      metrics = get_endpoint_metrics()
      for provider, enabled in [
        ("quickbooks", env.CONNECTION_QUICKBOOKS_ENABLED),
        ("external", env.CONNECTION_EXTERNAL_ENABLED),
      ]:
        metrics.record_business_event(
          endpoint="provider_registry",
          method="INIT",
          event_type="feature_flag_status",
          event_data={
            "provider": provider,
            "enabled": str(enabled).lower(),
          },
        )
        logger.debug(
          f"Provider {provider} feature flag: {'enabled' if enabled else 'disabled'}"
        )
    except Exception as e:
      # Metrics must never keep the registry from constructing.
      logger.warning(f"Failed to record feature flag metrics: {e}")

  def is_enabled(self, provider_type: str) -> bool:
    """Whether a provider is registered (feature-flag enabled)."""
    return provider_type.lower() in self._providers

  def get_provider(self, provider_type: str) -> dict[str, Any]:
    """Look up a provider's handler dict.

    Raises ValueError either way, but distinguishes "known provider, turned
    off" from "no such provider" so the caller-facing message is actionable.
    """
    provider_lower = provider_type.lower()

    self._record_provider_request(provider_lower)

    provider = self._providers.get(provider_lower)
    if not provider:
      if provider_lower == "quickbooks" and not env.CONNECTION_QUICKBOOKS_ENABLED:
        self._record_disabled_provider_request(provider_lower)
        raise ValueError(
          "QuickBooks provider is not enabled. Please contact support to enable this connection type."
        )
      elif provider_lower == "external" and not env.CONNECTION_EXTERNAL_ENABLED:
        self._record_disabled_provider_request(provider_lower)
        raise ValueError(
          "External provider is not enabled. Please contact support to enable this connection type."
        )
      else:
        raise ValueError(f"Unknown provider type: {provider_type}")
    return provider

  def _record_provider_request(self, provider: str):
    try:
      metrics = get_endpoint_metrics()
      metrics.record_business_event(
        endpoint="provider_registry",
        method="GET",
        event_type="provider_requested",
        event_data={
          "provider": provider,
        },
      )
    except Exception as e:
      logger.debug(f"Failed to record provider request metric: {e}")

  def _record_disabled_provider_request(self, provider: str):
    try:
      metrics = get_endpoint_metrics()
      metrics.record_business_event(
        endpoint="provider_registry",
        method="GET",
        event_type="disabled_provider_requested",
        event_data={
          "provider": provider,
        },
      )
      logger.warning(f"Request for disabled provider: {provider}")
    except Exception as e:
      logger.debug(f"Failed to record disabled provider metric: {e}")

  async def create_connection(
    self,
    provider_type: str,
    entity_id: str,
    config: Any,
    user_id: str,
    graph_id: str,
    db: Session,
  ) -> str:
    provider = self.get_provider(provider_type)
    create_func = provider["create"]
    return await create_func(entity_id, config, user_id, graph_id, db)

  async def sync_connection(
    self,
    provider_type: str,
    connection: dict[str, Any],
    sync_options: dict[str, Any] | None,
    graph_id: str,
  ) -> SyncOutcome:
    provider = self.get_provider(provider_type)
    sync_func = provider["sync"]
    return await sync_func(connection, sync_options, graph_id)

  async def cleanup_connection(
    self, provider_type: str, connection: dict[str, Any], graph_id: str
  ) -> None:
    provider = self.get_provider(provider_type)
    cleanup_func = provider["cleanup"]
    return await cleanup_func(connection, graph_id)


# Built at import: the enabled set is a snapshot of the feature flags, so
# flipping one takes effect only after a process restart.
provider_registry = ProviderRegistry()
