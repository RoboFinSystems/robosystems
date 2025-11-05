"""Provider registry for dynamic provider management."""

import logging
from typing import Dict, Any, Optional, Protocol
from sqlalchemy.orm import Session

from .sec_provider import (
  create_sec_connection,
  sync_sec_connection,
  cleanup_sec_connection,
)
from .plaid_provider import PlaidProvider
from .quickbooks_provider import (
  create_quickbooks_connection,
  sync_quickbooks_connection,
  cleanup_quickbooks_connection,
)
from ...models.api.graphs.connections import (
  SECConnectionConfig,
  PlaidConnectionConfig,
  QuickBooksConnectionConfig,
)
from ...config import env
from ...middleware.otel.metrics import get_endpoint_metrics

logger = logging.getLogger(__name__)


class ConnectionProvider(Protocol):
  """Protocol for connection providers."""

  async def create_connection(
    self, entity_id: str, config: Any, user_id: str, graph_id: str, db: Session
  ) -> str:
    """Create a connection."""
    ...

  async def sync_connection(
    self,
    connection: Dict[str, Any],
    sync_options: Optional[Dict[str, Any]],
    graph_id: str,
  ) -> str:
    """Sync a connection."""
    ...

  async def cleanup_connection(self, connection: Dict[str, Any], graph_id: str) -> None:
    """Clean up a connection."""
    ...


class ProviderRegistry:
  """Registry for connection providers."""

  def __init__(self):
    # Initialize Plaid provider instance only if enabled
    self._plaid_provider = PlaidProvider() if env.CONNECTION_PLAID_ENABLED else None

    # Map provider types to their handlers - only include enabled providers
    self._providers = {}

    # Track feature flag status for metrics
    self._record_feature_flag_status()

    # Add SEC provider if enabled
    if env.CONNECTION_SEC_ENABLED:
      self._providers["sec"] = {
        "create": create_sec_connection,
        "sync": sync_sec_connection,
        "cleanup": cleanup_sec_connection,
        "config_class": SECConnectionConfig,
      }

    # Add QuickBooks provider if enabled
    if env.CONNECTION_QUICKBOOKS_ENABLED:
      self._providers["quickbooks"] = {
        "create": create_quickbooks_connection,
        "sync": sync_quickbooks_connection,
        "cleanup": cleanup_quickbooks_connection,
        "config_class": QuickBooksConnectionConfig,
      }

    # Add Plaid provider if enabled
    if env.CONNECTION_PLAID_ENABLED and self._plaid_provider:
      self._providers["plaid"] = {
        "create": self._plaid_provider.create_connection,
        "sync": self._plaid_provider.sync_connection,
        "cleanup": self._plaid_provider.cleanup_connection,
        "config_class": PlaidConnectionConfig,
        "instance": self._plaid_provider,  # For special methods like link token
      }

  def _record_feature_flag_status(self):
    """Record feature flag status at initialization for monitoring."""
    try:
      metrics = get_endpoint_metrics()
      # Record feature flag status for each provider
      for provider, enabled in [
        ("sec", env.CONNECTION_SEC_ENABLED),
        ("quickbooks", env.CONNECTION_QUICKBOOKS_ENABLED),
        ("plaid", env.CONNECTION_PLAID_ENABLED),
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
      # Don't fail initialization if metrics fail
      logger.warning(f"Failed to record feature flag metrics: {e}")

  def get_provider(self, provider_type: str) -> Dict[str, Any]:
    """Get provider configuration."""
    provider_lower = provider_type.lower()

    # Record provider request metric
    self._record_provider_request(provider_lower)

    provider = self._providers.get(provider_lower)
    if not provider:
      # Check if provider exists but is disabled
      if provider_lower == "sec" and not env.CONNECTION_SEC_ENABLED:
        self._record_disabled_provider_request(provider_lower)
        raise ValueError(
          "SEC provider is not enabled. Please contact support to enable this connection type."
        )
      elif provider_lower == "quickbooks" and not env.CONNECTION_QUICKBOOKS_ENABLED:
        self._record_disabled_provider_request(provider_lower)
        raise ValueError(
          "QuickBooks provider is not enabled. Please contact support to enable this connection type."
        )
      elif provider_lower == "plaid" and not env.CONNECTION_PLAID_ENABLED:
        self._record_disabled_provider_request(provider_lower)
        raise ValueError(
          "Plaid provider is not enabled. Please contact support to enable this connection type."
        )
      else:
        raise ValueError(f"Unknown provider type: {provider_type}")
    return provider

  def _record_provider_request(self, provider: str):
    """Record metrics for provider requests."""
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
    """Record metrics for disabled provider requests."""
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

  def get_plaid_provider(self) -> PlaidProvider:
    """Get Plaid provider instance for special operations."""
    # Record provider request metric
    self._record_provider_request("plaid")

    if not self._plaid_provider:
      self._record_disabled_provider_request("plaid")
      raise ValueError(
        "Plaid provider is not enabled. Please contact support to enable this connection type."
      )
    return self._plaid_provider

  async def create_connection(
    self,
    provider_type: str,
    entity_id: str,
    config: Any,
    user_id: str,
    graph_id: str,
    db: Session,
  ) -> str:
    """Create a connection using the appropriate provider."""
    provider = self.get_provider(provider_type)
    create_func = provider["create"]
    return await create_func(entity_id, config, user_id, graph_id, db)

  async def sync_connection(
    self,
    provider_type: str,
    connection: Dict[str, Any],
    sync_options: Optional[Dict[str, Any]],
    graph_id: str,
  ) -> str:
    """Sync a connection using the appropriate provider."""
    provider = self.get_provider(provider_type)
    sync_func = provider["sync"]
    return await sync_func(connection, sync_options, graph_id)

  async def cleanup_connection(
    self, provider_type: str, connection: Dict[str, Any], graph_id: str
  ) -> None:
    """Clean up a connection using the appropriate provider."""
    provider = self.get_provider(provider_type)
    cleanup_func = provider["cleanup"]
    return await cleanup_func(connection, graph_id)


# Global registry instance
provider_registry = ProviderRegistry()
