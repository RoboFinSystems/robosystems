"""
Graph API Client Configuration.

Centralized configuration for Graph API clients with multi-backend support.
"""

import os
from typing import Dict, Any
from dataclasses import dataclass, field


@dataclass
class GraphClientConfig:
  """Configuration for Graph API clients."""

  # Connection settings
  base_url: str = ""
  timeout: int = 30
  max_retries: int = 3
  retry_delay: float = 1.0
  retry_backoff: float = 2.0

  # Connection pool settings
  max_connections: int = 100
  max_keepalive_connections: int = 20
  keepalive_expiry: float = 5.0

  # Circuit breaker settings
  circuit_breaker_threshold: int = 5
  circuit_breaker_timeout: int = 60

  # Request settings
  headers: Dict[str, str] = field(default_factory=dict)
  verify_ssl: bool = True

  @classmethod
  def from_env(cls, prefix: str = "GRAPH_CLIENT_") -> "GraphClientConfig":
    """
    Create configuration from environment variables.

    Args:
        prefix: Environment variable prefix

    Returns:
        GraphClientConfig instance
    """
    config = cls()

    # Map of config attribute to env var suffix
    env_mappings = {
      "base_url": "BASE_URL",
      "timeout": "TIMEOUT",
      "max_retries": "MAX_RETRIES",
      "retry_delay": "RETRY_DELAY",
      "retry_backoff": "RETRY_BACKOFF",
      "max_connections": "MAX_CONNECTIONS",
      "max_keepalive_connections": "MAX_KEEPALIVE_CONNECTIONS",
      "keepalive_expiry": "KEEPALIVE_EXPIRY",
      "circuit_breaker_threshold": "CIRCUIT_BREAKER_THRESHOLD",
      "circuit_breaker_timeout": "CIRCUIT_BREAKER_TIMEOUT",
      "verify_ssl": "VERIFY_SSL",
    }

    for attr, env_suffix in env_mappings.items():
      env_var = prefix + env_suffix
      value = os.environ.get(env_var)

      if value is not None:
        # Convert to appropriate type
        attr_type = type(getattr(config, attr))
        if attr_type is bool:
          setattr(config, attr, value.lower() in ("true", "1", "yes"))
        elif attr_type in (int, float):
          setattr(config, attr, attr_type(value))
        else:
          setattr(config, attr, value)

    return config

  def with_overrides(self, **kwargs: Any) -> "GraphClientConfig":
    """
    Create a new config with overridden values.

    Args:
        **kwargs: Values to override

    Returns:
        New GraphClientConfig instance
    """
    config_dict: Dict[str, Any] = {
      "base_url": self.base_url,
      "timeout": self.timeout,
      "max_retries": self.max_retries,
      "retry_delay": self.retry_delay,
      "retry_backoff": self.retry_backoff,
      "max_connections": self.max_connections,
      "max_keepalive_connections": self.max_keepalive_connections,
      "keepalive_expiry": self.keepalive_expiry,
      "circuit_breaker_threshold": self.circuit_breaker_threshold,
      "circuit_breaker_timeout": self.circuit_breaker_timeout,
      "headers": self.headers.copy(),
      "verify_ssl": self.verify_ssl,
    }
    config_dict.update(kwargs)
    return GraphClientConfig(**config_dict)
