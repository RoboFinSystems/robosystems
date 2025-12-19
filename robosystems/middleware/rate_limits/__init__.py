"""
Rate limiting middleware for RoboSystems API.

This module provides comprehensive rate limiting functionality including:
- Burst protection based on user subscription tiers
- Repository-specific volume limits for shared repositories
- Dual-layer rate limiting system
- Rate limit headers for API responses
"""

from ...config.rate_limits import EndpointCategory
from .cache import rate_limit_cache
from .headers import RateLimitHeaderMiddleware
from .rate_limiting import (
  analytics_rate_limit_dependency,
  auth_rate_limit_dependency,
  auth_status_rate_limit_dependency,
  backup_operations_rate_limit_dependency,
  connection_management_rate_limit_dependency,
  create_custom_rate_limit_dependency,
  general_api_rate_limit_dependency,
  graph_scoped_rate_limit_dependency,
  logout_rate_limit_dependency,
  public_api_rate_limit_dependency,
  rate_limit_dependency,
  sensitive_auth_rate_limit_dependency,
  sse_connection_rate_limit_dependency,
  sso_rate_limit_dependency,
  subscription_aware_rate_limit_dependency,
  sync_operations_rate_limit_dependency,
  tasks_management_rate_limit_dependency,
  user_management_rate_limit_dependency,
)
from .repository_rate_limits import (
  BLOCKED_SHARED_ENDPOINTS,
  AllowedSharedEndpoints,
  DualLayerRateLimiter,
  SharedRepositoryRateLimits,
)
from .subscription_rate_limits import (
  SUBSCRIPTION_RATE_LIMITS,
  get_endpoint_category,
  get_subscription_rate_limit,
  should_use_subscription_limits,
)

__all__ = [
  "BLOCKED_SHARED_ENDPOINTS",
  "SUBSCRIPTION_RATE_LIMITS",
  "AllowedSharedEndpoints",
  # Repository rate limits
  "DualLayerRateLimiter",
  "EndpointCategory",
  # Headers middleware
  "RateLimitHeaderMiddleware",
  "SharedRepositoryRateLimits",
  "analytics_rate_limit_dependency",
  "auth_rate_limit_dependency",
  "auth_status_rate_limit_dependency",
  "backup_operations_rate_limit_dependency",
  "connection_management_rate_limit_dependency",
  "create_custom_rate_limit_dependency",
  "general_api_rate_limit_dependency",
  "get_endpoint_category",
  # Subscription rate limits
  "get_subscription_rate_limit",
  "graph_scoped_rate_limit_dependency",
  "logout_rate_limit_dependency",
  "public_api_rate_limit_dependency",
  # Cache
  "rate_limit_cache",
  # Main rate limiting
  "rate_limit_dependency",
  "sensitive_auth_rate_limit_dependency",
  "should_use_subscription_limits",
  "sse_connection_rate_limit_dependency",
  "sso_rate_limit_dependency",
  "subscription_aware_rate_limit_dependency",
  "sync_operations_rate_limit_dependency",
  "tasks_management_rate_limit_dependency",
  "user_management_rate_limit_dependency",
]
