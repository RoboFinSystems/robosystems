"""
Rate limiting middleware for RoboSystems API.

This module provides comprehensive rate limiting functionality including:
- Burst protection based on user subscription tiers
- Repository-specific volume limits for shared repositories
- Dual-layer rate limiting system
- Rate limit headers for API responses
"""

from .rate_limiting import (
  rate_limit_dependency,
  auth_rate_limit_dependency,
  create_custom_rate_limit_dependency,
  user_management_rate_limit_dependency,
  sync_operations_rate_limit_dependency,
  connection_management_rate_limit_dependency,
  analytics_rate_limit_dependency,
  backup_operations_rate_limit_dependency,
  sensitive_auth_rate_limit_dependency,
  logout_rate_limit_dependency,
  tasks_management_rate_limit_dependency,
  auth_status_rate_limit_dependency,
  sso_rate_limit_dependency,
  general_api_rate_limit_dependency,
  public_api_rate_limit_dependency,
  subscription_aware_rate_limit_dependency,
  graph_scoped_rate_limit_dependency,
  sse_connection_rate_limit_dependency,
)
from .repository_rate_limits import (
  DualLayerRateLimiter,
  SharedRepositoryRateLimits,
  AllowedSharedEndpoints,
  BLOCKED_SHARED_ENDPOINTS,
)
from .subscription_rate_limits import (
  get_subscription_rate_limit,
  get_endpoint_category,
  should_use_subscription_limits,
  SUBSCRIPTION_RATE_LIMITS,
)
from ...config.rate_limits import EndpointCategory
from .headers import RateLimitHeaderMiddleware
from .cache import rate_limit_cache

__all__ = [
  # Main rate limiting
  "rate_limit_dependency",
  "auth_rate_limit_dependency",
  "create_custom_rate_limit_dependency",
  "user_management_rate_limit_dependency",
  "sync_operations_rate_limit_dependency",
  "connection_management_rate_limit_dependency",
  "analytics_rate_limit_dependency",
  "backup_operations_rate_limit_dependency",
  "sensitive_auth_rate_limit_dependency",
  "logout_rate_limit_dependency",
  "tasks_management_rate_limit_dependency",
  "auth_status_rate_limit_dependency",
  "sso_rate_limit_dependency",
  "general_api_rate_limit_dependency",
  "public_api_rate_limit_dependency",
  "subscription_aware_rate_limit_dependency",
  "graph_scoped_rate_limit_dependency",
  "sse_connection_rate_limit_dependency",
  # Repository rate limits
  "DualLayerRateLimiter",
  "SharedRepositoryRateLimits",
  "AllowedSharedEndpoints",
  "BLOCKED_SHARED_ENDPOINTS",
  # Subscription rate limits
  "get_subscription_rate_limit",
  "get_endpoint_category",
  "should_use_subscription_limits",
  "SUBSCRIPTION_RATE_LIMITS",
  "EndpointCategory",
  # Headers middleware
  "RateLimitHeaderMiddleware",
  # Cache
  "rate_limit_cache",
]
