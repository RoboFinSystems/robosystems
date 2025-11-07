"""Authentication module initialization."""

from .dependencies import get_current_user, get_optional_user
from .utils import validate_api_key
from .admin import AdminAuthMiddleware, admin_auth, require_admin
from ..rate_limits import (
  subscription_aware_rate_limit_dependency,
  graph_scoped_rate_limit_dependency,
)

__all__ = [
  "get_current_user",
  "get_optional_user",
  "validate_api_key",
  "AdminAuthMiddleware",
  "admin_auth",
  "require_admin",
  "subscription_aware_rate_limit_dependency",
  "graph_scoped_rate_limit_dependency",
]
