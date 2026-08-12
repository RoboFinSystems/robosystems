"""Admin API routers."""

from .cache import router as cache_router
from .credits import router as credits_router
from .graphs import router as graphs_router
from .invoice import router as invoice_router
from .orgs import router as orgs_router
from .scim import router as scim_router
from .subscription import router as subscription_router
from .users import router as users_router
from .webhooks import router as webhooks_router

__all__ = [
  "cache_router",
  "credits_router",
  "graphs_router",
  "invoice_router",
  "orgs_router",
  "scim_router",
  "subscription_router",
  "users_router",
  "webhooks_router",
]
