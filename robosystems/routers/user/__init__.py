"""
User management and profile endpoints.

Provides user profile management, usage limits, and analytics functionality.
"""

from fastapi import APIRouter

from .user import router as user_router
from .limits import router as limits_router
from .analytics import router as analytics_router
from .subscription import router as subscription_router

# Create composite router
router = APIRouter()

# Include sub-routers
router.include_router(user_router)
router.include_router(limits_router, prefix="/user/limits")
router.include_router(analytics_router, prefix="/user/analytics")
router.include_router(subscription_router, prefix="/user/subscriptions")

__all__ = ["router"]
