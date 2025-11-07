"""
User management and profile endpoints.

Provides user profile management and usage limits functionality.
"""

from fastapi import APIRouter

from .user import router as user_router
from .limits import router as limits_router

# Create composite router
router = APIRouter()

# Include sub-routers
router.include_router(user_router)
router.include_router(limits_router, prefix="/user/limits")

__all__ = ["router"]
