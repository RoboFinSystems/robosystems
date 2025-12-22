"""
User management and profile endpoints.

Provides user profile management and security functionality.
"""

from fastapi import APIRouter

from .api_keys import router as api_keys_router
from .main import router as main_router
from .password import router as password_router

router = APIRouter()

router.include_router(main_router)
router.include_router(password_router)
router.include_router(api_keys_router)

__all__ = ["router"]
