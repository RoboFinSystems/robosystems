"""User endpoints under `/v1/user`: profile, password, and API keys."""

from fastapi import APIRouter

from .api_keys import router as api_keys_router
from .main import router as main_router
from .oauth_grants import router as oauth_grants_router
from .password import router as password_router

router = APIRouter()

router.include_router(main_router)
router.include_router(password_router)
router.include_router(api_keys_router)
router.include_router(oauth_grants_router)

__all__ = ["router"]
