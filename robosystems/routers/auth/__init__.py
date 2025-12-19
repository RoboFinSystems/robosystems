"""Authentication router module."""

from fastapi import APIRouter

from .captcha import router as captcha_router
from .email_verification import router as email_verification_router
from .login import router as login_router
from .logout import router as logout_router
from .password import router as password_router
from .password_reset import router as password_reset_router

# Import sub-routers
from .register import router as register_router
from .session import router as session_router
from .sso import router as sso_router

# Create main auth router
router = APIRouter()

# Include all sub-routers in logical order

# Core authentication
router.include_router(register_router)
router.include_router(login_router)
router.include_router(logout_router)
router.include_router(session_router)

# Email verification
router.include_router(email_verification_router)

# Password management
router.include_router(password_router)
router.include_router(password_reset_router)

# SSO
router.include_router(sso_router)

# Security
router.include_router(captcha_router)
