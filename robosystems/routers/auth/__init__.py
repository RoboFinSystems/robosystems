"""Authentication router module."""

from fastapi import APIRouter

from ...config import env
from .captcha import router as captcha_router
from .email_verification import router as email_verification_router
from .invitations import router as invitations_router
from .login import router as login_router
from .logout import router as logout_router
from .mfa import router as mfa_router
from .passkeys import router as passkeys_router
from .password import router as password_router
from .password_reset import router as password_reset_router
from .providers import router as providers_router

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

router.include_router(email_verification_router)

# Org invitation preview (public, token-gated)
router.include_router(invitations_router)

# Password management
router.include_router(password_router)
router.include_router(password_reset_router)

router.include_router(sso_router)

# Passkey MFA — mounted unconditionally with a runtime PASSKEYS_ENABLED guard
# (403 when off), unlike OIDC's import-time conditional: the posture-drift
# test table needs the routes to exist to prove they refuse.
router.include_router(passkeys_router)
router.include_router(mfa_router)

# Enterprise SSO (OIDC) — flag-gated so the surface doesn't exist unless the
# deployment opted in (the managed platform never mounts it).
if env.SSO_OIDC_ENABLED:
  from .oidc import router as oidc_router

  router.include_router(oidc_router)

# Configuration
router.include_router(captcha_router)
router.include_router(providers_router)
