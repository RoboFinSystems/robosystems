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
from .register import router as register_router
from .session import router as session_router
from .sso import router as sso_router

router = APIRouter()


# Core authentication
router.include_router(register_router, tags=["Auth"])
router.include_router(login_router, tags=["Auth"])
router.include_router(logout_router, tags=["Auth"])
router.include_router(session_router, tags=["Auth"])

router.include_router(email_verification_router, tags=["Auth"])

# Org invitation preview (public, token-gated)
router.include_router(invitations_router, tags=["Auth"])

# Password management
router.include_router(password_router, tags=["Auth"])
router.include_router(password_reset_router, tags=["Auth"])

router.include_router(sso_router, tags=["Auth: SSO"])

# Passkey MFA mounts unconditionally with a runtime PASSKEYS_ENABLED guard,
# so the posture-drift tests can prove the routes refuse.
router.include_router(passkeys_router, tags=["Auth: Passkeys"])
router.include_router(mfa_router, tags=["Auth: MFA"])

# Enterprise SSO (OIDC) only exists when the deployment opts in.
if env.SSO_OIDC_ENABLED:
  from .oidc import router as oidc_router

  router.include_router(oidc_router, tags=["Auth: SSO"])

# Configuration
router.include_router(captcha_router, tags=["Auth"])
router.include_router(providers_router, tags=["Auth"])
