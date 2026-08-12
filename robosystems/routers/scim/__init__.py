"""SCIM 2.0 provisioning surface, mounted at /scim/v2.

Every endpoint sits behind the per-org SCIM bearer dependency (which also
publishes ``request.state.scim_org_id``) and the SCIM rate bucket, so both the
conformance probes and the Users CRUD are authenticated uniformly. A user JWT
or API key is never accepted here.
"""

from fastapi import APIRouter, Depends

from ...middleware.auth.scim import require_scim_org
from ...middleware.rate_limits import scim_rate_limit_dependency
from .config import router as config_router
from .users import router as users_router

router = APIRouter(
  prefix="/scim/v2",
  tags=["SCIM"],
  dependencies=[Depends(scim_rate_limit_dependency), Depends(require_scim_org)],
)
router.include_router(config_router)
router.include_router(users_router)

__all__ = ["router"]
