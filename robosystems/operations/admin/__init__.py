"""Administrative operations — support-plane actions with no self-serve surface."""

from .scim_bootstrap import (
  OrgBoundaryError,
  OrgNotFoundError,
  ScimBootstrapResult,
  bootstrap_scim,
  revoke_scim_token,
)
from .user_deletion import (
  DeletionBlocker,
  UserDeletionBlocked,
  UserDeletionPlan,
  UserNotFound,
  execute_user_deletion,
  plan_user_deletion,
)
from .user_status import UserStatusChange, set_user_active

__all__ = [
  "DeletionBlocker",
  "OrgBoundaryError",
  "OrgNotFoundError",
  "ScimBootstrapResult",
  "UserDeletionBlocked",
  "UserDeletionPlan",
  "UserNotFound",
  "UserStatusChange",
  "bootstrap_scim",
  "execute_user_deletion",
  "plan_user_deletion",
  "revoke_scim_token",
  "set_user_active",
]
