"""Activate or deactivate a user account.

Deactivation invalidates sessions *and* revokes every API key, unlike a
password change, which leaves keys alone so routine rotation doesn't break
integrations.
"""

from dataclasses import dataclass

from sqlalchemy.orm import Session

from ...logger import get_logger
from ...models.core import User
from .user_deletion import UserNotFound

logger = get_logger(__name__)


@dataclass(frozen=True)
class UserStatusChange:
  """Outcome of an activate/deactivate, including what it revoked."""

  user_id: str
  email: str
  is_active: bool
  changed: bool
  api_keys_revoked: int
  api_keys_failed: int = 0
  # False when a revocation did not take; a cached credential may work until
  # its TTL. Re-run the deactivation.
  fully_applied: bool = True


def set_user_active(
  user_id: str,
  active: bool,
  session: Session,
  *,
  actor: str | None = None,
) -> UserStatusChange:
  """Set a user's active flag, revoking access when deactivating.

  Never short-circuits when already in the target state: revocation is
  best-effort per key, so a re-run must be able to finish a partial one.
  Reactivation does not restore revoked keys.
  """
  user = User.get_by_id(user_id, session)
  if not user:
    raise UserNotFound(user_id)

  was_active = bool(user.is_active)

  if active:
    found = 0
    revoked = 0
    fully_applied = True
    user.activate(session)
  else:
    result = user.deactivate(session)
    found = result.keys_found
    revoked = result.keys_revoked
    fully_applied = result.fully_applied

  # found == -1: the key list could not be loaded; report one failure.
  failed = (found - revoked) if found >= 0 else 1

  logger.info(
    f"User {user_id} {'activated' if active else 'deactivated'}",
    extra={
      "user_id": user_id,
      "actor": actor,
      "was_active": was_active,
      "api_keys_revoked": revoked,
      "api_keys_failed": failed,
      "fully_applied": fully_applied,
    },
  )

  return UserStatusChange(
    user_id=str(user.id),
    email=str(user.email),
    is_active=active,
    changed=was_active != active,
    api_keys_revoked=revoked,
    api_keys_failed=failed,
    fully_applied=fully_applied,
  )
