"""Activate or deactivate a user account.

Deactivation is the support-plane response to a compromised or suspended
account, and the only one short of deletion. It is deliberately heavier than a
credential rotation: a password change invalidates sessions but leaves API keys
alone, because rotation is routine and revoking keys on it would break
integrations. Deactivation is the escalation — it invalidates sessions *and*
revokes every key.
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


def set_user_active(
  user_id: str,
  active: bool,
  session: Session,
  *,
  actor: str | None = None,
) -> UserStatusChange:
  """Set a user's active flag, revoking access when deactivating.

  The underlying model call is always performed rather than skipped when the
  user is already in the target state. Key revocation is best-effort per key, so
  a re-run after a partial failure must be able to finish the job — a no-op
  short-circuit would leave keys live on exactly the retry an operator reaches
  for during an incident.

  Reactivation does **not** restore revoked keys: `UserAPIKey.deactivate` flips
  each key's own flag, and nothing flips it back. The user must issue new ones.

  ``api_keys_revoked`` counts keys that were actually revoked, and
  ``api_keys_failed`` the ones that were not. It used to report the number
  found before the attempt, so a partial revocation — the case an operator
  most needs to see, mid-incident — was indistinguishable from a clean one.
  """
  user = User.get_by_id(user_id, session)
  if not user:
    raise UserNotFound(user_id)

  was_active = bool(user.is_active)

  if active:
    found = 0
    revoked = 0
    user.activate(session)
  else:
    result = user.deactivate(session)
    found = result.keys_found
    revoked = result.keys_revoked

  # found == -1 means the key list could not be loaded at all; report one
  # failure rather than a negative count so the operator still sees red.
  failed = (found - revoked) if found >= 0 else 1

  logger.info(
    f"User {user_id} {'activated' if active else 'deactivated'}",
    extra={
      "user_id": user_id,
      "actor": actor,
      "was_active": was_active,
      "api_keys_revoked": revoked,
      "api_keys_failed": failed,
    },
  )

  return UserStatusChange(
    user_id=str(user.id),
    email=str(user.email),
    is_active=active,
    changed=was_active != active,
    api_keys_revoked=revoked,
    api_keys_failed=failed,
  )
