"""Administrative user deletion.

Two support cases share one implementation: freeing an email that a self-registered
account is squatting so the org can invite it, and honoring an account-deletion
request (a standing SOC 2 obligation).

Deletion is deliberately narrow. It refuses while the user's organization still
holds anything of value — live graphs, subscriptions in force, repository access —
so it can never be the path by which paid infrastructure or a billing relationship
disappears. Financial and audit history outlives the account: billing rows are
retained with the actor de-referenced rather than deleted, and an organization
carrying any financial artifact is kept even once its last member is gone.
"""

from dataclasses import dataclass, field

from sqlalchemy.orm import Session

from robosystems.logger import get_logger
from robosystems.models.core import (
  BillingAuditLog,
  BillingCustomer,
  BillingInvoice,
  BillingSubscription,
  Connection,
  Document,
  Graph,
  GraphBackup,
  GraphCredits,
  GraphStatus,
  GraphUser,
  Org,
  OrgInvitation,
  OrgLimits,
  OrgRole,
  OrgUser,
  User,
  UserAPIKey,
  UserRepository,
  UserRepositoryCredits,
  UserToken,
)
from robosystems.models.core.billing.subscription import TERMINAL_SUBSCRIPTION_STATUSES
from robosystems.models.core.user.user_repository import RepositoryAccessLevel
from robosystems.models.core.user.user_repository_credits import (
  UserRepositoryCreditTransaction,
)

logger = get_logger(__name__)

LIVE_GRAPH_STATUSES = (GraphStatus.ACTIVE.value, GraphStatus.SUSPENDED.value)


class UserNotFound(Exception):
  """No user with the given identifier."""


class UserDeletionBlocked(Exception):
  """The user's account still holds resources that must be resolved first."""

  def __init__(self, blockers: list["DeletionBlocker"]):
    self.blockers = blockers
    super().__init__("; ".join(b.detail for b in blockers))


@dataclass(frozen=True)
class DeletionBlocker:
  """One reason the account cannot be deleted, with the resolution implied."""

  code: str
  detail: str


@dataclass
class UserDeletionPlan:
  """What deletion would do — computed without writing anything."""

  user_id: str
  email: str
  org_ids: list[str]
  blockers: list[DeletionBlocker] = field(default_factory=list)
  removals: dict[str, int] = field(default_factory=dict)
  orgs_to_delete: list[str] = field(default_factory=list)
  orgs_retained: list[str] = field(default_factory=list)

  @property
  def can_delete(self) -> bool:
    return not self.blockers


def _org_must_be_retained(org_id: str, session: Session) -> bool:
  """Whether an org carries records that must outlive its last member.

  Billing history is kept for compliance, and deprovisioned graphs stay as the
  record of what once ran — so an org holding either is retained as an empty
  shell rather than deleted. It is invisible to the product either way.
  """
  if session.query(BillingSubscription).filter_by(org_id=org_id).count():
    return True
  if session.query(BillingInvoice).filter_by(org_id=org_id).count():
    return True
  if session.query(BillingAuditLog).filter_by(org_id=org_id).count():
    return True
  return bool(session.query(Graph).filter_by(org_id=org_id).count())


def plan_user_deletion(user_id: str, session: Session) -> UserDeletionPlan:
  """Assess a deletion: what blocks it, and what it would remove.

  Read-only — safe to call for a dry run and called again by the executor so
  the guards are evaluated against the same state as the deletes.
  """
  user = User.get_by_id(user_id, session)
  if not user:
    raise UserNotFound(f"User {user_id} not found")

  memberships = OrgUser.get_user_orgs(user_id, session)
  org_ids = [m.org_id for m in memberships]

  blockers: list[DeletionBlocker] = []
  orgs_to_delete: list[str] = []
  orgs_retained: list[str] = []

  for membership in memberships:
    other_members = (
      session.query(OrgUser)
      .filter(OrgUser.org_id == membership.org_id, OrgUser.user_id != user_id)
      .count()
    )
    if membership.role == OrgRole.OWNER and other_members:
      blockers.append(
        DeletionBlocker(
          code="org_has_other_members",
          detail=(
            f"User owns organization {membership.org_id}, which has "
            f"{other_members} other member(s). Transfer ownership first."
          ),
        )
      )
    elif other_members == 0:
      if _org_must_be_retained(membership.org_id, session):
        orgs_retained.append(membership.org_id)
      else:
        orgs_to_delete.append(membership.org_id)
    else:
      orgs_retained.append(membership.org_id)

  if org_ids:
    live_graphs = (
      session.query(Graph)
      .filter(Graph.org_id.in_(org_ids), Graph.status.in_(LIVE_GRAPH_STATUSES))
      .count()
    )
    if live_graphs:
      blockers.append(
        DeletionBlocker(
          code="org_has_live_graphs",
          detail=(
            f"Organization still has {live_graphs} graph(s) that are not "
            "deprovisioned. Delete them first."
          ),
        )
      )

    live_subscriptions = (
      session.query(BillingSubscription)
      .filter(
        BillingSubscription.org_id.in_(org_ids),
        BillingSubscription.status.notin_(TERMINAL_SUBSCRIPTION_STATUSES),
      )
      .count()
    )
    if live_subscriptions:
      blockers.append(
        DeletionBlocker(
          code="org_has_active_subscriptions",
          detail=(
            f"Organization has {live_subscriptions} subscription(s) still in "
            "force. Cancel them first."
          ),
        )
      )

  active_repos = (
    session.query(UserRepository)
    .filter(
      UserRepository.user_id == user_id,
      UserRepository.is_active,
      UserRepository.access_level != RepositoryAccessLevel.NONE,
    )
    .count()
  )
  if active_repos:
    blockers.append(
      DeletionBlocker(
        code="active_repository_access",
        detail=(
          f"User still has active access to {active_repos} shared "
          "repository(ies). Cancel those subscriptions first."
        ),
      )
    )

  credit_pools = (
    session.query(GraphCredits)
    .filter(
      (GraphCredits.user_id == user_id) | (GraphCredits.billing_admin_id == user_id)
    )
    .count()
  )
  if credit_pools:
    blockers.append(
      DeletionBlocker(
        code="graph_credit_pools",
        detail=(
          f"User is still referenced by {credit_pools} graph credit pool(s). "
          "Those graphs must finish deprovisioning first."
        ),
      )
    )

  removals = {
    "api_keys": session.query(UserAPIKey).filter_by(user_id=user_id).count(),
    "tokens": session.query(UserToken).filter_by(user_id=user_id).count(),
    "graph_grants": session.query(GraphUser).filter_by(user_id=user_id).count(),
    "connections": session.query(Connection).filter_by(user_id=user_id).count(),
    "documents": session.query(Document).filter_by(user_id=user_id).count(),
    "repository_grants": session.query(UserRepository)
    .filter_by(user_id=user_id)
    .count(),
    "invitations_sent": session.query(OrgInvitation)
    .filter_by(invited_by=user_id)
    .count(),
    "org_memberships": len(memberships),
  }

  return UserDeletionPlan(
    user_id=user_id,
    email=user.email,
    org_ids=org_ids,
    blockers=blockers,
    removals=removals,
    orgs_to_delete=orgs_to_delete,
    orgs_retained=orgs_retained,
  )


def _invalidate_auth_caches(user_id: str, api_key_fingerprints: list[str]) -> None:
  """Evict everything that could still authenticate the deleted account.

  A JWT is refused once its user row is gone, and an API key once its row is
  gone — but only when the check reaches the database. Both paths are fronted
  by a cache with a TTL of minutes, so without this sweep a deleted account
  keeps authenticating from cache. Best-effort: a Redis failure is logged at
  CRITICAL rather than raised, because the deletion has already committed and
  the entries lapse on TTL.
  """
  import importlib

  try:
    cache_module = importlib.import_module("robosystems.middleware.auth.cache")
    api_key_cache = cache_module.api_key_cache
  except Exception as e:
    logger.error(
      f"CRITICAL: auth cache module unavailable after deleting user {user_id}; "
      f"cached credentials survive until TTL: {e}"
    )
    return

  cleared = True
  for fingerprint in api_key_fingerprints:
    cleared = bool(api_key_cache.invalidate_api_key(fingerprint)) and cleared
  cleared = bool(api_key_cache.invalidate_jwt_user_data(user_id)) and cleared
  cleared = bool(api_key_cache.invalidate_user_jwt_graph_access(user_id)) and cleared
  # Catches any key entry the fingerprints could not address.
  api_key_cache.invalidate_user_data(user_id)

  if not cleared:
    logger.error(
      f"CRITICAL: auth cache invalidation incomplete after deleting user "
      f"{user_id}; a cached credential may keep authenticating until TTL."
    )


def execute_user_deletion(
  user_id: str, session: Session, actor: str = "admin"
) -> UserDeletionPlan:
  """Delete a user account and everything scoped to it.

  Re-plans first and raises `UserDeletionBlocked` if anything still holds the
  account, so a stale dry run can never authorize a deletion.
  """
  plan = plan_user_deletion(user_id, session)
  if not plan.can_delete:
    raise UserDeletionBlocked(plan.blockers)

  user = User.get_by_id(user_id, session)
  if not user:
    raise UserNotFound(f"User {user_id} not found")

  # The bulk delete below bypasses UserAPIKey.delete(), which is where a key
  # normally clears its own cache entry — and the fingerprint that addresses
  # that entry dies with the row. Capture it now; the caches are purged after
  # the commit so a request racing the deletion cannot re-populate them from
  # a row that still exists.
  api_key_fingerprints = [
    row.key_fingerprint
    for row in session.query(UserAPIKey.key_fingerprint)
    .filter_by(user_id=user_id)
    .all()
    if row.key_fingerprint
  ]

  # Audit and billing history survive the account — the actor is de-referenced,
  # never deleted, because retention is itself a compliance requirement.
  session.query(BillingAuditLog).filter_by(actor_user_id=user_id).update(
    {"actor_user_id": None}, synchronize_session=False
  )
  session.query(BillingSubscription).filter_by(user_id=user_id).update(
    {"user_id": None}, synchronize_session=False
  )
  session.query(GraphBackup).filter_by(created_by_user_id=user_id).update(
    {"created_by_user_id": None}, synchronize_session=False
  )
  session.query(UserRepository).filter_by(granted_by=user_id).update(
    {"granted_by": None}, synchronize_session=False
  )
  session.query(OrgInvitation).filter_by(accepted_user_id=user_id).update(
    {"accepted_user_id": None}, synchronize_session=False
  )

  # Repository access, deepest child first.
  repo_ids = [
    row.id for row in session.query(UserRepository.id).filter_by(user_id=user_id).all()
  ]
  if repo_ids:
    pool_ids = [
      row.id
      for row in session.query(UserRepositoryCredits.id)
      .filter(UserRepositoryCredits.user_repository_id.in_(repo_ids))
      .all()
    ]
    if pool_ids:
      session.query(UserRepositoryCreditTransaction).filter(
        UserRepositoryCreditTransaction.credit_pool_id.in_(pool_ids)
      ).delete(synchronize_session=False)
      session.query(UserRepositoryCredits).filter(
        UserRepositoryCredits.id.in_(pool_ids)
      ).delete(synchronize_session=False)
    session.query(UserRepository).filter(UserRepository.id.in_(repo_ids)).delete(
      synchronize_session=False
    )

  session.query(OrgInvitation).filter_by(invited_by=user_id).delete(
    synchronize_session=False
  )
  session.query(Document).filter_by(user_id=user_id).delete(synchronize_session=False)
  session.query(Connection).filter_by(user_id=user_id).delete(synchronize_session=False)
  session.query(GraphUser).filter_by(user_id=user_id).delete(synchronize_session=False)
  session.query(UserAPIKey).filter_by(user_id=user_id).delete(synchronize_session=False)
  session.query(UserToken).filter_by(user_id=user_id).delete(synchronize_session=False)
  session.query(OrgUser).filter_by(user_id=user_id).delete(synchronize_session=False)

  for org_id in plan.orgs_to_delete:
    session.query(OrgInvitation).filter_by(org_id=org_id).delete(
      synchronize_session=False
    )
    session.query(BillingCustomer).filter_by(org_id=org_id).delete(
      synchronize_session=False
    )
    session.query(OrgLimits).filter_by(org_id=org_id).delete(synchronize_session=False)
    session.query(Org).filter_by(id=org_id).delete(synchronize_session=False)

  session.delete(user)

  try:
    session.commit()
  except Exception:
    session.rollback()
    raise

  _invalidate_auth_caches(user_id, api_key_fingerprints)

  logger.info(
    f"Deleted user {user_id} ({plan.email}) by {actor}",
    extra={
      "user_id": user_id,
      "actor": actor,
      "orgs_deleted": plan.orgs_to_delete,
      "orgs_retained": plan.orgs_retained,
      "removals": plan.removals,
    },
  )

  return plan
