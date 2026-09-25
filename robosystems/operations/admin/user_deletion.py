"""Administrative user deletion.

Refuses while the user's org still holds anything of value (live graphs,
subscriptions in force, repository access), so it is never how paid
infrastructure disappears. Billing and audit history outlive the account: rows
are kept with the actor de-referenced, and an org with any financial artifact
is retained even once empty.
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
  OAuthGrant,
  OAuthToken,
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
from robosystems.models.core.user.oauth_token import TOKEN_TYPE_ACCESS
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

  Billing history (compliance) and deprovisioned graphs (the record of what
  ran) keep the org as an empty shell.
  """
  if session.query(BillingSubscription).filter_by(org_id=org_id).count():
    return True
  if session.query(BillingInvoice).filter_by(org_id=org_id).count():
    return True
  if session.query(BillingAuditLog).filter_by(org_id=org_id).count():
    return True
  return bool(session.query(Graph).filter_by(org_id=org_id).count())


def _pool_successors(user_id: str, session: Session) -> dict[str, str | None]:
  """``GraphCredits.id -> successor`` for every pool that names the user.

  A pool on a graph whose org has another owner passes to that owner: the org
  keeps the graph, and member removal reassigns nothing. None marks a pool
  with no one to take it (the user's own org, or no org), which blocks.
  """
  pools = (
    session.query(GraphCredits.id, Graph.org_id)
    .outerjoin(Graph, Graph.graph_id == GraphCredits.graph_id)
    .filter(
      (GraphCredits.user_id == user_id) | (GraphCredits.billing_admin_id == user_id)
    )
    .all()
  )
  successors: dict[str, str | None] = {}
  for pool_id, org_id in pools:
    owner = None
    if org_id is not None:
      owner = (
        session.query(OrgUser.user_id)
        .filter(
          OrgUser.org_id == org_id,
          OrgUser.role == OrgRole.OWNER,
          OrgUser.user_id != user_id,
        )
        .order_by(OrgUser.user_id)
        .limit(1)
        .scalar()
      )
    successors[pool_id] = owner
  return successors


def plan_user_deletion(user_id: str, session: Session) -> UserDeletionPlan:
  """Assess a deletion: what blocks it, and what it would remove.

  Read-only; the executor re-plans so guards see the same state as the deletes.
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

  credit_pools = sum(
    1 for successor in _pool_successors(user_id, session).values() if successor is None
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
    "oauth_grants": session.query(OAuthGrant).filter_by(user_id=user_id).count(),
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

  JWT and API-key checks are cached for minutes, so a deleted account would
  keep authenticating from cache. Best-effort: the deletion has committed, so
  failures log CRITICAL and the entries lapse on TTL.
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

  Re-plans first and raises `UserDeletionBlocked`, so a stale dry run can
  never authorize a deletion.
  """
  plan = plan_user_deletion(user_id, session)
  if not plan.can_delete:
    raise UserDeletionBlocked(plan.blockers)

  user = User.get_by_id(user_id, session)
  if not user:
    raise UserNotFound(f"User {user_id} not found")

  # The bulk delete bypasses UserAPIKey.delete()'s cache eviction, and the
  # fingerprint dies with the row: capture now, purge after commit (so a racing
  # request cannot re-populate from a row that still exists).
  api_key_fingerprints = [
    row.key_fingerprint
    for row in session.query(UserAPIKey.key_fingerprint)
    .filter_by(user_id=user_id)
    .all()
    if row.key_fingerprint
  ]
  # Access tokens are cached under their digest; refresh tokens never are.
  oauth_token_digests = [
    row.token_hash
    for row in session.query(OAuthToken.token_hash)
    .filter_by(user_id=user_id, token_type=TOKEN_TYPE_ACCESS)
    .all()
  ]

  # Retention is a compliance requirement: de-reference, never delete.
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
  # Pools on graphs the org keeps pass to its owner (the plan refused any
  # pool without one).
  for pool_id, successor in _pool_successors(user_id, session).items():
    pool = session.get(GraphCredits, pool_id)
    if pool is None or successor is None:
      continue
    if pool.user_id == user_id:
      pool.user_id = successor
    if pool.billing_admin_id == user_id:
      pool.billing_admin_id = successor

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
  # Graph teardown owns documents and connections; this by-creator delete is a
  # backstop that is safe only because the org_has_live_graphs blocker means
  # every reachable graph is deprovisioned. Relaxing that blocker would let a
  # departing member's deletion take the org's live QB connection with it.
  session.query(Document).filter_by(user_id=user_id).delete(synchronize_session=False)
  session.query(Connection).filter_by(user_id=user_id).delete(synchronize_session=False)
  session.query(GraphUser).filter_by(user_id=user_id).delete(synchronize_session=False)
  # Tokens before grants before the user (FK order).
  session.query(OAuthToken).filter_by(user_id=user_id).delete(synchronize_session=False)
  session.query(OAuthGrant).filter_by(user_id=user_id).delete(synchronize_session=False)
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

  _invalidate_auth_caches(user_id, api_key_fingerprints + oauth_token_digests)

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
