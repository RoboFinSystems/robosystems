"""Service for managing shared repository subscriptions (SEC, industry, economic data)."""

import logging
from datetime import UTC, datetime

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from ...config import env
from ...config.shared_repositories import (
  get_available_repositories as _get_available_manifests,
)
from ...config.shared_repositories import (
  get_manifest as _get_manifest,
)
from ...config.shared_repositories import (
  get_plan_details as _get_plan_details,
)
from ...config.shared_repositories import (
  get_repository_metadata as _get_repository_metadata,
)
from ...config.shared_repositories import (
  is_repository_enabled as _is_repository_enabled,
)
from ...models.core.user.user_repository import (
  RepositoryAccessLevel,
  RepositoryType,
  UserRepository,
)
from ...models.core.user.user_repository_credits import UserRepositoryCredits

logger = logging.getLogger(__name__)

# Repository configuration from environment
ENVIRONMENT = env.ENVIRONMENT


def get_available_repositories() -> list[RepositoryType]:
  """Get list of available repository types based on enabled status."""
  return [RepositoryType(m.id) for m in _get_available_manifests()]


def get_available_plans_for_repository(
  repository_type: RepositoryType,
) -> list[str]:
  """Get available plans for a specific repository type."""
  if not _is_repository_enabled(repository_type.value):
    return []
  manifest = _get_manifest(repository_type.value)
  if not manifest or not manifest.plans:
    return []
  return list(manifest.plans.keys())


class RepositorySubscriptionService:
  """Service for managing shared repository subscriptions and access."""

  def __init__(self, session: Session):
    self.session = session

  def ensure_repository_graph_exists(self, repository_type: RepositoryType) -> None:
    """Create the shared repository's `Graph` row if it is missing.

    Idempotent. In production the data loading pipeline creates this row; the
    on-demand path exists so a subscription can be exercised in dev and test
    before any data has been loaded.

    Raises ValueError when the repository type has no configuration.
    """
    from ...models.core.graph import Graph

    graph_id = repository_type.value
    existing = self.session.query(Graph).filter(Graph.graph_id == graph_id).first()

    if existing:
      logger.debug(f"Repository graph '{graph_id}' already exists")
      return

    config = _get_repository_metadata(repository_type.value)
    if not config:
      raise ValueError(
        f"No configuration found for repository type {repository_type.value}"
      )

    repository_graph = Graph(
      graph_id=graph_id,
      graph_name=config["name"],
      graph_type="repository",
      graph_tier=config["graph_tier"],
      graph_instance_id=config["graph_instance_id"],
      graph_cluster_region="us-east-1",
      is_repository=True,
      repository_type=repository_type.value,
      data_source_type=config["data_source_type"],
      data_source_url=config["data_source_url"],
      sync_status="active",
      sync_frequency=config["sync_frequency"],
      org_id=None,
      base_schema=None,
      schema_extensions=[],
      is_subgraph=False,
      parent_graph_id=None,
      created_at=datetime.now(UTC),
      updated_at=datetime.now(UTC),
    )

    self.session.add(repository_graph)
    self.session.commit()

    logger.info(
      f"Auto-created repository graph '{graph_id}' for subscription workflow",
      extra={
        "graph_id": graph_id,
        "repository_type": repository_type.value,
        "note": "Graph will be populated by data loading pipeline",
      },
    )

  def create_repository_subscription(
    self,
    user_id: str,
    repository_type: RepositoryType,
    repository_plan: str = "starter",
  ) -> UserRepository:
    """Subscribe a user to a shared repository.

    Returns the existing record unchanged when the user is already subscribed,
    so this is safe to retry. Raises ValueError if the repository is disabled
    or the plan is not offered for it.
    """
    if not _is_repository_enabled(repository_type.value):
      raise ValueError(
        f"Repository type {repository_type.value} is not available for subscription"
      )

    available_plans = get_available_plans_for_repository(repository_type)
    if repository_plan not in available_plans:
      raise ValueError(
        f"Plan {repository_plan} not available for repository {repository_type.value}"
      )

    plan_details = _get_plan_details(repository_plan, repo_id=repository_type.value)
    if not plan_details:
      raise ValueError(f"Repository {repository_type.value} configuration not found")

    existing = UserRepository.get_by_user_and_repository(
      user_id=user_id, repository_name=repository_type.value, session=self.session
    )

    if existing:
      logger.warning(
        f"Repository subscription already exists for {repository_type.value}"
      )
      return existing

    monthly_price_cents = int(plan_details["price_monthly"] * 100)
    access_level_str = plan_details.get("access_level", "READ")
    try:
      access_level = RepositoryAccessLevel(access_level_str.lower())
    except (ValueError, AttributeError):
      access_level = RepositoryAccessLevel.READ

    try:
      access_record = UserRepository.create_access(
        user_id=user_id,
        repository_type=repository_type,
        repository_name=repository_type.value,
        access_level=access_level,
        repository_plan=repository_plan,
        session=self.session,
        monthly_price_cents=monthly_price_cents,
        monthly_credits=plan_details["monthly_credits"],
        metadata={
          "subscribed_at": datetime.now(UTC).isoformat(),
          "subscription_method": "api",
          "plan_features": plan_details.get("features", []),
        },
      )

      logger.info(
        f"Created repository subscription for user {user_id}, "
        f"repository {repository_type.value}, plan {repository_plan}"
      )
      return access_record

    except SQLAlchemyError as e:
      self.session.rollback()
      logger.error(f"Failed to create repository subscription: {e}")
      raise

  def upgrade_repository_subscription(
    self,
    user_id: str,
    repository_type: RepositoryType,
    new_plan: str,
  ) -> UserRepository:
    """Move an existing subscription to a different plan.

    Raises ValueError when there is no subscription, the repository has since
    been disabled, or the plan is not offered for it.
    """
    access_record = UserRepository.get_by_user_and_repository(
      user_id=user_id, repository_name=repository_type.value, session=self.session
    )

    if not access_record:
      raise ValueError(f"No subscription found for repository {repository_type.value}")

    if not _is_repository_enabled(repository_type.value):
      raise ValueError(f"Repository {repository_type.value} is no longer available")

    available_plans = get_available_plans_for_repository(repository_type)
    if new_plan not in available_plans:
      raise ValueError(
        f"Plan {new_plan} not available for repository {repository_type.value}"
      )

    plan_details = _get_plan_details(new_plan, repo_id=repository_type.value)
    if not plan_details:
      raise ValueError(f"Repository {repository_type.value} configuration not found")
    new_price_cents = int(plan_details["price_monthly"] * 100)

    try:
      access_record.upgrade_tier(
        new_plan=new_plan, session=self.session, new_price_cents=new_price_cents
      )

      logger.info(
        f"Upgraded repository subscription for user {user_id}, "
        f"repository {repository_type.value} to plan {new_plan}"
      )
      return access_record

    except SQLAlchemyError as e:
      self.session.rollback()
      logger.error(f"Failed to upgrade repository subscription: {e}")
      raise

  def cancel_repository_subscription(
    self,
    user_id: str,
    repository_type: RepositoryType,
  ) -> bool:
    """Revoke a user's access to a repository. Raises if none exists."""
    access_record = UserRepository.get_by_user_and_repository(
      user_id=user_id, repository_name=repository_type.value, session=self.session
    )

    if not access_record:
      raise ValueError(f"No subscription found for repository {repository_type.value}")

    try:
      access_record.revoke_access(session=self.session)

      logger.info(
        f"Cancelled repository subscription for user {user_id}, "
        f"repository {repository_type.value}"
      )
      return True

    except SQLAlchemyError as e:
      self.session.rollback()
      logger.error(f"Failed to cancel repository subscription: {e}")
      raise

  def get_user_repository_subscriptions(
    self, user_id: str, active_only: bool = True
  ) -> list[UserRepository]:
    """List a user's repository subscriptions."""
    return list(
      UserRepository.get_user_repositories(
        user_id=user_id, session=self.session, active_only=active_only
      )
    )

  def get_repository_credits_summary(
    self, user_id: str, repository_type: RepositoryType | None = None
  ) -> dict:
    """Credit balances for one repository, or a roll-up across all of them.

    The per-repository shape is `UserRepositoryCredits.get_summary()`; the
    roll-up shape is `{repositories, total_credits, total_subscriptions}`.
    """
    if repository_type:
      credits = UserRepositoryCredits.get_user_repository_credits(
        user_id=user_id, repository_type=repository_type.value, session=self.session
      )
      return credits.get_summary() if credits else {}
    else:
      access_records = self.get_user_repository_subscriptions(user_id, active_only=True)
      summary = {
        "repositories": [],
        "total_credits": 0,
        "total_subscriptions": len(access_records),
      }

      for access_record in access_records:
        if access_record.user_credits:
          credit_info = access_record.user_credits.get_summary()
          credit_info["repository_type"] = access_record.repository_type
          credit_info["repository_plan"] = access_record.repository_plan
          summary["repositories"].append(credit_info)
          summary["total_credits"] += credit_info["current_balance"]

      return summary

  def allocate_credits(
    self,
    repository_type: RepositoryType,
    repository_plan: str,
    user_id: str,
  ) -> int:
    """Create or resize the user's monthly credit pool for a repository.

    Called during provisioning. When no access record exists yet the plan's
    allocation is returned without persisting anything — the pool is created
    later by `grant_access`.
    """
    plan_details = _get_plan_details(repository_plan, repo_id=repository_type.value)
    if not plan_details:
      raise ValueError(
        f"Plan {repository_plan} not available for repository {repository_type.value}"
      )

    monthly_credits = plan_details["monthly_credits"]

    access_record = UserRepository.get_by_user_and_repository(
      user_id=user_id, repository_name=repository_type.value, session=self.session
    )

    if not access_record:
      logger.warning(
        f"Access record not found for user {user_id}, repository {repository_type.value}. "
        f"Credits will be allocated when access is granted."
      )
      return monthly_credits

    if access_record.user_credits:
      access_record.user_credits.update_monthly_allocation(
        new_allocation=monthly_credits, session=self.session
      )
    else:
      UserRepositoryCredits.create_for_access(
        access_id=str(access_record.id),
        repository_type=repository_type,
        repository_plan=repository_plan,
        monthly_allocation=monthly_credits,
        session=self.session,
      )

    logger.info(
      f"Allocated {monthly_credits} credits for user {user_id}, "
      f"repository {repository_type.value}, plan {repository_plan}"
    )

    return monthly_credits

  def grant_access(
    self,
    repository_type: RepositoryType,
    user_id: str,
    repository_plan: str | None = None,
  ) -> bool:
    """Grant a user access to a repository after payment is confirmed.

    Idempotent: an existing record is reactivated rather than duplicated. Also
    ensures the repository `Graph` row exists, defaulting to the "starter" plan.
    """
    self.ensure_repository_graph_exists(repository_type)

    existing = UserRepository.get_by_user_and_repository(
      user_id=user_id, repository_name=repository_type.value, session=self.session
    )

    if existing:
      logger.info(
        f"Access already exists for user {user_id}, repository {repository_type.value}"
      )
      if not existing.is_active:
        existing.is_active = True
        existing.updated_at = datetime.now(UTC)
        self.session.commit()
        logger.info(f"Reactivated access for user {user_id}")
      return True

    if repository_plan is None:
      repository_plan = "starter"

    plan_config = _get_plan_details(repository_plan)
    if not plan_config:
      raise ValueError(
        f"Plan {repository_plan} not available for repository {repository_type.value}"
      )

    access_level_str = plan_config.get("access_level", "READ")
    try:
      access_level = RepositoryAccessLevel(access_level_str.lower())
    except (ValueError, AttributeError):
      access_level = RepositoryAccessLevel.READ

    UserRepository.create_access(
      user_id=user_id,
      repository_type=repository_type,
      repository_name=repository_type.value,
      access_level=access_level,
      repository_plan=repository_plan,
      session=self.session,
      monthly_price_cents=plan_config["price_cents"],
      monthly_credits=plan_config["monthly_credits"],
      metadata={
        "granted_at": datetime.now(UTC).isoformat(),
        "granted_via": "provisioning",
      },
    )

    logger.info(
      f"Granted access for user {user_id}, repository {repository_type.value}, "
      f"plan {repository_plan}"
    )

    return True

  def revoke_access(
    self,
    repository_type: RepositoryType,
    user_id: str,
  ) -> bool:
    """Revoke access, returning False when there was nothing to revoke.

    Used to unwind a partially provisioned subscription, so a missing record is
    a normal outcome rather than an error.
    """
    access_record = UserRepository.get_by_user_and_repository(
      user_id=user_id, repository_name=repository_type.value, session=self.session
    )

    if not access_record:
      logger.warning(
        f"No access record to revoke for user {user_id}, repository {repository_type.value}"
      )
      return False

    access_record.revoke_access(session=self.session)

    logger.info(
      f"Revoked access for user {user_id}, repository {repository_type.value}"
    )

    return True
