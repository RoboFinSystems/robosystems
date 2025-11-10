"""Service for managing shared repository subscriptions (SEC, industry, economic data)."""

import logging
from datetime import datetime, timezone
from typing import Optional, Dict, List

from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from ...config import env
from ...models.iam.user_repository import (
  UserRepository,
  RepositoryType,
  RepositoryPlan,
)
from ...models.iam.user_repository_credits import UserRepositoryCredits

logger = logging.getLogger(__name__)

# Repository configuration from environment
ENVIRONMENT = env.ENVIRONMENT


def get_available_repositories() -> List[RepositoryType]:
  """Get list of available repository types based on enabled status."""
  all_configs = UserRepository.get_all_repository_configs()
  return [
    RepositoryType(repo_type)
    for repo_type, config in all_configs.items()
    if config.get("enabled", False)
  ]


def get_available_plans_for_repository(
  repository_type: RepositoryType,
) -> List[RepositoryPlan]:
  """Get available plans for a specific repository type."""
  if not UserRepository.is_repository_enabled(repository_type):
    return []

  repo_config = UserRepository.get_all_repository_configs().get(repository_type)
  if not repo_config or "plans" not in repo_config:
    return []

  return list(repo_config["plans"].keys())


class RepositorySubscriptionService:
  """Service for managing shared repository subscriptions and access."""

  def __init__(self, session: Session):
    """Initialize repository subscription service with database session."""
    self.session = session

  def ensure_repository_graph_exists(self, repository_type: RepositoryType) -> None:
    """
    Ensure the shared repository graph exists in the database.

    In production, the repository graph is created by the data loading pipeline
    (e.g., SEC data loader). However, for development and testing, we need to
    create the graph entry on-demand when subscriptions are triggered.

    This method is idempotent - it only creates the graph if it doesn't exist.

    Args:
        repository_type: Type of repository (SEC, industry, economic)

    Raises:
        ValueError: If repository type is invalid or not configured
    """
    from ...models.iam.graph import Graph
    from ...config.billing.repositories import RepositoryBillingConfig

    graph_id = repository_type.value
    existing = self.session.query(Graph).filter(Graph.graph_id == graph_id).first()

    if existing:
      logger.debug(f"Repository graph '{graph_id}' already exists")
      return

    config = RepositoryBillingConfig.get_repository_metadata(repository_type)
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
      created_at=datetime.now(timezone.utc),
      updated_at=datetime.now(timezone.utc),
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
    repository_plan: RepositoryPlan = RepositoryPlan.STARTER,
  ) -> UserRepository:
    """
    Create a subscription for a shared repository.

    Args:
        user_id: User ID
        repository_type: Type of repository (SEC, industry, etc.)
        repository_plan: Repository plan (starter, advanced, unlimited)

    Returns:
        UserRepository instance
    """
    # Check if repository is enabled
    if not UserRepository.is_repository_enabled(repository_type):
      raise ValueError(
        f"Repository type {repository_type.value} is not available for subscription"
      )

    # Check if plan is available for this repository
    available_plans = get_available_plans_for_repository(repository_type)
    if repository_plan not in available_plans:
      raise ValueError(
        f"Plan {repository_plan.value} not available for repository {repository_type.value}"
      )

    # Get repository configuration
    repo_config = UserRepository.get_all_repository_configs().get(repository_type)
    if not repo_config or "plans" not in repo_config:
      raise ValueError(f"Repository {repository_type.value} configuration not found")
    plan_config = repo_config["plans"][repository_plan]

    # Check if subscription already exists
    existing = UserRepository.get_by_user_and_repository(
      user_id=user_id, repository_name=repository_type.value, session=self.session
    )

    if existing:
      logger.warning(
        f"Repository subscription already exists for {repository_type.value}"
      )
      return existing

    # Calculate pricing
    monthly_price_cents = int(plan_config["price_monthly"] * 100)

    # Create the subscription
    try:
      access_record = UserRepository.create_access(
        user_id=user_id,
        repository_type=repository_type,
        repository_name=repository_type.value,
        access_level=plan_config["access_level"],
        repository_plan=repository_plan,
        session=self.session,
        monthly_price_cents=monthly_price_cents,
        monthly_credits=plan_config["monthly_credits"],
        metadata={
          "subscribed_at": datetime.now(timezone.utc).isoformat(),
          "subscription_method": "api",
          "plan_features": plan_config.get("features", []),
        },
      )

      logger.info(
        f"Created repository subscription for user {user_id}, "
        f"repository {repository_type.value}, plan {repository_plan.value}"
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
    new_plan: RepositoryPlan,
  ) -> UserRepository:
    """
    Upgrade a repository subscription to a higher plan.

    Args:
        user_id: User ID
        repository_type: Repository type
        new_plan: New repository plan

    Returns:
        Updated UserRepository instance
    """
    # Get existing subscription
    access_record = UserRepository.get_by_user_and_repository(
      user_id=user_id, repository_name=repository_type.value, session=self.session
    )

    if not access_record:
      raise ValueError(f"No subscription found for repository {repository_type.value}")

    # Check if repository is still enabled
    if not UserRepository.is_repository_enabled(repository_type):
      raise ValueError(f"Repository {repository_type.value} is no longer available")

    # Check if new plan is available
    available_plans = get_available_plans_for_repository(repository_type)
    if new_plan not in available_plans:
      raise ValueError(
        f"Plan {new_plan.value} not available for repository {repository_type.value}"
      )

    # Get new plan configuration
    repo_config = UserRepository.get_all_repository_configs().get(repository_type)
    if not repo_config or "plans" not in repo_config:
      raise ValueError(f"Repository {repository_type.value} configuration not found")
    plan_config = repo_config["plans"][new_plan]
    new_price_cents = int(plan_config["price_monthly"] * 100)

    try:
      # Upgrade the plan
      access_record.upgrade_tier(
        new_plan=new_plan, session=self.session, new_price_cents=new_price_cents
      )

      logger.info(
        f"Upgraded repository subscription for user {user_id}, "
        f"repository {repository_type.value} to plan {new_plan.value}"
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
    """
    Cancel a repository subscription.

    Args:
        user_id: User ID
        repository_type: Repository type

    Returns:
        True if cancelled successfully
    """
    # Get existing subscription
    access_record = UserRepository.get_by_user_and_repository(
      user_id=user_id, repository_name=repository_type.value, session=self.session
    )

    if not access_record:
      raise ValueError(f"No subscription found for repository {repository_type.value}")

    try:
      # Cancel the subscription
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
  ) -> List[UserRepository]:
    """
    Get all repository subscriptions for a user.

    Args:
        user_id: User ID
        active_only: Only return active subscriptions

    Returns:
        List of UserRepository records
    """
    return list(
      UserRepository.get_user_repositories(
        user_id=user_id, session=self.session, active_only=active_only
      )
    )

  def get_repository_credits_summary(
    self, user_id: str, repository_type: Optional[RepositoryType] = None
  ) -> Dict:
    """
    Get credits summary for repository subscriptions.

    Args:
        user_id: User ID
        repository_type: Optional specific repository type

    Returns:
        Credits summary dictionary
    """
    if repository_type:
      # Get credits for specific repository
      credits = UserRepositoryCredits.get_user_repository_credits(
        user_id=user_id, repository_type=repository_type.value, session=self.session
      )
      return credits.get_summary() if credits else {}
    else:
      # Get credits for all repositories
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
    repository_plan: RepositoryPlan,
    user_id: str,
  ) -> int:
    """
    Allocate monthly credits to user for repository access.

    This method is called during provisioning to set up the initial credit allocation
    for a user's repository subscription. It retrieves the plan configuration to
    determine credit amounts and creates/updates the credit pool.

    Args:
        repository_type: Type of repository (SEC, industry, etc.)
        repository_plan: Repository plan tier (starter, advanced, unlimited)
        user_id: User ID to allocate credits to

    Returns:
        Number of credits allocated

    Raises:
        ValueError: If repository or plan configuration is invalid
        SQLAlchemyError: If database operation fails
    """
    repo_config = UserRepository.get_all_repository_configs().get(repository_type)
    if not repo_config or "plans" not in repo_config:
      raise ValueError(f"Repository {repository_type.value} configuration not found")

    plan_config = repo_config["plans"].get(repository_plan)
    if not plan_config:
      raise ValueError(
        f"Plan {repository_plan.value} not available for repository {repository_type.value}"
      )

    monthly_credits = plan_config["monthly_credits"]

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
      f"repository {repository_type.value}, plan {repository_plan.value}"
    )

    return monthly_credits

  def grant_access(
    self,
    repository_type: RepositoryType,
    user_id: str,
    repository_plan: Optional[RepositoryPlan] = None,
  ) -> bool:
    """
    Grant repository access to a user.

    This method creates a UserRepository access record for the user if one doesn't
    already exist. It's called during provisioning after payment is confirmed.

    This method will auto-create the repository graph entry if it doesn't exist,
    which is necessary for dev/testing. In production, the data loading pipeline
    should create the graph entry.

    Args:
        repository_type: Type of repository to grant access to
        user_id: User ID to grant access to
        repository_plan: Optional plan tier (if not provided, uses STARTER)

    Returns:
        True if access was granted or already exists

    Raises:
        ValueError: If repository configuration is invalid
        SQLAlchemyError: If database operation fails
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
        existing.updated_at = datetime.now(timezone.utc)
        self.session.commit()
        logger.info(f"Reactivated access for user {user_id}")
      return True

    if repository_plan is None:
      repository_plan = RepositoryPlan.STARTER

    from ...config.billing.repositories import RepositoryBillingConfig
    from ...models.iam.user_repository import RepositoryAccessLevel

    plan_config = RepositoryBillingConfig.get_plan_details(repository_plan)
    if not plan_config:
      raise ValueError(
        f"Plan {repository_plan.value} not available for repository {repository_type.value}"
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
        "granted_at": datetime.now(timezone.utc).isoformat(),
        "granted_via": "provisioning",
      },
    )

    logger.info(
      f"Granted access for user {user_id}, repository {repository_type.value}, "
      f"plan {repository_plan.value}"
    )

    return True

  def revoke_access(
    self,
    repository_type: RepositoryType,
    user_id: str,
  ) -> bool:
    """
    Revoke repository access for a user.

    This is a helper method used during error cleanup in provisioning tasks.

    Args:
        repository_type: Type of repository
        user_id: User ID

    Returns:
        True if access was revoked

    Raises:
        ValueError: If access record doesn't exist
        SQLAlchemyError: If database operation fails
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
