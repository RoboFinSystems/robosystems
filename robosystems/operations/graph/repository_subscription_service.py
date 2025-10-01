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
