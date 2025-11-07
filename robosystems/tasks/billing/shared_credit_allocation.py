"""
Shared Repository Credits Allocation Tasks

Handles automated monthly allocation of credits for shared repository subscriptions.

Integration with BillingSubscription:
------------------------------------
BillingSubscription is the source of truth for subscription status.
This task ensures UserRepositoryCredits stays synchronized:
- Creates UserRepositoryCredits if BillingSubscription exists but credits don't
- Only allocates if BillingSubscription status is ACTIVE
- Deactivates UserRepositoryCredits if subscription is canceled

Shared Repositories:
-------------------
- SEC (Securities and Exchange Commission data)
- Industry (industry benchmarks)
- Economic (economic indicators)
- Market (market data)

Monthly Allocation Flow:
-----------------------
1. Query active BillingSubscriptions (resource_type="repository")
2. Ensure UserRepositoryCredits exists for each subscription
3. Allocate monthly credits with rollover logic
4. Deactivate credits for canceled subscriptions
"""

import logging
from datetime import datetime, timezone
from typing import Dict, Any

from celery import shared_task
from sqlalchemy.exc import SQLAlchemyError

from ...database import session as SessionLocal
from ...models.iam.user_repository_credits import UserRepositoryCredits
from ...models.iam.user_repository import UserRepository, RepositoryType, RepositoryPlan
from ...models.billing import BillingSubscription

logger = logging.getLogger(__name__)


def get_celery_db_session():
  """Get a database session for Celery tasks."""
  return SessionLocal()


def deactivate_canceled_subscription_credits(db) -> int:
  """
  Deactivate UserRepositoryCredits for canceled subscriptions.

  Ensures credit pools stay synchronized with subscription status.
  If a subscription is canceled or inactive, the credit pool should be deactivated.

  Returns:
      Number of credit pools deactivated
  """
  # Get all active UserRepositoryCredits
  active_credit_pools = (
    db.query(UserRepositoryCredits)
    .join(UserRepository)
    .filter(UserRepositoryCredits.is_active)
    .all()
  )

  deactivated_count = 0

  for credit_pool in active_credit_pools:
    user_repo = credit_pool.user_repository
    if not user_repo:
      continue

    # Check if corresponding BillingSubscription is active
    subscription = BillingSubscription.get_by_resource_and_user(
      resource_type="repository",
      resource_id=user_repo.repository_name,
      user_id=user_repo.user_id,
      session=db,
    )

    # If no active subscription exists, deactivate the credit pool
    if not subscription or subscription.status != "active":
      credit_pool.is_active = False
      deactivated_count += 1
      logger.info(
        f"Deactivated UserRepositoryCredits for {user_repo.user_id}/{user_repo.repository_name} "
        f"(subscription status: {subscription.status if subscription else 'not found'})"
      )

  return deactivated_count


@shared_task(bind=True, max_retries=3, default_retry_delay=300)
def allocate_monthly_shared_credits(self) -> Dict[str, Any]:
  """
  Allocate monthly credits for all active shared repository subscriptions.

  BillingSubscription Integration:
  --------------------------------
  This task uses BillingSubscription as the source of truth:
  1. Query active BillingSubscriptions (resource_type="repository")
  2. Ensure UserRepository and UserRepositoryCredits exist
  3. Only allocate if subscription status is ACTIVE
  4. Sync credit pool status with subscription status

  Returns:
      Dict containing allocation results and statistics
  """
  try:
    db = get_celery_db_session()
    try:
      now = datetime.now(timezone.utc)

      # Query active repository subscriptions from BillingSubscription (source of truth)
      active_subscriptions = (
        db.query(BillingSubscription)
        .filter(
          BillingSubscription.resource_type == "repository",
          BillingSubscription.status == "active",
        )
        .all()
      )

      logger.info(
        f"Found {len(active_subscriptions)} active repository subscriptions from BillingSubscription"
      )

      allocated_count = 0
      failed_count = 0
      created_count = 0
      synced_count = 0
      total_credits_allocated = 0
      total_credits_rolled_over = 0
      allocation_details = []

      for subscription in active_subscriptions:
        try:
          user_id = subscription.billing_customer_user_id
          repository_name = subscription.resource_id
          plan_name = subscription.plan_name

          # Get or create UserRepository
          user_repo = (
            db.query(UserRepository)
            .filter(
              UserRepository.user_id == user_id,
              UserRepository.repository_name == repository_name,
            )
            .first()
          )

          if not user_repo:
            # Parse plan name to extract tier (e.g., "sec-starter" -> "starter")
            plan_tier = plan_name.split("-")[-1] if "-" in plan_name else plan_name

            try:
              repository_type = RepositoryType(repository_name.lower())
              repository_plan = RepositoryPlan(plan_tier.lower())
            except ValueError as e:
              logger.error(
                f"Invalid repository type or plan for {repository_name}/{plan_tier}: {e}"
              )
              failed_count += 1
              continue

            user_repo = UserRepository(
              user_id=user_id,
              repository_name=repository_name,
              repository_type=repository_type,
              repository_plan=repository_plan,
              is_active=True,
            )
            db.add(user_repo)
            db.flush()
            logger.info(f"Created UserRepository for {user_id}/{repository_name}")

          # Get or create UserRepositoryCredits
          credit_pool = UserRepositoryCredits.get_user_repository_credits(
            user_id=user_id, repository_type=repository_name, session=db
          )

          if not credit_pool:
            # Create credit pool - let the model determine monthly allocation
            credit_pool = UserRepositoryCredits(
              user_repository_id=user_repo.id,
              is_active=True,
            )
            db.add(credit_pool)
            db.flush()
            created_count += 1
            logger.info(
              f"Created UserRepositoryCredits for {user_id}/{repository_name}"
            )
          elif not credit_pool.is_active:
            # Reactivate if subscription is active but credits are inactive
            credit_pool.is_active = True
            synced_count += 1
            logger.info(
              f"Reactivated UserRepositoryCredits for {user_id}/{repository_name}"
            )

          # Check if allocation is due
          if (
            credit_pool.next_allocation_date and credit_pool.next_allocation_date > now
          ):
            logger.debug(
              f"Credit pool for {user_id}/{repository_name} not due yet (next: {credit_pool.next_allocation_date})"
            )
            continue

          # Store pre-allocation state
          pre_balance = float(credit_pool.current_balance)
          pre_rollover = float(credit_pool.rollover_credits)

          # Perform allocation
          was_allocated = credit_pool.allocate_monthly_credits(db)

          if was_allocated:
            allocated_count += 1

            # Calculate allocation details
            credits_allocated = float(credit_pool.monthly_allocation)
            credits_rolled_over = float(credit_pool.rollover_credits) - pre_rollover

            total_credits_allocated += credits_allocated
            total_credits_rolled_over += credits_rolled_over

            allocation_detail = {
              "subscription_id": subscription.id,
              "user_id": user_id,
              "repository": repository_name,
              "plan_name": plan_name,
              "credits_allocated": credits_allocated,
              "credits_rolled_over": credits_rolled_over,
              "new_balance": float(credit_pool.current_balance),
              "previous_balance": pre_balance,
            }
            allocation_details.append(allocation_detail)

            logger.info(
              f"Allocated {credits_allocated} credits (+ {credits_rolled_over} rollover) "
              f"for {repository_name} {plan_name} subscription (user: {user_id})"
            )

        except SQLAlchemyError as e:
          failed_count += 1
          logger.error(f"Database error allocating credits for {subscription.id}: {e}")
          db.rollback()
        except Exception as e:
          failed_count += 1
          logger.error(
            f"Unexpected error allocating credits for {subscription.id}: {e}"
          )
          db.rollback()

      # Deactivate credit pools for canceled subscriptions
      deactivated_count = deactivate_canceled_subscription_credits(db)

      # Final commit for successful allocations
      try:
        db.commit()
      except SQLAlchemyError as e:
        logger.error(f"Failed to commit allocation transactions: {e}")
        db.rollback()
        raise

      result = {
        "success": True,
        "timestamp": now.isoformat(),
        "active_subscriptions_found": len(active_subscriptions),
        "allocations_performed": allocated_count,
        "allocations_failed": failed_count,
        "credit_pools_created": created_count,
        "credit_pools_synced": synced_count,
        "credit_pools_deactivated": deactivated_count,
        "total_credits_allocated": total_credits_allocated,
        "total_credits_rolled_over": total_credits_rolled_over,
        "allocation_details": allocation_details,
        "message": f"Successfully allocated credits for {allocated_count} subscriptions",
      }

      logger.info(
        f"Monthly shared credits allocation completed: "
        f"{allocated_count} allocated, {created_count} created, "
        f"{synced_count} synced, {deactivated_count} deactivated, "
        f"{failed_count} failed, "
        f"{total_credits_allocated:,.0f} credits allocated"
      )

      return result

    except Exception:
      db.rollback()
      raise
    finally:
      db.close()

  except Exception as e:
    logger.error(f"Critical error in monthly shared credits allocation: {e}")

    # Retry with exponential backoff for transient errors
    if self.request.retries < self.max_retries:
      logger.info(f"Retrying allocation task (attempt {self.request.retries + 1})")
      raise self.retry(countdown=300 * (2**self.request.retries))

    return {
      "success": False,
      "error": str(e),
      "timestamp": datetime.now(timezone.utc).isoformat(),
      "message": "Failed to complete monthly shared credits allocation",
    }


@shared_task
def allocate_shared_credits_for_user(user_id: str) -> Dict[str, Any]:
  """
  Allocate monthly credits for a specific user's shared repository subscriptions.

  Args:
      user_id: The user ID to allocate credits for

  Returns:
      Dict containing allocation results for the user
  """
  try:
    db = get_celery_db_session()
    try:
      # Find user's active credit pools
      credit_pools = (
        db.query(UserRepositoryCredits)
        .join(
          UserRepository,
          UserRepositoryCredits.user_repository_id == UserRepository.id,
        )
        .filter(
          UserRepository.user_id == user_id,
          UserRepository.is_active,
          UserRepositoryCredits.is_active,
        )
        .all()
      )

      logger.info(f"Found {len(credit_pools)} credit pools for user {user_id}")

      allocated_count = 0
      allocation_details = []

      for pool in credit_pools:
        was_allocated = pool.allocate_monthly_credits(db)
        if was_allocated:
          allocated_count += 1
          allocation_details.append(
            {
              "pool_id": pool.id,
              "repository_name": pool.user_repository.repository_name,
              "repository_type": pool.user_repository.repository_type.value,
              "repository_plan": pool.user_repository.repository_plan.value,
              "credits_allocated": float(pool.monthly_allocation),
              "new_balance": float(pool.current_balance),
            }
          )

      db.commit()

      return {
        "success": True,
        "user_id": user_id,
        "allocations_performed": allocated_count,
        "allocation_details": allocation_details,
        "message": f"Allocated credits for {allocated_count} subscriptions",
      }

    except Exception as e:
      db.rollback()
      logger.error(f"Error allocating shared credits for user {user_id}: {e}")
      return {
        "success": False,
        "user_id": user_id,
        "error": str(e),
        "message": "Failed to allocate credits for user",
      }
    finally:
      db.close()

  except Exception as e:
    logger.error(f"Error allocating shared credits for user {user_id}: {e}")
    return {
      "success": False,
      "user_id": user_id,
      "error": str(e),
      "message": "Failed to allocate credits for user",
    }


@shared_task
def check_credit_allocation_health() -> Dict[str, Any]:
  """
  Health check task to monitor credit allocation system.

  Checks for:
  - Overdue allocations
  - Inactive credit pools with recent activity
  - Credit pools with negative balances

  Returns:
      Dict containing health check results
  """
  try:
    db = get_celery_db_session()
    try:
      now = datetime.now(timezone.utc)

      # Check for overdue allocations (more than 2 days past due)
      from datetime import timedelta

      overdue_threshold = now - timedelta(days=2)

      overdue_pools = (
        db.query(UserRepositoryCredits)
        .filter(
          UserRepositoryCredits.is_active,
          UserRepositoryCredits.next_allocation_date < overdue_threshold,
        )
        .count()
      )

      # Check for negative balances
      negative_balance_pools = (
        db.query(UserRepositoryCredits)
        .filter(
          UserRepositoryCredits.is_active, UserRepositoryCredits.current_balance < 0
        )
        .count()
      )

      # Check total active pools
      total_active_pools = (
        db.query(UserRepositoryCredits).filter(UserRepositoryCredits.is_active).count()
      )

      # Check pools due for allocation in next 7 days
      upcoming_threshold = now + timedelta(days=7)
      upcoming_allocations = (
        db.query(UserRepositoryCredits)
        .filter(
          UserRepositoryCredits.is_active,
          UserRepositoryCredits.next_allocation_date <= upcoming_threshold,
          UserRepositoryCredits.next_allocation_date > now,
        )
        .count()
      )

      health_status = {
        "healthy": overdue_pools == 0 and negative_balance_pools == 0,
        "timestamp": now.isoformat(),
        "total_active_pools": total_active_pools,
        "overdue_allocations": overdue_pools,
        "negative_balance_pools": negative_balance_pools,
        "upcoming_allocations_7_days": upcoming_allocations,
        "warnings": [],
      }

      if overdue_pools > 0:
        health_status["warnings"].append(
          f"{overdue_pools} credit pools have overdue allocations"
        )

      if negative_balance_pools > 0:
        health_status["warnings"].append(
          f"{negative_balance_pools} credit pools have negative balances"
        )

      logger.info(f"Credit allocation health check: {health_status}")
      return health_status

    except Exception as e:
      logger.error(f"Error in credit allocation health check: {e}")
      return {
        "healthy": False,
        "error": str(e),
        "timestamp": datetime.now(timezone.utc).isoformat(),
      }
    finally:
      db.close()

  except Exception as e:
    logger.error(f"Error in credit allocation health check: {e}")
    return {
      "healthy": False,
      "error": str(e),
      "timestamp": datetime.now(timezone.utc).isoformat(),
    }
