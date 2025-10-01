"""
Shared Repository Credits Allocation Tasks

Handles automated monthly allocation of credits for shared repository subscriptions.
"""

import logging
from datetime import datetime, timezone
from typing import Dict, Any

from celery import shared_task
from sqlalchemy.exc import SQLAlchemyError

from ...database import session as SessionLocal
from ...models.iam.user_repository_credits import UserRepositoryCredits
from ...models.iam.user_repository import UserRepository

logger = logging.getLogger(__name__)


def get_celery_db_session():
  """Get a database session for Celery tasks."""
  return SessionLocal()


@shared_task(bind=True, max_retries=3, default_retry_delay=300)
def allocate_monthly_shared_credits(self) -> Dict[str, Any]:
  """
  Allocate monthly credits for all active shared repository subscriptions.

  This task runs monthly to:
  1. Find all credit pools due for allocation
  2. Allocate monthly credits with rollover logic
  3. Reset monthly consumption counters
  4. Record allocation transactions

  Returns:
      Dict containing allocation results and statistics
  """
  try:
    db = get_celery_db_session()
    try:
      # Find all credit pools due for allocation
      now = datetime.now(timezone.utc)

      credit_pools = (
        db.query(UserRepositoryCredits)
        .filter(
          UserRepositoryCredits.is_active,
          UserRepositoryCredits.next_allocation_date <= now,
        )
        .all()
      )

      logger.info(f"Found {len(credit_pools)} credit pools due for allocation")

      allocated_count = 0
      failed_count = 0
      total_credits_allocated = 0
      total_credits_rolled_over = 0
      allocation_details = []

      for pool in credit_pools:
        try:
          # Get addon info for logging
          addon = pool.addon
          if not addon:
            logger.warning(f"Credit pool {pool.id} has no associated addon, skipping")
            failed_count += 1
            continue

          # Store pre-allocation state
          pre_balance = float(pool.current_balance)
          pre_rollover = float(pool.rollover_credits)

          # Perform allocation
          was_allocated = pool.allocate_monthly_credits(db)

          if was_allocated:
            allocated_count += 1

            # Calculate allocation details
            credits_allocated = float(pool.monthly_allocation)
            credits_rolled_over = float(pool.rollover_credits) - pre_rollover

            total_credits_allocated += credits_allocated
            total_credits_rolled_over += credits_rolled_over

            allocation_detail = {
              "pool_id": pool.id,
              "addon_id": addon.id,
              "user_id": addon.user_id,
              "addon_type": addon.addon_type,
              "addon_tier": addon.addon_tier,
              "credits_allocated": credits_allocated,
              "credits_rolled_over": credits_rolled_over,
              "new_balance": float(pool.current_balance),
              "previous_balance": pre_balance,
            }
            allocation_details.append(allocation_detail)

            logger.info(
              f"Allocated {credits_allocated} credits (+ {credits_rolled_over} rollover) "
              f"for {addon.addon_type} {addon.addon_tier} subscription "
              f"(user: {addon.user_id}, pool: {pool.id})"
            )
          else:
            logger.debug(f"Credit pool {pool.id} not due for allocation yet")

        except SQLAlchemyError as e:
          failed_count += 1
          logger.error(f"Database error allocating credits for pool {pool.id}: {e}")
          db.rollback()
        except Exception as e:
          failed_count += 1
          logger.error(f"Unexpected error allocating credits for pool {pool.id}: {e}")
          db.rollback()

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
        "total_pools_checked": len(credit_pools),
        "allocations_performed": allocated_count,
        "allocations_failed": failed_count,
        "total_credits_allocated": total_credits_allocated,
        "total_credits_rolled_over": total_credits_rolled_over,
        "allocation_details": allocation_details,
        "message": f"Successfully allocated credits for {allocated_count} subscription pools",
      }

      logger.info(
        f"Monthly shared credits allocation completed: "
        f"{allocated_count} successful, {failed_count} failed, "
        f"{total_credits_allocated:,.0f} credits allocated, "
        f"{total_credits_rolled_over:,.0f} credits rolled over"
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
          UserRepositoryCredits.access_id == UserRepository.id,
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
              "addon_type": pool.access_record.repository_type.value,
              "addon_tier": pool.access_record.subscription_tier.value,
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
