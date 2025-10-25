#!/usr/bin/env python3
"""
Credit Admin CLI Tool

Manual credit operations for customer support and system administration.

IMPORTANT: Monthly credit allocation is automated via Celery Beat (1st of month at 3:00 AM UTC).
This tool should only be used for manual operations that cannot be automated.

Usage:
    # Add bonus credits to a user graph
    uv run python -m robosystems.scripts.credit_admin bonus-graph GRAPH_ID --amount 1000 --description "Compensation for downtime"

    # Add bonus credits to a user's repository subscription
    uv run python -m robosystems.scripts.credit_admin bonus-repository USER_ID REPOSITORY_NAME --amount 1000 --description "Promotional credits"

    # Check credit system health
    uv run python -m robosystems.scripts.credit_admin health

    # Emergency manual allocation (ONLY if automated system failed)
    uv run python -m robosystems.scripts.credit_admin force-allocate-user USER_ID --confirm
    uv run python -m robosystems.scripts.credit_admin force-allocate-all --confirm

Automated Tasks (via Celery Beat):
    - Monthly graph credit allocation: 1st of month at 3:30 AM UTC
    - Monthly shared repository credit allocation: 1st of month at 3:00 AM UTC
    - Weekly health checks: Every Monday at 3:00 AM UTC
"""

import argparse
import logging
from decimal import Decimal

from robosystems.database import get_db_session
from robosystems.operations.graph.credit_service import CreditService
from robosystems.tasks.billing.shared_credit_allocation import (
  allocate_monthly_shared_credits,
  allocate_shared_credits_for_user,
  check_credit_allocation_health,
)
from robosystems.tasks.billing.credit_allocation import (
  allocate_monthly_graph_credits,
  allocate_graph_credits_for_user,
  check_graph_credit_health,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def add_bonus_credits_to_graph(
  graph_id: str, amount: float, description: str, dry_run: bool = False
):
  """Add bonus credits to a user graph."""
  print(
    f"{'[DRY RUN] ' if dry_run else ''}"
    f"Adding {amount} bonus credits to graph {graph_id}: {description}"
  )

  db = next(get_db_session())
  try:
    if dry_run:
      from robosystems.models.iam.graph_credits import GraphCredits

      credits = GraphCredits.get_by_graph_id(graph_id, db)
      if not credits:
        print(f"Error: No credit pool found for graph {graph_id}")
        return

      print(f"Graph ID: {graph_id}")
      print(f"User ID: {credits.user_id}")
      print(f"Current Balance: {credits.current_balance}")
      print(f"Bonus Amount: {amount}")
      print(f"Description: {description}")
      print(
        f"\nWOULD ADD: {amount} credits "
        f"(new balance would be: {credits.current_balance + Decimal(str(amount))})"
      )
    else:
      credit_service = CreditService(db)
      result = credit_service.add_bonus_credits(
        graph_id=graph_id,
        amount=Decimal(str(amount)),
        description=description,
        metadata={"source": "admin_cli", "admin_action": True},
      )
      print(f"Success: {result}")
  finally:
    db.close()


def add_bonus_credits_to_repository(
  user_id: str,
  repository_name: str,
  amount: float,
  description: str,
  dry_run: bool = False,
):
  """Add bonus credits to a user's repository subscription."""
  print(
    f"{'[DRY RUN] ' if dry_run else ''}"
    f"Adding {amount} bonus credits to {user_id}'s {repository_name} subscription: {description}"
  )

  db = next(get_db_session())
  try:
    from robosystems.models.iam.user_repository import UserRepository
    from robosystems.models.iam.user_repository_credits import UserRepositoryCredits
    from datetime import datetime, timezone

    # Find the user's repository access
    access = UserRepository.get_by_user_and_repository(user_id, repository_name, db)
    if not access:
      print(f"Error: User {user_id} has no access to {repository_name}")
      return

    # Find the credit pool
    credits = (
      db.query(UserRepositoryCredits)
      .filter(UserRepositoryCredits.user_repository_id == access.id)
      .first()
    )
    if not credits:
      print(
        f"Error: No credit pool found for {user_id}'s {repository_name} subscription"
      )
      return

    if dry_run:
      print(f"User ID: {user_id}")
      print(f"Repository: {repository_name}")
      print(f"Current Balance: {credits.current_balance}")
      print(f"Bonus Amount: {amount}")
      print(f"Description: {description}")
      print(
        f"\nWOULD ADD: {amount} credits "
        f"(new balance would be: {credits.current_balance + Decimal(str(amount))})"
      )
    else:
      # Add credits
      credits.current_balance += Decimal(str(amount))
      credits.updated_at = datetime.now(timezone.utc)

      # Record transaction
      from robosystems.models.iam.user_repository_credits import (
        UserRepositoryCreditTransaction,
        UserRepositoryCreditTransactionType,
      )
      import uuid

      idempotency_key = f"bonus_{user_id}_{repository_name}_{uuid.uuid4()}"

      UserRepositoryCreditTransaction.create_transaction(
        credit_pool_id=credits.id,
        transaction_type=UserRepositoryCreditTransactionType.BONUS,
        amount=Decimal(str(amount)),
        description=description,
        metadata={
          "source": "admin_cli",
          "admin_action": True,
          "idempotency_key": idempotency_key,
        },
        session=db,
      )

      db.commit()
      print(
        f"Success: Added {amount} bonus credits to {user_id}'s {repository_name} subscription. "
        f"New balance: {credits.current_balance}"
      )
  except Exception as e:
    db.rollback()
    print(f"Error: {str(e)}")
  finally:
    db.close()


def force_allocate_user(user_id: str, confirm: bool = False):
  """
  Emergency manual allocation for a specific user.

  WARNING: This should ONLY be used if automated Celery Beat allocation failed.
  Running this on the 1st of the month could cause double-allocation.
  """
  if not confirm:
    print("ERROR: This is a dangerous operation that could cause double-allocation.")
    print(
      "Monthly allocation is automated via Celery Beat (1st of month at 3:00 AM UTC)."
    )
    print("Only use this if the automated system has failed.")
    print("\nTo proceed, add --confirm flag")
    return

  print(f"WARNING: Force allocating credits for user {user_id}")
  print("This bypasses normal Celery Beat scheduling.\n")

  # Shared repository credits
  print("Allocating shared repository credits...")
  shared_result = allocate_shared_credits_for_user(user_id)
  print(f"Shared credits result: {shared_result}")

  # Graph credits
  print("Allocating graph credits...")
  graph_result = allocate_graph_credits_for_user(user_id)
  print(f"Graph credits result: {graph_result}")


def force_allocate_all(confirm: bool = False):
  """
  Emergency manual allocation for ALL users and graphs.

  WARNING: This should ONLY be used if automated Celery Beat allocation failed.
  Running this on the 1st of the month WILL cause double-allocation.
  """
  if not confirm:
    print("ERROR: This is an EXTREMELY dangerous operation.")
    print(
      "Monthly allocation is automated via Celery Beat (1st of month at 3:00 AM UTC)."
    )
    print("Running this will allocate credits to ALL users and graphs in the system.")
    print("If run on the 1st of the month, this WILL cause double-allocation.")
    print("\nTo proceed, add --confirm flag")
    return

  print("WARNING: Force allocating credits for ALL users and graphs")
  print("This bypasses normal Celery Beat scheduling.\n")

  # Shared repository credits
  print("Allocating all shared repository credits...")
  shared_result = allocate_monthly_shared_credits()
  print(f"Shared credits result: {shared_result}")

  # Graph credits
  print("Allocating all graph credits...")
  graph_result = allocate_monthly_graph_credits()
  print(f"Graph credits result: {graph_result}")


def check_health():
  """Check health of the credit system."""
  logger.info("Checking credit system health...")

  # Check shared repository health
  logger.info("\nShared Repository Credit Health:")
  shared_health = check_credit_allocation_health()
  logger.info(f"{shared_health}")

  # Check graph credit health
  logger.info("\nGraph Credit Health:")
  graph_health = check_graph_credit_health()
  logger.info(f"{graph_health}")


def main():
  parser = argparse.ArgumentParser(
    description="Credit administration tool for RoboSystems",
    formatter_class=argparse.RawDescriptionHelpFormatter,
    epilog=__doc__,
  )

  subparsers = parser.add_subparsers(dest="command", help="Command to run")

  # Bonus credits for graph
  bonus_graph_parser = subparsers.add_parser(
    "bonus-graph", help="Add bonus credits to a user graph"
  )
  bonus_graph_parser.add_argument("graph_id", help="Graph ID to add credits to")
  bonus_graph_parser.add_argument(
    "--amount", type=float, required=True, help="Amount of credits to add"
  )
  bonus_graph_parser.add_argument(
    "--description", required=True, help="Description for the credit addition"
  )
  bonus_graph_parser.add_argument(
    "--dry-run", action="store_true", help="Preview without making changes"
  )

  # Bonus credits for repository
  bonus_repo_parser = subparsers.add_parser(
    "bonus-repository", help="Add bonus credits to a user's repository subscription"
  )
  bonus_repo_parser.add_argument("user_id", help="User ID")
  bonus_repo_parser.add_argument(
    "repository_name", help="Repository name (sec, industry, economic, etc.)"
  )
  bonus_repo_parser.add_argument(
    "--amount", type=float, required=True, help="Amount of credits to add"
  )
  bonus_repo_parser.add_argument(
    "--description", required=True, help="Description for the credit addition"
  )
  bonus_repo_parser.add_argument(
    "--dry-run", action="store_true", help="Preview without making changes"
  )

  # Force allocate user (emergency)
  force_user_parser = subparsers.add_parser(
    "force-allocate-user",
    help="Emergency manual allocation for a specific user (DANGEROUS)",
  )
  force_user_parser.add_argument("user_id", help="User ID to allocate credits for")
  force_user_parser.add_argument(
    "--confirm",
    action="store_true",
    help="Confirm you understand this bypasses automated scheduling",
  )

  # Force allocate all (emergency)
  force_all_parser = subparsers.add_parser(
    "force-allocate-all",
    help="Emergency manual allocation for ALL users (EXTREMELY DANGEROUS)",
  )
  force_all_parser.add_argument(
    "--confirm",
    action="store_true",
    help="Confirm you understand this bypasses automated scheduling",
  )

  # Health check command
  subparsers.add_parser("health", help="Check credit system health")

  args = parser.parse_args()

  if not args.command:
    parser.print_help()
    return

  if args.command == "bonus-graph":
    add_bonus_credits_to_graph(
      args.graph_id, args.amount, args.description, args.dry_run
    )
  elif args.command == "bonus-repository":
    add_bonus_credits_to_repository(
      args.user_id, args.repository_name, args.amount, args.description, args.dry_run
    )
  elif args.command == "force-allocate-user":
    force_allocate_user(args.user_id, args.confirm)
  elif args.command == "force-allocate-all":
    force_allocate_all(args.confirm)
  elif args.command == "health":
    check_health()


if __name__ == "__main__":
  main()
