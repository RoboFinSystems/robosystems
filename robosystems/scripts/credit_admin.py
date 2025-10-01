#!/usr/bin/env python3
"""
Credit Admin CLI Tool

Provides administrative credit operations that were removed from the public API.
These operations require direct server access for security.

Usage:
    # Allocate credits for a specific user
    uv run python -m robosystems.scripts.credit_admin allocate-user USER_ID

    # Allocate credits for a specific graph
    uv run python -m robosystems.scripts.credit_admin allocate-graph GRAPH_ID

    # Run global allocation (all users)
    uv run python -m robosystems.scripts.credit_admin allocate-all

    # Add bonus credits
    uv run python -m robosystems.scripts.credit_admin bonus GRAPH_ID --amount 1000 --description "Customer support credit"

    # Check credit health
    uv run python -m robosystems.scripts.credit_admin health
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


def allocate_user_credits(user_id: str, dry_run: bool = False):
  """Allocate credits for a specific user's shared repositories and graphs."""
  logger.info(f"{'[DRY RUN] ' if dry_run else ''}Allocating credits for user {user_id}")

  if not dry_run:
    # Shared repository credits
    logger.info("Allocating shared repository credits...")
    shared_result = allocate_shared_credits_for_user(user_id)
    logger.info(f"Shared credits result: {shared_result}")

    # Graph credits
    logger.info("Allocating graph credits...")
    graph_result = allocate_graph_credits_for_user(user_id)
    logger.info(f"Graph credits result: {graph_result}")
  else:
    logger.info("Dry run completed - no changes made")


def allocate_graph_credits(graph_id: str, dry_run: bool = False):
  """Allocate credits for a specific graph."""
  logger.info(
    f"{'[DRY RUN] ' if dry_run else ''}Allocating credits for graph {graph_id}"
  )

  if not dry_run:
    db = next(get_db_session())
    try:
      credit_service = CreditService(db)
      result = credit_service.allocate_monthly_credits(graph_id)
      logger.info(f"Result: {result}")
    finally:
      db.close()
  else:
    logger.info("Dry run completed - no changes made")


def allocate_all_credits(dry_run: bool = False):
  """Run global credit allocation for all users and graphs."""
  logger.info(f"{'[DRY RUN] ' if dry_run else ''}Running global credit allocation")

  if not dry_run:
    # Shared repository credits
    logger.info("Allocating all shared repository credits...")
    shared_result = allocate_monthly_shared_credits()
    logger.info(f"Shared credits result: {shared_result}")

    # Graph credits
    logger.info("Allocating all graph credits...")
    graph_result = allocate_monthly_graph_credits()
    logger.info(f"Graph credits result: {graph_result}")
  else:
    logger.info("Dry run completed - no changes made")


def add_bonus_credits(
  graph_id: str, amount: float, description: str, dry_run: bool = False
):
  """Add bonus credits to a graph."""
  logger.info(
    f"{'[DRY RUN] ' if dry_run else ''}"
    f"Adding {amount} bonus credits to graph {graph_id}: {description}"
  )

  if not dry_run:
    db = next(get_db_session())
    try:
      credit_service = CreditService(db)
      result = credit_service.add_bonus_credits(
        graph_id=graph_id,
        amount=Decimal(str(amount)),
        description=description,
        metadata={"source": "admin_cli", "admin_action": True},
      )
      logger.info(f"Result: {result}")
    finally:
      db.close()
  else:
    logger.info("Dry run completed - no changes made")


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

  # Allocate user command
  user_parser = subparsers.add_parser(
    "allocate-user", help="Allocate credits for a specific user"
  )
  user_parser.add_argument("user_id", help="User ID to allocate credits for")
  user_parser.add_argument(
    "--dry-run", action="store_true", help="Preview without making changes"
  )

  # Allocate graph command
  graph_parser = subparsers.add_parser(
    "allocate-graph", help="Allocate credits for a specific graph"
  )
  graph_parser.add_argument("graph_id", help="Graph ID to allocate credits for")
  graph_parser.add_argument(
    "--dry-run", action="store_true", help="Preview without making changes"
  )

  # Allocate all command
  all_parser = subparsers.add_parser(
    "allocate-all", help="Run global credit allocation"
  )
  all_parser.add_argument(
    "--dry-run", action="store_true", help="Preview without making changes"
  )

  # Bonus credits command
  bonus_parser = subparsers.add_parser("bonus", help="Add bonus credits to a graph")
  bonus_parser.add_argument("graph_id", help="Graph ID to add credits to")
  bonus_parser.add_argument(
    "--amount", type=float, required=True, help="Amount of credits to add"
  )
  bonus_parser.add_argument(
    "--description", required=True, help="Description for the credit addition"
  )
  bonus_parser.add_argument(
    "--dry-run", action="store_true", help="Preview without making changes"
  )

  # Health check command
  subparsers.add_parser("health", help="Check credit system health")

  args = parser.parse_args()

  if not args.command:
    parser.print_help()
    return

  if args.command == "allocate-user":
    allocate_user_credits(args.user_id, args.dry_run)
  elif args.command == "allocate-graph":
    allocate_graph_credits(args.graph_id, args.dry_run)
  elif args.command == "allocate-all":
    allocate_all_credits(args.dry_run)
  elif args.command == "bonus":
    add_bonus_credits(args.graph_id, args.amount, args.description, args.dry_run)
  elif args.command == "health":
    check_health()


if __name__ == "__main__":
  main()
