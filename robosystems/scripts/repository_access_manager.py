#!/usr/bin/env python
"""
Generic Repository Access Management Script

This script provides commands for managing user access to shared data repositories
like SEC, industry data, economic indicators, etc.
"""

import argparse
import sys
import os
from datetime import datetime, timedelta, timezone

# Add parent directory to path to avoid circular imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from robosystems.models.iam import (
  UserRepository,
  UserRepositoryAccessLevel as RepositoryAccessLevel,
  RepositoryType,
  RepositoryPlan,
  User,
)
from robosystems.database import session

# Repository configuration (avoid importing MultiTenantUtils to prevent circular import)
SUPPORTED_REPOSITORIES = {
  "sec": RepositoryType.SEC,
  "industry": RepositoryType.INDUSTRY,
  "economic": RepositoryType.ECONOMIC,
}


def grant_access(
  user_id: str, repository_name: str, access_level: str, expires_days: int = None
) -> None:
  """Grant repository access to a user."""
  try:
    # Validate user exists
    user = User.get_by_id(user_id, session)
    if not user:
      print(f"Error: User '{user_id}' not found")
      sys.exit(1)

    # Validate repository exists
    if repository_name not in SUPPORTED_REPOSITORIES:
      print(f"Error: Unknown repository '{repository_name}'")
      print(f"Available repositories: {', '.join(SUPPORTED_REPOSITORIES.keys())}")
      sys.exit(1)

    # Validate access level
    try:
      level = RepositoryAccessLevel(access_level.lower())
    except ValueError:
      valid_levels = [
        level.value
        for level in RepositoryAccessLevel
        if level != RepositoryAccessLevel.NONE
      ]
      print(
        f"Error: Invalid access level '{access_level}'. Valid levels: {', '.join(valid_levels)}"
      )
      sys.exit(1)

    # Calculate expiration if specified
    expires_at = None
    if expires_days:
      expires_at = datetime.now(timezone.utc) + timedelta(days=expires_days)

    # Get repository type
    repository_type = SUPPORTED_REPOSITORIES[repository_name]

    # Determine monthly credits based on repository and plan
    monthly_credits_map = {
      (RepositoryType.SEC, RepositoryPlan.STARTER): 5000,
      (RepositoryType.SEC, RepositoryPlan.ADVANCED): 25000,
      (RepositoryType.SEC, RepositoryPlan.UNLIMITED): 100000,
      (RepositoryType.INDUSTRY, RepositoryPlan.STARTER): 3000,
      (RepositoryType.ECONOMIC, RepositoryPlan.STARTER): 2000,
    }
    monthly_credits = monthly_credits_map.get(
      (repository_type, RepositoryPlan.STARTER), 0
    )

    # Grant access
    UserRepository.create_access(
      user_id=user_id,
      repository_type=repository_type,
      repository_name=repository_name,
      access_level=level,
      repository_plan=RepositoryPlan.STARTER,  # Default plan
      session=session,
      granted_by=None,  # Script-based grants don't have a specific granting user
      monthly_credits=monthly_credits,
      expires_at=expires_at,
    )

    expire_msg = f" (expires in {expires_days} days)" if expires_days else ""
    print(
      f"✓ {repository_name.upper()} {access_level} access granted to user {user_id}{expire_msg}"
    )
    if monthly_credits > 0:
      print(f"  Credit pool created with {monthly_credits:,} monthly credits")

  except Exception as e:
    print(f"Error granting repository access: {str(e)}")
    sys.exit(1)


def revoke_access(user_id: str, repository_name: str) -> None:
  """Revoke repository access from a user."""
  try:
    # Validate repository exists
    if repository_name not in SUPPORTED_REPOSITORIES:
      print(f"Error: Unknown repository '{repository_name}'")
      print(f"Available repositories: {', '.join(SUPPORTED_REPOSITORIES.keys())}")
      sys.exit(1)

    # Find the access record
    access_record = UserRepository.get_by_user_and_repository(
      user_id, repository_name, session
    )
    if access_record:
      access_record.revoke_access(session)
      print(f"✓ {repository_name.upper()} access revoked for user {user_id}")
    else:
      print(f"No {repository_name.upper()} access found for user {user_id}")

  except Exception as e:
    print(f"Error revoking repository access: {str(e)}")
    sys.exit(1)


def list_access(repository_name: str = None) -> None:
  """List users with repository access."""
  try:
    if repository_name:
      # List users for specific repository
      if repository_name not in SUPPORTED_REPOSITORIES:
        print(f"Error: Unknown repository '{repository_name}'")
        print(f"Available repositories: {', '.join(SUPPORTED_REPOSITORIES.keys())}")
        sys.exit(1)

      access_records = UserRepository.get_repository_users(repository_name, session)
      title = f"Users with {repository_name.upper()} Repository Access:"
    else:
      # List all repository access records
      access_records = []
      for repo in SUPPORTED_REPOSITORIES.keys():
        access_records.extend(UserRepository.get_repository_users(repo, session))
      title = "All Repository Access Records:"

    if not access_records:
      print("No repository access records found")
      return

    print(title)
    print("-" * 100)
    print(
      f"{'User ID':<25} {'Repository':<12} {'Access Level':<12} {'Active':<8} {'Granted At':<20} {'Expires At':<20}"
    )
    print("-" * 100)

    for access in access_records:
      granted_at = (
        access.granted_at.strftime("%Y-%m-%d %H:%M") if access.granted_at else "N/A"
      )
      expires_at = (
        access.expires_at.strftime("%Y-%m-%d %H:%M") if access.expires_at else "Never"
      )
      active_status = "Yes" if access.is_active and not access.is_expired() else "No"

      print(
        f"{access.user_id:<25} {access.repository_name:<12} {access.access_level.value:<12} {active_status:<8} {granted_at:<20} {expires_at:<20}"
      )

  except Exception as e:
    print(f"Error listing repository access: {str(e)}")
    sys.exit(1)


def check_access(user_id: str, repository_name: str = None) -> None:
  """Check repository access for a specific user."""
  try:
    # Check if user exists
    user = User.get_by_id(user_id, session)
    if not user:
      print(f"Error: User '{user_id}' not found")
      sys.exit(1)

    if repository_name:
      # Check access for specific repository
      if repository_name not in SUPPORTED_REPOSITORIES:
        print(f"Error: Unknown repository '{repository_name}'")
        print(f"Available repositories: {', '.join(SUPPORTED_REPOSITORIES.keys())}")
        sys.exit(1)

      access = UserRepository.get_by_user_and_repository(
        user_id, repository_name, session
      )

      if not access:
        print(f"User {user_id} has no {repository_name.upper()} access")
        return

      print(f"{repository_name.upper()} Access for User: {user_id}")
      print("-" * 50)
      _print_access_details(access)
    else:
      # Check access for all repositories
      all_access = UserRepository.get_user_repositories(user_id, session)

      if not all_access:
        print(f"User {user_id} has no repository access")
        return

      print(f"All Repository Access for User: {user_id}")
      print("=" * 60)

      for access in all_access:
        print(f"\n{access.repository_name.upper()} Repository:")
        print("-" * 30)
        _print_access_details(access)

  except Exception as e:
    print(f"Error checking repository access: {str(e)}")
    sys.exit(1)


def list_repositories() -> None:
  """List all available shared repositories."""
  try:
    repositories = list(SUPPORTED_REPOSITORIES.keys())

    print("Available Shared Repositories:")
    print("-" * 40)
    print(f"{'Repository ID':<15} {'Description'}")
    print("-" * 40)

    descriptions = {
      "sec": "SEC Public Filings",
      "industry": "Industry Benchmarks",
      "economic": "Economic Indicators",
      "regulatory": "Regulatory Data",
      "market": "Market Data",
      "esg": "ESG Data",
    }

    for repo in repositories:
      desc = descriptions.get(repo, "Shared data repository")
      print(f"{repo:<15} {desc}")

  except Exception as e:
    print(f"Error listing repositories: {str(e)}")
    sys.exit(1)


def _print_access_details(access):
  """Helper function to print access details."""
  print(f"Access Level: {access.access_level.value}")
  print(f"Repository Type: {access.repository_type.value}")
  print(f"Active: {'Yes' if access.is_active else 'No'}")
  print(f"Expired: {'Yes' if access.is_expired() else 'No'}")
  print(
    f"Granted At: {access.granted_at.strftime('%Y-%m-%d %H:%M') if access.granted_at else 'N/A'}"
  )
  print(
    f"Expires At: {access.expires_at.strftime('%Y-%m-%d %H:%M') if access.expires_at else 'Never'}"
  )

  print("\nPermissions:")
  print(f"  Can Read: {'Yes' if access.can_read() else 'No'}")
  print(f"  Can Write: {'Yes' if access.can_write() else 'No'}")
  print(f"  Can Admin: {'Yes' if access.can_admin() else 'No'}")


def main():
  parser = argparse.ArgumentParser(
    description="Manage user access to shared data repositories"
  )
  subparsers = parser.add_subparsers(dest="command", help="Available commands")

  # Grant access command
  grant_parser = subparsers.add_parser(
    "grant", help="Grant repository access to a user"
  )
  grant_parser.add_argument("user_id", help="User ID to grant access to")
  grant_parser.add_argument(
    "repository", help="Repository name (sec, industry, economic, etc.)"
  )
  grant_parser.add_argument(
    "access_level", choices=["read", "write", "admin"], help="Access level to grant"
  )
  grant_parser.add_argument(
    "--expires-days",
    type=int,
    help="Number of days until access expires (optional)",
  )

  # Revoke access command
  revoke_parser = subparsers.add_parser(
    "revoke", help="Revoke repository access from a user"
  )
  revoke_parser.add_argument("user_id", help="User ID to revoke access from")
  revoke_parser.add_argument(
    "repository", help="Repository name (sec, industry, economic, etc.)"
  )

  # List access command
  list_parser = subparsers.add_parser("list", help="List repository access records")
  list_parser.add_argument(
    "--repository",
    help="Filter by specific repository (optional)",
    choices=list(SUPPORTED_REPOSITORIES.keys()),
  )

  # Check access command
  check_parser = subparsers.add_parser(
    "check", help="Check repository access for a specific user"
  )
  check_parser.add_argument("user_id", help="User ID to check access for")
  check_parser.add_argument(
    "--repository",
    help="Check access for specific repository (optional)",
    choices=list(SUPPORTED_REPOSITORIES.keys()),
  )

  # List repositories command
  subparsers.add_parser("repositories", help="List all available repositories")

  args = parser.parse_args()

  if not args.command:
    parser.print_help()
    sys.exit(1)

  if args.command == "grant":
    grant_access(args.user_id, args.repository, args.access_level, args.expires_days)
  elif args.command == "revoke":
    revoke_access(args.user_id, args.repository)
  elif args.command == "list":
    list_access(args.repository)
  elif args.command == "check":
    check_access(args.user_id, args.repository)
  elif args.command == "repositories":
    list_repositories()


if __name__ == "__main__":
  main()
