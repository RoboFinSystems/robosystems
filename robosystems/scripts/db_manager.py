#!/usr/bin/env python3
"""Database management utility."""

import argparse
import bcrypt

from robosystems.models.iam import User, UserAPIKey, UserGraph
from robosystems.database import session, engine
from robosystems.logger import logger


def list_users():
  """List all users in the database."""
  try:
    users = User.get_all(session)

    if not users:
      logger.info("No users found in the database")
      return

    logger.info(f"Found {len(users)} users:")
    for user in users:
      logger.info(f"  - {user.email} ({user.name}) - Active: {user.is_active}")

      # Show API keys
      api_keys = UserAPIKey.get_by_user_id(user.id, session)
      if api_keys:
        logger.info(f"    API Keys: {len(api_keys)}")
        for key in api_keys:
          status = "Active" if key.is_active else "Inactive"
          logger.info(f"      - {key.name} ({key.prefix}...) - {status}")

      # Show graph access
      user_graphs = UserGraph.get_by_user_id(user.id, session)
      if user_graphs:
        logger.info(f"    Graph Access: {len(user_graphs)}")
        for ug in user_graphs:
          selected = "Selected" if ug.is_selected else ""
          logger.info(f"      - {ug.graph_name} ({ug.role}) {selected}")

      logger.info("")

  except Exception as e:
    logger.error(f"❌ Error listing users: {e}")
    raise


def create_user(email: str, name: str, password: str):
  """Create a new user."""
  try:
    # Check if user already exists
    existing_user = User.get_by_email(email, session)
    if existing_user:
      logger.error(f"❌ User with email {email} already exists")
      return False

    # Create user
    password_hash = bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode(
      "utf-8"
    )

    user = User.create(
      email=email, name=name, password_hash=password_hash, session=session
    )

    logger.info(f"✅ Created user: {user.email}")
    return True

  except Exception as e:
    logger.error(f"❌ Error creating user: {e}")
    raise


def create_api_key(email: str, key_name: str, description: str = None):
  """Create an API key for a user."""
  try:
    # Find user
    user = User.get_by_email(email, session)
    if not user:
      logger.error(f"❌ User with email {email} not found")
      return False

    # Create API key
    api_key, plain_key = UserAPIKey.create(
      user_id=user.id, name=key_name, description=description, session=session
    )

    logger.info(f"✅ Created API key for {email}:")
    logger.info(f"   Name: {api_key.name}")
    logger.info(f"   Key: {plain_key}")
    logger.info(f"   Prefix: {api_key.prefix}")

    return True

  except Exception as e:
    logger.error(f"❌ Error creating API key: {e}")
    raise


def grant_graph_access(
  email: str, graph_id: str, graph_name: str, role: str = "member"
):
  """Grant a user access to a graph."""
  try:
    # Find user
    user = User.get_by_email(email, session)
    if not user:
      logger.error(f"❌ User with email {email} not found")
      return False

    # Check if access already exists
    existing_access = UserGraph.get_by_user_and_graph(user.id, graph_id, session)
    if existing_access:
      logger.error(f"❌ User already has access to graph {graph_id}")
      return False

    # Create graph access
    UserGraph.create(
      user_id=user.id,
      graph_id=graph_id,
      role=role,
      is_selected=False,
      session=session,
    )

    logger.info(f"✅ Granted {role} access to {graph_name} for {email}")
    return True

  except Exception as e:
    logger.error(f"❌ Error granting graph access: {e}")
    raise


def show_database_info():
  """Show database connection and table information."""
  try:
    logger.info("📊 Database Information:")
    logger.info(f"   Engine: {engine.url}")

    # Count records in each table
    user_count = session.query(User).count()
    api_key_count = session.query(UserAPIKey).count()
    user_graph_count = session.query(UserGraph).count()

    logger.info(f"   Users: {user_count}")
    logger.info(f"   API Keys: {api_key_count}")
    logger.info(f"   User-Graph relationships: {user_graph_count}")

  except Exception as e:
    logger.error(f"❌ Error getting database info: {e}")
    raise


if __name__ == "__main__":
  import argparse

  parser = argparse.ArgumentParser(description="Database management utility")
  subparsers = parser.add_subparsers(dest="command", help="Available commands")

  # List users
  subparsers.add_parser("list-users", help="List all users")

  # Database info
  subparsers.add_parser("info", help="Show database information")

  # Create user
  create_user_parser = subparsers.add_parser("create-user", help="Create a new user")
  create_user_parser.add_argument("email", help="User email")
  create_user_parser.add_argument("name", help="User name")
  create_user_parser.add_argument("password", help="User password")

  # Create API key
  create_key_parser = subparsers.add_parser("create-key", help="Create an API key")
  create_key_parser.add_argument("email", help="User email")
  create_key_parser.add_argument("name", help="API key name")
  create_key_parser.add_argument("--description", help="API key description")

  # Grant graph access
  grant_access_parser = subparsers.add_parser("grant-access", help="Grant graph access")
  grant_access_parser.add_argument("email", help="User email")
  grant_access_parser.add_argument("graph_id", help="Graph ID")
  grant_access_parser.add_argument("graph_name", help="Graph name")
  grant_access_parser.add_argument(
    "--role", default="member", help="User role (admin, member, viewer)"
  )

  args = parser.parse_args()

  if args.command == "list-users":
    list_users()
  elif args.command == "info":
    show_database_info()
  elif args.command == "create-user":
    create_user(args.email, args.name, args.password)
  elif args.command == "create-key":
    create_api_key(args.email, args.name, args.description)
  elif args.command == "grant-access":
    grant_graph_access(args.email, args.graph_id, args.graph_name, args.role)
  else:
    parser.print_help()
