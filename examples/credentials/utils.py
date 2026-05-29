#!/usr/bin/env python3
"""
Shared credential utilities for demo scripts.

Provides helpers to create or reuse demo users so that multiple demos can share
the same RoboSystems account and API key.

Config I/O helpers (``load_credentials``, ``save_credentials``,
``get_graph_id``, ``save_graph_id``, ``get_user_id``) live in
``examples/_common/config.py`` — import them from there.
"""

from __future__ import annotations

import secrets
import string
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

from robosystems_client import AuthenticatedClient, Client
from robosystems_client.api.auth.login_user import sync_detailed as login
from robosystems_client.api.auth.register_user import sync_detailed as register
from robosystems_client.api.subscriptions.create_repository_subscription import (
  sync_detailed as subscribe_repository,
)
from robosystems_client.api.user.create_user_api_key import (
  sync_detailed as create_api_key,
)
from robosystems_client.models.create_api_key_request import CreateAPIKeyRequest
from robosystems_client.models.create_repository_subscription_request import (
  CreateRepositorySubscriptionRequest,
)
from robosystems_client.models.login_request import LoginRequest
from robosystems_client.models.register_request import RegisterRequest

from examples._common.config import load_credentials, save_credentials


@dataclass
class CredentialContext:
  """Data required to provision (or reuse) demo credentials."""

  base_url: str
  credentials_path: Path
  force: bool = False
  default_name_prefix: str = "Demo User"
  default_email_prefix: str = "demo_user"
  api_key_prefix: str = "Demo API Key"
  display_title: str = "Demo - User Setup"


def generate_secure_password(length: int = 16) -> str:
  """Generate a cryptographically secure password."""
  if length < 4:
    raise ValueError("Password length must be at least 4 characters.")

  chars_per_type = length // 4
  password = (
    "".join(secrets.choice(string.ascii_lowercase) for _ in range(chars_per_type))
    + "".join(secrets.choice(string.ascii_uppercase) for _ in range(chars_per_type))
    + "".join(secrets.choice(string.digits) for _ in range(chars_per_type))
    + "".join(secrets.choice("!@#$%^&*") for _ in range(chars_per_type))
  )
  password_list = list(password)
  secrets.SystemRandom().shuffle(password_list)
  return "".join(password_list)


def ensure_user_credentials(
  context: CredentialContext,
  name: Optional[str] = None,
  email: Optional[str] = None,
  password: Optional[str] = None,
) -> Dict[str, Any]:
  """
  Create or reuse a demo user and API key.

  Returns the credential dictionary containing user metadata and API key.
  """
  existing = load_credentials(context.credentials_path)
  if existing and not context.force:
    print("\n⚠️  Reusing existing credentials")
    user_id = existing.get("user_id") or existing.get("user", {}).get("id")
    if user_id:
      print(f"   User ID: {user_id}")
    print(f"   User:  {existing.get('user', {}).get('name')}")
    print(f"   Email: {existing.get('user', {}).get('email')}")
    print(f"   API Key: {existing.get('api_key', '')[:20]}...")
    return existing

  if existing and context.force:
    num_graphs = len(existing.get("graphs", {}))
    if num_graphs > 0:
      print("\n⚠️  WARNING: Creating a new user will reset ALL demos!")
      print(f"   This will delete {num_graphs} existing graph(s):")
      for demo_name, graph_data in existing.get("graphs", {}).items():
        print(f"     - {demo_name}: {graph_data.get('graph_id', 'unknown')}")
      print(
        "   The old graphs belong to the old user and won't be accessible with the new API key."
      )
      print()

  client = Client(base_url=context.base_url)

  timestamp = int(time.time())
  user_name = name or f"{context.default_name_prefix} {timestamp}"
  user_email = email or f"{context.default_email_prefix}_{timestamp}@example.com"
  user_password = password or generate_secure_password()

  print("\n" + "=" * 70)
  print(f"📊 {context.display_title}")
  print("=" * 70)
  print("\n📧 Creating user account...")
  print(f"   Name: {user_name}")
  print(f"   Email: {user_email}")

  register_request = RegisterRequest(
    name=user_name, email=user_email, password=user_password
  )
  register_response = register(client=client, body=register_request)
  if not register_response.parsed:
    print(f"\n❌ Failed to create user: {register_response.status_code}")
    if hasattr(register_response, "content"):
      print(f"   Response: {register_response.content}")
    sys.exit(1)

  user_id = register_response.parsed.user["id"]
  print(f"✅ User created: {user_name} ({user_email})")
  print(f"   User ID: {user_id}")

  print("\n🔑 Logging in...")
  login_request = LoginRequest(email=user_email, password=user_password)
  login_response = login(client=client, body=login_request)
  if not login_response.parsed:
    print(f"\n❌ Failed to login: {login_response.status_code}")
    sys.exit(1)

  token = login_response.parsed.token
  print("✅ Login successful")

  api_key_name = f"{context.api_key_prefix} - {user_name}"
  print("\n🔑 Creating API key...")

  # Create authenticated client with JWT token
  auth_client = AuthenticatedClient(base_url=context.base_url, token=token)

  api_key_request = CreateAPIKeyRequest(name=api_key_name)
  api_key_response = create_api_key(client=auth_client, body=api_key_request)
  if not api_key_response.parsed:
    print(f"\n❌ Failed to create API key: {api_key_response.status_code}")
    sys.exit(1)

  api_key = api_key_response.parsed.key
  print(f"✅ API key created: {api_key[:20]}...")

  credentials = {
    "user": {"id": user_id, "name": user_name, "email": user_email},
    "user_id": user_id,
    "email": user_email,
    "password": user_password,
    "api_key": api_key,
    "base_url": context.base_url,
    "created_at": time.strftime("%Y-%m-%d %H:%M:%S"),
    "graphs": {},
  }

  save_credentials(context.credentials_path, credentials)

  print("\n" + "=" * 70)
  print("✅ Setup Complete!")
  print("=" * 70)
  print(f"\nUser ID: {user_id}")
  print(f"Name: {user_name}")
  print(f"Email: {user_email}")
  print(f"API Key: {api_key[:20]}...")
  print("=" * 70 + "\n")

  return credentials


def grant_repository_access(
  auth_client: AuthenticatedClient,
  repository_type: str,
  repository_plan: str = "starter",
  credentials_path: Optional[Path] = None,
) -> bool:
  """
  Grant shared repository access to the authenticated user.

  Args:
      auth_client: Authenticated client with JWT token
      repository_type: Type of repository (sec, industry, economic, etc.)
      repository_plan: Plan tier (starter, advanced, etc.)
      credentials_path: Path to credentials file to save graph entry

  Returns:
      True if successful, False otherwise
  """
  print(
    f"\n🔄 Granting {repository_type.upper()} repository access ({repository_plan} tier)..."
  )

  plan_name = f"{repository_type}-{repository_plan}"
  request = CreateRepositorySubscriptionRequest(
    plan_name=plan_name,
  )

  response = subscribe_repository(
    client=auth_client, graph_id=repository_type, body=request
  )

  if response.status_code in (200, 201):
    print(f"✅ {repository_type.upper()} repository access granted successfully")

    # Save repository as a graph in credentials config
    if credentials_path:
      credentials = load_credentials(credentials_path)
      if credentials:
        if "graphs" not in credentials:
          credentials["graphs"] = {}
        credentials["graphs"][repository_type] = {
          "graph_id": repository_type,
          "graph_created_at": time.strftime("%Y-%m-%d %H:%M:%S"),
          "repository_type": "shared",
          "description": f"{repository_type.upper()} Shared Repository",
        }
        save_credentials(credentials_path, credentials)

    return True
  else:
    print(
      f"⚠️  Failed to grant {repository_type.upper()} repository access: {response.status_code}"
    )
    if hasattr(response, "content"):
      print(f"   Response: {response.content}")
    return False
