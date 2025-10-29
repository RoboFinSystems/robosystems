#!/usr/bin/env python3
"""
Shared credential utilities for demo scripts.

Provides helpers to create or reuse demo users so that multiple demos can share
the same RoboSystems account and API key.
"""

from __future__ import annotations

import json
import secrets
import string
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Dict, Any

from robosystems_client import Client, AuthenticatedClient
from robosystems_client.api.auth.login_user import sync_detailed as login
from robosystems_client.api.auth.register_user import sync_detailed as register
from robosystems_client.api.user.create_user_api_key import (
  sync_detailed as create_api_key,
)
from robosystems_client.models.create_api_key_request import CreateAPIKeyRequest
from robosystems_client.models.login_request import LoginRequest
from robosystems_client.models.register_request import RegisterRequest


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


def load_credentials(path: Path) -> Optional[Dict[str, Any]]:
  """Load credentials if they exist."""
  if path.exists():
    with path.open() as fh:
      return json.load(fh)
  return None


def save_credentials(path: Path, data: Dict[str, Any]) -> None:
  """Persist credential data."""
  path.parent.mkdir(parents=True, exist_ok=True)
  with path.open("w") as fh:
    json.dump(data, fh, indent=2)
  print(f"\n💾 Credentials saved to: {path}")


def get_user_id(path: Path) -> Optional[str]:
  """Get user_id from credentials."""
  credentials = load_credentials(path)
  if not credentials:
    return None
  return credentials.get("user_id") or credentials.get("user", {}).get("id")


def get_graph_id(path: Path, demo_name: str) -> Optional[str]:
  """Get graph_id for a specific demo from credentials."""
  credentials = load_credentials(path)
  if not credentials:
    return None
  graphs = credentials.get("graphs", {})
  demo_data = graphs.get(demo_name, {})
  return demo_data.get("graph_id")


def save_graph_id(
  path: Path, demo_name: str, graph_id: str, graph_created_at: str
) -> None:
  """Save graph_id for a specific demo to credentials."""
  credentials = load_credentials(path)
  if not credentials:
    raise ValueError(f"No credentials found at {path}")

  if "graphs" not in credentials:
    credentials["graphs"] = {}

  credentials["graphs"][demo_name] = {
    "graph_id": graph_id,
    "graph_created_at": graph_created_at,
  }

  save_credentials(path, credentials)


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
    user_id = existing.get('user_id') or existing.get('user', {}).get('id')
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
  print(f"   Password: {user_password}")

  register_request = RegisterRequest(
    name=user_name, email=user_email, password=user_password
  )
  register_response = register(client=client, body=register_request)
  if not register_response.parsed:
    print(f"\n❌ Failed to create user: {register_response.status_code}")
    if hasattr(register_response, "content"):
      print(f"   Response: {register_response.content}")
    sys.exit(1)

  user_id = register_response.parsed.user.id
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
  print(f"API Key: {api_key}")
  print("=" * 70 + "\n")

  return credentials
