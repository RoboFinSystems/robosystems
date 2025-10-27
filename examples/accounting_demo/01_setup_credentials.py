#!/usr/bin/env python3
"""
Setup User and API Key for Accounting Demo

This script creates a user account and API key, then saves the credentials
to a local file for reuse. Run this once to set up authentication.

Usage:
    uv run 01_setup_credentials.py                    # Auto-generate credentials
    uv run 01_setup_credentials.py --name "John Doe"  # Specify user name
    uv run 01_setup_credentials.py --email custom@example.com

After running, credentials are saved to credentials/config.json
"""

import argparse
import json
import secrets
import string
import sys
import time
from pathlib import Path

from robosystems_client import Client
from robosystems_client.api.auth.register_user import sync_detailed as register
from robosystems_client.api.auth.login_user import sync_detailed as login
from robosystems_client.api.user.create_user_api_key import (
  sync_detailed as create_api_key,
)
from robosystems_client.models.register_request import RegisterRequest
from robosystems_client.models.login_request import LoginRequest
from robosystems_client.models.create_api_key_request import CreateAPIKeyRequest


CREDENTIALS_DIR = Path(__file__).parent / "credentials"
CREDENTIALS_FILE = CREDENTIALS_DIR / "config.json"


def generate_secure_password(length: int = 16) -> str:
  """Generate a cryptographically secure password."""
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


def load_credentials():
  """Load existing credentials if available."""
  if CREDENTIALS_FILE.exists():
    with open(CREDENTIALS_FILE) as f:
      return json.load(f)
  return None


def save_credentials(credentials: dict):
  """Save credentials to file."""
  CREDENTIALS_DIR.mkdir(exist_ok=True)
  with open(CREDENTIALS_FILE, "w") as f:
    json.dump(credentials, f, indent=2)
  print(f"\n💾 Credentials saved to: {CREDENTIALS_FILE}")


def setup_user(
  base_url: str,
  name: str = None,
  email: str = None,
  password: str = None,
  force: bool = False,
):
  """Create user and API key, save credentials."""

  if not force:
    existing = load_credentials()
    if existing:
      print("\n⚠️  Credentials already exist!")
      print(f"   User: {existing.get('user', {}).get('name')}")
      print(f"   Email: {existing.get('user', {}).get('email')}")
      print(f"   API Key: {existing.get('api_key', '')[:20]}...")
      print(
        "\nUse --force to create new credentials (this won't delete the old account)"
      )
      return existing

  client = Client(base_url=base_url)

  timestamp = int(time.time())
  name = name or f"Accounting Demo User {timestamp}"
  email = email or f"accounting_demo_{timestamp}@example.com"
  password = password or generate_secure_password()

  print("\n" + "=" * 70)
  print("📊 Accounting Demo - User Setup")
  print("=" * 70)
  print("\n📧 Creating user account...")
  print(f"   Name: {name}")
  print(f"   Email: {email}")
  print(f"   Password: {password}")

  user_create = RegisterRequest(name=name, email=email, password=password)
  response = register(client=client, body=user_create)

  if not response.parsed:
    print(f"\n❌ Failed to create user: {response.status_code}")
    if hasattr(response, "content"):
      print(f"   Response: {response.content}")
    sys.exit(1)

  print(f"✅ User created: {name} ({email})")

  print("\n🔑 Logging in...")
  user_login = LoginRequest(email=email, password=password)
  response = login(client=client, body=user_login)

  if not response.parsed:
    print(f"\n❌ Failed to login: {response.status_code}")
    sys.exit(1)

  token = response.parsed.token
  print("✅ Login successful")

  print("\n🔑 Creating API key...")
  api_key_create = CreateAPIKeyRequest(name=f"Accounting Demo API Key - {name}")
  response = create_api_key(client=client, token=token, body=api_key_create)

  if not response.parsed:
    print(f"\n❌ Failed to create API key: {response.status_code}")
    sys.exit(1)

  api_key = response.parsed.key
  print(f"✅ API key created: {api_key[:20]}...")

  credentials = {
    "user": {"name": name, "email": email},
    "email": email,
    "password": password,
    "api_key": api_key,
    "base_url": base_url,
    "created_at": time.strftime("%Y-%m-%d %H:%M:%S"),
  }

  save_credentials(credentials)

  print("\n" + "=" * 70)
  print("✅ Setup Complete!")
  print("=" * 70)
  print(f"\nName: {name}")
  print(f"Email: {email}")
  print(f"API Key: {api_key}")
  print(f"\n💡 Credentials saved to: {CREDENTIALS_FILE}")
  print("   You can now run: uv run 02_create_graph.py")
  print("=" * 70 + "\n")

  return credentials


def main():
  parser = argparse.ArgumentParser(
    description="Setup user and API key for Accounting Demo"
  )
  parser.add_argument(
    "--base-url",
    default="http://localhost:8000",
    help="API base URL (default: http://localhost:8000)",
  )
  parser.add_argument(
    "--name",
    help="User name (auto-generated if not provided)",
  )
  parser.add_argument(
    "--email",
    help="User email (auto-generated if not provided)",
  )
  parser.add_argument(
    "--password",
    help="User password (auto-generated if not provided)",
  )
  parser.add_argument(
    "--force",
    action="store_true",
    help="Force create new credentials even if they exist",
  )

  args = parser.parse_args()

  try:
    setup_user(
      base_url=args.base_url,
      name=args.name,
      email=args.email,
      password=args.password,
      force=args.force,
    )
  except Exception as e:
    print(f"\n❌ Error: {e}")
    import traceback

    traceback.print_exc()
    sys.exit(1)


if __name__ == "__main__":
  main()
