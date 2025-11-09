#!/usr/bin/env python3
"""
Create or reuse demo user credentials for local development.

This script manages the shared examples/credentials/config.json file used by
all demo scripts. It creates a new user or reuses existing credentials.

Usage:
    just demo-user                                    # Create/reuse demo user
    just demo-user --repositories sec industry        # With repository access
    just demo-user --json                             # JSON output
"""

import argparse
import json
import sys
from pathlib import Path

from robosystems_client.client import AuthenticatedClient
from utils import ensure_user_credentials, grant_repository_access, CredentialContext

CREDENTIALS_FILE = Path(__file__).parent / "config.json"


def main():
  parser = argparse.ArgumentParser(
    description="Create or reuse demo user for local development"
  )
  parser.add_argument(
    "--base-url",
    default="http://localhost:8000",
    help="API base URL (default: http://localhost:8000)",
  )
  parser.add_argument("--email", help="Email address (default: auto-generated)")
  parser.add_argument(
    "--name", default="Demo User", help="User display name (default: Demo User)"
  )
  parser.add_argument(
    "--force",
    action="store_true",
    help="Force creation of new user (WARNING: resets all demo graphs)",
  )
  parser.add_argument(
    "--repositories",
    nargs="+",
    choices=["sec", "industry", "economic"],
    help="List of repositories to grant access to",
  )
  parser.add_argument(
    "--json",
    action="store_true",
    help="Output credentials in JSON format",
  )

  args = parser.parse_args()

  context = CredentialContext(
    base_url=args.base_url,
    credentials_path=CREDENTIALS_FILE,
    force=args.force,
    default_name_prefix="Demo User",
    default_email_prefix="demo_user",
    api_key_prefix="Demo API Key",
    display_title="Demo User Setup",
  )

  credentials = ensure_user_credentials(context, name=args.name, email=args.email)

  if args.repositories:
    print("\n📚 Granting repository access...")
    api_key = credentials.get("api_key")
    if not api_key:
      print("❌ No API key found in credentials")
      sys.exit(1)

    auth_client = AuthenticatedClient(
      base_url=args.base_url,
      token=api_key,
      prefix="",
      auth_header_name="X-API-Key",
    )

    granted = []
    for repo_type in args.repositories:
      if grant_repository_access(auth_client, repo_type):
        granted.append(repo_type)

    if granted:
      print(f"\n✅ Granted access to: {', '.join(granted).upper()}")

  if args.json:
    output = {
      "user_id": credentials.get("user_id"),
      "email": credentials.get("email"),
      "password": credentials.get("password"),
      "api_key": credentials.get("api_key"),
      "base_url": args.base_url,
      "granted_repositories": args.repositories or [],
    }
    print(json.dumps(output, indent=2))
  else:
    print("\n" + "=" * 70)
    print("🎯 Quick Start")
    print("=" * 70)
    print(f"\nFrontend Login: {args.base_url.replace(':8000', ':3000')}/login")
    print(f"  Email: {credentials['email']}")
    print(f"  Password: {credentials['password']}")
    print(f"\nAPI Key: {credentials['api_key']}")
    print("\nTest API Call:")
    print(f'  curl -H "X-API-Key: {credentials["api_key"]}" {args.base_url}/v1/user')
    print("=" * 70 + "\n")


if __name__ == "__main__":
  main()
