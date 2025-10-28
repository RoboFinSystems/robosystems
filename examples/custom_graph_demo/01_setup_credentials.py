#!/usr/bin/env python3
"""
Setup user and API key for the Custom Graph Demo.

This script creates a user account and API key, then saves the credentials
to a local file for reuse. Run this once to set up authentication.

Usage:
    uv run 01_setup_credentials.py                    # Auto-generate credentials
    uv run 01_setup_credentials.py --name "John Doe"  # Specify user name
    uv run 01_setup_credentials.py --email custom@example.com

After running, credentials are saved to credentials/config.json
"""

import argparse
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
  sys.path.insert(0, str(PROJECT_ROOT))

from examples.credentials.utils import (
  CredentialContext,
  ensure_user_credentials,
)


CREDENTIALS_DIR = Path(__file__).resolve().parents[1] / "credentials"
DEFAULT_CREDENTIALS_FILE = CREDENTIALS_DIR / "config.json"


def main():
  parser = argparse.ArgumentParser(
    description="Setup user and API key for the Custom Graph Demo"
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
  parser.add_argument(
    "--credentials-file",
    default=str(DEFAULT_CREDENTIALS_FILE),
    help="Path to the credentials file to use (default: credentials/config.json)",
  )

  args = parser.parse_args()

  credentials_path = Path(args.credentials_file).expanduser()

  try:
    context = CredentialContext(
      base_url=args.base_url,
      credentials_path=credentials_path,
      force=args.force,
      default_name_prefix="Custom Graph Demo User",
      default_email_prefix="custom_graph_demo",
      api_key_prefix="Custom Graph Demo API Key",
      display_title="Custom Graph Demo - User Setup",
    )
    credentials = ensure_user_credentials(
      context=context,
      name=args.name,
      email=args.email,
      password=args.password,
    )

    if credentials_path == DEFAULT_CREDENTIALS_FILE:
      print("   You can now run: uv run 02_create_graph.py")
    else:
      print(
        f"\nℹ️  Remember to pass --credentials-file {credentials_path} to other demo scripts"
      )
  except Exception as e:
    print(f"\n❌ Error: {e}")
    import traceback

    traceback.print_exc()
    sys.exit(1)


if __name__ == "__main__":
  main()
