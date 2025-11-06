#!/usr/bin/env python3
"""
Create a test user account for automated testing and development.

This script creates a test user with an API key for programmatic access.
Perfect for Puppeteer automation (frontend login) and API testing.
"""

import argparse
import json
import random
import string
import sys
import time
from typing import Dict, Any, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from robosystems.config.billing.repositories import SharedRepository

# Default configuration
DEFAULT_BASE_URL = "http://localhost:8000"
DEFAULT_PASSWORD = "RoboSys$2024#Secure&Test!"


def generate_test_email() -> str:
  """Generate a unique test email address."""
  timestamp = int(time.time())
  random_suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=6))
  return f"test-user-{timestamp}-{random_suffix}@robosystems.dev"


def create_requests_session() -> requests.Session:
  """Create a requests session with retry strategy."""
  session = requests.Session()

  # Configure retry strategy
  retry_strategy = Retry(
    total=3,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["HEAD", "GET", "OPTIONS", "POST"],  # Updated parameter name
    backoff_factor=1,
  )

  adapter = HTTPAdapter(max_retries=retry_strategy)
  session.mount("http://", adapter)
  session.mount("https://", adapter)

  # Set reasonable timeout
  session.timeout = 30

  return session


def register_user(
  session: requests.Session, base_url: str, email: str, password: str
) -> Dict[str, Any]:
  """Register a new user account."""
  url = f"{base_url}/v1/auth/register"

  payload = {"name": "Test User", "email": email, "password": password}

  print(f"🔄 Registering user: {email}")

  try:
    response = session.post(url, json=payload)
    response.raise_for_status()

    data = response.json()
    print("✅ User registered successfully")
    return data

  except requests.exceptions.RequestException as e:
    print(f"❌ Failed to register user: {e}")
    if hasattr(e, "response") and e.response is not None:
      print(f"   Response: {e.response.text}")
    raise


def login_user(
  session: requests.Session, base_url: str, email: str, password: str
) -> Dict[str, Any]:
  """Login and obtain JWT token."""
  url = f"{base_url}/v1/auth/login"

  payload = {"email": email, "password": password}

  print(f"🔄 Logging in user: {email}")

  try:
    response = session.post(url, json=payload)
    response.raise_for_status()

    data = response.json()

    # Extract JWT token from response or cookies
    jwt_token = data.get("token")  # API returns 'token' field, not 'access_token'
    if not jwt_token and response.cookies.get("auth-token"):
      jwt_token = response.cookies.get("auth-token")

    if jwt_token:
      print("✅ Login successful, JWT token obtained")
      # Set token for subsequent requests
      session.headers.update({"Authorization": f"Bearer {jwt_token}"})
      return {"jwt_token": jwt_token, "user_data": data}
    else:
      print("⚠️  Login successful but no JWT token found in response")
      return {"user_data": data}

  except requests.exceptions.RequestException as e:
    print(f"❌ Failed to login: {e}")
    if hasattr(e, "response") and e.response is not None:
      print(f"   Response: {e.response.text}")
    raise


def create_api_key(session: requests.Session, base_url: str) -> Dict[str, Any]:
  """Create an API key for the user."""
  url = f"{base_url}/v1/user/api-keys"

  payload = {
    "name": "Test API Key",
    "description": "API key for automated testing and AI development",
  }

  print("🔄 Creating API key")

  try:
    response = session.post(url, json=payload)
    response.raise_for_status()

    data = response.json()
    print("✅ API key created successfully")
    return data

  except requests.exceptions.RequestException as e:
    print(f"❌ Failed to create API key: {e}")
    if hasattr(e, "response") and e.response is not None:
      print(f"   Response: {e.response.text}")
    raise


def get_user_profile(session: requests.Session, base_url: str) -> Dict[str, Any]:
  """Get user profile information."""
  url = f"{base_url}/v1/user"

  print("🔄 Fetching user profile")

  try:
    response = session.get(url)
    response.raise_for_status()

    data = response.json()
    print("✅ User profile retrieved")
    return data

  except requests.exceptions.RequestException as e:
    print(f"⚠️  Failed to get user profile: {e}")
    return {}


def grant_repository_access(
  session: requests.Session, base_url: str, user_id: str, repository_type: str
) -> bool:
  """Grant shared repository access to the user with highest tier (admin)."""
  url = f"{base_url}/v1/user/subscriptions/shared-repositories/subscribe"

  payload = {
    "repository_type": repository_type,
    "repository_plan": "unlimited",  # Highest tier plan
  }

  print(f"🔄 Granting {repository_type.upper()} repository access (admin level)")

  try:
    response = session.post(url, json=payload)
    response.raise_for_status()

    print(f"✅ {repository_type.upper()} repository access granted successfully")
    return True

  except requests.exceptions.RequestException as e:
    print(f"⚠️  Failed to grant {repository_type.upper()} repository access: {e}")
    if hasattr(e, "response") and e.response is not None:
      print(f"   Response: {e.response.text}")
    return False


def check_api_connectivity(session: requests.Session, base_url: str) -> bool:
  """Check if the API is accessible."""
  url = f"{base_url}/v1/status"

  print(f"🔄 Checking API connectivity: {base_url}")

  try:
    response = session.get(url, timeout=10)
    response.raise_for_status()
    print("✅ API is accessible")
    return True

  except requests.exceptions.RequestException as e:
    print(f"❌ API not accessible: {e}")
    print(f"   Make sure the API server is running at {base_url}")
    return False


def output_credentials(
  email: str,
  password: str,
  jwt_token: Optional[str],
  api_key_data: Optional[Dict],
  user_data: Dict,
  base_url: str,
  output_format: str = "pretty",
  save_file: bool = False,
  granted_repositories: list = None,
):
  """Output the credentials in a formatted way."""
  granted_repositories = granted_repositories or []

  # Extract the actual API key value
  api_key_value = None
  if api_key_data:
    # Handle both formats - direct key or nested object
    if isinstance(api_key_data, dict):
      api_key_value = api_key_data.get("key") or api_key_data.get("api_key")
    else:
      api_key_value = api_key_data

  # Derive frontend URL from API base URL
  frontend_url = base_url.replace(":8000", ":3000")

  print("\n" + "=" * 60)
  print("🎉 TEST USER CREATED")
  print("=" * 60)

  print("\n📧 Credentials:")
  print(f"   Email:    {email}")
  print(f"   Password: {password}")

  if api_key_value:
    print("\n🔐 API Key (use for programmatic access):")
    print(f"   {api_key_value}")
    print("\n   Example:")
    print(f'   curl -H "X-API-Key: {api_key_value}" {base_url}/v1/user')

  if granted_repositories:
    print("\n📚 Shared Repository Access:")
    for repo in granted_repositories:
      print(f"   ✅ {repo.upper()}")

  print("\n🌐 URLs:")
  print(f"   Frontend Login: {frontend_url}/login")
  print(f"   API Base:       {base_url}")
  print(f"   OpenAPI Spec:   {base_url}/openapi.json")

  print("\n🎭 Puppeteer Login:")
  print(f"   await page.goto('{frontend_url}/login');")
  print(f"   await page.type('#email', '{email}');")
  print(f"   await page.type('#password', '{password}');")
  print("   await page.click('button[type=\"submit\"]');")

  # Only save to file if explicitly requested
  if save_file:
    # Create credentials object for file saving
    credentials = {
      "email": email,
      "password": password,
      "base_url": base_url,
      "frontend_url": frontend_url,
      "jwt_token": jwt_token,
      "api_key": api_key_value,
      "user_id": user_data.get("user", {}).get("id"),
      "user_name": user_data.get("user", {}).get("name"),
      "granted_repositories": granted_repositories,
      "created_at": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
    }

    # Save to a git-ignored location (local/ directory)
    import os

    # Use local/ directory which should be git-ignored
    local_dir = os.path.join(os.path.dirname(__file__), "..", "..", "local")
    os.makedirs(local_dir, exist_ok=True)

    # Use user_id in filename to avoid overwriting previous test users
    user_id = credentials.get("user_id", "unknown")
    credentials_file = os.path.join(local_dir, f"{user_id}_credentials.json")
    try:
      with open(credentials_file, "w") as f:
        json.dump(credentials, f, indent=2)
      print(f"\n💾 Credentials saved to: {credentials_file}")
    except Exception as e:
      print(f"\n⚠️  Could not save credentials file: {e}")

  print("\n" + "=" * 60)


def main():
  """Main function to create test user."""
  parser = argparse.ArgumentParser(description="Create a test user for AI testing")
  parser.add_argument(
    "--base-url",
    default=DEFAULT_BASE_URL,
    help=f"API base URL (default: {DEFAULT_BASE_URL})",
  )
  parser.add_argument("--email", help="Email address (default: auto-generated)")
  parser.add_argument(
    "--password",
    default=DEFAULT_PASSWORD,
    help="Password (default: strong auto-generated password)",
  )
  parser.add_argument(
    "--name", default="Test User", help="User display name (default: Test User)"
  )
  parser.add_argument(
    "--skip-api-key", action="store_true", help="Skip API key creation"
  )
  parser.add_argument(
    "--output-format",
    choices=["pretty", "json"],
    default="pretty",
    help="Output format (default: pretty)",
  )
  parser.add_argument(
    "--json",
    action="store_true",
    help="Output credentials in JSON format (shorthand for --output-format json)",
  )
  parser.add_argument(
    "--save-file",
    action="store_true",
    help="Save credentials to local/test_credentials.json file for programmatic use",
  )
  parser.add_argument(
    "--with-sec-access",
    action="store_true",
    help="Grant admin access to SEC shared repository (deprecated: use --modes sec)",
  )
  parser.add_argument(
    "--repositories",
    nargs="+",
    help=f"List of repositories to grant access to (e.g., {' '.join([r.value for r in SharedRepository])})",
  )

  # Build modes help text dynamically from SharedRepository enum
  repo_list = ",".join([r.value for r in SharedRepository])
  parser.add_argument(
    "--modes",
    help=f"Comma-separated list of modes: json,file,{repo_list} (e.g., 'json,sec,industry' for JSON output with SEC and Industry access)",
  )

  args = parser.parse_args()

  # Initialize repository list
  repositories_to_grant = []

  # Get supported repositories from enum
  supported_repos = [r.value for r in SharedRepository]

  # Process comma-separated modes if provided
  if args.modes:
    modes = [mode.strip().lower() for mode in args.modes.split(",")]
    if "json" in modes:
      args.json = True
    if "file" in modes:
      args.save_file = True

    # Check for repository types in modes using SharedRepository enum
    for repo in supported_repos:
      if repo in modes:
        repositories_to_grant.append(repo)

  # Handle --repositories flag
  if args.repositories:
    repositories_to_grant.extend([repo.lower() for repo in args.repositories])

  # Handle legacy --with-sec-access flag
  if args.with_sec_access and "sec" not in repositories_to_grant:
    repositories_to_grant.append("sec")

  # Remove duplicates while preserving order
  repositories_to_grant = list(dict.fromkeys(repositories_to_grant))

  # Validate repository names
  invalid_repos = [r for r in repositories_to_grant if r not in supported_repos]
  if invalid_repos:
    print(f"❌ Invalid repository names: {', '.join(invalid_repos)}")
    print(f"   Supported: {', '.join(supported_repos)}")
    sys.exit(1)

  # Generate email if not provided
  email = args.email or generate_test_email()

  # Create session
  session = create_requests_session()

  try:
    # Check API connectivity
    if not check_api_connectivity(session, args.base_url):
      sys.exit(1)

    # Register user
    registration_data = register_user(session, args.base_url, email, args.password)

    # Login to get JWT token
    login_data = login_user(session, args.base_url, email, args.password)
    jwt_token = login_data.get("jwt_token")

    # Create API key (if not skipped)
    api_key_data = None
    if not args.skip_api_key:
      try:
        api_key_data = create_api_key(session, args.base_url)
      except Exception as e:
        print(f"⚠️  Could not create API key: {e}")

    # Get user profile for additional info
    user_profile = get_user_profile(session, args.base_url)

    # Combine user data
    user_data = {**registration_data, **login_data.get("user_data", {}), **user_profile}

    # Grant repository access if requested
    granted_repositories = []
    if repositories_to_grant:
      # Extract user ID from user data
      user_id = (
        user_data.get("user", {}).get("id")
        or user_data.get("id")
        or user_profile.get("id")
      )

      if user_id:
        for repo_type in repositories_to_grant:
          if grant_repository_access(session, args.base_url, user_id, repo_type):
            granted_repositories.append(repo_type)
      else:
        print("⚠️  Could not extract user ID for repository access")

    # Extract the actual API key value
    api_key_value = None
    if api_key_data:
      if isinstance(api_key_data, dict):
        api_key_value = api_key_data.get("key") or api_key_data.get("api_key")
      else:
        api_key_value = api_key_data

    # Override output format if --json flag is used
    output_format = "json" if args.json else args.output_format

    # Output credentials
    if output_format == "json":
      frontend_url = args.base_url.replace(":8000", ":3000")
      credentials = {
        "email": email,
        "password": args.password,
        "api_key": api_key_value,
        "user_id": user_data.get("user", {}).get("id"),
        "granted_repositories": granted_repositories,
        "base_url": args.base_url,
        "frontend_url": frontend_url,
        "urls": {
          "frontend_login": f"{frontend_url}/login",
          "api_base": args.base_url,
          "openapi_spec": f"{args.base_url}/openapi.json",
        },
        "puppeteer": {
          "url": f"{frontend_url}/login",
          "selectors": {
            "email": "#email",
            "password": "#password",
            "submit": 'button[type="submit"]',
          },
        },
        "api_example": f'curl -H "X-API-Key: {api_key_value}" {args.base_url}/v1/user',
        "created_at": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()),
      }
      print(json.dumps(credentials, indent=2))
    else:
      output_credentials(
        email,
        args.password,
        jwt_token,
        api_key_data,
        user_data,
        args.base_url,
        output_format,
        args.save_file,
        granted_repositories,
      )

  except KeyboardInterrupt:
    print("\n❌ Operation cancelled by user")
    sys.exit(1)
  except Exception as e:
    print(f"\n❌ Unexpected error: {e}")
    sys.exit(1)


if __name__ == "__main__":
  main()
