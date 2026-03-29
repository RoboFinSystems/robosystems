"""
AWS Secrets Manager integration for dynamic secret retrieval.

This module provides a centralized way to fetch secrets from AWS Secrets Manager
rather than relying on environment variables passed through userdata scripts.

## Architecture

Secrets are organized in AWS Secrets Manager with the following structure:
- Base secret: `robosystems/{environment}` (e.g., robosystems/prod, robosystems/staging)
  Contains:
    Encryption keys: JWT_SECRET_KEY, CONNECTION_CREDENTIALS_KEY, GRAPH_BACKUP_ENCRYPTION_KEY
    JWT/Auth: JWT_ISSUER, JWT_AUDIENCE
    Service URLs: ROBOSYSTEMS_URL, ROBOLEDGER_URL, ROBOINVESTOR_URL
    Email: EMAIL_FROM_ADDRESS, EMAIL_FROM_NAME
    External services: INTUIT_*, SEC_GOV_USER_AGENT, OPENFIGI_API_KEY,
      STRIPE_SECRET_KEY, STRIPE_PUBLISHABLE_KEY, STRIPE_WEBHOOK_SECRET,
      TURNSTILE_SECRET_KEY, TURNSTILE_SITE_KEY
    Note: STRIPE_API_VERSION is a constant in config/constants.py (not a secret)

- Extension secrets: `robosystems/{environment}/{type}`
  - `/postgres`: POSTGRES_USER, POSTGRES_PASSWORD
  - `/valkey`: VALKEY_AUTH_TOKEN
  - `/admin`: ADMIN_API_KEY
  - `/graph-api`: GRAPH_API_KEY

## Feature Flags (Moved to SSM Parameter Store)

Feature flags have been migrated to SSM Parameter Store for cost efficiency.
See parameter_store.py for the new implementation. Feature flags are stored at:
  /robosystems/{environment}/features/{FLAG_NAME}

## Usage

The module automatically detects the environment (dev/staging/prod) and:
- For prod/staging: Fetches secrets from AWS Secrets Manager with caching
- For dev: Returns empty dict, falling back to environment variables

Secrets are cached using two mechanisms:
1. LRU cache on the get_secret method (function-level)
2. Instance-level cache in _cache dict (cross-function)

Performance: ~256ms for first fetch, ~0.01ms for cached access

## Integration with env.py

The env.py module uses this for all sensitive configuration:
```python
try:
    from robosystems.config.secrets_manager import get_secret_value
    SECRET_VALUE = get_secret_value("SECRET_KEY", "default")
except ImportError:
    SECRET_VALUE = get_str_env("SECRET_KEY", "default")
```

This pattern ensures backward compatibility and graceful fallback.
"""

import json
import logging
import os
import time
from typing import Any

import boto3
from botocore.exceptions import ClientError

# Use standard logging to avoid circular import with robosystems.logger
logger = logging.getLogger(__name__)


class SecretsManager:
  """Manages retrieval of secrets from AWS Secrets Manager."""

  def __init__(
    self,
    environment: str | None = None,
    region: str | None = None,
    cache_ttl_seconds: int = 3600,
  ):
    """
    Initialize the secrets manager.

    Args:
        environment: Environment name (prod/staging). Defaults to ENVIRONMENT env var.
        region: AWS region. Defaults to AWS_REGION env var or us-east-1.
        cache_ttl_seconds: TTL for cached secrets in seconds. Default 1 hour.
    """
    self.environment = environment or os.getenv("ENVIRONMENT", "dev")
    self.region = region or os.getenv("AWS_REGION", "us-east-1")
    self.cache_ttl_seconds = cache_ttl_seconds

    # Initialize boto3 client
    self.client = boto3.client("secretsmanager", region_name=self.region)

    # Cache for retrieved secrets with timestamps
    # Format: {cache_key: (secret_data, timestamp)}
    self._cache: dict[str, tuple[dict[str, Any], float]] = {}

  def get_secret(self, secret_type: str | None = None) -> dict[str, Any]:
    """
    Retrieve a secret from AWS Secrets Manager with TTL-based caching.

    Args:
        secret_type: Optional type of secret (e.g., "s3", "postgres").
                    If None, retrieves the base environment secret.

    Returns:
        Dictionary containing secret values.
    """
    # Only use Secrets Manager for prod/staging
    if self.environment not in ["prod", "staging"]:
      return {}

    # Build cache key
    cache_key = f"{self.environment}/{secret_type}" if secret_type else self.environment

    # Check cache with TTL
    if cache_key in self._cache:
      secret_data, timestamp = self._cache[cache_key]
      if time.time() - timestamp < self.cache_ttl_seconds:
        return secret_data
      else:
        # Cache expired, remove it
        del self._cache[cache_key]
        logger.info(f"Cache expired for secret: {cache_key}")

    # Build secret ID
    if secret_type:
      secret_id = f"robosystems/{self.environment}/{secret_type}"
    else:
      secret_id = f"robosystems/{self.environment}"

    try:
      # Retrieve secret from AWS
      response = self.client.get_secret_value(SecretId=secret_id)

      # Parse the secret string
      if "SecretString" in response:
        # Special case: admin key is stored as raw string, not JSON
        if secret_type == "admin":
          secret_data = {"ADMIN_API_KEY": response["SecretString"]}
        else:
          secret_data = json.loads(response["SecretString"])
      else:
        # Handle binary secrets (not expected for our use case)
        raise ValueError(f"Binary secret not supported for {secret_id}")

      # Cache the result with timestamp
      self._cache[cache_key] = (secret_data, time.time())

      logger.info(f"Successfully retrieved secret: {secret_id}")
      return secret_data

    except ClientError as e:
      error_code = e.response.get("Error", {}).get("Code", "Unknown")

      if error_code == "ResourceNotFoundException":
        logger.warning(f"Secret not found: {secret_id}")
        # For missing secrets, return empty dict to allow fallback
        return {}
      elif error_code == "AccessDeniedException":
        logger.error(f"Access denied to secret: {secret_id}")
        # For access issues in prod/staging, this is critical
        if self.environment in ["prod", "staging"]:
          raise
        return {}
      else:
        logger.error(f"Error retrieving secret {secret_id}: {error_code}")
        # For other errors in prod/staging, raise to surface issues
        if self.environment in ["prod", "staging"]:
          raise
        return {}
    except Exception as e:
      logger.error(f"Unexpected error retrieving secret {secret_id}: {e}")
      # For unexpected errors in prod/staging, raise to surface issues
      if self.environment in ["prod", "staging"]:
        raise
      return {}

  def get_admin_key(self) -> str:
    """
    Get the admin API key from secrets.

    Returns:
        Admin API key string.
    """
    if self.environment not in ["prod", "staging"]:
      # For local dev, optionally use env var
      return os.getenv("ADMIN_API_KEY", "")

    secrets = self.get_secret("admin")
    return secrets.get("ADMIN_API_KEY", "")

  def refresh(self, secret_type: str | None = None):
    """
    Refresh cached secrets.

    Args:
        secret_type: Specific secret to refresh, or None to refresh all.
    """
    if secret_type:
      cache_key = f"{self.environment}/{secret_type}"
      self._cache.pop(cache_key, None)
    else:
      # Clear all caches
      self._cache.clear()


# Global instance for easy access
_secrets_manager: SecretsManager | None = None


def get_secrets_manager() -> SecretsManager:
  """
  Get or create the global secrets manager instance.

  Returns:
      SecretsManager instance.
  """
  global _secrets_manager
  if _secrets_manager is None:
    _secrets_manager = SecretsManager()
  return _secrets_manager


# Secret mapping configuration
# Organization mirrors env.py sections. Tuple format: (extension_secret_type, key_name)
# extension_secret_type=None means the key is in the base secret (robosystems/{env})
#
# NOTE: Feature flags have been moved to SSM Parameter Store (see parameter_store.py).
# Only actual secrets (credentials, API keys, encryption keys) remain here.
SECRET_MAPPINGS = {
  # --- Core: Encryption Keys ---
  "CONNECTION_CREDENTIALS_KEY": (None, "CONNECTION_CREDENTIALS_KEY"),
  "GRAPH_BACKUP_ENCRYPTION_KEY": (None, "GRAPH_BACKUP_ENCRYPTION_KEY"),
  # --- Core: JWT & Auth ---
  "JWT_SECRET_KEY": (None, "JWT_SECRET_KEY"),
  "JWT_ISSUER": (None, "JWT_ISSUER"),
  "JWT_AUDIENCE": (None, "JWT_AUDIENCE"),
  # --- Core: Email ---
  "EMAIL_FROM_ADDRESS": (None, "EMAIL_FROM_ADDRESS"),
  "EMAIL_FROM_NAME": (None, "EMAIL_FROM_NAME"),
  # --- Core: CAPTCHA ---
  "TURNSTILE_SECRET_KEY": (None, "TURNSTILE_SECRET_KEY"),
  "TURNSTILE_SITE_KEY": (None, "TURNSTILE_SITE_KEY"),
  # --- Graph Databases ---
  "GRAPH_API_KEY": ("graph-api", "GRAPH_API_KEY"),
  # --- PostgreSQL ---
  # ECS: DATABASE_URL set via task definition env var (CF params + secret resolve)
  # EC2: DATABASE_URL constructed at runtime from DATABASE_ENDPOINT + POSTGRES_PASSWORD (fetched here)
  "POSTGRES_PASSWORD": ("postgres", "POSTGRES_PASSWORD"),
  # --- Valkey/Redis ---
  "VALKEY_AUTH_TOKEN": ("valkey", "VALKEY_AUTH_TOKEN"),
  # --- AWS: S3 Credentials ---
  # Note: Bucket names are computed from environment in env.py, not secrets
  # Note: PUBLIC_DATA_CDN_URL is passed via ECS task definition, not secrets
  "AWS_S3_ACCESS_KEY_ID": (None, "AWS_S3_ACCESS_KEY_ID"),
  "AWS_S3_SECRET_ACCESS_KEY": (None, "AWS_S3_SECRET_ACCESS_KEY"),
  # --- Admin ---
  "ADMIN_API_KEY": ("admin", "ADMIN_API_KEY"),
  # --- External Service API Keys ---
  "INTUIT_CLIENT_ID": (None, "INTUIT_CLIENT_ID"),
  "INTUIT_CLIENT_SECRET": (None, "INTUIT_CLIENT_SECRET"),
  "INTUIT_REDIRECT_URI": (None, "INTUIT_REDIRECT_URI"),
  "INTUIT_ENVIRONMENT": (None, "INTUIT_ENVIRONMENT"),
  "SEC_GOV_USER_AGENT": (None, "SEC_GOV_USER_AGENT"),
  "OPENFIGI_API_KEY": (None, "OPENFIGI_API_KEY"),
  "STRIPE_SECRET_KEY": (None, "STRIPE_SECRET_KEY"),
  "STRIPE_PUBLISHABLE_KEY": (None, "STRIPE_PUBLISHABLE_KEY"),
  "STRIPE_WEBHOOK_SECRET": (None, "STRIPE_WEBHOOK_SECRET"),
  # NOTE: STRIPE_API_VERSION moved to constants.py - it's a fixed API version, not a secret
  # --- Cloudflare R2 ---
  "R2_ACCESS_KEY_ID": (None, "R2_ACCESS_KEY_ID"),
  "R2_SECRET_ACCESS_KEY": (None, "R2_SECRET_ACCESS_KEY"),
  "R2_ENDPOINT_URL": (None, "R2_ENDPOINT_URL"),
  "R2_BUCKET_NAME": (None, "R2_BUCKET_NAME"),
  "R2_PUBLIC_BUCKET_NAME": (None, "R2_PUBLIC_BUCKET_NAME"),
  "R2_PUBLIC_URL": (None, "R2_PUBLIC_URL"),
}


def get_secret_value(key: str, default: str = "") -> str:
  """
  Get a specific secret value from AWS Secrets Manager.

  This is a convenience function that handles the logic of:
  1. Checking if we're in prod/staging (use Secrets Manager)
  2. Otherwise returning the default or environment variable

  Args:
      key: The key name to retrieve (e.g., "JWT_SECRET_KEY", "DATABASE_URL")
      default: Default value if not found

  Returns:
      The secret value or default
  """
  # First check environment variable
  env_value = os.getenv(key)
  if env_value:
    return env_value

  # Only use Secrets Manager for prod/staging
  environment = os.getenv("ENVIRONMENT", "dev")
  if environment not in ["prod", "staging"]:
    return default

  try:
    manager = get_secrets_manager()

    if key in SECRET_MAPPINGS:
      secret_type, secret_key = SECRET_MAPPINGS[key]
      secrets = manager.get_secret(secret_type)
      return secrets.get(secret_key, default)

    # If not in mappings, try base secret
    secrets = manager.get_secret()
    return secrets.get(key, default)

  except Exception as e:
    # Log the error but don't fail - return default
    logger.warning(f"Failed to retrieve secret '{key}' from Secrets Manager: {e}")
    return default


def get_secret_list_value(
  key: str, default: str = "", separator: str = ","
) -> list[str]:
  """
  Get a list secret value from AWS Secrets Manager.

  Retrieves a comma-separated string from Secrets Manager and splits it into a list.

  Args:
      key: The key name to retrieve (e.g., "JWT_AUDIENCE")
      default: Default comma-separated value if not found
      separator: String separator for list items (default: comma)

  Returns:
      List of strings from secret or default
  """
  value = get_secret_value(key, default)
  if not value:
    return []
  return [item.strip() for item in value.split(separator) if item.strip()]
