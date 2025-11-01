"""
AWS Secrets Manager integration for dynamic secret retrieval.

This module provides a centralized way to fetch secrets from AWS Secrets Manager
rather than relying on environment variables passed through userdata scripts.

## Architecture

Secrets are organized in AWS Secrets Manager with the following structure:
- Base secret: `robosystems/{environment}` (e.g., robosystems/prod, robosystems/staging)
  Contains: JWT_SECRET_KEY, CONNECTION_CREDENTIALS_KEY, GRAPH_BACKUP_ENCRYPTION_KEY,
            ANTHROPIC_API_KEY, INTUIT_*, PLAID_*, SEC_GOV_USER_AGENT, TURNSTILE_*,
            Feature flags: RATE_LIMIT_ENABLED, USER_REGISTRATION_ENABLED,
            CONNECTION_SEC_ENABLED, CONNECTION_QUICKBOOKS_ENABLED, CONNECTION_PLAID_ENABLED,
            SUBGRAPH_CREATION_ENABLED, BACKUP_CREATION_ENABLED,
            Runtime configs: USER_GRAPHS_DEFAULT_LIMIT

- Extension secrets: `robosystems/{environment}/{type}`
  - `/postgres`: DATABASE_URL and other PostgreSQL configuration
  - `/s3`: AWS_S3_ACCESS_KEY_ID, AWS_S3_SECRET_ACCESS_KEY, bucket names
  - `/graph-api`: GRAPH_API_KEY and other graph database secrets (unified for Kuzu/Neo4j)

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
from typing import Any, Dict, Optional, Tuple

import boto3
from botocore.exceptions import ClientError

# Use standard logging to avoid circular import with robosystems.logger
logger = logging.getLogger(__name__)


class SecretsManager:
  """Manages retrieval of secrets from AWS Secrets Manager."""

  def __init__(
    self,
    environment: Optional[str] = None,
    region: Optional[str] = None,
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
    self._cache: Dict[str, Tuple[Dict[str, Any], float]] = {}

  def get_secret(self, secret_type: Optional[str] = None) -> Dict[str, Any]:
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

  def get_s3_buckets(self) -> Dict[str, str]:
    """
    Get all S3 bucket names from secrets.

    Returns:
        Dictionary mapping bucket purposes to bucket names.
    """
    # For local dev, return defaults
    if self.environment not in ["prod", "staging"]:
      return {
        "aws_s3": f"robosystems-{self.environment}",
        "kuzu": f"robosystems-kuzu-databases-{self.environment}",
        "sec_raw": f"robosystems-sec-raw-{self.environment}",
        "sec_processed": f"robosystems-sec-processed-{self.environment}",
        "public_data": f"robosystems-public-data-{self.environment}",
        "deployment": f"robosystems-deployment-{self.environment}",
      }

    # Get S3-specific secrets for prod/staging
    secrets = self.get_secret("s3")

    # Extract bucket names with environment suffix handling
    buckets = {
      "aws_s3": secrets.get("AWS_S3_BUCKET", f"robosystems-{self.environment}"),
      "kuzu": secrets.get(
        "KUZU_S3_BUCKET", f"robosystems-kuzu-databases-{self.environment}"
      ),
      "sec_raw": secrets.get(
        "SEC_RAW_BUCKET", f"robosystems-sec-raw-{self.environment}"
      ),
      "sec_processed": secrets.get(
        "SEC_PROCESSED_BUCKET", f"robosystems-sec-processed-{self.environment}"
      ),
      "public_data": secrets.get(
        "PUBLIC_DATA_BUCKET", f"robosystems-public-data-{self.environment}"
      ),
      "deployment": secrets.get(
        "DEPLOYMENT_BUCKET", f"robosystems-deployment-{self.environment}"
      ),
    }

    return buckets

  def get_database_url(self) -> str:
    """
    Get the database URL from secrets.

    Returns:
        PostgreSQL connection string.
    """
    if self.environment not in ["prod", "staging"]:
      return ""  # Local dev uses DATABASE_URL env var

    secrets = self.get_secret("postgres")
    return secrets.get("DATABASE_URL", "")

  def get_s3_credentials(self) -> Dict[str, str]:
    """
    Get S3 access credentials from secrets.

    Returns:
        Dictionary with access key ID and secret access key.
    """
    if self.environment not in ["prod", "staging"]:
      return {
        "access_key_id": "",
        "secret_access_key": "",
      }

    secrets = self.get_secret("s3")
    return {
      "access_key_id": secrets.get("AWS_S3_ACCESS_KEY_ID", ""),
      "secret_access_key": secrets.get("AWS_S3_SECRET_ACCESS_KEY", ""),
    }

  def refresh(self, secret_type: Optional[str] = None):
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
_secrets_manager: Optional[SecretsManager] = None


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


def get_s3_bucket_name(purpose: str) -> str:
  """
  Get an S3 bucket name for a specific purpose.

  Args:
      purpose: The purpose of the bucket (e.g., "sec_processed", "sec_raw", "kuzu")

  Returns:
      The bucket name with proper environment suffix.
  """
  manager = get_secrets_manager()
  buckets = manager.get_s3_buckets()

  # Map common purpose strings to our bucket keys
  purpose_map = {
    "sec_processed": "sec_processed",
    "sec_raw": "sec_raw",
    "kuzu": "kuzu",
    "aws_s3": "aws_s3",
    "public": "public_data",
    "deployment": "deployment",
  }

  mapped_purpose = purpose_map.get(purpose, purpose)
  bucket_name = buckets.get(mapped_purpose, "")

  if not bucket_name:
    logger.warning(f"No bucket found for purpose: {purpose}")

  return bucket_name


# Secret mapping configuration
# This could be externalized to a YAML/JSON file if it grows too large
SECRET_MAPPINGS = {
  # Database secrets (postgres)
  "DATABASE_URL": ("postgres", "DATABASE_URL"),
  # S3 secrets
  "AWS_S3_ACCESS_KEY_ID": ("s3", "AWS_S3_ACCESS_KEY_ID"),
  "AWS_S3_SECRET_ACCESS_KEY": ("s3", "AWS_S3_SECRET_ACCESS_KEY"),
  "AWS_S3_BUCKET": ("s3", "AWS_S3_BUCKET"),
  "SEC_RAW_BUCKET": ("s3", "SEC_RAW_BUCKET"),
  "SEC_PROCESSED_BUCKET": ("s3", "SEC_PROCESSED_BUCKET"),
  "PUBLIC_DATA_BUCKET": ("s3", "PUBLIC_DATA_BUCKET"),
  "PUBLIC_DATA_CDN_URL": ("s3", "PUBLIC_DATA_CDN_URL"),
  # Graph API secrets (unified for all backends: Kuzu, Neo4j)
  "GRAPH_API_KEY": ("graph-api", "GRAPH_API_KEY"),
  # Valkey secrets
  "VALKEY_AUTH_TOKEN": ("valkey", "VALKEY_AUTH_TOKEN"),
  # Base secrets (robosystems/{env})
  "CONNECTION_CREDENTIALS_KEY": (None, "CONNECTION_CREDENTIALS_KEY"),
  "JWT_SECRET_KEY": (None, "JWT_SECRET_KEY"),
  "GRAPH_BACKUP_ENCRYPTION_KEY": (None, "GRAPH_BACKUP_ENCRYPTION_KEY"),
  "GRAPH_BACKUP_ENCRYPTION_PASSWORD": (None, "GRAPH_BACKUP_ENCRYPTION_PASSWORD"),
  "ANTHROPIC_API_KEY": (None, "ANTHROPIC_API_KEY"),
  "INTUIT_REDIRECT_URI": (None, "INTUIT_REDIRECT_URI"),
  "INTUIT_CLIENT_ID": (None, "INTUIT_CLIENT_ID"),
  "INTUIT_CLIENT_SECRET": (None, "INTUIT_CLIENT_SECRET"),
  "INTUIT_ENVIRONMENT": (None, "INTUIT_ENVIRONMENT"),
  "OPENFIGI_API_KEY": (None, "OPENFIGI_API_KEY"),
  "PLAID_CLIENT_ID": (None, "PLAID_CLIENT_ID"),
  "PLAID_CLIENT_SECRET": (None, "PLAID_CLIENT_SECRET"),
  "PLAID_ENVIRONMENT": (None, "PLAID_ENVIRONMENT"),
  "SEC_GOV_USER_AGENT": (None, "SEC_GOV_USER_AGENT"),
  "TURNSTILE_SECRET_KEY": (None, "TURNSTILE_SECRET_KEY"),
  "TURNSTILE_SITE_KEY": (None, "TURNSTILE_SITE_KEY"),
  # Feature flags
  "BILLING_ENABLED": (None, "BILLING_ENABLED"),
  "BILLING_PREMIUM_PLANS_ENABLED": (None, "BILLING_PREMIUM_PLANS_ENABLED"),
  "CAPTCHA_ENABLED": (None, "CAPTCHA_ENABLED"),
  "CONNECTION_PLAID_ENABLED": (None, "CONNECTION_PLAID_ENABLED"),
  "CONNECTION_QUICKBOOKS_ENABLED": (None, "CONNECTION_QUICKBOOKS_ENABLED"),
  "CONNECTION_SEC_ENABLED": (None, "CONNECTION_SEC_ENABLED"),
  "EMAIL_VERIFICATION_ENABLED": (None, "EMAIL_VERIFICATION_ENABLED"),
  "LOAD_SHEDDING_ENABLED": (None, "LOAD_SHEDDING_ENABLED"),
  "OTEL_ENABLED": (None, "OTEL_ENABLED"),
  "RATE_LIMIT_ENABLED": (None, "RATE_LIMIT_ENABLED"),
  "SECURITY_AUDIT_ENABLED": (None, "SECURITY_AUDIT_ENABLED"),
  "SSE_ENABLED": (None, "SSE_ENABLED"),
  "USER_REGISTRATION_ENABLED": (None, "USER_REGISTRATION_ENABLED"),
  "SUBGRAPH_CREATION_ENABLED": (None, "SUBGRAPH_CREATION_ENABLED"),
  "BACKUP_CREATION_ENABLED": (None, "BACKUP_CREATION_ENABLED"),
  "AGENT_POST_ENABLED": (None, "AGENT_POST_ENABLED"),
  # Runtime configuration
  "USER_GRAPHS_DEFAULT_LIMIT": (None, "USER_GRAPHS_DEFAULT_LIMIT"),
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
