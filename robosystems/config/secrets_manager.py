"""AWS Secrets Manager access for prod/staging.

Base secret ``robosystems/{env}`` holds keys, auth, SSO, service URLs and
external API keys; extension secrets ``robosystems/{env}/{postgres,valkey,
admin,graph-api}`` hold the rest (see SECRET_MAPPINGS). Feature flags live in
SSM instead (parameter_store.py).

Secrets are cached with a 1-hour TTL, deliberately not an ``lru_cache``: a
TTL lets a rotated secret take effect without a restart (``refresh()`` makes
it immediate).
"""

import json
import logging
import os
import time
from typing import Any

import boto3
from botocore.exceptions import ClientError

# Not robosystems.logger: that would be a circular import.
logger = logging.getLogger(__name__)


class SecretsManager:
  """Manages retrieval of secrets from AWS Secrets Manager."""

  def __init__(
    self,
    environment: str | None = None,
    region: str | None = None,
    cache_ttl_seconds: int = 3600,
  ):
    """Unset arguments fall back to env vars."""
    self.environment = environment or os.getenv("ENVIRONMENT", "dev")
    self.region = region or os.getenv("AWS_REGION", "us-east-1")
    self.cache_ttl_seconds = cache_ttl_seconds

    self.client = boto3.client("secretsmanager", region_name=self.region)

    # {cache_key: (secret_data, fetched_at)}
    self._cache: dict[str, tuple[dict[str, Any], float]] = {}

  def get_secret(self, secret_type: str | None = None) -> dict[str, Any]:
    """Fetch a secret (TTL-cached); None ``secret_type`` is the base secret.

    Returns {} outside prod/staging or when the secret does not exist; other
    failures raise in prod/staging (fail closed).
    """
    if self.environment not in ["prod", "staging"]:
      return {}

    cache_key = f"{self.environment}/{secret_type}" if secret_type else self.environment

    if cache_key in self._cache:
      secret_data, timestamp = self._cache[cache_key]
      if time.time() - timestamp < self.cache_ttl_seconds:
        return secret_data
      else:
        del self._cache[cache_key]
        logger.info(f"Cache expired for secret: {cache_key}")

    if secret_type:
      secret_id = f"robosystems/{self.environment}/{secret_type}"
    else:
      secret_id = f"robosystems/{self.environment}"

    try:
      response = self.client.get_secret_value(SecretId=secret_id)

      if "SecretString" in response:
        # The admin key is stored as a raw string, not JSON.
        if secret_type == "admin":
          secret_data = {"ADMIN_API_KEY": response["SecretString"]}
        else:
          secret_data = json.loads(response["SecretString"])
      else:
        raise ValueError(f"Binary secret not supported for {secret_id}")

      self._cache[cache_key] = (secret_data, time.time())

      logger.info(f"Successfully retrieved secret: {secret_id}")
      return secret_data

    except ClientError as e:
      error_code = e.response.get("Error", {}).get("Code", "Unknown")

      if error_code == "ResourceNotFoundException":
        logger.warning(f"Secret not found: {secret_id}")
        return {}
      elif error_code == "AccessDeniedException":
        logger.error(f"Access denied to secret: {secret_id}")
        if self.environment in ["prod", "staging"]:
          raise
        return {}
      else:
        logger.error(f"Error retrieving secret {secret_id}: {error_code}")
        if self.environment in ["prod", "staging"]:
          raise
        return {}
    except Exception as e:
      logger.error(f"Unexpected error retrieving secret {secret_id}: {e}")
      if self.environment in ["prod", "staging"]:
        raise
      return {}

  def get_admin_key(self) -> str:
    """The admin API key (the ADMIN_API_KEY env var outside prod/staging)."""
    if self.environment not in ["prod", "staging"]:
      return os.getenv("ADMIN_API_KEY", "")

    secrets = self.get_secret("admin")
    return secrets.get("ADMIN_API_KEY", "")

  def refresh(self, secret_type: str | None = None):
    """Drop cached secrets: one by type, or all when ``secret_type`` is None."""
    if secret_type:
      cache_key = f"{self.environment}/{secret_type}"
      self._cache.pop(cache_key, None)
    else:
      self._cache.clear()


_secrets_manager: SecretsManager | None = None


def get_secrets_manager() -> SecretsManager:
  """The process-wide SecretsManager."""
  global _secrets_manager
  if _secrets_manager is None:
    _secrets_manager = SecretsManager()
  return _secrets_manager


# key -> (extension_secret_type, key_name); None type = the base secret.
SECRET_MAPPINGS = {
  # --- Core: Encryption Keys ---
  "CONNECTION_CREDENTIALS_KEY": (None, "CONNECTION_CREDENTIALS_KEY"),
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
  # Only EC2 hosts read this; ECS gets DATABASE_URL from the task definition.
  "POSTGRES_PASSWORD": ("postgres", "POSTGRES_PASSWORD"),
  # --- Valkey/Redis ---
  "VALKEY_AUTH_TOKEN": ("valkey", "VALKEY_AUTH_TOKEN"),
  # --- AWS: S3 Credentials ---
  "AWS_S3_ACCESS_KEY_ID": (None, "AWS_S3_ACCESS_KEY_ID"),
  "AWS_S3_SECRET_ACCESS_KEY": (None, "AWS_S3_SECRET_ACCESS_KEY"),
  # --- Admin ---
  "ADMIN_API_KEY": ("admin", "ADMIN_API_KEY"),
  # --- Enterprise SSO connection (not all secret, but kept together) ---
  "SSO_OIDC_CLIENT_SECRET": (None, "SSO_OIDC_CLIENT_SECRET"),
  "SSO_OIDC_ISSUER": (None, "SSO_OIDC_ISSUER"),
  "SSO_OIDC_CLIENT_ID": (None, "SSO_OIDC_CLIENT_ID"),
  "SSO_OIDC_PROVIDER_LABEL": (None, "SSO_OIDC_PROVIDER_LABEL"),
  "SSO_OIDC_BINDING_CLAIM": (None, "SSO_OIDC_BINDING_CLAIM"),
  "SSO_DEFAULT_ROLE": (None, "SSO_DEFAULT_ROLE"),
  "ENTERPRISE_ORG_ID": (None, "ENTERPRISE_ORG_ID"),
  # --- External Service API Keys ---
  "INTUIT_CLIENT_ID": (None, "INTUIT_CLIENT_ID"),
  "INTUIT_CLIENT_SECRET": (None, "INTUIT_CLIENT_SECRET"),
  "INTUIT_REDIRECT_URI": (None, "INTUIT_REDIRECT_URI"),
  "INTUIT_ENVIRONMENT": (None, "INTUIT_ENVIRONMENT"),
  "MERCURY_CLIENT_ID": (None, "MERCURY_CLIENT_ID"),
  "MERCURY_CLIENT_SECRET": (None, "MERCURY_CLIENT_SECRET"),
  "MERCURY_ENVIRONMENT": (None, "MERCURY_ENVIRONMENT"),
  "PLAID_CLIENT_ID": (None, "PLAID_CLIENT_ID"),
  "PLAID_SECRET": (None, "PLAID_SECRET"),
  "PLAID_ENVIRONMENT": (None, "PLAID_ENVIRONMENT"),
  "SEC_GOV_USER_AGENT": (None, "SEC_GOV_USER_AGENT"),
  "OPENFIGI_API_KEY": (None, "OPENFIGI_API_KEY"),
  "STRIPE_SECRET_KEY": (None, "STRIPE_SECRET_KEY"),
  "STRIPE_PUBLISHABLE_KEY": (None, "STRIPE_PUBLISHABLE_KEY"),
  "STRIPE_WEBHOOK_SECRET": (None, "STRIPE_WEBHOOK_SECRET"),
  # --- Cloudflare R2 ---
  "R2_ACCESS_KEY_ID": (None, "R2_ACCESS_KEY_ID"),
  "R2_SECRET_ACCESS_KEY": (None, "R2_SECRET_ACCESS_KEY"),
  "R2_ENDPOINT_URL": (None, "R2_ENDPOINT_URL"),
  "R2_BUCKET_NAME": (None, "R2_BUCKET_NAME"),
  "R2_PUBLIC_BUCKET_NAME": (None, "R2_PUBLIC_BUCKET_NAME"),
  "R2_PUBLIC_URL": (None, "R2_PUBLIC_URL"),
  "HF_TOKEN": (None, "HF_TOKEN"),
}


def get_secret_value(key: str, default: str = "") -> str:
  """One secret: the env var of the same name wins, then Secrets Manager in
  prod/staging, else ``default``.
  """
  env_value = os.getenv(key)
  if env_value:
    return env_value

  environment = os.getenv("ENVIRONMENT", "dev")
  if environment not in ["prod", "staging"]:
    return default

  try:
    manager = get_secrets_manager()

    if key in SECRET_MAPPINGS:
      secret_type, secret_key = SECRET_MAPPINGS[key]
      secrets = manager.get_secret(secret_type)
      return secrets.get(secret_key, default)

    secrets = manager.get_secret()
    return secrets.get(key, default)

  except Exception as e:
    # Fail closed: swallowing would substitute a possibly-insecure default
    # (e.g. an empty signing key). A missing secret never reaches here.
    logger.error(f"Failed to retrieve secret '{key}' from Secrets Manager: {e}")
    raise


def get_secret_list_value(
  key: str, default: str = "", separator: str = ","
) -> list[str]:
  """A separator-joined list secret, e.g. "JWT_AUDIENCE", trimmed."""
  value = get_secret_value(key, default)
  if not value:
    return []
  return [item.strip() for item in value.split(separator) if item.strip()]
