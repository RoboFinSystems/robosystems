"""
AWS Configuration Module

Centralized interface for AWS resource configuration, primarily through CloudFormation
stack outputs but extensible for other AWS services.
"""

import time
import yaml
import boto3
import logging
from pathlib import Path
from typing import Optional, Dict, Any
from threading import Lock
from botocore.exceptions import ClientError
from retrying import retry

from robosystems.config import env

logger = logging.getLogger(__name__)


class AWSConfig:
  """
  Centralized AWS configuration manager.

  Provides access to CloudFormation stack outputs and configuration with caching
  to minimize API calls. Extensible for other AWS service configurations.
  """

  def __init__(self, cache_ttl: int = 3600):
    """
    Initialize AWS configuration manager.

    Args:
        cache_ttl: Cache time-to-live in seconds (default: 1 hour)
    """
    self.cache_ttl = cache_ttl
    self._cache: Dict[str, tuple[Any, float]] = {}
    self._lock = Lock()
    self._stack_config: Optional[Dict] = None
    self._cf_client = None

    # Initialize CloudFormation client if in AWS environment
    if env.is_aws_environment():
      try:
        self._cf_client = boto3.client("cloudformation", region_name=env.AWS_REGION)
      except Exception as e:
        # Client initialization may fail in some environments (e.g., during testing)
        logger.warning(f"Failed to initialize CloudFormation client: {e}")
        self._cf_client = None

  def _load_stack_config(self) -> Dict:
    """Load stack configuration from YAML file."""
    if self._stack_config is not None:
      return self._stack_config

    # Try different config paths
    config_paths = [
      Path("/app/configs/stacks.yml"),  # Docker container
      Path(__file__).parent.parent.parent / ".github/configs/stacks.yml",  # Local dev
    ]

    for config_path in config_paths:
      if config_path.exists():
        try:
          with open(config_path, "r") as f:
            self._stack_config = yaml.safe_load(f)
            if self._stack_config and self._validate_stack_config(self._stack_config):
              logger.debug(f"Loaded stack config from {config_path}")
              return self._stack_config
            else:
              logger.warning(f"Invalid stack config at {config_path}")
        except Exception as e:
          logger.error(f"Failed to load stack config from {config_path}: {e}")

    # Return empty config if file not found
    logger.warning("Stack configuration file not found, using empty config")
    self._stack_config = {}
    return self._stack_config

  def _validate_stack_config(self, config: Dict) -> bool:
    """Validate stack configuration structure."""
    if not isinstance(config, dict):
      return False

    # Check for at least one environment
    valid_envs = ["production", "staging", "development"]
    has_env = any(env in config for env in valid_envs)

    if not has_env:
      logger.warning(
        f"Stack config missing environment keys. Expected one of: {valid_envs}"
      )
      return False

    return True

  def get_stack_name(
    self, component: str, variant: Optional[str] = None
  ) -> Optional[str]:
    """
    Get CloudFormation stack name from configuration.

    Args:
        component: Component name (e.g., "valkey", "api", "kuzu.infra")
        variant: Optional variant (e.g., "standard" for kuzu.writers.standard)

    Returns:
        Stack name or None if not found
    """
    config = self._load_stack_config()
    environment = env.get_environment_key()

    if environment not in config:
      return None

    env_config = config[environment]

    # Navigate to the component
    parts = component.split(".")
    current = env_config

    for part in parts:
      if isinstance(current, dict):
        if variant and part in current:
          current = current.get(part, {}).get(variant, {})
        else:
          current = current.get(part, {})
      else:
        return None

    # Get stack name
    if isinstance(current, dict) and "stack_name" in current:
      return current["stack_name"]

    return None

  @retry(
    stop_max_attempt_number=3,
    wait_exponential_multiplier=1000,
    wait_exponential_max=10000,
  )
  def get_stack_output(
    self, stack_name: str, output_key: str, default: Optional[str] = None
  ) -> Optional[str]:
    """
    Get a specific output value from a CloudFormation stack with caching.

    Args:
        stack_name: Name of the CloudFormation stack
        output_key: Key of the output to retrieve
        default: Default value if output not found

    Returns:
        Output value or default
    """
    if not self._cf_client:
      logger.warning("CloudFormation client not available, using default value")
      return self._get_env_fallback(output_key, default)

    # Include region in cache key to avoid multi-region conflicts
    cache_key = f"{env.AWS_REGION}:{stack_name}:{output_key}"

    # Check cache first
    with self._lock:
      if self._is_cache_valid(cache_key):
        value, _ = self._cache[cache_key]
        logger.debug(f"Cache hit for {cache_key}")
        return value

    try:
      # Fetch from CloudFormation
      response = self._cf_client.describe_stacks(StackName=stack_name)

      # Extract outputs
      if "Stacks" in response and len(response["Stacks"]) > 0:
        stack = response["Stacks"][0]
        if "Outputs" in stack:
          for output in stack["Outputs"]:
            if output.get("OutputKey") == output_key:
              value = output.get("OutputValue")
              # Cache the result
              with self._lock:
                self._cache[cache_key] = (value, time.time())
              logger.info(
                "Retrieved stack output",
                extra={
                  "stack_name": stack_name,
                  "output_key": output_key,
                  "cache_hit": False,
                  "region": env.AWS_REGION,
                },
              )
              return value

    except ClientError as e:
      # Stack doesn't exist or access denied
      if e.response["Error"]["Code"] != "ValidationError":
        logger.error(
          f"Error fetching CloudFormation output: {e}",
          extra={
            "stack_name": stack_name,
            "output_key": output_key,
            "error_code": e.response["Error"]["Code"],
          },
        )

    logger.warning(
      "Stack output not found, using default",
      extra={"stack_name": stack_name, "output_key": output_key, "default": default},
    )
    return default

  def _get_env_fallback(
    self, output_key: str, default: Optional[str] = None
  ) -> Optional[str]:
    """Get fallback value from environment variables."""
    env_mappings = {
      "ValkeyUrl": "VALKEY_URL",
      "DatabaseUrl": "DATABASE_URL",
      "BucketName": "AWS_S3_BUCKET",
    }
    env_var = env_mappings.get(output_key)
    if env_var:
      value = getattr(env, env_var, None)
      if value:
        logger.debug(f"Using environment fallback for {output_key}")
        return value
    return default

  def get_all_stack_outputs(self, stack_name: str) -> Dict[str, str]:
    """
    Get all outputs from a CloudFormation stack.

    Args:
        stack_name: Name of the CloudFormation stack

    Returns:
        Dictionary of output key-value pairs
    """
    if not self._cf_client:
      return {}

    cache_key = f"{stack_name}:__all__"

    # Check cache first
    with self._lock:
      if self._is_cache_valid(cache_key):
        outputs, _ = self._cache[cache_key]
        return outputs

    outputs = {}
    try:
      response = self._cf_client.describe_stacks(StackName=stack_name)

      if "Stacks" in response and len(response["Stacks"]) > 0:
        stack = response["Stacks"][0]
        if "Outputs" in stack:
          for output in stack["Outputs"]:
            key = output.get("OutputKey")
            value = output.get("OutputValue")
            if key and value:
              outputs[key] = value

          # Cache the result
          with self._lock:
            self._cache[cache_key] = (outputs, time.time())

    except ClientError as e:
      if e.response["Error"]["Code"] != "ValidationError":
        logger.error(
          f"Error fetching CloudFormation outputs: {e}",
          extra={"stack_name": stack_name, "error_code": e.response["Error"]["Code"]},
        )

    return outputs

  def _is_cache_valid(self, key: str) -> bool:
    """Check if a cache entry is still valid."""
    if key not in self._cache:
      return False
    _, timestamp = self._cache[key]
    return (time.time() - timestamp) < self.cache_ttl

  def clear_cache(self):
    """Clear all cached values."""
    with self._lock:
      self._cache.clear()

  def get_valkey_url(self) -> Optional[str]:
    """
    Get Valkey/Redis URL from CloudFormation.

    Returns:
        Valkey URL or None
    """
    stack_name = self.get_stack_name("valkey")
    if stack_name:
      return self.get_stack_output(stack_name, "ValkeyUrl")
    return None

  def get_database_url(self) -> Optional[str]:
    """
    Get PostgreSQL database URL from CloudFormation.

    Returns:
        Database URL or None
    """
    stack_name = self.get_stack_name("postgres_iam")
    if stack_name:
      return self.get_stack_output(stack_name, "DatabaseUrl")
    return None

  def get_s3_bucket(self) -> Optional[str]:
    """
    Get S3 bucket name from CloudFormation.

    Returns:
        S3 bucket name or None
    """
    stack_name = self.get_stack_name("s3")
    if stack_name:
      return self.get_stack_output(stack_name, "BucketName")
    return None


# Global instance
_aws_config = None


def get_aws_config() -> AWSConfig:
  """Get or create the global AWS configuration instance."""
  global _aws_config
  if _aws_config is None:
    _aws_config = AWSConfig()
  return _aws_config


# Convenience functions for common operations
def get_valkey_url_from_cloudformation() -> Optional[str]:
  """Get Valkey URL from CloudFormation stack outputs."""
  return get_aws_config().get_valkey_url()


def get_database_url_from_cloudformation() -> Optional[str]:
  """Get database URL from CloudFormation stack outputs."""
  return get_aws_config().get_database_url()


def get_stack_output(
  component: str, output_key: str, default: Optional[str] = None
) -> Optional[str]:
  """
  Get a CloudFormation stack output for a component.

  Args:
      component: Component name (e.g., "valkey", "api")
      output_key: Output key to retrieve
      default: Default value if not found

  Returns:
      Output value or default
  """
  config = get_aws_config()
  stack_name = config.get_stack_name(component)
  if stack_name:
    return config.get_stack_output(stack_name, output_key, default)
  return default
