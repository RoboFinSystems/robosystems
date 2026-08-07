"""
SSM Parameter Store integration for feature flags and runtime configuration.

Uses a layered override model:
1. Environment variable (local dev, CI, testing)
2. SSM Parameter Store (runtime config in AWS — applies without a redeploy)
3. Default from env.py

Feature flags live in SSM rather than Secrets Manager because they are not
sensitive: Standard-tier parameters are free, are optimized for frequent reads,
and hold plain strings instead of JSON blobs.

## Parameter Naming Convention

Parameters are stored under two hierarchies:

Feature Flags: /robosystems/{environment}/features/{KEY}
Examples:
- /robosystems/prod/features/RATE_LIMIT_ENABLED
- /robosystems/staging/features/USER_REGISTRATION_ENABLED

Tuning Parameters: /robosystems/{environment}/tuning/{PATH}
Examples:
- /robosystems/prod/tuning/cache/BALANCE_TTL
- /robosystems/staging/tuning/admission/MEMORY_THRESHOLD

## Usage

Feature flags are read from env.py:

```python
from robosystems.config.parameter_store import get_parameter_value

RATE_LIMIT_ENABLED = get_bool_env(
    "RATE_LIMIT_ENABLED",
    bool(get_parameter_value("RATE_LIMIT_ENABLED", "true").lower() == "true"),
)
```

Tuning parameters go through tuning.py, which wraps this module with defaults:

```python
from robosystems.config.tuning import TuningConfig

balance_ttl = TuningConfig.get_cache_balance_ttl()  # int, SSM-overridable
```
"""

import logging
import os
import time

# Use standard logging to avoid circular import with robosystems.logger
logger = logging.getLogger(__name__)

# Lazy-load boto3 to avoid import errors in environments without AWS SDK
_ssm_client = None


def _get_ssm_client():
  """Get or create the SSM client (lazy initialization)."""
  global _ssm_client
  if _ssm_client is None:
    # Only initialize boto3 SSM client in AWS environments to avoid
    # triggering credential resolution (SSO token refresh) in dev/test
    environment = os.getenv("ENVIRONMENT", "dev")
    if environment not in ("prod", "staging"):
      return None
    try:
      import boto3

      region = os.getenv("AWS_REGION", "us-east-1")
      _ssm_client = boto3.client("ssm", region_name=region)
    except ImportError:
      logger.debug("boto3 not available, SSM Parameter Store disabled")
      return None
  return _ssm_client


class ParameterStoreManager:
  """SSM Parameter Store client with TTL-based caching."""

  def __init__(
    self,
    environment: str | None = None,
    region: str | None = None,
    cache_ttl_seconds: int = 300,  # 5 min cache (more frequent than secrets)
  ):
    """Initialize the manager; unset arguments fall back to env vars."""
    self.environment = environment or os.getenv("ENVIRONMENT", "dev")
    self.region = region or os.getenv("AWS_REGION", "us-east-1")
    self.cache_ttl_seconds = cache_ttl_seconds

    # Cache for retrieved parameters with timestamps
    # Format: {parameter_name: (value, timestamp)}
    self._cache: dict[str, tuple[str, float]] = {}

    # Batch cache for all feature flags
    # Format: (parameters_dict, timestamp)
    self._batch_cache: tuple[dict[str, str], float] | None = None

  def _get_client(self):
    """Get the SSM client."""
    return _get_ssm_client()

  def get_parameter(self, name: str, default: str = "") -> str:
    """Get one feature flag by unprefixed name, e.g. "RATE_LIMIT_ENABLED".

    Returns ``default`` outside prod/staging, where SSM is not consulted.
    """
    # Only use Parameter Store for prod/staging
    if self.environment not in ["prod", "staging"]:
      return default

    client = self._get_client()
    if client is None:
      return default

    # Check cache with TTL
    if name in self._cache:
      value, timestamp = self._cache[name]
      if time.time() - timestamp < self.cache_ttl_seconds:
        return value
      else:
        del self._cache[name]

    # Build full parameter path
    parameter_path = f"/robosystems/{self.environment}/features/{name}"

    try:
      response = client.get_parameter(Name=parameter_path)
      value = response["Parameter"]["Value"]

      # Cache the result
      self._cache[name] = (value, time.time())

      logger.debug(f"Retrieved parameter: {parameter_path}")
      return value

    except client.exceptions.ParameterNotFound:
      logger.debug(f"Parameter not found: {parameter_path}, using default")
      return default
    except Exception as e:
      logger.warning(f"Failed to retrieve parameter '{parameter_path}': {e}")
      return default

  def get_all_feature_flags(self) -> dict[str, str]:
    """Batch fetch every flag under /robosystems/{env}/features/.

    One paginated call instead of one call per flag, which is why startup uses
    this rather than looping over :meth:`get_parameter`.
    """
    # Only use Parameter Store for prod/staging
    if self.environment not in ["prod", "staging"]:
      return {}

    client = self._get_client()
    if client is None:
      return {}

    # Check batch cache with TTL
    if self._batch_cache is not None:
      params, timestamp = self._batch_cache
      if time.time() - timestamp < self.cache_ttl_seconds:
        return params

    path = f"/robosystems/{self.environment}/features"
    parameters: dict[str, str] = {}

    try:
      paginator = client.get_paginator("get_parameters_by_path")
      for page in paginator.paginate(Path=path, Recursive=True):
        for param in page.get("Parameters", []):
          # Extract just the parameter name (last part of path)
          name = param["Name"].split("/")[-1]
          parameters[name] = param["Value"]

      # Cache the batch result
      self._batch_cache = (parameters, time.time())

      # Also update individual cache entries
      current_time = time.time()
      for name, value in parameters.items():
        self._cache[name] = (value, current_time)

      logger.info(f"Loaded {len(parameters)} feature flags from SSM")
      return parameters

    except Exception as e:
      logger.warning(f"Failed to batch fetch feature flags: {e}")
      return {}

  def refresh(self, name: str | None = None):
    """Drop cached parameters — one by name, or all when ``name`` is None."""
    if name:
      self._cache.pop(name, None)
    else:
      self._cache.clear()
      self._batch_cache = None

  # =========================================================================
  # TUNING PARAMETER METHODS
  # =========================================================================
  # These methods support the /robosystems/{env}/tuning/ parameter hierarchy
  # for runtime-adjustable operational parameters.

  def get_tuning_parameter(self, path: str, default: str = "") -> str:
    """
    Get a tuning parameter from /robosystems/{env}/tuning/{path}.

    Tuning parameters are operational values (cache TTLs, thresholds, limits)
    adjustable at runtime without a redeploy. ``path`` is relative, e.g.
    "cache/BALANCE_TTL".
    """
    # Only use Parameter Store for prod/staging
    if self.environment not in ["prod", "staging"]:
      return default

    client = self._get_client()
    if client is None:
      return default

    # Use tuning-prefixed cache key to avoid collision with feature flags
    cache_key = f"tuning:{path}"

    # Check cache with TTL
    if cache_key in self._cache:
      value, timestamp = self._cache[cache_key]
      if time.time() - timestamp < self.cache_ttl_seconds:
        return value
      else:
        del self._cache[cache_key]

    # Build full parameter path
    parameter_path = f"/robosystems/{self.environment}/tuning/{path}"

    try:
      response = client.get_parameter(Name=parameter_path)
      value = response["Parameter"]["Value"]

      # Cache the result
      self._cache[cache_key] = (value, time.time())

      logger.debug(f"Retrieved tuning parameter: {parameter_path}")
      return value

    except client.exceptions.ParameterNotFound:
      logger.debug(f"Tuning parameter not found: {parameter_path}, using default")
      return default
    except Exception as e:
      logger.warning(f"Failed to retrieve tuning parameter '{parameter_path}': {e}")
      return default

  def get_tuning_int(self, path: str, default: int) -> int:
    """Get a tuning parameter as an int, falling back if unset or unparseable."""
    value = self.get_tuning_parameter(path, str(default))
    try:
      return int(value)
    except (ValueError, TypeError):
      logger.warning(f"Invalid int value for tuning/{path}: {value}, using default")
      return default

  def get_tuning_float(self, path: str, default: float) -> float:
    """Get a tuning parameter as a float, falling back if unset or unparseable."""
    value = self.get_tuning_parameter(path, str(default))
    try:
      return float(value)
    except (ValueError, TypeError):
      logger.warning(f"Invalid float value for tuning/{path}: {value}, using default")
      return default

  def get_all_tuning_parameters(self) -> dict[str, str]:
    """Batch fetch every parameter under /robosystems/{env}/tuning/.

    Keys are paths relative to that prefix. Also warms the per-parameter cache.
    """
    # Only use Parameter Store for prod/staging
    if self.environment not in ["prod", "staging"]:
      return {}

    client = self._get_client()
    if client is None:
      return {}

    path = f"/robosystems/{self.environment}/tuning"
    parameters: dict[str, str] = {}

    try:
      paginator = client.get_paginator("get_parameters_by_path")
      for page in paginator.paginate(Path=path, Recursive=True):
        for param in page.get("Parameters", []):
          # Extract path relative to /robosystems/{env}/tuning/
          full_name = param["Name"]
          # Remove the prefix to get the relative path
          prefix = f"/robosystems/{self.environment}/tuning/"
          if full_name.startswith(prefix):
            relative_path = full_name[len(prefix) :]
            parameters[relative_path] = param["Value"]

      # Update individual cache entries
      current_time = time.time()
      for param_path, value in parameters.items():
        cache_key = f"tuning:{param_path}"
        self._cache[cache_key] = (value, current_time)

      logger.info(f"Loaded {len(parameters)} tuning parameters from SSM")
      return parameters

    except Exception as e:
      logger.warning(f"Failed to batch fetch tuning parameters: {e}")
      return {}


# Global instance for easy access
_parameter_manager: ParameterStoreManager | None = None


def get_parameter_manager() -> ParameterStoreManager:
  """Get or create the process-wide ParameterStoreManager."""
  global _parameter_manager
  if _parameter_manager is None:
    _parameter_manager = ParameterStoreManager()
  return _parameter_manager


def get_parameter_value(key: str, default: str = "") -> str:
  """
  Get a feature flag with layered fallback.

  Priority order:
  1. Environment variable (local dev, CI, testing)
  2. SSM Parameter Store (prod/staging only, applies without a redeploy)
  3. ``default``
  """
  # Priority 1: Environment variable
  env_value = os.getenv(key)
  if env_value is not None:
    return env_value

  # Priority 2: SSM Parameter Store (prod/staging only)
  environment = os.getenv("ENVIRONMENT", "dev")
  if environment in ["prod", "staging"]:
    try:
      manager = get_parameter_manager()
      ssm_value = manager.get_parameter(key, default="")
      if ssm_value:
        return ssm_value
      else:
        # SSM returned empty — parameter may not exist
        print(
          f"WARNING: SSM parameter '{key}' returned empty, using default: '{default}'"
        )
    except Exception as e:
      print(f"WARNING: Failed to get SSM parameter '{key}': {type(e).__name__}: {e}")
      logger.warning(f"Failed to get parameter '{key}' from SSM: {e}")

  # Priority 3: Default value
  return default


def preload_feature_flags() -> dict[str, str]:
  """Warm the flag cache at startup with a single batched SSM call.

  env.py calls this at import so no individual flag lookup has to hit SSM,
  which would otherwise mean one API call per flag and a failure mode under
  load. Returns {} outside prod/staging.
  """
  environment = os.getenv("ENVIRONMENT", "dev")
  if environment not in ["prod", "staging"]:
    logger.debug("Feature flag preload skipped (not in AWS environment)")
    return {}

  try:
    manager = get_parameter_manager()
    return manager.get_all_feature_flags()
  except Exception as e:
    logger.warning(f"Failed to preload feature flags: {e}")
    return {}
