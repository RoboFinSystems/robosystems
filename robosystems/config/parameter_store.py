"""SSM Parameter Store access for feature flags and tuning parameters.

Flags live at ``/robosystems/{env}/features/{KEY}``, tuning parameters at
``/robosystems/{env}/tuning/{path}`` (read through tuning.py). Env vars
override both; SSM is only consulted in prod/staging.
"""

import logging
import os
import time

# Not robosystems.logger: that would be a circular import.
logger = logging.getLogger(__name__)

_ssm_client = None


def _get_ssm_client():
  """Lazily create the SSM client; None outside prod/staging."""
  global _ssm_client
  if _ssm_client is None:
    # Creating it elsewhere would trigger credential resolution (SSO refresh).
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
    cache_ttl_seconds: int = 300,
  ):
    """Initialize the manager; unset arguments fall back to env vars."""
    self.environment = environment or os.getenv("ENVIRONMENT", "dev")
    self.region = region or os.getenv("AWS_REGION", "us-east-1")
    self.cache_ttl_seconds = cache_ttl_seconds

    # {name: (value, fetched_at)}; tuning keys are prefixed "tuning:".
    self._cache: dict[str, tuple[str, float]] = {}

    self._batch_cache: tuple[dict[str, str], float] | None = None

  def _get_client(self):
    return _get_ssm_client()

  def get_parameter(self, name: str, default: str = "") -> str:
    """One feature flag by unprefixed name, e.g. "RATE_LIMIT_ENABLED"."""
    if self.environment not in ["prod", "staging"]:
      return default

    client = self._get_client()
    if client is None:
      return default

    if name in self._cache:
      value, timestamp = self._cache[name]
      if time.time() - timestamp < self.cache_ttl_seconds:
        return value
      else:
        del self._cache[name]

    parameter_path = f"/robosystems/{self.environment}/features/{name}"

    try:
      response = client.get_parameter(Name=parameter_path)
      value = response["Parameter"]["Value"]

      self._cache[name] = (value, time.time())

      logger.debug(f"Retrieved parameter: {parameter_path}")
      return value

    except client.exceptions.ParameterNotFound:
      logger.debug(f"Parameter not found: {parameter_path}, using default")
      return default
    except Exception as e:
      logger.warning(f"Failed to retrieve parameter '{parameter_path}': {e}")
      return default

  def get_parameter_uncached(self, name: str, default: str = "") -> str:
    """One feature flag read straight from SSM, for a flag whose change must
    take effect at once (a maintenance pause) rather than after the cache TTL."""
    if self.environment not in ["prod", "staging"]:
      return default
    client = self._get_client()
    if client is None:
      return default
    parameter_path = f"/robosystems/{self.environment}/features/{name}"
    try:
      return client.get_parameter(Name=parameter_path)["Parameter"]["Value"]
    except client.exceptions.ParameterNotFound:
      return default
    except Exception as e:
      logger.warning(f"Failed to retrieve parameter '{parameter_path}': {e}")
      return default

  def get_all_feature_flags(self) -> dict[str, str]:
    """Batch fetch every flag under /robosystems/{env}/features/."""
    if self.environment not in ["prod", "staging"]:
      return {}

    client = self._get_client()
    if client is None:
      return {}

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
          name = param["Name"].split("/")[-1]
          parameters[name] = param["Value"]

      self._batch_cache = (parameters, time.time())

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

  def get_tuning_parameter(self, path: str, default: str = "") -> str:
    """``path`` is relative to /robosystems/{env}/tuning/, e.g. "cache/BALANCE_TTL"."""
    if self.environment not in ["prod", "staging"]:
      return default

    client = self._get_client()
    if client is None:
      return default

    cache_key = f"tuning:{path}"

    if cache_key in self._cache:
      value, timestamp = self._cache[cache_key]
      if time.time() - timestamp < self.cache_ttl_seconds:
        return value
      else:
        del self._cache[cache_key]

    parameter_path = f"/robosystems/{self.environment}/tuning/{path}"

    try:
      response = client.get_parameter(Name=parameter_path)
      value = response["Parameter"]["Value"]

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
          full_name = param["Name"]
          prefix = f"/robosystems/{self.environment}/tuning/"
          if full_name.startswith(prefix):
            relative_path = full_name[len(prefix) :]
            parameters[relative_path] = param["Value"]

      current_time = time.time()
      for param_path, value in parameters.items():
        cache_key = f"tuning:{param_path}"
        self._cache[cache_key] = (value, current_time)

      logger.info(f"Loaded {len(parameters)} tuning parameters from SSM")
      return parameters

    except Exception as e:
      logger.warning(f"Failed to batch fetch tuning parameters: {e}")
      return {}


_parameter_manager: ParameterStoreManager | None = None


def get_parameter_manager() -> ParameterStoreManager:
  """Get or create the process-wide ParameterStoreManager."""
  global _parameter_manager
  if _parameter_manager is None:
    _parameter_manager = ParameterStoreManager()
  return _parameter_manager


def get_parameter_value(key: str, default: str = "") -> str:
  """Feature flag: env var, then SSM (prod/staging), then ``default``."""
  env_value = os.getenv(key)
  if env_value is not None:
    return env_value

  environment = os.getenv("ENVIRONMENT", "dev")
  if environment in ["prod", "staging"]:
    try:
      manager = get_parameter_manager()
      ssm_value = manager.get_parameter(key, default="")
      if ssm_value:
        return ssm_value
      else:
        print(
          f"WARNING: SSM parameter '{key}' returned empty, using default: '{default}'"
        )
    except Exception as e:
      print(f"WARNING: Failed to get SSM parameter '{key}': {type(e).__name__}: {e}")
      logger.warning(f"Failed to get parameter '{key}' from SSM: {e}")

  return default


def preload_feature_flags() -> dict[str, str]:
  """Warm the flag cache with one batched SSM call; {} outside prod/staging."""
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
