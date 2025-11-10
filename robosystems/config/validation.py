"""
Environment variable validation for startup checks.

This module provides validation functions to ensure all required
environment variables are properly configured at application startup.
"""

from typing import List, Dict, Any
import logging
import os

logger = logging.getLogger(__name__)


class ConfigValidationError(Exception):
  """Raised when configuration validation fails."""

  pass


class EnvValidator:
  """Validates environment configuration at startup."""

  @staticmethod
  def validate_required_vars(env_config) -> None:
    """
    Validate that all required environment variables are set.

    Args:
        env_config: The EnvConfig instance to validate

    Raises:
        ConfigValidationError: If validation fails
    """
    errors = []
    warnings = []

    # Critical variables that must be set in production
    if env_config.ENVIRONMENT == "prod":
      required_prod_vars = {
        "DATABASE_URL": "PostgreSQL connection string",
        "JWT_SECRET_KEY": "JWT signing key (must not be default)",
        "VALKEY_URL": "Valkey/Redis base URL",  # Changed from CELERY_BROKER_URL
        "AWS_REGION": "AWS region",
        "CONNECTION_CREDENTIALS_KEY": "Encryption key for credentials",
      }

      # Check for S3 credentials - IAM roles preferred, access keys for development only
      has_s3_credentials = getattr(
        env_config, "AWS_S3_ACCESS_KEY_ID", None
      ) and getattr(env_config, "AWS_S3_SECRET_ACCESS_KEY", None)

      # In production/staging, IAM roles are used automatically
      # In development, credentials are optional - can use AWS CLI profile or default chain
      # Only warn if no credentials are found and we're not in test/CI environment
      if (
        env_config.ENVIRONMENT not in ["prod", "staging", "test", "dev"]
        and not has_s3_credentials
        and not os.getenv("CI")
      ):
        warnings.append(
          "S3 credentials not found: Consider setting AWS_S3_ACCESS_KEY_ID/AWS_S3_SECRET_ACCESS_KEY "
          "or AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY, or use AWS CLI profile"
        )

      for var_name, description in required_prod_vars.items():
        value = getattr(env_config, var_name, None)
        if not value:
          errors.append(f"{var_name}: {description} is required in production")
        elif var_name == "JWT_SECRET_KEY":
          if "development" in str(value).lower() or "dev-jwt" in str(value).lower():
            errors.append(f"{var_name}: Must not use development default in production")
          elif len(str(value)) < 32:
            errors.append(f"{var_name}: Must be at least 32 characters for security")

    # Billing/Stripe validation - required when billing is enabled
    if getattr(env_config, "BILLING_ENABLED", False):
      stripe_vars = {
        "STRIPE_SECRET_KEY": "Stripe payment processing",
        "STRIPE_WEBHOOK_SECRET": "Stripe webhook verification",
      }
      for var_name, description in stripe_vars.items():
        value = getattr(env_config, var_name, None)
        if not value:
          errors.append(
            f"{var_name}: {description} is required when BILLING_ENABLED=true"
          )
        elif var_name == "STRIPE_SECRET_KEY":
          if value.startswith("sk_test_") and env_config.ENVIRONMENT == "prod":
            errors.append(
              f"{var_name}: Cannot use test key (sk_test_) in production environment"
            )
          elif not value.startswith(("sk_live_", "sk_test_")):
            errors.append(f"{var_name}: Must be a valid Stripe secret key")
        elif var_name == "STRIPE_WEBHOOK_SECRET":
          if not value.startswith("whsec_"):
            errors.append(f"{var_name}: Must be a valid Stripe webhook secret")

    # Variables that should be set for specific features
    feature_vars = {
      # QuickBooks integration
      "INTUIT_CLIENT_ID": "QuickBooks OAuth",
      "INTUIT_CLIENT_SECRET": "QuickBooks OAuth",
      # Plaid integration
      "PLAID_CLIENT_ID": "Plaid bank connections",
      "PLAID_CLIENT_SECRET": "Plaid bank connections",
      # Kuzu database
      "KUZU_DATABASE_PATH": "Kuzu database storage",
    }

    # Only check GRAPH_API_URL in dev/local environments
    if env_config.ENVIRONMENT in ["dev", "local"]:
      feature_vars["GRAPH_API_URL"] = "Graph API endpoint (local development)"

    for var_name, feature in feature_vars.items():
      value = getattr(env_config, var_name, None)
      if not value:
        warnings.append(f"{var_name}: Not configured - {feature} will not be available")

    # Special validation for Graph API key
    if (
      not getattr(env_config, "GRAPH_API_KEY", None)
      and env_config.ENVIRONMENT != "dev"
      and env_config.ENVIRONMENT != "local"
    ):
      warnings.append(
        "GRAPH_API_KEY: Not configured - "
        "Graph database operations will fail without proper authentication."
      )

    # Validate value ranges and formats
    EnvValidator._validate_numeric_ranges(env_config, errors)
    EnvValidator._validate_urls(env_config, errors)
    EnvValidator._validate_paths(env_config, warnings)

    # Report results
    if warnings:
      for warning in warnings:
        logger.warning(f"Config validation warning: {warning}")

    if errors:
      logger.error("Configuration validation failed:")
      for error in errors:
        logger.error(f"  - {error}")
      raise ConfigValidationError(
        f"Configuration validation failed with {len(errors)} errors. "
        "Please check environment variables."
      )

    logger.info("Configuration validation passed")

  @staticmethod
  def _validate_numeric_ranges(env_config, errors: List[str]) -> None:
    """Validate numeric configuration values are within reasonable ranges."""
    validations = [
      ("WORKER_AUTOSCALE", 1, 100, "Worker autoscale"),
      ("CELERY_TASK_TIME_LIMIT", 60, 7200, "Task time limit"),
      ("JWT_ACCESS_TOKEN_EXPIRE_MINUTES", 1, 1440, "JWT access token expiry"),
      ("RATE_LIMIT_API_KEY", 100, 1000000, "API key rate limit"),
      ("KUZU_MAX_DATABASES_PER_NODE", 1, 1000, "Max databases per node"),
    ]

    for var_name, min_val, max_val, description in validations:
      value = getattr(env_config, var_name, None)
      if value is not None:
        if not (min_val <= value <= max_val):
          errors.append(
            f"{var_name}: {description} must be between {min_val} and {max_val}, got {value}"
          )

  @staticmethod
  def _validate_urls(env_config, errors: List[str]) -> None:
    """Validate URL format for various endpoints."""
    url_vars = [
      "DATABASE_URL",
      "VALKEY_URL",  # CELERY_BROKER_URL is now auto-constructed
      "GRAPH_API_URL",
    ]

    # Also validate CELERY_BROKER_URL if it's explicitly set
    if getattr(env_config, "CELERY_BROKER_URL", None):
      url_vars.append("CELERY_BROKER_URL")

    for var_name in url_vars:
      value = getattr(env_config, var_name, None)
      if value and not (
        value.startswith(
          (
            "http://",
            "https://",
            "redis://",
            "rediss://",
            "postgresql://",
            "postgres://",
          )
        )
      ):
        errors.append(f"{var_name}: Invalid URL format - {value}")

      # Special validation for GRAPH_API_URL
      # In production, this should NOT be explicitly set via environment variable
      # The factory handles dynamic endpoint selection based on the database
      # The default value in env.py is fine and will be ignored by the factory
      if var_name == "GRAPH_API_URL" and env_config.ENVIRONMENT == "prod":
        import os

        # Only error if explicitly set via environment variable, not if using default
        if os.getenv("GRAPH_API_URL"):
          errors.append(
            f"{var_name}: Should not be explicitly set in production environment. "
            f"Dynamic endpoint selection is handled by the factory. Remove this environment variable."
          )

  @staticmethod
  def _validate_paths(env_config, warnings: List[str]) -> None:
    """Validate file paths exist or can be created."""
    import os

    path_vars = [
      ("KUZU_DATABASE_PATH", "Kuzu database directory"),
      ("LOG_FILE_PATH", "Log file directory"),
    ]

    for var_name, description in path_vars:
      value = getattr(env_config, var_name, None)
      if value and value not in ["stdout", "stderr"]:
        # Check if path exists or parent directory exists
        if not os.path.exists(value):
          parent_dir = os.path.dirname(value)
          if parent_dir and not os.path.exists(parent_dir):
            warnings.append(
              f"{var_name}: {description} path does not exist and parent directory "
              f"is missing - {value}"
            )

  @staticmethod
  def validate_startup(env_config) -> bool:
    """
    Perform startup validation and return success status.

    Args:
        env_config: The EnvConfig instance to validate

    Returns:
        bool: True if validation passed, False otherwise
    """
    try:
      EnvValidator.validate_required_vars(env_config)
      return True
    except ConfigValidationError as e:
      logger.error(f"Startup validation failed: {e}")
      return False

  @staticmethod
  def get_config_summary(env_config) -> Dict[str, Any]:
    """
    Get a summary of the current configuration for logging.

    Args:
        env_config: The EnvConfig instance

    Returns:
        Dict with configuration summary
    """
    from robosystems.config import AgentConfig

    agent_validation = AgentConfig.validate_configuration()

    return {
      "environment": env_config.ENVIRONMENT,
      "debug": env_config.DEBUG,
      "features": {
        "quickbooks": bool(env_config.INTUIT_CLIENT_ID),
        "plaid": bool(env_config.PLAID_CLIENT_ID),
        "sec": True,  # Always available
      },
      "database": {
        "type": "postgresql",
        "configured": bool(env_config.DATABASE_URL),
      },
      "kuzu": {
        "access_pattern": env_config.KUZU_ACCESS_PATTERN,
        "max_databases": env_config.KUZU_MAX_DATABASES_PER_NODE,
        "api_key_configured": bool(env_config.GRAPH_API_KEY),
      },
      "security": {
        "rate_limiting": env_config.RATE_LIMIT_ENABLED,
        "audit_logging": env_config.SECURITY_AUDIT_ENABLED,
      },
      "workers": {
        "autoscale": env_config.WORKER_AUTOSCALE,
      },
      "agents": {
        "config_valid": agent_validation["valid"],
        "default_model": AgentConfig.DEFAULT_MODEL_CONFIG.default_model.value,
        "fallback_agent": AgentConfig.ORCHESTRATOR_CONFIG["fallback_agent"],
        "available_models": len(AgentConfig.BEDROCK_MODELS),
        "execution_modes": len(AgentConfig.EXECUTION_PROFILES),
      },
    }
