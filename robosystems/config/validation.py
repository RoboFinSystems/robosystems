"""Startup validation of environment configuration."""

import logging
import os
from typing import Any

logger = logging.getLogger(__name__)


class ConfigValidationError(Exception):
  """Raised when configuration validation fails."""

  pass


class EnvValidator:
  """Validates environment configuration at startup."""

  # Flags whose code default is permissive; their presence in SSM is asserted
  # when deployed (the value may legitimately be false).
  SAFETY_CRITICAL_FEATURE_FLAGS: tuple[str, ...] = (
    "RATE_LIMIT_ENABLED",
    "BILLING_ENABLED",
    "CAPTCHA_ENABLED",
    "EMAIL_VERIFICATION_ENABLED",
    "USER_REGISTRATION_ENABLED",
  )

  @staticmethod
  def validate_required_vars(env_config) -> None:
    """Raise ConfigValidationError on any error; log warnings."""
    errors = []
    warnings = []

    backend_type = getattr(env_config, "GRAPH_BACKEND_TYPE", "ladybug")
    if backend_type != "ladybug":
      errors.append(
        f"GRAPH_BACKEND_TYPE='{backend_type}' is not supported. "
        f"Only 'ladybug' is supported."
      )

    if env_config.ENVIRONMENT in ("prod", "staging"):
      required_prod_vars = {
        "DATABASE_URL": "PostgreSQL connection string",
        "JWT_SECRET_KEY": "JWT signing key (must not be default)",
        "VALKEY_URL": "Valkey/Redis base URL",
        "AWS_REGION": "AWS region",
        "CONNECTION_CREDENTIALS_KEY": "Encryption key for credentials",
      }

      for var_name, description in required_prod_vars.items():
        value = getattr(env_config, var_name, None)
        if not value:
          errors.append(
            f"{var_name}: {description} is required in {env_config.ENVIRONMENT}"
          )
        elif var_name == "JWT_SECRET_KEY":
          if "development" in str(value).lower() or "dev-jwt" in str(value).lower():
            errors.append(
              f"{var_name}: Must not use the development default in "
              f"{env_config.ENVIRONMENT}"
            )
          elif len(str(value)) < 32:
            errors.append(f"{var_name}: Must be at least 32 characters for security")

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

    feature_vars = {
      # QuickBooks integration
      "INTUIT_CLIENT_ID": "QuickBooks OAuth",
      "INTUIT_CLIENT_SECRET": "QuickBooks OAuth",
      # LadybugDB database
      "LBUG_DATABASE_PATH": "LadybugDB database storage",
    }
    if getattr(env_config, "CONNECTION_MERCURY_ENABLED", False):
      feature_vars["MERCURY_CLIENT_ID"] = "Mercury OAuth"
      feature_vars["MERCURY_CLIENT_SECRET"] = "Mercury OAuth"
    if getattr(env_config, "CONNECTION_PLAID_ENABLED", False):
      feature_vars["PLAID_CLIENT_ID"] = "Plaid bank feed"
      feature_vars["PLAID_SECRET"] = "Plaid bank feed"

    if env_config.ENVIRONMENT in ["dev", "local"]:
      feature_vars["GRAPH_API_URL"] = "Graph API endpoint (local development)"

    for var_name, feature in feature_vars.items():
      value = getattr(env_config, var_name, None)
      if not value:
        warnings.append(f"{var_name}: Not configured - {feature} will not be available")

    if (
      not getattr(env_config, "GRAPH_API_KEY", None)
      and env_config.ENVIRONMENT != "dev"
      and env_config.ENVIRONMENT != "local"
    ):
      warnings.append(
        "GRAPH_API_KEY: Not configured - "
        "Graph database operations will fail without proper authentication."
      )

    # Fail at boot rather than as a confusing error at first login.
    deployed = env_config.ENVIRONMENT not in ("dev", "local", "test")
    if getattr(env_config, "SSO_OIDC_ENABLED", False):
      for var_name in (
        "SSO_OIDC_ISSUER",
        "SSO_OIDC_CLIENT_ID",
        "SSO_OIDC_CLIENT_SECRET",
      ):
        if not getattr(env_config, var_name, None):
          errors.append(f"{var_name}: Required when SSO_OIDC_ENABLED=true")
      issuer = getattr(env_config, "SSO_OIDC_ISSUER", "") or ""
      if issuer and deployed and not issuer.startswith("https://"):
        errors.append(
          "SSO_OIDC_ISSUER: Must be an https:// URL outside local development"
        )
      if "?" in issuer or "#" in issuer:
        errors.append("SSO_OIDC_ISSUER: Must not contain a query or fragment")
    if (
      not getattr(env_config, "PASSWORD_AUTH_ENABLED", True)
      and not getattr(env_config, "SSO_OIDC_ENABLED", False)
      and not getattr(env_config, "PASSKEYS_ENABLED", False)
    ):
      errors.append(
        "PASSWORD_AUTH_ENABLED=false requires SSO_OIDC_ENABLED=true or "
        "PASSKEYS_ENABLED=true — otherwise no one can log in to this deployment"
      )
    if getattr(env_config, "MFA_ENFORCEMENT_ENABLED", False) and not getattr(
      env_config, "PASSKEYS_ENABLED", False
    ):
      errors.append(
        "MFA_ENFORCEMENT_ENABLED=true requires PASSKEYS_ENABLED=true — "
        "enforcement gates password logins on a factor nobody could enroll"
      )
    if (
      deployed
      and getattr(env_config, "PASSKEYS_ENABLED", False)
      and (
        not getattr(env_config, "get_passkey_rp_id", lambda: "")()
        or not getattr(env_config, "get_passkey_origin", lambda: "")()
      )
    ):
      errors.append(
        "PASSKEYS_ENABLED=true but no WebAuthn RP ID/origin is derivable — set "
        "ROBOSYSTEMS_URL to the login home's URL, or PASSKEY_RP_ID and "
        "PASSKEY_ORIGIN explicitly"
      )
    if getattr(env_config, "SCIM_ENABLED", False):
      default_role = getattr(env_config, "SSO_DEFAULT_ROLE", "member")
      if default_role not in ("member", "admin"):
        errors.append(
          "SSO_DEFAULT_ROLE: Must be 'member' or 'admin' — IdP-provisioned "
          "users must not default to a privileged role"
        )
      # Warning, not error: the id exists only after the first bootstrap.
      if deployed and not getattr(env_config, "ENTERPRISE_ORG_ID", ""):
        warnings.append(
          "ENTERPRISE_ORG_ID: Unset while SCIM_ENABLED=true — SCIM/OIDC are "
          "not scoped to a single org. Pin it after the first scim bootstrap."
        )
    if (
      deployed
      and (
        getattr(env_config, "SSO_OIDC_ENABLED", False)
        or getattr(env_config, "SCIM_ENABLED", False)
        or getattr(env_config, "PASSKEYS_ENABLED", False)
      )
      and not getattr(env_config, "RATE_LIMIT_ENABLED", False)
    ):
      errors.append(
        "RATE_LIMIT_ENABLED: Required when SSO_OIDC_ENABLED, SCIM_ENABLED, or "
        "PASSKEYS_ENABLED — their auth rate buckets are no-ops without it"
      )

    # Flags resolve once at import, so a deployed boot that could not read SSM
    # would serve its whole life on permissive code defaults. Refuse to boot.
    if deployed:
      if not getattr(env_config, "PARAMETER_STORE_AVAILABLE", False):
        errors.append(
          "PARAMETER_STORE_AVAILABLE is False in a deployed environment — SSM "
          "could not be reached, so every feature flag would fall back to its "
          "code default. Refusing to boot on inert controls."
        )
      elif not getattr(env_config, "FEATURE_FLAGS_PRELOADED", False):
        errors.append(
          "No feature flags were preloaded from SSM in a deployed environment "
          "— the batched read returned nothing, so controls would run on code "
          "defaults. Refusing to boot on inert controls."
        )
      # Catches the symptom whatever the cause.
      if not getattr(env_config, "RATE_LIMIT_ENABLED", False):
        errors.append(
          "RATE_LIMIT_ENABLED is false in a deployed environment — this is the "
          "signature of an SSM read that fell back to defaults. Refusing to boot."
        )
      # Per flag: one missing parameter passes the store-level checks above.
      preloaded = getattr(env_config, "PRELOADED_FEATURE_FLAG_NAMES", frozenset())
      missing = [
        name
        for name in EnvValidator.SAFETY_CRITICAL_FEATURE_FLAGS
        if name not in preloaded
      ]
      if missing:
        errors.append(
          "Safety-critical feature flag(s) absent from SSM in a deployed "
          f"environment: {', '.join(missing)} — each would run on its code "
          "default. Set the parameter(s) under /features/ and redeploy."
        )

    EnvValidator._validate_urls(env_config, errors)
    EnvValidator._validate_paths(env_config, warnings)

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
  def _validate_urls(env_config, errors: list[str]) -> None:
    """Validate URL format for various endpoints."""
    url_vars = [
      "DATABASE_URL",
      "VALKEY_URL",
      "GRAPH_API_URL",
    ]

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

      # In prod the client factory selects endpoints; an explicit value is a
      # misconfiguration (the env.py default is ignored).
      if var_name == "GRAPH_API_URL" and env_config.ENVIRONMENT == "prod":
        import os

        if os.getenv("GRAPH_API_URL"):
          errors.append(
            f"{var_name}: Should not be explicitly set in production environment. "
            f"Dynamic endpoint selection is handled by the factory. Remove this environment variable."
          )

  @staticmethod
  def _validate_paths(env_config, warnings: list[str]) -> None:
    """Validate file paths exist or can be created."""
    path_vars = [
      ("LBUG_DATABASE_PATH", "LadybugDB database directory"),
      ("LOG_FILE_PATH", "Log file directory"),
    ]

    for var_name, description in path_vars:
      value = getattr(env_config, var_name, None)
      if value and value not in ["stdout", "stderr"]:
        if not os.path.exists(value):
          parent_dir = os.path.dirname(value)
          if parent_dir and not os.path.exists(parent_dir):
            warnings.append(
              f"{var_name}: {description} path does not exist and parent directory "
              f"is missing - {value}"
            )

  @staticmethod
  def validate_startup(env_config) -> bool:
    """:meth:`validate_required_vars` as a bool."""
    try:
      EnvValidator.validate_required_vars(env_config)
      return True
    except ConfigValidationError as e:
      logger.error(f"Startup validation failed: {e}")
      return False

  @staticmethod
  def get_config_summary(env_config) -> dict[str, Any]:
    """Configuration summary for logging."""
    from robosystems.config import OperatorConfig
    from robosystems.config.billing import BillingConfig

    operator_validation = OperatorConfig.validate_configuration()
    # Never raises, so it cannot block startup.
    billing_validation = BillingConfig.validate_configuration()

    return {
      "environment": env_config.ENVIRONMENT,
      "debug": env_config.DEBUG,
      "features": {
        "quickbooks": bool(env_config.INTUIT_CLIENT_ID),
        "mercury": bool(getattr(env_config, "MERCURY_CLIENT_ID", "")),
        "plaid": bool(getattr(env_config, "PLAID_CLIENT_ID", "")),
        "sec": True,  # Always available
      },
      "database": {
        "type": "postgresql",
        "configured": bool(env_config.DATABASE_URL),
      },
      "ladybug": {
        "access_pattern": env_config.LBUG_ACCESS_PATTERN,
        "api_key_configured": bool(env_config.GRAPH_API_KEY),
      },
      "security": {
        "rate_limiting": env_config.RATE_LIMIT_ENABLED,
        "audit_logging": env_config.SECURITY_AUDIT_ENABLED,
        "email_verification": env_config.EMAIL_VERIFICATION_ENABLED,
        "captcha": env_config.CAPTCHA_ENABLED,
      },
      "operators": {
        "config_valid": operator_validation["valid"],
        "default_profile": OperatorConfig.DEFAULT_MODEL_CONFIG.default_profile.value,
        "default_model": OperatorConfig.resolve_model().model_id,
        "fallback_operator": OperatorConfig.ORCHESTRATOR_CONFIG["fallback_operator"],
        "available_models": len(OperatorConfig.MODEL_REGISTRY),
        "execution_modes": len(OperatorConfig.EXECUTION_PROFILES),
      },
      "billing": {
        "config_valid": billing_validation["valid"],
        "issues": billing_validation["issues"],
        "billing_plans": billing_validation["summary"]["billing_plans"],
        "enabled": env_config.BILLING_ENABLED,
      },
    }
