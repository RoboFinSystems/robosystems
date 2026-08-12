"""Single source of truth for environment variables.

Read configuration through ``env`` rather than calling ``os.getenv`` directly —
the accessors here apply the type conversion, default, and validation each
variable is supposed to have, and tuning parameters additionally layer in SSM.

Organization (mirrors .env file section order):
- Helper functions for type-safe env var access
- Core application configuration (env, encryption, service URLs, JWT, email, CAPTCHA)
- Feature flags (security, graph ops, connections, shared repos, org, platform)
- Graph databases (API config, resiliency, tiers, LadybugDB)
- PostgreSQL
- Valkey/Redis
- Dagster configuration
- AWS configuration
- External service API keys
- Performance and scaling
- XBRL and Arelle
- Observability
"""

import os
from functools import lru_cache
from typing import TYPE_CHECKING, Any, Union

if TYPE_CHECKING:
  from .valkey_registry import ValkeyDatabase

# Import secrets manager with graceful fallback
# This handles cases where boto3 isn't installed or circular imports occur
try:
  from .secrets_manager import get_secret_list_value, get_secret_value

  SECRETS_MANAGER_AVAILABLE = True
except ImportError:
  # If secrets_manager can't be imported (missing boto3, circular import, etc),
  # provide a fallback that uses environment variables
  SECRETS_MANAGER_AVAILABLE = False

  def get_secret_value(key: str, default: str = "") -> str:
    """Fallback when secrets_manager is unavailable: read the env var."""
    return os.getenv(key, default)

  def get_secret_list_value(
    key: str, default: str = "", separator: str = ","
  ) -> list[str]:
    """Fallback when secrets_manager is unavailable: split the env var."""
    value = os.getenv(key, default)
    if not value:
      return []
    return [item.strip() for item in value.split(separator) if item.strip()]


# Import parameter store for feature flags (SSM Parameter Store)
# Feature flags use SSM instead of Secrets Manager for cost efficiency
try:
  from .parameter_store import get_parameter_value, preload_feature_flags

  PARAMETER_STORE_AVAILABLE = True

  # Preload all feature flags in a single batch API call before EnvConfig
  # class definition. This populates the cache so individual get_parameter_value
  # calls don't each make a separate SSM API call (which can fail under load).
  _preloaded_flags = preload_feature_flags()
  if _preloaded_flags:
    print(f"Preloaded {len(_preloaded_flags)} feature flags from SSM")
  elif os.getenv("ENVIRONMENT", "dev") in ("prod", "staging"):
    print("WARNING: No feature flags loaded from SSM. All flags will use defaults.")
except Exception as _e:
  # If parameter_store can't be imported, fall back to default values
  # Catch all exceptions (not just ImportError) to handle transitive failures
  PARAMETER_STORE_AVAILABLE = False
  print(
    f"WARNING: Parameter store unavailable ({type(_e).__name__}: {_e}). "
    "All feature flags will use defaults (false)."
  )

  def get_parameter_value(key: str, default: str = "") -> str:
    """
    Fallback implementation when parameter_store isn't available.
    Simply returns environment variable or default value.
    """
    return os.getenv(key, default)


# Import tunable defaults (runtime-adjustable via SSM)
from .defaults import (
  AdmissionDefaults,
  CacheDefaults,
  CircuitBreakerDefaults,
  LimitsDefaults,
  LoadSheddingDefaults,
  MCPDefaults,
  QueueDefaults,
  SSEDefaults,
  TimeoutDefaults,
)

# ==========================================================================
# HELPER FUNCTIONS FOR TYPE-SAFE ENVIRONMENT VARIABLE ACCESS
# ==========================================================================


def get_int_env(key: str, default: int) -> int:
  """Get an int env var, falling back to ``default`` if unset or unparseable."""
  try:
    return int(os.getenv(key, str(default)))
  except (ValueError, TypeError):
    # Use print instead of logger to avoid circular import
    print(f"Warning: Invalid {key} value, using default: {default}")
    return default


def get_float_env(key: str, default: float) -> float:
  """Get a float env var, falling back to ``default`` if unset or unparseable."""
  try:
    return float(os.getenv(key, str(default)))
  except (ValueError, TypeError):
    # Use print instead of logger to avoid circular import
    print(f"Warning: Invalid {key} value, using default: {default}")
    return default


def get_bool_env(key: str, default: bool = False) -> bool:
  """Get a bool env var; "true", "1", "yes", and "on" are true, all else false."""
  value = os.getenv(key, str(default)).lower()
  return value in ("true", "1", "yes", "on")


def get_str_env(key: str, default: str = "") -> str:
  """Get a string env var."""
  return os.getenv(key, default)


def _tuning_env_key(ssm_path: str) -> str:
  """Env-var name for a tuning param following the documented convention.

  ``TUNING_{CATEGORY}_{KEY}`` derived from the SSM path — e.g.
  ``"graphql/MAX_DEPTH"`` -> ``"TUNING_GRAPHQL_MAX_DEPTH"``. Mirrors the naming
  in ``config/tuning.py`` and ``.env.example`` so the documented override
  convention is actually honored (the raw ``env_key`` still works too).
  """
  return "TUNING_" + ssm_path.upper().replace("/", "_")


def get_tuning_float(env_key: str, ssm_path: str, default: float) -> float:
  """
  Get a float tuning parameter with layered fallback.

  Priority order:
  1. Environment variable (local dev, CI, testing) — either the raw ``env_key``
     or the ``TUNING_{CATEGORY}_{KEY}`` form derived from ``ssm_path``
  2. SSM Parameter Store under ``tuning/`` (prod/staging only, applies without
     a redeploy)
  3. ``default``
  """
  # Priority 1: Environment variable — accept both the raw env_key (backward
  # compat) and the documented TUNING_{CATEGORY}_{KEY} convention.
  for candidate in (env_key, _tuning_env_key(ssm_path)):
    env_value = os.getenv(candidate)
    if env_value is not None:
      try:
        return float(env_value)
      except (ValueError, TypeError):
        print(f"Warning: Invalid {env_key} value, using default: {default}")
        return default

  # Priority 2: SSM Parameter Store (prod/staging only)
  environment = os.getenv("ENVIRONMENT", "dev")
  if environment in ["prod", "staging"]:
    try:
      from .parameter_store import get_parameter_manager

      manager = get_parameter_manager()
      ssm_value = manager.get_tuning_float(ssm_path, default)
      return ssm_value
    except Exception:
      pass  # Fall through to default

  # Priority 3: Default value
  return default


def get_tuning_int(env_key: str, ssm_path: str, default: int) -> int:
  """
  Get an integer tuning parameter with layered fallback.

  Priority order:
  1. Environment variable (local dev, CI, testing) — either the raw ``env_key``
     or the ``TUNING_{CATEGORY}_{KEY}`` form derived from ``ssm_path``
  2. SSM Parameter Store under ``tuning/`` (prod/staging only, applies without
     a redeploy)
  3. ``default``
  """
  # Priority 1: Environment variable — accept both the raw env_key (backward
  # compat) and the documented TUNING_{CATEGORY}_{KEY} convention.
  for candidate in (env_key, _tuning_env_key(ssm_path)):
    env_value = os.getenv(candidate)
    if env_value is not None:
      try:
        return int(env_value)
      except (ValueError, TypeError):
        print(f"Warning: Invalid {env_key} value, using default: {default}")
        return default

  # Priority 2: SSM Parameter Store (prod/staging only)
  environment = os.getenv("ENVIRONMENT", "dev")
  if environment in ["prod", "staging"]:
    try:
      from .parameter_store import get_parameter_manager

      manager = get_parameter_manager()
      ssm_value = manager.get_tuning_int(ssm_path, default)
      return ssm_value
    except Exception:
      pass  # Fall through to default

  # Priority 3: Default value
  return default


def get_list_env(key: str, default: str = "", separator: str = ",") -> list[str]:
  """Get a list env var, splitting on ``separator`` and trimming each item."""
  value = os.getenv(key, default)
  if not value:
    return []
  return [item.strip() for item in value.split(separator) if item.strip()]


# Cache for CloudFormation lookups (avoid repeated API calls)
_cloudformation_cache: dict[str, str | None] = {}


def _get_cf_stack_output(stack_name: str, output_key: str) -> str:
  """Look up a CloudFormation stack output by key, with caching.

  Args:
      stack_name: Full CloudFormation stack name
      output_key: The OutputKey to find

  Returns:
      Output value or empty string if not found
  """
  cache_key = f"{stack_name}:{output_key}"
  if cache_key in _cloudformation_cache:
    return _cloudformation_cache[cache_key] or ""

  try:
    import boto3

    region = os.getenv("AWS_REGION", "us-east-1")
    cf_client = boto3.client("cloudformation", region_name=region)
    response = cf_client.describe_stacks(StackName=stack_name)

    if "Stacks" in response and len(response["Stacks"]) > 0:
      stack = response["Stacks"][0]
      for output in stack.get("Outputs", []):
        if output.get("OutputKey") == output_key:
          value = output.get("OutputValue", "")
          _cloudformation_cache[cache_key] = value
          return value

    _cloudformation_cache[cache_key] = None
    return ""

  except ImportError:
    _cloudformation_cache[cache_key] = None
    return ""
  except Exception as e:
    print(f"CloudFormation lookup failed for {stack_name}:{output_key}: {e}")
    _cloudformation_cache[cache_key] = None
    return ""


def get_volume_manager_function_arn() -> str:
  """Look up the Volume Manager Lambda ARN from CloudFormation exports.

  Stack: RoboSystemsGraphVolumes{Prod|Staging}
  Output: VolumeManagerFunctionArn
  """
  environment = os.getenv("ENVIRONMENT", "dev")
  if environment not in ["prod", "staging"]:
    return ""

  env_suffix = "Prod" if environment == "prod" else "Staging"
  stack_name = f"RoboSystemsGraphVolumes{env_suffix}"
  return _get_cf_stack_output(stack_name, "VolumeManagerFunctionArn")


def _get_shared_replica_alb_url_from_cloudformation() -> str:
  """Auto-discover the shared replica ALB URL from CloudFormation.

  Reads the ``ALBEndpoint`` output of the
  ``RoboSystemsGraphSharedReplicas{Prod|Staging}`` stack. Returns "" outside
  prod/staging, or when the stack is not deployed.
  """
  cache_key = "shared_replica_alb_url"
  if cache_key in _cloudformation_cache:
    return _cloudformation_cache[cache_key] or ""

  try:
    import boto3

    environment = os.getenv("ENVIRONMENT", "dev")
    if environment not in ["prod", "staging"]:
      _cloudformation_cache[cache_key] = None
      return ""

    # Stack name follows pattern: RoboSystemsGraphSharedReplicas{Prod|Staging}
    env_suffix = "Prod" if environment == "prod" else "Staging"
    stack_name = f"RoboSystemsGraphSharedReplicas{env_suffix}"

    region = os.getenv("AWS_REGION", "us-east-1")
    cf_client = boto3.client("cloudformation", region_name=region)
    response = cf_client.describe_stacks(StackName=stack_name)

    if "Stacks" in response and len(response["Stacks"]) > 0:
      stack = response["Stacks"][0]
      if "Outputs" in stack:
        for output in stack["Outputs"]:
          if output.get("OutputKey") == "ALBEndpoint":
            url = output.get("OutputValue", "")
            _cloudformation_cache[cache_key] = url
            return url

    _cloudformation_cache[cache_key] = None
    return ""

  except ImportError:
    _cloudformation_cache[cache_key] = None
    return ""
  except Exception:
    print(
      "Shared replica ALB: not configured (stack RoboSystemsGraphSharedReplicas not deployed)"
    )
    _cloudformation_cache[cache_key] = None
    return ""


# ==========================================================================
# MAIN CONFIGURATION CLASS
# ==========================================================================


class EnvConfig:
  """Every environment variable, grouped to match the .env file layout.

  Class attributes are resolved once at import; the ``get_*`` classmethods
  compute derived configuration on each call.
  """

  # ==========================================================================
  # 1. CORE APPLICATION CONFIGURATION
  # ==========================================================================

  # Environment and debugging
  ENVIRONMENT = get_str_env("ENVIRONMENT", "dev")
  DEBUG = get_bool_env("DEBUG", False)
  LOG_LEVEL = get_str_env("LOG_LEVEL", "INFO")

  # Secrets Manager availability (set at module level during import)
  SECRETS_MANAGER_AVAILABLE = SECRETS_MANAGER_AVAILABLE

  # Server configuration
  HOST = get_str_env("HOST", "0.0.0.0")
  PORT = get_int_env("PORT", 8000)

  # Encryption Keys
  CONNECTION_CREDENTIALS_KEY = get_secret_value("CONNECTION_CREDENTIALS_KEY", "")

  # Service URLs
  # ROBOSYSTEMS_API_URL is set by CloudFormation based on access mode (domain or ALB DNS)
  ROBOSYSTEMS_API_URL = get_str_env("ROBOSYSTEMS_API_URL", "https://api.robosystems.ai")
  # Frontend URLs - set via CloudFormation environment variables
  ROBOLEDGER_URL = get_str_env("ROBOLEDGER_URL", "https://roboledger.ai")
  ROBOINVESTOR_URL = get_str_env("ROBOINVESTOR_URL", "https://roboinvestor.ai")
  ROBOSYSTEMS_URL = get_str_env("ROBOSYSTEMS_URL", "https://robosystems.ai")
  # Which app hosts the interactive auth surface ("login home"). Single-app
  # deployments designate their own app key here.
  LOGIN_HOME_APP = get_str_env("LOGIN_HOME_APP", "robosystems")

  # JWT configuration
  JWT_SECRET_KEY = get_secret_value("JWT_SECRET_KEY", "")

  # JWT Issuer and Audience - configurable for different deployments
  # Default JWT_ISSUER is derived from ROBOSYSTEMS_API_URL (strips protocol)
  # Override via Secrets Manager for custom deployments:
  # - internal mode: set JWT_ISSUER=localhost (access via SSM tunnel)
  # - public mode: uses custom domain automatically, or set custom value
  # Derive default JWT issuer/audience from ROBOSYSTEMS_API_URL, stripping protocol
  _jwt_default_domain = (
    os.getenv("ROBOSYSTEMS_API_URL", "https://api.robosystems.ai")
    .replace("https://", "")
    .replace("http://", "")
  )
  JWT_ISSUER = get_secret_value("JWT_ISSUER", _jwt_default_domain)
  JWT_AUDIENCE = get_secret_list_value("JWT_AUDIENCE", _jwt_default_domain)

  # Email service configuration
  EMAIL_FROM_ADDRESS = get_str_env(
    "EMAIL_FROM_ADDRESS",
    get_secret_value("EMAIL_FROM_ADDRESS", "noreply@robosystems.ai"),
  )
  EMAIL_FROM_NAME = get_str_env(
    "EMAIL_FROM_NAME",
    get_secret_value("EMAIL_FROM_NAME", "RoboSystems"),
  )

  # Cloudflare Turnstile (CAPTCHA)
  TURNSTILE_SECRET_KEY = get_secret_value("TURNSTILE_SECRET_KEY", "")
  TURNSTILE_SITE_KEY = get_secret_value("TURNSTILE_SITE_KEY", "")

  # Cloudflare R2 (S3-compatible object storage for zero-egress downloads)
  R2_ACCESS_KEY_ID = get_secret_value("R2_ACCESS_KEY_ID", "")
  R2_SECRET_ACCESS_KEY = get_secret_value("R2_SECRET_ACCESS_KEY", "")
  R2_ENDPOINT_URL = get_secret_value("R2_ENDPOINT_URL", "")
  R2_BUCKET_NAME = get_secret_value("R2_BUCKET_NAME", "")
  R2_PUBLIC_BUCKET_NAME = get_secret_value(
    "R2_PUBLIC_BUCKET_NAME", ""
  )  # Public bucket for artifacts
  R2_PUBLIC_URL = get_secret_value(
    "R2_PUBLIC_URL", ""
  )  # Public bucket URL (no auth needed)

  # ==========================================================================
  # 2. FEATURE FLAGS
  # ==========================================================================
  # Feature flags use SSM Parameter Store instead of Secrets Manager.
  # This provides cost savings (SSM Standard tier is FREE) and better
  # separation between secrets (credentials) and configuration (flags).
  #
  # Override priority: env var > SSM Parameter Store > default value

  # --- Platform Operations ---
  USER_REGISTRATION_ENABLED = get_bool_env(
    "USER_REGISTRATION_ENABLED",
    get_parameter_value("USER_REGISTRATION_ENABLED", "true").lower() == "true",
  )
  # For forked/self-hosted deployments: Set BILLING_ENABLED=false in SSM Parameter Store
  # This disables payment requirements since you're paying for your own infrastructure
  BILLING_ENABLED = get_bool_env(
    "BILLING_ENABLED",
    get_parameter_value("BILLING_ENABLED", "false").lower() == "true",
  )
  SSE_ENABLED = get_bool_env(
    "SSE_ENABLED",
    get_parameter_value("SSE_ENABLED", "true").lower() == "true",
  )
  RATE_LIMIT_ENABLED = get_bool_env(
    "RATE_LIMIT_ENABLED",
    get_parameter_value("RATE_LIMIT_ENABLED", "false").lower() == "true",
  )
  LOAD_SHEDDING_ENABLED = get_bool_env(
    "LOAD_SHEDDING_ENABLED",
    get_parameter_value("LOAD_SHEDDING_ENABLED", "true").lower() == "true",
  )
  OTEL_ENABLED = get_bool_env(
    "OTEL_ENABLED",
    get_parameter_value("OTEL_ENABLED", "false").lower() == "true",
  )
  # --- Security & Authentication ---
  SECURITY_AUDIT_ENABLED = get_bool_env(
    "SECURITY_AUDIT_ENABLED",
    get_parameter_value("SECURITY_AUDIT_ENABLED", "false").lower() == "true",
  )
  EMAIL_VERIFICATION_ENABLED = get_bool_env(
    "EMAIL_VERIFICATION_ENABLED",
    get_parameter_value("EMAIL_VERIFICATION_ENABLED", "false").lower() == "true",
  )
  CAPTCHA_ENABLED = get_bool_env(
    "CAPTCHA_ENABLED",
    get_parameter_value("CAPTCHA_ENABLED", "false").lower() == "true",
  )
  # Auth posture flags surfaced by GET /v1/auth/providers so the login home
  # renders the deployment's auth methods from runtime config.
  PASSWORD_AUTH_ENABLED = get_bool_env(
    "PASSWORD_AUTH_ENABLED",
    get_parameter_value("PASSWORD_AUTH_ENABLED", "true").lower() == "true",
  )
  SSO_OIDC_ENABLED = get_bool_env(
    "SSO_OIDC_ENABLED",
    get_parameter_value("SSO_OIDC_ENABLED", "false").lower() == "true",
  )
  # The SSO connection block lives in the base robosystems/{env} secret on
  # deployed environments (JWT_ISSUER precedent — not secrets per se, but
  # they ride with SSO_OIDC_CLIENT_SECRET so the whole connection is one
  # operator surface). Env vars win everywhere, which is the local-dev path.
  SSO_OIDC_PROVIDER_LABEL = get_secret_value("SSO_OIDC_PROVIDER_LABEL", "SSO")
  # The issuer is the IdP's org authorization server (e.g.
  # https://<org>.okta.com — NOT /oauth2/default, which mints API access
  # tokens rather than sign-in ID tokens).
  SSO_OIDC_ISSUER = get_secret_value("SSO_OIDC_ISSUER", "")
  SSO_OIDC_CLIENT_ID = get_secret_value("SSO_OIDC_CLIENT_ID", "")
  # SCIM 2.0 provisioning surface (IdP pushes users into the enterprise org).
  # Gated independently of OIDC — a deployment may run one without the other.
  SCIM_ENABLED = get_bool_env(
    "SCIM_ENABLED",
    get_parameter_value("SCIM_ENABLED", "false").lower() == "true",
  )
  # Org role SCIM-provisioned users join the enterprise org with.
  SSO_DEFAULT_ROLE = get_secret_value("SSO_DEFAULT_ROLE", "member")
  # Pins the single org this deployment's SCIM/OIDC surface operates against.
  # Set after the first `scim bootstrap` mints the enterprise org (the id only
  # exists then); once set, bearer tokens for other orgs are refused, bootstrap
  # can only target this org, and OIDC first-login linking requires membership.
  # Rides in the base secret with the SSO connection block.
  ENTERPRISE_ORG_ID = get_secret_value("ENTERPRISE_ORG_ID", "")
  # ID-token claim compared against the SCIM-provisioned external_id at
  # first-login linking. Okta: externalId ≡ sub. Entra pairs SCIM externalId
  # (objectId) with the `oid` claim, not `sub` — override there.
  SSO_OIDC_BINDING_CLAIM = get_secret_value("SSO_OIDC_BINDING_CLAIM", "sub")
  PASSKEYS_ENABLED = get_bool_env(
    "PASSKEYS_ENABLED",
    get_parameter_value("PASSKEYS_ENABLED", "false").lower() == "true",
  )
  # When enabled, verification/reset/invitation email links target the login
  # home instead of the originating app.
  AUTH_EMAIL_LINKS_TO_LOGIN_HOME = get_bool_env(
    "AUTH_EMAIL_LINKS_TO_LOGIN_HOME",
    get_parameter_value("AUTH_EMAIL_LINKS_TO_LOGIN_HOME", "false").lower() == "true",
  )

  # --- Organization ---
  ORG_MEMBER_INVITATIONS_ENABLED = get_bool_env(
    "ORG_MEMBER_INVITATIONS_ENABLED",
    get_parameter_value("ORG_MEMBER_INVITATIONS_ENABLED", "false").lower() == "true",
  )
  # Organization limits (SSM: /tuning/limits/)
  ORG_GRAPHS_DEFAULT_LIMIT = get_tuning_int(
    "ORG_GRAPHS_DEFAULT_LIMIT",
    "limits/ORG_GRAPHS_DEFAULT",
    LimitsDefaults.ORG_GRAPHS_DEFAULT,
  )

  # --- Graph Operations ---
  DIRECT_GRAPH_MATERIALIZATION_ENABLED = get_bool_env(
    "DIRECT_GRAPH_MATERIALIZATION_ENABLED",
    get_parameter_value("DIRECT_GRAPH_MATERIALIZATION_ENABLED", "true").lower()
    == "true",
  )
  SUBGRAPH_CREATION_ENABLED = get_bool_env(
    "SUBGRAPH_CREATION_ENABLED",
    get_parameter_value("SUBGRAPH_CREATION_ENABLED", "true").lower() == "true",
  )
  BACKUP_CREATION_ENABLED = get_bool_env(
    "BACKUP_CREATION_ENABLED",
    get_parameter_value("BACKUP_CREATION_ENABLED", "true").lower() == "true",
  )
  OPERATOR_POST_ENABLED = get_bool_env(
    "OPERATOR_POST_ENABLED",
    get_parameter_value("OPERATOR_POST_ENABLED", "true").lower() == "true",
  )
  FACT_GRID_ENABLED = get_bool_env(
    "FACT_GRID_ENABLED",
    get_parameter_value("FACT_GRID_ENABLED", "true").lower() == "true",
  )
  MCP_AUTO_LIMIT_ENABLED = get_bool_env(
    "MCP_AUTO_LIMIT_ENABLED",
    get_parameter_value("MCP_AUTO_LIMIT_ENABLED", "true").lower() == "true",
  )
  MCP_WORKSPACE_ENABLED = get_bool_env(
    "MCP_WORKSPACE_ENABLED",
    get_parameter_value("MCP_WORKSPACE_ENABLED", "true").lower() == "true",
  )
  # Gates the subgraph write/DDL MCP tools (write-graph-cypher, add-node-table,
  # add-relationship-table). These operate on subgraphs only — the main graph is
  # read-only to raw statements (see StatementKernel / _validate_subgraph_context).
  MCP_SUBGRAPH_OPS_ENABLED = get_bool_env(
    "MCP_SUBGRAPH_OPS_ENABLED",
    get_parameter_value("MCP_SUBGRAPH_OPS_ENABLED", "true").lower() == "true",
  )
  MCP_SEMANTIC_MEMORY_ENABLED = get_bool_env(
    "MCP_SEMANTIC_MEMORY_ENABLED",
    get_parameter_value("MCP_SEMANTIC_MEMORY_ENABLED", "false").lower() == "true",
  )
  MCP_GRAPHQL_ENABLED = get_bool_env(
    "MCP_GRAPHQL_ENABLED",
    get_parameter_value("MCP_GRAPHQL_ENABLED", "true").lower() == "true",
  )
  SEMANTIC_SEARCH_ENABLED = get_bool_env(
    "SEMANTIC_SEARCH_ENABLED",
    get_parameter_value("SEMANTIC_SEARCH_ENABLED", "true").lower() == "true",
  )
  # Master gate for AI semantic memory (REST + ops + recall + governance).
  # MCP_SEMANTIC_MEMORY_ENABLED is an additional sub-gate for the MCP tools.
  SEMANTIC_MEMORY_ENABLED = get_bool_env(
    "SEMANTIC_MEMORY_ENABLED",
    get_parameter_value("SEMANTIC_MEMORY_ENABLED", "false").lower() == "true",
  )
  # Gates tenant authoring of framework-shaped taxonomy blocks
  # (reporting_extension / custom_ontology) on create + update; delete stays
  # open so gated content can always be removed. chart_of_accounts is never
  # gated — it is the core product path. Fail-closed in prod/staging: the
  # default is "false" there, so a missing SSM parameter cannot open the
  # surface. Dev/test default on (the scenario demos author
  # reporting_extension disclosure notes).
  TAXONOMY_AUTHORING_ENABLED = get_bool_env(
    "TAXONOMY_AUTHORING_ENABLED",
    get_parameter_value(
      "TAXONOMY_AUTHORING_ENABLED",
      "false" if ENVIRONMENT in ("prod", "staging") else "true",
    ).lower()
    == "true",
  )

  # --- OpenSearch ---
  OPENSEARCH_URL = get_str_env("OPENSEARCH_URL", "http://localhost:9200")
  OPENSEARCH_INDEX = get_str_env("OPENSEARCH_INDEX", "documents")

  # --- Shared Repository Operations ---
  SHARED_MASTER_READS_ENABLED = get_bool_env(
    "SHARED_MASTER_READS_ENABLED",
    get_parameter_value("SHARED_MASTER_READS_ENABLED", "true").lower() == "true",
  )
  # Shared Replica ALB URL (for read scaling)
  # When set, reads to shared repositories will route to the replica ALB
  # instead of the shared master. This allows horizontal scaling of reads.
  # Auto-discovered from CloudFormation if not explicitly set via env var
  # Format: http://internal-robosystems-shared-{env}.{region}.elb.amazonaws.com:8001
  SHARED_REPLICA_ALB_URL = (
    get_str_env("SHARED_REPLICA_ALB_URL", "")
    or _get_shared_replica_alb_url_from_cloudformation()
  )
  # Shared repositories list for infrastructure/deployment (used by userdata scripts)
  # This configures which repositories should be deployed on shared writer instances
  # For application logic (checking if a graph is a shared repo), use config.shared_repositories
  SHARED_REPOSITORIES = get_list_env("SHARED_REPOSITORIES", "")

  # --- Connection Providers ---
  # CONNECTIONS_ENABLED controls whether the /connections router is included.
  # Code defaults to TRUE — the fully-featured platform exposes connection
  # providers. Prod and staging currently override to `false` via SSM during
  # the staged rollout (flipped at the same time as the extensions). Individual
  # provider flags below require CONNECTIONS_ENABLED=true to function.
  CONNECTIONS_ENABLED = get_bool_env(
    "CONNECTIONS_ENABLED",
    get_parameter_value("CONNECTIONS_ENABLED", "true").lower() == "true",
  )
  # Individual provider flags (require CONNECTIONS_ENABLED=true)
  # Defaults off: the SEC connection is a filing-driven entity-creation path
  # that predates the shared SEC repository, and it is disabled in every
  # deployed environment. Keeping the default on meant local dev registered a
  # provider prod does not, and a fresh deployment would have shipped it live.
  # The provider itself is intact — flip the flag to bring it back.
  CONNECTION_SEC_ENABLED = get_bool_env(
    "CONNECTION_SEC_ENABLED",
    get_parameter_value("CONNECTION_SEC_ENABLED", "false").lower() == "true",
  )
  CONNECTION_QUICKBOOKS_ENABLED = get_bool_env(
    "CONNECTION_QUICKBOOKS_ENABLED",
    get_parameter_value("CONNECTION_QUICKBOOKS_ENABLED", "true").lower() == "true",
  )
  # External provider: registers a source namespace for integrations that
  # run outside the platform and write through the public API (the
  # event-source registry). No credentials, no sync — registration only.
  CONNECTION_EXTERNAL_ENABLED = get_bool_env(
    "CONNECTION_EXTERNAL_ENABLED",
    get_parameter_value("CONNECTION_EXTERNAL_ENABLED", "true").lower() == "true",
  )

  # Routes QB Reports API calls (JournalReport — our live GL posting source)
  # through Intuit's modernized "v2" reporting service via the
  # `testing_migration` query param. Defaults TRUE: we cut over to v2 ahead of
  # Intuit's 2026-08-31 hard cutover (validated to parse identically to v1).
  # Set FALSE via SSM (features/INTUIT_REPORTS_TESTING_MIGRATION) to fall back
  # to v1 at runtime with no redeploy — but ONLY until 2026-08-31, after which
  # Intuit serves v2 unconditionally and the param becomes a no-op. Retire this
  # flag (and the get_transactions param) once that cutover lands.
  # https://medium.com/intuitdev/upcoming-changes-to-reports-apis-5083ec9aadce
  INTUIT_REPORTS_TESTING_MIGRATION = get_bool_env(
    "INTUIT_REPORTS_TESTING_MIGRATION",
    get_parameter_value("INTUIT_REPORTS_TESTING_MIGRATION", "true").lower() == "true",
  )

  # SEC shared-master parking on/off (ASG name/timeout live in section 3).
  # When False, the nightly SEC pipeline still wakes and health-gates the master
  # (staging depends on the wake job succeeding) but never sleeps it back to 0 —
  # so a reserved-instance-backed master stays pinned awake and utilized. Flip to
  # True via SSM (features/SHARED_MASTER_PARKING_ENABLED) once the master is 100%
  # on-demand to reclaim the ~85% of idle hours it otherwise sits unused.
  SHARED_MASTER_PARKING_ENABLED = get_bool_env(
    "SHARED_MASTER_PARKING_ENABLED",
    get_parameter_value("SHARED_MASTER_PARKING_ENABLED", "true").lower() == "true",
  )

  # ==========================================================================
  # EXTENSIONS — RoboLedger & RoboInvestor product surfaces
  # ==========================================================================
  #
  # Two per-domain flags gate the extension surfaces:
  #
  #   - ROBOLEDGER_ENABLED → mounts the roboledger ops router and exposes
  #     ledger resolvers on the GraphQL endpoint
  #   - ROBOINVESTOR_ENABLED → mounts the roboinvestor ops router and
  #     exposes investor resolvers on the GraphQL endpoint
  #
  # `EXTENSIONS_ENABLED` is **derived** (see property below) — it's true
  # whenever either domain is on. There's no scenario where you'd want
  # the extensions PostgreSQL connection without at least one domain
  # enabled, so it's not a separate user-facing flag.
  #
  # Code defaults to TRUE — the fully-featured platform exposes both
  # product surfaces. Prod and staging currently override to `false` via
  # SSM during the staged extensions rollout; flip the SSM values to `true`
  # when each domain is ready to ship to that environment.
  ROBOLEDGER_ENABLED = get_bool_env(
    "ROBOLEDGER_ENABLED",
    get_parameter_value("ROBOLEDGER_ENABLED", "true").lower() == "true",
  )

  ROBOINVESTOR_ENABLED = get_bool_env(
    "ROBOINVESTOR_ENABLED",
    get_parameter_value("ROBOINVESTOR_ENABLED", "true").lower() == "true",
  )

  # --- Extensions GraphQL Endpoint ---
  # Controls the /extensions/{graph_id}/graphql endpoint (Strawberry).
  #
  # Defaults to TRUE alongside the per-domain ROBOLEDGER/ROBOINVESTOR flags
  # so a fresh deployment with extensions on automatically gets the read
  # surface (legacy /v1/ledger/* and /v1/investor/* routers are gone — this
  # GraphQL endpoint is the only way to read extensions data). Kept as an
  # independent kill switch for incident response (e.g. lock down
  # introspection without disabling write operations). Prod/staging
  # currently override to `false` via SSM during the staged rollout.
  EXTENSIONS_GRAPHQL_ENABLED = get_bool_env(
    "EXTENSIONS_GRAPHQL_ENABLED",
    get_parameter_value("EXTENSIONS_GRAPHQL_ENABLED", "true").lower() == "true",
  )

  # Query-bounding limits for the extensions GraphQL endpoint. The OLTP pool
  # is small and each resolved field can open a session, so an unbounded
  # nested / alias-heavy / oversized document is a DoS vector. Defaults are
  # generous for legitimate nested reads; introspection is unaffected (the
  # depth limiter does not count introspection fields). SSM-tunable at runtime
  # (like the other query/pool limits) so a limit can be tightened on abuse or
  # loosened for a false-positive block without a redeploy —
  # `just ssm-set <env> tuning/graphql/MAX_DEPTH <n>`.
  EXTENSIONS_GRAPHQL_MAX_DEPTH = get_tuning_int(
    "EXTENSIONS_GRAPHQL_MAX_DEPTH", "graphql/MAX_DEPTH", 15
  )
  EXTENSIONS_GRAPHQL_MAX_ALIASES = get_tuning_int(
    "EXTENSIONS_GRAPHQL_MAX_ALIASES", "graphql/MAX_ALIASES", 30
  )
  EXTENSIONS_GRAPHQL_MAX_TOKENS = get_tuning_int(
    "EXTENSIONS_GRAPHQL_MAX_TOKENS", "graphql/MAX_TOKENS", 2000
  )

  # --- Derived: EXTENSIONS_ENABLED ---
  # Whether the extensions PostgreSQL database should open at all.
  # Computed at class-body evaluation time from the per-domain flags
  # above (Python evaluates class bodies sequentially, so the names are
  # in scope here). Replaces the standalone `EXTENSIONS_ENABLED` env var.
  EXTENSIONS_ENABLED = ROBOLEDGER_ENABLED or ROBOINVESTOR_ENABLED

  # Period-boundary obligation promoter. When True, the Dagster sensor
  # not only flips matured `pending` schedule_entry_due events to
  # `classified` but also dispatches the registered handler so the
  # closing-entry draft lands in the GL on the same tick. Defaults to
  # False (co-pilot mode): status flips, drafts wait for an explicit
  # operator action. The per-graph autopilot column on the Graph row
  # overrides this default when set.
  EXTENSIONS_PROMOTION_AUTO_DISPATCH = get_bool_env(
    "EXTENSIONS_PROMOTION_AUTO_DISPATCH",
    get_parameter_value("EXTENSIONS_PROMOTION_AUTO_DISPATCH", "false").lower()
    == "true",
  )

  # --- Adapter Pipelines (Dagster) ---
  # Controls whether adapter-specific Dagster assets, jobs, sensors, and schedules
  # are loaded into the Dagster definitions. Disabling gives a clean Dagster slate.
  SEC_PIPELINE_ENABLED = get_bool_env(
    "SEC_PIPELINE_ENABLED",
    get_parameter_value("SEC_PIPELINE_ENABLED", "true").lower() == "true",
  )

  # ==========================================================================
  # 3. GRAPH DATABASES (LADYBUGDB)
  # ==========================================================================

  GRAPH_BACKEND_TYPE = get_str_env("GRAPH_BACKEND_TYPE", "ladybug")

  # SHACL validation of the JSON-LD report bundle at publish time, against
  # frameworks/ontology/v1/shapes.ttl. Opt-in (default off) so the publish
  # path stays fast — the standalone validator + the SHACL regression test
  # cover the demos/CI. When enabled, the structured result is logged onto
  # Report.metadata['bundle_validation'].
  #   off    — skip (default)
  #   warn   — validate, record the result, never block the publish
  #   strict — validate, record, and raise on non-conformance (block publish)
  REPORT_BUNDLE_SHACL_VALIDATION = get_str_env("REPORT_BUNDLE_SHACL_VALIDATION", "off")

  # ===========================================================================
  # GRAPH API CONFIGURATION
  # ===========================================================================

  # Graph API Endpoint
  GRAPH_API_URL = get_str_env("GRAPH_API_URL", "http://localhost:8001")
  GRAPH_API_KEY = get_secret_value("GRAPH_API_KEY", "")

  # Shared repository backend selection (dev/local only)
  # In AWS environments, backend is determined by graph.yml tier configuration
  GRAPH_SHARED_REPOSITORY_BACKEND = get_str_env("GRAPH_SHARED_REPOSITORY_BACKEND", "")

  # Graph Registry Tables (DynamoDB)
  # These tables track graph allocations, instance health, and volume management
  GRAPH_REGISTRY_TABLE = get_str_env(
    "GRAPH_REGISTRY_TABLE", f"robosystems-graph-{ENVIRONMENT}-graph-registry"
  )
  INSTANCE_REGISTRY_TABLE = get_str_env(
    "INSTANCE_REGISTRY_TABLE", f"robosystems-graph-{ENVIRONMENT}-instance-registry"
  )
  VOLUME_REGISTRY_TABLE = get_str_env(
    "VOLUME_REGISTRY_TABLE", f"robosystems-graph-{ENVIRONMENT}-volume-registry"
  )

  # Shared-master ASG (off-hours parking / wake). The single writer that hosts
  # the platform's shared repositories (SEC today). Adapter pipelines scale this
  # to 1 before staging and back to 0 after publish.
  SHARED_MASTER_ASG_NAME = get_str_env(
    "SHARED_MASTER_ASG_NAME",
    f"robosystems-ladybug-shared-writers-{ENVIRONMENT}-asg",
  )
  SHARED_MASTER_WAKE_TIMEOUT_S = get_int_env("SHARED_MASTER_WAKE_TIMEOUT_S", 900)

  # Instance Metadata
  EC2_INSTANCE_ID = get_str_env("INSTANCE_ID", "")
  INSTANCE_ID = get_str_env("INSTANCE_ID", "")
  CLUSTER_TIER = get_str_env("CLUSTER_TIER", "")

  # Graph API Timeouts and Limits (SSM: /tuning/timeouts/)
  GRAPH_HTTP_TIMEOUT = get_tuning_int(
    "GRAPH_HTTP_TIMEOUT", "timeouts/GRAPH_HTTP", TimeoutDefaults.GRAPH_HTTP
  )
  GRAPH_QUERY_TIMEOUT = get_tuning_int(
    "GRAPH_QUERY_TIMEOUT", "timeouts/GRAPH_QUERY", TimeoutDefaults.GRAPH_QUERY
  )

  # --- Graph Resiliency (configurable in dev, always enabled in prod/staging) ---
  GRAPH_CIRCUIT_BREAKERS_ENABLED = get_bool_env("GRAPH_CIRCUIT_BREAKERS_ENABLED", True)
  GRAPH_REDIS_CACHE_ENABLED = get_bool_env("GRAPH_REDIS_CACHE_ENABLED", True)
  GRAPH_RETRY_LOGIC_ENABLED = get_bool_env("GRAPH_RETRY_LOGIC_ENABLED", True)
  GRAPH_HEALTH_CHECKS_ENABLED = get_bool_env("GRAPH_HEALTH_CHECKS_ENABLED", True)

  GRAPH_CIRCUIT_BREAKER_THRESHOLD = get_tuning_int(
    "GRAPH_CIRCUIT_BREAKER_THRESHOLD",
    "circuits/THRESHOLD",
    CircuitBreakerDefaults.FAILURE_THRESHOLD,
  )
  GRAPH_CIRCUIT_BREAKER_TIMEOUT = get_tuning_int(
    "GRAPH_CIRCUIT_BREAKER_TIMEOUT",
    "circuits/TIMEOUT",
    CircuitBreakerDefaults.TIMEOUT,
  )

  # ===========================================================================
  # LADYBUGDB CONFIGURATION
  # ===========================================================================
  LBUG_DATABASE_PATH = get_str_env("LBUG_DATABASE_PATH", "./data/lbug-dbs")
  LBUG_ACCESS_PATTERN = get_str_env("LBUG_ACCESS_PATTERN", "api_auto")
  LBUG_NODE_TYPE = get_str_env("LBUG_NODE_TYPE", "writer")

  # DuckDB Staging Configuration (for data ingestion/materialization)
  DUCKDB_STAGING_PATH = get_str_env("DUCKDB_STAGING_PATH", "./data/staging")
  # Fallbacks for the staging connection's resource caps. The tier config in
  # .github/configs/graph.yml wins when CLUSTER_TIER is set; these apply on
  # hosts that carry no tier. Defaults match GraphTierConfig's own fallbacks.
  DUCKDB_MEMORY_LIMIT = get_str_env("DUCKDB_MEMORY_LIMIT", "2GB")
  DUCKDB_MAX_THREADS = get_int_env("DUCKDB_MAX_THREADS", 4)

  # Artifact Storage (precomputed Parquet files for enrichment refinement)
  ARTIFACT_PATH = get_str_env("ARTIFACT_PATH", "./data/artifacts")

  # LanceDB Vector Search Index (for MCP element resolution)
  LANCE_INDEX_PATH = get_str_env("LANCE_INDEX_PATH", "./data/lance")

  # LadybugDB Admission Control
  # These use SSM tuning parameters in prod/staging for runtime adjustability
  # Override priority: env var > SSM /tuning/lbug_admission/ > default
  LBUG_ADMISSION_MEMORY_THRESHOLD = get_tuning_float(
    "LBUG_ADMISSION_MEMORY_THRESHOLD",
    "lbug_admission/MEMORY_THRESHOLD",
    AdmissionDefaults.MEMORY_THRESHOLD,
  )
  LBUG_ADMISSION_MIN_AVAILABLE_MB = get_tuning_float(
    "LBUG_ADMISSION_MIN_AVAILABLE_MB",
    "lbug_admission/MIN_AVAILABLE_MB",
    AdmissionDefaults.MIN_AVAILABLE_MB,
  )
  LBUG_ADMISSION_CPU_THRESHOLD = get_tuning_float(
    "LBUG_ADMISSION_CPU_THRESHOLD",
    "lbug_admission/CPU_THRESHOLD",
    AdmissionDefaults.CPU_THRESHOLD,
  )

  # ==========================================================================
  # 4. DATABASE CONFIGURATION - POSTGRESQL
  # ==========================================================================

  DATABASE_ENDPOINT = get_str_env("DATABASE_ENDPOINT", "")
  DATABASE_PORT = get_str_env("DATABASE_PORT", "5432")

  # DATABASE_URL resolution order:
  # 1. DATABASE_URL env var (local dev, ECS with CF resolve)
  # 2. Constructed from DATABASE_ENDPOINT + POSTGRES_PASSWORD from Secrets Manager (EC2 graph instances)
  # 3. Local dev default
  DATABASE_URL = get_str_env("DATABASE_URL", "") or (
    f"postgresql://postgres:{get_secret_value('POSTGRES_PASSWORD', 'postgres')}@{get_str_env('DATABASE_ENDPOINT', '')}:{get_str_env('DATABASE_PORT', '5432')}/robosystems?sslmode=require"
    if get_str_env("DATABASE_ENDPOINT", "")
    else "postgresql://postgres:postgres@localhost:5432/robosystems"
  )
  DATABASE_ECHO = get_bool_env("DATABASE_ECHO", False)

  # Extensions OLTP Database (domain data for all extensions: roboledger, roboinvestor, etc.)
  EXTENSIONS_DATABASE_URL = get_str_env("EXTENSIONS_DATABASE_URL", "") or (
    f"postgresql://postgres:{get_secret_value('POSTGRES_PASSWORD', 'postgres')}@{get_str_env('DATABASE_ENDPOINT', '')}:{get_str_env('DATABASE_PORT', '5432')}/extensions?sslmode=require"
    if get_str_env("DATABASE_ENDPOINT", "")
    else "postgresql://postgres:postgres@localhost:5432/extensions"
  )

  # ==========================================================================
  # 5. CACHE AND QUEUE CONFIGURATION (VALKEY/REDIS)
  # ==========================================================================

  # Valkey/Redis URLs
  # Base URL without database number (database numbers are managed in valkey_registry.py)
  VALKEY_URL = get_str_env("VALKEY_URL", "redis://localhost:6379")

  # Valkey authentication (for encrypted/production environments)
  # This is fetched from AWS Secrets Manager in prod/staging environments
  # Secret path: robosystems/{env}/valkey (defined in secrets_manager.py SECRET_MAPPINGS)
  VALKEY_AUTH_TOKEN = get_str_env("VALKEY_AUTH_TOKEN", "")

  # Cache TTLs (SSM: /tuning/cache/)
  CREDIT_BALANCE_CACHE_TTL = get_tuning_int(
    "CREDIT_BALANCE_CACHE_TTL", "cache/BALANCE_TTL", CacheDefaults.BALANCE_TTL
  )
  CREDIT_SUMMARY_CACHE_TTL = get_tuning_int(
    "CREDIT_SUMMARY_CACHE_TTL", "cache/SUMMARY_TTL", CacheDefaults.SUMMARY_TTL
  )
  CREDIT_OPERATION_COST_CACHE_TTL = get_tuning_int(
    "CREDIT_OPERATION_COST_CACHE_TTL",
    "cache/OPERATION_COST_TTL",
    CacheDefaults.OPERATION_COST_TTL,
  )
  JWT_CACHE_TTL = get_tuning_int(
    "JWT_CACHE_TTL", "cache/JWT_TTL", CacheDefaults.JWT_TTL
  )
  API_KEY_CACHE_TTL = get_tuning_int(
    "API_KEY_CACHE_TTL", "cache/API_KEY_TTL", CacheDefaults.API_KEY_TTL
  )

  # ==========================================================================
  # 6. DAGSTER CONFIGURATION
  # ==========================================================================

  DAGSTER_HOST = get_str_env("DAGSTER_HOST", "dagster-webserver")
  DAGSTER_PORT = get_int_env("DAGSTER_PORT", 3000)

  # ==========================================================================
  # 7. AWS CONFIGURATION
  # ==========================================================================

  # AWS Region configuration (credentials come from IAM roles in ECS/EC2)
  AWS_DEFAULT_REGION = get_str_env("AWS_DEFAULT_REGION", "us-east-1")
  AWS_REGION = get_str_env("AWS_REGION", AWS_DEFAULT_REGION)
  AWS_ENDPOINT_URL = get_str_env("AWS_ENDPOINT_URL", "")  # For LocalStack
  # Optional override for the *S3 endpoint embedded in presigned URLs*.
  # AWS_ENDPOINT_URL is used for internal API↔S3 traffic (docker DNS hostname
  # like ``http://localstack:4566`` in dev) but those URLs aren't reachable
  # from the host browser. Setting this to ``http://localhost:4566`` makes
  # ``S3Client.generate_presigned_url`` sign URLs against a separate boto3
  # client whose endpoint is browser-reachable. Unset in staging/prod —
  # boto3's default real-AWS endpoints are already browser-reachable.
  AWS_S3_PRESIGN_ENDPOINT_URL = get_str_env("AWS_S3_PRESIGN_ENDPOINT_URL", "")

  # AWS Bedrock configuration (for AI agent features)
  # DEV ONLY: Explicit credentials for local development
  # PROD/STAGING: Uses IAM role credentials (ECS task role / EC2 instance profile)
  # NOT stored in Secrets Manager - these are dev-only overrides in .env
  AWS_BEDROCK_REGION = get_str_env("AWS_BEDROCK_REGION", "us-east-1")
  AWS_BEDROCK_ACCESS_KEY_ID = get_str_env("AWS_BEDROCK_ACCESS_KEY_ID", "")
  AWS_BEDROCK_SECRET_ACCESS_KEY = get_str_env("AWS_BEDROCK_SECRET_ACCESS_KEY", "")

  # S3-specific credentials
  # Use secrets manager for prod/staging, environment variables for local dev
  AWS_S3_ACCESS_KEY_ID = get_secret_value("AWS_S3_ACCESS_KEY_ID", "")
  AWS_S3_SECRET_ACCESS_KEY = get_secret_value("AWS_S3_SECRET_ACCESS_KEY", "")

  # S3 Bucket Configuration
  # Bucket names are passed as env vars from CloudFormation (api.yaml, dagster.yaml)
  # Defaults are for local development only
  SHARED_RAW_BUCKET = get_str_env("SHARED_RAW_BUCKET", "robosystems-shared-raw")
  SHARED_PROCESSED_BUCKET = get_str_env(
    "SHARED_PROCESSED_BUCKET", "robosystems-shared-processed"
  )
  USER_DATA_BUCKET = get_str_env("USER_DATA_BUCKET", "robosystems-user")
  PUBLIC_DATA_BUCKET = get_str_env("PUBLIC_DATA_BUCKET", "robosystems-public-data")
  DEPLOYMENT_BUCKET = get_str_env("DEPLOYMENT_BUCKET", "robosystems-deployment")
  LOGS_BUCKET = get_str_env("LOGS_BUCKET", "robosystems-logs")

  # CDN URL passed via ECS task definition (depends on CloudFront distribution)
  PUBLIC_DATA_CDN_URL = get_str_env("PUBLIC_DATA_CDN_URL", "")

  # ==========================================================================
  # 8. EXTERNAL SERVICE API KEYS
  # ==========================================================================

  # Enterprise SSO (OIDC) — IdP client secret; issuer/client_id are plain
  # config in the auth-posture block above
  SSO_OIDC_CLIENT_SECRET = get_secret_value("SSO_OIDC_CLIENT_SECRET", "")

  # QuickBooks/Intuit
  INTUIT_CLIENT_ID = get_secret_value("INTUIT_CLIENT_ID", "")
  INTUIT_CLIENT_SECRET = get_secret_value("INTUIT_CLIENT_SECRET", "")
  INTUIT_REDIRECT_URI = get_secret_value(
    "INTUIT_REDIRECT_URI", "http://localhost:8000/auth/callback"
  )
  INTUIT_ENVIRONMENT = get_secret_value("INTUIT_ENVIRONMENT", "sandbox")

  # SEC
  # SEC_GOV_USER_AGENT is a secret identity for API access
  SEC_GOV_USER_AGENT = get_secret_value(
    "SEC_GOV_USER_AGENT", "YourCompany your-email@example.com"
  )

  # OpenFIGI (financial identifiers)
  OPENFIGI_API_KEY = get_secret_value("OPENFIGI_API_KEY", "")

  # Stripe (payment processing)
  STRIPE_SECRET_KEY = get_secret_value("STRIPE_SECRET_KEY", "")
  STRIPE_PUBLISHABLE_KEY = get_secret_value("STRIPE_PUBLISHABLE_KEY", "")
  STRIPE_WEBHOOK_SECRET = get_secret_value("STRIPE_WEBHOOK_SECRET", "")

  # ==========================================================================
  # 9. PERFORMANCE AND SCALING
  # ==========================================================================

  # Query queue configuration (SSM: /tuning/queues/)
  QUERY_QUEUE_MAX_SIZE = get_tuning_int(
    "QUERY_QUEUE_MAX_SIZE", "queues/MAX_SIZE", QueueDefaults.MAX_SIZE
  )
  QUERY_QUEUE_MAX_CONCURRENT = get_tuning_int(
    "QUERY_QUEUE_MAX_CONCURRENT", "queues/MAX_CONCURRENT", QueueDefaults.MAX_CONCURRENT
  )
  QUERY_QUEUE_MAX_PER_USER = get_tuning_int(
    "QUERY_QUEUE_MAX_PER_USER", "queues/MAX_PER_USER", QueueDefaults.MAX_PER_USER
  )
  QUERY_QUEUE_TIMEOUT = get_tuning_int(
    "QUERY_QUEUE_TIMEOUT", "queues/TIMEOUT", QueueDefaults.TIMEOUT
  )

  # Admission control (SSM: /tuning/admission/)
  ADMISSION_MEMORY_THRESHOLD = get_tuning_float(
    "ADMISSION_MEMORY_THRESHOLD",
    "admission/MEMORY_THRESHOLD",
    AdmissionDefaults.MEMORY_THRESHOLD,
  )
  ADMISSION_CPU_THRESHOLD = get_tuning_float(
    "ADMISSION_CPU_THRESHOLD",
    "admission/CPU_THRESHOLD",
    AdmissionDefaults.CPU_THRESHOLD,
  )
  ADMISSION_QUEUE_THRESHOLD = get_tuning_float(
    "ADMISSION_QUEUE_THRESHOLD",
    "admission/QUEUE_THRESHOLD",
    AdmissionDefaults.QUEUE_THRESHOLD,
  )

  # Load shedding (SSM: /tuning/load_shedding/)
  LOAD_SHED_START_PRESSURE = get_tuning_float(
    "LOAD_SHED_START_PRESSURE",
    "load_shedding/START_PRESSURE",
    LoadSheddingDefaults.START_PRESSURE,
  )
  LOAD_SHED_STOP_PRESSURE = get_tuning_float(
    "LOAD_SHED_STOP_PRESSURE",
    "load_shedding/STOP_PRESSURE",
    LoadSheddingDefaults.STOP_PRESSURE,
  )

  # SSE (Server-Sent Events) (SSM: /tuning/sse/)
  MAX_SSE_CONNECTIONS_PER_USER = get_tuning_int(
    "MAX_SSE_CONNECTIONS_PER_USER",
    "sse/MAX_CONNECTIONS_PER_USER",
    SSEDefaults.MAX_CONNECTIONS_PER_USER,
  )
  SSE_QUEUE_SIZE = get_tuning_int(
    "SSE_QUEUE_SIZE", "sse/QUEUE_SIZE", SSEDefaults.QUEUE_SIZE
  )

  # MCP (Model Context Protocol) (SSM: /tuning/mcp/)
  MCP_MAX_RESULT_ROWS = get_tuning_int(
    "MCP_MAX_RESULT_ROWS", "mcp/MAX_RESULT_ROWS", MCPDefaults.MAX_RESULT_ROWS
  )
  MCP_MAX_RESULT_SIZE_MB = get_tuning_float(
    "MCP_MAX_RESULT_SIZE_MB", "mcp/MAX_RESULT_SIZE_MB", MCPDefaults.MAX_RESULT_SIZE_MB
  )

  # ==========================================================================
  # 10. ARELLE RUNTIME CONFIGURATION
  # ==========================================================================

  # Arelle cache directory (runtime path, varies by deployment)
  ARELLE_CACHE_DIR = get_str_env("ARELLE_CACHE_DIR", "")

  # ==========================================================================
  # 11. OBSERVABILITY
  # ==========================================================================

  # OpenTelemetry configuration
  OTEL_SERVICE_NAME = get_str_env("OTEL_SERVICE_NAME", "robosystems")
  OTEL_EXPORTER_OTLP_ENDPOINT = get_str_env(
    "OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317"
  )
  OTEL_RESOURCE_ATTRIBUTES = get_str_env("OTEL_RESOURCE_ATTRIBUTES", "")
  OTEL_CONSOLE_EXPORT = get_bool_env("OTEL_CONSOLE_EXPORT", False)

  # ==========================================================================
  # HELPER METHODS
  # ==========================================================================

  @classmethod
  def is_production(cls) -> bool:
    """Check if running in production environment."""
    return cls.ENVIRONMENT.lower() in ["prod", "production"]

  @classmethod
  def is_development(cls) -> bool:
    """Check if running in development environment."""
    return cls.ENVIRONMENT.lower() in ["dev", "development", "local"]

  @classmethod
  def is_staging(cls) -> bool:
    """Check if running in staging environment."""
    return cls.ENVIRONMENT.lower() in ["staging", "stage"]

  @classmethod
  def is_test(cls) -> bool:
    """Check if running in test environment."""
    return cls.ENVIRONMENT.lower() in ["test", "testing"]

  @classmethod
  def is_aws_environment(cls) -> bool:
    """Check if running in AWS environment (production or staging)."""
    return cls.ENVIRONMENT.lower() in ["prod", "production", "staging", "stage"]

  @classmethod
  def get_environment_key(cls) -> str:
    """Normalize ENVIRONMENT to 'production', 'staging', or 'development'."""
    env_lower = cls.ENVIRONMENT.lower()
    if env_lower in ["prod", "production"]:
      return "production"
    elif env_lower in ["staging", "stage"]:
      return "staging"
    else:
      return "development"

  @classmethod
  def is_using_secrets_manager(cls) -> bool:
    """True when the secrets_manager module loaded and the env is prod/staging."""
    return SECRETS_MANAGER_AVAILABLE and cls.ENVIRONMENT.lower() in [
      "prod",
      "production",
      "staging",
      "stage",
    ]

  @classmethod
  @lru_cache(maxsize=1)
  def validate(cls) -> list[str]:
    """Check required variables and numeric ranges.

    Returns one message per problem; an empty list means the configuration is
    usable. Production additionally requires DATABASE_URL and JWT_SECRET_KEY to
    be set to non-default values.
    """
    errors = []

    # Check required variables in production
    if cls.is_production():
      required_vars = [
        ("DATABASE_URL", cls.DATABASE_URL, None),
        ("JWT_SECRET_KEY", cls.JWT_SECRET_KEY, ""),
        # Note: AWS credentials come from IAM roles, not environment variables
      ]

      for var_name, var_value, default_value in required_vars:
        if not var_value or var_value == default_value:
          errors.append(f"{var_name} must be set in production")

    # Validate numeric ranges
    if cls.PORT < 1 or cls.PORT > 65535:
      errors.append("PORT must be between 1 and 65535")

    return errors

  @classmethod
  def get_lbug_tier_config(cls) -> dict[str, Any]:
    """Get LadybugDB tier configuration, with graph.yml overriding env vars.

    Lets a container inherit its tier's memory settings (max_memory_mb,
    memory_per_db_mb), performance settings (chunk_size, query_timeout,
    max_query_length), and connection settings (connection_pool_size,
    databases_per_instance) from ``.github/configs/graph.yml``.
    """
    # Import constants at function level to avoid circular imports
    from robosystems.config.constants import MAX_QUERY_LENGTH

    # Try to load tier-specific config if available
    try:
      from robosystems.config.graph_tier import GraphTierConfig

      # Determine tier from environment
      # CLUSTER_TIER is set by CloudFormation to the actual tier (e.g., "ladybug-standard")
      tier = cls.CLUSTER_TIER

      if tier:
        # Get instance config from graph.yml
        instance_config = GraphTierConfig.get_instance_config(tier)

        if instance_config:
          # Get full tier config for additional settings
          full_tier_config = GraphTierConfig.get_tier_config(tier)

          # Override with values from config file if present
          return {
            # Memory settings (from graph.yml tier config)
            "max_memory_mb": instance_config.get("max_memory_mb", 2048),
            "memory_per_db_mb": instance_config.get("memory_per_db_mb", 0),
            "memory_per_subgraph_mb": instance_config.get("memory_per_subgraph_mb", 0),
            # Performance settings
            "chunk_size": instance_config.get("chunk_size", 1000),
            "query_timeout": instance_config.get(
              "query_timeout", cls.GRAPH_QUERY_TIMEOUT
            ),
            "max_query_length": instance_config.get(
              "max_query_length", MAX_QUERY_LENGTH
            ),
            "connection_pool_size": instance_config.get("connection_pool_size", 10),
            # Database settings - prioritize environment variable in dev
            "databases_per_instance": (
              get_int_env("LBUG_DATABASES_PER_INSTANCE", 0)
              if cls.ENVIRONMENT == "dev"
              and get_int_env("LBUG_DATABASES_PER_INSTANCE", 0) > 0
              else instance_config.get("databases_per_instance", 10)
            ),
            "max_databases": (
              get_int_env("LBUG_DATABASES_PER_INSTANCE", 0)
              if cls.ENVIRONMENT == "dev"
              and get_int_env("LBUG_DATABASES_PER_INSTANCE", 0) > 0
              else instance_config.get("databases_per_instance", 10)
            ),
            # Tier-level settings from full config. Storage, credits, and
            # rate multipliers deliberately do not appear here: those keys
            # never existed in graph.yml, so this dict only fabricated
            # defaults (500 GB / 10000 / 1.0) that nothing should read.
            # Their sources of truth are GraphTierConfig, BillingConfig,
            # and RateLimitConfig respectively.
            "tier": tier,
            "max_subgraphs": full_tier_config.get("max_subgraphs", 0),
          }
    except ImportError:
      pass  # GraphTierConfig not available
    except Exception:
      pass  # Any other error loading config

    # Fall back to sensible defaults (used when CLUSTER_TIER is not set)
    return {
      # Memory settings (defaults for local dev)
      "max_memory_mb": get_int_env("LBUG_MAX_MEMORY_MB", 2048),
      "memory_per_db_mb": get_int_env("LBUG_MAX_MEMORY_PER_DB_MB", 0),
      "memory_per_subgraph_mb": 0,
      # Performance settings
      "chunk_size": get_int_env("LBUG_CHUNK_SIZE", 1000),
      "query_timeout": cls.GRAPH_QUERY_TIMEOUT,
      "max_query_length": MAX_QUERY_LENGTH,
      "connection_pool_size": get_int_env("LBUG_CONNECTION_POOL_SIZE", 10),
      # Database settings
      "databases_per_instance": get_int_env("LBUG_DATABASES_PER_INSTANCE", 10),
      "max_databases": get_int_env("LBUG_DATABASES_PER_INSTANCE", 10),
      # Default tier settings
      "tier": "ladybug-standard",
      "max_subgraphs": 0,
    }

  @classmethod
  def get_lbug_memory_config(cls) -> dict[str, Any]:
    """Alias for backward compatibility with existing code."""
    return cls.get_lbug_tier_config()

  @classmethod
  def get_database_url(cls, database_name: str | None = None) -> str:
    """Get DATABASE_URL, optionally swapping in a different database name."""
    if not database_name:
      return cls.DATABASE_URL

    # Parse and replace database name
    base_url = cls.DATABASE_URL.rsplit("/", 1)[0]
    return f"{base_url}/{database_name}"

  @classmethod
  def get_aws_config(cls) -> dict:
    """Get boto3 kwargs for AWS: region, plus endpoint when one is set.

    Deliberately sets no credentials — production authenticates via IAM roles.
    """
    config = {
      "region_name": cls.AWS_DEFAULT_REGION,
    }

    if cls.AWS_ENDPOINT_URL:
      config["endpoint_url"] = cls.AWS_ENDPOINT_URL

    return config

  @classmethod
  def get_s3_config(cls) -> dict:
    """Get boto3 kwargs for S3.

    Uses the S3-specific credentials when set (cross-account access, local dev)
    and otherwise relies on IAM roles.
    """
    config = {
      "region_name": cls.AWS_DEFAULT_REGION,
    }

    # Use S3-specific credentials if available (for cross-account access or local dev)
    if cls.AWS_S3_ACCESS_KEY_ID:
      config["aws_access_key_id"] = cls.AWS_S3_ACCESS_KEY_ID

    if cls.AWS_S3_SECRET_ACCESS_KEY:
      config["aws_secret_access_key"] = cls.AWS_S3_SECRET_ACCESS_KEY

    if cls.AWS_ENDPOINT_URL:
      config["endpoint_url"] = cls.AWS_ENDPOINT_URL

    return config

  @classmethod
  def get_r2_config(cls) -> dict:
    """Get boto3 kwargs for Cloudflare R2, or {} when R2 is not configured.

    R2 speaks the S3 API against a custom endpoint, with region "auto".
    """
    if not cls.R2_ENDPOINT_URL:
      return {}

    return {
      "endpoint_url": cls.R2_ENDPOINT_URL,
      "aws_access_key_id": cls.R2_ACCESS_KEY_ID,
      "aws_secret_access_key": cls.R2_SECRET_ACCESS_KEY,
      "region_name": "auto",
    }

  @classmethod
  def get_cors_origins(cls) -> list[str]:
    """Get CORS origins for Main API (backward compatibility)."""
    return cls.get_main_cors_origins()

  @classmethod
  def get_main_cors_origins(cls) -> list[str]:
    """Get CORS origins for Main API (public-facing)."""
    if cls.is_production():
      return [
        "https://roboledger.ai",
        "https://roboinvestor.ai",
        "https://robosystems.ai",
        # Holon Viewer — static SPA that calls the API client-side with a
        # user-supplied API key (no app backend of its own).
        "https://holon.robosystems.ai",
      ]
    elif cls.is_staging():
      return [
        "https://staging.roboledger.ai",
        "https://staging.roboinvestor.ai",
        "https://staging.robosystems.ai",
        "https://staging.holon.robosystems.ai",
      ]
    else:
      # Development
      origins = [
        "http://localhost:3000",
        "http://localhost:3001",
        "http://localhost:3002",
        "http://localhost:8000",
        "https://roboledger.ai",
        "https://roboinvestor.ai",
        "https://robosystems.ai",
      ]
      # Allow extra origins via env (e.g., ngrok tunnels for OAuth callbacks).
      # Dev-only — production/staging lists are hardcoded above on purpose.
      extra = get_secret_list_value("EXTRA_CORS_ORIGINS", "")
      origins.extend(o for o in extra if o not in origins)
      return origins

  @classmethod
  def get_lbug_cors_origins(cls) -> list[str]:
    """Get CORS origins for Graph API (VPC-internal)."""
    if cls.is_production() or cls.is_staging():
      # VPC-internal APIs don't need CORS for browsers
      return []
    else:
      # Development only
      return ["*"]

  @classmethod
  def get_valkey_url(cls, database: Union[int, "ValkeyDatabase"] | None = None) -> str:
    """Get the Valkey/Redis URL, optionally for a specific database.

    Pass a :class:`~robosystems.config.valkey_registry.ValkeyDatabase` member
    rather than a bare int — see that module for why numbers are never
    hardcoded. A None ``database`` returns the base URL.

    Example:
        >>> from robosystems.config.valkey_registry import ValkeyDatabase
        >>> env.get_valkey_url(ValkeyDatabase.AUTH)
        'redis://localhost:6379/0'
    """
    if database is None:
      return cls.VALKEY_URL

    # Import here to avoid circular dependency
    from .valkey_registry import ValkeyDatabase, ValkeyURLBuilder

    # Handle both int and enum
    if isinstance(database, ValkeyDatabase):
      return ValkeyURLBuilder.build_url(cls.VALKEY_URL, database)
    else:
      # Create a temporary enum value for the integer
      return f"{cls.VALKEY_URL.rstrip('/')}/{database}"


# ==========================================================================
# SINGLETON INSTANCE
# ==========================================================================

# Create a singleton instance for easy import
env = EnvConfig()
