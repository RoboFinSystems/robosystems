"""Single source of truth for environment variables.

Read configuration through ``env`` rather than ``os.getenv``: the accessors
apply each variable's type, default, and validation, and tuning parameters
layer in SSM. Sections mirror the .env file order.
"""

import os
from functools import lru_cache
from typing import TYPE_CHECKING, Any, Union
from urllib.parse import urlparse

if TYPE_CHECKING:
  from .valkey_registry import ValkeyDatabase

# Falls back to plain env vars when boto3 is missing or the import is circular.
try:
  from .secrets_manager import get_secret_list_value, get_secret_value

  SECRETS_MANAGER_AVAILABLE = True
except ImportError:
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


# Feature flags live in SSM Parameter Store (cheaper than Secrets Manager).
try:
  from .parameter_store import get_parameter_value, preload_feature_flags

  PARAMETER_STORE_AVAILABLE = True

  # One batched SSM read before EnvConfig is defined, so each flag lookup hits
  # the cache instead of making its own call (which can fail under load).
  _preloaded_flags = preload_feature_flags()
  # EnvValidator refuses a deployed boot where this is False: a boot that could
  # not reach SSM would serve its whole life on code defaults. Always False
  # outside prod/staging.
  FEATURE_FLAGS_PRELOADED = bool(_preloaded_flags)
  # Per-flag presence: a resolved bool cannot distinguish "absent from SSM"
  # from "false".
  PRELOADED_FEATURE_FLAG_NAMES = frozenset(_preloaded_flags or ())
  if _preloaded_flags:
    print(f"Preloaded {len(_preloaded_flags)} feature flags from SSM")
  elif os.getenv("ENVIRONMENT", "dev") in ("prod", "staging"):
    print("WARNING: No feature flags loaded from SSM. All flags will use defaults.")
except Exception as _e:
  # Broad except: transitive import failures must also fall back to defaults.
  PARAMETER_STORE_AVAILABLE = False
  FEATURE_FLAGS_PRELOADED = False
  PRELOADED_FEATURE_FLAG_NAMES = frozenset()
  print(
    f"WARNING: Parameter store unavailable ({type(_e).__name__}: {_e}). "
    "All feature flags will use defaults (false)."
  )

  def get_parameter_value(key: str, default: str = "") -> str:
    """Fallback when parameter_store is unavailable: read the env var."""
    return os.getenv(key, default)


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
    # print, not logger: the logger imports this module.
    print(f"Warning: Invalid {key} value, using default: {default}")
    return default


def get_float_env(key: str, default: float) -> float:
  """Get a float env var, falling back to ``default`` if unset or unparseable."""
  try:
    return float(os.getenv(key, str(default)))
  except (ValueError, TypeError):
    print(f"Warning: Invalid {key} value, using default: {default}")
    return default


def get_bool_env(key: str, default: bool = False) -> bool:
  """Get a bool env var; "true", "1", "yes", and "on" are true, all else false."""
  value = os.getenv(key, str(default)).lower()
  return value in ("true", "1", "yes", "on")


def get_str_env(key: str, default: str = "") -> str:
  """Get a string env var."""
  return os.getenv(key, default)


def _url_origin(url: str) -> str | None:
  """Reduce a URL to its origin (scheme://netloc); None if it lacks either."""
  parsed = urlparse((url or "").strip())
  if not parsed.scheme or not parsed.netloc:
    return None
  return f"{parsed.scheme}://{parsed.netloc}"


def _tuning_env_key(ssm_path: str) -> str:
  """``"graphql/MAX_DEPTH"`` -> ``"TUNING_GRAPHQL_MAX_DEPTH"``."""
  return "TUNING_" + ssm_path.upper().replace("/", "_")


def get_tuning_float(env_key: str, ssm_path: str, default: float) -> float:
  """Float tuning parameter: env var (``env_key`` or ``TUNING_{CATEGORY}_{KEY}``),
  then SSM ``tuning/`` in prod/staging (applies without a redeploy), then
  ``default``.
  """
  for candidate in (env_key, _tuning_env_key(ssm_path)):
    env_value = os.getenv(candidate)
    if env_value is not None:
      try:
        return float(env_value)
      except (ValueError, TypeError):
        print(f"Warning: Invalid {env_key} value, using default: {default}")
        return default

  environment = os.getenv("ENVIRONMENT", "dev")
  if environment in ["prod", "staging"]:
    try:
      from .parameter_store import get_parameter_manager

      manager = get_parameter_manager()
      ssm_value = manager.get_tuning_float(ssm_path, default)
      return ssm_value
    except Exception:
      pass  # Fall through to default

  return default


def get_tuning_int(env_key: str, ssm_path: str, default: int) -> int:
  """Int counterpart of ``get_tuning_float``, same precedence."""
  for candidate in (env_key, _tuning_env_key(ssm_path)):
    env_value = os.getenv(candidate)
    if env_value is not None:
      try:
        return int(env_value)
      except (ValueError, TypeError):
        print(f"Warning: Invalid {env_key} value, using default: {default}")
        return default

  environment = os.getenv("ENVIRONMENT", "dev")
  if environment in ["prod", "staging"]:
    try:
      from .parameter_store import get_parameter_manager

      manager = get_parameter_manager()
      ssm_value = manager.get_tuning_int(ssm_path, default)
      return ssm_value
    except Exception:
      pass  # Fall through to default

  return default


def get_list_env(key: str, default: str = "", separator: str = ",") -> list[str]:
  """Get a list env var, splitting on ``separator`` and trimming each item."""
  value = os.getenv(key, default)
  if not value:
    return []
  return [item.strip() for item in value.split(separator) if item.strip()]


_cloudformation_cache: dict[str, str | None] = {}


def _get_cf_stack_output(stack_name: str, output_key: str) -> str:
  """Cached CloudFormation stack output lookup; "" if not found."""
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

  # SSM reachability captured at import; EnvValidator asserts on it when deployed.
  PARAMETER_STORE_AVAILABLE = PARAMETER_STORE_AVAILABLE
  FEATURE_FLAGS_PRELOADED = FEATURE_FLAGS_PRELOADED
  PRELOADED_FEATURE_FLAG_NAMES = PRELOADED_FEATURE_FLAG_NAMES

  # Environment and debugging
  ENVIRONMENT = get_str_env("ENVIRONMENT", "dev")
  DEBUG = get_bool_env("DEBUG", False)
  LOG_LEVEL = get_str_env("LOG_LEVEL", "INFO")

  SECRETS_MANAGER_AVAILABLE = SECRETS_MANAGER_AVAILABLE

  # Server configuration
  HOST = get_str_env("HOST", "0.0.0.0")
  PORT = get_int_env("PORT", 8000)

  # Encryption Keys
  CONNECTION_CREDENTIALS_KEY = get_secret_value("CONNECTION_CREDENTIALS_KEY", "")

  # Service URLs
  # Set by CloudFormation per access mode (custom domain or ALB DNS).
  ROBOSYSTEMS_API_URL = get_str_env("ROBOSYSTEMS_API_URL", "https://api.robosystems.ai")
  ROBOLEDGER_URL = get_str_env("ROBOLEDGER_URL", "https://roboledger.ai")
  ROBOINVESTOR_URL = get_str_env("ROBOINVESTOR_URL", "https://roboinvestor.ai")
  ROBOSYSTEMS_URL = get_str_env("ROBOSYSTEMS_URL", "https://robosystems.ai")
  # xbrlkit viewer origin (a static SPA); CORS allowlist only.
  VIEWER_URL = get_str_env(
    "VIEWER_URL",
    "https://staging.xbrlkit.com"
    if ENVIRONMENT == "staging"
    else "https://xbrlkit.com",
  )
  # The viewer's original host, kept as an alias: `xbrlkit view` before 0.10
  # names it as the only origin allowed to read the report it serves.
  HOLON_URL = get_str_env(
    "HOLON_URL",
    "https://staging.holon.robosystems.ai"
    if ENVIRONMENT == "staging"
    else "https://holon.robosystems.ai",
  )
  # App key of the app hosting the interactive auth surface ("login home").
  LOGIN_HOME_APP = get_str_env("LOGIN_HOME_APP", "robosystems")

  # JWT configuration
  JWT_SECRET_KEY = get_secret_value("JWT_SECRET_KEY", "")

  # Issuer/audience default to the API host; internal-mode deployments (SSM
  # tunnel access) override JWT_ISSUER=localhost in Secrets Manager.
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
  )  # public artifacts bucket
  R2_PUBLIC_URL = get_secret_value("R2_PUBLIC_URL", "")

  # Hugging Face (public SEC dataset publish — a manual Dagster job)
  HF_TOKEN = get_secret_value("HF_TOKEN", "")  # write token; also runs HF Jobs
  HF_SEC_DATASET_REPO = get_str_env(
    "HF_SEC_DATASET_REPO", "robosystems/sec-xbrl-knowledge-graphs"
  )

  # ==========================================================================
  # 2. FEATURE FLAGS
  # ==========================================================================
  # Override priority: env var > SSM Parameter Store > default.

  # --- Platform Operations ---
  USER_REGISTRATION_ENABLED = get_bool_env(
    "USER_REGISTRATION_ENABLED",
    get_parameter_value("USER_REGISTRATION_ENABLED", "true").lower() == "true",
  )
  # Self-hosted deployments leave this false: no payment requirements.
  BILLING_ENABLED = get_bool_env(
    "BILLING_ENABLED",
    get_parameter_value("BILLING_ENABLED", "false").lower() == "true",
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
  # Span export, separate from OTEL_ENABLED (metrics); no traces backend exists yet.
  OTEL_TRACES_ENABLED = get_bool_env(
    "OTEL_TRACES_ENABLED",
    get_parameter_value("OTEL_TRACES_ENABLED", "false").lower() == "true",
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
  # The SSO connection block rides in the base robosystems/{env} secret with
  # SSO_OIDC_CLIENT_SECRET; env vars win (the local-dev path).
  SSO_OIDC_PROVIDER_LABEL = get_secret_value("SSO_OIDC_PROVIDER_LABEL", "SSO")
  # The IdP's org authorization server (https://<org>.okta.com), NOT
  # /oauth2/default, which mints API access tokens rather than ID tokens.
  SSO_OIDC_ISSUER = get_secret_value("SSO_OIDC_ISSUER", "")
  SSO_OIDC_CLIENT_ID = get_secret_value("SSO_OIDC_CLIENT_ID", "")
  # SCIM 2.0 provisioning, gated independently of OIDC.
  SCIM_ENABLED = get_bool_env(
    "SCIM_ENABLED",
    get_parameter_value("SCIM_ENABLED", "false").lower() == "true",
  )
  # Org role SCIM-provisioned users join the enterprise org with.
  SSO_DEFAULT_ROLE = get_secret_value("SSO_DEFAULT_ROLE", "member")
  # Pins the one org the SCIM/OIDC surface operates against; set after the
  # first `scim bootstrap`. Once set, other orgs' bearer tokens are refused and
  # OIDC first-login linking requires membership.
  ENTERPRISE_ORG_ID = get_secret_value("ENTERPRISE_ORG_ID", "")
  # ID-token claim compared against the SCIM-provisioned external_id at
  # first-login linking. Okta: externalId ≡ sub. Entra pairs SCIM externalId
  # (objectId) with the `oid` claim, not `sub` — override there.
  SSO_OIDC_BINDING_CLAIM = get_secret_value("SSO_OIDC_BINDING_CLAIM", "sub")
  PASSKEYS_ENABLED = get_bool_env(
    "PASSKEYS_ENABLED",
    get_parameter_value("PASSKEYS_ENABLED", "false").lower() == "true",
  )
  # MCP OAuth 2.1 (authorization server, Bearer on /v1/graphs/{g}/mcp, and the
  # OAuth-only /v1/mcp routes). Off: those routes 404 and the MCP 401 carries no
  # resource_metadata, so clients treat the server as header-auth only.
  MCP_OAUTH_ENABLED = get_bool_env(
    "MCP_OAUTH_ENABLED",
    get_parameter_value("MCP_OAUTH_ENABLED", "false").lower() == "true",
  )
  # Org owner/admin password logins without a passkey get
  # mfa_enrollment_required instead of a session. Requires PASSKEYS_ENABLED.
  MFA_ENFORCEMENT_ENABLED = get_bool_env(
    "MFA_ENFORCEMENT_ENABLED",
    get_parameter_value("MFA_ENFORCEMENT_ENABLED", "false").lower() == "true",
  )
  # WebAuthn RP overrides; normally derived from ROBOSYSTEMS_URL.
  PASSKEY_RP_ID = get_str_env("PASSKEY_RP_ID", "")
  PASSKEY_ORIGIN = get_str_env("PASSKEY_ORIGIN", "")

  # --- Organization ---
  ORG_MEMBER_INVITATIONS_ENABLED = get_bool_env(
    "ORG_MEMBER_INVITATIONS_ENABLED",
    get_parameter_value("ORG_MEMBER_INVITATIONS_ENABLED", "false").lower() == "true",
  )
  # Test support: returns the raw invite token in the invitation response
  # (non-prod only, see expose_invite_token_in_response). Deliberately not SSM:
  # a credential-exposing flag must not be flippable live.
  AUTH_INVITE_TOKEN_IN_RESPONSE = get_bool_env("AUTH_INVITE_TOKEN_IN_RESPONSE", False)
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
  # Gates the subgraph write/DDL MCP tools; the main graph stays read-only to
  # raw statements regardless.
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
  # Gates create/update of reporting_extension / custom_ontology taxonomy
  # blocks (delete stays open; chart_of_accounts is never gated). Fail-closed
  # in prod/staging so a missing SSM parameter cannot open the surface.
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
  # When set, shared-repository reads route to the replica ALB instead of the
  # shared master. Auto-discovered from CloudFormation if unset.
  SHARED_REPLICA_ALB_URL = (
    get_str_env("SHARED_REPLICA_ALB_URL", "")
    or _get_shared_replica_alb_url_from_cloudformation()
  )
  # Repositories deployed on shared writer instances (infrastructure only; for
  # "is this a shared repo" use config.shared_repositories).
  SHARED_REPOSITORIES = get_list_env("SHARED_REPOSITORIES", "")

  # --- Connection Providers ---
  # Mounts the /connections router; the provider flags below require it.
  CONNECTIONS_ENABLED = get_bool_env(
    "CONNECTIONS_ENABLED",
    get_parameter_value("CONNECTIONS_ENABLED", "true").lower() == "true",
  )
  CONNECTION_QUICKBOOKS_ENABLED = get_bool_env(
    "CONNECTION_QUICKBOOKS_ENABLED",
    get_parameter_value("CONNECTION_QUICKBOOKS_ENABLED", "true").lower() == "true",
  )
  # Source-namespace registration for integrations that write through the
  # public API; no credentials, no sync.
  CONNECTION_EXTERNAL_ENABLED = get_bool_env(
    "CONNECTION_EXTERNAL_ENABLED",
    get_parameter_value("CONNECTION_EXTERNAL_ENABLED", "true").lower() == "true",
  )
  # Mercury bank feed; needs Mercury's partner OAuth client.
  CONNECTION_MERCURY_ENABLED = get_bool_env(
    "CONNECTION_MERCURY_ENABLED",
    get_parameter_value("CONNECTION_MERCURY_ENABLED", "false").lower() == "true",
  )
  # Mercury personal read-only token mode, for self-hosted/local only: Mercury's
  # terms bar third-party automated access without the OAuth approval.
  MERCURY_API_KEY_CONNECTIONS_ENABLED = get_bool_env(
    "MERCURY_API_KEY_CONNECTIONS_ENABLED",
    get_parameter_value("MERCURY_API_KEY_CONNECTIONS_ENABLED", "false").lower()
    == "true",
  )
  # Plaid bank feed, one connection per Item (one institution login).
  CONNECTION_PLAID_ENABLED = get_bool_env(
    "CONNECTION_PLAID_ENABLED",
    get_parameter_value("CONNECTION_PLAID_ENABLED", "false").lower() == "true",
  )

  # When False the nightly SEC pipeline still wakes and health-gates the shared
  # master but never parks it back to 0 (keeps a reserved-instance master busy).
  SHARED_MASTER_PARKING_ENABLED = get_bool_env(
    "SHARED_MASTER_PARKING_ENABLED",
    get_parameter_value("SHARED_MASTER_PARKING_ENABLED", "true").lower() == "true",
  )

  # ==========================================================================
  # EXTENSIONS — RoboLedger & RoboInvestor product surfaces
  # ==========================================================================
  # Each flag mounts its domain's ops router and GraphQL resolvers.
  ROBOLEDGER_ENABLED = get_bool_env(
    "ROBOLEDGER_ENABLED",
    get_parameter_value("ROBOLEDGER_ENABLED", "true").lower() == "true",
  )

  ROBOINVESTOR_ENABLED = get_bool_env(
    "ROBOINVESTOR_ENABLED",
    get_parameter_value("ROBOINVESTOR_ENABLED", "true").lower() == "true",
  )

  # Kill switch for /extensions/{graph_id}/graphql, the only extensions read
  # surface; independent of the domain flags so reads can be cut in an incident.
  EXTENSIONS_GRAPHQL_ENABLED = get_bool_env(
    "EXTENSIONS_GRAPHQL_ENABLED",
    get_parameter_value("EXTENSIONS_GRAPHQL_ENABLED", "true").lower() == "true",
  )

  # Query-bounding limits: the OLTP pool is small and each resolved field can
  # open a session, so unbounded documents are a DoS vector. The depth limiter
  # ignores introspection fields.
  EXTENSIONS_GRAPHQL_MAX_DEPTH = get_tuning_int(
    "EXTENSIONS_GRAPHQL_MAX_DEPTH", "graphql/MAX_DEPTH", 15
  )
  EXTENSIONS_GRAPHQL_MAX_ALIASES = get_tuning_int(
    "EXTENSIONS_GRAPHQL_MAX_ALIASES", "graphql/MAX_ALIASES", 30
  )
  EXTENSIONS_GRAPHQL_MAX_TOKENS = get_tuning_int(
    "EXTENSIONS_GRAPHQL_MAX_TOKENS", "graphql/MAX_TOKENS", 2000
  )

  # Derived, not an env var: whether the extensions database opens at all.
  EXTENSIONS_ENABLED = ROBOLEDGER_ENABLED or ROBOINVESTOR_ENABLED

  # When True the obligation-promoter sensor also dispatches the handler, so
  # the closing-entry draft lands on the same tick; False only flips status.
  # The per-graph autopilot column overrides this default.
  EXTENSIONS_PROMOTION_AUTO_DISPATCH = get_bool_env(
    "EXTENSIONS_PROMOTION_AUTO_DISPATCH",
    get_parameter_value("EXTENSIONS_PROMOTION_AUTO_DISPATCH", "false").lower()
    == "true",
  )

  # --- Adapter Pipelines (Dagster) ---
  SEC_PIPELINE_ENABLED = get_bool_env(
    "SEC_PIPELINE_ENABLED",
    get_parameter_value("SEC_PIPELINE_ENABLED", "true").lower() == "true",
  )

  # ==========================================================================
  # 3. GRAPH DATABASES (LADYBUGDB)
  # ==========================================================================

  GRAPH_BACKEND_TYPE = get_str_env("GRAPH_BACKEND_TYPE", "ladybug")

  # SHACL validation of the report bundle at publish, recorded on
  # Report.metadata['bundle_validation']: off | warn (never blocks) | strict
  # (raises on non-conformance).
  REPORT_BUNDLE_SHACL_VALIDATION = get_str_env("REPORT_BUNDLE_SHACL_VALIDATION", "off")

  # ===========================================================================
  # GRAPH API CONFIGURATION
  # ===========================================================================

  GRAPH_API_URL = get_str_env("GRAPH_API_URL", "http://localhost:8001")
  GRAPH_API_KEY = get_secret_value("GRAPH_API_KEY", "")

  # Dev/local only; in AWS the graph.yml tier decides the backend.
  GRAPH_SHARED_REPOSITORY_BACKEND = get_str_env("GRAPH_SHARED_REPOSITORY_BACKEND", "")

  # DynamoDB registries
  GRAPH_REGISTRY_TABLE = get_str_env(
    "GRAPH_REGISTRY_TABLE", f"robosystems-graph-{ENVIRONMENT}-graph-registry"
  )
  INSTANCE_REGISTRY_TABLE = get_str_env(
    "INSTANCE_REGISTRY_TABLE", f"robosystems-graph-{ENVIRONMENT}-instance-registry"
  )
  VOLUME_REGISTRY_TABLE = get_str_env(
    "VOLUME_REGISTRY_TABLE", f"robosystems-graph-{ENVIRONMENT}-volume-registry"
  )

  # The single writer hosting shared repositories; adapter pipelines scale it
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
  # Staging-connection caps for hosts with no CLUSTER_TIER (the tier config
  # wins otherwise).
  DUCKDB_MEMORY_LIMIT = get_str_env("DUCKDB_MEMORY_LIMIT", "2GB")
  DUCKDB_MAX_THREADS = get_int_env("DUCKDB_MAX_THREADS", 4)

  # Artifact Storage (precomputed Parquet files for enrichment refinement)
  ARTIFACT_PATH = get_str_env("ARTIFACT_PATH", "./data/artifacts")

  # LanceDB Vector Search Index (for MCP element resolution)
  LANCE_INDEX_PATH = get_str_env("LANCE_INDEX_PATH", "./data/lance")

  # LadybugDB Admission Control (SSM: /tuning/lbug_admission/)
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

  # DATABASE_URL env var, else built from DATABASE_ENDPOINT + the Secrets
  # Manager password (EC2 graph instances), else the local default.
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

  # Base URL without a database number (see valkey_registry.py).
  VALKEY_URL = get_str_env("VALKEY_URL", "redis://localhost:6379")

  # From Secrets Manager robosystems/{env}/valkey in prod/staging.
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

  # Credentials come from IAM roles in ECS/EC2.
  AWS_DEFAULT_REGION = get_str_env("AWS_DEFAULT_REGION", "us-east-1")
  AWS_REGION = get_str_env("AWS_REGION", AWS_DEFAULT_REGION)
  AWS_ENDPOINT_URL = get_str_env("AWS_ENDPOINT_URL", "")  # For LocalStack
  # Browser-reachable S3 endpoint for presigned URLs in dev (AWS_ENDPOINT_URL
  # is a docker hostname). Unset in staging/prod.
  AWS_S3_PRESIGN_ENDPOINT_URL = get_str_env("AWS_S3_PRESIGN_ENDPOINT_URL", "")

  # Bedrock keys are dev-only .env overrides; prod/staging use IAM roles.
  AWS_BEDROCK_REGION = get_str_env("AWS_BEDROCK_REGION", "us-east-1")
  AWS_BEDROCK_ACCESS_KEY_ID = get_str_env("AWS_BEDROCK_ACCESS_KEY_ID", "")
  AWS_BEDROCK_SECRET_ACCESS_KEY = get_str_env("AWS_BEDROCK_SECRET_ACCESS_KEY", "")

  # Self-hosted OpenAI-compatible inference (vLLM, Ollama, NIM). Off means the
  # model row does not exist and nothing below is read.
  OPENAI_COMPAT_ENABLED = get_bool_env(
    "OPENAI_COMPAT_ENABLED",
    get_parameter_value("OPENAI_COMPAT_ENABLED", "false").lower() == "true",
  )
  OPENAI_COMPAT_BASE_URL = (
    get_secret_value("OPENAI_COMPAT_BASE_URL", "") if OPENAI_COMPAT_ENABLED else ""
  )
  OPENAI_COMPAT_MODEL = (
    get_secret_value("OPENAI_COMPAT_MODEL", "") if OPENAI_COMPAT_ENABLED else ""
  )
  # Empty for servers that take no key (a local Ollama or vLLM).
  OPENAI_COMPAT_API_KEY = (
    get_secret_value("OPENAI_COMPAT_API_KEY", "") if OPENAI_COMPAT_ENABLED else ""
  )
  # The model's own output cap when below the profiles' ask; 0 = none.
  OPENAI_COMPAT_MAX_OUTPUT_TOKENS = get_int_env("OPENAI_COMPAT_MAX_OUTPUT_TOKENS", 0)
  # A local model on modest hardware can take minutes per call.
  OPENAI_COMPAT_TIMEOUT_SECONDS = get_int_env("OPENAI_COMPAT_TIMEOUT_SECONDS", 300)
  # Credits per 1K tokens; 0 because a self-hosted GPU has no per-token cost.
  OPENAI_COMPAT_CREDITS_PER_1K_INPUT = get_str_env(
    "OPENAI_COMPAT_CREDITS_PER_1K_INPUT", "0"
  )
  OPENAI_COMPAT_CREDITS_PER_1K_OUTPUT = get_str_env(
    "OPENAI_COMPAT_CREDITS_PER_1K_OUTPUT", "0"
  )

  # Model short name (config/operators.py) backing each operator tier; unset
  # keeps the platform mapping, an unknown name fails the boot.
  OPERATOR_PROFILE_ECONOMY = get_str_env("OPERATOR_PROFILE_ECONOMY", "")
  OPERATOR_PROFILE_BALANCED = get_str_env("OPERATOR_PROFILE_BALANCED", "")
  OPERATOR_PROFILE_QUALITY = get_str_env("OPERATOR_PROFILE_QUALITY", "")

  AWS_S3_ACCESS_KEY_ID = get_secret_value("AWS_S3_ACCESS_KEY_ID", "")
  AWS_S3_SECRET_ACCESS_KEY = get_secret_value("AWS_S3_SECRET_ACCESS_KEY", "")

  # Set by CloudFormation; defaults are local-dev only.
  SHARED_RAW_BUCKET = get_str_env("SHARED_RAW_BUCKET", "robosystems-shared-raw")
  SHARED_PROCESSED_BUCKET = get_str_env(
    "SHARED_PROCESSED_BUCKET", "robosystems-shared-processed"
  )
  USER_DATA_BUCKET = get_str_env("USER_DATA_BUCKET", "robosystems-user")
  PUBLIC_DATA_BUCKET = get_str_env("PUBLIC_DATA_BUCKET", "robosystems-public-data")
  DEPLOYMENT_BUCKET = get_str_env("DEPLOYMENT_BUCKET", "robosystems-deployment")
  LOGS_BUCKET = get_str_env("LOGS_BUCKET", "robosystems-logs")

  PUBLIC_DATA_CDN_URL = get_str_env("PUBLIC_DATA_CDN_URL", "")

  # ==========================================================================
  # 8. EXTERNAL SERVICE API KEYS
  # ==========================================================================

  # Enterprise SSO (OIDC) IdP client secret
  SSO_OIDC_CLIENT_SECRET = get_secret_value("SSO_OIDC_CLIENT_SECRET", "")

  # QuickBooks/Intuit
  INTUIT_CLIENT_ID = get_secret_value("INTUIT_CLIENT_ID", "")
  INTUIT_CLIENT_SECRET = get_secret_value("INTUIT_CLIENT_SECRET", "")
  INTUIT_REDIRECT_URI = get_secret_value(
    "INTUIT_REDIRECT_URI", "http://localhost:8000/auth/callback"
  )
  INTUIT_ENVIRONMENT = get_secret_value("INTUIT_ENVIRONMENT", "sandbox")

  # Mercury partner OAuth client; MERCURY_ENVIRONMENT picks the host.
  MERCURY_CLIENT_ID = get_secret_value("MERCURY_CLIENT_ID", "")
  MERCURY_CLIENT_SECRET = get_secret_value("MERCURY_CLIENT_SECRET", "")
  MERCURY_ENVIRONMENT = get_secret_value("MERCURY_ENVIRONMENT", "sandbox")

  # Plaid: one client id, a secret per environment.
  PLAID_CLIENT_ID = get_secret_value("PLAID_CLIENT_ID", "")
  PLAID_SECRET = get_secret_value("PLAID_SECRET", "")
  PLAID_ENVIRONMENT = get_secret_value("PLAID_ENVIRONMENT", "sandbox")

  # SEC
  SEC_GOV_USER_AGENT = get_secret_value(
    "SEC_GOV_USER_AGENT", "YourCompany your-email@example.com"
  )
  # Also keep text-block values in the graph (local experiments only: they are
  # most of a filing's bytes). Such filings carry value_type=inline, so search
  # resolves no content_url for them.
  XBRL_KEEP_TEXTBLOCKS_INLINE = get_bool_env("XBRL_KEEP_TEXTBLOCKS_INLINE", False)
  # Publish each filing's holon, Tavi model, primary document and manifest,
  # plus the per-filer catalog, to the public data bucket.
  SEC_FILING_ARTIFACTS_ENABLED = get_bool_env("SEC_FILING_ARTIFACTS_ENABLED", True)

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
  def expose_invite_token_in_response(cls) -> bool:
    """Fail-closed: requires an explicit dev or staging ENVIRONMENT, so an
    unrecognized value denies rather than leaks the invite token.
    """
    return cls.AUTH_INVITE_TOKEN_IN_RESPONSE and (
      cls.is_development() or cls.is_staging()
    )

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
    """One message per problem; empty means usable."""
    errors = []

    if cls.is_production():
      required_vars = [
        ("DATABASE_URL", cls.DATABASE_URL, None),
        ("JWT_SECRET_KEY", cls.JWT_SECRET_KEY, ""),
      ]

      for var_name, var_value, default_value in required_vars:
        if not var_value or var_value == default_value:
          errors.append(f"{var_name} must be set in production")

    if cls.PORT < 1 or cls.PORT > 65535:
      errors.append("PORT must be between 1 and 65535")

    return errors

  @classmethod
  def get_lbug_tier_config(cls) -> dict[str, Any]:
    """LadybugDB tier settings from graph.yml for CLUSTER_TIER, else env vars."""
    from robosystems.config.constants import MAX_QUERY_LENGTH

    try:
      from robosystems.config.graph_tier import GraphTierConfig

      tier = cls.CLUSTER_TIER

      if tier:
        instance_config = GraphTierConfig.get_instance_config(tier)

        if instance_config:
          full_tier_config = GraphTierConfig.get_tier_config(tier)

          return {
            "max_memory_mb": instance_config.get("max_memory_mb", 2048),
            "memory_per_db_mb": instance_config.get("memory_per_db_mb", 0),
            "memory_per_subgraph_mb": instance_config.get("memory_per_subgraph_mb", 0),
            "chunk_size": instance_config.get("chunk_size", 1000),
            "query_timeout": instance_config.get(
              "query_timeout", cls.GRAPH_QUERY_TIMEOUT
            ),
            "max_query_length": instance_config.get(
              "max_query_length", MAX_QUERY_LENGTH
            ),
            "connection_pool_size": instance_config.get("connection_pool_size", 10),
            # Dev may override via LBUG_DATABASES_PER_INSTANCE.
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
            # Storage, credits and rate multipliers live in GraphTierConfig,
            # BillingConfig and RateLimitConfig, not here.
            "tier": tier,
            "max_subgraphs": full_tier_config.get("max_subgraphs", 0),
          }
    except ImportError:
      pass
    except Exception:
      pass

    return {
      "max_memory_mb": get_int_env("LBUG_MAX_MEMORY_MB", 2048),
      "memory_per_db_mb": get_int_env("LBUG_MAX_MEMORY_PER_DB_MB", 0),
      "memory_per_subgraph_mb": 0,
      "chunk_size": get_int_env("LBUG_CHUNK_SIZE", 1000),
      "query_timeout": cls.GRAPH_QUERY_TIMEOUT,
      "max_query_length": MAX_QUERY_LENGTH,
      "connection_pool_size": get_int_env("LBUG_CONNECTION_POOL_SIZE", 10),
      "databases_per_instance": get_int_env("LBUG_DATABASES_PER_INSTANCE", 10),
      "max_databases": get_int_env("LBUG_DATABASES_PER_INSTANCE", 10),
      "tier": "ladybug-standard",
      "max_subgraphs": 0,
    }

  @classmethod
  def get_lbug_memory_config(cls) -> dict[str, Any]:
    """Alias of get_lbug_tier_config."""
    return cls.get_lbug_tier_config()

  @classmethod
  def get_database_url(cls, database_name: str | None = None) -> str:
    """Get DATABASE_URL, optionally swapping in a different database name."""
    if not database_name:
      return cls.DATABASE_URL

    base_url = cls.DATABASE_URL.rsplit("/", 1)[0]
    return f"{base_url}/{database_name}"

  @classmethod
  def get_aws_config(cls) -> dict:
    """boto3 kwargs; deliberately no credentials (IAM roles)."""
    config = {
      "region_name": cls.AWS_DEFAULT_REGION,
    }

    if cls.AWS_ENDPOINT_URL:
      config["endpoint_url"] = cls.AWS_ENDPOINT_URL

    return config

  @classmethod
  def get_s3_config(cls) -> dict:
    """boto3 kwargs for S3; S3-specific credentials when set, else IAM roles."""
    config = {
      "region_name": cls.AWS_DEFAULT_REGION,
    }

    if cls.AWS_S3_ACCESS_KEY_ID:
      config["aws_access_key_id"] = cls.AWS_S3_ACCESS_KEY_ID

    if cls.AWS_S3_SECRET_ACCESS_KEY:
      config["aws_secret_access_key"] = cls.AWS_S3_SECRET_ACCESS_KEY

    if cls.AWS_ENDPOINT_URL:
      config["endpoint_url"] = cls.AWS_ENDPOINT_URL

    return config

  @classmethod
  def get_r2_config(cls) -> dict:
    """boto3 kwargs for Cloudflare R2, or {} when R2 is not configured."""
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
    """Alias of get_main_cors_origins."""
    return cls.get_main_cors_origins()

  @classmethod
  def get_main_cors_origins(cls) -> list[str]:
    """CORS origins for the main API.

    Deployed environments derive them from the app URLs, so a fork on its own
    domain needs no code change. Also backs OAuth redirect_uri validation and
    the MCP remote origin check.
    """
    if cls.is_production() or cls.is_staging():
      origins = []
      for url in (
        cls.ROBOLEDGER_URL,
        cls.ROBOINVESTOR_URL,
        cls.ROBOSYSTEMS_URL,
        cls.VIEWER_URL,
        cls.HOLON_URL,
      ):
        origin = _url_origin(url)
        if origin and origin not in origins:
          origins.append(origin)
      return origins
    else:
      origins = [
        "http://localhost:3000",
        "http://localhost:3001",
        "http://localhost:3002",
        "http://localhost:8000",
        "https://roboledger.ai",
        "https://roboinvestor.ai",
        "https://robosystems.ai",
      ]
      # Dev-only extras (e.g. ngrok tunnels for OAuth callbacks).
      extra = get_secret_list_value("EXTRA_CORS_ORIGINS", "")
      origins.extend(o for o in extra if o not in origins)
      return origins

  @classmethod
  def get_passkey_rp_id(cls) -> str:
    """WebAuthn RP ID: the host of ROBOSYSTEMS_URL (the login home hosts every
    ceremony). Dev uses ``localhost`` because its ROBOSYSTEMS_URL default is
    the managed domain.
    """
    if cls.PASSKEY_RP_ID:
      return cls.PASSKEY_RP_ID
    if cls.is_production() or cls.is_staging():
      return urlparse((cls.ROBOSYSTEMS_URL or "").strip()).hostname or ""
    return "localhost"

  @classmethod
  def get_passkey_origin(cls) -> str:
    """Expected WebAuthn ceremony origin: the login home's origin."""
    if cls.PASSKEY_ORIGIN:
      return cls.PASSKEY_ORIGIN
    if cls.is_production() or cls.is_staging():
      return _url_origin(cls.ROBOSYSTEMS_URL) or ""
    return "http://localhost:3000"

  @classmethod
  def get_lbug_cors_origins(cls) -> list[str]:
    """Get CORS origins for Graph API (VPC-internal)."""
    if cls.is_production() or cls.is_staging():
      return []
    else:
      return ["*"]

  @classmethod
  def get_valkey_url(cls, database: Union[int, "ValkeyDatabase"] | None = None) -> str:
    """Valkey URL for ``database`` (prefer a ValkeyDatabase member over an
    int); None returns the base URL.
    """
    if database is None:
      return cls.VALKEY_URL

    from .valkey_registry import ValkeyDatabase, ValkeyURLBuilder

    if isinstance(database, ValkeyDatabase):
      return ValkeyURLBuilder.build_url(cls.VALKEY_URL, database)
    else:
      return f"{cls.VALKEY_URL.rstrip('/')}/{database}"


env = EnvConfig()
