"""
Centralized environment variable configuration.

This module provides a single source of truth for all environment variables,
with type conversions, validation, and default values.

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
    """
    Fallback implementation when secrets_manager isn't available.
    Simply returns environment variable or default value.
    """
    return os.getenv(key, default)

  def get_secret_list_value(
    key: str, default: str = "", separator: str = ","
  ) -> list[str]:
    """
    Fallback implementation when secrets_manager isn't available.
    Returns environment variable split by separator or default value split.
    """
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
  """
  Get an integer environment variable with safe type conversion.

  Args:
      key: Environment variable name
      default: Default value if not set or invalid

  Returns:
      Integer value from environment or default
  """
  try:
    return int(os.getenv(key, str(default)))
  except (ValueError, TypeError):
    # Use print instead of logger to avoid circular import
    print(f"Warning: Invalid {key} value, using default: {default}")
    return default


def get_float_env(key: str, default: float) -> float:
  """
  Get a float environment variable with safe type conversion.

  Args:
      key: Environment variable name
      default: Default value if not set or invalid

  Returns:
      Float value from environment or default
  """
  try:
    return float(os.getenv(key, str(default)))
  except (ValueError, TypeError):
    # Use print instead of logger to avoid circular import
    print(f"Warning: Invalid {key} value, using default: {default}")
    return default


def get_bool_env(key: str, default: bool = False) -> bool:
  """
  Get a boolean environment variable.

  Args:
      key: Environment variable name
      default: Default value if not set

  Returns:
      Boolean value from environment or default
  """
  value = os.getenv(key, str(default)).lower()
  return value in ("true", "1", "yes", "on")


def get_str_env(key: str, default: str = "") -> str:
  """
  Get a string environment variable.

  Args:
      key: Environment variable name
      default: Default value if not set

  Returns:
      String value from environment or default
  """
  return os.getenv(key, default)


def get_tuning_float(env_key: str, ssm_path: str, default: float) -> float:
  """
  Get a float tuning parameter with layered fallback.

  Priority order:
  1. Environment variable (highest - for local dev, CI, testing)
  2. SSM Parameter Store /tuning/ path (for AWS runtime config)
  3. Default value (lowest - sensible defaults)

  Args:
      env_key: Environment variable name (e.g., "LBUG_ADMISSION_MEMORY_THRESHOLD")
      ssm_path: SSM path under tuning/ (e.g., "lbug_admission/MEMORY_THRESHOLD")
      default: Default value if not found anywhere

  Returns:
      Float value from the highest-priority source
  """
  # Priority 1: Environment variable
  env_value = os.getenv(env_key)
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
  1. Environment variable (highest - for local dev, CI, testing)
  2. SSM Parameter Store /tuning/ path (for AWS runtime config)
  3. Default value (lowest - sensible defaults)

  Args:
      env_key: Environment variable name (e.g., "GRAPH_HTTP_TIMEOUT")
      ssm_path: SSM path under tuning/ (e.g., "timeouts/GRAPH_HTTP")
      default: Default value if not found anywhere

  Returns:
      Integer value from the highest-priority source
  """
  # Priority 1: Environment variable
  env_value = os.getenv(env_key)
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
  """
  Get a list environment variable (comma-separated by default).

  Args:
      key: Environment variable name
      default: Default value if not set
      separator: String separator for list items

  Returns:
      List of strings from environment or default
  """
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
  """
  Auto-discover shared replica ALB URL from CloudFormation.

  Looks up the RoboSystemsGraphSharedReplicas{Prod|Staging} stack
  and returns the ALBEndpoint output value.

  Returns:
      ALB endpoint URL (e.g., http://internal-...:8001) or empty string if not found
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
  """
  Centralized environment variable configuration.

  Variables are organized into logical groups matching the .env file layout.
  All variables use type-safe helper functions for consistent behavior.
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
  GRAPH_BACKUP_ENCRYPTION_KEY = get_secret_value("GRAPH_BACKUP_ENCRYPTION_KEY", "")

  # Service URLs
  # ROBOSYSTEMS_API_URL is set by CloudFormation based on access mode (domain or ALB DNS)
  ROBOSYSTEMS_API_URL = get_str_env("ROBOSYSTEMS_API_URL", "https://api.robosystems.ai")
  # Frontend URLs - set via CloudFormation environment variables
  ROBOLEDGER_URL = get_str_env("ROBOLEDGER_URL", "https://roboledger.ai")
  ROBOINVESTOR_URL = get_str_env("ROBOINVESTOR_URL", "https://roboinvestor.ai")
  ROBOSYSTEMS_URL = get_str_env("ROBOSYSTEMS_URL", "https://robosystems.ai")

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
  AGENT_POST_ENABLED = get_bool_env(
    "AGENT_POST_ENABLED",
    get_parameter_value("AGENT_POST_ENABLED", "true").lower() == "true",
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
  MCP_MEMORY_ENABLED = get_bool_env(
    "MCP_MEMORY_ENABLED",
    get_parameter_value("MCP_MEMORY_ENABLED", "true").lower() == "true",
  )
  MCP_SEMANTIC_MEMORY_ENABLED = get_bool_env(
    "MCP_SEMANTIC_MEMORY_ENABLED",
    get_parameter_value("MCP_SEMANTIC_MEMORY_ENABLED", "false").lower() == "true",
  )
  MCP_VECTOR_SEARCH_ENABLED = get_bool_env(
    "MCP_VECTOR_SEARCH_ENABLED",
    get_parameter_value("MCP_VECTOR_SEARCH_ENABLED", "false").lower() == "true",
  )
  SEMANTIC_SEARCH_ENABLED = get_bool_env(
    "SEMANTIC_SEARCH_ENABLED",
    get_parameter_value("SEMANTIC_SEARCH_ENABLED", "false").lower() == "true",
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
  # CONNECTIONS_ENABLED controls whether the /connections router is included
  # Individual provider flags below require CONNECTIONS_ENABLED=true to function
  CONNECTIONS_ENABLED = get_bool_env(
    "CONNECTIONS_ENABLED",
    get_parameter_value("CONNECTIONS_ENABLED", "true").lower() == "true",
  )
  # Individual provider flags (require CONNECTIONS_ENABLED=true)
  CONNECTION_SEC_ENABLED = get_bool_env(
    "CONNECTION_SEC_ENABLED",
    get_parameter_value("CONNECTION_SEC_ENABLED", "true").lower() == "true",
  )
  CONNECTION_QUICKBOOKS_ENABLED = get_bool_env(
    "CONNECTION_QUICKBOOKS_ENABLED",
    get_parameter_value("CONNECTION_QUICKBOOKS_ENABLED", "true").lower() == "true",
  )

  # --- Extensions OLTP Database ---
  # Controls the extensions PostgreSQL database (engine, migrations, Dagster jobs).
  # Required for any extension module (RoboLedger, RoboInvestor, etc.).
  EXTENSIONS_ENABLED = get_bool_env(
    "EXTENSIONS_ENABLED",
    get_parameter_value("EXTENSIONS_ENABLED", "false").lower() == "true",
  )

  # --- Ledger API Endpoints ---
  # Controls the /v1/ledger/* API routes. Requires EXTENSIONS_ENABLED=true.
  LEDGER_ENABLED = get_bool_env(
    "LEDGER_ENABLED",
    get_parameter_value("LEDGER_ENABLED", "false").lower() == "true",
  )

  # --- Investor API Endpoints ---
  # Controls the /v1/investor/* API routes. Requires EXTENSIONS_ENABLED=true.
  INVESTOR_ENABLED = get_bool_env(
    "INVESTOR_ENABLED",
    get_parameter_value("INVESTOR_ENABLED", "false").lower() == "true",
  )

  # --- Extensions GraphQL Endpoint ---
  # Controls the /extensions/graphql endpoint (Strawberry). Requires at least
  # one of LEDGER_ENABLED or INVESTOR_ENABLED to be useful.
  EXTENSIONS_GRAPHQL_ENABLED = get_bool_env(
    "EXTENSIONS_GRAPHQL_ENABLED",
    get_parameter_value("EXTENSIONS_GRAPHQL_ENABLED", "false").lower() == "true",
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
    "SEC_GOV_USER_AGENT", "RoboSystems hello@robosystems.ai"
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
    """
    Get normalized environment key for configuration lookups.

    Returns:
        Normalized environment name: 'production', 'staging', or 'development'
    """
    env_lower = cls.ENVIRONMENT.lower()
    if env_lower in ["prod", "production"]:
      return "production"
    elif env_lower in ["staging", "stage"]:
      return "staging"
    else:
      return "development"

  @classmethod
  def is_using_secrets_manager(cls) -> bool:
    """
    Check if AWS Secrets Manager is available and being used.

    Returns:
        True if secrets_manager module is loaded and environment is prod/staging
    """
    return SECRETS_MANAGER_AVAILABLE and cls.ENVIRONMENT.lower() in [
      "prod",
      "production",
      "staging",
      "stage",
    ]

  @classmethod
  @lru_cache(maxsize=1)
  def validate(cls) -> list[str]:
    """
    Validate required environment variables.

    Returns:
        List of validation errors (empty if all valid)
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
    """
    Get LadybugDB tier-specific configuration, with overrides from graph.yml.

    This allows the container to override environment variables with
    tier-specific configuration from the graph.yml file, including:
    - Memory settings (max_memory_mb, memory_per_db_mb)
    - Performance settings (chunk_size, query_timeout, max_query_length)
    - Connection settings (connection_pool_size, databases_per_instance)

    Returns:
        Dictionary with tier configuration values
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
            # Tier-level settings from full config
            "tier": tier,
            "storage_limit_gb": full_tier_config.get("storage_limit_gb", 500),
            "monthly_credits": full_tier_config.get("monthly_credits", 10000),
            "api_rate_multiplier": full_tier_config.get("api_rate_multiplier", 1.0),
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
      "storage_limit_gb": 500,
      "monthly_credits": 10000,
      "api_rate_multiplier": 1.0,
      "max_subgraphs": 0,
    }

  @classmethod
  def get_lbug_memory_config(cls) -> dict[str, Any]:
    """Alias for backward compatibility with existing code."""
    return cls.get_lbug_tier_config()

  @classmethod
  def get_database_url(cls, database_name: str | None = None) -> str:
    """
    Get database URL, optionally with a specific database name.

    Args:
        database_name: Optional database name to use

    Returns:
        Database URL string
    """
    if not database_name:
      return cls.DATABASE_URL

    # Parse and replace database name
    base_url = cls.DATABASE_URL.rsplit("/", 1)[0]
    return f"{base_url}/{database_name}"

  @classmethod
  def get_aws_config(cls) -> dict:
    """
    Get AWS configuration as a dict for boto3.

    Note: AWS credentials should come from IAM roles in production.
    This method only sets region and endpoint configuration.
    """
    config = {
      "region_name": cls.AWS_DEFAULT_REGION,
    }

    if cls.AWS_ENDPOINT_URL:
      config["endpoint_url"] = cls.AWS_ENDPOINT_URL

    return config

  @classmethod
  def get_s3_config(cls) -> dict:
    """
    Get S3-specific AWS configuration as a dict for boto3.
    Uses S3-specific credentials if provided, otherwise relies on IAM roles.
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
    """
    Get Cloudflare R2 configuration as a dict for boto3.
    R2 uses S3-compatible API with a custom endpoint URL.
    Returns empty dict if R2 is not configured.
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
      ]
    elif cls.is_staging():
      return [
        "https://staging.roboledger.ai",
        "https://staging.roboinvestor.ai",
        "https://staging.robosystems.ai",
      ]
    else:
      # Development
      return [
        "http://localhost:3000",
        "http://localhost:3001",
        "http://localhost:3002",
        "http://localhost:8000",
        "https://roboledger.ai",
        "https://roboinvestor.ai",
        "https://robosystems.ai",
      ]

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
    """
    Get Valkey/Redis URL with optional database number.

    Args:
        database: Database number (0-15) or ValkeyDatabase enum value.
                  If None, returns base URL without database.

    Returns:
        Valkey/Redis URL string

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
