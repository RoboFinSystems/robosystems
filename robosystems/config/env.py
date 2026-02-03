"""
Centralized environment variable configuration.

This module provides a single source of truth for all environment variables,
with type conversions, validation, and default values.

Organization (mirrors .env file section order):
- Helper functions for type-safe env var access
- Core application configuration (env, encryption, service URLs, JWT, email, CAPTCHA)
- Feature flags (security, graph ops, connections, shared repos, org, platform)
- Graph databases (backend selection, API config, resiliency, tiers, LadybugDB, Neo4j)
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
  from .parameter_store import get_parameter_value

  PARAMETER_STORE_AVAILABLE = True
except ImportError:
  # If parameter_store can't be imported, fall back to default values
  PARAMETER_STORE_AVAILABLE = False

  def get_parameter_value(key: str, default: str = "") -> str:
    """
    Fallback implementation when parameter_store isn't available.
    Simply returns environment variable or default value.
    """
    return os.getenv(key, default)


from .constants import (
  ADMISSION_CHECK_INTERVAL,
  ADMISSION_CPU_THRESHOLD_DEFAULT,
  # Task constants
  ADMISSION_MEMORY_THRESHOLD_DEFAULT,
  ADMISSION_QUEUE_THRESHOLD_DEFAULT,
  CACHE_TTL_LONG,
  # Cache constants
  CACHE_TTL_SHORT,
  # Fixed business rules
  CREDIT_ALLOCATION_DAY,
  CREDIT_ALLOCATION_HOUR,
  # Performance constants
  DEFAULT_HTTP_TIMEOUT,
  DEFAULT_MAX_CONCURRENT,
  DEFAULT_MAX_OVERFLOW,
  DEFAULT_POOL_RECYCLE,
  # Database constants
  DEFAULT_POOL_SIZE,
  DEFAULT_POOL_TIMEOUT,
  DEFAULT_QUERY_LIMIT,
  DEFAULT_QUERY_TIMEOUT,
  DEFAULT_QUEUE_SIZE,
  # DuckDB configuration
  DUCKDB_MAX_THREADS,
  DUCKDB_MEMORY_LIMIT,
  EMAIL_TOKEN_EXPIRY_HOURS,
  GRAPH_CIRCUIT_BREAKER_THRESHOLD,
  GRAPH_CIRCUIT_BREAKER_TIMEOUT,
  GRAPH_CONNECT_TIMEOUT,
  # Resiliency and circuit breaker
  GRAPH_INSTANCE_CACHE_TTL,
  GRAPH_LARGE_CHUNK_SIZE,
  GRAPH_LARGE_MAX_MEMORY_MB,
  GRAPH_LARGE_MEMORY_PER_DB_MB,
  # Fixed technical limits
  GRAPH_MAX_REQUEST_SIZE,
  GRAPH_READ_TIMEOUT,
  # Tier-specific chunk sizes
  GRAPH_STANDARD_CHUNK_SIZE,
  # Tier-specific memory allocations
  GRAPH_STANDARD_MAX_MEMORY_MB,
  GRAPH_STANDARD_MEMORY_PER_DB_MB,
  GRAPH_XLARGE_CHUNK_SIZE,
  GRAPH_XLARGE_MAX_MEMORY_MB,
  GRAPH_XLARGE_MEMORY_PER_DB_MB,
  JWT_EXPIRY_HOURS,
  # Load shedding
  LOAD_SHED_START_PRESSURE_DEFAULT,
  LOAD_SHED_STOP_PRESSURE_DEFAULT,
  MAX_DATABASES_PER_NODE,
  MAX_QUERY_LENGTH,
  OPENFIGI_RETRY_MAX_WAIT,
  OPENFIGI_RETRY_MIN_WAIT,
  PASSWORD_RESET_TOKEN_EXPIRY_HOURS,
  QUERY_DEFAULT_PRIORITY,
  QUERY_PRIORITY_BOOST_PREMIUM,
  # Queue configuration
  QUERY_QUEUE_MAX_PER_USER,
  QUERY_QUEUE_TIMEOUT,
  # Retry configuration
  SEC_PIPELINE_MAX_RETRIES,
  SEC_RATE_LIMIT,
  # API version constants
  STRIPE_API_VERSION,
  TOKEN_GRACE_PERIOD_MINUTES,
  XBRL_GRAPH_LARGE_NODES,
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
  # Frontend URLs - configurable via Secrets Manager for forks with custom domains
  ROBOLEDGER_URL = get_secret_value("ROBOLEDGER_URL", "https://roboledger.ai")
  ROBOINVESTOR_URL = get_secret_value("ROBOINVESTOR_URL", "https://roboinvestor.ai")
  ROBOSYSTEMS_URL = get_secret_value("ROBOSYSTEMS_URL", "https://robosystems.ai")

  # JWT configuration
  JWT_SECRET_KEY = get_secret_value("JWT_SECRET_KEY", "")
  JWT_EXPIRY_HOURS = JWT_EXPIRY_HOURS  # Constant - 30 minutes

  # JWT Issuer and Audience - configurable for different deployments
  # Default JWT_ISSUER is derived from ROBOSYSTEMS_API_URL (strips protocol)
  # Override via Secrets Manager for custom deployments:
  # - internal mode: set JWT_ISSUER=localhost (access via bastion tunnel)
  # - public-http mode: uses ALB DNS automatically, or set custom value
  # Derive default JWT issuer/audience from ROBOSYSTEMS_API_URL, stripping protocol
  _jwt_default_domain = (
    os.getenv("ROBOSYSTEMS_API_URL", "https://api.robosystems.ai")
    .replace("https://", "")
    .replace("http://", "")
  )
  JWT_ISSUER = get_secret_value("JWT_ISSUER", _jwt_default_domain)
  JWT_AUDIENCE = get_secret_list_value("JWT_AUDIENCE", _jwt_default_domain)

  # Authentication Security Settings (constants - not runtime configurable)
  TOKEN_GRACE_PERIOD_MINUTES = TOKEN_GRACE_PERIOD_MINUTES

  # Authentication Rate Limiting (overrides for defaults in constants.py)
  JWT_REFRESH_RATE_LIMIT = get_int_env("JWT_REFRESH_RATE_LIMIT", 20)
  AUTH_RATE_LIMIT_LOGIN = get_int_env("AUTH_RATE_LIMIT_LOGIN", 5)
  AUTH_RATE_LIMIT_REGISTER = get_int_env("AUTH_RATE_LIMIT_REGISTER", 3)

  # Email service configuration
  EMAIL_FROM_ADDRESS = get_str_env(
    "EMAIL_FROM_ADDRESS",
    get_secret_value("EMAIL_FROM_ADDRESS", "noreply@robosystems.ai"),
  )
  EMAIL_FROM_NAME = get_str_env(
    "EMAIL_FROM_NAME",
    get_secret_value("EMAIL_FROM_NAME", "RoboSystems"),
  )

  # Token expiry configuration (constants - not runtime configurable)
  EMAIL_TOKEN_EXPIRY_HOURS = EMAIL_TOKEN_EXPIRY_HOURS
  PASSWORD_RESET_TOKEN_EXPIRY_HOURS = PASSWORD_RESET_TOKEN_EXPIRY_HOURS

  # Cloudflare Turnstile (CAPTCHA)
  TURNSTILE_SECRET_KEY = get_secret_value("TURNSTILE_SECRET_KEY", "")
  TURNSTILE_SITE_KEY = get_secret_value("TURNSTILE_SITE_KEY", "")

  # ==========================================================================
  # 2. FEATURE FLAGS
  # ==========================================================================
  # Feature flags use SSM Parameter Store instead of Secrets Manager.
  # This provides cost savings (SSM Standard tier is FREE) and better
  # separation between secrets (credentials) and configuration (flags).
  #
  # Override priority: env var > SSM Parameter Store > default value

  # --- Security & Auth ---
  USER_REGISTRATION_ENABLED = get_bool_env(
    "USER_REGISTRATION_ENABLED",
    get_parameter_value("USER_REGISTRATION_ENABLED", "false").lower() == "true",
  )
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
  CORS_ALLOW_CREDENTIALS = get_bool_env("CORS_ALLOW_CREDENTIALS", True)
  CSP_TRUSTED_TYPES_ENABLED = get_bool_env(
    "CSP_TRUSTED_TYPES_ENABLED",
    get_parameter_value("CSP_TRUSTED_TYPES_ENABLED", "false").lower() == "true",
  )

  # --- Graph Operations ---
  DIRECT_GRAPH_PROVISIONING_ENABLED = get_bool_env(
    "DIRECT_GRAPH_PROVISIONING_ENABLED",
    get_parameter_value("DIRECT_GRAPH_PROVISIONING_ENABLED", "true").lower() == "true",
  )
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

  # --- Connection Providers ---
  CONNECTION_SEC_ENABLED = get_bool_env(
    "CONNECTION_SEC_ENABLED",
    get_parameter_value("CONNECTION_SEC_ENABLED", "false").lower() == "true",
  )
  CONNECTION_QUICKBOOKS_ENABLED = get_bool_env(
    "CONNECTION_QUICKBOOKS_ENABLED",
    get_parameter_value("CONNECTION_QUICKBOOKS_ENABLED", "false").lower() == "true",
  )
  CONNECTION_PLAID_ENABLED = get_bool_env(
    "CONNECTION_PLAID_ENABLED",
    get_parameter_value("CONNECTION_PLAID_ENABLED", "false").lower() == "true",
  )

  # --- Shared Repository Operations ---
  SHARED_MASTER_READS_ENABLED = get_bool_env(
    "SHARED_MASTER_READS_ENABLED",
    get_parameter_value("SHARED_MASTER_READS_ENABLED", "true").lower() == "true",
  )

  # Shared Replica ALB URL (for read scaling)
  # When set, reads to shared repositories will route to the replica ALB
  # instead of the shared master. This allows horizontal scaling of reads.
  # Format: http://internal-robosystems-shared-{env}.{region}.elb.amazonaws.com:8001
  SHARED_REPLICA_ALB_URL = get_str_env("SHARED_REPLICA_ALB_URL", "")

  # Shared repositories list for infrastructure/deployment (used by userdata scripts)
  # This configures which repositories should be deployed on shared writer instances
  # For application logic (checking if a graph is a shared repo), use GraphTypeRegistry.SHARED_REPOSITORIES
  SHARED_REPOSITORIES = get_list_env("SHARED_REPOSITORIES", "")

  # --- Organization ---
  # Note: ORG_GRAPHS_DEFAULT_LIMIT is now tunable via SSM /tuning/limits/
  # This env var remains for backward compatibility and SQLAlchemy column default
  ORG_GRAPHS_DEFAULT_LIMIT = get_int_env("ORG_GRAPHS_DEFAULT_LIMIT", 10)
  ORG_MEMBER_INVITATIONS_ENABLED = get_bool_env(
    "ORG_MEMBER_INVITATIONS_ENABLED",
    get_parameter_value("ORG_MEMBER_INVITATIONS_ENABLED", "false").lower() == "true",
  )

  # --- Platform Operations ---
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

  # ==========================================================================
  # 3. GRAPH DATABASES - MULTI-BACKEND (LADYBUGDB AND NEO4J)
  # ==========================================================================

  # --- Backend Selection ---
  GRAPH_BACKEND_TYPE = get_str_env(
    "GRAPH_BACKEND_TYPE", "ladybug"
  )  # Options: ladybug, neo4j_community, neo4j_enterprise

  # ===========================================================================
  # GRAPH API CONFIGURATION (Backend-Agnostic)
  # ===========================================================================
  # Configuration that applies to all graph database backends (LadybugDB, Neo4j)

  # Graph API Endpoint
  GRAPH_API_URL = get_str_env("GRAPH_API_URL", "http://localhost:8001")
  GRAPH_API_KEY = get_secret_value("GRAPH_API_KEY", "")

  # Shared repository backend selection (dev/local only)
  # In AWS environments, backend is determined by graph.yml tier configuration
  # Values: "ladybug" or "neo4j"
  GRAPH_SHARED_REPOSITORY_BACKEND = get_str_env("GRAPH_SHARED_REPOSITORY_BACKEND", "")

  # Graph Registry Tables (DynamoDB - applies to all backends)
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

  # Instance Metadata (applies to all backends)
  EC2_INSTANCE_ID = get_str_env("INSTANCE_ID", "")
  INSTANCE_ID = get_str_env("INSTANCE_ID", "")
  CLUSTER_TIER = get_str_env("CLUSTER_TIER", "")

  # Graph API Timeouts and Limits
  GRAPH_HTTP_TIMEOUT = get_int_env("GRAPH_HTTP_TIMEOUT", DEFAULT_HTTP_TIMEOUT)
  GRAPH_QUERY_TIMEOUT = get_int_env("GRAPH_QUERY_TIMEOUT", DEFAULT_QUERY_TIMEOUT)
  GRAPH_MAX_QUERY_LENGTH = get_int_env("GRAPH_MAX_QUERY_LENGTH", MAX_QUERY_LENGTH)
  GRAPH_MAX_REQUEST_SIZE = get_int_env("GRAPH_MAX_REQUEST_SIZE", GRAPH_MAX_REQUEST_SIZE)
  GRAPH_CONNECT_TIMEOUT = get_float_env("GRAPH_CONNECT_TIMEOUT", GRAPH_CONNECT_TIMEOUT)
  GRAPH_READ_TIMEOUT = get_float_env("GRAPH_READ_TIMEOUT", GRAPH_READ_TIMEOUT)

  # --- Graph Resiliency and Circuit Breaker Configuration ---
  GRAPH_CIRCUIT_BREAKERS_ENABLED = get_bool_env("GRAPH_CIRCUIT_BREAKERS_ENABLED", True)
  GRAPH_REDIS_CACHE_ENABLED = get_bool_env("GRAPH_REDIS_CACHE_ENABLED", True)
  GRAPH_RETRY_LOGIC_ENABLED = get_bool_env("GRAPH_RETRY_LOGIC_ENABLED", True)
  GRAPH_HEALTH_CHECKS_ENABLED = get_bool_env("GRAPH_HEALTH_CHECKS_ENABLED", True)

  # Maximum staged data size (MB) for direct materialization.
  # Graphs with staged data above this threshold automatically route to
  # Dagster even when DIRECT_GRAPH_MATERIALIZATION_ENABLED=true.
  # This prevents long-running materializations from tying up API workers.
  GRAPH_MATERIALIZATION_THRESHOLD_MB = get_int_env(
    "GRAPH_MATERIALIZATION_THRESHOLD_MB", 500
  )

  GRAPH_INSTANCE_CACHE_TTL = get_int_env(
    "GRAPH_INSTANCE_CACHE_TTL", GRAPH_INSTANCE_CACHE_TTL
  )
  GRAPH_CIRCUIT_BREAKER_THRESHOLD = get_int_env(
    "GRAPH_CIRCUIT_BREAKER_THRESHOLD", GRAPH_CIRCUIT_BREAKER_THRESHOLD
  )
  GRAPH_CIRCUIT_BREAKER_TIMEOUT = get_int_env(
    "GRAPH_CIRCUIT_BREAKER_TIMEOUT", GRAPH_CIRCUIT_BREAKER_TIMEOUT
  )
  GRAPH_HEALTH_CHECK_INTERVAL_MINUTES = get_float_env(
    "GRAPH_HEALTH_CHECK_INTERVAL_MINUTES", 5.0
  )

  # --- Tier-Specific Memory and Performance Overrides ---
  # Tier-specific memory allocations (with environment variable overrides)
  GRAPH_STANDARD_MAX_MEMORY_MB_OVERRIDE = get_int_env(
    "GRAPH_STANDARD_MAX_MEMORY_MB", GRAPH_STANDARD_MAX_MEMORY_MB
  )
  GRAPH_LARGE_MAX_MEMORY_MB_OVERRIDE = get_int_env(
    "GRAPH_LARGE_MAX_MEMORY_MB", GRAPH_LARGE_MAX_MEMORY_MB
  )
  GRAPH_XLARGE_MAX_MEMORY_MB_OVERRIDE = get_int_env(
    "GRAPH_XLARGE_MAX_MEMORY_MB", GRAPH_XLARGE_MAX_MEMORY_MB
  )
  GRAPH_STANDARD_MEMORY_PER_DB_MB_OVERRIDE = get_int_env(
    "GRAPH_STANDARD_MEMORY_PER_DB_MB", GRAPH_STANDARD_MEMORY_PER_DB_MB
  )
  GRAPH_LARGE_MEMORY_PER_DB_MB_OVERRIDE = get_int_env(
    "GRAPH_LARGE_MEMORY_PER_DB_MB", GRAPH_LARGE_MEMORY_PER_DB_MB
  )
  GRAPH_XLARGE_MEMORY_PER_DB_MB_OVERRIDE = get_int_env(
    "GRAPH_XLARGE_MEMORY_PER_DB_MB", GRAPH_XLARGE_MEMORY_PER_DB_MB
  )

  # Tier-specific chunk sizes (with environment variable overrides)
  GRAPH_STANDARD_CHUNK_SIZE_OVERRIDE = get_int_env(
    "GRAPH_STANDARD_CHUNK_SIZE", GRAPH_STANDARD_CHUNK_SIZE
  )
  GRAPH_LARGE_CHUNK_SIZE_OVERRIDE = get_int_env(
    "GRAPH_LARGE_CHUNK_SIZE", GRAPH_LARGE_CHUNK_SIZE
  )
  GRAPH_XLARGE_CHUNK_SIZE_OVERRIDE = get_int_env(
    "GRAPH_XLARGE_CHUNK_SIZE", GRAPH_XLARGE_CHUNK_SIZE
  )

  # ===========================================================================
  # BACKEND-SPECIFIC CONFIGURATION
  # ===========================================================================

  # --- LadybugDB-Specific Configuration (when GRAPH_BACKEND_TYPE=ladybug) ---
  LBUG_DATABASE_PATH = get_str_env("LBUG_DATABASE_PATH", "./data/lbug-dbs")
  LBUG_ACCESS_PATTERN = get_str_env("LBUG_ACCESS_PATTERN", "api_auto")
  LBUG_NODE_TYPE = get_str_env("LBUG_NODE_TYPE", "writer")

  # DuckDB Staging Configuration (for data ingestion/materialization)
  DUCKDB_STAGING_PATH = get_str_env("DUCKDB_STAGING_PATH", "./data/staging")

  # LadybugDB Capacity and Performance
  LBUG_MAX_DATABASES_PER_NODE = get_int_env(
    "LBUG_MAX_DATABASES_PER_NODE", MAX_DATABASES_PER_NODE
  )

  # LadybugDB Memory Configuration (can be overridden per-tier)
  LBUG_MAX_MEMORY_MB = get_int_env("LBUG_MAX_MEMORY_MB", 2048)
  LBUG_MAX_MEMORY_PER_DB_MB = get_int_env("LBUG_MAX_MEMORY_PER_DB_MB", 0)

  # LadybugDB Connection Management
  LBUG_MAX_CONNECTIONS_PER_DB = get_int_env("LBUG_MAX_CONNECTIONS_PER_DB", 10)
  LBUG_CONNECTION_TTL_MINUTES = get_float_env(
    "LBUG_CONNECTION_TTL_MINUTES", 30.0
  )  # 30 minutes default
  LBUG_HEALTH_CHECK_INTERVAL_MINUTES = get_float_env(
    "LBUG_HEALTH_CHECK_INTERVAL_MINUTES", 5.0
  )  # 5 minutes default

  # LadybugDB Admission Control
  # These use SSM tuning parameters in prod/staging for runtime adjustability
  # Override priority: env var > SSM /tuning/lbug_admission/ > default
  LBUG_ADMISSION_MEMORY_THRESHOLD = get_tuning_float(
    "LBUG_ADMISSION_MEMORY_THRESHOLD",
    "lbug_admission/MEMORY_THRESHOLD",
    ADMISSION_MEMORY_THRESHOLD_DEFAULT,
  )
  LBUG_ADMISSION_CPU_THRESHOLD = get_tuning_float(
    "LBUG_ADMISSION_CPU_THRESHOLD",
    "lbug_admission/CPU_THRESHOLD",
    ADMISSION_CPU_THRESHOLD_DEFAULT,
  )

  # DuckDB Configuration (with environment variable overrides)
  DUCKDB_MAX_THREADS = get_int_env("DUCKDB_MAX_THREADS", DUCKDB_MAX_THREADS)
  DUCKDB_MEMORY_LIMIT = os.getenv("DUCKDB_MEMORY_LIMIT", DUCKDB_MEMORY_LIMIT)

  # --- Neo4j-Specific Configuration (when GRAPH_BACKEND_TYPE=neo4j_*) ---
  NEO4J_URI = get_str_env("NEO4J_URI", "bolt://localhost:7687")
  NEO4J_USERNAME = get_str_env("NEO4J_USERNAME", "neo4j")
  NEO4J_PASSWORD = get_secret_value("NEO4J_PASSWORD", "")
  NEO4J_ENTERPRISE = get_bool_env("NEO4J_ENTERPRISE", False)
  NEO4J_MAX_CONNECTION_POOL_SIZE = get_int_env("NEO4J_MAX_CONNECTION_POOL_SIZE", 50)
  NEO4J_CONNECTION_ACQUISITION_TIMEOUT = get_int_env(
    "NEO4J_CONNECTION_ACQUISITION_TIMEOUT", 60
  )
  NEO4J_MAX_CONNECTION_LIFETIME = get_int_env("NEO4J_MAX_CONNECTION_LIFETIME", 3600)

  # ==========================================================================
  # 4. DATABASE CONFIGURATION - POSTGRESQL
  # ==========================================================================

  DATABASE_URL = get_secret_value(
    "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/robosystems"
  )
  DATABASE_POOL_SIZE = get_int_env("DATABASE_POOL_SIZE", DEFAULT_POOL_SIZE)
  DATABASE_MAX_OVERFLOW = get_int_env("DATABASE_MAX_OVERFLOW", DEFAULT_MAX_OVERFLOW)
  DATABASE_POOL_TIMEOUT = get_int_env("DATABASE_POOL_TIMEOUT", DEFAULT_POOL_TIMEOUT)
  DATABASE_POOL_RECYCLE = get_int_env("DATABASE_POOL_RECYCLE", DEFAULT_POOL_RECYCLE)
  DATABASE_ECHO = get_bool_env("DATABASE_ECHO", False)

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

  # Cache TTLs
  CREDIT_BALANCE_CACHE_TTL = get_int_env("CREDIT_BALANCE_CACHE_TTL", CACHE_TTL_SHORT)
  CREDIT_SUMMARY_CACHE_TTL = get_int_env("CREDIT_SUMMARY_CACHE_TTL", 600)  # 10 minutes
  CREDIT_OPERATION_COST_CACHE_TTL = get_int_env(
    "CREDIT_OPERATION_COST_CACHE_TTL", CACHE_TTL_LONG
  )
  JWT_CACHE_TTL = get_int_env("JWT_CACHE_TTL", 1800)  # 30 minutes
  API_KEY_CACHE_TTL = get_int_env("API_KEY_CACHE_TTL", 300)  # 5 minutes

  # Distributed lock TTLs
  INGESTION_LOCK_TTL = get_int_env("INGESTION_LOCK_TTL", 3600)  # 1 hour

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

  # S3 configuration
  AWS_S3_PREFIX = get_str_env("AWS_S3_PREFIX", "robosystems")

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
  QUICKBOOKS_SANDBOX = get_bool_env("QUICKBOOKS_SANDBOX", True)
  # Plaid
  PLAID_CLIENT_ID = get_secret_value("PLAID_CLIENT_ID", "")
  PLAID_CLIENT_SECRET = get_secret_value("PLAID_CLIENT_SECRET", "")
  PLAID_ENVIRONMENT = get_secret_value("PLAID_ENVIRONMENT", "sandbox")

  # SEC
  # SEC_GOV_USER_AGENT is a secret identity for API access
  SEC_GOV_USER_AGENT = get_secret_value(
    "SEC_GOV_USER_AGENT", "RoboSystems hello@robosystems.ai"
  )
  # SEC rate limiting and retry configuration (compliance requirement)
  SEC_RATE_LIMIT = get_int_env("SEC_RATE_LIMIT", SEC_RATE_LIMIT)
  SEC_PIPELINE_MAX_RETRIES = get_int_env(
    "SEC_PIPELINE_MAX_RETRIES", SEC_PIPELINE_MAX_RETRIES
  )
  # Parallel processing concurrency (for local sec-process-parallel command)
  SEC_PARALLEL_CONCURRENCY = get_int_env("SEC_PARALLEL_CONCURRENCY", 2)
  # Note: SEC processing constants (SEC_VALIDATE_CIK, SEC_PIPELINE_PARTIAL_TOLERANCE,
  # SEC_PIPELINE_CLEANUP_TEMP_FILES, SEC_MAX_CONCURRENT_DOWNLOADS) are in
  # robosystems/adapters/sec/config.py - they are not runtime-configurable

  # OpenFIGI (financial identifiers)
  OPENFIGI_API_KEY = get_secret_value("OPENFIGI_API_KEY", "")
  OPENFIGI_RETRY_MIN_WAIT = get_int_env(
    "OPENFIGI_RETRY_MIN_WAIT", OPENFIGI_RETRY_MIN_WAIT
  )
  OPENFIGI_RETRY_MAX_WAIT = get_int_env(
    "OPENFIGI_RETRY_MAX_WAIT", OPENFIGI_RETRY_MAX_WAIT
  )

  # Stripe (payment processing)
  STRIPE_SECRET_KEY = get_secret_value("STRIPE_SECRET_KEY", "")
  STRIPE_PUBLISHABLE_KEY = get_secret_value("STRIPE_PUBLISHABLE_KEY", "")
  STRIPE_WEBHOOK_SECRET = get_secret_value("STRIPE_WEBHOOK_SECRET", "")
  # STRIPE_API_VERSION is a constant, not a secret - imported from constants.py
  STRIPE_API_VERSION = STRIPE_API_VERSION  # Re-export for backward compatibility

  # ==========================================================================
  # 9. PERFORMANCE AND SCALING
  # ==========================================================================

  # Query queue configuration
  QUERY_QUEUE_MAX_SIZE = get_int_env("QUERY_QUEUE_MAX_SIZE", DEFAULT_QUEUE_SIZE)
  QUERY_QUEUE_MAX_CONCURRENT = get_int_env(
    "QUERY_QUEUE_MAX_CONCURRENT", DEFAULT_MAX_CONCURRENT
  )
  QUERY_QUEUE_MAX_PER_USER = get_int_env(
    "QUERY_QUEUE_MAX_PER_USER", QUERY_QUEUE_MAX_PER_USER
  )
  QUERY_QUEUE_TIMEOUT = get_int_env("QUERY_QUEUE_TIMEOUT", QUERY_QUEUE_TIMEOUT)
  QUERY_DEFAULT_PRIORITY = get_int_env("QUERY_DEFAULT_PRIORITY", QUERY_DEFAULT_PRIORITY)
  QUERY_PRIORITY_BOOST_PREMIUM = get_int_env(
    "QUERY_PRIORITY_BOOST_PREMIUM", QUERY_PRIORITY_BOOST_PREMIUM
  )

  # Admission control
  ADMISSION_MEMORY_THRESHOLD = get_float_env(
    "ADMISSION_MEMORY_THRESHOLD", ADMISSION_MEMORY_THRESHOLD_DEFAULT
  )
  ADMISSION_CPU_THRESHOLD = get_float_env(
    "ADMISSION_CPU_THRESHOLD", ADMISSION_CPU_THRESHOLD_DEFAULT
  )
  ADMISSION_QUEUE_THRESHOLD = get_float_env(
    "ADMISSION_QUEUE_THRESHOLD", ADMISSION_QUEUE_THRESHOLD_DEFAULT
  )
  ADMISSION_CHECK_INTERVAL = get_float_env(
    "ADMISSION_CHECK_INTERVAL", ADMISSION_CHECK_INTERVAL
  )

  # Load shedding
  LOAD_SHED_START_PRESSURE = get_float_env(
    "LOAD_SHED_START_PRESSURE", LOAD_SHED_START_PRESSURE_DEFAULT
  )
  LOAD_SHED_STOP_PRESSURE = get_float_env(
    "LOAD_SHED_STOP_PRESSURE", LOAD_SHED_STOP_PRESSURE_DEFAULT
  )

  # SSE (Server-Sent Events)
  # Note: MAX_SSE_CONNECTIONS_PER_USER and SSE_QUEUE_SIZE are now tunable via SSM
  # These env vars remain for backward compatibility but TuningConfig is preferred
  MAX_SSE_CONNECTIONS_PER_USER = get_int_env("MAX_SSE_CONNECTIONS_PER_USER", 5)
  SSE_QUEUE_SIZE = get_int_env("SSE_QUEUE_SIZE", 100)
  # SSE Rate limiting
  RATE_LIMIT_SSE_CONNECTIONS = get_int_env("RATE_LIMIT_SSE_CONNECTIONS", 10)
  RATE_LIMIT_SSE_CONNECTIONS_WINDOW = get_int_env(
    "RATE_LIMIT_SSE_CONNECTIONS_WINDOW", 60
  )

  # MCP (Model Context Protocol)
  MCP_AUTO_LIMIT_ENABLED = get_bool_env("MCP_AUTO_LIMIT_ENABLED", True)
  MCP_MAX_RESULT_ROWS = get_int_env("MCP_MAX_RESULT_ROWS", DEFAULT_QUERY_LIMIT)
  MCP_MAX_RESULT_SIZE_MB = get_float_env("MCP_MAX_RESULT_SIZE_MB", 5.0)

  # Credit allocation schedule
  CREDIT_ALLOCATION_DAY = get_int_env("CREDIT_ALLOCATION_DAY", CREDIT_ALLOCATION_DAY)
  CREDIT_ALLOCATION_HOUR = get_int_env("CREDIT_ALLOCATION_HOUR", CREDIT_ALLOCATION_HOUR)

  # ==========================================================================
  # 10. ARELLE RUNTIME CONFIGURATION
  # ==========================================================================
  # Note: Arelle and XBRL processing constants are in robosystems/adapters/sec/config.py
  # They are not runtime-configurable - change them in that module if needed.
  # Only ARELLE_CACHE_DIR remains here as it's a runtime path that varies by deployment.

  # Arelle cache directory (runtime path, varies by deployment)
  ARELLE_CACHE_DIR = get_str_env("ARELLE_CACHE_DIR", "")

  # XBRL graph large nodes (imported from constants.py - not configurable via env)
  # These tables contain millions of rows and consume significant memory
  XBRL_GRAPH_LARGE_NODES = XBRL_GRAPH_LARGE_NODES

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

    if cls.DATABASE_POOL_SIZE < 1:
      errors.append("DATABASE_POOL_SIZE must be at least 1")

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
            # Memory settings
            "max_memory_mb": instance_config.get(
              "max_memory_mb", cls.LBUG_MAX_MEMORY_MB
            ),
            "memory_per_db_mb": instance_config.get(
              "memory_per_db_mb", cls.LBUG_MAX_MEMORY_PER_DB_MB
            ),
            # Performance settings
            "chunk_size": instance_config.get("chunk_size", 1000),
            "query_timeout": instance_config.get(
              "query_timeout", cls.GRAPH_QUERY_TIMEOUT
            ),
            "max_query_length": instance_config.get(
              "max_query_length", cls.GRAPH_MAX_QUERY_LENGTH
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
              else instance_config.get(
                "databases_per_instance", cls.LBUG_MAX_DATABASES_PER_NODE
              )
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

    # Fall back to environment variables
    return {
      # Memory settings
      "max_memory_mb": cls.LBUG_MAX_MEMORY_MB,
      "memory_per_db_mb": cls.LBUG_MAX_MEMORY_PER_DB_MB,
      # Performance settings
      "chunk_size": get_int_env("LBUG_CHUNK_SIZE", 1000),
      "query_timeout": cls.GRAPH_QUERY_TIMEOUT,
      "max_query_length": cls.GRAPH_MAX_QUERY_LENGTH,
      "connection_pool_size": get_int_env("LBUG_CONNECTION_POOL_SIZE", 10),
      # Database settings
      "databases_per_instance": get_int_env("LBUG_DATABASES_PER_INSTANCE", 10),
      "max_databases": cls.LBUG_MAX_DATABASES_PER_NODE,
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
        >>> env.get_valkey_url(ValkeyDatabase.AUTH_CACHE)
        'redis://localhost:6379/2'
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
