"""
Centralized environment variable configuration.

This module provides a single source of truth for all environment variables,
with type conversions, validation, and default values.

Organization:
- Helper functions for type-safe env var access
- Core application settings
- Database configuration (PostgreSQL, Kuzu)
- External service integrations
- Security and authentication
- Performance and scaling
- Feature flags and toggles
"""

import os
from typing import Any, Dict, Optional, List, Union, TYPE_CHECKING
from functools import lru_cache

if TYPE_CHECKING:
  from .valkey_registry import ValkeyDatabase

# Import secrets manager with graceful fallback
# This handles cases where boto3 isn't installed or circular imports occur
try:
  from .secrets_manager import get_secret_value

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


from .constants import (
  # Database constants
  DEFAULT_POOL_SIZE,
  DEFAULT_MAX_OVERFLOW,
  DEFAULT_POOL_TIMEOUT,
  DEFAULT_POOL_RECYCLE,
  # Performance constants
  DEFAULT_HTTP_TIMEOUT,
  DEFAULT_QUERY_TIMEOUT,
  MAX_QUERY_LENGTH,
  DEFAULT_RETRY_DELAY,
  DEFAULT_QUERY_LIMIT,
  DEFAULT_QUEUE_SIZE,
  DEFAULT_MAX_CONCURRENT,
  MAX_CONCURRENT_DOWNLOADS,
  MAX_DATABASES_PER_NODE,
  # Cache constants
  CACHE_TTL_SHORT,
  CACHE_TTL_LONG,
  # Task constants
  TASK_TIME_LIMIT,
  TASK_SOFT_TIME_LIMIT,
  # Admission control
  ADMISSION_MEMORY_THRESHOLD_DEFAULT,
  ADMISSION_CPU_THRESHOLD_DEFAULT,
  ADMISSION_QUEUE_THRESHOLD_DEFAULT,
  ADMISSION_CHECK_INTERVAL,
  # Fixed technical limits
  KUZU_MAX_REQUEST_SIZE,
  KUZU_CONNECT_TIMEOUT,
  KUZU_READ_TIMEOUT,
  ARELLE_MIN_SCHEMA_COUNT,
  ARELLE_DOWNLOAD_TIMEOUT,
  XBRL_EXTERNALIZATION_THRESHOLD,
  # Resiliency and circuit breaker
  KUZU_ALB_HEALTH_CACHE_TTL,
  KUZU_INSTANCE_CACHE_TTL,
  KUZU_CIRCUIT_BREAKER_THRESHOLD,
  KUZU_CIRCUIT_BREAKER_TIMEOUT,
  # Queue configuration
  QUERY_QUEUE_MAX_PER_USER,
  QUERY_DEFAULT_PRIORITY,
  QUERY_PRIORITY_BOOST_PREMIUM,
  QUERY_QUEUE_TIMEOUT,
  # Load shedding
  LOAD_SHED_START_PRESSURE_DEFAULT,
  LOAD_SHED_STOP_PRESSURE_DEFAULT,
  # Retry configuration
  SEC_PIPELINE_MAX_RETRIES,
  OPENFIGI_RETRY_MIN_WAIT,
  OPENFIGI_RETRY_MAX_WAIT,
  # Fixed business rules
  CREDIT_ALLOCATION_DAY,
  CREDIT_ALLOCATION_HOUR,
  SEC_RATE_LIMIT,
  # Tier-specific memory allocations
  KUZU_STANDARD_MAX_MEMORY_MB,
  KUZU_ENTERPRISE_MAX_MEMORY_MB,
  KUZU_PREMIUM_MAX_MEMORY_MB,
  KUZU_STANDARD_MEMORY_PER_DB_MB,
  KUZU_ENTERPRISE_MEMORY_PER_DB_MB,
  KUZU_PREMIUM_MEMORY_PER_DB_MB,
  # Tier-specific chunk sizes
  KUZU_STANDARD_CHUNK_SIZE,
  KUZU_ENTERPRISE_CHUNK_SIZE,
  KUZU_PREMIUM_CHUNK_SIZE,
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


def get_list_env(key: str, default: str = "", separator: str = ",") -> List[str]:
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

  Variables are organized into logical groups for easier maintenance.
  All variables use type-safe helper functions for consistent behavior.
  """

  # ==========================================================================
  # CORE APPLICATION SETTINGS
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

  # Service URLs
  ROBOSYSTEMS_API_URL = get_str_env("ROBOSYSTEMS_API_URL", "https://api.robosystems.ai")
  ROBOLEDGER_URL = get_str_env("ROBOLEDGER_URL", "https://roboledger.ai")
  ROBOINVESTOR_URL = get_str_env("ROBOINVESTOR_URL", "https://roboinvestor.ai")
  ROBOSYSTEMS_URL = get_str_env("ROBOSYSTEMS_URL", "https://robosystems.ai")

  # ==========================================================================
  # FEATURE FLAGS
  # ==========================================================================

  # Core Feature Flags
  USER_REGISTRATION_ENABLED = get_bool_env(
    "USER_REGISTRATION_ENABLED",
    bool(get_secret_value("USER_REGISTRATION_ENABLED", "false").lower() == "true"),
  )
  RATE_LIMIT_ENABLED = get_bool_env(
    "RATE_LIMIT_ENABLED",
    bool(get_secret_value("RATE_LIMIT_ENABLED", "true").lower() == "true"),
  )
  SSE_ENABLED = get_bool_env(
    "SSE_ENABLED",
    bool(get_secret_value("SSE_ENABLED", "true").lower() == "true"),
  )
  OTEL_ENABLED = get_bool_env(
    "OTEL_ENABLED",
    bool(get_secret_value("OTEL_ENABLED", "false").lower() == "true"),
  )
  LOAD_SHEDDING_ENABLED = get_bool_env(
    "LOAD_SHEDDING_ENABLED",
    bool(get_secret_value("LOAD_SHEDDING_ENABLED", "true").lower() == "true"),
  )
  CSP_TRUSTED_TYPES_ENABLED = get_bool_env(
    "CSP_TRUSTED_TYPES_ENABLED",
    bool(get_secret_value("CSP_TRUSTED_TYPES_ENABLED", "true").lower() == "true"),
  )
  MCP_AUTO_LIMIT_ENABLED = get_bool_env("MCP_AUTO_LIMIT_ENABLED", True)

  # Connection Feature Flags
  CONNECTION_SEC_ENABLED = get_bool_env(
    "CONNECTION_SEC_ENABLED",
    bool(get_secret_value("CONNECTION_SEC_ENABLED", "false").lower() == "true"),
  )
  CONNECTION_QUICKBOOKS_ENABLED = get_bool_env(
    "CONNECTION_QUICKBOOKS_ENABLED",
    bool(get_secret_value("CONNECTION_QUICKBOOKS_ENABLED", "false").lower() == "true"),
  )
  CONNECTION_PLAID_ENABLED = get_bool_env(
    "CONNECTION_PLAID_ENABLED",
    bool(get_secret_value("CONNECTION_PLAID_ENABLED", "false").lower() == "true"),
  )

  # Billing Feature Flags
  # For forked/self-hosted deployments: Set BILLING_ENABLED=false in AWS Secrets Manager
  # This disables payment requirements since you're paying for your own infrastructure
  BILLING_ENABLED = get_bool_env(
    "BILLING_ENABLED",
    bool(get_secret_value("BILLING_ENABLED", "true").lower() == "true"),
  )
  BILLING_PREMIUM_PLANS_ENABLED = get_bool_env(
    "BILLING_PREMIUM_PLANS_ENABLED",
    bool(get_secret_value("BILLING_PREMIUM_PLANS_ENABLED", "false").lower() == "true"),
  )

  # Security Feature Flags
  SECURITY_AUDIT_ENABLED = get_bool_env(
    "SECURITY_AUDIT_ENABLED",
    bool(get_secret_value("SECURITY_AUDIT_ENABLED", "true").lower() == "true"),
  )
  CORS_ALLOW_CREDENTIALS = get_bool_env("CORS_ALLOW_CREDENTIALS", True)

  # Graph Infrastructure Feature Flags (applies to all backends)
  GRAPH_CIRCUIT_BREAKERS_ENABLED = get_bool_env("GRAPH_CIRCUIT_BREAKERS_ENABLED", True)
  GRAPH_REDIS_CACHE_ENABLED = get_bool_env("GRAPH_REDIS_CACHE_ENABLED", True)
  GRAPH_RETRY_LOGIC_ENABLED = get_bool_env("GRAPH_RETRY_LOGIC_ENABLED", True)
  GRAPH_HEALTH_CHECKS_ENABLED = get_bool_env("GRAPH_HEALTH_CHECKS_ENABLED", True)
  SHARED_REPLICA_ALB_ENABLED = get_bool_env("SHARED_REPLICA_ALB_ENABLED", False)
  ALLOW_SHARED_MASTER_READS = get_bool_env("ALLOW_SHARED_MASTER_READS", True)

  # Graph backup encryption and compression are always enabled for security and efficiency

  # Graph Operations Feature Flags
  SUBGRAPH_CREATION_ENABLED = get_bool_env(
    "SUBGRAPH_CREATION_ENABLED",
    bool(get_secret_value("SUBGRAPH_CREATION_ENABLED", "true").lower() == "true"),
  )
  BACKUP_CREATION_ENABLED = get_bool_env(
    "BACKUP_CREATION_ENABLED",
    bool(get_secret_value("BACKUP_CREATION_ENABLED", "true").lower() == "true"),
  )
  AGENT_POST_ENABLED = get_bool_env(
    "AGENT_POST_ENABLED",
    bool(get_secret_value("AGENT_POST_ENABLED", "true").lower() == "true"),
  )

  # Registration and Verification Feature Flags
  EMAIL_VERIFICATION_ENABLED = get_bool_env(
    "EMAIL_VERIFICATION_ENABLED",
    bool(get_secret_value("EMAIL_VERIFICATION_ENABLED", "true").lower() == "true")
    if get_str_env("ENVIRONMENT", "dev") in ["prod", "staging"]
    else False,
  )

  # Email service configuration
  EMAIL_FROM_ADDRESS = get_str_env(
    "EMAIL_FROM_ADDRESS",
    get_secret_value("EMAIL_FROM_ADDRESS", "noreply@robosystems.ai"),
  )
  EMAIL_FROM_NAME = get_str_env(
    "EMAIL_FROM_NAME",
    get_secret_value("EMAIL_FROM_NAME", "RoboSystems"),
  )

  # Token expiry configuration
  EMAIL_TOKEN_EXPIRY_HOURS = get_int_env("EMAIL_TOKEN_EXPIRY_HOURS", 24)
  PASSWORD_RESET_TOKEN_EXPIRY_HOURS = get_int_env(
    "PASSWORD_RESET_TOKEN_EXPIRY_HOURS", 1
  )
  CAPTCHA_ENABLED = get_bool_env(
    "CAPTCHA_ENABLED",
    bool(get_secret_value("CAPTCHA_ENABLED", "true").lower() == "true")
    if get_str_env("ENVIRONMENT", "dev") in ["prod", "staging"]
    else False,
  )

  # ==========================================================================
  # DATABASE CONFIGURATION - POSTGRESQL
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
  # DATABASE CONFIGURATION - GRAPH DATABASES (MULTI-BACKEND: KUZU AND NEO4J)
  # ==========================================================================

  # Graph Backend Selection
  GRAPH_BACKEND_TYPE = get_str_env(
    "GRAPH_BACKEND_TYPE", "kuzu"
  )  # Options: kuzu, neo4j_community, neo4j_enterprise

  # Graph API Configuration (applies to all backends - unified access layer)
  GRAPH_API_URL = get_str_env("GRAPH_API_URL", "http://localhost:8001")
  GRAPH_API_KEY = get_secret_value("GRAPH_API_KEY", "")

  # Shared repository backend selection (dev/local only)
  # In AWS environments, backend is determined by graph.yml tier configuration
  # Values: "kuzu" or "neo4j"
  GRAPH_SHARED_REPOSITORY_BACKEND = get_str_env("GRAPH_SHARED_REPOSITORY_BACKEND", "")

  # Graph API Timeouts and Limits (applies to all backends)
  GRAPH_HTTP_TIMEOUT = get_int_env("GRAPH_HTTP_TIMEOUT", DEFAULT_HTTP_TIMEOUT)
  GRAPH_QUERY_TIMEOUT = get_int_env("GRAPH_QUERY_TIMEOUT", DEFAULT_QUERY_TIMEOUT)
  GRAPH_MAX_QUERY_LENGTH = get_int_env("GRAPH_MAX_QUERY_LENGTH", MAX_QUERY_LENGTH)
  GRAPH_MAX_REQUEST_SIZE = get_int_env("GRAPH_MAX_REQUEST_SIZE", KUZU_MAX_REQUEST_SIZE)
  GRAPH_CONNECT_TIMEOUT = get_float_env("GRAPH_CONNECT_TIMEOUT", KUZU_CONNECT_TIMEOUT)
  GRAPH_READ_TIMEOUT = get_float_env("GRAPH_READ_TIMEOUT", KUZU_READ_TIMEOUT)

  # Graph Routing and Load Balancing (applies to all backends)
  GRAPH_REPLICA_ALB_URL = get_str_env("GRAPH_REPLICA_ALB_URL", "")

  # Graph Resiliency and Circuit Breaker Configuration (applies to all backends)
  GRAPH_ALB_HEALTH_CACHE_TTL = get_int_env(
    "GRAPH_ALB_HEALTH_CACHE_TTL", KUZU_ALB_HEALTH_CACHE_TTL
  )
  GRAPH_INSTANCE_CACHE_TTL = get_int_env(
    "GRAPH_INSTANCE_CACHE_TTL", KUZU_INSTANCE_CACHE_TTL
  )
  GRAPH_CIRCUIT_BREAKER_THRESHOLD = get_int_env(
    "GRAPH_CIRCUIT_BREAKER_THRESHOLD", KUZU_CIRCUIT_BREAKER_THRESHOLD
  )
  GRAPH_CIRCUIT_BREAKER_TIMEOUT = get_int_env(
    "GRAPH_CIRCUIT_BREAKER_TIMEOUT", KUZU_CIRCUIT_BREAKER_TIMEOUT
  )
  GRAPH_HEALTH_CHECK_INTERVAL_MINUTES = get_float_env(
    "GRAPH_HEALTH_CHECK_INTERVAL_MINUTES", 5.0
  )

  # Graph Backup Configuration (applies to all backends)
  GRAPH_BACKUP_ENCRYPTION_KEY = get_secret_value("GRAPH_BACKUP_ENCRYPTION_KEY", "")
  GRAPH_BACKUP_ENCRYPTION_PASSWORD = get_secret_value(
    "GRAPH_BACKUP_ENCRYPTION_PASSWORD", ""
  )

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

  # Kuzu-Specific Configuration (when GRAPH_BACKEND_TYPE=kuzu)
  KUZU_DATABASE_PATH = get_str_env("KUZU_DATABASE_PATH", "./data/kuzu-dbs")
  KUZU_ACCESS_PATTERN = get_str_env("KUZU_ACCESS_PATTERN", "api_auto")
  KUZU_NODE_TYPE = get_str_env("KUZU_NODE_TYPE", "writer")
  KUZU_S3_BUCKET = get_str_env("KUZU_S3_BUCKET", "")
  KUZU_HOME = get_str_env("KUZU_HOME", "/app/data/.kuzu")

  # Kuzu Capacity and Performance
  KUZU_MAX_DATABASES_PER_NODE = get_int_env(
    "KUZU_MAX_DATABASES_PER_NODE", MAX_DATABASES_PER_NODE
  )

  # Kuzu Memory Configuration (can be overridden per-tier)
  KUZU_MAX_MEMORY_MB = get_int_env("KUZU_MAX_MEMORY_MB", 2048)
  KUZU_MAX_MEMORY_PER_DB_MB = get_int_env("KUZU_MAX_MEMORY_PER_DB_MB", 0)

  # Tier-specific memory allocations (with environment variable overrides)
  KUZU_STANDARD_MAX_MEMORY_MB = get_int_env(
    "KUZU_STANDARD_MAX_MEMORY_MB", KUZU_STANDARD_MAX_MEMORY_MB
  )
  KUZU_ENTERPRISE_MAX_MEMORY_MB = get_int_env(
    "KUZU_ENTERPRISE_MAX_MEMORY_MB", KUZU_ENTERPRISE_MAX_MEMORY_MB
  )
  KUZU_PREMIUM_MAX_MEMORY_MB = get_int_env(
    "KUZU_PREMIUM_MAX_MEMORY_MB", KUZU_PREMIUM_MAX_MEMORY_MB
  )
  KUZU_STANDARD_MEMORY_PER_DB_MB = get_int_env(
    "KUZU_STANDARD_MEMORY_PER_DB_MB", KUZU_STANDARD_MEMORY_PER_DB_MB
  )
  KUZU_ENTERPRISE_MEMORY_PER_DB_MB = get_int_env(
    "KUZU_ENTERPRISE_MEMORY_PER_DB_MB", KUZU_ENTERPRISE_MEMORY_PER_DB_MB
  )
  KUZU_PREMIUM_MEMORY_PER_DB_MB = get_int_env(
    "KUZU_PREMIUM_MEMORY_PER_DB_MB", KUZU_PREMIUM_MEMORY_PER_DB_MB
  )

  # Tier-specific chunk sizes (with environment variable overrides)
  KUZU_STANDARD_CHUNK_SIZE = get_int_env(
    "KUZU_STANDARD_CHUNK_SIZE", KUZU_STANDARD_CHUNK_SIZE
  )
  KUZU_ENTERPRISE_CHUNK_SIZE = get_int_env(
    "KUZU_ENTERPRISE_CHUNK_SIZE", KUZU_ENTERPRISE_CHUNK_SIZE
  )
  KUZU_PREMIUM_CHUNK_SIZE = get_int_env(
    "KUZU_PREMIUM_CHUNK_SIZE", KUZU_PREMIUM_CHUNK_SIZE
  )

  # Kuzu-specific admission control
  KUZU_ADMISSION_MEMORY_THRESHOLD = get_float_env(
    "KUZU_ADMISSION_MEMORY_THRESHOLD", ADMISSION_MEMORY_THRESHOLD_DEFAULT
  )
  KUZU_ADMISSION_CPU_THRESHOLD = get_float_env(
    "KUZU_ADMISSION_CPU_THRESHOLD", ADMISSION_CPU_THRESHOLD_DEFAULT
  )
  KUZU_MAX_CONNECTIONS_PER_DB = get_int_env("KUZU_MAX_CONNECTIONS_PER_DB", 10)
  KUZU_CONNECTION_TTL_MINUTES = get_float_env("KUZU_CONNECTION_TTL_MINUTES", 30.0)

  # Neo4j-Specific Configuration (when GRAPH_BACKEND_TYPE=neo4j_*)
  NEO4J_URI = get_str_env("NEO4J_URI", "bolt://localhost:7687")
  NEO4J_USERNAME = get_str_env("NEO4J_USERNAME", "neo4j")
  NEO4J_PASSWORD = get_secret_value("NEO4J_PASSWORD", "")
  NEO4J_ENTERPRISE = get_bool_env("NEO4J_ENTERPRISE", False)
  NEO4J_MAX_CONNECTION_POOL_SIZE = get_int_env("NEO4J_MAX_CONNECTION_POOL_SIZE", 50)
  NEO4J_CONNECTION_ACQUISITION_TIMEOUT = get_int_env(
    "NEO4J_CONNECTION_ACQUISITION_TIMEOUT", 60
  )
  NEO4J_MAX_CONNECTION_LIFETIME = get_int_env("NEO4J_MAX_CONNECTION_LIFETIME", 3600)

  # User Graph Creation Limits (safety valve)
  USER_GRAPHS_DEFAULT_LIMIT = get_int_env(
    "USER_GRAPHS_DEFAULT_LIMIT",
    int(get_secret_value("USER_GRAPHS_DEFAULT_LIMIT", "100")),
  )

  # Instance Metadata (applies to all backends)
  EC2_INSTANCE_ID = get_str_env("INSTANCE_ID", "")
  INSTANCE_ID = get_str_env("INSTANCE_ID", "")
  CLUSTER_TIER = get_str_env("CLUSTER_TIER", "")
  # ==========================================================================
  # CACHE AND QUEUE CONFIGURATION (VALKEY/REDIS)
  # ==========================================================================

  # Valkey/Redis URLs
  # Base URL without database number (database numbers are managed in valkey_registry.py)
  VALKEY_URL = get_str_env("VALKEY_URL", "redis://localhost:6379")

  # Valkey authentication (for encrypted/production environments)
  # This is fetched from AWS Secrets Manager in prod/staging environments
  VALKEY_AUTH_TOKEN = get_str_env("VALKEY_AUTH_TOKEN", "")

  # Name of the Secrets Manager secret containing Valkey auth token
  VALKEY_AUTH_SECRET_NAME = get_str_env(
    "VALKEY_AUTH_SECRET_NAME", f"robosystems/{ENVIRONMENT}/valkey/auth"
  )

  # Celery URLs with explicit database numbers (see valkey_registry.py for allocation)
  # These will be dynamically constructed with auth in prod/staging via get_celery_config()
  CELERY_BROKER_URL = get_str_env("CELERY_BROKER_URL", "redis://localhost:6379/0")
  CELERY_RESULT_BACKEND = get_str_env(
    "CELERY_RESULT_BACKEND", "redis://localhost:6379/1"
  )

  # Celery task configuration
  CELERY_TASK_TIME_LIMIT = get_int_env("CELERY_TASK_TIME_LIMIT", TASK_TIME_LIMIT)
  CELERY_TASK_SOFT_TIME_LIMIT = get_int_env(
    "CELERY_TASK_SOFT_TIME_LIMIT", TASK_SOFT_TIME_LIMIT
  )
  CELERY_WORKER_PREFETCH_MULTIPLIER = get_int_env(
    "CELERY_WORKER_PREFETCH_MULTIPLIER",
    0,  # 0 disables prefetching for proper queue-based scaling
  )
  CELERY_TASK_RETRY_DELAY = get_int_env("CELERY_TASK_RETRY_DELAY", DEFAULT_RETRY_DELAY)
  CELERY_TASK_MAX_RETRIES = get_int_env("CELERY_TASK_MAX_RETRIES", 3)
  CELERY_RESULT_EXPIRES = get_int_env("CELERY_RESULT_EXPIRES", CACHE_TTL_LONG)
  # Soft shutdown timeout - time to wait during warm shutdown before forcing cold shutdown
  # This allows tasks to finish gracefully and re-queue ETA tasks
  CELERY_WORKER_SOFT_SHUTDOWN_TIMEOUT = get_int_env(
    "CELERY_WORKER_SOFT_SHUTDOWN_TIMEOUT",
    60,  # 60 seconds default
  )

  # Worker configuration
  WORKER_AUTOSCALE = get_int_env("WORKER_AUTOSCALE", 1)

  # Queue names
  QUEUE_DEFAULT = get_str_env("QUEUE_DEFAULT", "default")
  QUEUE_CRITICAL = get_str_env("QUEUE_CRITICAL", "critical")
  QUEUE_SHARED_EXTRACTION = get_str_env("QUEUE_SHARED_EXTRACTION", "shared-extraction")
  QUEUE_SHARED_PROCESSING = get_str_env("QUEUE_SHARED_PROCESSING", "shared-processing")
  QUEUE_SHARED_INGESTION = get_str_env("QUEUE_SHARED_INGESTION", "shared-ingestion")
  QUEUE_DATA_SYNC = get_str_env("QUEUE_DATA_SYNC", "default")  # Future: "data-sync"
  QUEUE_ANALYTICS = get_str_env("QUEUE_ANALYTICS", "default")  # Future: "analytics"

  # Worker configuration (used by entrypoint.sh and CloudFormation, not in application code)
  # Specifies which queue(s) a worker process listens to (e.g., "default", "critical", "shared-processing")
  WORKER_QUEUE = get_str_env("WORKER_QUEUE", QUEUE_DEFAULT)
  # Cache TTLs
  CREDIT_BALANCE_CACHE_TTL = get_int_env("CREDIT_BALANCE_CACHE_TTL", CACHE_TTL_SHORT)
  CREDIT_SUMMARY_CACHE_TTL = get_int_env("CREDIT_SUMMARY_CACHE_TTL", 600)  # 10 minutes
  CREDIT_OPERATION_COST_CACHE_TTL = get_int_env(
    "CREDIT_OPERATION_COST_CACHE_TTL", CACHE_TTL_LONG
  )
  JWT_CACHE_TTL = get_int_env("JWT_CACHE_TTL", 1800)  # 30 minutes
  API_KEY_CACHE_TTL = get_int_env("API_KEY_CACHE_TTL", 300)  # 5 minutes

  # ==========================================================================
  # AWS CONFIGURATION
  # ==========================================================================

  # AWS Region configuration (credentials come from IAM roles in ECS/EC2)
  AWS_DEFAULT_REGION = get_str_env("AWS_DEFAULT_REGION", "us-east-1")
  AWS_REGION = get_str_env("AWS_REGION", AWS_DEFAULT_REGION)
  AWS_ENDPOINT_URL = get_str_env("AWS_ENDPOINT_URL", "")  # For LocalStack

  # S3 configuration
  AWS_S3_PREFIX = get_str_env("AWS_S3_PREFIX", "robosystems")

  # S3-specific credentials and buckets
  # Use secrets manager for prod/staging, environment variables for local dev
  AWS_S3_ACCESS_KEY_ID = get_secret_value("AWS_S3_ACCESS_KEY_ID", "")
  AWS_S3_SECRET_ACCESS_KEY = get_secret_value("AWS_S3_SECRET_ACCESS_KEY", "")
  AWS_S3_BUCKET = get_secret_value("AWS_S3_BUCKET", f"robosystems-{ENVIRONMENT}")
  SEC_RAW_BUCKET = get_secret_value("SEC_RAW_BUCKET", "robosystems-sec-raw")
  SEC_PROCESSED_BUCKET = get_secret_value(
    "SEC_PROCESSED_BUCKET", "robosystems-sec-processed"
  )
  PUBLIC_DATA_BUCKET = get_secret_value("PUBLIC_DATA_BUCKET", "robosystems-public-data")
  PUBLIC_DATA_CDN_URL = get_secret_value("PUBLIC_DATA_CDN_URL", "")

  # ==========================================================================
  # SECURITY AND AUTHENTICATION
  # ==========================================================================

  # JWT configuration
  JWT_SECRET_KEY = get_secret_value("JWT_SECRET_KEY", "")
  JWT_EXPIRY_HOURS = get_float_env("JWT_EXPIRY_HOURS", 0.5)  # Default 30 minutes

  # JWT Issuer and Audience - configurable for different deployments
  # Note: https:// prefix is stripped at infrastructure layer (GitHub Actions workflows)
  JWT_ISSUER = get_str_env("JWT_ISSUER", "api.robosystems.ai")
  JWT_AUDIENCE = get_list_env(
    "JWT_AUDIENCE", "robosystems.ai,roboledger.ai,roboinvestor.ai"
  )

  # Authentication Security Settings (configurable per environment)
  TOKEN_GRACE_PERIOD_MINUTES = get_int_env("TOKEN_GRACE_PERIOD_MINUTES", 5)

  # Rate Limiting Configuration (overrides for defaults in constants.py)
  JWT_REFRESH_RATE_LIMIT = get_int_env("JWT_REFRESH_RATE_LIMIT", 20)
  AUTH_RATE_LIMIT_LOGIN = get_int_env("AUTH_RATE_LIMIT_LOGIN", 5)
  AUTH_RATE_LIMIT_REGISTER = get_int_env("AUTH_RATE_LIMIT_REGISTER", 3)

  # API key configuration
  CONNECTION_CREDENTIALS_KEY = get_secret_value("CONNECTION_CREDENTIALS_KEY", "")

  # Cloudflare Turnstile (CAPTCHA)
  TURNSTILE_SECRET_KEY = get_secret_value("TURNSTILE_SECRET_KEY", "")
  TURNSTILE_SITE_KEY = get_secret_value("TURNSTILE_SITE_KEY", "")

  # ==========================================================================
  # EXTERNAL SERVICE INTEGRATIONS
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
  SEC_GOV_USER_AGENT = get_secret_value(
    "SEC_GOV_USER_AGENT", "RoboSystems hello@robosystems.ai"
  )
  SEC_MAX_CONCURRENT_DOWNLOADS = get_int_env(
    "SEC_MAX_CONCURRENT_DOWNLOADS", MAX_CONCURRENT_DOWNLOADS
  )
  SEC_VALIDATE_CIK = get_bool_env("SEC_VALIDATE_CIK", True)
  SEC_PIPELINE_PARTIAL_TOLERANCE = get_bool_env("SEC_PIPELINE_PARTIAL_TOLERANCE", True)
  SEC_PIPELINE_CLEANUP_TEMP_FILES = get_bool_env(
    "SEC_PIPELINE_CLEANUP_TEMP_FILES", True
  )
  # SEC rate limiting and retry configuration
  SEC_RATE_LIMIT = get_int_env("SEC_RATE_LIMIT", SEC_RATE_LIMIT)
  SEC_PIPELINE_MAX_RETRIES = get_int_env(
    "SEC_PIPELINE_MAX_RETRIES", SEC_PIPELINE_MAX_RETRIES
  )

  # Anthropic (Claude AI)
  ANTHROPIC_API_KEY = get_secret_value("ANTHROPIC_API_KEY", "")
  ANTHROPIC_MODEL = get_str_env("ANTHROPIC_MODEL", "claude-sonnet-4-20250514")

  # OpenFIGI (financial identifiers)
  OPENFIGI_API_KEY = get_secret_value("OPENFIGI_API_KEY", "")
  OPENFIGI_RETRY_MIN_WAIT = get_int_env(
    "OPENFIGI_RETRY_MIN_WAIT", OPENFIGI_RETRY_MIN_WAIT
  )
  OPENFIGI_RETRY_MAX_WAIT = get_int_env(
    "OPENFIGI_RETRY_MAX_WAIT", OPENFIGI_RETRY_MAX_WAIT
  )

  # ==========================================================================
  # DATA PROCESSING CONFIGURATION
  # ==========================================================================

  # Arelle (XBRL processing)
  ARELLE_LOG_FILE = get_str_env("ARELLE_LOG_FILE", "logToBuffer")
  ARELLE_TIMEOUT = get_int_env("ARELLE_TIMEOUT", 30)
  ARELLE_WORK_OFFLINE = get_str_env("ARELLE_WORK_OFFLINE", "false")
  ARELLE_CACHE_DIR = get_str_env("ARELLE_CACHE_DIR", "")
  # Arelle technical limits
  ARELLE_MIN_SCHEMA_COUNT = get_int_env(
    "ARELLE_MIN_SCHEMA_COUNT", ARELLE_MIN_SCHEMA_COUNT
  )
  ARELLE_DOWNLOAD_TIMEOUT = get_int_env(
    "ARELLE_DOWNLOAD_TIMEOUT", ARELLE_DOWNLOAD_TIMEOUT
  )

  # XBRL processing
  XBRL_EXTERNALIZE_LARGE_VALUES = get_bool_env("XBRL_EXTERNALIZE_LARGE_VALUES", True)
  XBRL_STANDARDIZED_FILENAMES = get_bool_env("XBRL_STANDARDIZED_FILENAMES", False)
  XBRL_TYPE_PREFIXES = get_bool_env("XBRL_TYPE_PREFIXES", False)
  XBRL_COLUMN_STANDARDIZATION = get_bool_env("XBRL_COLUMN_STANDARDIZATION", False)
  # XBRL technical limits
  XBRL_EXTERNALIZATION_THRESHOLD = get_int_env(
    "XBRL_EXTERNALIZATION_THRESHOLD", XBRL_EXTERNALIZATION_THRESHOLD
  )

  # XBRL graph large nodes that require aggressive memory cleanup after Kuzu ingestion
  # These tables contain millions of rows and consume significant memory
  XBRL_GRAPH_LARGE_NODES = get_str_env(
    "XBRL_GRAPH_LARGE_NODES",
    "Fact,Element,Label,Association,Structure,FactDimension,Report",
  )

  # MCP (Model Context Protocol)
  MCP_MAX_RESULT_ROWS = get_int_env("MCP_MAX_RESULT_ROWS", DEFAULT_QUERY_LIMIT)
  MCP_MAX_RESULT_SIZE_MB = get_float_env("MCP_MAX_RESULT_SIZE_MB", 5.0)

  # Agent memory configuration
  AGENT_MEMORY_BACKEND = get_str_env(
    "AGENT_MEMORY_BACKEND", "memory"
  )  # Options: memory, subgraph, hybrid

  # ==========================================================================
  # BILLING AND SUBSCRIPTIONS
  # ==========================================================================

  # Credit allocation schedule
  CREDIT_ALLOCATION_DAY = get_int_env("CREDIT_ALLOCATION_DAY", CREDIT_ALLOCATION_DAY)
  CREDIT_ALLOCATION_HOUR = get_int_env("CREDIT_ALLOCATION_HOUR", CREDIT_ALLOCATION_HOUR)

  # ==========================================================================
  # PERFORMANCE AND SCALING
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

  # Kuzu-specific admission control (can override general settings)
  KUZU_ADMISSION_MEMORY_THRESHOLD = get_float_env(
    "KUZU_ADMISSION_MEMORY_THRESHOLD", ADMISSION_MEMORY_THRESHOLD
  )
  KUZU_ADMISSION_CPU_THRESHOLD = get_float_env(
    "KUZU_ADMISSION_CPU_THRESHOLD", ADMISSION_CPU_THRESHOLD
  )
  KUZU_MAX_CONNECTIONS_PER_DB = get_int_env("KUZU_MAX_CONNECTIONS_PER_DB", 10)
  KUZU_CONNECTION_TTL_MINUTES = get_float_env(
    "KUZU_CONNECTION_TTL_MINUTES", 30.0
  )  # 30 minutes default
  KUZU_HEALTH_CHECK_INTERVAL_MINUTES = get_float_env(
    "KUZU_HEALTH_CHECK_INTERVAL_MINUTES", 5.0
  )  # 5 minutes default

  # Load shedding
  LOAD_SHED_START_PRESSURE = get_float_env(
    "LOAD_SHED_START_PRESSURE", LOAD_SHED_START_PRESSURE_DEFAULT
  )
  LOAD_SHED_STOP_PRESSURE = get_float_env(
    "LOAD_SHED_STOP_PRESSURE", LOAD_SHED_STOP_PRESSURE_DEFAULT
  )

  # SSE (Server-Sent Events)
  MAX_SSE_CONNECTIONS_PER_USER = get_int_env("MAX_SSE_CONNECTIONS_PER_USER", 5)
  SSE_QUEUE_SIZE = get_int_env("SSE_QUEUE_SIZE", 100)
  SSE_MAX_REDIS_FAILURES = get_int_env("SSE_MAX_REDIS_FAILURES", 3)
  # SSE Rate limiting
  RATE_LIMIT_SSE_CONNECTIONS = get_int_env("RATE_LIMIT_SSE_CONNECTIONS", 10)
  RATE_LIMIT_SSE_CONNECTIONS_WINDOW = get_int_env(
    "RATE_LIMIT_SSE_CONNECTIONS_WINDOW", 60
  )

  # ==========================================================================
  # OBSERVABILITY
  # ==========================================================================

  # OpenTelemetry configuration
  OTEL_SERVICE_NAME = get_str_env("OTEL_SERVICE_NAME", "robosystems-service")
  OTEL_EXPORTER_OTLP_ENDPOINT = get_str_env(
    "OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317"
  )
  OTEL_RESOURCE_ATTRIBUTES = get_str_env("OTEL_RESOURCE_ATTRIBUTES", "")
  OTEL_CONSOLE_EXPORT = get_bool_env("OTEL_CONSOLE_EXPORT", False)

  # ==========================================================================
  # SHARED REPOSITORIES CONFIGURATION
  # ==========================================================================

  # Shared repositories list for infrastructure/deployment (used by userdata scripts)
  # This configures which repositories should be deployed on shared writer instances
  # For application logic (checking if a graph is a shared repo), use GraphTypeRegistry.SHARED_REPOSITORIES
  SHARED_REPOSITORIES = get_list_env("SHARED_REPOSITORIES", "")

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
  def validate(cls) -> List[str]:
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
        ("CELERY_BROKER_URL", cls.CELERY_BROKER_URL, None),
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
  def get_kuzu_tier_config(cls) -> Dict[str, Any]:
    """
    Get Kuzu tier-specific configuration, with overrides from kuzu.yml.

    This allows the container to override environment variables with
    tier-specific configuration from the kuzu.yml file, including:
    - Memory settings (max_memory_mb, memory_per_db_mb)
    - Performance settings (chunk_size, query_timeout, max_query_length)
    - Connection settings (connection_pool_size, databases_per_instance)

    Returns:
        Dictionary with tier configuration values
    """
    # Try to load tier-specific config if available
    try:
      from robosystems.config.tier_config import TierConfig

      # Determine tier from environment
      tier = cls.CLUSTER_TIER or cls.KUZU_NODE_TYPE

      # Map node types to tiers
      tier_mapping = {
        "shared_master": "shared",
        "shared_replica": "shared",
        "shared_repository": "shared",
        "standard": "standard",
        "enterprise": "enterprise",
        "premium": "premium",
      }

      tier = tier_mapping.get(tier, tier)

      if tier:
        # Get instance config from kuzu.yml
        instance_config = TierConfig.get_instance_config(tier)

        if instance_config:
          # Get full tier config for additional settings
          full_tier_config = TierConfig.get_tier_config(tier)

          # Override with values from config file if present
          return {
            # Memory settings
            "max_memory_mb": instance_config.get(
              "max_memory_mb", cls.KUZU_MAX_MEMORY_MB
            ),
            "memory_per_db_mb": instance_config.get(
              "memory_per_db_mb", cls.KUZU_MAX_MEMORY_PER_DB_MB
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
              get_int_env("KUZU_DATABASES_PER_INSTANCE", 0)
              if cls.ENVIRONMENT == "dev"
              and get_int_env("KUZU_DATABASES_PER_INSTANCE", 0) > 0
              else instance_config.get("databases_per_instance", 10)
            ),
            "max_databases": (
              get_int_env("KUZU_DATABASES_PER_INSTANCE", 0)
              if cls.ENVIRONMENT == "dev"
              and get_int_env("KUZU_DATABASES_PER_INSTANCE", 0) > 0
              else instance_config.get(
                "databases_per_instance", cls.KUZU_MAX_DATABASES_PER_NODE
              )
            ),
            # Tier-level settings from full config
            "tier": tier,
            "storage_limit_gb": full_tier_config.get("storage_limit_gb", 500),
            "monthly_credits": full_tier_config.get("monthly_credits", 10000),
            "rate_limit_multiplier": full_tier_config.get("rate_limit_multiplier", 1.0),
            "max_subgraphs": full_tier_config.get("max_subgraphs", 0),
          }
    except ImportError:
      pass  # TierConfig not available
    except Exception:
      pass  # Any other error loading config

    # Fall back to environment variables
    return {
      # Memory settings
      "max_memory_mb": cls.KUZU_MAX_MEMORY_MB,
      "memory_per_db_mb": cls.KUZU_MAX_MEMORY_PER_DB_MB,
      # Performance settings
      "chunk_size": get_int_env("KUZU_CHUNK_SIZE", 1000),
      "query_timeout": cls.GRAPH_QUERY_TIMEOUT,
      "max_query_length": cls.GRAPH_MAX_QUERY_LENGTH,
      "connection_pool_size": get_int_env("KUZU_CONNECTION_POOL_SIZE", 10),
      # Database settings
      "databases_per_instance": get_int_env("KUZU_DATABASES_PER_INSTANCE", 10),
      "max_databases": cls.KUZU_MAX_DATABASES_PER_NODE,
      # Default tier settings
      "tier": "standard",
      "storage_limit_gb": 500,
      "monthly_credits": 10000,
      "rate_limit_multiplier": 1.0,
      "max_subgraphs": 0,
    }

  @classmethod
  def get_kuzu_memory_config(cls) -> Dict[str, Any]:
    """Alias for backward compatibility with existing code."""
    return cls.get_kuzu_tier_config()

  @classmethod
  def get_database_url(cls, database_name: Optional[str] = None) -> str:
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
  def get_cors_origins(cls) -> List[str]:
    """Get CORS origins for Main API (backward compatibility)."""
    return cls.get_main_cors_origins()

  @classmethod
  def get_main_cors_origins(cls) -> List[str]:
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
  def get_kuzu_cors_origins(cls) -> List[str]:
    """Get CORS origins for Kuzu API (VPC-internal)."""
    if cls.is_production() or cls.is_staging():
      # VPC-internal APIs don't need CORS for browsers
      return []
    else:
      # Development only
      return ["*"]

  @classmethod
  def get_celery_config(cls) -> dict:
    """Get Celery configuration as a dict."""
    # Build authenticated URLs for prod/staging if not explicitly set
    broker_url = cls.CELERY_BROKER_URL
    result_backend = cls.CELERY_RESULT_BACKEND

    # Check if we need to build authenticated URLs
    # Try to get auth token from any source (env var, Secrets Manager, etc.)
    try:
      from .valkey_registry import ValkeyDatabase, ValkeyURLBuilder

      # Check if we can get an auth token from any source
      auth_token = ValkeyURLBuilder.get_auth_token()

      # Build authenticated URLs if we have a token or are in prod/staging
      if auth_token or cls.ENVIRONMENT in ["prod", "staging"]:
        # Only build if not explicitly set via environment
        if not os.getenv("CELERY_BROKER_URL"):
          try:
            broker_url = ValkeyURLBuilder.build_authenticated_url(
              database=ValkeyDatabase.CELERY_BROKER
            )
          except Exception:
            # Keep default if unable to build
            pass

        if not os.getenv("CELERY_RESULT_BACKEND"):
          try:
            result_backend = ValkeyURLBuilder.build_authenticated_url(
              database=ValkeyDatabase.CELERY_RESULTS
            )
          except Exception:
            # Keep default if unable to build
            pass
    except (ImportError, Exception):
      # If we can't import or get auth token, keep defaults
      pass

    return {
      "broker_url": broker_url,
      "result_backend": result_backend,
      "task_time_limit": cls.CELERY_TASK_TIME_LIMIT,
      "task_soft_time_limit": cls.CELERY_TASK_SOFT_TIME_LIMIT,
      "worker_prefetch_multiplier": cls.CELERY_WORKER_PREFETCH_MULTIPLIER,
      "task_serializer": "json",
      "result_serializer": "json",
      "accept_content": ["json"],
      "timezone": "UTC",
      "enable_utc": True,
    }

  @classmethod
  def get_valkey_url(
    cls, database: Optional[Union[int, "ValkeyDatabase"]] = None
  ) -> str:
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
    from .valkey_registry import ValkeyURLBuilder, ValkeyDatabase

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
