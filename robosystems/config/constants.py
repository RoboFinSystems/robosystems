"""
Static constants configuration.

This module contains both operational constants (timeouts, limits, etc.) and
static URI/string constants that don't change based on environment.
"""

# =============================================================================
# OPERATIONAL CONSTANTS - Merged from robosystems/constants.py
# =============================================================================

# Storage Pricing
STORAGE_CREDITS_PER_GB_PER_DAY = 0.05

# Default Timeouts (seconds)
DEFAULT_HTTP_TIMEOUT = 30
DEFAULT_QUERY_TIMEOUT = 30
DEFAULT_CONNECTION_TIMEOUT = 10

# Cache TTL Values (seconds)
CACHE_TTL_SHORT = 300  # 5 minutes
CACHE_TTL_MEDIUM = 600  # 10 minutes
CACHE_TTL_LONG = 3600  # 1 hour
CACHE_TTL_EXTRA_LONG = 7200  # 2 hours

# Rate Limiting Constants
RATE_LIMIT_WINDOW_SHORT = 300  # 5 minutes
RATE_LIMIT_WINDOW_LONG = 3600  # 1 hour

# Query Limits
DEFAULT_QUERY_LIMIT = 1000
MAX_QUERY_LIMIT = 10000
MIN_QUERY_LIMIT = 1

# Result Size Limits (MB)
DEFAULT_MAX_RESULT_SIZE_MB = 5.0
MIN_RESULT_SIZE_MB = 0.1
MAX_RESULT_SIZE_MB = 50.0

# Retry Configuration
DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_DELAY = 60  # seconds
MIN_RETRY_DELAY = 1
MAX_RETRY_DELAY = 300

# Database Pool Configuration
DEFAULT_POOL_SIZE = 20
DEFAULT_MAX_OVERFLOW = 40
DEFAULT_POOL_TIMEOUT = 30
DEFAULT_POOL_RECYCLE = 3600  # 1 hour

# Worker Configuration
DEFAULT_WORKER_COUNT = 4
MIN_WORKER_COUNT = 1
MAX_WORKER_COUNT = 16

# Credit Allocation
DEFAULT_CREDIT_ALLOCATION_DAY = 1  # 1st of month
DEFAULT_CREDIT_ALLOCATION_HOUR = 3  # 3 AM UTC

# Port Configuration
MIN_PORT = 1
MAX_PORT = 65535
DEFAULT_API_PORT = 8000
DEFAULT_KUZU_PORT = 8001

# Percentage Thresholds
ADMISSION_MEMORY_THRESHOLD_DEFAULT = 85.0  # percent (of total instance memory)
ADMISSION_CPU_THRESHOLD_DEFAULT = 90.0  # percent
ADMISSION_QUEUE_THRESHOLD_DEFAULT = 0.8  # 80% as decimal

# String Length Limits
MAX_QUERY_LENGTH = 10000  # characters
MAX_ERROR_MESSAGE_LENGTH = 1000  # characters

# Batch Processing
DEFAULT_BATCH_SIZE = 5000  # Optimized for Kuzu bulk ingestion
MIN_BATCH_SIZE = 1
MAX_BATCH_SIZE = 10000  # Increased for large-scale operations

# File Processing
MAX_FILES_PER_TASK = 1000
MAX_FILE_SIZE_MB = 100

# Queue Sizes
DEFAULT_QUEUE_SIZE = 1000
MAX_QUEUE_SIZE = 10000
MIN_QUEUE_SIZE = 10

# Concurrent Operations
DEFAULT_MAX_CONCURRENT = 50
MAX_CONCURRENT_DOWNLOADS = 5
MAX_DATABASES_PER_NODE = (
  10  # Default for standard tier (can be overridden per tier in kuzu.yml)
  # Standard: 10 databases with 2GB each (oversubscribed on 14GB instance)
  # Enterprise/Premium: 1 database (dedicated instance)
  # Future upgrade path: Increase instance size, keep same DB allocation
)

# Time Limits
TASK_TIME_LIMIT = 7200  # 2 hours
TASK_SOFT_TIME_LIMIT = 6900  # 1 hour 55 minutes

# OpenTelemetry
DEFAULT_SAMPLING_RATE = 0.1
MIN_SAMPLING_RATE = 0.0
MAX_SAMPLING_RATE = 1.0

# JWT Token Expiration
JWT_ACCESS_TOKEN_EXPIRE_MINUTES = 30
JWT_REFRESH_TOKEN_EXPIRE_DAYS = 30

# Authentication Security Constants
TOKEN_GRACE_PERIOD_MINUTES = 5  # Grace period for expired token refresh
JWT_DEVICE_FINGERPRINT_ENABLED = True  # Enable device fingerprinting for token binding

# Rate Limiting Defaults
JWT_REFRESH_RATE_LIMIT_DEFAULT = 20  # Requests per minute for token refresh
AUTH_RATE_LIMIT_LOGIN_DEFAULT = 5  # Login attempts per 5 minutes
AUTH_RATE_LIMIT_REGISTER_DEFAULT = 3  # Registration attempts per hour
AUTH_RATE_LIMIT_WINDOW_LOGIN = 300  # 5 minutes for login rate limiting
AUTH_RATE_LIMIT_WINDOW_REGISTER = 3600  # 1 hour for registration rate limiting

# =============================================================================
# FIXED TECHNICAL LIMITS - Not configurable per environment
# =============================================================================

# Kuzu Fixed Limits
KUZU_MAX_REQUEST_SIZE = 10 * 1024 * 1024  # 10MB
KUZU_CONNECT_TIMEOUT = 5.0  # seconds
KUZU_READ_TIMEOUT = 30.0  # seconds

# Arelle (XBRL Processing) Fixed Limits
ARELLE_MIN_SCHEMA_COUNT = 10
ARELLE_DOWNLOAD_TIMEOUT = 10  # seconds

# XBRL Fixed Limits
XBRL_EXTERNALIZATION_THRESHOLD = 1024  # characters

# =============================================================================
# RESILIENCY AND CIRCUIT BREAKER DEFAULTS
# =============================================================================

# Kuzu Circuit Breaker/Resiliency
KUZU_ALB_HEALTH_CACHE_TTL = 30  # seconds
KUZU_INSTANCE_CACHE_TTL = 60  # seconds
KUZU_CIRCUIT_BREAKER_THRESHOLD = 5  # failures before opening
KUZU_CIRCUIT_BREAKER_TIMEOUT = 60  # seconds before retry

# =============================================================================
# QUEUE CONFIGURATION DEFAULTS
# =============================================================================

# Query Queue Defaults
QUERY_QUEUE_MAX_PER_USER = 10
QUERY_DEFAULT_PRIORITY = 5
QUERY_PRIORITY_BOOST_PREMIUM = 2
QUERY_QUEUE_TIMEOUT = 300  # 5 minutes

# Admission Control
ADMISSION_CHECK_INTERVAL = 1.0  # seconds

# Load Shedding Pressure Thresholds (decimal values)
LOAD_SHED_START_PRESSURE_DEFAULT = 0.8  # 80% pressure
LOAD_SHED_STOP_PRESSURE_DEFAULT = 0.6  # 60% pressure

# =============================================================================
# RETRY CONFIGURATION
# =============================================================================

# SEC Pipeline Retries
SEC_PIPELINE_MAX_RETRIES = 3

# OpenFIGI API Retries
OPENFIGI_RETRY_MIN_WAIT = 10000  # milliseconds (10 seconds)
OPENFIGI_RETRY_MAX_WAIT = 30000  # milliseconds (30 seconds)

# =============================================================================
# FIXED BUSINESS RULES
# =============================================================================

# Credit Allocation Schedule
CREDIT_ALLOCATION_DAY = 1  # 1st of month
CREDIT_ALLOCATION_HOUR = 3  # 3 AM UTC

# SEC API Rate Limiting
SEC_RATE_LIMIT = 10  # requests per second (SEC.gov requirement)

# =============================================================================
# TIER-SPECIFIC MEMORY ALLOCATIONS
# =============================================================================

# Instance Memory Limits by Tier (MB)
KUZU_STANDARD_MAX_MEMORY_MB = 14336  # 14GB for r7g.large
KUZU_ENTERPRISE_MAX_MEMORY_MB = 14336  # 14GB for r7g.large
KUZU_PREMIUM_MAX_MEMORY_MB = 28672  # 28GB for r7g.xlarge

# Per-Database Memory Limits by Tier (MB)
KUZU_STANDARD_MEMORY_PER_DB_MB = 2048  # 2GB per database
KUZU_ENTERPRISE_MEMORY_PER_DB_MB = 14336  # Full instance memory (dedicated)
KUZU_PREMIUM_MEMORY_PER_DB_MB = 28672  # Full instance memory (dedicated)

# =============================================================================
# TIER-SPECIFIC STREAMING CHUNK SIZES
# =============================================================================

# Streaming Chunk Sizes by Tier
KUZU_STANDARD_CHUNK_SIZE = 1000  # rows
KUZU_ENTERPRISE_CHUNK_SIZE = 5000  # rows
KUZU_PREMIUM_CHUNK_SIZE = 10000  # rows

# =============================================================================
# DUCKDB CONFIGURATION
# =============================================================================

# DuckDB Performance Settings
DUCKDB_MAX_THREADS = 4  # Limit threads to prevent oversubscription
DUCKDB_MEMORY_LIMIT = "2GB"  # Per-connection memory limit

# =============================================================================
# STATIC STRING/URI CONSTANTS
# =============================================================================


class URIConstants:
  """URI constants for various services and standards."""

  # RoboSystems URIs
  ROBOSYSTEMS_BASE_URI = "https://robosystems.ai"
  ROBOLEDGER_BASE_URI = "https://roboledger.ai"
  ROBOINVESTOR_BASE_URI = "https://roboinvestor.ai"

  # External service URIs
  QUICKBOOKS_BASE_URI = "https://quickbooks.intuit.com"
  SEC_BASE_URI = "https://www.sec.gov"
  SEC_FILING_URI = SEC_BASE_URI + "/Archives/edgar/data"
  SEC_FILER_URI = SEC_BASE_URI + "/CIK#"

  # Standard URIs
  ISO_8601_URI = "http://www.w3.org/2001/XMLSchema#dateTime"
  ISO_4217_URI = "http://www.xbrl.org/2003/iso4217"


class PrefixConstants:
  """Prefix constants for namespacing."""

  # RoboSystems prefixes
  ROBOSYSTEMS_PREFIX = "rsai"
  ROBOLEDGER_PREFIX = "rlai"
  ROBOINVESTOR_PREFIX = "riai"

  # External service prefixes
  QUICKBOOKS_PREFIX = "qbo"
  SEC_PREFIX = "sec"

  # Standard prefixes
  ISO_8601_PREFIX = "iso8601"
  ISO_4217_PREFIX = "iso4217"


class XBRLConstants:
  """XBRL-specific constants."""

  # XBRL role URIs
  SRT_EXTENSIBLE_ENUMERATION_LISTS = (
    "http://fasb.org/srt/role/srt-eedm/ExtensibleEnumerationLists"
  )
  USGAAP_EXTENSIBLE_ENUMERATION_LISTS = (
    "http://fasb.org/us-gaap/role/eedm/ExtensibleEnumerationLists"
  )
  XBRL_ROLE_LINK = "http://www.xbrl.org/2003/role/link"

  # Filtered roles
  ROLES_FILTERED = [
    SRT_EXTENSIBLE_ENUMERATION_LISTS,
    USGAAP_EXTENSIBLE_ENUMERATION_LISTS,
    XBRL_ROLE_LINK,
  ]

  # XBRL namespaces
  XBRL_NAMESPACES = {
    "xbrl": "http://www.xbrl.org/2003/instance",
    "xbrli": "http://www.xbrl.org/2003/instance",
    "link": "http://www.xbrl.org/2003/linkbase",
    "xlink": "http://www.w3.org/1999/xlink",
    "xsd": "http://www.w3.org/2001/XMLSchema",
    "iso4217": "http://www.xbrl.org/2003/iso4217",
  }
