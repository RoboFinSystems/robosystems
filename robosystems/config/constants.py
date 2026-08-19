"""
Static constants configuration.

This module contains fixed values that never change at runtime.
For tunable defaults (SSM Parameter Store), see defaults.py.

Categories:
- CONSTANTS (this file): Fixed values that never change
- TUNABLES (defaults.py): Operational parameters adjustable via SSM
- SECRETS (secrets_manager.py): Sensitive credentials and API keys
"""

# =============================================================================
# OPERATIONAL CONSTANTS
# =============================================================================

# Port Configuration
MIN_PORT = 1
MAX_PORT = 65535
DEFAULT_API_PORT = 8000
DEFAULT_GRAPH_API_PORT = 8001

# String Length Limits
MAX_QUERY_LENGTH = 10000  # characters
MAX_ERROR_MESSAGE_LENGTH = 1000  # characters

# Batch Processing
DEFAULT_BATCH_SIZE = 5000  # Optimized for Graph API bulk ingestion
MIN_BATCH_SIZE = 1
MAX_BATCH_SIZE = 10000  # Increased for large-scale operations

# File Processing
MAX_FILES_PER_TASK = 1000
MAX_FILE_SIZE_MB = 100
PRESIGNED_URL_EXPIRY_SECONDS = 3600  # 1 hour

# Small file threshold for direct staging (bypasses Dagster for speed)
# Files below this size are staged directly in the HTTP request
# Files above this size use Dagster for async processing with progress tracking
SMALL_FILE_STAGING_THRESHOLD_MB = 50  # 50MB

# Platform-wide ceiling on rows in a single uploaded file, checked at ingest
# from the measured (or estimated) row count. Equal to the largest tier's
# `max_single_table_rows` in .github/configs/graph.yml — no tier can materialize
# a table bigger than this, so a file above it is refused before it burns
# storage. Smaller tiers are capped tighter by their own `max_single_table_rows`
# at ingest as well; this is the bound that holds even for a hostile parquet
# footer that declares an absurd row count against a 100 MB object.
MAX_ROWS_PER_FILE = 100_000_000

# Row Count Estimation Fallback (bytes per row for different formats)
FALLBACK_BYTES_PER_ROW_PARQUET = 50  # Compressed format
FALLBACK_BYTES_PER_ROW_CSV = 200  # Text format with moderate row size
FALLBACK_BYTES_PER_ROW_JSON = 300  # Text format with more verbose structure

# Concurrent Operations (fixed limits)
MAX_CONCURRENT_DOWNLOADS = 5

# Time Limits
TASK_TIME_LIMIT = 7200  # 2 hours
TASK_SOFT_TIME_LIMIT = 6900  # 1 hour 55 minutes

# OpenTelemetry
DEFAULT_SAMPLING_RATE = 0.1
MIN_SAMPLING_RATE = 0.0
MAX_SAMPLING_RATE = 1.0

# JWT Token Expiration
JWT_ACCESS_TOKEN_EXPIRE_MINUTES = 30
JWT_EXPIRY_HOURS = 0.5  # 30 minutes - used for access token creation

# Email Token Expiration
EMAIL_TOKEN_EXPIRY_HOURS = 24  # Email verification token validity
PASSWORD_RESET_TOKEN_EXPIRY_HOURS = 1  # Password reset token validity
ORG_INVITATION_EXPIRY_DAYS = 7  # Org invitation link validity

# Authentication Security Constants
TOKEN_GRACE_PERIOD_MINUTES = 5  # Grace period for expired token refresh
JWT_REVOCATION_GRACE_SECONDS = (
  5  # Grace period for in-flight requests during token refresh
)
JWT_DEVICE_FINGERPRINT_ENABLED = True  # Enable device fingerprinting for token binding

# Rate Limiting Defaults
JWT_REFRESH_RATE_LIMIT_DEFAULT = 20  # Requests per minute for token refresh
AUTH_RATE_LIMIT_LOGIN_DEFAULT = 5  # Login attempts per 5 minutes
AUTH_RATE_LIMIT_REGISTER_DEFAULT = 3  # Registration attempts per hour
AUTH_RATE_LIMIT_WINDOW_LOGIN = 300  # 5 minutes for login rate limiting
AUTH_RATE_LIMIT_WINDOW_REGISTER = 3600  # 1 hour for registration rate limiting

# =============================================================================
# GRAPH API CONFIGURATION
# =============================================================================

# Graph API Fixed Limits
GRAPH_MAX_REQUEST_SIZE = 10 * 1024 * 1024  # 10MB
GRAPH_CONNECT_TIMEOUT = 5.0  # seconds
GRAPH_READ_TIMEOUT = 30.0  # seconds

# Graph API Cache TTLs (infrastructure, not tunables)
GRAPH_ALB_HEALTH_CACHE_TTL = 30  # seconds
GRAPH_INSTANCE_CACHE_TTL = 60  # seconds

# Query Priority (fixed business rules)
QUERY_DEFAULT_PRIORITY = 5
QUERY_PRIORITY_BOOST_PREMIUM = 2

# Admission Control Interval (fixed timing)
ADMISSION_CHECK_INTERVAL = 1.0  # seconds

# Health Check Intervals (minutes)
GRAPH_HEALTH_CHECK_INTERVAL_MINUTES = 5.0
LBUG_HEALTH_CHECK_INTERVAL_MINUTES = 5.0

# Materialization Threshold - staged data above this size routes to Dagster
GRAPH_MATERIALIZATION_THRESHOLD_MB = 500

# =============================================================================
# LADYBUGDB CONFIGURATION
# =============================================================================

# LadybugDB Connection Management
LBUG_MAX_CONNECTIONS_PER_DB = 10
LBUG_CONNECTION_TTL_MINUTES = 30.0  # Connection time-to-live

# Distributed Lock TTL
INGESTION_LOCK_TTL = 3600  # 1 hour - for graph materialization locks

# =============================================================================
# AWS CONFIGURATION
# =============================================================================

# S3 bucket prefix (fixed naming convention)
AWS_S3_PREFIX = "robosystems"

# =============================================================================
# SSE RATE LIMITING
# =============================================================================

# Server-Sent Events connection limits
RATE_LIMIT_SSE_CONNECTIONS = 10
RATE_LIMIT_SSE_CONNECTIONS_WINDOW = 60  # seconds

# =============================================================================
# DATA PROCESSING CONFIGURATION
# =============================================================================

# Arelle (XBRL Processing) Fixed Limits
ARELLE_MIN_SCHEMA_COUNT = 10
ARELLE_DOWNLOAD_TIMEOUT = 10  # seconds

# XBRL Fixed Limits
XBRL_EXTERNALIZATION_THRESHOLD = 1024  # characters

# XBRL graph large nodes that require aggressive memory cleanup after LadybugDB ingestion
# These tables contain millions of rows and consume significant memory
XBRL_GRAPH_LARGE_NODES = "Fact,Element,Label,Association,Structure,Dimension,Report"

# SEC Processing Batch Size
# Each Dagster run processes exactly one batch, then exits. The sensor
# re-triggers if pending files remain, enabling natural memory release
# between batches and crash resilience (at most one batch lost).
# Part-file output: each batch writes one part_{uuid}.parquet per table.
# 250 filings keeps Arrow concat well under memory limits (~325 MB peak
# for Label at ~1.3 MB/file), producing one part file per table per batch.
# S3 zip cache makes batch size independent of Spot interruption risk.
# Q2 (proxy season, ~11k filings) = ~44 sensor-triggered runs.
SEC_PROCESS_BATCH_SIZE = 250

# =============================================================================
# API VERSION CONSTANTS
# =============================================================================
# These are pinned API versions for external service compatibility.
# They should only change when explicitly upgrading API versions,
# not as part of secrets or runtime configuration.

# Stripe API Version
# Pinned to ensure consistent behavior across deployments.
# See: https://stripe.com/docs/api/versioning
STRIPE_API_VERSION = "2026-01-28.clover"

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


class ReportingStyleConstants:
  """Reporting Style identifiers (Charlie Hoffman's term).

  Library-seeded Structure UUIDs for the default-family Reporting Styles
  declared in ``rs-gaap-reporting-styles/v1`` — the equity-form axis
  (CORP/PART/LLC) over a fixed BSC / multi-step IS / indirect CF layout.
  Each id is derived deterministically from its style's role URI via
  ``generate_deterministic_uuid(role, namespace='structure')``; pinned
  here so the ``entities.reporting_style_id`` default (stamped from the
  entity's legal form at creation) and the renderer's picker share a
  single source of truth.
  """

  DEFAULT_STYLE_ID = "025f5d48-12ce-5d65-b9eb-4f137a10ef06"
  PARTNERSHIP_STYLE_ID = "10d05f23-8ea8-5348-b8c9-f1e65bbda4a3"
  LLC_STYLE_ID = "69bee020-87d6-5e5d-8c1e-9007d8eb8d4f"


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
