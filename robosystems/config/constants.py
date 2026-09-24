"""Fixed constants. Runtime-tunable values live in defaults.py."""

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

# Public request-body limits. FastAPI reads the body before auth or rate limits
# run, so this is the only bound; uploads go to S3 via presigned URLs.
PUBLIC_MAX_REQUEST_SIZE = 10 * 1024 * 1024  # 10 MB
WEBHOOK_MAX_REQUEST_SIZE = 512 * 1024  # 512 KB

# Dagster GraphQL client timeout (library default 300s). It runs on the API
# event loop, so a hung webserver would stall every tenant on the task.
DAGSTER_CLIENT_TIMEOUT_SECONDS = 15

# Batch Processing
DEFAULT_BATCH_SIZE = 5000
MIN_BATCH_SIZE = 1
MAX_BATCH_SIZE = 10000

# File Processing
MAX_FILES_PER_TASK = 1000
MAX_FILE_SIZE_MB = 100
PRESIGNED_URL_EXPIRY_SECONDS = 3600

# Below this, files stage inline in the request; above it, via Dagster.
SMALL_FILE_STAGING_THRESHOLD_MB = 50

# Platform-wide row ceiling per uploaded file; equals the largest tier's
# `max_single_table_rows` in graph.yml. Holds even against a hostile parquet
# footer declaring an absurd row count.
MAX_ROWS_PER_FILE = 100_000_000

# Row Count Estimation Fallback (bytes per row for different formats)
FALLBACK_BYTES_PER_ROW_PARQUET = 50
FALLBACK_BYTES_PER_ROW_CSV = 200
FALLBACK_BYTES_PER_ROW_JSON = 300

# Concurrent Operations (fixed limits)
MAX_CONCURRENT_DOWNLOADS = 5

# Time Limits (seconds)
TASK_TIME_LIMIT = 7200
TASK_SOFT_TIME_LIMIT = 6900

# OpenTelemetry
DEFAULT_SAMPLING_RATE = 0.1
MIN_SAMPLING_RATE = 0.0
MAX_SAMPLING_RATE = 1.0

# JWT Token Expiration
JWT_ACCESS_TOKEN_EXPIRE_MINUTES = 30
JWT_EXPIRY_HOURS = 0.5  # used for access token creation

# Email Token Expiration
EMAIL_TOKEN_EXPIRY_HOURS = 24
PASSWORD_RESET_TOKEN_EXPIRY_HOURS = 1
ORG_INVITATION_EXPIRY_DAYS = 7

# Authentication Security Constants
TOKEN_GRACE_PERIOD_MINUTES = 5  # Grace period for expired token refresh
JWT_REVOCATION_GRACE_SECONDS = (
  5  # Grace period for in-flight requests during token refresh
)
# Valkey key prefix for revoked JWTs (`{prefix}{jti}`), shared by writer,
# reader and cache stats.
JWT_REVOCATION_KEY_PREFIX = "revoked_jwt:"
JWT_DEVICE_FINGERPRINT_ENABLED = True  # token binding

# MCP OAuth 2.1 authorization server. Refresh tokens rotate on every use with
# a fresh lifetime, so a connector lapses only after this many idle days.
OAUTH_ACCESS_TOKEN_TTL_SECONDS = 3600
OAUTH_REFRESH_TOKEN_TTL_DAYS = 90
OAUTH_AUTHORIZATION_CODE_TTL_SECONDS = 120  # single-use; clients exchange at once
OAUTH_PENDING_AUTHORIZATION_TTL_SECONDS = 600  # login + consent must finish
OAUTH_DCR_UNUSED_REGISTRATION_TTL_HOURS = 24  # dynamic registrations that never consent


# =============================================================================
# GRAPH API CONFIGURATION
# =============================================================================

# Graph API Fixed Limits
GRAPH_MAX_REQUEST_SIZE = 10 * 1024 * 1024
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
LBUG_CONNECTION_TTL_MINUTES = 30.0

INGESTION_LOCK_TTL = 3600  # seconds; graph materialization locks

# =============================================================================
# AWS CONFIGURATION
# =============================================================================

# S3 bucket prefix (fixed naming convention)
AWS_S3_PREFIX = "robosystems"

# =============================================================================
# DATA PROCESSING CONFIGURATION
# =============================================================================

# Arelle (XBRL Processing) Fixed Limits
ARELLE_MIN_SCHEMA_COUNT = 10
ARELLE_DOWNLOAD_TIMEOUT = 10  # seconds

# XBRL Fixed Limits
XBRL_EXTERNALIZATION_THRESHOLD = 1024  # characters

# Multi-million-row tables that need aggressive memory cleanup after ingestion.
XBRL_GRAPH_LARGE_NODES = "Fact,Element,Label,Association,Structure,Dimension,Report"

# Filings per Dagster run (one batch per run; the sensor re-triggers while
# files remain, releasing memory between batches). 250 keeps the Arrow concat
# near ~325 MB peak for Label.
SEC_PROCESS_BATCH_SIZE = 250

# =============================================================================
# API VERSION CONSTANTS
# =============================================================================
# Pinned external API versions; change only as a deliberate upgrade.

# https://stripe.com/docs/api/versioning
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
  """Library-seeded Reporting Style Structure ids, one per equity form.

  Derived from each style's role URI via
  ``generate_deterministic_uuid(role, namespace='structure')``; pinned so
  the entity default and the renderer's picker agree.
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
