"""
SEC adapter configuration constants.

These are processing-specific constants for the SEC/XBRL pipeline.
They are not runtime-configurable - change them here if needed.
"""

from robosystems.config import env
from robosystems.config.constants import MAX_CONCURRENT_DOWNLOADS

# =============================================================================
# SEC EDGAR API CONFIGURATION
# =============================================================================
# Configuration for SEC EDGAR API access and rate limiting

SEC_CONFIG = {
  "base_url": "https://www.sec.gov",
  "data_base_url": "https://data.sec.gov",
  "user_agent": env.SEC_GOV_USER_AGENT,
  "rate_limit": 10,  # requests per second (SEC.gov requirement)
  "timeout": 30,
  "sync_timeout": 10,
  "filing_download_timeout": 300,  # 5 minutes for large files
  "filing_metadata_timeout": 60,
  "xbrl_download_timeout": 30,
  "retry_attempts": 3,
  "retry_delay": 1,
  "retry_min_wait": 600,
  "retry_max_wait": 1000,
  "max_concurrent_downloads": MAX_CONCURRENT_DOWNLOADS,
  "bulk_download_url": "https://www.sec.gov/Archives/edgar/daily-index/bulkdata/",
  "xbrl_rss_url": "https://www.sec.gov/Archives/edgar/xbrlrss.all.xml",
  "startup_delay": 30,
  "headers": {"User-Agent": env.SEC_GOV_USER_AGENT},
}

# =============================================================================
# ARELLE CONFIGURATION
# =============================================================================
# Arelle is the XBRL processing library

# Logging configuration
ARELLE_LOG_FILE = "logToBuffer"

# Timeout for Arelle operations (seconds)
ARELLE_TIMEOUT = 30

# Timeout for individual schema downloads (seconds)
ARELLE_DOWNLOAD_TIMEOUT = 10

# Work offline mode - use cached schemas only
ARELLE_WORK_OFFLINE = False

# Minimum number of cached schemas required for offline operation
ARELLE_MIN_SCHEMA_COUNT = 10

# =============================================================================
# XBRL PROCESSING CONFIGURATION
# =============================================================================
# Configuration for XBRL graph extraction and transformation

# Externalize large text values to S3 (reduces database size)
XBRL_EXTERNALIZE_LARGE_VALUES = True

# Character threshold for externalizing values
XBRL_EXTERNALIZATION_THRESHOLD = 1024

# Skip textblock facts entirely (for historical data where text isn't needed)
XBRL_SKIP_TEXTBLOCK_FACTS = False

# Feature flags for upstream simplification (disabled by default)
XBRL_STANDARDIZED_FILENAMES = False
XBRL_TYPE_PREFIXES = False
XBRL_COLUMN_STANDARDIZATION = False

# =============================================================================
# SEC PIPELINE CONFIGURATION
# =============================================================================
# Configuration for SEC EDGAR data fetching and processing

# Maximum concurrent downloads from SEC.gov
SEC_MAX_CONCURRENT_DOWNLOADS = 5

# Validate CIK with SEC API before processing
SEC_VALIDATE_CIK = True

# Allow partial failures in pipeline (continue if some filings fail)
SEC_PIPELINE_PARTIAL_TOLERANCE = True

# Clean up temporary files after processing
SEC_PIPELINE_CLEANUP_TEMP_FILES = True
