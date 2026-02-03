"""
SEC adapter configuration constants.

These are processing-specific constants for the SEC/XBRL pipeline.
They are not runtime-configurable - change them here if needed.
"""

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
