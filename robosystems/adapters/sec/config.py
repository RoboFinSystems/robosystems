"""SEC/XBRL pipeline constants; not runtime-configurable."""

from robosystems.config import env
from robosystems.config.constants import MAX_CONCURRENT_DOWNLOADS

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

# Platform settings for xbrlkit's Arelle load; the cache is env.ARELLE_CACHE_DIR.
ARELLE_TIMEOUT = 30  # seconds per DTS document fetch

# Serve the DTS from cache only; a miss raises DtsResolutionError.
ARELLE_WORK_OFFLINE = False


def xbrlkit_config():
  from xbrlkit.config import Config

  return Config(
    user_agent=SEC_CONFIG["user_agent"],
    request_timeout=SEC_CONFIG["timeout"],
    arelle_timeout=ARELLE_TIMEOUT,
    arelle_offline=ARELLE_WORK_OFFLINE,
  )


# XBRL processing
XBRL_EXTERNALIZE_LARGE_VALUES = True  # move large text values to the public bucket
XBRL_EXTERNALIZATION_THRESHOLD = 1024  # characters

# Also keep externalized values in the graph; local experiments only.
XBRL_KEEP_TEXTBLOCKS_INLINE = env.XBRL_KEEP_TEXTBLOCKS_INLINE

# Publish holon, Tavi, primary document and manifest at process time.
XBRL_FILING_ARTIFACTS = env.SEC_FILING_ARTIFACTS_ENABLED

XBRL_SKIP_TEXTBLOCK_FACTS = False

XBRL_STANDARDIZED_FILENAMES = False
XBRL_TYPE_PREFIXES = False
XBRL_COLUMN_STANDARDIZATION = False

XBRL_SEMANTIC_ENRICHMENT = True  # embedding-based canonical concepts
XBRL_GRAPH_REFINEMENT = True  # adjust scores with precomputed graph artifacts
XBRL_ASSOCIATION_CLASSIFICATION = True

SEC_MAX_CONCURRENT_DOWNLOADS = 5
SEC_VALIDATE_CIK = True
SEC_PIPELINE_PARTIAL_TOLERANCE = True  # continue when some filings fail
SEC_PIPELINE_CLEANUP_TEMP_FILES = True
