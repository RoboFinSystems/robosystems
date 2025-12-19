"""
Centralized external services configuration.

This module contains all configuration for external APIs and services.
"""

import os
from typing import Any

from . import env


class ExternalServicesConfig:
  """Centralized external services configuration."""

  # SEC EDGAR API Configuration
  SEC_CONFIG = {
    "base_url": "https://www.sec.gov",
    "data_base_url": "https://data.sec.gov",  # For data API calls
    "user_agent": env.SEC_GOV_USER_AGENT,
    "rate_limit": env.SEC_RATE_LIMIT,
    "timeout": 30,  # seconds
    "sync_timeout": 10,  # seconds for sync operations
    "filing_download_timeout": 300,  # 5 minutes for large files
    "filing_metadata_timeout": 60,  # 1 minute for metadata
    "xbrl_download_timeout": 30,  # 30 seconds for XBRL files
    "retry_attempts": 3,
    "retry_delay": 1,  # seconds
    "retry_min_wait": 600,  # 10 seconds min wait for retries
    "retry_max_wait": 1000,  # 16.7 seconds max wait for retries
    "max_concurrent_downloads": env.SEC_MAX_CONCURRENT_DOWNLOADS,
    "bulk_download_url": "https://www.sec.gov/Archives/edgar/daily-index/bulkdata/",
    "xbrl_rss_url": "https://www.sec.gov/Archives/edgar/xbrlrss.all.xml",
    "startup_delay": 30,  # seconds to wait before starting SEC tasks
    "headers": {"User-Agent": env.SEC_GOV_USER_AGENT},
  }

  # OpenFIGI API Configuration
  OPENFIGI_CONFIG = {
    "base_url": "https://api.openfigi.com/v3/",
    "mapping_endpoint": "mapping",
    "timeout": 30,  # seconds
    "retry_attempts": 3,
    "retry_min_wait": 10000,  # milliseconds (10 seconds)
    "retry_max_wait": 30000,  # milliseconds (30 seconds)
    "rate_limit_delay": 60,  # seconds to wait on 429 rate limit
    "headers": {"Content-Type": "application/json"},
  }

  # QuickBooks API Configuration
  QUICKBOOKS_CONFIG = {
    "base_url": "https://quickbooks.api.intuit.com",
    "sandbox_url": "https://sandbox-quickbooks.api.intuit.com",
    "auth_url": "https://appcenter.intuit.com/connect/oauth2",
    "discovery_url": "https://developer.api.intuit.com/.well-known/openid_configuration",
    "api_version": "v3",
    "timeout": 60,  # seconds
    "retry_attempts": 3,
    "retry_delay": 2,  # seconds
    "token_refresh_buffer": 300,  # Refresh token 5 minutes before expiry
    "scopes": [
      "com.intuit.quickbooks.accounting",
      "com.intuit.quickbooks.payment",
    ],
  }

  # AWS S3 Configuration
  S3_CONFIG = {
    "region": env.AWS_DEFAULT_REGION,
    "bucket_prefix": "robosystems",
    "multipart_threshold": 100 * 1024 * 1024,  # 100 MB
    "multipart_chunksize": 10 * 1024 * 1024,  # 10 MB
    "max_bandwidth": 100 * 1024 * 1024,  # 100 MB/s
    "transfer_config": {
      "max_concurrency": 10,
      "num_download_attempts": 5,
      "max_io_queue": 100,
    },
    "lifecycle_rules": {
      "temp_files_expiry_days": 1,
      "backup_transition_days": 30,  # Move to Glacier
      "backup_expiry_days": 365,
    },
  }

  # Stripe Payment Configuration
  STRIPE_CONFIG = {
    "api_version": "2023-10-16",
    "webhook_tolerance": 300,  # 5 minutes
    "payment_method_types": ["card", "us_bank_account"],
    "currency": "usd",
    "invoice_settings": {
      "days_until_due": 7,
      "auto_advance": True,
    },
  }

  @classmethod
  def get_config(cls, service: str) -> dict[str, Any]:
    """Get configuration for a specific service."""
    config_attr = f"{service.upper()}_CONFIG"
    return getattr(cls, config_attr, {})

  @classmethod
  def get_api_key(cls, service: str) -> str | None:
    """Get API key for a service from environment."""
    # Dynamic env var lookup - must use os.getenv directly
    env_var = f"{service.upper()}_API_KEY"
    return os.getenv(env_var)

  @classmethod
  def get_endpoint(cls, service: str, path: str = "") -> str:
    """Build full endpoint URL for a service."""
    config = cls.get_config(service)
    base_url = config.get("base_url", "")

    # Handle sandbox/production environments
    if service == "quickbooks" and env.QUICKBOOKS_SANDBOX:
      base_url = config.get("sandbox_url", base_url)

    return f"{base_url.rstrip('/')}/{path.lstrip('/')}"

  @classmethod
  def is_sandbox(cls, service: str) -> bool:
    """Check if service is in sandbox mode."""
    # Dynamic env var lookup - must use os.getenv directly
    return os.getenv(f"{service.upper()}_SANDBOX", "false").lower() == "true"
