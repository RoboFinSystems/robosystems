"""Utility functions and helpers for RoboSystems."""

# Constants and URIs - Re-export from centralized config
from ..config import URIConstants, PrefixConstants, XBRLConstants

# UUID v7 utilities for time-ordered unique IDs
from .uuid import (
  generate_uuid7,
  generate_prefixed_uuid7,
  generate_deterministic_uuid7,
  parse_uuid7,
  get_timestamp_from_uuid7,
  create_prefixed_id,
)

# HTML parsing utilities
from .html_parser import extract_structured_content, save_structured_content

# Documentation template utilities
from .docs_template import (
  generate_swagger_docs,
  generate_robosystems_docs,
  generate_kuzu_docs,
)

# ULID utilities for time-ordered unique IDs
from .ulid import (
  generate_ulid,
  generate_prefixed_ulid,
  parse_ulid,
  get_timestamp_from_ulid,
  default_ulid,
  default_transaction_ulid,
  default_usage_ulid,
  default_credit_ulid,
)

# Query cost calculation utilities - removed (all queries are included now)

# Re-export constants for convenience
ROBOSYSTEMS_BASE_URI = URIConstants.ROBOSYSTEMS_BASE_URI
ROBOLEDGER_BASE_URI = URIConstants.ROBOLEDGER_BASE_URI
ROBOINVESTOR_BASE_URI = URIConstants.ROBOINVESTOR_BASE_URI
QUICKBOOKS_BASE_URI = URIConstants.QUICKBOOKS_BASE_URI
SEC_BASE_URI = URIConstants.SEC_BASE_URI
SEC_FILING_URI = URIConstants.SEC_FILING_URI
SEC_FILER_URI = URIConstants.SEC_FILER_URI
ISO_8601_URI = URIConstants.ISO_8601_URI
ISO_4217_URI = URIConstants.ISO_4217_URI
ROBOSYSTEMS_PREFIX = PrefixConstants.ROBOSYSTEMS_PREFIX
ROBOLEDGER_PREFIX = PrefixConstants.ROBOLEDGER_PREFIX
ROBOINVESTOR_PREFIX = PrefixConstants.ROBOINVESTOR_PREFIX
QUICKBOOKS_PREFIX = PrefixConstants.QUICKBOOKS_PREFIX
SEC_PREFIX = PrefixConstants.SEC_PREFIX
ISO_8601_PREFIX = PrefixConstants.ISO_8601_PREFIX
ISO_4217_PREFIX = PrefixConstants.ISO_4217_PREFIX
SRT_EXTENSIBLE_ENUMERATION_LISTS = XBRLConstants.SRT_EXTENSIBLE_ENUMERATION_LISTS
USGAAP_EXTENSIBLE_ENUMERATION_LISTS = XBRLConstants.USGAAP_EXTENSIBLE_ENUMERATION_LISTS
XBRL_ROLE_LINK = XBRLConstants.XBRL_ROLE_LINK
ROLES_FILTERED = XBRLConstants.ROLES_FILTERED

__all__ = [
  # UUID v7 utilities
  "generate_uuid7",
  "generate_prefixed_uuid7",
  "generate_deterministic_uuid7",
  "parse_uuid7",
  "get_timestamp_from_uuid7",
  "create_prefixed_id",
  # HTML parsing
  "extract_structured_content",
  "save_structured_content",
  # Documentation
  "generate_swagger_docs",
  "generate_robosystems_docs",
  "generate_kuzu_docs",
  # ULID utilities
  "generate_ulid",
  "generate_prefixed_ulid",
  "parse_ulid",
  "get_timestamp_from_ulid",
  "default_ulid",
  "default_transaction_ulid",
  "default_usage_ulid",
  "default_credit_ulid",
  # Constants
  "ROBOSYSTEMS_BASE_URI",
  "ROBOLEDGER_BASE_URI",
  "ROBOINVESTOR_BASE_URI",
  "QUICKBOOKS_BASE_URI",
  "SEC_BASE_URI",
  "SEC_FILING_URI",
  "SEC_FILER_URI",
  "ISO_8601_URI",
  "ISO_4217_URI",
  "ROBOSYSTEMS_PREFIX",
  "ROBOLEDGER_PREFIX",
  "ROBOINVESTOR_PREFIX",
  "QUICKBOOKS_PREFIX",
  "SEC_PREFIX",
  "ISO_8601_PREFIX",
  "ISO_4217_PREFIX",
  "SRT_EXTENSIBLE_ENUMERATION_LISTS",
  "USGAAP_EXTENSIBLE_ENUMERATION_LISTS",
  "XBRL_ROLE_LINK",
  "ROLES_FILTERED",
]
