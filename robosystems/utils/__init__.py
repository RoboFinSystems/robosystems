"""Utility functions and helpers for RoboSystems."""

from ..config import PrefixConstants, URIConstants, XBRLConstants
from .docs_template import (
  generate_lbug_docs,
  generate_robosystems_docs,
  generate_swagger_docs,
)
from .html_parser import extract_structured_content, save_structured_content
from .ulid import (
  generate_prefixed_ulid,
  generate_ulid,
  get_timestamp_from_ulid,
  parse_ulid,
)
from .uuid import (
  generate_deterministic_uuid,
  generate_uuid7,
)

# Re-export config constants so callers can import them from robosystems.utils
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
  "ISO_4217_PREFIX",
  "ISO_4217_URI",
  "ISO_8601_PREFIX",
  "ISO_8601_URI",
  "QUICKBOOKS_BASE_URI",
  "QUICKBOOKS_PREFIX",
  "ROBOINVESTOR_BASE_URI",
  "ROBOINVESTOR_PREFIX",
  "ROBOLEDGER_BASE_URI",
  "ROBOLEDGER_PREFIX",
  # Constants
  "ROBOSYSTEMS_BASE_URI",
  "ROBOSYSTEMS_PREFIX",
  "ROLES_FILTERED",
  "SEC_BASE_URI",
  "SEC_FILER_URI",
  "SEC_FILING_URI",
  "SEC_PREFIX",
  "SRT_EXTENSIBLE_ENUMERATION_LISTS",
  "USGAAP_EXTENSIBLE_ENUMERATION_LISTS",
  "XBRL_ROLE_LINK",
  # HTML parsing
  "extract_structured_content",
  "generate_deterministic_uuid",
  "generate_lbug_docs",
  "generate_prefixed_ulid",
  "generate_robosystems_docs",
  # Documentation
  "generate_swagger_docs",
  # ULID utilities
  "generate_ulid",
  # UUID utilities
  "generate_uuid7",
  "get_timestamp_from_ulid",
  "parse_ulid",
  "save_structured_content",
]
