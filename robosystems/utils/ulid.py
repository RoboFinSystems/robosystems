"""
ULID (Universally Unique Lexicographically Sortable Identifier) utilities.

ULIDs provide time-ordered unique identifiers that solve B-tree index fragmentation
issues in PostgreSQL, especially for high-volume tables like transactions and logs.

Benefits over UUIDs:
- Lexicographically sortable (time-ordered)
- Better B-tree index performance
- Natural chronological ordering
- Still globally unique
"""

from typing import Optional
from ulid import ULID


def generate_ulid() -> str:
  """
  Generate a time-ordered ULID for database primary keys.

  Returns:
      A string representation of a ULID (26 characters).
      Example: "01ARZ3NDEKTSV4RRFFQ69G5FAV"
  """
  return str(ULID())


def generate_prefixed_ulid(prefix: str) -> str:
  """
  Generate a prefixed ULID for better readability and type identification.

  Args:
      prefix: A short prefix to identify the record type

  Returns:
      A prefixed ULID string.
      Example: "txn_01ARZ3NDEKTSV4RRFFQ69G5FAV"
  """
  return f"{prefix}_{ULID()}"


def parse_ulid(ulid_str: str) -> Optional[ULID]:
  """
  Parse a ULID string back to a ULID object.

  Args:
      ulid_str: The ULID string to parse (with or without prefix)

  Returns:
      A ULID object if valid, None otherwise
  """
  try:
    # Handle prefixed ULIDs
    if "_" in ulid_str:
      ulid_str = ulid_str.split("_", 1)[1]
    return ULID.from_str(ulid_str)
  except (ValueError, IndexError):
    return None


def get_timestamp_from_ulid(ulid_str: str) -> Optional[float]:
  """
  Extract the timestamp from a ULID string.

  Args:
      ulid_str: The ULID string (with or without prefix)

  Returns:
      Unix timestamp in seconds if valid, None otherwise
  """
  ulid_obj = parse_ulid(ulid_str)
  if ulid_obj:
    return ulid_obj.timestamp
  return None


# Default generator functions for SQLAlchemy column defaults
def default_ulid() -> str:
  """Default function for SQLAlchemy columns."""
  return generate_ulid()


def default_transaction_ulid() -> str:
  """Default function for transaction tables."""
  return generate_prefixed_ulid("txn")


def default_usage_ulid() -> str:
  """Default function for usage tracking tables."""
  return generate_prefixed_ulid("usg")


def default_credit_ulid() -> str:
  """Default function for credit records."""
  return generate_prefixed_ulid("crd")
