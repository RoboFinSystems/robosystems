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


def generate_ulid_hex(num_chars: int = 16) -> str:
  """
  Generate a ULID and convert it to hexadecimal format.

  This is specifically useful for graph IDs where we want time-ordered
  hex strings that prevent B-tree fragmentation.

  Args:
      num_chars: Number of hex characters to return (default: 16)

  Returns:
      Lowercase hex string of specified length
      Example: "018c5f9e8a2f4d1b"
  """
  ulid = ULID()
  ulid_int = int.from_bytes(ulid.bytes, byteorder="big")
  hex_str = hex(ulid_int)[2:]  # Remove '0x' prefix
  return hex_str[:num_chars]
