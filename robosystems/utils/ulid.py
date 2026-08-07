"""
ULID (Universally Unique Lexicographically Sortable Identifier) utilities.

ULIDs are globally unique like UUIDs but sort lexicographically by creation
time. Preferred over random UUIDs for PostgreSQL primary keys on high-volume
tables: inserts land at the end of the B-tree index instead of scattering
across it, which avoids the page fragmentation random keys cause.
"""

from ulid import ULID


def generate_ulid() -> str:
  """Generate a 26-character ULID, e.g. "01ARZ3NDEKTSV4RRFFQ69G5FAV"."""
  return str(ULID())


def generate_prefixed_ulid(prefix: str) -> str:
  """Generate a record-type-tagged ULID, e.g. "txn_01ARZ3NDEKTSV4RRFFQ69G5FAV"."""
  return f"{prefix}_{ULID()}"


def parse_ulid(ulid_str: str) -> ULID | None:
  """Parse a ULID string, with or without a prefix. None if malformed."""
  try:
    # Handle prefixed ULIDs
    if "_" in ulid_str:
      ulid_str = ulid_str.split("_", 1)[1]
    return ULID.from_str(ulid_str)
  except (ValueError, IndexError):
    return None


def get_timestamp_from_ulid(ulid_str: str) -> float | None:
  """Unix timestamp in seconds encoded in a ULID. None if malformed."""
  ulid_obj = parse_ulid(ulid_str)
  if ulid_obj:
    return ulid_obj.timestamp
  return None


def generate_ulid_hex(num_chars: int = 16) -> str:
  """Generate a ULID as a lowercase hex string, e.g. "018c5f9e8a2f4d1b".

  Used for graph IDs, which need the time ordering of a ULID in a hex-only
  alphabet. Truncating to `num_chars` keeps the leading timestamp bits and
  trades random bits for length, so shorter values collide sooner.
  """
  ulid = ULID()
  ulid_int = int.from_bytes(ulid.bytes, byteorder="big")
  hex_str = hex(ulid_int)[2:]  # Remove '0x' prefix
  return hex_str[:num_chars]
