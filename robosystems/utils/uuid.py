"""
UUID Utilities for Optimal Database Performance

UUIDv7 provides time-ordered UUIDs that solve the performance problems
with random UUIDs or MD5 hashes as primary keys. They maintain:
- Sequential ordering (better index performance)
- Better cache locality (fewer cache misses)
- Efficient B-tree insertions (no random page splits)

UUID5 provides truly deterministic UUIDs based on namespace + content hashing.
Used for entities that need consistent IDs across pipeline runs and workers.

Performance comparison:
- MD5 hash: Random 32-char hex strings, worst performance
- UUID v4: Random 128-bit values, poor locality
- UUID v5: Deterministic, hash-based, consistent across runs
- UUID v7: Time-ordered, sequential, optimal for indexes
"""

import uuid

from uuid6 import uuid7

# Custom namespace UUID for RoboSystems deterministic ID generation
# This ensures our UUIDs don't collide with other systems using UUID5
ROBOSYSTEMS_NAMESPACE = uuid.UUID("a1b2c3d4-e5f6-7890-abcd-ef1234567890")


def generate_uuid7() -> str:
  """
  Generate a time-ordered UUID v7 string.

  UUIDv7 format:
  - First 48 bits: Unix timestamp in milliseconds
  - Next 12 bits: Random or counter
  - Last 64 bits: Random

  This ensures:
  - Sequential ordering by creation time
  - No index fragmentation
  - Optimal B-tree performance

  Returns:
      str: UUID v7 string (36 chars with hyphens)
      Example: "0198d51e-3767-70c8-8a4e-6a3f304fbe85"
  """
  return str(uuid7())


def generate_prefixed_uuid7(prefix: str) -> str:
  """
  Generate a prefixed UUID v7 for better readability and type identification.

  Args:
      prefix: A short prefix to identify the record type

  Returns:
      str: Prefixed UUID v7 string
      Example: "user_0198d51e-3767-70c8-8a4e-6a3f304fbe85"
  """
  return f"{prefix}_{uuid7()}"


def generate_deterministic_uuid7(content: str, namespace: str | None = None) -> str:
  """
  Generate a truly deterministic UUID based on content using UUID5.

  Uses SHA-1 hashing of a namespace UUID + content to produce the same
  UUID every time for the same input, regardless of process, worker,
  or pipeline run. This ensures consistent entity identification across
  parallel processing and multiple pipeline executions.

  Args:
      content: String to generate ID from
      namespace: Optional namespace to prevent collisions between entity types

  Returns:
      str: Deterministic UUID5 string (always the same for same inputs)
  """
  # Combine namespace with content to prevent collisions between entity types
  # e.g., "entity:https://sec.gov/..." vs "element:https://sec.gov/..."
  full_content = f"{namespace}:{content}" if namespace else content

  # Generate UUID5 using our custom namespace - truly deterministic
  deterministic_id = uuid.uuid5(ROBOSYSTEMS_NAMESPACE, full_content)
  return str(deterministic_id)


def parse_uuid7(uuid_str: str) -> str | None:
  """
  Parse and validate a UUID v7 string.

  Args:
      uuid_str: The UUID string to parse (with or without prefix)

  Returns:
      The UUID portion if valid, None otherwise
  """
  try:
    # Handle prefixed UUIDs
    if "_" in uuid_str:
      uuid_str = uuid_str.split("_", 1)[1]

    # Basic UUID format validation (8-4-4-4-12 hex digits)
    parts = uuid_str.split("-")
    if len(parts) == 5:
      if (
        len(parts[0]) == 8
        and len(parts[1]) == 4
        and len(parts[2]) == 4
        and len(parts[3]) == 4
        and len(parts[4]) == 12
      ):
        # Check if it's UUID v7 (version bits)
        if parts[2].startswith("7"):
          return uuid_str
  except (ValueError, IndexError):
    pass
  return None


def get_timestamp_from_uuid7(uuid_str: str) -> int | None:
  """
  Extract the timestamp from a UUID v7 string.

  Args:
      uuid_str: The UUID v7 string (with or without prefix)

  Returns:
      Unix timestamp in milliseconds if valid, None otherwise
  """
  parsed = parse_uuid7(uuid_str)
  if parsed:
    # Extract first 48 bits (12 hex chars) as timestamp
    timestamp_hex = parsed.replace("-", "")[:12]
    try:
      return int(timestamp_hex, 16)
    except ValueError:
      pass
  return None


def create_prefixed_id(prefix: str, content: str) -> str:
  """
  Create a prefixed deterministic ID for any entity type.

  This is a generic helper that can be used by any module.

  Args:
      prefix: Entity type prefix (e.g., "user", "doc", "txn")
      content: Unique content to generate ID from

  Returns:
      str: Prefixed deterministic UUID v7
      Example: "user_0198d51e-3767-70c8-8a4e-6a3f304fbe85"
  """
  return f"{prefix}_{generate_deterministic_uuid7(content, namespace=prefix)}"
