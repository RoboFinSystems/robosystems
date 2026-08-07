"""
UUID generation for RoboSystems.

Two shapes, for two needs. UUID5 hashes namespace + content, so the same input
always yields the same ID — that is what lets XBRL entities keep stable IDs
across parallel workers and repeated pipeline runs. UUID7 is time-ordered, for
cases where insertion order matters (see also `ulid.py`).
"""

import uuid

from uuid6 import uuid7

# Custom namespace UUID for RoboSystems deterministic ID generation
# This ensures our UUIDs don't collide with other systems using UUID5
ROBOSYSTEMS_NAMESPACE = uuid.UUID("a1b2c3d4-e5f6-7890-abcd-ef1234567890")


def generate_uuid7() -> str:
  """Generate a time-ordered UUID v7 string (36 chars, hyphenated)."""
  return str(uuid7())


def generate_deterministic_uuid(content: str, namespace: str | None = None) -> str:
  """Derive a UUID5 from `content`, identical for identical inputs.

  `namespace` is prepended to the content so the same source string used for
  two different entity types yields two different IDs.
  """
  # Combine namespace with content to prevent collisions between entity types
  # e.g., "entity:https://sec.gov/..." vs "element:https://sec.gov/..."
  full_content = f"{namespace}:{content}" if namespace else content

  # Generate UUID5 using our custom namespace - truly deterministic
  deterministic_id = uuid.uuid5(ROBOSYSTEMS_NAMESPACE, full_content)
  return str(deterministic_id)
