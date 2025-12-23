"""
Tests for UUID utilities.

Comprehensive test coverage for UUID generation, parsing, and timestamp extraction.
"""

import time
from unittest.mock import patch

from robosystems.utils.uuid import (
  create_prefixed_id,
  generate_deterministic_uuid7,
  generate_prefixed_uuid7,
  generate_uuid7,
  get_timestamp_from_uuid7,
  parse_uuid7,
)


class TestGenerateUuid7:
  """Test basic UUID v7 generation."""

  def test_generate_uuid7_format(self):
    """Test that generated UUID v7 has correct format."""
    uuid_str = generate_uuid7()

    # Should be 36 characters (32 hex + 4 hyphens)
    assert len(uuid_str) == 36

    # Should have proper hyphen placement
    parts = uuid_str.split("-")
    assert len(parts) == 5
    assert len(parts[0]) == 8  # time_hi
    assert len(parts[1]) == 4  # time_mid
    assert len(parts[2]) == 4  # time_low + version
    assert len(parts[3]) == 4  # clock_seq + variant
    assert len(parts[4]) == 12  # node

    # Should be version 7
    assert parts[2].startswith("7")

  def test_generate_uuid7_uniqueness(self):
    """Test that generated UUIDs are unique."""
    uuids = [generate_uuid7() for _ in range(100)]
    assert len(set(uuids)) == 100

  def test_generate_uuid7_ordering(self):
    """Test that UUID v7s are time-ordered."""
    uuid1 = generate_uuid7()
    time.sleep(0.001)  # Small delay to ensure timestamp difference
    uuid2 = generate_uuid7()

    # Extract timestamps and compare
    ts1 = get_timestamp_from_uuid7(uuid1)
    ts2 = get_timestamp_from_uuid7(uuid2)

    assert ts1 is not None
    assert ts2 is not None
    assert ts1 < ts2


class TestGeneratePrefixedUuid7:
  """Test prefixed UUID v7 generation."""

  def test_generate_prefixed_uuid7_format(self):
    """Test prefixed UUID format."""
    uuid_str = generate_prefixed_uuid7("user")

    assert uuid_str.startswith("user_")
    uuid_part = uuid_str[5:]  # Remove "user_" prefix

    # UUID part should be valid
    assert len(uuid_part) == 36
    parts = uuid_part.split("-")
    assert len(parts) == 5
    assert parts[2].startswith("7")

  def test_generate_prefixed_uuid7_different_prefixes(self):
    """Test different prefixes work correctly."""
    user_uuid = generate_prefixed_uuid7("user")
    doc_uuid = generate_prefixed_uuid7("doc")

    assert user_uuid.startswith("user_")
    assert doc_uuid.startswith("doc_")
    assert user_uuid != doc_uuid


class TestGenerateDeterministicUuid7:
  """Test deterministic UUID generation using UUID5 hashing."""

  def test_deterministic_uuid7_same_content(self):
    """Test that same content generates same UUID."""
    content = "test-entity-123"

    uuid1 = generate_deterministic_uuid7(content)
    uuid2 = generate_deterministic_uuid7(content)

    assert uuid1 == uuid2

  def test_deterministic_uuid7_different_content(self):
    """Test that different content generates different UUIDs."""
    uuid1 = generate_deterministic_uuid7("content-1")
    uuid2 = generate_deterministic_uuid7("content-2")

    assert uuid1 != uuid2

  def test_deterministic_uuid7_with_namespace(self):
    """Test deterministic UUIDs with namespaces."""
    content = "entity-123"

    uuid1 = generate_deterministic_uuid7(content, namespace="users")
    uuid2 = generate_deterministic_uuid7(content, namespace="docs")
    uuid3 = generate_deterministic_uuid7(content)  # No namespace

    # All should be different due to different namespaces
    assert uuid1 != uuid2
    assert uuid1 != uuid3
    assert uuid2 != uuid3

  def test_deterministic_uuid7_consistent_across_calls(self):
    """Test that the same input always produces the same UUID (truly deterministic)."""
    content = "consistent-entity"
    namespace = "test"

    # Multiple calls should always return the exact same value
    uuid1 = generate_deterministic_uuid7(content, namespace=namespace)
    uuid2 = generate_deterministic_uuid7(content, namespace=namespace)
    uuid3 = generate_deterministic_uuid7(content, namespace=namespace)

    assert uuid1 == uuid2 == uuid3

  def test_deterministic_uuid7_format(self):
    """Test that deterministic UUIDs have valid UUID5 format."""
    uuid_str = generate_deterministic_uuid7("test-content", namespace="test")

    # Should be 36 characters (32 hex + 4 hyphens)
    assert len(uuid_str) == 36

    # Should have proper hyphen placement
    parts = uuid_str.split("-")
    assert len(parts) == 5
    assert len(parts[0]) == 8
    assert len(parts[1]) == 4
    assert len(parts[2]) == 4
    assert len(parts[3]) == 4
    assert len(parts[4]) == 12

    # Should be version 5 (hash-based)
    assert parts[2].startswith("5")


class TestParseUuid7:
  """Test UUID v7 parsing and validation."""

  def test_parse_valid_uuid7(self):
    """Test parsing valid UUID v7."""
    # Generate a valid UUID v7
    uuid_str = generate_uuid7()
    parsed = parse_uuid7(uuid_str)

    assert parsed == uuid_str

  def test_parse_prefixed_uuid7(self):
    """Test parsing prefixed UUID v7."""
    prefixed_uuid = generate_prefixed_uuid7("user")
    parsed = parse_uuid7(prefixed_uuid)

    # Should extract UUID part without prefix
    expected_uuid = prefixed_uuid.split("_", 1)[1]
    assert parsed == expected_uuid

  def test_parse_invalid_uuid_format(self):
    """Test parsing invalid UUID formats."""
    invalid_uuids = [
      "invalid-uuid",
      "12345678-1234-1234-1234",  # Too short
      "12345678-1234-1234-1234-123456789012",  # Wrong segment lengths
      "12345678-1234-4234-1234-123456789012",  # Not version 7
      "",
      None,
    ]

    for invalid_uuid in invalid_uuids:
      if invalid_uuid is None:
        continue
      result = parse_uuid7(invalid_uuid)
      assert result is None

  def test_parse_uuid7_version_check(self):
    """Test that only version 7 UUIDs are accepted."""
    # Create a UUID v4 format string (not version 7)
    uuid_v4_format = "12345678-1234-4234-8234-123456789012"

    result = parse_uuid7(uuid_v4_format)
    assert result is None

  def test_parse_uuid7_exception_handling(self):
    """Test exception handling in parse_uuid7."""
    # Test with malformed input that might cause exceptions
    malformed_inputs = [
      "user_",  # Prefix with empty UUID part
      "user_invalid",  # Prefix with invalid UUID
    ]

    for malformed_input in malformed_inputs:
      result = parse_uuid7(malformed_input)
      assert result is None


class TestGetTimestampFromUuid7:
  """Test timestamp extraction from UUID v7."""

  def test_get_timestamp_from_valid_uuid7(self):
    """Test timestamp extraction from valid UUID v7."""
    before_time = int(time.time() * 1000)  # Current time in milliseconds
    uuid_str = generate_uuid7()
    after_time = int(time.time() * 1000) + 1000  # Add 1 second buffer

    timestamp = get_timestamp_from_uuid7(uuid_str)

    assert timestamp is not None
    # Allow for some timing variance
    assert before_time - 1000 <= timestamp <= after_time

  def test_get_timestamp_from_prefixed_uuid7(self):
    """Test timestamp extraction from prefixed UUID v7."""
    before_time = int(time.time() * 1000)
    prefixed_uuid = generate_prefixed_uuid7("user")
    after_time = int(time.time() * 1000) + 1000

    timestamp = get_timestamp_from_uuid7(prefixed_uuid)

    assert timestamp is not None
    assert before_time - 1000 <= timestamp <= after_time

  def test_get_timestamp_from_invalid_uuid(self):
    """Test timestamp extraction from invalid UUID."""
    invalid_uuids = [
      "invalid-uuid",
      "12345678-1234-4234-1234-123456789012",  # Not version 7
      "",
      "user_invalid-uuid",
    ]

    for invalid_uuid in invalid_uuids:
      timestamp = get_timestamp_from_uuid7(invalid_uuid)
      assert timestamp is None

  def test_get_timestamp_hex_conversion_error(self):
    """Test handling of hex conversion errors."""
    # Mock parse_uuid7 to return a UUID with invalid hex characters
    with patch("robosystems.utils.uuid.parse_uuid7") as mock_parse:
      mock_parse.return_value = "XXXXXXXX-XXXX-7XXX-XXXX-XXXXXXXXXXXX"

      timestamp = get_timestamp_from_uuid7("test-uuid")
      assert timestamp is None


class TestCreatePrefixedId:
  """Test prefixed ID creation utility."""

  def test_create_prefixed_id_format(self):
    """Test prefixed ID format."""
    prefix = "user"
    content = "john@example.com"

    prefixed_id = create_prefixed_id(prefix, content)

    assert prefixed_id.startswith(f"{prefix}_")

    # UUID part should be valid (36 chars)
    uuid_part = prefixed_id.split("_", 1)[1]
    assert len(uuid_part) == 36

  def test_create_prefixed_id_deterministic(self):
    """Test that prefixed IDs are deterministic."""
    prefix = "doc"
    content = "document-123"

    id1 = create_prefixed_id(prefix, content)
    id2 = create_prefixed_id(prefix, content)

    assert id1 == id2

  def test_create_prefixed_id_different_prefixes(self):
    """Test different prefixes with same content."""
    content = "entity-123"

    user_id = create_prefixed_id("user", content)
    doc_id = create_prefixed_id("doc", content)

    assert user_id != doc_id
    assert user_id.startswith("user_")
    assert doc_id.startswith("doc_")


class TestEdgeCases:
  """Test edge cases and error conditions."""

  def test_empty_string_content(self):
    """Test handling of empty string content."""
    uuid_str = generate_deterministic_uuid7("")
    assert uuid_str is not None
    assert len(uuid_str) == 36

  def test_unicode_content(self):
    """Test handling of unicode content."""
    unicode_content = "用户-123-测试"
    uuid_str = generate_deterministic_uuid7(unicode_content)
    assert uuid_str is not None
    assert len(uuid_str) == 36

  def test_very_long_content(self):
    """Test handling of very long content."""
    long_content = "x" * 10000
    uuid_str = generate_deterministic_uuid7(long_content)
    assert uuid_str is not None
    assert len(uuid_str) == 36

  def test_special_characters_in_prefix(self):
    """Test handling of special characters in prefix."""
    prefixed_uuid = generate_prefixed_uuid7("user-test")
    assert prefixed_uuid.startswith("user-test_")

    prefixed_id = create_prefixed_id("doc.v2", "content")
    assert prefixed_id.startswith("doc.v2_")
