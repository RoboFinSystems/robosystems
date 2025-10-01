"""
Tests for ULID utilities.

Comprehensive test coverage for ULID generation, parsing, and timestamp extraction.
"""

import time

from robosystems.utils.ulid import (
  generate_ulid,
  generate_prefixed_ulid,
  parse_ulid,
  get_timestamp_from_ulid,
  default_ulid,
  default_transaction_ulid,
  default_usage_ulid,
  default_credit_ulid,
)
from ulid import ULID


class TestGenerateUlid:
  """Test basic ULID generation."""

  def test_generate_ulid_format(self):
    """Test that generated ULID has correct format."""
    ulid_str = generate_ulid()

    # Should be 26 characters
    assert len(ulid_str) == 26

    # Should be all uppercase alphanumeric (Crockford's Base32)
    assert ulid_str.isalnum()
    assert ulid_str.isupper()

  def test_generate_ulid_uniqueness(self):
    """Test that generated ULIDs are unique."""
    ulids = [generate_ulid() for _ in range(100)]
    assert len(set(ulids)) == 100

  def test_generate_ulid_ordering(self):
    """Test that ULIDs are time-ordered."""
    ulid1 = generate_ulid()
    time.sleep(0.001)  # Small delay to ensure timestamp difference
    ulid2 = generate_ulid()

    # ULIDs should be lexicographically sortable by time
    assert ulid1 < ulid2

  def test_generate_ulid_timestamp_precision(self):
    """Test ULID timestamp precision."""
    before_time = time.time()
    ulid_str = generate_ulid()
    after_time = time.time() + 1.0  # Add 1 second buffer

    # Extract timestamp from ULID
    timestamp = get_timestamp_from_ulid(ulid_str)

    assert timestamp is not None
    # Allow for some timing variance
    assert before_time - 1.0 <= timestamp <= after_time


class TestGeneratePrefixedUlid:
  """Test prefixed ULID generation."""

  def test_generate_prefixed_ulid_format(self):
    """Test prefixed ULID format."""
    ulid_str = generate_prefixed_ulid("txn")

    assert ulid_str.startswith("txn_")
    ulid_part = ulid_str[4:]  # Remove "txn_" prefix

    # ULID part should be valid
    assert len(ulid_part) == 26
    assert ulid_part.isalnum()
    assert ulid_part.isupper()

  def test_generate_prefixed_ulid_different_prefixes(self):
    """Test different prefixes work correctly."""
    txn_ulid = generate_prefixed_ulid("txn")
    usg_ulid = generate_prefixed_ulid("usg")

    assert txn_ulid.startswith("txn_")
    assert usg_ulid.startswith("usg_")
    assert txn_ulid != usg_ulid

  def test_generate_prefixed_ulid_uniqueness(self):
    """Test that prefixed ULIDs are unique."""
    ulids = [generate_prefixed_ulid("test") for _ in range(100)]
    assert len(set(ulids)) == 100


class TestParseUlid:
  """Test ULID parsing and validation."""

  def test_parse_valid_ulid(self):
    """Test parsing valid ULID."""
    ulid_str = generate_ulid()
    parsed = parse_ulid(ulid_str)

    assert parsed is not None
    assert isinstance(parsed, ULID)
    assert str(parsed) == ulid_str

  def test_parse_prefixed_ulid(self):
    """Test parsing prefixed ULID."""
    prefixed_ulid = generate_prefixed_ulid("txn")
    parsed = parse_ulid(prefixed_ulid)

    assert parsed is not None
    assert isinstance(parsed, ULID)

    # Should extract ULID part without prefix
    expected_ulid = prefixed_ulid.split("_", 1)[1]
    assert str(parsed) == expected_ulid

  def test_parse_invalid_ulid_format(self):
    """Test parsing invalid ULID formats."""
    invalid_ulids = [
      "invalid-ulid",
      "TOO_SHORT",
      "THIS_IS_TOO_LONG_FOR_A_ULID_STRING",
      "01ARZ3NDEKTSV4RRFFQ69G5FA",  # 25 chars, too short
      "01ARZ3NDEKTSV4RRFFQ69G5FAVV",  # 27 chars, too long
      "",
      "lowercase_ulid_not_valid",
    ]

    for invalid_ulid in invalid_ulids:
      result = parse_ulid(invalid_ulid)
      assert result is None

  def test_parse_ulid_with_invalid_characters(self):
    """Test parsing ULID with invalid characters."""
    # ULIDs use Crockford's Base32, which excludes I, L, O, U
    invalid_chars_ulid = "01ARZ3NDEKTSV4RRFFQ69G5ILO"  # Contains I, L, O

    result = parse_ulid(invalid_chars_ulid)
    assert result is None

  def test_parse_ulid_exception_handling(self):
    """Test exception handling in parse_ulid."""
    # Test with malformed input that might cause exceptions
    malformed_inputs = [
      "txn_",  # Prefix with empty ULID part
      "txn_INVALID",  # Prefix with invalid ULID
      "01ARZ3NDEKTSV4RRFFQ69G5F@V",  # Invalid character @
    ]

    for malformed_input in malformed_inputs:
      result = parse_ulid(malformed_input)
      assert result is None


class TestGetTimestampFromUlid:
  """Test timestamp extraction from ULID."""

  def test_get_timestamp_from_valid_ulid(self):
    """Test timestamp extraction from valid ULID."""
    before_time = time.time()
    ulid_str = generate_ulid()
    after_time = time.time() + 1.0

    timestamp = get_timestamp_from_ulid(ulid_str)

    assert timestamp is not None
    assert before_time - 1.0 <= timestamp <= after_time

  def test_get_timestamp_from_prefixed_ulid(self):
    """Test timestamp extraction from prefixed ULID."""
    before_time = time.time()
    prefixed_ulid = generate_prefixed_ulid("txn")
    after_time = time.time() + 1.0

    timestamp = get_timestamp_from_ulid(prefixed_ulid)

    assert timestamp is not None
    assert before_time - 1.0 <= timestamp <= after_time

  def test_get_timestamp_from_invalid_ulid(self):
    """Test timestamp extraction from invalid ULID."""
    invalid_ulids = [
      "invalid-ulid",
      "TOO_SHORT",
      "",
      "txn_invalid-ulid",
    ]

    for invalid_ulid in invalid_ulids:
      timestamp = get_timestamp_from_ulid(invalid_ulid)
      assert timestamp is None

  def test_get_timestamp_precision(self):
    """Test timestamp precision and consistency."""
    ulid_str = generate_ulid()
    ulid_obj = parse_ulid(ulid_str)

    # Both methods should return the same timestamp
    timestamp1 = get_timestamp_from_ulid(ulid_str)
    timestamp2 = ulid_obj.timestamp

    assert timestamp1 == timestamp2


class TestDefaultGenerators:
  """Test default generator functions for SQLAlchemy."""

  def test_default_ulid(self):
    """Test default ULID generator."""
    ulid_str = default_ulid()

    assert isinstance(ulid_str, str)
    assert len(ulid_str) == 26
    assert parse_ulid(ulid_str) is not None

  def test_default_transaction_ulid(self):
    """Test default transaction ULID generator."""
    txn_ulid = default_transaction_ulid()

    assert txn_ulid.startswith("txn_")
    ulid_part = txn_ulid[4:]
    assert len(ulid_part) == 26
    assert parse_ulid(txn_ulid) is not None

  def test_default_usage_ulid(self):
    """Test default usage ULID generator."""
    usage_ulid = default_usage_ulid()

    assert usage_ulid.startswith("usg_")
    ulid_part = usage_ulid[4:]
    assert len(ulid_part) == 26
    assert parse_ulid(usage_ulid) is not None

  def test_default_credit_ulid(self):
    """Test default credit ULID generator."""
    credit_ulid = default_credit_ulid()

    assert credit_ulid.startswith("crd_")
    ulid_part = credit_ulid[4:]
    assert len(ulid_part) == 26
    assert parse_ulid(credit_ulid) is not None

  def test_default_generators_uniqueness(self):
    """Test that default generators produce unique values."""
    # Test each generator multiple times
    generators = [
      default_ulid,
      default_transaction_ulid,
      default_usage_ulid,
      default_credit_ulid,
    ]

    for generator in generators:
      ulids = [generator() for _ in range(50)]
      assert len(set(ulids)) == 50  # All should be unique

  def test_default_generators_ordering(self):
    """Test that default generators produce time-ordered values."""
    generators = [
      default_ulid,
      default_transaction_ulid,
      default_usage_ulid,
      default_credit_ulid,
    ]

    for generator in generators:
      ulid1 = generator()
      time.sleep(0.001)  # Small delay
      ulid2 = generator()

      # Should be lexicographically ordered
      assert ulid1 < ulid2


class TestEdgeCases:
  """Test edge cases and error conditions."""

  def test_ulid_case_sensitivity(self):
    """Test ULID case sensitivity."""
    ulid_str = generate_ulid()
    lower_ulid = ulid_str.lower()

    # Original should parse successfully
    assert parse_ulid(ulid_str) is not None

    # Lowercase version should not parse (ULIDs are uppercase)
    assert parse_ulid(lower_ulid) is None

  def test_mixed_case_prefixed_ulid(self):
    """Test parsing mixed case prefixed ULID."""
    # Generate a valid prefixed ULID
    txn_ulid = generate_prefixed_ulid("txn")

    # Create a lowercase ULID part
    prefix, ulid_part = txn_ulid.split("_", 1)
    mixed_case_ulid = f"{prefix}_{ulid_part.lower()}"

    # Should not parse due to lowercase ULID part
    assert parse_ulid(mixed_case_ulid) is None

  def test_very_long_prefix(self):
    """Test handling of very long prefix."""
    long_prefix = "x" * 100
    prefixed_ulid = generate_prefixed_ulid(long_prefix)

    assert prefixed_ulid.startswith(f"{long_prefix}_")

    # Should still parse correctly
    parsed = parse_ulid(prefixed_ulid)
    assert parsed is not None

  def test_special_characters_in_prefix(self):
    """Test handling of special characters in prefix."""
    special_prefix = "txn-v2.1"
    prefixed_ulid = generate_prefixed_ulid(special_prefix)

    assert prefixed_ulid.startswith(f"{special_prefix}_")

    # Should still parse correctly
    parsed = parse_ulid(prefixed_ulid)
    assert parsed is not None

  def test_multiple_underscores_in_prefix(self):
    """Test handling of multiple underscores in prefix."""
    # Prefix with underscore
    prefix_with_underscore = "test_prefix"
    prefixed_ulid = generate_prefixed_ulid(prefix_with_underscore)

    # Should use the first underscore as separator (implementation detail)
    assert prefixed_ulid.startswith(f"{prefix_with_underscore}_")

    # When parsing, the implementation splits on first underscore
    # So "test_prefix_ULID" becomes "prefix_ULID" which is invalid
    # This is expected behavior - prefixes with underscores may not parse correctly
    parsed = parse_ulid(prefixed_ulid)
    # Since the extracted part "prefix_ULID" is not a valid ULID, this should be None
    assert parsed is None


class TestPerformanceAndComparisons:
  """Test performance characteristics and comparisons."""

  def test_ulid_lexicographic_sorting(self):
    """Test that ULIDs sort lexicographically by time."""
    # Generate ULIDs with small delays
    ulids = []
    for _ in range(10):
      ulids.append(generate_ulid())
      time.sleep(0.001)

    # Sort lexicographically
    sorted_ulids = sorted(ulids)

    # Should be in the same order as generated
    assert ulids == sorted_ulids

  def test_ulid_vs_uuid_ordering(self):
    """Test ULID ordering properties."""
    # Generate a batch of ULIDs
    batch1 = [generate_ulid() for _ in range(5)]
    time.sleep(0.01)  # Longer delay
    batch2 = [generate_ulid() for _ in range(5)]

    # All ULIDs in batch2 should be greater than all in batch1
    for ulid1 in batch1:
      for ulid2 in batch2:
        assert ulid1 < ulid2

  def test_ulid_prefix_performance(self):
    """Test that prefixed ULIDs maintain ordering within same prefix."""
    # Generate prefixed ULIDs with delays
    prefixed_ulids = []
    for _ in range(10):
      prefixed_ulids.append(generate_prefixed_ulid("txn"))
      time.sleep(0.001)

    # Extract ULID parts and verify ordering
    ulid_parts = [ulid.split("_", 1)[1] for ulid in prefixed_ulids]
    sorted_parts = sorted(ulid_parts)

    assert ulid_parts == sorted_parts
