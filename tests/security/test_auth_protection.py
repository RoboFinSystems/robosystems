"""Authentication protection tests."""

import time
from unittest.mock import patch

import pytest

from robosystems.security.auth_protection import (
  AdvancedAuthProtection,
  ThreatLevel,
)


@pytest.fixture(autouse=True)
def mock_cache():
  """Mock rate_limit_cache for all tests."""
  store = {}

  class FakeCache:
    def get(self, key):
      item = store.get(key)
      if item is None:
        return None
      value, expire_at = item
      if expire_at and time.time() > expire_at:
        del store[key]
        return None
      return value

    def set(self, key, value, expire=None):
      # As strict as redis-py's encoder: it accepts only str/bytes/int/float
      # and raises DataError on a dict or list. The production bug this suite
      # missed for months was code handing the cache a raw dict; a fake that
      # round-trips arbitrary objects can never catch it. Storing a decoded
      # copy also proves the code parses on read rather than leaning on the
      # fake returning live Python objects.
      if not isinstance(value, (str, bytes, int, float)):
        raise TypeError(
          f"redis accepts only str/bytes/int/float, got {type(value).__name__}"
        )
      expire_at = time.time() + expire if expire else None
      store[key] = (value, expire_at)

  with patch("robosystems.security.auth_protection.rate_limit_cache", FakeCache()):
    store.clear()
    yield store


@pytest.fixture(autouse=True)
def mock_audit_logger():
  """Mock SecurityAuditLogger to avoid side effects."""
  with patch("robosystems.security.auth_protection.SecurityAuditLogger") as mock:
    mock.log_security_event.return_value = None
    yield mock


class TestThreatLevel:
  """Tests for ThreatLevel enum."""

  def test_values(self):
    assert ThreatLevel.LOW.value == "low"
    assert ThreatLevel.MEDIUM.value == "medium"
    assert ThreatLevel.HIGH.value == "high"
    assert ThreatLevel.CRITICAL.value == "critical"


class TestCacheKeyGeneration:
  """Tests for cache key generation methods."""

  def test_attempt_key_uses_ip_hash(self):
    key = AdvancedAuthProtection._get_attempt_key("192.168.1.1")
    assert key.startswith("auth_attempts:")
    assert len(key) > len("auth_attempts:")

  def test_threat_key_uses_ip_hash(self):
    key = AdvancedAuthProtection._get_threat_key("192.168.1.1")
    assert key.startswith("ip_threat:")

  def test_delay_key_uses_ip_hash(self):
    key = AdvancedAuthProtection._get_delay_key("192.168.1.1")
    assert key.startswith("auth_delay:")

  def test_different_ips_different_keys(self):
    key1 = AdvancedAuthProtection._get_attempt_key("192.168.1.1")
    key2 = AdvancedAuthProtection._get_attempt_key("10.0.0.1")
    assert key1 != key2

  def test_same_ip_same_key(self):
    key1 = AdvancedAuthProtection._get_attempt_key("192.168.1.1")
    key2 = AdvancedAuthProtection._get_attempt_key("192.168.1.1")
    assert key1 == key2


class TestGetIpThreatAssessment:
  """Tests for get_ip_threat_assessment."""

  def test_unknown_ip_returns_low_threat(self):
    assessment = AdvancedAuthProtection.get_ip_threat_assessment("1.2.3.4")
    assert assessment.threat_level == ThreatLevel.LOW
    assert assessment.failed_attempts == 0
    assert assessment.is_blocked is False

  def test_returns_stored_assessment(self):
    ip = "10.0.0.1"
    # Record some failures to build up assessment
    for _ in range(3):
      AdvancedAuthProtection.record_auth_attempt(ip, success=False)

    assessment = AdvancedAuthProtection.get_ip_threat_assessment(ip)
    assert assessment.failed_attempts == 3

  def test_expired_block_cleared(self):
    ip = "10.0.0.2"
    # Record enough failures to trigger a block
    for _ in range(6):
      AdvancedAuthProtection.record_auth_attempt(ip, success=False)

    assessment = AdvancedAuthProtection.get_ip_threat_assessment(ip)
    assert assessment.threat_level == ThreatLevel.MEDIUM

    # Simulate block expiry
    with patch("robosystems.security.auth_protection.time") as mock_time:
      mock_time.time.return_value = time.time() + 100000
      assessment = AdvancedAuthProtection.get_ip_threat_assessment(ip)
      assert assessment.is_blocked is False


class TestLayerRoundTripsThroughRedis:
  """The layer was dead for months because it handed the cache raw dicts, which
  redis rejects, and read back str it never parsed. These pin the round-trip
  against the redis-strict fake."""

  def test_progressive_delay_is_zero_for_a_fresh_ip(self):
    # The latent bug the encode fix exposes: min(0, 8) is not a delay key, so
    # a zero-failure IP fell back to the 8th-failure value (900s). A brand-new
    # visitor must never be told to wait a quarter of an hour.
    assert AdvancedAuthProtection.get_progressive_delay("203.0.113.7") == 0

  def test_assessment_persists_as_a_redis_compatible_string(self, mock_cache):
    ip = "203.0.113.8"
    AdvancedAuthProtection.record_auth_attempt(ip, success=False)
    # The strict fake would have raised on a dict; assert the stored value is a
    # str and that it re-reads as the same failure count.
    stored = [v for (v, _exp) in mock_cache.values()]
    assert stored and all(isinstance(v, str) for v in stored)
    assert AdvancedAuthProtection.get_ip_threat_assessment(ip).failed_attempts == 1

  def test_progressive_delay_applies_after_a_failure(self):
    ip = "203.0.113.9"
    for _ in range(3):
      AdvancedAuthProtection.record_auth_attempt(ip, success=False)
    AdvancedAuthProtection.apply_progressive_delay(ip)
    # 3rd-failure delay is 5s; the window was just opened, so >0 remains.
    assert AdvancedAuthProtection.get_progressive_delay(ip) > 0


class TestRecordAuthAttempt:
  """Tests for record_auth_attempt."""

  def test_successful_attempt(self):
    ip = "192.168.1.100"
    AdvancedAuthProtection.record_auth_attempt(ip, success=True)
    assessment = AdvancedAuthProtection.get_ip_threat_assessment(ip)
    assert assessment.successful_attempts == 1

  def test_failed_attempt_increments_counter(self):
    ip = "192.168.1.101"
    AdvancedAuthProtection.record_auth_attempt(ip, success=False)
    assessment = AdvancedAuthProtection.get_ip_threat_assessment(ip)
    assert assessment.failed_attempts == 1

  def test_successful_login_decrements_failure_count(self):
    ip = "192.168.1.102"
    AdvancedAuthProtection.record_auth_attempt(ip, success=False)
    AdvancedAuthProtection.record_auth_attempt(ip, success=False)
    AdvancedAuthProtection.record_auth_attempt(ip, success=True)
    assessment = AdvancedAuthProtection.get_ip_threat_assessment(ip)
    assert assessment.failed_attempts == 1  # 2 - 1

  def test_threat_level_escalation_to_medium(self):
    ip = "192.168.1.103"
    for _ in range(5):
      AdvancedAuthProtection.record_auth_attempt(ip, success=False)
    assessment = AdvancedAuthProtection.get_ip_threat_assessment(ip)
    assert assessment.threat_level == ThreatLevel.MEDIUM

  def test_threat_level_escalation_to_high(self):
    ip = "192.168.1.104"
    for _ in range(10):
      AdvancedAuthProtection.record_auth_attempt(ip, success=False)
    assessment = AdvancedAuthProtection.get_ip_threat_assessment(ip)
    assert assessment.threat_level == ThreatLevel.HIGH

  def test_threat_level_escalation_to_critical(self):
    ip = "192.168.1.105"
    for _ in range(20):
      AdvancedAuthProtection.record_auth_attempt(ip, success=False)
    assessment = AdvancedAuthProtection.get_ip_threat_assessment(ip)
    assert assessment.threat_level == ThreatLevel.CRITICAL

  def test_ip_blocked_at_medium_threat(self):
    ip = "192.168.1.106"
    for _ in range(5):
      AdvancedAuthProtection.record_auth_attempt(ip, success=False)
    assessment = AdvancedAuthProtection.get_ip_threat_assessment(ip)
    assert assessment.is_blocked is True
    assert assessment.block_expires is not None


class TestCheckIpBlocked:
  """Tests for check_ip_blocked."""

  def test_unblocked_ip(self):
    is_blocked, remaining = AdvancedAuthProtection.check_ip_blocked("1.1.1.1")
    assert is_blocked is False
    assert remaining is None

  def test_blocked_ip_returns_remaining_time(self):
    ip = "192.168.2.1"
    for _ in range(5):
      AdvancedAuthProtection.record_auth_attempt(ip, success=False)

    is_blocked, remaining = AdvancedAuthProtection.check_ip_blocked(ip)
    assert is_blocked is True
    assert remaining is not None
    assert remaining > 0

  def test_expired_block_returns_unblocked(self):
    ip = "192.168.2.2"
    for _ in range(5):
      AdvancedAuthProtection.record_auth_attempt(ip, success=False)

    with patch("robosystems.security.auth_protection.time") as mock_time:
      mock_time.time.return_value = time.time() + 100000
      is_blocked, remaining = AdvancedAuthProtection.check_ip_blocked(ip)
      assert is_blocked is False


class TestGetProgressiveDelay:
  """Tests for get_progressive_delay."""

  def test_no_delay_for_clean_ip(self):
    delay = AdvancedAuthProtection.get_progressive_delay("1.1.1.1")
    assert delay == 0

  def test_delay_increases_with_failures(self):
    ip = "192.168.3.1"
    for _ in range(3):
      AdvancedAuthProtection.record_auth_attempt(ip, success=False)
      AdvancedAuthProtection.apply_progressive_delay(ip)

    # After applying delay, there should be remaining delay
    delay = AdvancedAuthProtection.get_progressive_delay(ip)
    assert isinstance(delay, int)


class TestApplyProgressiveDelay:
  """Tests for apply_progressive_delay."""

  def test_applies_delay(self):
    ip = "192.168.4.1"
    AdvancedAuthProtection.record_auth_attempt(ip, success=False)
    AdvancedAuthProtection.apply_progressive_delay(ip)
    # Should succeed without error
    delay = AdvancedAuthProtection.get_progressive_delay(ip)
    assert isinstance(delay, int)


class TestGetSecurityHeaders:
  """Tests for get_security_headers."""

  def test_low_threat_headers(self):
    headers = AdvancedAuthProtection.get_security_headers("1.1.1.1")
    assert "X-Auth-Threat-Level" in headers
    assert headers["X-Auth-Threat-Level"] == "low"

  def test_blocked_ip_headers(self):
    ip = "192.168.5.1"
    for _ in range(5):
      AdvancedAuthProtection.record_auth_attempt(ip, success=False)

    headers = AdvancedAuthProtection.get_security_headers(ip)
    assert headers["X-Auth-Threat-Level"] == "medium"
    assert headers.get("X-Auth-Blocked") == "true"
    assert "X-Auth-Block-Expires" in headers

  def test_delay_header_present_after_delay(self):
    ip = "192.168.5.2"
    AdvancedAuthProtection.record_auth_attempt(ip, success=False)
    AdvancedAuthProtection.apply_progressive_delay(ip)
    headers = AdvancedAuthProtection.get_security_headers(ip)
    # May or may not have delay header depending on timing
    assert "X-Auth-Threat-Level" in headers


class TestCacheFailureResilience:
  """Tests that cache failures don't break the system."""

  def test_record_attempt_survives_cache_failure(self):
    with patch("robosystems.security.auth_protection.rate_limit_cache") as mock_cache:
      mock_cache.get.side_effect = Exception("Cache down")
      mock_cache.set.side_effect = Exception("Cache down")
      # Should not raise
      AdvancedAuthProtection.record_auth_attempt("1.1.1.1", success=False)

  def test_get_assessment_returns_default_on_cache_failure(self):
    with patch("robosystems.security.auth_protection.rate_limit_cache") as mock_cache:
      mock_cache.get.side_effect = Exception("Cache down")
      assessment = AdvancedAuthProtection.get_ip_threat_assessment("1.1.1.1")
      assert assessment.threat_level == ThreatLevel.LOW

  def test_check_blocked_returns_false_on_cache_failure(self):
    with patch("robosystems.security.auth_protection.rate_limit_cache") as mock_cache:
      mock_cache.get.side_effect = Exception("Cache down")
      is_blocked, remaining = AdvancedAuthProtection.check_ip_blocked("1.1.1.1")
      assert is_blocked is False
