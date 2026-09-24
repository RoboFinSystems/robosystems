"""
Authentication protection: progressive delays, per-IP threat scoring, blocking.

A layer above rate limiting — repeated failures from one IP escalate through
delay tiers into timed blocks. IPs are keyed by hash, never stored in clear.
"""

import hashlib
import json
import time
from dataclasses import asdict, dataclass
from enum import Enum

from ..logger import logger
from ..middleware.rate_limits import rate_limit_cache
from ..security import SecurityAuditLogger, SecurityEventType


class ThreatLevel(Enum):
  LOW = "low"
  MEDIUM = "medium"
  HIGH = "high"
  CRITICAL = "critical"


@dataclass
class AuthAttempt:
  timestamp: float
  success: bool
  ip_address: str
  user_agent: str | None = None
  email: str | None = None


@dataclass
class IPThreatAssessment:
  threat_level: ThreatLevel
  failed_attempts: int
  successful_attempts: int
  first_seen: float
  last_attempt: float
  is_blocked: bool
  block_expires: float | None


class AdvancedAuthProtection:
  # Failed attempts -> delay seconds. Keys must be contiguous: a missing key
  # falls back to the highest delay, not the next tier down.
  PROGRESSIVE_DELAYS = {
    0: 0,
    1: 1,
    2: 2,
    3: 5,
    4: 10,
    5: 30,
    6: 60,
    7: 300,
    8: 900,
  }

  # Minimum failed attempts per level.
  THREAT_THRESHOLDS = {
    ThreatLevel.MEDIUM: 5,
    ThreatLevel.HIGH: 10,
    ThreatLevel.CRITICAL: 20,
  }

  # Seconds.
  BLOCK_DURATIONS = {
    ThreatLevel.MEDIUM: 900,
    ThreatLevel.HIGH: 3600,
    ThreatLevel.CRITICAL: 86400,
  }

  ATTEMPT_KEY_PREFIX = "auth_attempts"
  IP_THREAT_KEY_PREFIX = "ip_threat"
  DELAY_KEY_PREFIX = "auth_delay"

  @classmethod
  def _get_attempt_key(cls, ip_address: str) -> str:
    ip_hash = hashlib.sha256(ip_address.encode()).hexdigest()[:16]
    return f"{cls.ATTEMPT_KEY_PREFIX}:{ip_hash}"

  @classmethod
  def _get_threat_key(cls, ip_address: str) -> str:
    ip_hash = hashlib.sha256(ip_address.encode()).hexdigest()[:16]
    return f"{cls.IP_THREAT_KEY_PREFIX}:{ip_hash}"

  @classmethod
  def _get_delay_key(cls, ip_address: str) -> str:
    ip_hash = hashlib.sha256(ip_address.encode()).hexdigest()[:16]
    return f"{cls.DELAY_KEY_PREFIX}:{ip_hash}"

  @classmethod
  def record_auth_attempt(
    cls,
    ip_address: str,
    success: bool,
    email: str | None = None,
    user_agent: str | None = None,
  ) -> None:
    """
    Record an authentication attempt, updating history and threat assessment.

    Failed attempts also emit an AUTH_FAILURE audit event.
    """
    attempt = AuthAttempt(
      timestamp=time.time(),
      success=success,
      ip_address=ip_address,
      user_agent=user_agent,
      email=email,
    )

    cls._update_attempt_history(ip_address, attempt)
    cls._update_threat_assessment(ip_address, attempt)

    if not success:
      SecurityAuditLogger.log_security_event(
        event_type=SecurityEventType.AUTH_FAILURE,
        user_id=None,
        ip_address=ip_address,
        user_agent=user_agent,
        details={
          "email": email,
          "threat_assessment": cls.get_ip_threat_assessment(
            ip_address
          ).threat_level.value,
        },
      )

  @staticmethod
  def _dump(value) -> str:
    """JSON-encode: redis rejects a raw dict or list."""
    return json.dumps(value)

  @staticmethod
  def _load(raw):
    """Parse a cached JSON value; already-decoded values pass through."""
    if raw is None:
      return None
    if isinstance(raw, (bytes, bytearray)):
      raw = raw.decode("utf-8")
    if isinstance(raw, str):
      return json.loads(raw)
    return raw

  @classmethod
  def _dump_assessment(cls, assessment: "IPThreatAssessment") -> str:
    data = asdict(assessment)
    data["threat_level"] = assessment.threat_level.value
    return json.dumps(data)

  @classmethod
  def _load_assessment(cls, raw) -> "IPThreatAssessment":
    data = cls._load(raw)
    data = dict(data)
    data["threat_level"] = ThreatLevel(data["threat_level"])
    return IPThreatAssessment(**data)

  @classmethod
  def _update_attempt_history(cls, ip_address: str, attempt: AuthAttempt) -> None:
    """Append to the IP's attempt history, keeping the last 24 hours."""
    key = cls._get_attempt_key(ip_address)

    try:
      attempts_data = cls._load(rate_limit_cache.get(key)) or []

      attempts_data.append(
        {
          "timestamp": attempt.timestamp,
          "success": attempt.success,
          "email": attempt.email,
          "user_agent": attempt.user_agent,
        }
      )

      cutoff = time.time() - 86400
      attempts_data = [a for a in attempts_data if a["timestamp"] > cutoff]

      rate_limit_cache.set(key, cls._dump(attempts_data), expire=86400 + 3600)

    except Exception as e:
      # Auth must not fail on the threat store, but a breakage must be visible.
      logger.warning(f"auth threat: attempt history not stored ({type(e).__name__})")

  @classmethod
  def _update_threat_assessment(cls, ip_address: str, attempt: AuthAttempt) -> None:
    key = cls._get_threat_key(ip_address)

    try:
      assessment_data = rate_limit_cache.get(key)

      if assessment_data:
        assessment = cls._load_assessment(assessment_data)
      else:
        assessment = IPThreatAssessment(
          threat_level=ThreatLevel.LOW,
          failed_attempts=0,
          successful_attempts=0,
          first_seen=attempt.timestamp,
          last_attempt=attempt.timestamp,
          is_blocked=False,
          block_expires=None,
        )

      if attempt.success:
        assessment.successful_attempts += 1
        # A success forgives one failure, not all of them.
        if assessment.failed_attempts > 0:
          assessment.failed_attempts = max(0, assessment.failed_attempts - 1)
      else:
        assessment.failed_attempts += 1

      assessment.last_attempt = attempt.timestamp

      if assessment.failed_attempts >= cls.THREAT_THRESHOLDS[ThreatLevel.CRITICAL]:
        assessment.threat_level = ThreatLevel.CRITICAL
      elif assessment.failed_attempts >= cls.THREAT_THRESHOLDS[ThreatLevel.HIGH]:
        assessment.threat_level = ThreatLevel.HIGH
      elif assessment.failed_attempts >= cls.THREAT_THRESHOLDS[ThreatLevel.MEDIUM]:
        assessment.threat_level = ThreatLevel.MEDIUM
      else:
        assessment.threat_level = ThreatLevel.LOW

      if assessment.threat_level in cls.BLOCK_DURATIONS and not attempt.success:
        block_duration = cls.BLOCK_DURATIONS[assessment.threat_level]
        assessment.is_blocked = True
        assessment.block_expires = time.time() + block_duration

      if assessment.is_blocked and assessment.block_expires:
        if time.time() > assessment.block_expires:
          assessment.is_blocked = False
          assessment.block_expires = None

      rate_limit_cache.set(key, cls._dump_assessment(assessment), expire=86400 + 3600)

    except Exception as e:
      logger.warning(f"auth threat: assessment not stored ({type(e).__name__})")

  @classmethod
  def get_ip_threat_assessment(cls, ip_address: str) -> IPThreatAssessment:
    """Get the current threat assessment for an IP, or a LOW default if unseen."""
    key = cls._get_threat_key(ip_address)

    try:
      assessment_data = rate_limit_cache.get(key)
      if assessment_data:
        assessment = cls._load_assessment(assessment_data)

        if assessment.is_blocked and assessment.block_expires:
          if time.time() > assessment.block_expires:
            assessment.is_blocked = False
            assessment.block_expires = None

        return assessment
    except Exception as e:
      logger.warning(f"auth threat: assessment not read ({type(e).__name__})")

    return IPThreatAssessment(
      threat_level=ThreatLevel.LOW,
      failed_attempts=0,
      successful_attempts=0,
      first_seen=time.time(),
      last_attempt=time.time(),
      is_blocked=False,
      block_expires=None,
    )

  @classmethod
  def check_ip_blocked(cls, ip_address: str) -> tuple[bool, int | None]:
    """Return ``(is_blocked, seconds_until_unblock)``."""
    assessment = cls.get_ip_threat_assessment(ip_address)

    if assessment.is_blocked and assessment.block_expires:
      remaining = int(assessment.block_expires - time.time())
      if remaining > 0:
        return True, remaining
      else:
        assessment.is_blocked = False
        assessment.block_expires = None
        key = cls._get_threat_key(ip_address)
        try:
          rate_limit_cache.set(
            key, cls._dump_assessment(assessment), expire=86400 + 3600
          )
        except Exception as e:
          logger.warning(f"auth threat: block clear not stored ({type(e).__name__})")

    return False, None

  @classmethod
  def get_progressive_delay(cls, ip_address: str) -> int:
    """Seconds still to wait before the IP's next attempt, or 0."""
    assessment = cls.get_ip_threat_assessment(ip_address)

    delay = cls.PROGRESSIVE_DELAYS.get(
      min(assessment.failed_attempts, max(cls.PROGRESSIVE_DELAYS.keys())),
      cls.PROGRESSIVE_DELAYS[max(cls.PROGRESSIVE_DELAYS.keys())],
    )

    delay_key = cls._get_delay_key(ip_address)
    try:
      last_delay_time = cls._load(rate_limit_cache.get(delay_key))
      if last_delay_time:
        elapsed = time.time() - float(last_delay_time)
        if elapsed < delay:
          return int(delay - elapsed)
    except Exception as e:
      logger.warning(f"auth threat: progressive delay not read ({type(e).__name__})")

    return 0

  @classmethod
  def apply_progressive_delay(cls, ip_address: str) -> None:
    """Start the progressive delay window after a failed authentication attempt."""
    delay_key = cls._get_delay_key(ip_address)
    try:
      rate_limit_cache.set(delay_key, cls._dump(time.time()), expire=3600)
    except Exception as e:
      logger.warning(f"auth threat: progressive delay not stored ({type(e).__name__})")

  @classmethod
  def get_security_headers(cls, ip_address: str) -> dict[str, str]:
    """Build the ``X-Auth-*`` headers describing an IP's threat, block, and delay."""
    assessment = cls.get_ip_threat_assessment(ip_address)

    headers = {
      "X-Auth-Threat-Level": assessment.threat_level.value,
    }

    if assessment.is_blocked:
      headers["X-Auth-Blocked"] = "true"
      if assessment.block_expires:
        remaining = int(assessment.block_expires - time.time())
        headers["X-Auth-Block-Expires"] = str(remaining)

    delay = cls.get_progressive_delay(ip_address)
    if delay > 0:
      headers["X-Auth-Delay"] = str(delay)

    return headers
