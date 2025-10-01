"""
Enhanced authentication protection including progressive delays and IP monitoring.

Provides additional security layers beyond basic rate limiting.
"""

import time
import hashlib
from typing import Dict, Optional, Tuple
from dataclasses import dataclass
from enum import Enum

from ..middleware.rate_limits import rate_limit_cache
from ..security import SecurityAuditLogger, SecurityEventType


class ThreatLevel(Enum):
  """IP address threat assessment levels."""

  LOW = "low"
  MEDIUM = "medium"
  HIGH = "high"
  CRITICAL = "critical"


@dataclass
class AuthAttempt:
  """Authentication attempt tracking."""

  timestamp: float
  success: bool
  ip_address: str
  user_agent: Optional[str] = None
  email: Optional[str] = None


@dataclass
class IPThreatAssessment:
  """IP address threat assessment."""

  threat_level: ThreatLevel
  failed_attempts: int
  successful_attempts: int
  first_seen: float
  last_attempt: float
  is_blocked: bool
  block_expires: Optional[float]


class AdvancedAuthProtection:
  """Advanced authentication protection system."""

  # Progressive delay configuration (seconds)
  PROGRESSIVE_DELAYS = {
    1: 1,  # 1st failure: 1 second
    2: 2,  # 2nd failure: 2 seconds
    3: 5,  # 3rd failure: 5 seconds
    4: 10,  # 4th failure: 10 seconds
    5: 30,  # 5th failure: 30 seconds
    6: 60,  # 6th failure: 1 minute
    7: 300,  # 7th failure: 5 minutes
    8: 900,  # 8th failure: 15 minutes
  }

  # Threat level thresholds
  THREAT_THRESHOLDS = {
    ThreatLevel.MEDIUM: 5,  # 5+ failed attempts
    ThreatLevel.HIGH: 10,  # 10+ failed attempts
    ThreatLevel.CRITICAL: 20,  # 20+ failed attempts
  }

  # Block durations (seconds)
  BLOCK_DURATIONS = {
    ThreatLevel.MEDIUM: 900,  # 15 minutes
    ThreatLevel.HIGH: 3600,  # 1 hour
    ThreatLevel.CRITICAL: 86400,  # 24 hours
  }

  # Cache keys
  ATTEMPT_KEY_PREFIX = "auth_attempts"
  IP_THREAT_KEY_PREFIX = "ip_threat"
  DELAY_KEY_PREFIX = "auth_delay"

  @classmethod
  def _get_attempt_key(cls, ip_address: str) -> str:
    """Get cache key for tracking attempts by IP."""
    ip_hash = hashlib.sha256(ip_address.encode()).hexdigest()[:16]
    return f"{cls.ATTEMPT_KEY_PREFIX}:{ip_hash}"

  @classmethod
  def _get_threat_key(cls, ip_address: str) -> str:
    """Get cache key for IP threat assessment."""
    ip_hash = hashlib.sha256(ip_address.encode()).hexdigest()[:16]
    return f"{cls.IP_THREAT_KEY_PREFIX}:{ip_hash}"

  @classmethod
  def _get_delay_key(cls, ip_address: str) -> str:
    """Get cache key for progressive delay tracking."""
    ip_hash = hashlib.sha256(ip_address.encode()).hexdigest()[:16]
    return f"{cls.DELAY_KEY_PREFIX}:{ip_hash}"

  @classmethod
  def record_auth_attempt(
    cls,
    ip_address: str,
    success: bool,
    email: Optional[str] = None,
    user_agent: Optional[str] = None,
  ) -> None:
    """
    Record an authentication attempt for analysis.

    Args:
        ip_address: Client IP address
        success: Whether the attempt was successful
        email: Email address attempted (if available)
        user_agent: Client user agent
    """
    attempt = AuthAttempt(
      timestamp=time.time(),
      success=success,
      ip_address=ip_address,
      user_agent=user_agent,
      email=email,
    )

    # Update attempt history
    cls._update_attempt_history(ip_address, attempt)

    # Update threat assessment
    cls._update_threat_assessment(ip_address, attempt)

    # Log security event for failed attempts
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

  @classmethod
  def _update_attempt_history(cls, ip_address: str, attempt: AuthAttempt) -> None:
    """Update attempt history for an IP address."""
    key = cls._get_attempt_key(ip_address)

    try:
      # Get existing attempts (last 24 hours)
      attempts_data = rate_limit_cache.get(key) or []

      # Add new attempt
      attempts_data.append(
        {
          "timestamp": attempt.timestamp,
          "success": attempt.success,
          "email": attempt.email,
          "user_agent": attempt.user_agent,
        }
      )

      # Keep only last 24 hours of attempts
      cutoff = time.time() - 86400  # 24 hours
      attempts_data = [a for a in attempts_data if a["timestamp"] > cutoff]

      # Store back in cache (expire in 25 hours to be safe)
      rate_limit_cache.set(key, attempts_data, expire=86400 + 3600)

    except Exception:
      # If cache fails, continue without storing
      pass

  @classmethod
  def _update_threat_assessment(cls, ip_address: str, attempt: AuthAttempt) -> None:
    """Update threat assessment for an IP address."""
    key = cls._get_threat_key(ip_address)

    try:
      # Get existing assessment
      assessment_data = rate_limit_cache.get(key)

      if assessment_data:
        assessment = IPThreatAssessment(**assessment_data)
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

      # Update counters
      if attempt.success:
        assessment.successful_attempts += 1
        # Reset failed attempts on successful login
        if assessment.failed_attempts > 0:
          assessment.failed_attempts = max(0, assessment.failed_attempts - 1)
      else:
        assessment.failed_attempts += 1

      assessment.last_attempt = attempt.timestamp

      # Determine threat level
      if assessment.failed_attempts >= cls.THREAT_THRESHOLDS[ThreatLevel.CRITICAL]:
        assessment.threat_level = ThreatLevel.CRITICAL
      elif assessment.failed_attempts >= cls.THREAT_THRESHOLDS[ThreatLevel.HIGH]:
        assessment.threat_level = ThreatLevel.HIGH
      elif assessment.failed_attempts >= cls.THREAT_THRESHOLDS[ThreatLevel.MEDIUM]:
        assessment.threat_level = ThreatLevel.MEDIUM
      else:
        assessment.threat_level = ThreatLevel.LOW

      # Update blocking status
      if assessment.threat_level in cls.BLOCK_DURATIONS and not attempt.success:
        block_duration = cls.BLOCK_DURATIONS[assessment.threat_level]
        assessment.is_blocked = True
        assessment.block_expires = time.time() + block_duration

      # Check if block has expired
      if assessment.is_blocked and assessment.block_expires:
        if time.time() > assessment.block_expires:
          assessment.is_blocked = False
          assessment.block_expires = None

      # Store assessment (expire in 25 hours)
      rate_limit_cache.set(key, assessment.__dict__, expire=86400 + 3600)

    except Exception:
      # If cache fails, continue without storing
      pass

  @classmethod
  def get_ip_threat_assessment(cls, ip_address: str) -> IPThreatAssessment:
    """
    Get threat assessment for an IP address.

    Args:
        ip_address: IP address to assess

    Returns:
        IPThreatAssessment with current threat level and status
    """
    key = cls._get_threat_key(ip_address)

    try:
      assessment_data = rate_limit_cache.get(key)
      if assessment_data:
        assessment = IPThreatAssessment(**assessment_data)

        # Check if block has expired
        if assessment.is_blocked and assessment.block_expires:
          if time.time() > assessment.block_expires:
            assessment.is_blocked = False
            assessment.block_expires = None

        return assessment
    except Exception:
      pass

    # Return default assessment
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
  def check_ip_blocked(cls, ip_address: str) -> Tuple[bool, Optional[int]]:
    """
    Check if an IP address is currently blocked.

    Args:
        ip_address: IP address to check

    Returns:
        Tuple of (is_blocked, seconds_until_unblock)
    """
    assessment = cls.get_ip_threat_assessment(ip_address)

    if assessment.is_blocked and assessment.block_expires:
      remaining = int(assessment.block_expires - time.time())
      if remaining > 0:
        return True, remaining
      else:
        # Block expired, clear it
        assessment.is_blocked = False
        assessment.block_expires = None
        # Update cache
        key = cls._get_threat_key(ip_address)
        try:
          rate_limit_cache.set(key, assessment.__dict__, expire=86400 + 3600)
        except Exception:
          pass

    return False, None

  @classmethod
  def get_progressive_delay(cls, ip_address: str) -> int:
    """
    Get progressive delay for failed authentication attempts.

    Args:
        ip_address: IP address of the client

    Returns:
        Delay in seconds before next attempt is allowed
    """
    assessment = cls.get_ip_threat_assessment(ip_address)

    # Get delay based on failed attempts
    delay = cls.PROGRESSIVE_DELAYS.get(
      min(assessment.failed_attempts, max(cls.PROGRESSIVE_DELAYS.keys())),
      cls.PROGRESSIVE_DELAYS[max(cls.PROGRESSIVE_DELAYS.keys())],
    )

    # Check if we're still in delay period
    delay_key = cls._get_delay_key(ip_address)
    try:
      last_delay_time = rate_limit_cache.get(delay_key)
      if last_delay_time:
        elapsed = time.time() - last_delay_time
        if elapsed < delay:
          return int(delay - elapsed)
    except Exception:
      pass

    return 0

  @classmethod
  def apply_progressive_delay(cls, ip_address: str) -> None:
    """
    Apply progressive delay after a failed authentication attempt.

    Args:
        ip_address: IP address of the client
    """
    delay_key = cls._get_delay_key(ip_address)
    try:
      # Record the delay application time
      rate_limit_cache.set(delay_key, time.time(), expire=3600)  # 1 hour max
    except Exception:
      pass

  @classmethod
  def get_security_headers(cls, ip_address: str) -> Dict[str, str]:
    """
    Get security headers to include in auth responses.

    Args:
        ip_address: Client IP address

    Returns:
        Dictionary of security headers
    """
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
