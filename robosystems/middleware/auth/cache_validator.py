"""
Cache validation service for comprehensive authentication cache security.

This module provides validation, monitoring, and cleanup services for the
encrypted authentication cache system.
"""

import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
import redis.asyncio as redis_async

from ...logger import logger
from ...security import SecurityAuditLogger, SecurityEventType
from .cache import APIKeyCache
from ...config.valkey_registry import ValkeyDatabase
from ...config.valkey_registry import create_async_redis_client


@dataclass
class CacheValidationResult:
  """Result of cache validation operation."""

  is_valid: bool
  issues_found: List[str]
  corrective_actions_taken: List[str]
  security_events_logged: int
  validation_timestamp: datetime

  @property
  def has_security_issues(self) -> bool:
    """Check if validation found security issues."""
    return len(self.issues_found) > 0


class CacheValidator:
  """Comprehensive cache validation and security monitoring service."""

  def __init__(self, api_key_cache: APIKeyCache):
    self.api_key_cache = api_key_cache
    self.logger = logging.getLogger(__name__)

    # Initialize async Redis client for async operations
    self._async_redis = None

    # Validation thresholds
    self.max_validation_failures = 10
    self.max_cache_age_hours = 24
    self.validation_interval_minutes = 30

  async def _get_async_redis(self) -> redis_async.Redis:
    """Get async Redis connection, creating if needed."""
    if self._async_redis is None:
      # Use the new connection factory with proper ElastiCache support
      self._async_redis = create_async_redis_client(ValkeyDatabase.AUTH_CACHE)
      # Test connection
      await self._async_redis.ping()
    return self._async_redis

  async def validate_cache_integrity(self) -> CacheValidationResult:
    """
    Perform comprehensive cache integrity validation.

    Returns:
        CacheValidationResult with detailed validation results
    """
    validation_start = datetime.now(timezone.utc)
    issues_found = []
    corrective_actions = []
    security_events = 0

    try:
      self.logger.info("Starting comprehensive cache integrity validation")

      # 1. Validate API key cache encryption and signatures
      api_key_issues = await self._validate_api_key_cache()
      issues_found.extend(api_key_issues["issues"])
      corrective_actions.extend(api_key_issues["actions"])
      security_events += api_key_issues["events"]

      # 2. Validate JWT cache encryption and signatures
      jwt_issues = await self._validate_jwt_cache()
      issues_found.extend(jwt_issues["issues"])
      corrective_actions.extend(jwt_issues["actions"])
      security_events += jwt_issues["events"]

      # 3. Check for cache consistency issues
      consistency_issues = await self._check_cache_consistency()
      issues_found.extend(consistency_issues["issues"])
      corrective_actions.extend(consistency_issues["actions"])
      security_events += consistency_issues["events"]

      # 4. Validate cache freshness and cleanup stale entries
      freshness_issues = await self._validate_cache_freshness()
      issues_found.extend(freshness_issues["issues"])
      corrective_actions.extend(freshness_issues["actions"])
      security_events += freshness_issues["events"]

      # 5. Check for suspicious cache patterns
      pattern_issues = await self._detect_suspicious_patterns()
      issues_found.extend(pattern_issues["issues"])
      corrective_actions.extend(pattern_issues["actions"])
      security_events += pattern_issues["events"]

      validation_result = CacheValidationResult(
        is_valid=len(issues_found) == 0,
        issues_found=issues_found,
        corrective_actions_taken=corrective_actions,
        security_events_logged=security_events,
        validation_timestamp=validation_start,
      )

      # Log overall validation result
      if validation_result.has_security_issues:
        SecurityAuditLogger.log_security_event(
          event_type=SecurityEventType.SUSPICIOUS_ACTIVITY,
          details={
            "action": "cache_validation_issues_found",
            "issues_count": len(issues_found),
            "corrective_actions": len(corrective_actions),
            "validation_duration_ms": (
              datetime.now(timezone.utc) - validation_start
            ).total_seconds()
            * 1000,
          },
          risk_level="high" if len(issues_found) > 5 else "medium",
        )
      else:
        SecurityAuditLogger.log_security_event(
          event_type=SecurityEventType.AUTH_SUCCESS,
          details={
            "action": "cache_validation_passed",
            "validation_duration_ms": (
              datetime.now(timezone.utc) - validation_start
            ).total_seconds()
            * 1000,
          },
          risk_level="low",
        )

      self.logger.info(
        f"Cache validation completed: {len(issues_found)} issues found, "
        f"{len(corrective_actions)} corrective actions taken"
      )

      return validation_result

    except Exception as e:
      self.logger.error(f"Cache validation failed: {e}")
      SecurityAuditLogger.log_security_event(
        event_type=SecurityEventType.SUSPICIOUS_ACTIVITY,
        details={
          "action": "cache_validation_failed",
          "error": str(e),
        },
        risk_level="high",
      )
      return CacheValidationResult(
        is_valid=False,
        issues_found=[f"Validation failed: {str(e)}"],
        corrective_actions_taken=[],
        security_events_logged=1,
        validation_timestamp=validation_start,
      )

  async def _validate_api_key_cache(self) -> Dict[str, Any]:
    """Validate API key cache entries for encryption and signature integrity."""
    issues = []
    actions = []
    events = 0

    try:
      # Get all API key cache entries using async Redis
      redis = await self._get_async_redis()
      api_key_pattern = f"{self.api_key_cache.CACHE_KEY_PREFIX}*"
      api_key_keys = await redis.keys(api_key_pattern)

      for cache_key in api_key_keys:
        try:
          # Extract API key hash from cache key
          api_key_hash = cache_key.replace(self.api_key_cache.CACHE_KEY_PREFIX, "")
          signature_key = f"{self.api_key_cache.CACHE_SIGNATURE_PREFIX}{api_key_hash}"

          # Check if both data and signature exist
          encrypted_data = await redis.get(cache_key)
          stored_signature = await redis.get(signature_key)

          if encrypted_data and not stored_signature:
            issues.append(
              f"API key cache entry missing signature: {api_key_hash[:8]}..."
            )
            # Clean up entry without signature
            await redis.delete(cache_key)
            actions.append(f"Removed unsigned cache entry: {api_key_hash[:8]}...")
            events += 1

          elif not encrypted_data and stored_signature:
            issues.append(f"Orphaned signature found: {api_key_hash[:8]}...")
            # Clean up orphaned signature
            await redis.delete(signature_key)
            actions.append(f"Removed orphaned signature: {api_key_hash[:8]}...")

          elif encrypted_data and stored_signature:
            # Validate decryption and signature
            try:
              cache_data = self.api_key_cache._decrypt_cache_data(encrypted_data)
              if not cache_data:
                issues.append(f"Failed to decrypt API key cache: {api_key_hash[:8]}...")
                await redis.delete(cache_key, signature_key)
                actions.append(f"Removed corrupted cache entry: {api_key_hash[:8]}...")
                events += 1
                continue

              if not self.api_key_cache._verify_cache_signature(
                cache_key, cache_data, stored_signature
              ):
                issues.append(f"Signature verification failed: {api_key_hash[:8]}...")
                await redis.delete(cache_key, signature_key)
                actions.append(f"Removed tampered cache entry: {api_key_hash[:8]}...")
                events += 1
                continue

              # Validate user data integrity
              user_data = cache_data.get("user_data", {})
              if not self.api_key_cache._validate_user_data_integrity(user_data):
                issues.append(f"Invalid user data in cache: {api_key_hash[:8]}...")
                await redis.delete(cache_key, signature_key)
                actions.append(
                  f"Removed invalid user data cache: {api_key_hash[:8]}..."
                )
                events += 1

            except Exception as e:
              issues.append(
                f"Cache validation error for {api_key_hash[:8]}...: {str(e)}"
              )
              await redis.delete(cache_key, signature_key)
              actions.append(f"Removed problematic cache entry: {api_key_hash[:8]}...")
              events += 1

        except Exception as e:
          issues.append(f"Error validating cache key {cache_key}: {str(e)}")

    except Exception as e:
      issues.append(f"API key cache validation failed: {str(e)}")
      events += 1

    return {"issues": issues, "actions": actions, "events": events}

  async def _validate_jwt_cache(self) -> Dict[str, Any]:
    """Validate JWT cache entries for encryption and signature integrity."""
    issues = []
    actions = []
    events = 0

    try:
      # Get all JWT cache entries using async Redis
      redis = await self._get_async_redis()
      jwt_pattern = f"{self.api_key_cache.JWT_CACHE_KEY_PREFIX}*"
      jwt_keys = await redis.keys(jwt_pattern)

      for cache_key in jwt_keys:
        try:
          # Extract JWT hash from cache key
          jwt_hash = cache_key.replace(self.api_key_cache.JWT_CACHE_KEY_PREFIX, "")
          signature_key = f"{self.api_key_cache.CACHE_SIGNATURE_PREFIX}jwt_{jwt_hash}"

          # Check if both data and signature exist
          encrypted_data = await redis.get(cache_key)
          stored_signature = await redis.get(signature_key)

          if encrypted_data and not stored_signature:
            issues.append(f"JWT cache entry missing signature: {jwt_hash[:8]}...")
            await redis.delete(cache_key)
            actions.append(f"Removed unsigned JWT cache: {jwt_hash[:8]}...")
            events += 1

          elif not encrypted_data and stored_signature:
            issues.append(f"Orphaned JWT signature: {jwt_hash[:8]}...")
            await redis.delete(signature_key)
            actions.append(f"Removed orphaned JWT signature: {jwt_hash[:8]}...")

          elif encrypted_data and stored_signature:
            # Validate decryption and signature
            try:
              cache_data = self.api_key_cache._decrypt_cache_data(encrypted_data)
              if not cache_data:
                issues.append(f"Failed to decrypt JWT cache: {jwt_hash[:8]}...")
                await redis.delete(cache_key, signature_key)
                actions.append(f"Removed corrupted JWT cache: {jwt_hash[:8]}...")
                events += 1
                continue

              if not self.api_key_cache._verify_cache_signature(
                cache_key, cache_data, stored_signature
              ):
                issues.append(f"JWT signature verification failed: {jwt_hash[:8]}...")
                await redis.delete(cache_key, signature_key)
                actions.append(f"Removed tampered JWT cache: {jwt_hash[:8]}...")
                events += 1
                continue

              # Validate user data integrity
              user_data = cache_data.get("user_data", {})
              if not self.api_key_cache._validate_user_data_integrity(user_data):
                issues.append(f"Invalid JWT user data: {jwt_hash[:8]}...")
                await redis.delete(cache_key, signature_key)
                actions.append(f"Removed invalid JWT user data: {jwt_hash[:8]}...")
                events += 1

            except Exception as e:
              issues.append(
                f"JWT cache validation error for {jwt_hash[:8]}...: {str(e)}"
              )
              await redis.delete(cache_key, signature_key)
              actions.append(f"Removed problematic JWT cache: {jwt_hash[:8]}...")
              events += 1

        except Exception as e:
          issues.append(f"Error validating JWT cache key {cache_key}: {str(e)}")

    except Exception as e:
      issues.append(f"JWT cache validation failed: {str(e)}")
      events += 1

    return {"issues": issues, "actions": actions, "events": events}

  async def _check_cache_consistency(self) -> Dict[str, Any]:
    """Check for cache consistency issues and data integrity problems."""
    issues = []
    actions = []
    events = 0

    try:
      redis = await self._get_async_redis()

      # Check for cache entries without corresponding signatures
      cache_keys = await redis.keys(f"{self.api_key_cache.CACHE_KEY_PREFIX}*")
      signature_keys = await redis.keys(f"{self.api_key_cache.CACHE_SIGNATURE_PREFIX}*")

      # Extract identifiers from cache keys
      cache_ids = set()
      for key in cache_keys:
        cache_id = key.replace(self.api_key_cache.CACHE_KEY_PREFIX, "")
        cache_ids.add(cache_id)

      # Extract identifiers from signature keys
      signature_ids = set()
      for key in signature_keys:
        if key.startswith(f"{self.api_key_cache.CACHE_SIGNATURE_PREFIX}jwt_"):
          sig_id = key.replace(f"{self.api_key_cache.CACHE_SIGNATURE_PREFIX}jwt_", "")
        else:
          sig_id = key.replace(self.api_key_cache.CACHE_SIGNATURE_PREFIX, "")
        signature_ids.add(sig_id)

      # Find orphaned entries
      orphaned_cache = cache_ids - signature_ids
      orphaned_signatures = signature_ids - cache_ids

      if orphaned_cache:
        for cache_id in orphaned_cache:
          issues.append(f"cache entry without signature: {cache_id[:8]}...")
          cache_key = f"{self.api_key_cache.CACHE_KEY_PREFIX}{cache_id}"
          await redis.delete(cache_key)
          actions.append(f"removed orphaned cache: {cache_id[:8]}...")
          events += 1

      if orphaned_signatures:
        for sig_id in orphaned_signatures:
          issues.append(f"signature without cache entry: {sig_id[:8]}...")
          # Try both signature key formats
          sig_key1 = f"{self.api_key_cache.CACHE_SIGNATURE_PREFIX}{sig_id}"
          sig_key2 = f"{self.api_key_cache.CACHE_SIGNATURE_PREFIX}jwt_{sig_id}"
          await redis.delete(sig_key1, sig_key2)
          actions.append(f"removed orphaned signature: {sig_id[:8]}...")

    except Exception as e:
      issues.append(f"Cache consistency check failed: {str(e)}")
      events += 1

    return {"issues": issues, "actions": actions, "events": events}

  async def _validate_cache_freshness(self) -> Dict[str, Any]:
    """Validate cache freshness and clean up stale entries."""
    issues = []
    actions = []
    events = 0

    try:
      redis = await self._get_async_redis()
      max_age = timedelta(hours=self.max_cache_age_hours)
      now = datetime.now(timezone.utc)

      # Check API key cache freshness
      api_key_keys = await redis.keys(f"{self.api_key_cache.CACHE_KEY_PREFIX}*")

      for cache_key in api_key_keys:
        try:
          encrypted_data = await redis.get(cache_key)
          if encrypted_data:
            cache_data = self.api_key_cache._decrypt_cache_data(encrypted_data)
            if cache_data and "cached_at" in cache_data:
              cached_at = datetime.fromisoformat(
                cache_data["cached_at"].replace("Z", "+00:00")
              )
              age = now - cached_at

              if age > max_age:
                api_key_hash = cache_key.replace(
                  self.api_key_cache.CACHE_KEY_PREFIX, ""
                )
                issues.append(f"stale cache entry: {api_key_hash[:8]}... (age: {age})")

                # Clean up stale entry and signature
                signature_key = (
                  f"{self.api_key_cache.CACHE_SIGNATURE_PREFIX}{api_key_hash}"
                )
                await redis.delete(cache_key, signature_key)
                actions.append(f"removed stale cache: {api_key_hash[:8]}...")

        except Exception as e:
          issues.append(f"Error checking cache freshness for {cache_key}: {str(e)}")

    except Exception as e:
      issues.append(f"Cache freshness validation failed: {str(e)}")
      events += 1

    return {"issues": issues, "actions": actions, "events": events}

  async def _detect_suspicious_patterns(self) -> Dict[str, Any]:
    """Detect suspicious patterns in cache usage."""
    issues = []
    actions = []
    events = 0

    try:
      redis = await self._get_async_redis()

      # Check for excessive cache entries (potential cache flooding attack)
      total_cache_keys = len(
        await redis.keys(f"{self.api_key_cache.CACHE_KEY_PREFIX}*")
      )
      total_jwt_keys = len(
        await redis.keys(f"{self.api_key_cache.JWT_CACHE_KEY_PREFIX}*")
      )

      # Define thresholds
      MAX_API_KEY_CACHE_ENTRIES = 10000
      MAX_JWT_CACHE_ENTRIES = 50000

      if total_cache_keys > MAX_API_KEY_CACHE_ENTRIES:
        issues.append(f"Excessive API key cache entries: {total_cache_keys}")
        SecurityAuditLogger.log_security_event(
          event_type=SecurityEventType.SUSPICIOUS_ACTIVITY,
          details={
            "action": "excessive_cache_entries",
            "cache_type": "api_key",
            "entry_count": total_cache_keys,
            "threshold": MAX_API_KEY_CACHE_ENTRIES,
          },
          risk_level="high",
        )
        events += 1

      if total_jwt_keys > MAX_JWT_CACHE_ENTRIES:
        issues.append(f"Excessive JWT cache entries: {total_jwt_keys}")
        SecurityAuditLogger.log_security_event(
          event_type=SecurityEventType.SUSPICIOUS_ACTIVITY,
          details={
            "action": "excessive_cache_entries",
            "cache_type": "jwt",
            "entry_count": total_jwt_keys,
            "threshold": MAX_JWT_CACHE_ENTRIES,
          },
          risk_level="high",
        )
        events += 1

    except Exception as e:
      issues.append(f"Suspicious pattern detection failed: {str(e)}")
      events += 1

    return {"issues": issues, "actions": actions, "events": events}

  async def emergency_cache_purge(self, reason: str) -> bool:
    """
    Emergency purge of all cache data in case of security breach.

    Args:
        reason: Reason for emergency purge

    Returns:
        True if successful
    """
    try:
      redis = await self._get_async_redis()

      # Get all cache-related keys
      patterns = [
        f"{self.api_key_cache.CACHE_KEY_PREFIX}*",
        f"{self.api_key_cache.GRAPH_CACHE_KEY_PREFIX}*",
        f"{self.api_key_cache.JWT_CACHE_KEY_PREFIX}*",
        f"{self.api_key_cache.JWT_GRAPH_CACHE_KEY_PREFIX}*",
        f"{self.api_key_cache.CACHE_SIGNATURE_PREFIX}*",
        f"{self.api_key_cache.CACHE_VALIDATION_PREFIX}*",
      ]

      total_deleted = 0
      for pattern in patterns:
        keys = await redis.keys(pattern)
        if keys:
          await redis.delete(*keys)
          total_deleted += len(keys)

      # Log emergency purge
      SecurityAuditLogger.log_security_event(
        event_type=SecurityEventType.SUSPICIOUS_ACTIVITY,
        details={
          "action": "emergency_cache_purge",
          "reason": reason,
          "keys_deleted": total_deleted,
          "timestamp": datetime.now(timezone.utc).isoformat(),
        },
        risk_level="critical",
      )

      self.logger.critical(
        f"Emergency cache purge completed: {total_deleted} keys deleted. Reason: {reason}"
      )
      return True

    except Exception as e:
      self.logger.error(f"Emergency cache purge failed: {e}")
      SecurityAuditLogger.log_security_event(
        event_type=SecurityEventType.SUSPICIOUS_ACTIVITY,
        details={
          "action": "emergency_cache_purge_failed",
          "reason": reason,
          "error": str(e),
        },
        risk_level="critical",
      )
      return False


# Global cache validator instance
cache_validator: Optional[CacheValidator] = None


def get_cache_validator() -> Optional[CacheValidator]:
  """Get the global cache validator instance."""
  global cache_validator
  if cache_validator is None:
    try:
      from .cache import api_key_cache

      if api_key_cache:
        cache_validator = CacheValidator(api_key_cache)
    except Exception as e:
      logger.error(f"Failed to initialize cache validator: {e}")
  return cache_validator
