"""API key caching service using Valkey/Redis."""

import base64
import hashlib
import hmac
import json
import secrets
import time
from datetime import UTC, datetime
from typing import Any, cast

import redis
from cryptography.fernet import Fernet, InvalidToken
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

from ...config import env
from ...config.valkey_registry import ValkeyDatabase, create_redis_client
from ...logger import logger
from ...security import SecurityAuditLogger, SecurityEventType


class APIKeyCache:
  """Manages API key and JWT caching in Valkey/Redis with comprehensive security validation."""

  # Cache configuration
  DEFAULT_TTL = 300  # 5 minutes
  CACHE_KEY_PREFIX = "apikey:"
  GRAPH_CACHE_KEY_PREFIX = "apikey_graph:"
  USER_DATA_PREFIX = "user:"
  AUDIT_LOG_RATE_LIMIT_PREFIX = "audit_rate_limit:"
  AUDIT_LOG_RATE_LIMIT_TTL = 300  # 5 minutes - only log once per user per 5 minutes
  JWT_CACHE_KEY_PREFIX = "jwt:"
  JWT_GRAPH_CACHE_KEY_PREFIX = "jwt_graph:"
  JWT_BLACKLIST_PREFIX = "jwt_blacklist:"

  # Rate limiting configuration
  RATE_LIMIT_PREFIX = "rate_limit:"

  # Cache validation configuration
  CACHE_VALIDATION_PREFIX = "cache_val:"
  CACHE_SIGNATURE_PREFIX = "cache_sig:"
  CACHE_VERSION = "v2.0"  # Version for cache format compatibility

  # Security thresholds
  MAX_CACHE_AGE_SECONDS = 1800  # 30 minutes max age regardless of TTL
  CACHE_REFRESH_THRESHOLD = 1200  # 20 minutes - refresh cache if older than this
  VALIDATION_FAILURE_THRESHOLD = 5  # Max validation failures before security alert

  # Key rotation configuration
  KEY_ROTATION_INTERVAL = 86400  # 24 hours - rotate encryption keys daily
  KEY_ROTATION_PREFIX = "key_rotation:"
  KEY_GENERATION_PREFIX = "key_gen:"

  # Signature optimization configuration
  SIGNATURE_CACHE_PREFIX = "sig_cache:"
  SIGNATURE_CACHE_TTL = 300  # 5 minutes - cache computed signatures
  MAX_SIGNATURE_CACHE_SIZE = 1000  # Limit in-memory signature cache size

  def __init__(self):
    """Initialize Redis connection with security features."""
    self._redis = None
    self.ttl = env.API_KEY_CACHE_TTL
    # JWT cache can have longer TTL since tokens are typically 30 days
    self.jwt_ttl = env.JWT_CACHE_TTL  # 30 minutes

    # Initialize cache encryption for sensitive data (lazy-loaded)
    self._encryption_key = None
    self._cipher = None

    # Validation failure tracking
    self._validation_failures = 0

    # In-memory signature cache for performance optimization
    self._signature_cache: dict[str, str] = {}
    self._signature_cache_times: dict[str, float] = {}

  @property
  def redis(self) -> redis.Redis:
    """Get Redis connection, creating if needed."""
    if self._redis is None:
      try:
        # Use the new connection factory with proper ElastiCache support
        self._redis = create_redis_client(ValkeyDatabase.AUTH_CACHE)
        # Test connection
        self._redis.ping()
        logger.info("Connected to Valkey/Redis for API key caching")
      except (redis.ConnectionError, redis.TimeoutError, redis.RedisError) as e:
        logger.error(f"Redis connection error: {e}")
        raise ConnectionError(f"Failed to connect to Redis: {e}")
      except Exception as e:
        logger.error(f"Unexpected error connecting to Redis: {e}")
        raise
    return self._redis

  @property
  def encryption_key(self) -> bytes:
    """Get encryption key, deriving if needed."""
    if self._encryption_key is None:
      self._encryption_key = self._derive_encryption_key()
    return self._encryption_key

  @property
  def cipher(self) -> Fernet:
    """Get Fernet cipher, creating if needed."""
    if self._cipher is None:
      self._cipher = Fernet(self.encryption_key)
    return self._cipher

  def _get_api_key_cache_key(self, api_key_hash: str) -> str:
    """Get cache key for API key data."""
    return f"{self.CACHE_KEY_PREFIX}{api_key_hash}"

  def _get_graph_cache_key(self, api_key_hash: str, graph_id: str) -> str:
    """Get cache key for API key + graph access."""
    return f"{self.GRAPH_CACHE_KEY_PREFIX}{api_key_hash}:{graph_id}"

  def _get_user_cache_key(self, user_id: str) -> str:
    """Get cache key for user data."""
    return f"{self.USER_DATA_PREFIX}{user_id}"

  def _get_jwt_cache_key(self, jwt_hash: str) -> str:
    """Get cache key for JWT validation data."""
    return f"{self.JWT_CACHE_KEY_PREFIX}{jwt_hash}"

  def _get_jwt_graph_cache_key(self, user_id: str, graph_id: str) -> str:
    """Get cache key for JWT user + graph access."""
    return f"{self.JWT_GRAPH_CACHE_KEY_PREFIX}{user_id}:{graph_id}"

  def _get_jwt_blacklist_key(self, jwt_hash: str) -> str:
    """Get cache key for JWT blacklist."""
    return f"{self.JWT_BLACKLIST_PREFIX}{jwt_hash}"

  def _hash_jwt_token(self, token: str) -> str:
    """Create a hash of the JWT token for caching."""
    return hashlib.sha256(token.encode()).hexdigest()

  def _derive_encryption_key(self) -> bytes:
    """Derive encryption key for cache data protection with rotation support."""
    # Check if we need to rotate keys
    rotation_key = f"{self.KEY_ROTATION_PREFIX}last_rotation"
    try:
      last_rotation = self.redis.get(rotation_key)
      current_time = time.time()

      # Check if rotation is needed
      should_rotate = False
      if last_rotation is None:
        should_rotate = True
      else:
        try:
          last_rotation_time = float(last_rotation)
          should_rotate = (
            current_time - last_rotation_time
          ) > self.KEY_ROTATION_INTERVAL
        except (ValueError, TypeError):
          # Invalid stored value, force rotation
          should_rotate = True

      if should_rotate:
        # Set rotation timestamp FIRST to prevent infinite recursion
        self.redis.setex(
          rotation_key, self.KEY_ROTATION_INTERVAL * 2, str(current_time)
        )
        self._rotate_encryption_key()
    except Exception as e:
      logger.warning(f"Key rotation check failed, using default key: {e}")

    # Get current key generation component if available
    generation_key = f"{self.KEY_GENERATION_PREFIX}current"
    key_component = ""
    try:
      stored_component = self.redis.get(generation_key)
      if stored_component and isinstance(stored_component, str):
        key_component = stored_component
    except Exception:
      # Fallback to empty component if Redis unavailable
      pass

    # Use a combination of environment-specific salt, system secret, and rotated component
    key_component_safe = key_component[:8] if key_component else ""
    salt = f"{env.ENVIRONMENT}_cache_salt_{env.JWT_SECRET_KEY[:16]}_{key_component_safe}".encode()
    key_material = f"{env.JWT_SECRET_KEY}_{key_component}".encode()

    kdf = PBKDF2HMAC(
      algorithm=hashes.SHA256(),
      length=32,
      salt=salt,
      iterations=100000,
    )
    key = base64.urlsafe_b64encode(kdf.derive(key_material))
    return key

  def _rotate_encryption_key(self) -> None:
    """Rotate encryption keys for enhanced security with rollback support."""
    old_key_component = None
    old_encryption_key = self._encryption_key
    old_cipher = self._cipher
    generation_key = f"{self.KEY_GENERATION_PREFIX}current"

    try:
      # Save current key component for potential rollback
      old_key_component = self.redis.get(generation_key)

      # Generate new key component
      new_key_component = secrets.token_hex(32)

      # Store new key component with expiration
      self.redis.setex(
        generation_key, self.KEY_ROTATION_INTERVAL * 2, new_key_component
      )

      # Reset lazy-loaded cipher to force regeneration with new key
      self._encryption_key = None
      self._cipher = None

      # Test the new key by attempting encryption/decryption
      test_data = {"test": "rotation_validation", "timestamp": time.time()}
      encrypted = self._encrypt_cache_data(test_data)
      decrypted = self._decrypt_cache_data(encrypted)

      if decrypted != test_data:
        raise ValueError(
          "Key rotation validation failed: encryption/decryption mismatch"
        )

      logger.info("Cache encryption key rotated successfully")

      # Clean up old cache entries that can't be decrypted with new key
      self._cleanup_incompatible_cache_entries()

    except Exception as e:
      logger.error(f"Key rotation failed: {e}")

      # Attempt rollback
      try:
        if old_key_component is not None:
          self.redis.setex(
            generation_key, self.KEY_ROTATION_INTERVAL * 2, old_key_component
          )
          self._encryption_key = old_encryption_key
          self._cipher = old_cipher
          logger.warning("Key rotation rolled back successfully")

          SecurityAuditLogger.log_security_event(
            event_type=SecurityEventType.SUSPICIOUS_ACTIVITY,
            details={
              "action": "key_rotation_rollback",
              "original_error": str(e),
              "rollback": "successful",
            },
            risk_level="medium",
          )
      except Exception as rollback_error:
        logger.critical(f"Key rotation rollback failed: {rollback_error}")
        SecurityAuditLogger.log_security_event(
          event_type=SecurityEventType.SUSPICIOUS_ACTIVITY,
          details={
            "action": "key_rotation_rollback_failed",
            "original_error": str(e),
            "rollback_error": str(rollback_error),
          },
          risk_level="critical",
        )
        # Consider circuit breaker or service degradation here
        raise

      # Re-raise original error after rollback
      raise

  def _cleanup_incompatible_cache_entries(self) -> None:
    """Clean up cache entries that can't be decrypted with rotated keys."""
    try:
      # Get all cache keys
      cache_patterns = [
        f"{self.CACHE_KEY_PREFIX}*",
        f"{self.JWT_CACHE_KEY_PREFIX}*",
        f"{self.GRAPH_CACHE_KEY_PREFIX}*",
        f"{self.JWT_GRAPH_CACHE_KEY_PREFIX}*",
      ]

      cleaned_count = 0
      for pattern in cache_patterns:
        keys = self.redis.keys(pattern)
        for key in keys:
          try:
            encrypted_data = self.redis.get(key)
            if encrypted_data:
              # Try to decrypt - if it fails, remove the entry
              self._decrypt_cache_data(encrypted_data)
          except (InvalidToken, Exception):
            self.redis.delete(key)
            cleaned_count += 1

      if cleaned_count > 0:
        logger.info(
          f"Cleaned up {cleaned_count} incompatible cache entries after key rotation"
        )

    except Exception as e:
      logger.warning(f"Cache cleanup after key rotation failed: {e}")

  def _encrypt_cache_data(self, data: dict[str, Any]) -> str:
    """Encrypt sensitive cache data."""
    try:
      # Add validation metadata
      protected_data = {
        "data": data,
        "version": self.CACHE_VERSION,
        "encrypted_at": datetime.now(UTC).isoformat(),
        "nonce": secrets.token_hex(16),
      }
      json_data = json.dumps(protected_data)
      encrypted = self.cipher.encrypt(json_data.encode())
      return base64.urlsafe_b64encode(encrypted).decode()
    except Exception as e:
      # Check for specific error types first
      if isinstance(e, TypeError):
        logger.error(f"Data serialization error during encryption: {e}")
        raise ValueError(f"Invalid data format for encryption: {e}")
      elif isinstance(e, (InvalidToken, ValueError)):
        logger.error(f"Encryption operation failed: {e}")
        SecurityAuditLogger.log_security_event(
          event_type=SecurityEventType.SUSPICIOUS_ACTIVITY,
          details={"action": "cache_encryption_failed", "error": str(e)},
          risk_level="high",
        )
        raise
      else:
        logger.error(f"Unexpected encryption error: {e}")
        SecurityAuditLogger.log_security_event(
          event_type=SecurityEventType.SUSPICIOUS_ACTIVITY,
          details={"action": "cache_encryption_unexpected_error", "error": str(e)},
          risk_level="high",
        )
        raise

  def _decrypt_cache_data(self, encrypted_data: str) -> dict[str, Any] | None:
    """Decrypt and validate cache data."""
    try:
      encrypted_bytes = base64.urlsafe_b64decode(encrypted_data.encode())
      decrypted = self.cipher.decrypt(encrypted_bytes)
      protected_data = json.loads(decrypted.decode())

      # Validate cache format version
      if protected_data.get("version") != self.CACHE_VERSION:
        logger.warning(
          f"Cache version mismatch: {protected_data.get('version')} != {self.CACHE_VERSION}"
        )
        return None

      # Check cache age
      encrypted_at = datetime.fromisoformat(
        protected_data["encrypted_at"].replace("Z", "+00:00")
      )
      age_seconds = (datetime.now(UTC) - encrypted_at).total_seconds()

      if age_seconds > self.MAX_CACHE_AGE_SECONDS:
        logger.warning(
          f"Cache data too old: {age_seconds}s > {self.MAX_CACHE_AGE_SECONDS}s"
        )
        SecurityAuditLogger.log_security_event(
          event_type=SecurityEventType.SUSPICIOUS_ACTIVITY,
          details={
            "action": "cache_age_violation",
            "age_seconds": age_seconds,
            "max_age": self.MAX_CACHE_AGE_SECONDS,
          },
          risk_level="medium",
        )
        return None

      return protected_data["data"]

    except InvalidToken:
      logger.error("Cache data decryption failed - invalid token")
      self._validation_failures += 1
      if self._validation_failures >= self.VALIDATION_FAILURE_THRESHOLD:
        SecurityAuditLogger.log_security_event(
          event_type=SecurityEventType.SUSPICIOUS_ACTIVITY,
          details={
            "action": "cache_validation_failure_threshold",
            "failure_count": self._validation_failures,
            "threshold": self.VALIDATION_FAILURE_THRESHOLD,
          },
          risk_level="high",
        )
      return None
    except (json.JSONDecodeError, KeyError) as e:
      logger.error(f"Cache data format error during decryption: {e}")
      return None
    except (ValueError, TypeError) as e:
      logger.error(f"Cache data validation error: {e}")
      return None
    except Exception as e:
      logger.error(f"Unexpected decryption error: {e}")
      return None

  def _validate_user_data_integrity(self, user_data: dict[str, Any]) -> bool:
    """Validate integrity of cached user data."""
    try:
      # Required fields validation
      required_fields = ["id", "email", "is_active"]
      for field in required_fields:
        if field not in user_data:
          logger.warning(f"Missing required field in cached user data: {field}")
          return False

      # Data type validation
      if not isinstance(user_data["id"], str) or not user_data["id"]:
        logger.warning("Invalid user ID in cached data")
        return False

      if not isinstance(user_data["email"], str) or "@" not in user_data["email"]:
        logger.warning("Invalid email in cached data")
        return False

      if not isinstance(user_data["is_active"], bool):
        logger.warning("Invalid is_active field in cached data")
        return False

      # Security checks
      if not user_data["is_active"]:
        logger.warning("Cached user data shows inactive user")
        SecurityAuditLogger.log_security_event(
          event_type=SecurityEventType.AUTHORIZATION_DENIED,
          details={
            "action": "inactive_user_cache_access",
            "user_id": user_data["id"],
            "email": user_data["email"],
          },
          risk_level="medium",
        )
        return False

      return True

    except Exception as e:
      logger.error(f"User data validation failed: {e}")
      return False

  def _create_cache_signature(self, cache_key: str, data: dict[str, Any]) -> str:
    """Create HMAC signature for cache data integrity with caching optimization."""
    try:
      # Create signature payload
      payload = f"{cache_key}:{json.dumps(data, sort_keys=True)}"
      payload_hash = hashlib.sha256(payload.encode()).hexdigest()

      # Check in-memory cache first for performance
      current_time = time.time()
      if (
        payload_hash in self._signature_cache
        and payload_hash in self._signature_cache_times
        and (current_time - self._signature_cache_times[payload_hash])
        < self.SIGNATURE_CACHE_TTL
      ):
        return self._signature_cache[payload_hash]

      # Clean up expired cache entries if cache is getting too large
      if len(self._signature_cache) > self.MAX_SIGNATURE_CACHE_SIZE:
        self._cleanup_signature_cache()

      # Compute new signature
      signature = hmac.new(
        env.JWT_SECRET_KEY.encode(), payload.encode(), hashlib.sha256
      ).hexdigest()

      # Cache the result
      self._signature_cache[payload_hash] = signature
      self._signature_cache_times[payload_hash] = current_time

      return signature
    except Exception as e:
      logger.error(f"Failed to create cache signature: {e}")
      raise

  def _cleanup_signature_cache(self) -> None:
    """Clean up expired signature cache entries using LRU eviction."""
    try:
      current_time = time.time()

      # First remove expired entries
      expired_keys = [
        key
        for key, cache_time in self._signature_cache_times.items()
        if (current_time - cache_time) > self.SIGNATURE_CACHE_TTL
      ]

      for key in expired_keys:
        self._signature_cache.pop(key, None)
        self._signature_cache_times.pop(key, None)

      # If still over limit, use LRU eviction
      if len(self._signature_cache) > self.MAX_SIGNATURE_CACHE_SIZE:
        # Sort by access time and remove oldest entries
        sorted_entries = sorted(self._signature_cache_times.items(), key=lambda x: x[1])

        # Calculate how many to remove (keep 80% of max size)
        target_size = int(self.MAX_SIGNATURE_CACHE_SIZE * 0.8)
        entries_to_remove = len(self._signature_cache) - target_size

        # Remove oldest entries
        for key, _ in sorted_entries[:entries_to_remove]:
          self._signature_cache.pop(key, None)
          self._signature_cache_times.pop(key, None)

        logger.debug(f"LRU evicted {entries_to_remove} cache entries")

      if expired_keys:
        logger.debug(f"Cleaned up {len(expired_keys)} expired signature cache entries")
    except Exception as e:
      logger.warning(f"Signature cache cleanup failed: {e}")

  def _verify_cache_signature(
    self, cache_key: str, data: dict[str, Any], expected_signature: str
  ) -> bool:
    """Verify cache data integrity using HMAC signature."""
    try:
      actual_signature = self._create_cache_signature(cache_key, data)
      is_valid = secrets.compare_digest(actual_signature, expected_signature)

      if not is_valid:
        logger.error(
          f"Cache signature verification failed for key: {cache_key[:20]}..."
        )
        SecurityAuditLogger.log_security_event(
          event_type=SecurityEventType.SUSPICIOUS_ACTIVITY,
          details={
            "action": "cache_signature_mismatch",
            "cache_key_prefix": cache_key[:20],
          },
          risk_level="high",
        )

      return is_valid
    except Exception as e:
      logger.error(f"Cache signature verification error: {e}")
      return False

  def cache_api_key_validation(
    self, api_key_hash: str, user_data: dict[str, Any], is_active: bool = True
  ) -> None:
    """
    Cache API key validation result with encryption and integrity protection.

    Args:
        api_key_hash: SHA-256 hash of the API key
        user_data: User data to cache (serializable dict)
        is_active: Whether the API key is active
    """
    try:
      # Validate input data integrity for positive cache entries
      # Negative cache entries (is_active=False) may have empty user_data
      if is_active and not self._validate_user_data_integrity(user_data):
        logger.error("Refusing to cache invalid user data")
        return

      cache_key = self._get_api_key_cache_key(api_key_hash)
      cache_data = {
        "user_data": user_data,
        "is_active": is_active,
        "cached_at": datetime.now(UTC).isoformat(),
        "cache_version": self.CACHE_VERSION,
      }

      # Create signature for integrity
      signature = self._create_cache_signature(cache_key, cache_data)
      signature_key = f"{self.CACHE_SIGNATURE_PREFIX}{api_key_hash}"

      # Encrypt sensitive data
      encrypted_data = self._encrypt_cache_data(cache_data)

      # Store encrypted data and signature separately
      pipe = self.redis.pipeline()
      pipe.setex(cache_key, self.ttl, encrypted_data)
      pipe.setex(signature_key, self.ttl, signature)
      pipe.execute()

      logger.debug(f"Cached API key validation with encryption: {api_key_hash[:8]}...")

      # Log security event for cache write
      SecurityAuditLogger.log_security_event(
        event_type=SecurityEventType.AUTH_SUCCESS,
        details={
          "action": "secure_cache_write",
          "cache_type": "api_key_validation",
          "user_id": user_data.get("id"),
          "encrypted": True,
        },
        risk_level="low",
      )

    except Exception as e:
      logger.error(f"Failed to cache API key validation: {e}")
      SecurityAuditLogger.log_security_event(
        event_type=SecurityEventType.SUSPICIOUS_ACTIVITY,
        details={"action": "cache_write_failed", "error": str(e)},
        risk_level="medium",
      )

  def get_cached_api_key_validation(self, api_key_hash: str) -> dict[str, Any] | None:
    """
    Get cached API key validation result with comprehensive security validation.

    Args:
        api_key_hash: SHA-256 hash of the API key

    Returns:
        Cached validation data or None if not found/expired/invalid
    """
    try:
      cache_key = self._get_api_key_cache_key(api_key_hash)
      signature_key = f"{self.CACHE_SIGNATURE_PREFIX}{api_key_hash}"

      # Get both encrypted data and signature
      pipe = self.redis.pipeline()
      pipe.get(cache_key)
      pipe.get(signature_key)
      results = pipe.execute()

      encrypted_data, stored_signature = results

      if not encrypted_data or not stored_signature:
        logger.debug(f"Cache miss for API key: {api_key_hash[:8]}...")
        return None

      # Decrypt the data
      cache_data = self._decrypt_cache_data(encrypted_data)
      if not cache_data:
        logger.warning(f"Failed to decrypt cached API key data: {api_key_hash[:8]}...")
        # Clean up corrupted cache entry
        self.redis.delete(cache_key, signature_key)
        return None

      # Verify signature integrity
      if not self._verify_cache_signature(cache_key, cache_data, stored_signature):
        logger.error(
          f"Cache signature verification failed for API key: {api_key_hash[:8]}..."
        )
        # Clean up compromised cache entry
        self.redis.delete(cache_key, signature_key)
        return None

      # Validate user data integrity
      user_data = cache_data.get("user_data", {})
      if not self._validate_user_data_integrity(user_data):
        logger.error(f"Cached user data failed integrity check: {api_key_hash[:8]}...")
        # Clean up invalid cache entry
        self.redis.delete(cache_key, signature_key)
        return None

      # Check cache freshness
      cached_at = datetime.fromisoformat(cache_data["cached_at"].replace("Z", "+00:00"))
      age_seconds = (datetime.now(UTC) - cached_at).total_seconds()

      if age_seconds > self.MAX_CACHE_AGE_SECONDS:
        logger.debug(f"Cache entry too old: {age_seconds}s")
        self.redis.delete(cache_key, signature_key)
        return None

      # Sliding window refresh: if cache is getting old but still valid, refresh it
      if age_seconds > self.CACHE_REFRESH_THRESHOLD:
        try:
          logger.debug(
            f"Refreshing aging API key cache entry: {api_key_hash[:8]}... (age: {age_seconds}s)"
          )
          # Update the cached_at timestamp to extend the session
          cache_data["cached_at"] = datetime.now(UTC).isoformat()

          # Re-encrypt and store the refreshed data
          encrypted_data = self._encrypt_cache_data(cache_data)
          signature = self._create_cache_signature(cache_key, cache_data)

          # Update both cache entry and signature with fresh TTL
          pipe = self.redis.pipeline()
          pipe.setex(cache_key, self.DEFAULT_TTL, encrypted_data)
          pipe.setex(signature_key, self.DEFAULT_TTL, signature)
          pipe.execute()

          logger.debug(f"Successfully refreshed API key cache: {api_key_hash[:8]}...")
        except Exception as e:
          logger.warning(f"Failed to refresh API key cache entry: {e}")
          # Continue with existing cache data if refresh fails

      logger.debug(f"Secure cache hit for API key: {api_key_hash[:8]}...")

      # Log successful secure cache read (rate limited to reduce noise)
      user_id = user_data.get("id")
      if user_id and self._should_log_audit_event(user_id, "cache_hit"):
        SecurityAuditLogger.log_security_event(
          event_type=SecurityEventType.AUTH_SUCCESS,
          details={
            "action": "secure_cache_read",
            "cache_type": "api_key_validation",
            "user_id": user_id,
            "cache_age_seconds": age_seconds,
          },
          risk_level="low",
        )

      return cache_data

    except Exception as e:
      logger.error(f"Failed to get cached API key validation: {e}")
      # Clean up potentially corrupted cache
      try:
        cache_key = self._get_api_key_cache_key(api_key_hash)
        signature_key = f"{self.CACHE_SIGNATURE_PREFIX}{api_key_hash}"
        self.redis.delete(cache_key, signature_key)
      except Exception:
        pass
      return None

  def cache_graph_access(
    self, api_key_hash: str, graph_id: str, has_access: bool
  ) -> None:
    """
    Cache API key + graph access result.

    Args:
        api_key_hash: SHA-256 hash of the API key
        graph_id: Graph database ID
        has_access: Whether user has access to the graph
    """
    try:
      cache_key = self._get_graph_cache_key(api_key_hash, graph_id)
      cache_data = {
        "has_access": has_access,
        "cached_at": datetime.now(UTC).isoformat(),
      }

      self.redis.setex(cache_key, self.ttl, json.dumps(cache_data))
      logger.debug(f"Cached graph access: {api_key_hash[:8]}... -> {graph_id}")

    except Exception as e:
      logger.error(f"Failed to cache graph access: {e}")

  def get_cached_graph_access(self, api_key_hash: str, graph_id: str) -> bool | None:
    """
    Get cached graph access result.

    Args:
        api_key_hash: SHA-256 hash of the API key
        graph_id: Graph database ID

    Returns:
        Cached access result or None if not found/expired
    """
    try:
      cache_key = self._get_graph_cache_key(api_key_hash, graph_id)
      cached_data = cast(str | None, self.redis.get(cache_key))

      if cached_data:
        data = json.loads(cached_data)
        logger.debug(f"Graph access cache hit: {api_key_hash[:8]}... -> {graph_id}")
        return data["has_access"]

      logger.debug(f"Graph access cache miss: {api_key_hash[:8]}... -> {graph_id}")
      return None

    except Exception as e:
      logger.error(f"Failed to get cached graph access: {e}")
      return None

  def invalidate_api_key(self, api_key_hash: str) -> None:
    """
    Securely invalidate all cached data for an API key.

    Args:
        api_key_hash: SHA-256 hash of the API key
    """
    try:
      # Remove API key validation cache and signature
      api_key_cache_key = self._get_api_key_cache_key(api_key_hash)
      signature_key = f"{self.CACHE_SIGNATURE_PREFIX}{api_key_hash}"

      # Remove all graph access caches for this API key
      pattern = f"{self.GRAPH_CACHE_KEY_PREFIX}{api_key_hash}:*"
      graph_keys = cast(list[str], self.redis.keys(pattern))

      # Remove all signature keys for graph access
      signature_pattern = f"{self.CACHE_SIGNATURE_PREFIX}graph_{api_key_hash}:*"
      signature_keys = cast(list[str], self.redis.keys(signature_pattern))

      # Batch delete all related keys
      keys_to_delete = [api_key_cache_key, signature_key, *graph_keys, *signature_keys]
      if keys_to_delete:
        self.redis.delete(*keys_to_delete)

      logger.info(f"Securely invalidated cache for API key: {api_key_hash[:8]}...")

      # Log cache invalidation for security audit
      SecurityAuditLogger.log_security_event(
        event_type=SecurityEventType.AUTHORIZATION_DENIED,
        details={
          "action": "secure_cache_invalidation",
          "cache_type": "api_key",
          "keys_deleted": len(keys_to_delete),
        },
        risk_level="medium",
      )

    except Exception as e:
      logger.error(f"Failed to invalidate API key cache: {e}")
      SecurityAuditLogger.log_security_event(
        event_type=SecurityEventType.SUSPICIOUS_ACTIVITY,
        details={"action": "cache_invalidation_failed", "error": str(e)},
        risk_level="medium",
      )

  # JWT Caching Methods

  def cache_jwt_validation(self, jwt_token: str, user_data: dict[str, Any]) -> None:
    """
    Cache JWT validation result with encryption and integrity protection.

    Args:
        jwt_token: The JWT token string
        user_data: User data to cache (serializable dict)
    """
    try:
      # Validate input data integrity
      if not self._validate_user_data_integrity(user_data):
        logger.error("Refusing to cache invalid JWT user data")
        return

      jwt_hash = self._hash_jwt_token(jwt_token)
      cache_key = self._get_jwt_cache_key(jwt_hash)
      cache_data = {
        "user_data": user_data,
        "cached_at": datetime.now(UTC).isoformat(),
        "cache_version": self.CACHE_VERSION,
        "token_hash": jwt_hash[:16],  # Partial hash for validation
      }

      # Create signature for integrity
      signature = self._create_cache_signature(cache_key, cache_data)
      signature_key = f"{self.CACHE_SIGNATURE_PREFIX}jwt_{jwt_hash}"

      # Encrypt sensitive data
      encrypted_data = self._encrypt_cache_data(cache_data)

      # Store encrypted data and signature separately
      pipe = self.redis.pipeline()
      pipe.setex(cache_key, self.jwt_ttl, encrypted_data)
      pipe.setex(signature_key, self.jwt_ttl, signature)
      pipe.execute()

      logger.debug(f"Cached JWT validation with encryption: {jwt_hash[:8]}...")

      # Log security event for JWT cache write
      SecurityAuditLogger.log_security_event(
        event_type=SecurityEventType.AUTH_SUCCESS,
        details={
          "action": "secure_jwt_cache_write",
          "user_id": user_data.get("id"),
          "encrypted": True,
        },
        risk_level="low",
      )

    except Exception as e:
      logger.error(f"Failed to cache JWT validation: {e}")
      SecurityAuditLogger.log_security_event(
        event_type=SecurityEventType.SUSPICIOUS_ACTIVITY,
        details={"action": "jwt_cache_write_failed", "error": str(e)},
        risk_level="medium",
      )

  def get_cached_jwt_validation(self, jwt_token: str) -> dict[str, Any] | None:
    """
    Get cached JWT validation result with comprehensive security validation.

    Args:
        jwt_token: The JWT token string

    Returns:
        Cached validation data or None if not found/expired/invalid
    """
    try:
      jwt_hash = self._hash_jwt_token(jwt_token)
      cache_key = self._get_jwt_cache_key(jwt_hash)
      signature_key = f"{self.CACHE_SIGNATURE_PREFIX}jwt_{jwt_hash}"

      # Get both encrypted data and signature
      pipe = self.redis.pipeline()
      pipe.get(cache_key)
      pipe.get(signature_key)
      results = pipe.execute()

      encrypted_data, stored_signature = results

      if not encrypted_data or not stored_signature:
        logger.debug(f"JWT cache miss: {jwt_hash[:8]}...")
        return None

      # Decrypt the data
      cache_data = self._decrypt_cache_data(encrypted_data)
      if not cache_data:
        logger.warning(f"Failed to decrypt cached JWT data: {jwt_hash[:8]}...")
        # Clean up corrupted cache entry
        self.redis.delete(cache_key, signature_key)
        return None

      # Verify signature integrity
      if not self._verify_cache_signature(cache_key, cache_data, stored_signature):
        logger.error(f"JWT cache signature verification failed: {jwt_hash[:8]}...")
        # Clean up compromised cache entry
        self.redis.delete(cache_key, signature_key)
        return None

      # Validate token hash consistency
      expected_hash_prefix = jwt_hash[:16]
      cached_hash_prefix = cache_data.get("token_hash", "")
      if not secrets.compare_digest(expected_hash_prefix, cached_hash_prefix):
        logger.error(f"JWT token hash mismatch: {jwt_hash[:8]}...")
        SecurityAuditLogger.log_security_event(
          event_type=SecurityEventType.SUSPICIOUS_ACTIVITY,
          details={
            "action": "jwt_hash_mismatch",
            "expected_prefix": expected_hash_prefix,
            "cached_prefix": cached_hash_prefix,
          },
          risk_level="high",
        )
        self.redis.delete(cache_key, signature_key)
        return None

      # Validate user data integrity
      user_data = cache_data.get("user_data", {})
      if not self._validate_user_data_integrity(user_data):
        logger.error(f"Cached JWT user data failed integrity check: {jwt_hash[:8]}...")
        self.redis.delete(cache_key, signature_key)
        return None

      # Check cache freshness
      cached_at = datetime.fromisoformat(cache_data["cached_at"].replace("Z", "+00:00"))
      age_seconds = (datetime.now(UTC) - cached_at).total_seconds()

      if age_seconds > self.MAX_CACHE_AGE_SECONDS:
        logger.debug(f"JWT cache entry too old: {age_seconds}s")
        self.redis.delete(cache_key, signature_key)
        return None

      # Sliding window refresh: if cache is getting old but still valid, refresh it
      if age_seconds > self.CACHE_REFRESH_THRESHOLD:
        try:
          logger.debug(
            f"Refreshing aging JWT cache entry: {jwt_hash[:8]}... (age: {age_seconds}s)"
          )
          # Update the cached_at timestamp to extend the session
          cache_data["cached_at"] = datetime.now(UTC).isoformat()

          # Re-encrypt and store the refreshed data
          encrypted_data = self._encrypt_cache_data(cache_data)
          signature = self._create_cache_signature(cache_key, cache_data)

          # Update both cache entry and signature with fresh TTL
          pipe = self.redis.pipeline()
          pipe.setex(cache_key, self.jwt_ttl, encrypted_data)
          pipe.setex(signature_key, self.jwt_ttl, signature)
          pipe.execute()

          logger.debug(f"Successfully refreshed JWT cache: {jwt_hash[:8]}...")
        except Exception as e:
          logger.warning(f"Failed to refresh JWT cache entry: {e}")
          # Continue with existing cache data if refresh fails

      logger.debug(f"Secure JWT cache hit: {jwt_hash[:8]}...")

      # Log successful secure JWT cache read (rate limited to reduce noise)
      user_id = user_data.get("id")
      if user_id and self._should_log_audit_event(user_id, "cache_hit"):
        SecurityAuditLogger.log_security_event(
          event_type=SecurityEventType.AUTH_SUCCESS,
          details={
            "action": "secure_jwt_cache_read",
            "user_id": user_id,
            "cache_age_seconds": age_seconds,
          },
          risk_level="low",
        )

      return cache_data

    except Exception as e:
      logger.error(f"Failed to get cached JWT validation: {e}")
      # Clean up potentially corrupted cache
      try:
        jwt_hash = self._hash_jwt_token(jwt_token)
        cache_key = self._get_jwt_cache_key(jwt_hash)
        signature_key = f"{self.CACHE_SIGNATURE_PREFIX}jwt_{jwt_hash}"
        self.redis.delete(cache_key, signature_key)
      except Exception:
        pass
      return None

  def cache_jwt_graph_access(
    self, user_id: str, graph_id: str, has_access: bool
  ) -> None:
    """
    Cache JWT user + graph access result.

    Args:
        user_id: User ID from JWT token
        graph_id: Graph database ID
        has_access: Whether user has access to the graph
    """
    try:
      cache_key = self._get_jwt_graph_cache_key(user_id, graph_id)
      cache_data = {
        "has_access": has_access,
        "cached_at": datetime.now(UTC).isoformat(),
      }

      # Use shorter TTL for graph access (10 minutes)
      graph_ttl = min(self.jwt_ttl, 600)
      self.redis.setex(cache_key, graph_ttl, json.dumps(cache_data))
      logger.debug(f"Cached JWT graph access: {user_id} -> {graph_id}")

    except Exception as e:
      logger.error(f"Failed to cache JWT graph access: {e}")

  def get_cached_jwt_graph_access(self, user_id: str, graph_id: str) -> bool | None:
    """
    Get cached JWT graph access result.

    Args:
        user_id: User ID from JWT token
        graph_id: Graph database ID

    Returns:
        Cached access result or None if not found/expired
    """
    try:
      cache_key = self._get_jwt_graph_cache_key(user_id, graph_id)
      cached_data = cast(str | None, self.redis.get(cache_key))

      if cached_data:
        data = json.loads(cached_data)
        logger.debug(f"JWT graph access cache hit: {user_id} -> {graph_id}")
        return data["has_access"]

      logger.debug(f"JWT graph access cache miss: {user_id} -> {graph_id}")
      return None

    except Exception as e:
      logger.error(f"Failed to get cached JWT graph access: {e}")
      return None

  def blacklist_jwt_token(self, jwt_token: str, exp_timestamp: int) -> None:
    """
    Add JWT token to blacklist until its natural expiry.

    Args:
        jwt_token: The JWT token string
        exp_timestamp: Token expiry timestamp
    """
    try:
      jwt_hash = self._hash_jwt_token(jwt_token)
      cache_key = self._get_jwt_blacklist_key(jwt_hash)

      # Calculate TTL based on token expiry
      ttl = max(0, exp_timestamp - int(time.time()))
      if ttl > 0:
        self.redis.setex(cache_key, ttl, "blacklisted")
        logger.info(f"Blacklisted JWT token: {jwt_hash[:8]}... (TTL: {ttl}s)")

    except Exception as e:
      logger.error(f"Failed to blacklist JWT token: {e}")

  def is_jwt_blacklisted(self, jwt_token: str) -> bool:
    """
    Check if JWT token is blacklisted.

    Args:
        jwt_token: The JWT token string

    Returns:
        True if blacklisted, False otherwise
    """
    try:
      jwt_hash = self._hash_jwt_token(jwt_token)
      cache_key = self._get_jwt_blacklist_key(jwt_hash)
      return cast(bool, self.redis.exists(cache_key))

    except Exception as e:
      logger.error(f"Failed to check JWT blacklist: {e}")
      return False

  def invalidate_jwt_token(self, jwt_token: str) -> None:
    """
    Securely invalidate cached JWT validation data.

    Args:
        jwt_token: The JWT token string
    """
    try:
      jwt_hash = self._hash_jwt_token(jwt_token)
      cache_key = self._get_jwt_cache_key(jwt_hash)
      signature_key = f"{self.CACHE_SIGNATURE_PREFIX}jwt_{jwt_hash}"

      # Delete both cache data and signature
      self.redis.delete(cache_key, signature_key)

      logger.info(f"Securely invalidated JWT cache: {jwt_hash[:8]}...")

      # Log JWT cache invalidation for security audit
      SecurityAuditLogger.log_security_event(
        event_type=SecurityEventType.AUTHORIZATION_DENIED,
        details={
          "action": "secure_jwt_cache_invalidation",
          "token_hash_prefix": jwt_hash[:8],
        },
        risk_level="medium",
      )

    except Exception as e:
      logger.error(f"Failed to invalidate JWT cache: {e}")
      SecurityAuditLogger.log_security_event(
        event_type=SecurityEventType.SUSPICIOUS_ACTIVITY,
        details={"action": "jwt_cache_invalidation_failed", "error": str(e)},
        risk_level="medium",
      )

  def invalidate_user_jwt_graph_access(
    self, user_id: str, graph_id: str | None = None
  ) -> None:
    """
    Invalidate cached JWT graph access for a user.

    Args:
        user_id: User ID
        graph_id: Specific graph ID, or None to invalidate all graphs for user
    """
    try:
      if graph_id:
        # Invalidate specific graph access for this user
        cache_key = self._get_jwt_graph_cache_key(user_id, graph_id)
        self.redis.delete(cache_key)
      else:
        # Invalidate all graph access for user
        pattern = f"{self.JWT_GRAPH_CACHE_KEY_PREFIX}{user_id}:*"
        keys = cast(list[str], self.redis.keys(pattern))
        if keys:
          self.redis.delete(*keys)

      logger.info(
        f"Invalidated JWT graph access cache for user {user_id}, graph: {graph_id or 'all'}"
      )

    except Exception as e:
      logger.error(f"Failed to invalidate JWT graph access cache: {e}")

  def invalidate_user_graph_access(
    self, user_id: str, graph_id: str | None = None
  ) -> None:
    """
    Invalidate cached graph access for a user.

    Args:
        user_id: User ID
        graph_id: Specific graph ID, or None to invalidate all graphs for user
    """
    try:
      if graph_id:
        # Invalidate specific graph access for all API keys of this user
        # This requires finding all API keys for the user, which would need DB access
        # For now, we'll use a pattern-based approach
        pattern = f"{self.GRAPH_CACHE_KEY_PREFIX}*:{graph_id}"
      else:
        # Invalidate all graph access for user (pattern-based)
        pattern = f"{self.GRAPH_CACHE_KEY_PREFIX}*"

      keys = cast(list[str], self.redis.keys(pattern))
      if keys:
        self.redis.delete(*keys)
        logger.info(
          f"Invalidated graph access cache for user {user_id}, graph: {graph_id or 'all'}"
        )

    except Exception as e:
      logger.error(f"Failed to invalidate user graph access cache: {e}")

  def invalidate_user_data(self, user_id: str) -> None:
    """
    Invalidate all cached data for a user when their profile is updated.

    This includes API key validation caches and JWT validation caches that
    contain outdated user profile information.

    Args:
        user_id: User ID as string
    """
    try:
      invalidated_count = 0

      # Invalidate all API key caches (contains user_data that might be stale)
      # We need to scan all API key cache entries to find ones containing this user
      api_key_pattern = f"{self.CACHE_KEY_PREFIX}*"
      api_key_keys = cast(list[str], self.redis.keys(api_key_pattern))

      for key in api_key_keys:
        try:
          cached_data = cast(str | None, self.redis.get(key))
          if cached_data:
            data = json.loads(cached_data)
            user_data = data.get("user_data", {})
            if user_data.get("id") == user_id:
              self.redis.delete(key)
              invalidated_count += 1
        except Exception as e:
          logger.error(f"Failed to check/invalidate API key cache {key}: {e}")

      # Invalidate all JWT caches (contains user_data that might be stale)
      jwt_pattern = f"{self.JWT_CACHE_KEY_PREFIX}*"
      jwt_keys = cast(list[str], self.redis.keys(jwt_pattern))

      for key in jwt_keys:
        try:
          cached_data = cast(str | None, self.redis.get(key))
          if cached_data:
            data = json.loads(cached_data)
            user_data = data.get("user_data", {})
            if user_data.get("id") == user_id:
              self.redis.delete(key)
              invalidated_count += 1
        except Exception as e:
          logger.error(f"Failed to check/invalidate JWT cache {key}: {e}")

      # Also invalidate JWT graph access for this user
      self.invalidate_user_jwt_graph_access(user_id)

      logger.info(f"Invalidated {invalidated_count} cached entries for user {user_id}")

    except Exception as e:
      logger.error(f"Failed to invalidate user data cache for user {user_id}: {e}")

  def get_cache_stats(self) -> dict[str, Any]:
    """Get cache statistics including security metrics."""
    try:
      info = cast(dict[str, Any], self.redis.info())
      api_key_count = len(cast(list[str], self.redis.keys(f"{self.CACHE_KEY_PREFIX}*")))
      graph_access_count = len(
        cast(list[str], self.redis.keys(f"{self.GRAPH_CACHE_KEY_PREFIX}*"))
      )
      jwt_count = len(cast(list[str], self.redis.keys(f"{self.JWT_CACHE_KEY_PREFIX}*")))
      jwt_graph_count = len(
        cast(list[str], self.redis.keys(f"{self.JWT_GRAPH_CACHE_KEY_PREFIX}*"))
      )
      jwt_blacklist_count = len(
        cast(list[str], self.redis.keys(f"{self.JWT_BLACKLIST_PREFIX}*"))
      )

      # Count signature keys for security validation
      signature_count = len(
        cast(list[str], self.redis.keys(f"{self.CACHE_SIGNATURE_PREFIX}*"))
      )
      validation_count = len(
        cast(list[str], self.redis.keys(f"{self.CACHE_VALIDATION_PREFIX}*"))
      )

      return {
        "connected": True,
        "redis_info": {
          "used_memory_human": info.get("used_memory_human"),
          "connected_clients": info.get("connected_clients"),
          "keyspace_hits": info.get("keyspace_hits"),
          "keyspace_misses": info.get("keyspace_misses"),
        },
        "cache_counts": {
          "api_keys": api_key_count,
          "graph_access": graph_access_count,
          "jwt_tokens": jwt_count,
          "jwt_graph_access": jwt_graph_count,
          "jwt_blacklisted": jwt_blacklist_count,
          "signatures": signature_count,
          "validations": validation_count,
        },
        "ttl_config": {
          "api_key_ttl": self.ttl,
          "jwt_ttl": self.jwt_ttl,
          "max_cache_age": self.MAX_CACHE_AGE_SECONDS,
        },
        "security_config": {
          "cache_version": self.CACHE_VERSION,
          "encryption_enabled": True,
          "signature_verification_enabled": True,
          "validation_failures": self._validation_failures,
          "failure_threshold": self.VALIDATION_FAILURE_THRESHOLD,
        },
      }
    except Exception as e:
      logger.error(f"Failed to get cache stats: {e}")
      return {"connected": False, "error": str(e)}

  def _should_log_audit_event(self, user_id: str, event_type: str) -> bool:
    """
    Check if we should log an audit event based on rate limiting.

    Prevents excessive logging of routine successful operations like cache hits.
    Only logs once per user per period for low-risk events.

    Args:
        user_id: User ID to check rate limiting for
        event_type: Type of event (e.g., 'cache_hit', 'auth_success')

    Returns:
        True if we should log the event, False if rate limited
    """
    try:
      rate_limit_key = f"{self.AUDIT_LOG_RATE_LIMIT_PREFIX}{user_id}:{event_type}"

      # Check if we've already logged this event type for this user recently
      existing = self.redis.get(rate_limit_key)
      if existing:
        return False  # Rate limited

      # Set the rate limit key with TTL
      self.redis.setex(rate_limit_key, self.AUDIT_LOG_RATE_LIMIT_TTL, "1")
      return True  # Not rate limited, log the event

    except Exception as e:
      logger.debug(f"Failed to check audit rate limit: {e}")
      return True  # Default to logging on error

  def perform_cache_integrity_audit(self) -> dict[str, Any]:
    """
    Perform comprehensive cache integrity audit.

    Returns:
        Audit results with any issues found
    """
    try:
      audit_results = {
        "audit_timestamp": datetime.now(UTC).isoformat(),
        "total_keys_scanned": 0,
        "valid_entries": 0,
        "invalid_entries": 0,
        "orphaned_signatures": 0,
        "missing_signatures": 0,
        "corrupted_entries_cleaned": 0,
        "issues_found": [],
      }

      # Scan API key caches
      api_key_pattern = f"{self.CACHE_KEY_PREFIX}*"
      api_keys = cast(list[str], self.redis.keys(api_key_pattern))

      for cache_key in api_keys:
        audit_results["total_keys_scanned"] += 1

        try:
          # Extract hash from cache key
          api_key_hash = cache_key.replace(self.CACHE_KEY_PREFIX, "")
          signature_key = f"{self.CACHE_SIGNATURE_PREFIX}{api_key_hash}"

          encrypted_data = cast(str | None, self.redis.get(cache_key))
          stored_signature = cast(str | None, self.redis.get(signature_key))

          if encrypted_data and stored_signature:
            # Try to decrypt and validate
            cache_data = self._decrypt_cache_data(encrypted_data)
            if cache_data and self._verify_cache_signature(
              cache_key, cache_data, stored_signature
            ):
              audit_results["valid_entries"] += 1
            else:
              audit_results["invalid_entries"] += 1
              audit_results["issues_found"].append(f"Invalid cache entry: {cache_key}")
              # Clean up invalid entry
              self.redis.delete(cache_key, signature_key)
              audit_results["corrupted_entries_cleaned"] += 1
          elif encrypted_data and not stored_signature:
            audit_results["missing_signatures"] += 1
            audit_results["issues_found"].append(f"Missing signature: {cache_key}")
            # Clean up entry without signature
            self.redis.delete(cache_key)
            audit_results["corrupted_entries_cleaned"] += 1
          elif not encrypted_data and stored_signature:
            audit_results["orphaned_signatures"] += 1
            # Clean up orphaned signature
            self.redis.delete(signature_key)

        except Exception as e:
          audit_results["invalid_entries"] += 1
          audit_results["issues_found"].append(
            f"Cache audit error for {cache_key}: {e!s}"
          )

      # Log audit results
      SecurityAuditLogger.log_security_event(
        event_type=SecurityEventType.AUTH_SUCCESS,
        details={
          "action": "cache_integrity_audit",
          "results": audit_results,
        },
        risk_level="low" if audit_results["invalid_entries"] == 0 else "medium",
      )

      return audit_results

    except Exception as e:
      logger.error(f"Cache integrity audit failed: {e}")
      return {
        "audit_timestamp": datetime.now(UTC).isoformat(),
        "error": str(e),
        "audit_failed": True,
      }


# Global API key cache instance
try:
  api_key_cache = APIKeyCache()
  # Log successful cache initialization
  logger.debug("Secure authentication cache system initialized successfully")
except Exception as e:
  logger.error(f"Failed to initialize secure authentication cache system: {e}")
  # Create fallback instance that will fail gracefully
  api_key_cache = None
