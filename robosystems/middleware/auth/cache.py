"""API key and JWT caching in Valkey/Redis.

Cached auth records are encrypted with a rotating Fernet key and carry an
HMAC signature under a separate key. A read that fails decryption, signature
verification, user-data validation, or the maximum-age check evicts the entry
and returns a miss, so tampering degrades to a database lookup rather than a
trusted answer.
"""

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
from ...config.defaults import CacheDefaults
from ...config.tuning import TuningConfig
from ...config.valkey_registry import ValkeyDatabase, create_redis_client
from ...logger import logger
from ...security import SecurityAuditLogger, SecurityEventType


class APIKeyCache:
  """Encrypted, signature-verified cache for API key and JWT auth records."""

  @classmethod
  def get_default_ttl(cls) -> int:
    """Default TTL, runtime-tunable via SSM."""
    return TuningConfig.get_cache_api_key_ttl()

  CACHE_KEY_PREFIX = "apikey:"
  GRAPH_CACHE_KEY_PREFIX = "apikey_graph:"
  USER_DATA_PREFIX = "user:"
  AUDIT_LOG_RATE_LIMIT_PREFIX = "audit_rate_limit:"
  AUDIT_LOG_RATE_LIMIT_TTL = CacheDefaults.SHORT  # only log once per user per 5 minutes
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
  MAX_CACHE_AGE_SECONDS = CacheDefaults.JWT_TTL  # max age regardless of TTL
  CACHE_REFRESH_THRESHOLD = 1200  # 20 minutes - refresh cache if older than this
  VALIDATION_FAILURE_THRESHOLD = 5  # Max validation failures before security alert

  # Key rotation configuration
  KEY_ROTATION_INTERVAL = 86400  # 24 hours - rotate encryption keys daily
  KEY_ROTATION_PREFIX = "key_rotation:"
  KEY_GENERATION_PREFIX = "key_gen:"

  # Signature optimization configuration
  SIGNATURE_CACHE_PREFIX = "sig_cache:"
  SIGNATURE_CACHE_TTL = CacheDefaults.SHORT  # cache computed signatures
  MAX_SIGNATURE_CACHE_SIZE = 1000  # Limit in-memory signature cache size

  def __init__(self):
    self._redis = None
    # TTLs are runtime-tunable via SSM Parameter Store.
    self.ttl = TuningConfig.get_cache_api_key_ttl()
    self.jwt_ttl = TuningConfig.get_cache_jwt_ttl()

    self._encryption_key = None
    self._cipher = None

    self._validation_failures = 0

    self._signature_cache: dict[str, str] = {}
    self._signature_cache_times: dict[str, float] = {}

  @property
  def redis(self) -> redis.Redis:
    """Get Redis connection, creating if needed."""
    if self._redis is None:
      try:
        self._redis = create_redis_client(ValkeyDatabase.AUTH)
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
    """Derive the cache encryption key, rotating it when it is due."""
    rotation_key = f"{self.KEY_ROTATION_PREFIX}last_rotation"
    try:
      last_rotation = self.redis.get(rotation_key)
      current_time = time.time()

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
          should_rotate = True

      if should_rotate:
        # Stamp the rotation time BEFORE rotating: `_rotate_encryption_key`
        # re-derives the key, and an unset timestamp would recurse forever.
        self.redis.setex(
          rotation_key, self.KEY_ROTATION_INTERVAL * 2, str(current_time)
        )
        self._rotate_encryption_key()
    except Exception as e:
      logger.warning(f"Key rotation check failed, using default key: {e}")

    generation_key = f"{self.KEY_GENERATION_PREFIX}current"
    key_component = ""
    try:
      stored_component = self.redis.get(generation_key)
      if stored_component and isinstance(stored_component, str):
        key_component = stored_component
    except Exception:
      # Redis unavailable: derive from the static secret alone.
      pass

    # Salt binds the key to the environment and the current rotation.
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
    """Rotate the cache encryption key, rolling back if the new one fails.

    Validates the new key with an encrypt/decrypt round trip before
    committing, then evicts entries the new key cannot read.
    """
    old_key_component = None
    old_encryption_key = self._encryption_key
    old_cipher = self._cipher
    generation_key = f"{self.KEY_GENERATION_PREFIX}current"

    try:
      old_key_component = self.redis.get(generation_key)

      new_key_component = secrets.token_hex(32)

      self.redis.setex(
        generation_key, self.KEY_ROTATION_INTERVAL * 2, new_key_component
      )

      # Clear the lazy cipher so it regenerates from the new component.
      self._encryption_key = None
      self._cipher = None

      test_data = {"test": "rotation_validation", "timestamp": time.time()}
      encrypted = self._encrypt_cache_data(test_data)
      decrypted = self._decrypt_cache_data(encrypted)

      if decrypted != test_data:
        raise ValueError(
          "Key rotation validation failed: encryption/decryption mismatch"
        )

      logger.info("Cache encryption key rotated successfully")

      self._cleanup_incompatible_cache_entries()

    except Exception as e:
      logger.error(f"Key rotation failed: {e}")

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
        raise

      raise

  def _cleanup_incompatible_cache_entries(self) -> None:
    """Evict cache entries the current encryption key cannot decrypt."""
    try:
      cache_patterns = [
        f"{self.CACHE_KEY_PREFIX}*",
        f"{self.USER_DATA_PREFIX}*",
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
    """Encrypt cache data, stamping it with the format version and a nonce."""
    try:
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
    """Decrypt cache data, returning None if it is unreadable, the wrong
    format version, or older than `MAX_CACHE_AGE_SECONDS`.
    """
    try:
      encrypted_bytes = base64.urlsafe_b64decode(encrypted_data.encode())
      decrypted = self.cipher.decrypt(encrypted_bytes)
      protected_data = json.loads(decrypted.decode())

      if protected_data.get("version") != self.CACHE_VERSION:
        logger.warning(
          f"Cache version mismatch: {protected_data.get('version')} != {self.CACHE_VERSION}"
        )
        return None

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
    """Reject cached user data that is malformed or marks the user inactive."""
    try:
      required_fields = ["id", "email", "is_active"]
      for field in required_fields:
        if field not in user_data:
          logger.warning(f"Missing required field in cached user data: {field}")
          return False

      if not isinstance(user_data["id"], str) or not user_data["id"]:
        logger.warning("Invalid user ID in cached data")
        return False

      if not isinstance(user_data["email"], str) or "@" not in user_data["email"]:
        logger.warning("Invalid email in cached data")
        return False

      if not isinstance(user_data["is_active"], bool):
        logger.warning("Invalid is_active field in cached data")
        return False

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
    """HMAC-sign cache data, memoizing recent signatures in process."""
    try:
      payload = f"{cache_key}:{json.dumps(data, sort_keys=True)}"
      payload_hash = hashlib.sha256(payload.encode()).hexdigest()

      current_time = time.time()
      if (
        payload_hash in self._signature_cache
        and payload_hash in self._signature_cache_times
        and (current_time - self._signature_cache_times[payload_hash])
        < self.SIGNATURE_CACHE_TTL
      ):
        return self._signature_cache[payload_hash]

      if len(self._signature_cache) > self.MAX_SIGNATURE_CACHE_SIZE:
        self._cleanup_signature_cache()

      signature = hmac.new(
        env.JWT_SECRET_KEY.encode(), payload.encode(), hashlib.sha256
      ).hexdigest()

      self._signature_cache[payload_hash] = signature
      self._signature_cache_times[payload_hash] = current_time

      return signature
    except Exception as e:
      logger.error(f"Failed to create cache signature: {e}")
      raise

  def _cleanup_signature_cache(self) -> None:
    """Drop expired signature-cache entries, then LRU-evict down to 80% of
    `MAX_SIGNATURE_CACHE_SIZE` if still over the limit.
    """
    try:
      current_time = time.time()

      expired_keys = [
        key
        for key, cache_time in self._signature_cache_times.items()
        if (current_time - cache_time) > self.SIGNATURE_CACHE_TTL
      ]

      for key in expired_keys:
        self._signature_cache.pop(key, None)
        self._signature_cache_times.pop(key, None)

      if len(self._signature_cache) > self.MAX_SIGNATURE_CACHE_SIZE:
        sorted_entries = sorted(self._signature_cache_times.items(), key=lambda x: x[1])

        target_size = int(self.MAX_SIGNATURE_CACHE_SIZE * 0.8)
        entries_to_remove = len(self._signature_cache) - target_size

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
    """Cache an API key validation result, encrypted and signed.

    `is_active=False` writes a negative entry, which may carry empty
    `user_data` and so skips the user-data integrity check.
    """
    try:
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

      signature = self._create_cache_signature(cache_key, cache_data)
      signature_key = f"{self.CACHE_SIGNATURE_PREFIX}{api_key_hash}"

      encrypted_data = self._encrypt_cache_data(cache_data)

      # Data and signature live under separate keys.
      pipe = self.redis.pipeline()
      pipe.setex(cache_key, self.ttl, encrypted_data)
      pipe.setex(signature_key, self.ttl, signature)
      pipe.execute()

      logger.debug(f"Cached API key validation with encryption: {api_key_hash[:8]}...")

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
    """Return a cached API key validation, or None on any miss.

    Every failed check — decryption, signature, user-data integrity, age —
    deletes the entry and reports a miss, so a tampered or stale record can
    never authenticate a request. Entries past `CACHE_REFRESH_THRESHOLD` but
    still within `MAX_CACHE_AGE_SECONDS` are re-stamped on read, giving
    active sessions a sliding window.
    """
    try:
      cache_key = self._get_api_key_cache_key(api_key_hash)
      signature_key = f"{self.CACHE_SIGNATURE_PREFIX}{api_key_hash}"

      pipe = self.redis.pipeline()
      pipe.get(cache_key)
      pipe.get(signature_key)
      results = pipe.execute()

      encrypted_data, stored_signature = results

      if not encrypted_data or not stored_signature:
        logger.debug(f"Cache miss for API key: {api_key_hash[:8]}...")
        return None

      cache_data = self._decrypt_cache_data(encrypted_data)
      if not cache_data:
        logger.warning(f"Failed to decrypt cached API key data: {api_key_hash[:8]}...")
        self.redis.delete(cache_key, signature_key)
        return None

      if not self._verify_cache_signature(cache_key, cache_data, stored_signature):
        logger.error(
          f"Cache signature verification failed for API key: {api_key_hash[:8]}..."
        )
        self.redis.delete(cache_key, signature_key)
        return None

      # Negative entries carry empty user_data, so skip the check for them.
      is_active = cache_data.get("is_active", True)
      user_data = cache_data.get("user_data", {})
      if is_active and not self._validate_user_data_integrity(user_data):
        logger.error(f"Cached user data failed integrity check: {api_key_hash[:8]}...")
        self.redis.delete(cache_key, signature_key)
        return None

      cached_at = datetime.fromisoformat(cache_data["cached_at"].replace("Z", "+00:00"))
      age_seconds = (datetime.now(UTC) - cached_at).total_seconds()

      if age_seconds > self.MAX_CACHE_AGE_SECONDS:
        logger.debug(f"Cache entry too old: {age_seconds}s")
        self.redis.delete(cache_key, signature_key)
        return None

      if age_seconds > self.CACHE_REFRESH_THRESHOLD:
        try:
          logger.debug(
            f"Refreshing aging API key cache entry: {api_key_hash[:8]}... (age: {age_seconds}s)"
          )
          cache_data["cached_at"] = datetime.now(UTC).isoformat()

          encrypted_data = self._encrypt_cache_data(cache_data)
          signature = self._create_cache_signature(cache_key, cache_data)

          pipe = self.redis.pipeline()
          pipe.setex(cache_key, self.get_default_ttl(), encrypted_data)
          pipe.setex(signature_key, self.get_default_ttl(), signature)
          pipe.execute()

          logger.debug(f"Successfully refreshed API key cache: {api_key_hash[:8]}...")
        except Exception as e:
          logger.warning(f"Failed to refresh API key cache entry: {e}")
          # A failed refresh is harmless; the existing entry is still valid.

      logger.debug(f"Secure cache hit for API key: {api_key_hash[:8]}...")

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
    """Cache whether an API key grants access to a graph."""
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
    """Return the cached access decision for an API key + graph, or None."""
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

  def cache_jwt_user_data(
    self, user_id: str, user_data: dict[str, Any], session_version: int
  ) -> None:
    """Cache JWT-authenticated user data by user ID and session version.

    This cache is keyed by user, not token. A cached entry is only reusable
    when the JWT's session_version claim matches the cached session_version.
    """
    try:
      if not self._validate_user_data_integrity(user_data):
        logger.error("Refusing to cache invalid JWT user data")
        return

      if str(user_data.get("id")) != str(user_id):
        logger.error("Refusing to cache JWT user data for mismatched user_id")
        return

      cache_key = self._get_user_cache_key(user_id)
      cache_data = {
        "user_data": user_data,
        "session_version": int(session_version),
        "cached_at": datetime.now(UTC).isoformat(),
        "cache_version": self.CACHE_VERSION,
      }
      signature_key = f"{self.CACHE_SIGNATURE_PREFIX}user:{user_id}"
      signature = self._create_cache_signature(cache_key, cache_data)
      encrypted_data = self._encrypt_cache_data(cache_data)

      pipe = self.redis.pipeline()
      pipe.setex(cache_key, self.jwt_ttl, encrypted_data)
      pipe.setex(signature_key, self.jwt_ttl, signature)
      pipe.execute()

      logger.debug(f"Cached JWT user data: {user_id}")

    except Exception as e:
      logger.error(f"Failed to cache JWT user data for {user_id}: {e}")
      SecurityAuditLogger.log_security_event(
        event_type=SecurityEventType.SUSPICIOUS_ACTIVITY,
        details={"action": "jwt_user_cache_write_failed", "error": str(e)},
        risk_level="medium",
      )

  def get_cached_jwt_user_data(
    self, user_id: str, session_version: int
  ) -> dict[str, Any] | None:
    """Get cached JWT user data when session_version matches the token."""
    try:
      cache_key = self._get_user_cache_key(user_id)
      signature_key = f"{self.CACHE_SIGNATURE_PREFIX}user:{user_id}"

      pipe = self.redis.pipeline()
      pipe.get(cache_key)
      pipe.get(signature_key)
      encrypted_data, stored_signature = pipe.execute()

      if not encrypted_data or not stored_signature:
        logger.debug(f"JWT user cache miss: {user_id}")
        return None

      cache_data = self._decrypt_cache_data(encrypted_data)
      if not cache_data:
        logger.warning(f"Failed to decrypt cached JWT user data: {user_id}")
        self.redis.delete(cache_key, signature_key)
        return None

      if not self._verify_cache_signature(cache_key, cache_data, stored_signature):
        logger.error(f"JWT user cache signature verification failed: {user_id}")
        self.redis.delete(cache_key, signature_key)
        return None

      cached_version = int(cache_data.get("session_version", -1))
      if cached_version != int(session_version):
        logger.debug(
          f"JWT user cache session_version mismatch for {user_id}: "
          f"cache={cached_version}, token={session_version}"
        )
        # Proactively evict the stale entry. Under normal operation
        # ``_invalidate_auth_cache`` already deleted this on the
        # version bump; reaching this branch usually means that
        # delete failed (the documented fail-open window). Removing
        # it here halves the DB overhead for the rest of the cache
        # TTL by preventing every subsequent request from also
        # taking the cache→DB fallthrough.
        try:
          self.redis.delete(cache_key, signature_key)
        except Exception as evict_err:
          logger.debug(
            f"Failed to evict stale JWT user cache for {user_id}: {evict_err}"
          )
        return None

      user_data = cache_data.get("user_data", {})
      if str(user_data.get("id")) != str(user_id):
        logger.error(f"JWT user cache user_id mismatch: {user_id}")
        self.redis.delete(cache_key, signature_key)
        return None

      if not self._validate_user_data_integrity(user_data):
        logger.error(f"Cached JWT user data failed integrity check: {user_id}")
        self.redis.delete(cache_key, signature_key)
        return None

      cached_at = datetime.fromisoformat(cache_data["cached_at"].replace("Z", "+00:00"))
      age_seconds = (datetime.now(UTC) - cached_at).total_seconds()
      if age_seconds > self.MAX_CACHE_AGE_SECONDS:
        logger.debug(f"JWT user cache entry too old: {age_seconds}s")
        self.redis.delete(cache_key, signature_key)
        return None

      logger.debug(f"JWT user cache hit: {user_id}")
      return cache_data

    except Exception as e:
      logger.error(f"Failed to get cached JWT user data for {user_id}: {e}")
      try:
        cache_key = self._get_user_cache_key(user_id)
        signature_key = f"{self.CACHE_SIGNATURE_PREFIX}user:{user_id}"
        self.redis.delete(cache_key, signature_key)
      except Exception as cleanup_err:
        # Best-effort cleanup; failure here is non-fatal (the outer return
        # is already None) but worth a debug breadcrumb for diagnosis.
        logger.debug(
          f"Cleanup of corrupted JWT user cache for {user_id} also failed: "
          f"{cleanup_err}"
        )
      return None

  def invalidate_jwt_user_data(self, user_id: str) -> bool:
    """Invalidate JWT user-data cache for a user.

    Returns True on success, False on Redis failure. Callers
    (notably ``User._invalidate_auth_cache``) use this to drive retries
    and surface critical failures — do NOT swallow exceptions silently
    here, the bool is the contract.
    """
    try:
      cache_key = self._get_user_cache_key(user_id)
      signature_key = f"{self.CACHE_SIGNATURE_PREFIX}user:{user_id}"
      self.redis.delete(cache_key, signature_key)
      logger.info(f"Invalidated JWT user data cache for user {user_id}")
      return True
    except Exception as e:
      logger.error(f"Failed to invalidate JWT user data cache for {user_id}: {e}")
      return False

  def invalidate_api_key(self, api_key_hash: str) -> bool:
    """Drop every cached record for an API key: validation, signature, and
    per-graph access decisions. Returns True when the deletes took; False
    means an entry may survive until TTL, which revocation callers must
    treat as incomplete.
    """
    try:
      api_key_cache_key = self._get_api_key_cache_key(api_key_hash)
      signature_key = f"{self.CACHE_SIGNATURE_PREFIX}{api_key_hash}"

      pattern = f"{self.GRAPH_CACHE_KEY_PREFIX}{api_key_hash}:*"
      graph_keys = cast(list[str], self.redis.keys(pattern))

      signature_pattern = f"{self.CACHE_SIGNATURE_PREFIX}graph_{api_key_hash}:*"
      signature_keys = cast(list[str], self.redis.keys(signature_pattern))

      keys_to_delete = [api_key_cache_key, signature_key, *graph_keys, *signature_keys]
      if keys_to_delete:
        self.redis.delete(*keys_to_delete)

      logger.info(f"Securely invalidated cache for API key: {api_key_hash[:8]}...")

      SecurityAuditLogger.log_security_event(
        event_type=SecurityEventType.AUTHORIZATION_DENIED,
        details={
          "action": "secure_cache_invalidation",
          "cache_type": "api_key",
          "keys_deleted": len(keys_to_delete),
        },
        risk_level="medium",
      )
      return True

    except Exception as e:
      logger.error(f"Failed to invalidate API key cache: {e}")
      SecurityAuditLogger.log_security_event(
        event_type=SecurityEventType.SUSPICIOUS_ACTIVITY,
        details={"action": "cache_invalidation_failed", "error": str(e)},
        risk_level="medium",
      )
      return False

  def cache_jwt_graph_access(
    self, user_id: str, graph_id: str, has_access: bool
  ) -> None:
    """Cache whether a JWT-authenticated user may access a graph.

    Capped at 10 minutes regardless of `jwt_ttl` so a revoked grant stops
    working promptly.
    """
    try:
      cache_key = self._get_jwt_graph_cache_key(user_id, graph_id)
      cache_data = {
        "has_access": has_access,
        "cached_at": datetime.now(UTC).isoformat(),
      }

      graph_ttl = min(self.jwt_ttl, 600)
      self.redis.setex(cache_key, graph_ttl, json.dumps(cache_data))
      logger.debug(f"Cached JWT graph access: {user_id} -> {graph_id}")

    except Exception as e:
      logger.error(f"Failed to cache JWT graph access: {e}")

  def get_cached_jwt_graph_access(self, user_id: str, graph_id: str) -> bool | None:
    """Return the cached access decision for a JWT user + graph, or None."""
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
    """Blacklist a JWT until its own expiry.

    The TTL matches the remaining token lifetime, so the entry costs nothing
    once the token would have expired anyway.
    """
    try:
      jwt_hash = self._hash_jwt_token(jwt_token)
      cache_key = self._get_jwt_blacklist_key(jwt_hash)

      ttl = max(0, exp_timestamp - int(time.time()))
      if ttl > 0:
        self.redis.setex(cache_key, ttl, "blacklisted")
        logger.info(f"Blacklisted JWT token: {jwt_hash[:8]}... (TTL: {ttl}s)")

    except Exception as e:
      logger.error(f"Failed to blacklist JWT token: {e}")

  def is_jwt_blacklisted(self, jwt_token: str) -> bool:
    """Return True if this JWT has been blacklisted."""
    try:
      jwt_hash = self._hash_jwt_token(jwt_token)
      cache_key = self._get_jwt_blacklist_key(jwt_hash)
      return cast(bool, self.redis.exists(cache_key))

    except Exception as e:
      logger.error(f"Failed to check JWT blacklist: {e}")
      return False

  def invalidate_user_jwt_graph_access(
    self, user_id: str, graph_id: str | None = None
  ) -> bool:
    """Invalidate a user's cached JWT graph access, or all of it when
    `graph_id` is None.

    Returns True on success, False on Redis failure. The bool is the
    contract for callers driving retries — see ``invalidate_jwt_user_data``.
    """
    try:
      if graph_id:
        cache_key = self._get_jwt_graph_cache_key(user_id, graph_id)
        self.redis.delete(cache_key)
      else:
        pattern = f"{self.JWT_GRAPH_CACHE_KEY_PREFIX}{user_id}:*"
        keys = cast(list[str], self.redis.keys(pattern))
        if keys:
          self.redis.delete(*keys)

      logger.info(
        f"Invalidated JWT graph access cache for user {user_id}, graph: {graph_id or 'all'}"
      )
      return True

    except Exception as e:
      logger.error(f"Failed to invalidate JWT graph access cache: {e}")
      return False

  def invalidate_user_graph_access(
    self, user_id: str, graph_id: str | None = None
  ) -> None:
    """Invalidate cached API-key graph access, for one graph or all of them.

    The API-key cache is keyed by key hash, not user, so this matches by
    pattern and clears every key's entry for the graph rather than only
    this user's — over-invalidating instead of leaving a stale grant.
    """
    try:
      if graph_id:
        pattern = f"{self.GRAPH_CACHE_KEY_PREFIX}*:{graph_id}"
      else:
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
    """Drop every cache entry embedding this user's profile.

    Covers the user-keyed JWT cache, any API key or JWT entry whose payload
    names this user, and their JWT graph access. Call after a profile change
    so no surface keeps serving the old record.
    """
    try:
      invalidated_count = 0

      user_cache_key = self._get_user_cache_key(user_id)
      user_signature_key = f"{self.CACHE_SIGNATURE_PREFIX}user:{user_id}"
      self.redis.delete(user_cache_key, user_signature_key)
      invalidated_count += 1

      api_key_pattern = f"{self.CACHE_KEY_PREFIX}*"
      api_key_keys = cast(list[str], self.redis.keys(api_key_pattern))

      for key in api_key_keys:
        try:
          cached_data = cast(str | None, self.redis.get(key))
          if cached_data:
            # Entries are written through _encrypt_cache_data, so they are
            # base64(fernet(json)) — a plain json.loads raised on every entry
            # and the except below swallowed it, which meant no API-key entry
            # was ever evicted. Decrypt through the same helper the read path
            # uses; it returns the inner payload, so user_data is at the top.
            data = self._decrypt_cache_data(cached_data)
            if data is None:
              continue
            user_data = data.get("user_data", {})
            if user_data.get("id") == user_id:
              self.redis.delete(key)
              invalidated_count += 1
        except Exception as e:
          logger.error(f"Failed to check/invalidate API key cache {key}: {e}")

      jwt_pattern = f"{self.JWT_CACHE_KEY_PREFIX}*"
      jwt_keys = cast(list[str], self.redis.keys(jwt_pattern))

      for key in jwt_keys:
        try:
          cached_data = cast(str | None, self.redis.get(key))
          if cached_data:
            # Same defect as the API-key loop above: JWT entries are written
            # encrypted too, so json.loads never succeeded here either.
            data = self._decrypt_cache_data(cached_data)
            if data is None:
              continue
            user_data = data.get("user_data", {})
            if user_data.get("id") == user_id:
              self.redis.delete(key)
              invalidated_count += 1
        except Exception as e:
          logger.error(f"Failed to check/invalidate JWT cache {key}: {e}")

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
    """Return True unless this (user, event_type) was logged recently.

    Keeps routine low-risk events such as cache hits from flooding the
    audit stream. Errors default to logging.
    """
    try:
      rate_limit_key = f"{self.AUDIT_LOG_RATE_LIMIT_PREFIX}{user_id}:{event_type}"

      existing = self.redis.get(rate_limit_key)
      if existing:
        return False  # Rate limited

      self.redis.setex(rate_limit_key, self.AUDIT_LOG_RATE_LIMIT_TTL, "1")
      return True  # Not rate limited, log the event

    except Exception as e:
      logger.debug(f"Failed to check audit rate limit: {e}")
      return True  # Default to logging on error

  def perform_cache_integrity_audit(self) -> dict[str, Any]:
    """Scan every cache entry and report decryption, signature, and
    orphaned/missing-signature problems.
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

      api_key_pattern = f"{self.CACHE_KEY_PREFIX}*"
      api_keys = cast(list[str], self.redis.keys(api_key_pattern))

      for cache_key in api_keys:
        audit_results["total_keys_scanned"] += 1

        try:
          api_key_hash = cache_key.replace(self.CACHE_KEY_PREFIX, "")
          signature_key = f"{self.CACHE_SIGNATURE_PREFIX}{api_key_hash}"

          encrypted_data = cast(str | None, self.redis.get(cache_key))
          stored_signature = cast(str | None, self.redis.get(signature_key))

          if encrypted_data and stored_signature:
            cache_data = self._decrypt_cache_data(encrypted_data)
            if cache_data and self._verify_cache_signature(
              cache_key, cache_data, stored_signature
            ):
              audit_results["valid_entries"] += 1
            else:
              audit_results["invalid_entries"] += 1
              audit_results["issues_found"].append(f"Invalid cache entry: {cache_key}")
              self.redis.delete(cache_key, signature_key)
              audit_results["corrupted_entries_cleaned"] += 1
          elif encrypted_data and not stored_signature:
            audit_results["missing_signatures"] += 1
            audit_results["issues_found"].append(f"Missing signature: {cache_key}")
            self.redis.delete(cache_key)
            audit_results["corrupted_entries_cleaned"] += 1
          elif not encrypted_data and stored_signature:
            audit_results["orphaned_signatures"] += 1
            self.redis.delete(signature_key)

        except Exception as e:
          audit_results["invalid_entries"] += 1
          audit_results["issues_found"].append(
            f"Cache audit error for {cache_key}: {e!s}"
          )

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


# Module-level singleton. Left as None when Redis is unreachable at import
# time; callers must treat a None cache as an unconditional miss.
try:
  api_key_cache = APIKeyCache()
  logger.debug("Secure authentication cache system initialized successfully")
except Exception as e:
  logger.error(f"Failed to initialize secure authentication cache system: {e}")
  api_key_cache = None
