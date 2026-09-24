"""Password policy validation, strength scoring, hashing, and generation."""

import re
import secrets
import string
from dataclasses import dataclass
from enum import Enum
from typing import Any

import bcrypt


class PasswordStrength(Enum):
  VERY_WEAK = "very_weak"
  WEAK = "weak"
  FAIR = "fair"
  GOOD = "good"
  STRONG = "strong"


@dataclass
class PasswordValidationResult:
  is_valid: bool
  strength: PasswordStrength
  score: int  # 0-100
  errors: list[str]
  suggestions: list[str]
  character_types: dict[str, bool]


class PasswordSecurity:
  """Password validation, strength scoring, hashing, and generation."""

  MIN_LENGTH = 12
  MAX_LENGTH = 128
  MIN_STRENGTH_SCORE = 60

  REQUIRE_UPPERCASE = True
  REQUIRE_LOWERCASE = True
  REQUIRE_DIGITS = True
  REQUIRE_SPECIAL = True
  MIN_UNIQUE_CHARS = 8

  BCRYPT_ROUNDS = 14
  # bcrypt 5 raises past 72 bytes; truncating explicitly keeps hashes written
  # under bcrypt 4 (which truncated silently) verifiable.
  BCRYPT_MAX_BYTES = 72

  WEAK_PATTERNS = [
    r"(.)\1{2,}",  # aaa, 111
    r"123456",
    r"abcdef",
    r"qwerty",
    r"password",
    r"admin",
    r"user",
    r"login",
    r"welcome",
    r"robosystems",
  ]

  COMMON_PASSWORDS = {
    "password123",
    "admin123",
    "qwerty123",
    "welcome123",
    "password1",
    "admin1",
    "user123",
    "login123",
    "123456789",
    "password!",
    "Password1",
    "Password123",
    "Admin123",
  }

  @classmethod
  def validate_password(
    cls, password: str, email: str | None = None
  ) -> PasswordValidationResult:
    """
    Validate a password against the policy and score it 0-100.

    Passing ``email`` additionally penalizes passwords containing parts of the
    user's own address. A password is valid only with zero errors AND a score
    of at least MIN_STRENGTH_SCORE.
    """
    errors = []
    suggestions = []
    score = 0

    if len(password) < cls.MIN_LENGTH:
      errors.append(f"Password must be at least {cls.MIN_LENGTH} characters long")
      suggestions.append(f"Add {cls.MIN_LENGTH - len(password)} more characters")
    elif len(password) >= cls.MIN_LENGTH:
      score += 20

    if len(password) > cls.MAX_LENGTH:
      errors.append(f"Password must not exceed {cls.MAX_LENGTH} characters")

    char_types = {
      "uppercase": bool(re.search(r"[A-Z]", password)),
      "lowercase": bool(re.search(r"[a-z]", password)),
      "digits": bool(re.search(r"[0-9]", password)),
      "special": bool(re.search(r'[!@#$%^&*()\-_+=\[\]{};:,.?"|<>\\/>~`]', password)),
    }

    if cls.REQUIRE_UPPERCASE and not char_types["uppercase"]:
      errors.append("Password must contain at least one uppercase letter")
      suggestions.append("Add an uppercase letter (A-Z)")
    elif char_types["uppercase"]:
      score += 15

    if cls.REQUIRE_LOWERCASE and not char_types["lowercase"]:
      errors.append("Password must contain at least one lowercase letter")
      suggestions.append("Add a lowercase letter (a-z)")
    elif char_types["lowercase"]:
      score += 15

    if cls.REQUIRE_DIGITS and not char_types["digits"]:
      errors.append("Password must contain at least one number")
      suggestions.append("Add a number (0-9)")
    elif char_types["digits"]:
      score += 15

    if cls.REQUIRE_SPECIAL and not char_types["special"]:
      errors.append("Password must contain at least one special character")
      suggestions.append("Add a special character (!@#$%^&*)")
    elif char_types["special"]:
      score += 15

    unique_chars = len(set(password))
    if unique_chars < cls.MIN_UNIQUE_CHARS:
      errors.append(
        f"Password must contain at least {cls.MIN_UNIQUE_CHARS} unique characters"
      )
      suggestions.append(
        f"Add {cls.MIN_UNIQUE_CHARS - unique_chars} more unique characters"
      )
    elif unique_chars >= cls.MIN_UNIQUE_CHARS:
      score += 10

    for pattern in cls.WEAK_PATTERNS:
      if re.search(pattern, password.lower()):
        errors.append("Password contains weak patterns")
        suggestions.append("Avoid repeated characters and common patterns")
        score -= 10
        break

    if password.lower() in cls.COMMON_PASSWORDS:
      errors.append("Password is too common")
      suggestions.append("Choose a more unique password")
      score -= 20

    if email:
      email_parts = email.lower().split("@")[0].split(".")
      for part in email_parts:
        if len(part) > 3 and part in password.lower():
          errors.append("Password should not contain parts of your email")
          suggestions.append("Avoid using your email or name in the password")
          score -= 15
          break

    if len(password) >= 16:
      score += 10
    if len(password) >= 20:
      score += 5

    char_variety = sum(char_types.values())
    if char_variety == 4:
      score += 10

    score = max(0, min(100, score))

    if score >= 90:
      strength = PasswordStrength.STRONG
    elif score >= 75:
      strength = PasswordStrength.GOOD
    elif score >= 50:
      strength = PasswordStrength.FAIR
    elif score >= 25:
      strength = PasswordStrength.WEAK
    else:
      strength = PasswordStrength.VERY_WEAK

    return PasswordValidationResult(
      is_valid=len(errors) == 0 and score >= cls.MIN_STRENGTH_SCORE,
      strength=strength,
      score=score,
      errors=errors,
      suggestions=suggestions,
      character_types=char_types,
    )

  @classmethod
  def hash_password(cls, password: str) -> str:
    """Hash a password with bcrypt at BCRYPT_ROUNDS cost."""
    salt = bcrypt.gensalt(rounds=cls.BCRYPT_ROUNDS)
    return bcrypt.hashpw(cls._bcrypt_bytes(password), salt).decode("utf-8")

  @classmethod
  def verify_password(cls, password: str, hashed: str) -> bool:
    """Verify a password against its bcrypt hash; malformed hashes return False."""
    try:
      return bcrypt.checkpw(cls._bcrypt_bytes(password), hashed.encode("utf-8"))
    except (ValueError, TypeError):
      return False

  @classmethod
  async def hash_password_async(cls, password: str) -> str:
    """hash_password off the event loop — cost-14 bcrypt is ~0.5-1 s of CPU
    that would otherwise stall every tenant sharing the loop."""
    import asyncio

    return await asyncio.to_thread(cls.hash_password, password)

  @classmethod
  async def verify_password_async(cls, password: str, hashed: str) -> bool:
    """verify_password off the event loop; see hash_password_async."""
    import asyncio

    return await asyncio.to_thread(cls.verify_password, password, hashed)

  # Exists only to burn a real verification's bcrypt work; never matches.
  _TIMING_EQUALIZER_HASH = (
    "$2b$14$QZqnSEVBjqfiGXIKfN1osuDqkogiNCpSZW3UFxutpEGp1hB7VxkZe"
  )

  @classmethod
  async def equalize_verify_timing(cls) -> None:
    """Burn one bcrypt verification's worth of work, off the loop.

    The login miss path calls this so it costs the same as a real check and
    the generic 401 doesn't leak which accounts exist.
    """
    await cls.verify_password_async(
      "timing-equalizer-candidate", cls._TIMING_EQUALIZER_HASH
    )

  @classmethod
  def _bcrypt_bytes(cls, password: str) -> bytes:
    """Encode to bcrypt's 72-byte input; must stay byte-exact, since stored
    hashes were produced against the truncated input."""
    return password.encode("utf-8")[: cls.BCRYPT_MAX_BYTES]

  @classmethod
  def generate_secure_password(cls, length: int = 16) -> str:
    """
    Generate a cryptographically secure password meeting the policy.

    ``length`` is raised to MIN_LENGTH if smaller.
    """
    if length < cls.MIN_LENGTH:
      length = cls.MIN_LENGTH

    all_chars = string.ascii_letters + string.digits + "!@#$%^&*()"

    required_pools = [
      string.ascii_uppercase,
      string.ascii_lowercase,
      string.digits,
      "!@#$%^&*()",
    ]

    max_attempts = 10
    password = ""
    for _ in range(max_attempts):
      password_chars = []
      used_chars = set()

      for pool in required_pools:
        char = secrets.choice(pool)
        password_chars.append(char)
        used_chars.add(char)

      while len(password_chars) < length:
        char = secrets.choice(all_chars)
        password_chars.append(char)
        used_chars.add(char)

        if len(used_chars) < cls.MIN_UNIQUE_CHARS and len(password_chars) < length:
          unused_chars = [c for c in all_chars if c not in used_chars]
          if unused_chars:
            char = secrets.choice(unused_chars)
            password_chars.append(char)
            used_chars.add(char)

      secrets.SystemRandom().shuffle(password_chars)
      password = "".join(password_chars)

      has_weak_pattern = any(
        re.search(pattern, password.lower()) for pattern in cls.WEAK_PATTERNS
      )
      if not has_weak_pattern:
        return password

    return password

  @classmethod
  def get_password_policy(cls) -> dict[str, Any]:
    """The password policy, for frontend display."""
    return {
      "min_length": cls.MIN_LENGTH,
      "max_length": cls.MAX_LENGTH,
      "require_uppercase": cls.REQUIRE_UPPERCASE,
      "require_lowercase": cls.REQUIRE_LOWERCASE,
      "require_digits": cls.REQUIRE_DIGITS,
      "require_special": cls.REQUIRE_SPECIAL,
      "min_unique_chars": cls.MIN_UNIQUE_CHARS,
      "min_strength_score": cls.MIN_STRENGTH_SCORE,
      "special_chars": "!@#$%^&*()",
    }
