"""
Password security utilities for RoboSystems.

Provides secure password validation, hashing, and strength assessment.
"""

import re
import secrets
import string
from dataclasses import dataclass
from typing import List, Optional, Dict, Any
from enum import Enum

import bcrypt


class PasswordStrength(Enum):
  """Password strength levels."""

  VERY_WEAK = "very_weak"
  WEAK = "weak"
  FAIR = "fair"
  GOOD = "good"
  STRONG = "strong"


@dataclass
class PasswordValidationResult:
  """Result of password validation."""

  is_valid: bool
  strength: PasswordStrength
  score: int  # 0-100
  errors: List[str]
  suggestions: List[str]
  character_types: Dict[str, bool]


class PasswordSecurity:
  """Secure password utilities with enterprise-grade requirements."""

  # Password requirements
  MIN_LENGTH = 12
  MAX_LENGTH = 128
  MIN_STRENGTH_SCORE = 60  # Minimum acceptable score

  # Character requirements
  REQUIRE_UPPERCASE = True
  REQUIRE_LOWERCASE = True
  REQUIRE_DIGITS = True
  REQUIRE_SPECIAL = True
  MIN_UNIQUE_CHARS = 8

  # bcrypt configuration
  BCRYPT_ROUNDS = 14  # Higher security for passwords vs API keys

  # Common weak patterns
  WEAK_PATTERNS = [
    r"(.)\1{2,}",  # Repeated characters (aaa, 111)
    r"123456",  # Sequential numbers
    r"abcdef",  # Sequential letters
    r"qwerty",  # Keyboard patterns
    r"password",  # Common words
    r"admin",
    r"user",
    r"login",
    r"welcome",
    r"robosystems",  # Entity name
  ]

  # Common passwords (subset - in production would use larger list)
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
    cls, password: str, email: Optional[str] = None
  ) -> PasswordValidationResult:
    """
    Validate password against security requirements.

    Args:
        password: Password to validate
        email: User email for personalization checks

    Returns:
        PasswordValidationResult with validation details
    """
    errors = []
    suggestions = []
    score = 0

    # Basic length checks
    if len(password) < cls.MIN_LENGTH:
      errors.append(f"Password must be at least {cls.MIN_LENGTH} characters long")
      suggestions.append(f"Add {cls.MIN_LENGTH - len(password)} more characters")
    elif len(password) >= cls.MIN_LENGTH:
      score += 20

    if len(password) > cls.MAX_LENGTH:
      errors.append(f"Password must not exceed {cls.MAX_LENGTH} characters")

    # Character type analysis
    char_types = {
      "uppercase": bool(re.search(r"[A-Z]", password)),
      "lowercase": bool(re.search(r"[a-z]", password)),
      "digits": bool(re.search(r"[0-9]", password)),
      "special": bool(re.search(r'[!@#$%^&*(),.?":{}|<>]', password)),
    }

    # Character requirements
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

    # Unique character count
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

    # Pattern checks
    for pattern in cls.WEAK_PATTERNS:
      if re.search(pattern, password.lower()):
        errors.append("Password contains weak patterns")
        suggestions.append("Avoid repeated characters and common patterns")
        score -= 10
        break

    # Common password check
    if password.lower() in cls.COMMON_PASSWORDS:
      errors.append("Password is too common")
      suggestions.append("Choose a more unique password")
      score -= 20

    # Email similarity check
    if email:
      email_parts = email.lower().split("@")[0].split(".")
      for part in email_parts:
        if len(part) > 3 and part in password.lower():
          errors.append("Password should not contain parts of your email")
          suggestions.append("Avoid using your email or name in the password")
          score -= 15
          break

    # Length bonus
    if len(password) >= 16:
      score += 10
    if len(password) >= 20:
      score += 5

    # Variety bonus
    char_variety = sum(char_types.values())
    if char_variety == 4:
      score += 10

    # Ensure score is in valid range
    score = max(0, min(100, score))

    # Determine strength
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
    """
    Hash a password using bcrypt with high security settings.

    Args:
        password: Plain text password

    Returns:
        Bcrypt hash string
    """
    salt = bcrypt.gensalt(rounds=cls.BCRYPT_ROUNDS)
    return bcrypt.hashpw(password.encode("utf-8"), salt).decode("utf-8")

  @classmethod
  def verify_password(cls, password: str, hashed: str) -> bool:
    """
    Verify a password against its bcrypt hash.

    Args:
        password: Plain text password
        hashed: Stored bcrypt hash

    Returns:
        True if password matches hash
    """
    try:
      return bcrypt.checkpw(password.encode("utf-8"), hashed.encode("utf-8"))
    except (ValueError, TypeError):
      return False

  @classmethod
  def generate_secure_password(cls, length: int = 16) -> str:
    """
    Generate a cryptographically secure password.

    Args:
        length: Password length (minimum 12)

    Returns:
        Secure password meeting all requirements
    """
    if length < cls.MIN_LENGTH:
      length = cls.MIN_LENGTH

    # All available characters for password generation
    all_chars = string.ascii_letters + string.digits + "!@#$%^&*()"

    # Ensure we have enough unique characters by avoiding duplicates
    password_chars = []
    used_chars = set()

    # Add required character types first
    required_pools = [
      string.ascii_uppercase,
      string.ascii_lowercase,
      string.digits,
      "!@#$%^&*()",
    ]

    for pool in required_pools:
      char = secrets.choice(pool)
      password_chars.append(char)
      used_chars.add(char)

    # Fill remaining length, ensuring we meet minimum unique character requirement
    while len(password_chars) < length:
      char = secrets.choice(all_chars)
      password_chars.append(char)
      used_chars.add(char)

      # If we still need more unique chars and we're running low on unused chars,
      # prioritize unused characters
      if len(used_chars) < cls.MIN_UNIQUE_CHARS and len(password_chars) < length:
        unused_chars = [c for c in all_chars if c not in used_chars]
        if unused_chars:
          char = secrets.choice(unused_chars)
          password_chars.append(char)
          used_chars.add(char)

    # Shuffle the password
    secrets.SystemRandom().shuffle(password_chars)

    return "".join(password_chars)

  @classmethod
  def get_password_policy(cls) -> Dict[str, Any]:
    """
    Get the current password policy for frontend display.

    Returns:
        Dictionary describing password requirements
    """
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
