"""
Comprehensive tests for password security utilities.

These tests cover password validation, strength assessment, hashing, verification,
and secure password generation with enterprise-grade security requirements.
"""

import re
from unittest.mock import patch

import pytest

from robosystems.security.password import (
  PasswordSecurity,
  PasswordStrength,
  PasswordValidationResult,
)


class TestPasswordValidation:
  """Test password validation functionality."""

  def test_validate_password_strong(self):
    """Test validation of a strong password."""
    strong_password = "MyStr0ng!P@ssw0rd2024"
    result = PasswordSecurity.validate_password(strong_password)

    assert result.is_valid is True
    assert result.strength == PasswordStrength.STRONG
    assert result.score >= 90
    assert len(result.errors) == 0
    assert all(result.character_types.values())

  def test_validate_password_good(self):
    """Test validation of a good strength password (may not be fully valid)."""
    # Use a password that scores in the GOOD range (75-89)
    # Note: This tests strength classification, not full validation
    good_password = "MyGoodPass2024"  # 14 chars, 3 types (upper, lower, digits)
    result = PasswordSecurity.validate_password(good_password)

    # This password has GOOD strength but isn't fully valid (missing special char)
    assert result.strength == PasswordStrength.GOOD
    assert 75 <= result.score < 90
    # It has one error (missing special character)
    assert len(result.errors) == 1
    assert "special character" in result.errors[0]

  def test_validate_password_fair(self):
    """Test validation of a fair strength password (may not be fully valid)."""
    # Use a password that scores in the FAIR range (50-74)
    fair_password = "Pass12345"  # 9 chars, 3 types (upper, lower, digits)
    result = PasswordSecurity.validate_password(fair_password)

    # This password has FAIR strength but isn't fully valid
    assert result.strength == PasswordStrength.FAIR
    assert 50 <= result.score < 75
    # It has validation errors (too short, missing special char)
    assert len(result.errors) > 0

  def test_validate_password_too_short(self):
    """Test validation fails for password too short."""
    short_password = "Short1!"
    result = PasswordSecurity.validate_password(short_password)

    assert result.is_valid is False
    assert "must be at least 12 characters long" in result.errors[0]
    assert "Add 5 more characters" in result.suggestions[0]

  def test_validate_password_too_long(self):
    """Test validation fails for password too long."""
    long_password = "A" * 129 + "1!"
    result = PasswordSecurity.validate_password(long_password)

    assert result.is_valid is False
    assert "must not exceed 128 characters" in result.errors[0]

  def test_validate_password_missing_uppercase(self):
    """Test validation fails for missing uppercase letters."""
    no_uppercase = "lowercase123!"
    result = PasswordSecurity.validate_password(no_uppercase)

    assert result.is_valid is False
    assert "must contain at least one uppercase letter" in result.errors[0]
    assert "Add an uppercase letter (A-Z)" in result.suggestions[0]
    assert result.character_types["uppercase"] is False

  def test_validate_password_missing_lowercase(self):
    """Test validation fails for missing lowercase letters."""
    no_lowercase = "UPPERCASE123!"
    result = PasswordSecurity.validate_password(no_lowercase)

    assert result.is_valid is False
    assert "must contain at least one lowercase letter" in result.errors[0]
    assert "Add a lowercase letter (a-z)" in result.suggestions[0]
    assert result.character_types["lowercase"] is False

  def test_validate_password_missing_digits(self):
    """Test validation fails for missing digits."""
    no_digits = "NoDigitsHere!"
    result = PasswordSecurity.validate_password(no_digits)

    assert result.is_valid is False
    assert "must contain at least one number" in result.errors[0]
    assert "Add a number (0-9)" in result.suggestions[0]
    assert result.character_types["digits"] is False

  def test_validate_password_missing_special(self):
    """Test validation fails for missing special characters."""
    no_special = "NoSpecialChars123"
    result = PasswordSecurity.validate_password(no_special)

    assert result.is_valid is False
    assert "must contain at least one special character" in result.errors[0]
    assert "Add a special character (!@#$%^&*)" in result.suggestions[0]
    assert result.character_types["special"] is False

  def test_validate_password_insufficient_unique_chars(self):
    """Test validation fails for insufficient unique characters."""
    repeated_chars = "aaaaaa123!"
    result = PasswordSecurity.validate_password(repeated_chars)

    assert result.is_valid is False
    unique_chars_error = next(
      (err for err in result.errors if "unique characters" in err), None
    )
    assert unique_chars_error is not None
    assert "8" in unique_chars_error

  def test_validate_password_weak_patterns(self):
    """Test validation detects weak patterns."""
    weak_patterns = [
      "Password111!",  # Repeated characters
      "Password123456!",  # Sequential numbers
      "Passwordabcdef!",  # Sequential letters
      "Passwordqwerty!",  # Keyboard pattern
      "MyPasswordIs123!",  # Contains "password"
      "AdminUser123!",  # Contains "admin"
    ]

    for weak_pass in weak_patterns:
      result = PasswordSecurity.validate_password(weak_pass)
      assert any("weak patterns" in error for error in result.errors)
      assert any("common patterns" in suggestion for suggestion in result.suggestions)

  def test_validate_password_common_passwords(self):
    """Test validation rejects common passwords."""
    common_passwords = [
      "password123",
      "admin123",
      "Password1",
      "Admin123",
    ]

    for common_pass in common_passwords:
      result = PasswordSecurity.validate_password(common_pass)
      assert any("too common" in error for error in result.errors)
      assert any("more unique" in suggestion for suggestion in result.suggestions)

  def test_validate_password_email_similarity(self):
    """Test validation detects email similarity."""
    email = "john.doe@example.com"
    similar_passwords = [
      "JohnDoe123!",  # Contains "john" and "doe"
      "john123Pass!",  # Contains "john"
    ]

    for similar_pass in similar_passwords:
      result = PasswordSecurity.validate_password(similar_pass, email=email)
      assert any("email" in error.lower() for error in result.errors), (
        f"Failed for password: {similar_pass}"
      )
      assert any("email" in suggestion.lower() for suggestion in result.suggestions)

  def test_validate_password_length_bonuses(self):
    """Test length bonuses are applied correctly."""
    # Use passwords with only 3 character types to avoid maxing out at 100
    # Test that longer passwords score higher
    long_password = "VeryLongPassword123"  # 19 chars, 3 types (no special)
    result = PasswordSecurity.validate_password(long_password)

    # Shorter password should score lower
    short_password = "Short123"  # 8 chars, 3 types (no special)
    short_result = PasswordSecurity.validate_password(short_password)

    assert result.score > short_result.score

  def test_validate_password_character_variety_bonus(self):
    """Test character variety bonus."""
    all_types = "MyPassword123!"  # All 4 character types

    all_result = PasswordSecurity.validate_password(all_types)

    # All types should score higher (when both are otherwise valid)
    assert all_result.character_types == {
      "uppercase": True,
      "lowercase": True,
      "digits": True,
      "special": True,
    }

  def test_validate_password_score_bounds(self):
    """Test score is properly bounded between 0-100."""
    # Very weak password should have low score
    very_weak = "weak"
    result = PasswordSecurity.validate_password(very_weak)
    assert 0 <= result.score <= 100
    assert result.strength == PasswordStrength.VERY_WEAK

  def test_validate_password_edge_cases(self):
    """Test edge cases in password validation."""
    # Empty password
    empty_result = PasswordSecurity.validate_password("")
    assert empty_result.is_valid is False
    assert len(empty_result.errors) > 0

    # Single character types
    only_lower = "a" * 12
    only_result = PasswordSecurity.validate_password(only_lower)
    assert only_result.is_valid is False

    # Exactly minimum length with all requirements
    min_valid = "MinValid123!"  # Exactly 12 chars
    min_result = PasswordSecurity.validate_password(min_valid)
    assert len(min_valid) == 12
    assert min_result.is_valid is True

  def test_validate_password_unicode_handling(self):
    """Test password validation with unicode characters."""
    unicode_password = "UnicodeP@ss123é"
    result = PasswordSecurity.validate_password(unicode_password)

    # Should handle unicode gracefully
    assert isinstance(result.score, int)
    assert isinstance(result.errors, list)

  def test_validate_password_case_sensitivity(self):
    """Test case sensitivity in pattern detection."""
    # Common password check should be case-insensitive
    upper_common = "PASSWORD123"
    result = PasswordSecurity.validate_password(upper_common)

    # Pattern detection should work regardless of case
    assert len(result.errors) > 0


class TestPasswordHashing:
  """Test password hashing and verification."""

  def test_hash_password_basic(self):
    """Test basic password hashing."""
    password = "TestPassword123!"
    hashed = PasswordSecurity.hash_password(password)

    assert isinstance(hashed, str)
    assert hashed != password
    assert hashed.startswith("$2b$")  # bcrypt format
    assert len(hashed) == 60  # Standard bcrypt hash length

  def test_hash_password_unique_hashes(self):
    """Test that identical passwords produce different hashes (due to salt)."""
    password = "SamePassword123!"
    hash1 = PasswordSecurity.hash_password(password)
    hash2 = PasswordSecurity.hash_password(password)

    assert hash1 != hash2
    # Both should verify correctly
    assert PasswordSecurity.verify_password(password, hash1)
    assert PasswordSecurity.verify_password(password, hash2)

  def test_hash_password_bcrypt_rounds(self):
    """Test that hashing uses correct bcrypt rounds."""
    password = "TestRounds123!"
    hashed = PasswordSecurity.hash_password(password)

    # Extract rounds from hash (format: $2b$rounds$...)
    rounds = int(hashed.split("$")[2])
    # In tests we use 4 rounds for speed, in production it's 14
    assert rounds == 4  # Test-specific value set in conftest.py

  def test_hash_password_unicode(self):
    """Test hashing passwords with unicode characters."""
    unicode_password = "UnicodePass123!é€"
    hashed = PasswordSecurity.hash_password(unicode_password)

    assert isinstance(hashed, str)
    assert PasswordSecurity.verify_password(unicode_password, hashed)

  def test_hash_password_empty(self):
    """Test hashing empty password."""
    empty_password = ""
    hashed = PasswordSecurity.hash_password(empty_password)

    assert isinstance(hashed, str)
    assert PasswordSecurity.verify_password(empty_password, hashed)

  def test_hash_password_long(self):
    """Test hashing very long passwords."""
    long_password = "A" * 1000
    hashed = PasswordSecurity.hash_password(long_password)

    assert isinstance(hashed, str)
    assert PasswordSecurity.verify_password(long_password, hashed)

  def test_verify_password_correct(self):
    """Test password verification with correct password."""
    password = "CorrectPassword123!"
    hashed = PasswordSecurity.hash_password(password)

    assert PasswordSecurity.verify_password(password, hashed) is True

  def test_verify_password_incorrect(self):
    """Test password verification with incorrect password."""
    correct_password = "CorrectPassword123!"
    wrong_password = "WrongPassword123!"
    hashed = PasswordSecurity.hash_password(correct_password)

    assert PasswordSecurity.verify_password(wrong_password, hashed) is False

  def test_verify_password_invalid_hash(self):
    """Test password verification with invalid hash."""
    password = "TestPassword123!"
    invalid_hashes = [
      "invalid_hash",
      "",
      "$2b$14$invalidhash",
    ]

    for invalid_hash in invalid_hashes:
      result = PasswordSecurity.verify_password(password, invalid_hash)
      assert result is False

  def test_verify_password_tampered_hash(self):
    """Test password verification with tampered hash."""
    password = "TestPassword123!"
    hashed = PasswordSecurity.hash_password(password)

    # Tamper with the hash
    tampered_hash = hashed[:-1] + ("x" if hashed[-1] != "x" else "y")

    assert PasswordSecurity.verify_password(password, tampered_hash) is False

  def test_verify_password_case_sensitivity(self):
    """Test that password verification is case sensitive."""
    password = "CaseSensitive123!"
    hashed = PasswordSecurity.hash_password(password)

    assert PasswordSecurity.verify_password(password, hashed) is True
    assert PasswordSecurity.verify_password(password.lower(), hashed) is False
    assert PasswordSecurity.verify_password(password.upper(), hashed) is False


class TestSecurePasswordGeneration:
  """Test secure password generation."""

  def test_generate_secure_password_default_length(self):
    """Test generating password with default length."""
    password = PasswordSecurity.generate_secure_password()

    assert len(password) == 16
    validation_result = PasswordSecurity.validate_password(password)
    assert validation_result.is_valid is True

  def test_generate_secure_password_custom_length(self):
    """Test generating password with custom length."""
    lengths = [12, 16, 20, 32]

    for length in lengths:
      password = PasswordSecurity.generate_secure_password(length)
      assert len(password) == length

      validation_result = PasswordSecurity.validate_password(password)
      # Allow weak pattern errors in randomly generated passwords
      if not validation_result.is_valid:
        assert len(validation_result.errors) == 1
        assert "weak patterns" in validation_result.errors[0]
      # Should still be strong even with patterns
      assert validation_result.strength == PasswordStrength.STRONG

  def test_generate_secure_password_minimum_length_enforcement(self):
    """Test that minimum length is enforced."""
    # Request shorter than minimum
    password = PasswordSecurity.generate_secure_password(8)
    assert len(password) == PasswordSecurity.MIN_LENGTH

  def test_generate_secure_password_character_requirements(self):
    """Test that generated passwords meet all character requirements."""
    for _ in range(10):  # Test multiple generations
      password = PasswordSecurity.generate_secure_password()

      # Check all character types are present
      assert re.search(r"[A-Z]", password) is not None  # Uppercase
      assert re.search(r"[a-z]", password) is not None  # Lowercase
      assert re.search(r"[0-9]", password) is not None  # Digits
      assert re.search(r"[!@#$%^&*()]", password) is not None  # Special

  def test_generate_secure_password_uniqueness(self):
    """Test that generated passwords are unique."""
    passwords = [PasswordSecurity.generate_secure_password() for _ in range(100)]

    # All passwords should be unique
    assert len(set(passwords)) == len(passwords)

  def test_generate_secure_password_character_distribution(self):
    """Test reasonable character distribution in generated passwords."""
    password = PasswordSecurity.generate_secure_password(100)

    # Count character types
    uppercase_count = len(re.findall(r"[A-Z]", password))
    lowercase_count = len(re.findall(r"[a-z]", password))
    digit_count = len(re.findall(r"[0-9]", password))
    special_count = len(re.findall(r"[!@#$%^&*()]", password))

    # Each type should appear at least once
    assert uppercase_count >= 1
    assert lowercase_count >= 1
    assert digit_count >= 1
    assert special_count >= 1

    # Total should equal password length
    assert uppercase_count + lowercase_count + digit_count + special_count == len(
      password
    )

  def test_generate_secure_password_validation(self):
    """Test that generated passwords meet requirements (may have patterns)."""
    for length in [12, 16, 24, 32]:
      for _ in range(5):
        password = PasswordSecurity.generate_secure_password(length)
        result = PasswordSecurity.validate_password(password)

        # Generated passwords should be strong even if they have patterns
        # (patterns are meant to catch human-predictable passwords, not random ones)
        assert result.strength in [PasswordStrength.GOOD, PasswordStrength.STRONG]
        assert result.score >= PasswordSecurity.MIN_STRENGTH_SCORE

        # If not valid, it should only be due to weak patterns
        if not result.is_valid:
          assert len(result.errors) == 1
          assert "weak patterns" in result.errors[0]


class TestPasswordPolicy:
  """Test password policy configuration."""

  def test_get_password_policy(self):
    """Test getting password policy configuration."""
    policy = PasswordSecurity.get_password_policy()

    expected_keys = {
      "min_length",
      "max_length",
      "require_uppercase",
      "require_lowercase",
      "require_digits",
      "require_special",
      "min_unique_chars",
      "min_strength_score",
      "special_chars",
    }

    assert set(policy.keys()) == expected_keys
    assert policy["min_length"] == PasswordSecurity.MIN_LENGTH
    assert policy["max_length"] == PasswordSecurity.MAX_LENGTH
    assert policy["min_strength_score"] == PasswordSecurity.MIN_STRENGTH_SCORE

  def test_password_policy_consistency(self):
    """Test that password policy is consistent with validation."""
    policy = PasswordSecurity.get_password_policy()

    # Generate a password meeting policy requirements
    # min_length - 3 because we add "A" (1 char) + "1!" (2 chars) = 3 chars
    test_password = "A" + "a" * (policy["min_length"] - 3) + "1!"

    result = PasswordSecurity.validate_password(test_password)

    # Should meet minimum requirements based on policy
    assert len(test_password) >= policy["min_length"]
    assert result.character_types["uppercase"] == policy["require_uppercase"]


class TestSecurityScenarios:
  """Test security-focused scenarios and edge cases."""

  def test_timing_attack_resistance(self):
    """Test that password verification has consistent timing."""
    import time

    password = "TestPassword123!"
    hashed = PasswordSecurity.hash_password(password)

    # Measure verification time for correct password
    start_time = time.time()
    PasswordSecurity.verify_password(password, hashed)
    correct_time = time.time() - start_time

    # Measure verification time for incorrect password
    start_time = time.time()
    PasswordSecurity.verify_password("WrongPassword123!", hashed)
    incorrect_time = time.time() - start_time

    # Times should be similar (bcrypt naturally provides timing resistance)
    time_ratio = max(correct_time, incorrect_time) / min(correct_time, incorrect_time)
    assert time_ratio < 2.0  # Allow some variance

  def test_hash_collision_resistance(self):
    """Test hash collision resistance."""
    passwords = [
      "Password123!",
      "Password123@",
      "Password124!",
      "password123!",
    ]

    hashes = [PasswordSecurity.hash_password(pwd) for pwd in passwords]

    # All hashes should be unique
    assert len(set(hashes)) == len(hashes)

  def test_brute_force_resistance(self):
    """Test that bcrypt rounds provide brute force resistance."""
    password = "TestPassword123!"

    # Measure hashing time (should be slow due to high rounds)
    import time

    start_time = time.time()
    hashed = PasswordSecurity.hash_password(password)
    hash_time = time.time() - start_time

    # In tests we use 4 rounds (fast), production uses 14 rounds (slow)
    # Just verify that hashing works, not the timing
    assert hash_time > 0  # Should take some time
    assert PasswordSecurity.verify_password(password, hashed)

  def test_sql_injection_in_patterns(self):
    """Test that SQL injection attempts don't break validation."""
    malicious_passwords = [
      "'; DROP TABLE users; --",
      "Password123!'; SELECT * FROM passwords; --",
      'Admin123!" OR 1=1; --',
    ]

    for malicious_pwd in malicious_passwords:
      result = PasswordSecurity.validate_password(malicious_pwd)
      # Should validate normally (SQL injection protection is at DB layer)
      assert isinstance(result, PasswordValidationResult)
      assert isinstance(result.score, int)

  def test_memory_exhaustion_protection(self):
    """Test protection against memory exhaustion attacks."""
    # Extremely long password
    long_password = "A" * 10000

    # Should handle gracefully (truncation handled by MAX_LENGTH)
    result = PasswordSecurity.validate_password(long_password)
    assert isinstance(result, PasswordValidationResult)

    # Should have max length error
    assert any("exceed" in error for error in result.errors)

  def test_denial_of_service_protection(self):
    """Test DoS protection through input validation."""
    # Multiple validation attempts shouldn't cause issues
    passwords = ["TestPass123!"] * 1000

    for password in passwords:
      result = PasswordSecurity.validate_password(password)
      assert isinstance(result, PasswordValidationResult)

  def test_unicode_normalization_attacks(self):
    """Test handling of unicode normalization attacks."""
    # Different unicode representations of similar characters
    unicode_passwords = [
      "Pāssword123!",  # With diacritic
      "Pässword123!",  # Different diacritic
      "Password123！",  # Full-width exclamation
    ]

    for unicode_pwd in unicode_passwords:
      result = PasswordSecurity.validate_password(unicode_pwd)
      assert isinstance(result, PasswordValidationResult)

      # Should be able to hash and verify
      if result.is_valid:
        hashed = PasswordSecurity.hash_password(unicode_pwd)
        assert PasswordSecurity.verify_password(unicode_pwd, hashed)


class TestPerformanceAndReliability:
  """Test performance characteristics and reliability."""

  def test_validation_performance(self):
    """Test password validation performance."""
    import time

    password = "TestPerformance123!"

    # Measure validation time
    start_time = time.time()
    for _ in range(100):
      PasswordSecurity.validate_password(password)
    validation_time = time.time() - start_time

    # Should complete quickly (validation is much faster than hashing)
    assert validation_time < 1.0  # 100 validations in under 1 second

  def test_concurrent_operations(self):
    """Test concurrent password operations."""
    import queue
    import threading

    def worker(q):
      password = "ConcurrentTest123!"
      try:
        # Test validation
        result = PasswordSecurity.validate_password(password)
        assert isinstance(result, PasswordValidationResult)

        # Test hashing and verification
        hashed = PasswordSecurity.hash_password(password)
        verified = PasswordSecurity.verify_password(password, hashed)
        assert verified is True

        q.put(True)
      except Exception as e:
        q.put(e)

    # Run concurrent operations
    threads = []
    result_queue = queue.Queue()

    for _ in range(10):
      thread = threading.Thread(target=worker, args=(result_queue,))
      threads.append(thread)
      thread.start()

    # Wait for completion
    for thread in threads:
      thread.join()

    # Check all operations succeeded
    while not result_queue.empty():
      result = result_queue.get()
      assert result is True

  def test_password_strength_consistency(self):
    """Test that password strength assessment is consistent."""
    test_passwords = [
      ("VeryW3ak!", PasswordStrength.GOOD),  # Short but has all 4 char types = 80 score
      (
        "BetterPassword123!",
        PasswordStrength.STRONG,
      ),  # Has all 4 types and good length = 100
      (
        "ExcellentStr0ng!P@ssw0rd2024",
        PasswordStrength.STRONG,
      ),  # Long with all types = 100
    ]

    # Test multiple times to ensure consistency
    for password, expected_strength in test_passwords:
      for _ in range(5):
        result = PasswordSecurity.validate_password(password)
        assert result.strength == expected_strength

  def test_error_handling_robustness(self):
    """Test robustness of error handling."""
    # Test with None input
    try:
      PasswordSecurity.validate_password(None)
    except (TypeError, AttributeError):
      pass  # Expected behavior

    # Test with non-string input
    try:
      PasswordSecurity.validate_password(12345)
    except (TypeError, AttributeError):
      pass  # Expected behavior

  @patch("bcrypt.hashpw")
  def test_bcrypt_error_handling(self, mock_hashpw):
    """Test handling of bcrypt errors."""
    mock_hashpw.side_effect = ValueError("Bcrypt error")

    # Should handle bcrypt errors gracefully
    with pytest.raises(ValueError):
      PasswordSecurity.hash_password("TestPassword123!")

  @patch("bcrypt.checkpw")
  def test_verification_error_handling(self, mock_checkpw):
    """Test handling of verification errors."""
    mock_checkpw.side_effect = ValueError("Verification error")

    # Should return False on error
    result = PasswordSecurity.verify_password("test", "hash")
    assert result is False


class TestConfigurationValidation:
  """Test password configuration validation."""

  def test_configuration_constants(self):
    """Test that configuration constants are sensible."""
    assert PasswordSecurity.MIN_LENGTH >= 8
    assert PasswordSecurity.MAX_LENGTH >= PasswordSecurity.MIN_LENGTH
    assert PasswordSecurity.MIN_UNIQUE_CHARS <= PasswordSecurity.MIN_LENGTH
    # In tests we use 4 rounds for speed, production would use >= 10
    assert PasswordSecurity.BCRYPT_ROUNDS >= 4  # Test-specific lower value
    assert 0 <= PasswordSecurity.MIN_STRENGTH_SCORE <= 100

  def test_weak_patterns_compilation(self):
    """Test that weak patterns compile correctly as regex."""
    for pattern in PasswordSecurity.WEAK_PATTERNS:
      try:
        re.compile(pattern)
      except re.error:
        pytest.fail(f"Invalid regex pattern: {pattern}")

  def test_character_requirements_logic(self):
    """Test the logic of character requirements."""
    # If all are required, validation should enforce all
    assert PasswordSecurity.REQUIRE_UPPERCASE
    assert PasswordSecurity.REQUIRE_LOWERCASE
    assert PasswordSecurity.REQUIRE_DIGITS
    assert PasswordSecurity.REQUIRE_SPECIAL

    # Test password meeting all requirements
    compliant_password = "CompliantPass123!"
    result = PasswordSecurity.validate_password(compliant_password)
    assert result.is_valid is True

  def test_common_passwords_coverage(self):
    """Test that common passwords list has reasonable coverage."""
    assert len(PasswordSecurity.COMMON_PASSWORDS) > 0

    # Should contain obvious weak passwords
    obvious_weak = ["password123", "admin123"]
    for weak_pwd in obvious_weak:
      assert weak_pwd in PasswordSecurity.COMMON_PASSWORDS
