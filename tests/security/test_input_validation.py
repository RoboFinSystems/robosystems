"""Input validation and sanitization tests."""

import pytest

from robosystems.security.input_validation import (
  sanitize_sql_identifier,
  sanitize_string,
  sanitize_url,
  sanitize_user_input,
  strip_html_tags,
  validate_api_key,
  validate_email,
  validate_password_strength,
  validate_username,
  validate_uuid,
)


class TestSanitizeString:
  """Tests for sanitize_string."""

  def test_basic_string(self):
    assert sanitize_string("hello world") == "hello world"

  def test_truncates_to_max_length(self):
    result = sanitize_string("a" * 2000, max_length=10)
    assert len(result) == 10

  def test_html_escapes(self):
    result = sanitize_string("a&b")
    assert "&amp;" in result

  def test_removes_angle_brackets(self):
    result = sanitize_string("hello<script>alert</script>world")
    assert "<" not in result
    assert ">" not in result

  def test_removes_quotes(self):
    result = sanitize_string('he said "hello"')
    assert '"' not in result

  def test_removes_single_quotes(self):
    result = sanitize_string("it's fine")
    assert "'" not in result

  def test_removes_null_bytes(self):
    result = sanitize_string("hello\0world")
    assert "\0" not in result

  def test_removes_newlines(self):
    result = sanitize_string("hello\r\nworld")
    assert "\r" not in result
    assert "\n" not in result

  def test_strips_whitespace(self):
    result = sanitize_string("  hello  ")
    assert result == "hello"

  def test_empty_string(self):
    assert sanitize_string("") == ""

  def test_custom_max_length(self):
    result = sanitize_string("abcdef", max_length=3)
    assert len(result) <= 3


class TestValidateEmail:
  """Tests for validate_email."""

  @pytest.mark.parametrize(
    "email",
    [
      "user@example.com",
      "user.name@example.com",
      "user+tag@example.com",
      "user@sub.domain.com",
      "user123@example.co.uk",
    ],
  )
  def test_valid_emails(self, email):
    assert validate_email(email) is True

  @pytest.mark.parametrize(
    "email",
    [
      "",
      "not-an-email",
      "@example.com",
      "user@",
      "user@.com",
      "user@com",
      "user @example.com",
    ],
  )
  def test_invalid_emails(self, email):
    assert validate_email(email) is False

  def test_email_too_long(self):
    # 246 + "@test.com" (9) = 255 chars, exceeds 254 limit
    long_email = "a" * 246 + "@test.com"
    assert len(long_email) == 255
    assert validate_email(long_email) is False


class TestSanitizeUserInput:
  """Tests for sanitize_user_input."""

  def test_sanitizes_string_values(self):
    data = {"name": "<script>alert('xss')</script>"}
    result = sanitize_user_input(data)
    assert "<script>" not in result["name"]

  def test_preserves_non_string_values(self):
    data = {"count": 42, "active": True, "rate": 3.14}
    result = sanitize_user_input(data)
    assert result["count"] == 42
    assert result["active"] is True
    assert result["rate"] == 3.14

  def test_recursive_dict_sanitization(self):
    data = {"nested": {"name": "<b>bold</b>"}}
    result = sanitize_user_input(data)
    assert "<b>" not in result["nested"]["name"]

  def test_list_sanitization(self):
    data = {"tags": ["<script>", "normal", 42]}
    result = sanitize_user_input(data)
    assert "<script>" not in result["tags"][0]
    assert result["tags"][1] == "normal"
    assert result["tags"][2] == 42

  def test_empty_dict(self):
    assert sanitize_user_input({}) == {}


class TestValidatePasswordStrength:
  """Tests for validate_password_strength."""

  def test_strong_password(self):
    is_valid, issues = validate_password_strength("MyP@ssw0rd!")
    assert is_valid is True
    assert issues == []

  def test_too_short(self):
    is_valid, issues = validate_password_strength("Ab1!")
    assert is_valid is False
    assert any("8 characters" in i for i in issues)

  def test_no_lowercase(self):
    is_valid, issues = validate_password_strength("ABCDEFGH1!")
    assert is_valid is False
    assert any("lowercase" in i for i in issues)

  def test_no_uppercase(self):
    is_valid, issues = validate_password_strength("abcdefgh1!")
    assert is_valid is False
    assert any("uppercase" in i for i in issues)

  def test_no_digit(self):
    is_valid, issues = validate_password_strength("Abcdefgh!")
    assert is_valid is False
    assert any("digit" in i for i in issues)

  def test_no_special_char(self):
    is_valid, issues = validate_password_strength("Abcdefgh1")
    assert is_valid is False
    assert any("special character" in i for i in issues)

  def test_multiple_issues(self):
    is_valid, issues = validate_password_strength("abc")
    assert is_valid is False
    assert len(issues) >= 3


class TestValidateUsername:
  """Tests for validate_username."""

  @pytest.mark.parametrize(
    "username",
    [
      "abc",
      "user123",
      "my-name",
      "my_name",
      "a" * 30,
    ],
  )
  def test_valid_usernames(self, username):
    assert validate_username(username) is True

  @pytest.mark.parametrize(
    "username",
    [
      "",
      "ab",
      "a" * 31,
      "user name",
      "user@name",
      "user!name",
    ],
  )
  def test_invalid_usernames(self, username):
    assert validate_username(username) is False


class TestSanitizeSqlIdentifier:
  """Tests for sanitize_sql_identifier."""

  def test_basic_identifier(self):
    assert sanitize_sql_identifier("my_table") == "my_table"

  def test_removes_special_chars(self):
    assert sanitize_sql_identifier("my-table!@#") == "mytable"

  def test_prefixes_leading_digit(self):
    result = sanitize_sql_identifier("123table")
    assert result.startswith("_")

  def test_truncates_to_63_chars(self):
    result = sanitize_sql_identifier("a" * 100)
    assert len(result) == 63

  def test_empty_string(self):
    assert sanitize_sql_identifier("") == ""

  def test_all_special_chars(self):
    assert sanitize_sql_identifier("!@#$%") == ""


class TestValidateUuid:
  """Tests for validate_uuid."""

  def test_valid_uuid(self):
    assert validate_uuid("550e8400-e29b-41d4-a716-446655440000") is True

  def test_uppercase_uuid(self):
    assert validate_uuid("550E8400-E29B-41D4-A716-446655440000") is True

  def test_invalid_uuid(self):
    assert validate_uuid("not-a-uuid") is False

  def test_missing_hyphens(self):
    assert validate_uuid("550e8400e29b41d4a716446655440000") is False

  def test_empty_string(self):
    assert validate_uuid("") is False


class TestSanitizeUrl:
  """Tests for sanitize_url."""

  def test_valid_http_url(self):
    assert sanitize_url("http://example.com") == "http://example.com"

  def test_valid_https_url(self):
    assert sanitize_url("https://example.com/path") == "https://example.com/path"

  def test_invalid_url(self):
    assert sanitize_url("not-a-url") is None

  def test_ftp_url_rejected(self):
    assert sanitize_url("ftp://example.com") is None

  def test_javascript_protocol_rejected(self):
    assert sanitize_url("javascript:alert(1)") is None

  def test_data_protocol_rejected(self):
    assert sanitize_url("data:text/html,<h1>hi</h1>") is None

  def test_empty_string(self):
    assert sanitize_url("") is None


class TestStripHtmlTags:
  """Tests for strip_html_tags."""

  def test_removes_tags(self):
    assert strip_html_tags("<p>Hello</p>") == "Hello"

  def test_removes_nested_tags(self):
    assert strip_html_tags("<div><b>Bold</b></div>") == "Bold"

  def test_preserves_plain_text(self):
    assert strip_html_tags("no tags here") == "no tags here"

  def test_empty_string(self):
    assert strip_html_tags("") == ""

  def test_self_closing_tags(self):
    assert strip_html_tags("line1<br/>line2") == "line1line2"


class TestValidateApiKey:
  """Tests for validate_api_key."""

  def test_valid_api_key(self):
    key = "rsk_" + "a" * 60
    assert validate_api_key(key) is True

  def test_wrong_prefix(self):
    key = "sk_" + "a" * 60
    assert validate_api_key(key) is False

  def test_too_short(self):
    key = "rsk_" + "a" * 10
    assert validate_api_key(key) is False

  def test_empty_string(self):
    assert validate_api_key("") is False

  def test_long_key(self):
    key = "rsk_" + "a" * 100
    assert validate_api_key(key) is True
