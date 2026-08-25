"""Unit tests for authentication API models."""

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from robosystems.models.api.auth import (
  AuthResponse,
  EmailVerificationRequest,
  ForgotPasswordRequest,
  LoginRequest,
  PasswordCheckRequest,
  PasswordCheckResponse,
  RegisterRequest,
  ResetPasswordRequest,
  ResetPasswordValidateResponse,
  SSOCompleteRequest,
  SSOExchangeRequest,
  SSOExchangeResponse,
  SSOTokenResponse,
)
from robosystems.security.password import PasswordSecurity


@pytest.mark.unit
class TestLoginRequest:
  def test_valid_login(self):
    model = LoginRequest(email="user@example.com", password="MyP@ssw0rd123")
    assert model.email == "user@example.com"
    assert model.password == "MyP@ssw0rd123"

  def test_invalid_email(self):
    with pytest.raises(ValidationError) as exc_info:
      LoginRequest(email="not-an-email", password="MyP@ssw0rd123")
    errors = exc_info.value.errors()
    assert any(e["loc"] == ("email",) for e in errors)

  def test_password_too_short(self):
    with pytest.raises(ValidationError) as exc_info:
      LoginRequest(email="user@example.com", password="short")
    errors = exc_info.value.errors()
    assert any(e["loc"] == ("password",) for e in errors)

  def test_password_minimum_length(self):
    # LoginRequest has min_length=8 (not the stricter PasswordSecurity.MIN_LENGTH)
    model = LoginRequest(email="user@example.com", password="12345678")
    assert model.password == "12345678"

  def test_email_required(self):
    with pytest.raises(ValidationError):
      LoginRequest(password="MyP@ssw0rd123")  # type: ignore[call-arg]

  def test_password_required(self):
    with pytest.raises(ValidationError):
      LoginRequest(email="user@example.com")  # type: ignore[call-arg]


@pytest.mark.unit
class TestRegisterRequest:
  def _valid_password(self):
    """Return a password that meets all security requirements."""
    return "Str0ng!P@ssw0rd_2024"

  def test_valid_registration(self):
    model = RegisterRequest(
      name="John Doe",
      email="john@example.com",
      password=self._valid_password(),
    )
    assert model.name == "John Doe"
    assert model.email == "john@example.com"

  def test_name_min_length(self):
    with pytest.raises(ValidationError) as exc_info:
      RegisterRequest(
        name="",
        email="john@example.com",
        password=self._valid_password(),
      )
    errors = exc_info.value.errors()
    assert any(e["loc"] == ("name",) for e in errors)

  def test_name_max_length(self):
    with pytest.raises(ValidationError) as exc_info:
      RegisterRequest(
        name="x" * 101,
        email="john@example.com",
        password=self._valid_password(),
      )
    errors = exc_info.value.errors()
    assert any(e["loc"] == ("name",) for e in errors)

  def test_password_min_length(self):
    with pytest.raises(ValidationError) as exc_info:
      RegisterRequest(
        name="John",
        email="john@example.com",
        password="short",
      )
    errors = exc_info.value.errors()
    assert any(e["loc"] == ("password",) for e in errors)

  def test_password_max_length(self):
    with pytest.raises(ValidationError) as exc_info:
      RegisterRequest(
        name="John",
        email="john@example.com",
        password="A1!a" + "x" * (PasswordSecurity.MAX_LENGTH + 1),
      )
    errors = exc_info.value.errors()
    assert any(e["loc"] == ("password",) for e in errors)

  def test_weak_password_rejected(self):
    # A password that meets length but is too simple (no special chars, etc.)
    with pytest.raises(ValidationError) as exc_info:
      RegisterRequest(
        name="John",
        email="john@example.com",
        password="a" * PasswordSecurity.MIN_LENGTH,
      )
    errors = exc_info.value.errors()
    assert any(e["loc"] == ("password",) for e in errors)
    assert any("security requirements" in str(e.get("msg", "")) for e in errors)

  def test_captcha_token_optional(self):
    model = RegisterRequest(
      name="John",
      email="john@example.com",
      password=self._valid_password(),
    )
    assert model.captcha_token is None

  def test_captcha_token_accepted(self):
    model = RegisterRequest(
      name="John",
      email="john@example.com",
      password=self._valid_password(),
      captcha_token="token_abc",
    )
    assert model.captcha_token == "token_abc"

  def test_invalid_email_rejected(self):
    with pytest.raises(ValidationError):
      RegisterRequest(
        name="John",
        email="bad-email",
        password=self._valid_password(),
      )


@pytest.mark.unit
class TestAuthResponse:
  def test_minimal_response(self):
    model = AuthResponse(
      user={"id": "user1", "name": "Test"},
      message="Logged in",
    )
    assert model.user == {"id": "user1", "name": "Test"}
    assert model.message == "Logged in"
    assert model.org is None
    assert model.token is None
    assert model.expires_in is None
    assert model.refresh_threshold is None

  def test_full_response(self):
    model = AuthResponse(
      user={"id": "user1"},
      org={"id": "org1", "name": "My Org"},
      message="OK",
      token="jwt_token_here",
      expires_in=3600,
      refresh_threshold=300,
    )
    assert model.org is not None
    assert model.token == "jwt_token_here"
    assert model.expires_in == 3600
    assert model.refresh_threshold == 300


@pytest.mark.unit
class TestSSOModels:
  def test_sso_token_response(self):
    ts = datetime(2024, 6, 1, tzinfo=UTC)
    model = SSOTokenResponse(
      token="sso_token_123",
      expires_at=ts,
      apps=["roboledger", "roboinvestor"],
    )
    assert model.token == "sso_token_123"
    assert len(model.apps) == 2

  def test_sso_exchange_request(self):
    model = SSOExchangeRequest(
      token="sso_token",
      target_app="roboledger",
    )
    assert model.token == "sso_token"
    assert model.target_app == "roboledger"
    assert model.return_url is None

  def test_sso_exchange_request_with_return_url(self):
    model = SSOExchangeRequest(
      token="sso_token",
      target_app="roboledger",
      return_url="https://app.example.com/dashboard",
    )
    assert model.return_url == "https://app.example.com/dashboard"

  def test_sso_exchange_response(self):
    ts = datetime(2024, 6, 1, tzinfo=UTC)
    model = SSOExchangeResponse(
      session_id="sess_123",
      redirect_url="https://app.example.com/auth",
      expires_at=ts,
    )
    assert model.session_id == "sess_123"

  def test_sso_complete_request(self):
    model = SSOCompleteRequest(session_id="sess_123")
    assert model.session_id == "sess_123"


@pytest.mark.unit
class TestPasswordCheckModels:
  def test_password_check_request(self):
    model = PasswordCheckRequest(password="test_password")
    assert model.password == "test_password"
    assert model.email is None

  def test_password_check_request_with_email(self):
    model = PasswordCheckRequest(password="test", email="user@example.com")
    assert model.email == "user@example.com"

  def test_password_check_request_rejects_overlong_password(self):
    with pytest.raises(ValidationError):
      PasswordCheckRequest(password="x" * 129)

  def test_password_check_response(self):
    model = PasswordCheckResponse(
      is_valid=True,
      strength="strong",
      score=85,
      errors=[],
      suggestions=[],
      character_types={
        "uppercase": True,
        "lowercase": True,
        "digits": True,
        "special": True,
      },
    )
    assert model.is_valid is True
    assert model.score == 85


@pytest.mark.unit
class TestResetPasswordRequest:
  def test_valid_reset(self):
    model = ResetPasswordRequest(
      token="reset_token_123",
      new_password="N3wStr0ng!P@ss_2024",
    )
    assert model.token == "reset_token_123"

  def test_weak_password_rejected(self):
    with pytest.raises(ValidationError) as exc_info:
      ResetPasswordRequest(
        token="reset_token_123",
        new_password="a" * PasswordSecurity.MIN_LENGTH,
      )
    errors = exc_info.value.errors()
    assert any("security requirements" in str(e.get("msg", "")) for e in errors)

  def test_short_password_rejected(self):
    with pytest.raises(ValidationError):
      ResetPasswordRequest(token="tok", new_password="short")


@pytest.mark.unit
class TestEmailVerificationRequest:
  def test_valid_request(self):
    model = EmailVerificationRequest(token="verify_abc123")
    assert model.token == "verify_abc123"

  def test_token_required(self):
    with pytest.raises(ValidationError):
      EmailVerificationRequest()  # type: ignore[call-arg]


@pytest.mark.unit
class TestForgotPasswordRequest:
  def test_valid_email(self):
    model = ForgotPasswordRequest(email="user@example.com")
    assert model.email == "user@example.com"

  def test_invalid_email(self):
    with pytest.raises(ValidationError):
      ForgotPasswordRequest(email="not-valid")


@pytest.mark.unit
class TestResetPasswordValidateResponse:
  def test_valid_token(self):
    model = ResetPasswordValidateResponse(valid=True, email="u***@example.com")
    assert model.valid is True
    assert model.email == "u***@example.com"

  def test_invalid_token(self):
    model = ResetPasswordValidateResponse(valid=False)
    assert model.valid is False
    assert model.email is None
