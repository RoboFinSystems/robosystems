"""Authentication API models."""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, EmailStr, Field, ValidationInfo, field_validator

from ...security.password import PasswordSecurity


class LoginRequest(BaseModel):
  """Login request model."""

  email: EmailStr = Field(..., description="User's email address")
  password: str = Field(..., min_length=8, description="User's password")


class RegisterRequest(BaseModel):
  """Registration request model."""

  name: str = Field(
    ..., min_length=1, max_length=100, description="User's display name"
  )
  email: EmailStr = Field(..., description="User's email address")
  password: str = Field(
    ...,
    min_length=PasswordSecurity.MIN_LENGTH,
    max_length=PasswordSecurity.MAX_LENGTH,
    description="User's password (must meet security requirements)",
  )
  captcha_token: str | None = Field(
    None, description="CAPTCHA verification token (required in production)"
  )
  invite_token: str | None = Field(
    None,
    description="Org invitation token from an invitation email. When present, the new user joins the inviting organization at the invited role instead of receiving a personal organization.",
  )

  @field_validator("password")
  def validate_password_strength(cls, v: str, info: ValidationInfo) -> str:
    """Validate password meets security requirements."""
    email: str | None = info.data.get("email")
    result = PasswordSecurity.validate_password(v, email)

    if not result.is_valid:
      error_msg = "Password does not meet security requirements: " + "; ".join(
        result.errors
      )
      raise ValueError(error_msg)

    return v


class AuthResponse(BaseModel):
  """Authentication response model."""

  user: dict[str, object] = Field(..., description="User information")
  org: dict[str, object] | None = Field(
    default=None,
    description="Organization information (personal org created automatically on registration)",
  )
  message: str = Field(..., description="Success message")
  # Always present, defaulting to authenticated, so producers that predate
  # the discriminator (register, SSO completion) keep their contract while
  # login can signal a second step. New non-authenticated members are
  # additive for clients: they only ever accompany token=None.
  status: Literal["authenticated", "mfa_required", "mfa_enrollment_required"] = Field(
    default="authenticated",
    description=(
      "Login flow state: authenticated (token present), or a passkey MFA "
      "step is required before a session is issued (mfa_token present)"
    ),
  )
  mfa_token: str | None = Field(
    default=None,
    description=(
      "Short-lived token authorizing the MFA second step or forced "
      "enrollment; present only when status is not 'authenticated'"
    ),
  )
  token: str | None = Field(
    default=None,
    description="JWT authentication token (optional for cookie-based auth)",
  )
  expires_in: int | None = Field(
    default=None, description="Token expiry time in seconds from now"
  )
  refresh_threshold: int | None = Field(
    default=None, description="Recommended refresh threshold in seconds before expiry"
  )


class SSOTokenResponse(BaseModel):
  """SSO token response model."""

  token: str = Field(
    ..., description="Temporary SSO token for cross-app authentication"
  )
  expires_at: datetime = Field(..., description="Token expiration time")
  apps: list[str] = Field(..., description="Available apps for this user")


class SSOExchangeRequest(BaseModel):
  """SSO token exchange request model."""

  token: str = Field(..., description="Temporary SSO token")
  target_app: str = Field(..., description="Target application identifier")
  return_url: str | None = Field(
    None, description="Optional return URL after authentication"
  )


class SSOExchangeResponse(BaseModel):
  """SSO token exchange response model."""

  session_id: str = Field(..., description="Temporary session ID for secure handoff")
  redirect_url: str = Field(..., description="URL to redirect to for authentication")
  expires_at: datetime = Field(..., description="Session expiration time")


class SSOCompleteRequest(BaseModel):
  """SSO completion request model."""

  session_id: str = Field(..., description="Temporary session ID from secure handoff")


class OIDCProviderInfo(BaseModel):
  """OIDC provider availability model."""

  enabled: bool = Field(..., description="Whether OIDC SSO is available")
  provider_label: str | None = Field(
    None, description="Display label for the OIDC provider (e.g. 'Okta')"
  )


class AuthProvidersResponse(BaseModel):
  """Auth posture response model.

  Describes which authentication methods this deployment offers so the
  login surface can render the correct posture from runtime configuration.
  """

  password_auth: bool = Field(
    ..., description="Whether password authentication is available"
  )
  oidc: OIDCProviderInfo = Field(..., description="OIDC SSO availability")
  registration: bool = Field(
    ..., description="Whether self-service registration is open"
  )
  passkeys: bool = Field(..., description="Whether passkey authentication is available")


class PasswordCheckRequest(BaseModel):
  """Password strength check request model."""

  password: str = Field(
    ...,
    min_length=1,
    max_length=128,
    description="Password to check",
  )
  email: str | None = Field(None, description="User email for personalization checks")


class PasswordCheckResponse(BaseModel):
  """Password strength check response model."""

  is_valid: bool = Field(..., description="Whether password meets requirements")
  strength: str = Field(..., description="Password strength level")
  score: int = Field(..., description="Password strength score (0-100)")
  errors: list[str] = Field(..., description="Validation errors")
  suggestions: list[str] = Field(..., description="Improvement suggestions")
  character_types: dict[str, bool] = Field(..., description="Character type analysis")


class PasswordPolicyResponse(BaseModel):
  """Password policy response model."""

  policy: dict[str, object] = Field(
    ..., description="Current password policy requirements"
  )


class EmailVerificationRequest(BaseModel):
  """Email verification request model."""

  token: str = Field(..., description="Email verification token from email link")


class EmailResendRequest(BaseModel):
  """Email verification resend request model."""

  # No body needed - uses current authenticated user


class ForgotPasswordRequest(BaseModel):
  """Forgot password request model."""

  email: EmailStr = Field(..., description="Email address to send reset link")


class ResetPasswordRequest(BaseModel):
  """Reset password request model."""

  token: str = Field(..., description="Password reset token from email link")
  new_password: str = Field(
    ...,
    min_length=PasswordSecurity.MIN_LENGTH,
    max_length=PasswordSecurity.MAX_LENGTH,
    description="New password (must meet security requirements)",
  )

  @field_validator("new_password")
  def validate_password_strength(cls, v: str) -> str:
    """Validate password meets security requirements."""
    result = PasswordSecurity.validate_password(v)

    if not result.is_valid:
      error_msg = "Password does not meet security requirements: " + "; ".join(
        result.errors
      )
      raise ValueError(error_msg)

    return v


class ResetPasswordValidateResponse(BaseModel):
  """Password reset token validation response model."""

  valid: bool = Field(..., description="Whether the token is valid")
  email: str | None = Field(None, description="Masked email address if token is valid")
