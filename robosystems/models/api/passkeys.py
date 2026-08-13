"""Passkey (WebAuthn) and MFA API models.

Ceremony options and credential responses are opaque JSON produced/consumed
by the browser's WebAuthn API — the platform validates them cryptographically
in the kernel, so the HTTP models deliberately leave them as loose dicts.
"""

from typing import Any

from pydantic import BaseModel, Field, model_validator

from .auth import AuthResponse


class PasskeyRegisterOptionsRequest(BaseModel):
  """Begin enrollment.

  Two disjoint lanes: ``mfa_token`` (forced enrollment — the token was minted
  seconds after a password verify, so it is its own freshness proof) or an
  authenticated settings-flow enrollment, which must carry a fresh re-auth
  proof — ``password``, or a ``reauth``-ceremony ``assertion`` when adding a
  passkey beside an existing one.
  """

  mfa_token: str | None = Field(
    default=None,
    description="Enrollment token from a login that returned mfa_enrollment_required",
  )
  password: str | None = Field(
    default=None,
    description="Current password — settings-lane re-authentication",
  )
  assertion: dict[str, Any] | None = Field(
    default=None,
    description="Fresh WebAuthn assertion from the re-auth ceremony (settings lane)",
  )

  @model_validator(mode="after")
  def _lanes_are_disjoint(self) -> "PasskeyRegisterOptionsRequest":
    if self.mfa_token and (self.password or self.assertion):
      raise ValueError("mfa_token and a re-auth proof are mutually exclusive")
    if bool(self.password) and bool(self.assertion):
      raise ValueError("Provide at most one of password or assertion")
    return self


class PasskeyRegisterVerifyRequest(BaseModel):
  """Finish enrollment with the authenticator's attestation response."""

  credential: dict[str, Any] = Field(
    ..., description="WebAuthn registration credential (browser JSON, opaque)"
  )
  name: str | None = Field(
    default=None, max_length=100, description="User-facing label for this passkey"
  )
  mfa_token: str | None = Field(
    default=None,
    description="Enrollment token when finishing a forced enrollment",
  )


class PasskeyInfo(BaseModel):
  """One enrolled passkey, as listed in account settings."""

  id: str = Field(..., description="Passkey identifier")
  name: str = Field(..., description="User-facing label")
  created_at: str = Field(..., description="Enrollment time (ISO 8601)")
  last_used_at: str | None = Field(
    default=None, description="Last successful assertion time (ISO 8601)"
  )
  backup_eligible: bool = Field(
    ..., description="Whether the credential is synced (multi-device) capable"
  )
  backup_state: bool = Field(
    ..., description="Whether the credential is currently backed up"
  )


class PasskeyRegisterVerifyResponse(BaseModel):
  """Enrollment result; the first passkey also carries recovery codes and,
  in the forced-enrollment lane, the completed login."""

  passkey: PasskeyInfo = Field(..., description="The newly enrolled passkey")
  recovery_codes: list[str] | None = Field(
    default=None,
    description="Single-use recovery codes — returned exactly once, at first enrollment",
  )
  auth: AuthResponse | None = Field(
    default=None,
    description="Completed login (forced-enrollment lane only)",
  )


class PasskeyListResponse(BaseModel):
  """The user's enrolled passkeys."""

  passkeys: list[PasskeyInfo] = Field(..., description="Enrolled passkeys")


class PasskeyDeleteRequest(BaseModel):
  """Re-authentication proof for removing a passkey."""

  password: str | None = Field(
    default=None, description="Current password (password-holding users)"
  )
  assertion: dict[str, Any] | None = Field(
    default=None,
    description="Fresh WebAuthn assertion from the re-auth ceremony",
  )

  @model_validator(mode="after")
  def _exactly_one_proof(self) -> "PasskeyDeleteRequest":
    if bool(self.password) == bool(self.assertion):
      raise ValueError("Provide exactly one of password or assertion")
    return self


class CeremonyOptionsResponse(BaseModel):
  """WebAuthn options for the browser, verbatim from the RP library."""

  options: dict[str, Any] = Field(
    ..., description="PublicKeyCredential options (browser JSON, opaque)"
  )


class MfaOptionsRequest(BaseModel):
  """Request assertion options for the second factor."""

  mfa_token: str = Field(
    ..., description="Token from a login that returned mfa_required"
  )


class MfaVerifyRequest(BaseModel):
  """Complete the second factor with an assertion or a recovery code."""

  mfa_token: str = Field(
    ..., description="Token from a login that returned mfa_required"
  )
  assertion: dict[str, Any] | None = Field(
    default=None, description="WebAuthn assertion (browser JSON, opaque)"
  )
  recovery_code: str | None = Field(
    default=None, description="Single-use recovery code"
  )

  @model_validator(mode="after")
  def _exactly_one_factor(self) -> "MfaVerifyRequest":
    if bool(self.assertion) == bool(self.recovery_code):
      raise ValueError("Provide exactly one of assertion or recovery_code")
    return self


class MfaStatusResponse(BaseModel):
  """The user's MFA posture, for account settings."""

  passkey_count: int = Field(..., description="Enrolled passkey count")
  recovery_codes_remaining: int = Field(
    ..., description="Unused recovery codes remaining"
  )
  enforcement_applies: bool = Field(
    ..., description="Whether the MFA requirement applies to this user's roles"
  )


class RecoveryCodesRequest(BaseModel):
  """Re-authentication proof for regenerating recovery codes."""

  password: str | None = Field(
    default=None, description="Current password (password-holding users)"
  )
  assertion: dict[str, Any] | None = Field(
    default=None,
    description="Fresh WebAuthn assertion from the re-auth ceremony",
  )

  @model_validator(mode="after")
  def _exactly_one_proof(self) -> "RecoveryCodesRequest":
    if bool(self.password) == bool(self.assertion):
      raise ValueError("Provide exactly one of password or assertion")
    return self


class RecoveryCodesResponse(BaseModel):
  """A fresh recovery-code set — shown exactly once."""

  codes: list[str] = Field(..., description="Single-use recovery codes")


class PasskeyLoginVerifyRequest(BaseModel):
  """Complete a passwordless login with a discoverable-credential assertion."""

  assertion: dict[str, Any] = Field(
    ..., description="WebAuthn assertion (browser JSON, opaque)"
  )
