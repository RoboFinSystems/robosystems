"""Passkey (WebAuthn) kernel: ceremonies, challenge state, recovery codes.

The browser's authenticator signs; the platform verifies and mints. These
functions cover everything between — generating registration/authentication
options, holding the single-use challenge, verifying attestations and
assertions (user verification always required, so one gesture is two
factors), and the recovery-code backstop.

Contract: session-in, dataclass-out, domain exceptions — never HTTP. Routers
own their gates (feature flag, rate limits, progressive delay) and side
channels (audit events, metrics), same as ``operations/oidc.py`` and
``operations/user_provisioning.py``.

Challenge flows are namespaced so a challenge minted for one ceremony can
never complete another: ``reg`` (enrollment), ``mfa`` (second factor, bound
to the login's mfa_token jti), ``pwl`` (passwordless login — no user known
until the assertion resolves), ``mgmt`` (re-auth for destructive lifecycle
actions).
"""

import hashlib
import json
import secrets
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import webauthn
from sqlalchemy.orm import Session
from webauthn.helpers import base64url_to_bytes, bytes_to_base64url
from webauthn.helpers.exceptions import (
  InvalidAuthenticationResponse,
  InvalidJSONStructure,
  InvalidRegistrationResponse,
)
from webauthn.helpers.structs import (
  AuthenticatorSelectionCriteria,
  CredentialDeviceType,
  PublicKeyCredentialDescriptor,
  ResidentKeyRequirement,
  UserVerificationRequirement,
)

from robosystems.config import env
from robosystems.config.valkey_registry import ValkeyDatabase, create_redis_client
from robosystems.logger import logger
from robosystems.models.core import (
  OrgUser,
  User,
  UserMfaRecoveryCode,
  UserPasskey,
)
from robosystems.models.core.org import OrgRole
from robosystems.security.password import PasswordSecurity

_CHALLENGE_TTL_SECONDS = 300
_CHALLENGE_KEY_PREFIX = "passkey:challenge:"

# Flows an MFA-required role can satisfy; also the display name shown by
# authenticator prompts.
_RP_NAME = "RoboSystems"

VALID_CHALLENGE_FLOWS = ("reg", "mfa", "pwl", "mgmt")


class PasskeyError(Exception):
  """Base class for passkey failures."""


class PasskeysUnavailableError(PasskeyError):
  """A dependency (challenge store, RP config) is unusable."""


class ChallengeInvalidError(PasskeyError):
  """The presented response has no live matching challenge."""


class PasskeyVerificationError(PasskeyError):
  """Attestation/assertion verification failed."""


class PasskeyNotFoundError(PasskeyError):
  """No enrolled credential matches the request."""


class DuplicateCredentialError(PasskeyError):
  """The credential is already registered."""


class LastPasskeyError(PasskeyError):
  """Refusing to remove the last passkey of an MFA-required user."""


class RecoveryCodeInvalidError(PasskeyError):
  """The submitted recovery code matched no unused code."""


class PasskeyUserInactiveError(PasskeyError):
  """The resolved user is deactivated."""


class ReauthInvalidError(PasskeyError):
  """The re-authentication proof for a lifecycle action failed."""


@dataclass(frozen=True)
class CeremonyOptions:
  """JSON-serialized WebAuthn options, opaque to everything but the browser."""

  options_json: str


@dataclass(frozen=True)
class RegisteredPasskey:
  passkey: UserPasskey
  # Present only when this enrollment created the user's first passkey.
  recovery_codes: list[str] | None


@dataclass(frozen=True)
class VerifiedAssertion:
  user: User
  passkey: UserPasskey
  sign_count_regressed: bool


@dataclass(frozen=True)
class MfaStatus:
  passkey_count: int
  recovery_codes_remaining: int
  enforcement_applies: bool


def _rp_id() -> str:
  rp_id = env.get_passkey_rp_id()
  if not rp_id:
    raise PasskeysUnavailableError("No WebAuthn RP ID is configured")
  return rp_id


def _expected_origin() -> str:
  origin = env.get_passkey_origin()
  if not origin:
    raise PasskeysUnavailableError("No WebAuthn origin is configured")
  return origin


class PasskeyChallenge:
  """Single-use, 5-minute WebAuthn challenge state.

  Same shape as ``operations/oidc.OIDCState`` and for the same reasons:
  Valkey because verify lands on an arbitrary task, SHA-256 keys so a store
  read can't replay a ceremony, ``GETDEL`` so consumption is atomically
  single-use, and fail-closed validation. Keyed by the challenge itself
  (extracted from the response's clientDataJSON at verify time), which works
  uniformly across flows — including passwordless, where no user is known
  until the assertion resolves.
  """

  @staticmethod
  def _key(challenge: bytes) -> str:
    return f"{_CHALLENGE_KEY_PREFIX}{hashlib.sha256(challenge).hexdigest()}"

  @classmethod
  def create(
    cls, *, flow: str, user_id: str | None = None, jti: str | None = None
  ) -> bytes:
    """Mint a challenge and persist what verify needs to cross-check."""
    if flow not in VALID_CHALLENGE_FLOWS:
      raise ValueError(f"Invalid challenge flow: {flow}")
    challenge = secrets.token_bytes(32)
    payload = {
      "flow": flow,
      "user_id": user_id,
      "jti": jti,
      "created_at": datetime.now(UTC).isoformat(),
    }
    try:
      client = create_redis_client(ValkeyDatabase.AUTH)
      client.setex(cls._key(challenge), _CHALLENGE_TTL_SECONDS, json.dumps(payload))
    except Exception as exc:
      logger.error(f"Failed to persist passkey challenge: {exc}")
      raise PasskeysUnavailableError("Unable to start the ceremony") from exc
    return challenge

  @classmethod
  def consume(cls, challenge: bytes) -> dict[str, Any] | None:
    """Consume a challenge, returning its payload, or None if invalid.

    Fails closed: an unreachable or unparseable store reads as an invalid
    challenge rather than waving the ceremony through.
    """
    try:
      client = create_redis_client(ValkeyDatabase.AUTH)
      raw = client.getdel(cls._key(challenge))
    except Exception as exc:
      logger.error(f"Failed to read passkey challenge: {exc}")
      return None
    if not raw:
      return None
    try:
      payload = json.loads(raw)
      if payload.get("flow") not in VALID_CHALLENGE_FLOWS:
        raise ValueError("missing or invalid flow")
    except (ValueError, TypeError) as exc:
      logger.error(f"Discarding malformed passkey challenge: {exc}")
      return None
    return payload


def extract_challenge(credential: dict[str, Any]) -> bytes:
  """Pull the challenge out of a response's clientDataJSON.

  The input is attacker-controlled; every malformation maps to
  ``ChallengeInvalidError`` so the caller's failure accounting fires instead
  of a 500.
  """
  try:
    client_data_raw = credential["response"]["clientDataJSON"]
    client_data = json.loads(base64url_to_bytes(client_data_raw))
    return base64url_to_bytes(client_data["challenge"])
  except Exception as exc:
    raise ChallengeInvalidError("Malformed client data") from exc


def _consume_challenge_for(
  credential: dict[str, Any],
  *,
  expected_flow: str,
  expected_user_id: str | None = None,
  expected_jti: str | None = None,
) -> dict[str, Any]:
  """Extract, consume, and cross-check the challenge behind a response."""
  challenge = extract_challenge(credential)
  payload = PasskeyChallenge.consume(challenge)
  if payload is None:
    raise ChallengeInvalidError("Unknown or expired challenge")
  if payload.get("flow") != expected_flow:
    raise ChallengeInvalidError("Challenge flow mismatch")
  if expected_user_id is not None and payload.get("user_id") != expected_user_id:
    raise ChallengeInvalidError("Challenge principal mismatch")
  if expected_jti is not None and payload.get("jti") != expected_jti:
    raise ChallengeInvalidError("Challenge token mismatch")
  return payload


def _allow_credentials(
  passkeys: list[UserPasskey],
) -> list[PublicKeyCredentialDescriptor]:
  return [
    PublicKeyCredentialDescriptor(id=base64url_to_bytes(str(pk.credential_id)))
    for pk in passkeys
  ]


def begin_registration(session: Session, user: User) -> CeremonyOptions:
  """Start an enrollment ceremony for an authenticated (or enroll-token) user."""
  existing = UserPasskey.get_all_for_user(str(user.id), session)
  challenge = PasskeyChallenge.create(flow="reg", user_id=str(user.id))
  options = webauthn.generate_registration_options(
    rp_id=_rp_id(),
    rp_name=_RP_NAME,
    user_id=str(user.id).encode(),
    user_name=str(user.email),
    user_display_name=str(user.name or user.email),
    challenge=challenge,
    # Discoverable credential + UV: what makes a passkey usable as a
    # usernameless primary login and two factors in one gesture.
    authenticator_selection=AuthenticatorSelectionCriteria(
      resident_key=ResidentKeyRequirement.REQUIRED,
      user_verification=UserVerificationRequirement.REQUIRED,
    ),
    exclude_credentials=_allow_credentials(existing),
  )
  return CeremonyOptions(options_json=webauthn.options_to_json(options))


def complete_registration(
  session: Session,
  user: User,
  credential: dict[str, Any],
  name: str | None = None,
) -> RegisteredPasskey:
  """Verify an attestation and store the credential.

  The first passkey also mints the user's recovery-code set — returned in
  plaintext exactly once.
  """
  _consume_challenge_for(credential, expected_flow="reg", expected_user_id=str(user.id))
  # Challenge equality is proven by the store hit: the key IS the hash of
  # the challenge the response presented, so pass that same value through.
  expected_challenge = extract_challenge(credential)
  try:
    verified = webauthn.verify_registration_response(
      credential=credential,
      expected_challenge=expected_challenge,
      expected_rp_id=_rp_id(),
      expected_origin=_expected_origin(),
      require_user_verification=True,
    )
  except (InvalidRegistrationResponse, InvalidJSONStructure) as exc:
    raise PasskeyVerificationError("Registration verification failed") from exc

  credential_id = bytes_to_base64url(verified.credential_id)
  if UserPasskey.get_by_credential_id(credential_id, session) is not None:
    raise DuplicateCredentialError("This passkey is already registered")

  transports = credential.get("response", {}).get("transports")
  if not isinstance(transports, list):
    transports = None

  first_passkey = UserPasskey.count_for_user(str(user.id), session) == 0
  passkey = UserPasskey.create(
    user_id=str(user.id),
    credential_id=credential_id,
    public_key=verified.credential_public_key,
    session=session,
    sign_count=verified.sign_count,
    transports=transports,
    aaguid=str(verified.aaguid) if verified.aaguid else None,
    backup_eligible=(
      verified.credential_device_type == CredentialDeviceType.MULTI_DEVICE
    ),
    backup_state=bool(verified.credential_backed_up),
    name=(name or "Passkey").strip()[:100] or "Passkey",
  )
  recovery_codes = (
    UserMfaRecoveryCode.create_set(str(user.id), session) if first_passkey else None
  )
  return RegisteredPasskey(passkey=passkey, recovery_codes=recovery_codes)


def begin_authentication(session: Session, user: User, jti: str) -> CeremonyOptions:
  """Second-factor assertion options, bound to the login's mfa_token jti."""
  passkeys = UserPasskey.get_all_for_user(str(user.id), session)
  if not passkeys:
    raise PasskeyNotFoundError("No passkeys enrolled")
  challenge = PasskeyChallenge.create(flow="mfa", user_id=str(user.id), jti=jti)
  options = webauthn.generate_authentication_options(
    rp_id=_rp_id(),
    challenge=challenge,
    allow_credentials=_allow_credentials(passkeys),
    user_verification=UserVerificationRequirement.REQUIRED,
  )
  return CeremonyOptions(options_json=webauthn.options_to_json(options))


def begin_passwordless_authentication() -> CeremonyOptions:
  """Usernameless assertion options — empty allow list, resolved at verify."""
  challenge = PasskeyChallenge.create(flow="pwl")
  options = webauthn.generate_authentication_options(
    rp_id=_rp_id(),
    challenge=challenge,
    allow_credentials=[],
    user_verification=UserVerificationRequirement.REQUIRED,
  )
  return CeremonyOptions(options_json=webauthn.options_to_json(options))


def begin_reauth(session: Session, user: User) -> CeremonyOptions:
  """Fresh-assertion options for destructive lifecycle actions."""
  passkeys = UserPasskey.get_all_for_user(str(user.id), session)
  if not passkeys:
    raise PasskeyNotFoundError("No passkeys enrolled")
  challenge = PasskeyChallenge.create(flow="mgmt", user_id=str(user.id))
  options = webauthn.generate_authentication_options(
    rp_id=_rp_id(),
    challenge=challenge,
    allow_credentials=_allow_credentials(passkeys),
    user_verification=UserVerificationRequirement.REQUIRED,
  )
  return CeremonyOptions(options_json=webauthn.options_to_json(options))


def complete_authentication(
  session: Session,
  credential: dict[str, Any],
  *,
  expected_flow: str,
  expected_user_id: str | None = None,
  expected_jti: str | None = None,
) -> VerifiedAssertion:
  """Verify an assertion against its single-use challenge and stored key.

  Resolves the credential (and, for passwordless, the user) from the
  response itself; user verification is always required. Sign-count
  regression is flagged, never fatal — synced passkeys legitimately report
  zero forever, and a hard clone-detection block would lock out every
  iCloud user.
  """
  payload = _consume_challenge_for(
    credential,
    expected_flow=expected_flow,
    expected_user_id=expected_user_id,
    expected_jti=expected_jti,
  )

  raw_credential_id = credential.get("id")
  if not isinstance(raw_credential_id, str) or not raw_credential_id:
    raise PasskeyVerificationError("Malformed credential")
  passkey = UserPasskey.get_by_credential_id(raw_credential_id, session)
  if passkey is None:
    raise PasskeyNotFoundError("Unknown credential")

  # For user-bound flows the challenge principal must own the credential;
  # for passwordless the credential itself names the principal.
  challenge_user_id = payload.get("user_id")
  if challenge_user_id is not None and str(passkey.user_id) != challenge_user_id:
    raise PasskeyVerificationError("Credential does not belong to this login")

  user = User.get_by_id(str(passkey.user_id), session)
  if user is None:
    raise PasskeyNotFoundError("Unknown credential")
  if not bool(user.is_active):
    raise PasskeyUserInactiveError("User account is deactivated")

  # Discoverable credentials return the userHandle we set at registration
  # (the user id); when present it must agree with the resolved owner.
  user_handle = credential.get("response", {}).get("userHandle")
  if user_handle:
    try:
      handle_value = base64url_to_bytes(user_handle).decode()
    except Exception as exc:
      raise PasskeyVerificationError("Malformed credential") from exc
    if handle_value != str(passkey.user_id):
      raise PasskeyVerificationError("Credential does not belong to this login")

  expected_challenge = extract_challenge(credential)
  try:
    verified = webauthn.verify_authentication_response(
      credential=credential,
      expected_challenge=expected_challenge,
      expected_rp_id=_rp_id(),
      expected_origin=_expected_origin(),
      credential_public_key=bytes(passkey.public_key),
      credential_current_sign_count=int(passkey.sign_count),
      require_user_verification=True,
    )
  except (InvalidAuthenticationResponse, InvalidJSONStructure) as exc:
    raise PasskeyVerificationError("Assertion verification failed") from exc

  regressed = int(passkey.sign_count) > 0 and verified.new_sign_count < int(
    passkey.sign_count
  )
  if regressed:
    logger.warning(
      f"Passkey sign count regressed for credential {passkey.id} "
      f"(stored={passkey.sign_count}, presented={verified.new_sign_count})"
    )
  passkey.touch_used(
    session,
    new_sign_count=verified.new_sign_count,
    backup_state=bool(verified.credential_backed_up),
  )
  return VerifiedAssertion(user=user, passkey=passkey, sign_count_regressed=regressed)


def generate_recovery_codes(session: Session, user: User) -> list[str]:
  """Mint a fresh recovery-code set, invalidating any prior set."""
  return UserMfaRecoveryCode.create_set(str(user.id), session)


def consume_recovery_code(session: Session, user: User, code: str) -> None:
  """Burn one unused recovery code; raises when nothing matches."""
  if not code or not code.strip():
    raise RecoveryCodeInvalidError("Invalid recovery code")
  if not UserMfaRecoveryCode.consume(str(user.id), code, session):
    raise RecoveryCodeInvalidError("Invalid recovery code")


def verify_reauth(
  session: Session,
  user: User,
  *,
  password: str | None = None,
  assertion: dict[str, Any] | None = None,
) -> None:
  """Prove a live principal before a destructive lifecycle action.

  Accepts a password re-entry (for password-holding users) or a fresh
  ``mgmt``-flow assertion. Exactly one proof must be supplied.
  """
  if bool(password) == bool(assertion):
    raise ReauthInvalidError("Provide a password or a passkey assertion")
  if password:
    if not user.password_hash or not PasswordSecurity.verify_password(
      password, str(user.password_hash)
    ):
      raise ReauthInvalidError("Re-authentication failed")
    return
  assert assertion is not None
  try:
    complete_authentication(
      session,
      assertion,
      expected_flow="mgmt",
      expected_user_id=str(user.id),
    )
  except PasskeyError as exc:
    raise ReauthInvalidError("Re-authentication failed") from exc


def remove_passkey(
  session: Session,
  user: User,
  passkey_id: str,
  *,
  password: str | None = None,
  assertion: dict[str, Any] | None = None,
) -> None:
  """Delete one credential after re-auth; never the last one of an
  MFA-required user while enforcement is on."""
  verify_reauth(session, user, password=password, assertion=assertion)
  passkeys = UserPasskey.get_all_for_user(str(user.id), session)
  target = next((pk for pk in passkeys if str(pk.id) == passkey_id), None)
  if target is None:
    raise PasskeyNotFoundError("No such passkey")
  if (
    len(passkeys) == 1
    and env.MFA_ENFORCEMENT_ENABLED
    and user_requires_mfa(session, user)
  ):
    raise LastPasskeyError(
      "This role requires MFA; enroll another passkey before removing this one"
    )
  session.delete(target)
  session.commit()


def user_requires_mfa(session: Session, user: User) -> bool:
  """Whether the user holds a role the enforcement gate applies to.

  Org OWNER/ADMIN only in P1 — org privilege already implies graph admin,
  and the residual explicit graph-admin population isn't worth a second
  per-login query yet.
  """
  return (
    session.query(OrgUser.org_id)
    .filter(
      OrgUser.user_id == str(user.id),
      OrgUser.role.in_([OrgRole.OWNER, OrgRole.ADMIN]),
    )
    .limit(1)
    .first()
    is not None
  )


def user_requires_mfa_enrollment(session: Session, user: User) -> bool:
  """Enforcement predicate at password login: required role, zero passkeys."""
  if not env.MFA_ENFORCEMENT_ENABLED:
    return False
  if UserPasskey.count_for_user(str(user.id), session) > 0:
    return False
  return user_requires_mfa(session, user)


def mfa_status(session: Session, user: User) -> MfaStatus:
  return MfaStatus(
    passkey_count=UserPasskey.count_for_user(str(user.id), session),
    recovery_codes_remaining=UserMfaRecoveryCode.remaining_count(str(user.id), session),
    enforcement_applies=(
      env.PASSKEYS_ENABLED
      and env.MFA_ENFORCEMENT_ENABLED
      and user_requires_mfa(session, user)
    ),
  )
