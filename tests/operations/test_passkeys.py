"""Tests for the passkey (WebAuthn) kernel.

Ceremony verification is mocked at the module's ``webauthn`` binding — the
library cannot generate authenticator responses, so these tests prove the
kernel's own obligations: single-use challenge handling, flow/principal
cross-checks, credential storage semantics, recovery-code lifecycle, and the
enforcement predicates. The challenge store tests mirror ``TestOIDCState``.
"""

import json
from types import SimpleNamespace
from unittest.mock import patch
from uuid import uuid4

import pytest
from webauthn.helpers import base64url_to_bytes, bytes_to_base64url
from webauthn.helpers.exceptions import (
  InvalidAuthenticationResponse,
  InvalidRegistrationResponse,
)
from webauthn.helpers.structs import CredentialDeviceType

from robosystems.config import env
from robosystems.models.core import (
  Org,
  OrgRole,
  OrgType,
  OrgUser,
  User,
  UserMfaRecoveryCode,
  UserPasskey,
)
from robosystems.operations import passkeys as passkey_ops
from robosystems.operations.passkeys import (
  ChallengeInvalidError,
  DuplicateCredentialError,
  LastPasskeyError,
  PasskeyChallenge,
  PasskeyNotFoundError,
  PasskeysUnavailableError,
  PasskeyUserInactiveError,
  PasskeyVerificationError,
  ReauthInvalidError,
  RecoveryCodeInvalidError,
  complete_authentication,
  complete_registration,
  consume_recovery_code,
  extract_challenge,
  generate_recovery_codes,
  mfa_status,
  remove_passkey,
  user_requires_mfa,
  user_requires_mfa_enrollment,
  verify_reauth,
)
from robosystems.security.password import PasswordSecurity


class _FakeRedis:
  def __init__(self, fail=False):
    self.store: dict[str, str] = {}
    self.fail = fail

  def setex(self, key, ttl, value):
    if self.fail:
      raise ConnectionError("valkey down")
    self.store[key] = value

  def getdel(self, key):
    if self.fail:
      raise ConnectionError("valkey down")
    return self.store.pop(key, None)


def _create_user(session, *, is_active=True, password_hash=None):
  user = User.create(
    email=f"passkey+{uuid4().hex[:8]}@customer.example",
    name="Passkey Tester",
    password_hash=password_hash,
    session=session,
  )
  if not is_active:
    user.update(session, is_active=False)
  return user


def _make_credential(
  challenge: bytes,
  *,
  credential_id: str = "",
  user_handle: str | None = None,
  transports: list[str] | None = None,
):
  """A wire-shaped credential response carrying the given challenge."""
  client_data = json.dumps(
    {"type": "webauthn.get", "challenge": bytes_to_base64url(challenge)}
  ).encode()
  response: dict = {"clientDataJSON": bytes_to_base64url(client_data)}
  if user_handle is not None:
    response["userHandle"] = bytes_to_base64url(user_handle.encode())
  if transports is not None:
    response["transports"] = transports
  return {
    "id": credential_id or bytes_to_base64url(uuid4().bytes),
    "response": response,
  }


def _verified_registration(credential_id_b64: str, *, sign_count=0, backed_up=False):
  return SimpleNamespace(
    credential_id=base64url_to_bytes(credential_id_b64),
    credential_public_key=b"cose-public-key-bytes",
    sign_count=sign_count,
    aaguid="test-aaguid",
    credential_device_type=CredentialDeviceType.MULTI_DEVICE,
    credential_backed_up=backed_up,
  )


def _verified_authentication(*, new_sign_count=1, backed_up=True):
  return SimpleNamespace(
    new_sign_count=new_sign_count,
    credential_backed_up=backed_up,
  )


def _enroll(test_db, user, fake, *, name="Test Key", sign_count=0):
  """Run a full mocked registration ceremony; returns the RegisteredPasskey."""
  with patch.object(passkey_ops, "create_redis_client", return_value=fake):
    challenge = PasskeyChallenge.create(flow="reg", user_id=str(user.id))
    credential = _make_credential(challenge)
    with patch.object(
      passkey_ops.webauthn,
      "verify_registration_response",
      return_value=_verified_registration(credential["id"], sign_count=sign_count),
    ):
      return complete_registration(test_db, user, credential, name=name)


class TestPasskeyChallenge:
  def test_roundtrip_and_single_use(self):
    fake = _FakeRedis()
    with patch.object(passkey_ops, "create_redis_client", return_value=fake):
      challenge = PasskeyChallenge.create(flow="mfa", user_id="u1", jti="j1")
      # The raw challenge is never stored as a key.
      assert all(bytes_to_base64url(challenge) not in key for key in fake.store)

      payload = PasskeyChallenge.consume(challenge)
      assert payload is not None
      assert payload["flow"] == "mfa"
      assert payload["user_id"] == "u1"
      assert payload["jti"] == "j1"

      # Consumed: a second redemption (replay) fails.
      assert PasskeyChallenge.consume(challenge) is None

  def test_unknown_challenge_is_invalid(self):
    with patch.object(passkey_ops, "create_redis_client", return_value=_FakeRedis()):
      assert PasskeyChallenge.consume(b"never-issued") is None

  def test_invalid_flow_refused_at_create(self):
    with pytest.raises(ValueError):
      PasskeyChallenge.create(flow="bogus")

  def test_store_down_at_create_raises(self):
    with patch.object(
      passkey_ops, "create_redis_client", return_value=_FakeRedis(fail=True)
    ):
      with pytest.raises(PasskeysUnavailableError):
        PasskeyChallenge.create(flow="reg", user_id="u1")

  def test_store_down_at_consume_fails_closed(self):
    with patch.object(
      passkey_ops, "create_redis_client", return_value=_FakeRedis(fail=True)
    ):
      assert PasskeyChallenge.consume(b"anything") is None

  def test_malformed_payload_is_invalid(self):
    fake = _FakeRedis()
    with patch.object(passkey_ops, "create_redis_client", return_value=fake):
      challenge = PasskeyChallenge.create(flow="reg", user_id="u1")
      key = next(iter(fake.store))
      fake.store[key] = json.dumps({"flow": "not-a-flow"})
      assert PasskeyChallenge.consume(challenge) is None


class TestExtractChallenge:
  def test_extracts_challenge_bytes(self):
    credential = _make_credential(b"the-challenge")
    assert extract_challenge(credential) == b"the-challenge"

  @pytest.mark.parametrize(
    "credential",
    [
      {},
      {"response": {}},
      {"response": {"clientDataJSON": "!!!not-base64url!!!"}},
      {"response": {"clientDataJSON": bytes_to_base64url(b"not json")}},
      {"response": {"clientDataJSON": bytes_to_base64url(b'{"no": "challenge"}')}},
    ],
  )
  def test_malformed_input_is_domain_error_not_crash(self, credential):
    with pytest.raises(ChallengeInvalidError):
      extract_challenge(credential)


class TestCompleteRegistration:
  def test_first_passkey_returns_recovery_codes(self, test_db):
    user = _create_user(test_db)
    registered = _enroll(test_db, user, _FakeRedis())

    assert registered.recovery_codes is not None
    assert len(registered.recovery_codes) == 10
    assert registered.passkey.user_id == user.id
    assert bool(registered.passkey.backup_eligible) is True
    assert UserPasskey.count_for_user(str(user.id), test_db) == 1

  def test_second_passkey_returns_no_codes(self, test_db):
    user = _create_user(test_db)
    _enroll(test_db, user, _FakeRedis())
    registered = _enroll(test_db, user, _FakeRedis(), name="Second Key")
    assert registered.recovery_codes is None
    assert UserPasskey.count_for_user(str(user.id), test_db) == 2

  def test_duplicate_credential_refused(self, test_db):
    user = _create_user(test_db)
    fake = _FakeRedis()
    registered = _enroll(test_db, user, fake)

    with patch.object(passkey_ops, "create_redis_client", return_value=fake):
      challenge = PasskeyChallenge.create(flow="reg", user_id=str(user.id))
      credential = _make_credential(
        challenge, credential_id=str(registered.passkey.credential_id)
      )
      with patch.object(
        passkey_ops.webauthn,
        "verify_registration_response",
        return_value=_verified_registration(credential["id"]),
      ):
        with pytest.raises(DuplicateCredentialError):
          complete_registration(test_db, user, credential)

  def test_challenge_principal_mismatch_refused(self, test_db):
    user = _create_user(test_db)
    other = _create_user(test_db)
    fake = _FakeRedis()
    with patch.object(passkey_ops, "create_redis_client", return_value=fake):
      challenge = PasskeyChallenge.create(flow="reg", user_id=str(other.id))
      credential = _make_credential(challenge)
      with pytest.raises(ChallengeInvalidError):
        complete_registration(test_db, user, credential)

  def test_wrong_flow_challenge_refused(self, test_db):
    user = _create_user(test_db)
    fake = _FakeRedis()
    with patch.object(passkey_ops, "create_redis_client", return_value=fake):
      challenge = PasskeyChallenge.create(flow="mfa", user_id=str(user.id))
      credential = _make_credential(challenge)
      with pytest.raises(ChallengeInvalidError):
        complete_registration(test_db, user, credential)

  def test_library_rejection_is_domain_error(self, test_db):
    user = _create_user(test_db)
    fake = _FakeRedis()
    with patch.object(passkey_ops, "create_redis_client", return_value=fake):
      challenge = PasskeyChallenge.create(flow="reg", user_id=str(user.id))
      credential = _make_credential(challenge)
      with patch.object(
        passkey_ops.webauthn,
        "verify_registration_response",
        side_effect=InvalidRegistrationResponse("bad attestation"),
      ):
        with pytest.raises(PasskeyVerificationError):
          complete_registration(test_db, user, credential)


class TestCompleteAuthentication:
  def _enrolled(self, test_db, fake, **user_kwargs):
    user = _create_user(test_db, **user_kwargs)
    registered = _enroll(test_db, user, fake, sign_count=5)
    return user, registered.passkey

  def test_mfa_flow_verifies_and_touches(self, test_db):
    fake = _FakeRedis()
    user, passkey = self._enrolled(test_db, fake)
    with patch.object(passkey_ops, "create_redis_client", return_value=fake):
      challenge = PasskeyChallenge.create(flow="mfa", user_id=str(user.id), jti="jti-1")
      credential = _make_credential(challenge, credential_id=str(passkey.credential_id))
      with patch.object(
        passkey_ops.webauthn,
        "verify_authentication_response",
        return_value=_verified_authentication(new_sign_count=6),
      ):
        result = complete_authentication(
          test_db,
          credential,
          expected_flow="mfa",
          expected_user_id=str(user.id),
          expected_jti="jti-1",
        )
    assert result.user.id == user.id
    assert result.sign_count_regressed is False
    assert int(result.passkey.sign_count) == 6
    assert result.passkey.last_used_at is not None
    assert bool(result.passkey.backup_state) is True

  def test_jti_mismatch_refused(self, test_db):
    fake = _FakeRedis()
    user, passkey = self._enrolled(test_db, fake)
    with patch.object(passkey_ops, "create_redis_client", return_value=fake):
      challenge = PasskeyChallenge.create(flow="mfa", user_id=str(user.id), jti="jti-a")
      credential = _make_credential(challenge, credential_id=str(passkey.credential_id))
      with pytest.raises(ChallengeInvalidError):
        complete_authentication(
          test_db,
          credential,
          expected_flow="mfa",
          expected_user_id=str(user.id),
          expected_jti="jti-b",
        )

  def test_sign_count_regression_flagged_not_fatal(self, test_db):
    fake = _FakeRedis()
    user, passkey = self._enrolled(test_db, fake)  # stored sign_count = 5
    with patch.object(passkey_ops, "create_redis_client", return_value=fake):
      challenge = PasskeyChallenge.create(flow="mfa", user_id=str(user.id), jti="jti-2")
      credential = _make_credential(challenge, credential_id=str(passkey.credential_id))
      with patch.object(
        passkey_ops.webauthn,
        "verify_authentication_response",
        return_value=_verified_authentication(new_sign_count=2),
      ):
        result = complete_authentication(
          test_db,
          credential,
          expected_flow="mfa",
          expected_user_id=str(user.id),
          expected_jti="jti-2",
        )
    assert result.sign_count_regressed is True

  def test_passwordless_resolves_user_from_credential(self, test_db):
    fake = _FakeRedis()
    user, passkey = self._enrolled(test_db, fake)
    with patch.object(passkey_ops, "create_redis_client", return_value=fake):
      challenge = PasskeyChallenge.create(flow="pwl")
      credential = _make_credential(
        challenge,
        credential_id=str(passkey.credential_id),
        user_handle=str(user.id),
      )
      with patch.object(
        passkey_ops.webauthn,
        "verify_authentication_response",
        return_value=_verified_authentication(new_sign_count=6),
      ):
        result = complete_authentication(test_db, credential, expected_flow="pwl")
    assert result.user.id == user.id

  def test_passwordless_user_handle_mismatch_refused(self, test_db):
    fake = _FakeRedis()
    user, passkey = self._enrolled(test_db, fake)
    other = _create_user(test_db)
    with patch.object(passkey_ops, "create_redis_client", return_value=fake):
      challenge = PasskeyChallenge.create(flow="pwl")
      credential = _make_credential(
        challenge,
        credential_id=str(passkey.credential_id),
        user_handle=str(other.id),
      )
      with pytest.raises(PasskeyVerificationError):
        complete_authentication(test_db, credential, expected_flow="pwl")

  def test_unknown_credential_refused(self, test_db):
    fake = _FakeRedis()
    with patch.object(passkey_ops, "create_redis_client", return_value=fake):
      challenge = PasskeyChallenge.create(flow="pwl")
      credential = _make_credential(challenge)
      with pytest.raises(PasskeyNotFoundError):
        complete_authentication(test_db, credential, expected_flow="pwl")

  def test_credential_owned_by_other_user_refused(self, test_db):
    """A challenge bound to user A cannot be completed with user B's key."""
    fake = _FakeRedis()
    user_a, _ = self._enrolled(test_db, fake)
    _user_b, passkey_b = self._enrolled(test_db, fake)
    with patch.object(passkey_ops, "create_redis_client", return_value=fake):
      challenge = PasskeyChallenge.create(
        flow="mfa", user_id=str(user_a.id), jti="jti-x"
      )
      credential = _make_credential(
        challenge, credential_id=str(passkey_b.credential_id)
      )
      with pytest.raises(PasskeyVerificationError):
        complete_authentication(
          test_db,
          credential,
          expected_flow="mfa",
          expected_user_id=str(user_a.id),
          expected_jti="jti-x",
        )

  def test_inactive_user_refused(self, test_db):
    fake = _FakeRedis()
    user, passkey = self._enrolled(test_db, fake)
    user.update(test_db, is_active=False)
    with patch.object(passkey_ops, "create_redis_client", return_value=fake):
      challenge = PasskeyChallenge.create(flow="pwl")
      credential = _make_credential(challenge, credential_id=str(passkey.credential_id))
      with pytest.raises(PasskeyUserInactiveError):
        complete_authentication(test_db, credential, expected_flow="pwl")

  def test_library_rejection_is_domain_error(self, test_db):
    fake = _FakeRedis()
    user, passkey = self._enrolled(test_db, fake)
    with patch.object(passkey_ops, "create_redis_client", return_value=fake):
      challenge = PasskeyChallenge.create(flow="pwl")
      credential = _make_credential(challenge, credential_id=str(passkey.credential_id))
      with patch.object(
        passkey_ops.webauthn,
        "verify_authentication_response",
        side_effect=InvalidAuthenticationResponse("bad signature"),
      ):
        with pytest.raises(PasskeyVerificationError):
          complete_authentication(test_db, credential, expected_flow="pwl")


class TestRecoveryCodes:
  def test_generate_consume_and_reuse_refused(self, test_db):
    user = _create_user(test_db)
    codes = generate_recovery_codes(test_db, user)
    assert len(codes) == 10
    assert UserMfaRecoveryCode.remaining_count(str(user.id), test_db) == 10

    consume_recovery_code(test_db, user, codes[0])
    assert UserMfaRecoveryCode.remaining_count(str(user.id), test_db) == 9

    with pytest.raises(RecoveryCodeInvalidError):
      consume_recovery_code(test_db, user, codes[0])

  def test_entry_is_forgiving(self, test_db):
    """Lowercase, spaces, and missing dashes must all match."""
    user = _create_user(test_db)
    codes = generate_recovery_codes(test_db, user)
    sloppy = codes[1].lower().replace("-", " ")
    consume_recovery_code(test_db, user, sloppy)

  def test_wrong_code_refused(self, test_db):
    user = _create_user(test_db)
    generate_recovery_codes(test_db, user)
    with pytest.raises(RecoveryCodeInvalidError):
      consume_recovery_code(test_db, user, "AAAAA-AAAAA")

  def test_empty_code_refused(self, test_db):
    user = _create_user(test_db)
    generate_recovery_codes(test_db, user)
    with pytest.raises(RecoveryCodeInvalidError):
      consume_recovery_code(test_db, user, "   ")

  def test_regeneration_invalidates_old_set(self, test_db):
    user = _create_user(test_db)
    old_codes = generate_recovery_codes(test_db, user)
    new_codes = generate_recovery_codes(test_db, user)
    assert UserMfaRecoveryCode.remaining_count(str(user.id), test_db) == 10
    with pytest.raises(RecoveryCodeInvalidError):
      consume_recovery_code(test_db, user, old_codes[0])
    consume_recovery_code(test_db, user, new_codes[0])

  def test_codes_are_not_cross_user(self, test_db):
    user_a = _create_user(test_db)
    user_b = _create_user(test_db)
    codes_a = generate_recovery_codes(test_db, user_a)
    generate_recovery_codes(test_db, user_b)
    with pytest.raises(RecoveryCodeInvalidError):
      consume_recovery_code(test_db, user_b, codes_a[0])


class TestReauthAndRemoval:
  def _password_user(self, test_db):
    return _create_user(
      test_db, password_hash=PasswordSecurity.hash_password("T3stP@ssw0rd!")
    )

  def test_password_reauth_ok(self, test_db):
    user = self._password_user(test_db)
    verify_reauth(test_db, user, password="T3stP@ssw0rd!")

  def test_wrong_password_refused(self, test_db):
    user = self._password_user(test_db)
    with pytest.raises(ReauthInvalidError):
      verify_reauth(test_db, user, password="wrong")

  def test_no_proof_or_both_proofs_refused(self, test_db):
    user = self._password_user(test_db)
    with pytest.raises(ReauthInvalidError):
      verify_reauth(test_db, user)
    with pytest.raises(ReauthInvalidError):
      verify_reauth(test_db, user, password="x", assertion={"id": "y"})

  def test_passwordless_user_cannot_password_reauth(self, test_db):
    user = _create_user(test_db, password_hash=None)
    with pytest.raises(ReauthInvalidError):
      verify_reauth(test_db, user, password="anything")

  def test_remove_passkey_with_password(self, test_db):
    user = self._password_user(test_db)
    registered = _enroll(test_db, user, _FakeRedis())
    remove_passkey(test_db, user, str(registered.passkey.id), password="T3stP@ssw0rd!")
    assert UserPasskey.count_for_user(str(user.id), test_db) == 0

  def test_remove_unknown_passkey_refused(self, test_db):
    user = self._password_user(test_db)
    _enroll(test_db, user, _FakeRedis())
    with pytest.raises(PasskeyNotFoundError):
      remove_passkey(test_db, user, "upk_nonexistent", password="T3stP@ssw0rd!")

  def test_last_passkey_of_enforced_role_refused(self, test_db):
    user = self._password_user(test_db)
    org = Org.create(name="Enforced", org_type=OrgType.ENTERPRISE, session=test_db)
    OrgUser.create(str(org.id), str(user.id), OrgRole.OWNER, test_db)
    registered = _enroll(test_db, user, _FakeRedis())

    with patch.object(env, "MFA_ENFORCEMENT_ENABLED", True):
      with pytest.raises(LastPasskeyError):
        remove_passkey(
          test_db, user, str(registered.passkey.id), password="T3stP@ssw0rd!"
        )
    # Enforcement off: same removal goes through.
    remove_passkey(test_db, user, str(registered.passkey.id), password="T3stP@ssw0rd!")


class TestEnforcementPredicates:
  def test_member_never_requires_mfa(self, test_db):
    user = _create_user(test_db)
    org = Org.create(name="Members", org_type=OrgType.ENTERPRISE, session=test_db)
    OrgUser.create(str(org.id), str(user.id), OrgRole.MEMBER, test_db)
    assert user_requires_mfa(test_db, user) is False
    with patch.object(env, "MFA_ENFORCEMENT_ENABLED", True):
      assert user_requires_mfa_enrollment(test_db, user) is False

  @pytest.mark.parametrize("role", [OrgRole.OWNER, OrgRole.ADMIN])
  def test_privileged_role_requires_enrollment_when_enforced(self, test_db, role):
    user = _create_user(test_db)
    org = Org.create(name=f"Org-{role}", org_type=OrgType.ENTERPRISE, session=test_db)
    OrgUser.create(str(org.id), str(user.id), role, test_db)

    assert user_requires_mfa(test_db, user) is True
    with patch.object(env, "MFA_ENFORCEMENT_ENABLED", True):
      assert user_requires_mfa_enrollment(test_db, user) is True
    # Flag off: predicate never fires.
    with patch.object(env, "MFA_ENFORCEMENT_ENABLED", False):
      assert user_requires_mfa_enrollment(test_db, user) is False

  def test_enrolled_owner_not_forced(self, test_db):
    user = _create_user(test_db)
    org = Org.create(name="Enrolled", org_type=OrgType.ENTERPRISE, session=test_db)
    OrgUser.create(str(org.id), str(user.id), OrgRole.OWNER, test_db)
    _enroll(test_db, user, _FakeRedis())
    with patch.object(env, "MFA_ENFORCEMENT_ENABLED", True):
      assert user_requires_mfa_enrollment(test_db, user) is False


class TestMfaStatus:
  def test_counts_and_enforcement(self, test_db):
    user = _create_user(test_db)
    org = Org.create(name="Status", org_type=OrgType.ENTERPRISE, session=test_db)
    OrgUser.create(str(org.id), str(user.id), OrgRole.ADMIN, test_db)
    registered = _enroll(test_db, user, _FakeRedis())
    consume_recovery_code(test_db, user, (registered.recovery_codes or [])[0])

    status = mfa_status(test_db, user)
    assert status.passkey_count == 1
    assert status.recovery_codes_remaining == 9

    with (
      patch.object(env, "PASSKEYS_ENABLED", True),
      patch.object(env, "MFA_ENFORCEMENT_ENABLED", True),
    ):
      assert mfa_status(test_db, user).enforcement_applies is True
