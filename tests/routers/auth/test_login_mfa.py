"""Login MFA branch + second-factor handshake seam tests.

HTTP-level through the real login endpoint: the producer (login mints the
mfa_token) and the consumer (/mfa/options + /mfa/verify redeem it) are
exercised as one flow, the way test_oidc.py proves its callback→completion
seam. WebAuthn assertion verification is mocked at the kernel's webauthn
binding; everything else — token minting/decoding, challenge store,
retry budget, enforcement query — runs real against the test DB and a fake
Valkey.
"""

import json
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import patch
from uuid import uuid4

import pytest
from webauthn.helpers import bytes_to_base64url

from robosystems.config import env
from robosystems.middleware.auth.jwt import decode_mfa_token
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
from robosystems.security.password import PasswordSecurity

PASSWORD = "T3stP@ssw0rd!x"


class _FakeRedis:
  def __init__(self):
    self.store: dict[str, str] = {}

  def setex(self, key, ttl, value):
    self.store[key] = str(value)

  def getdel(self, key):
    return self.store.pop(key, None)

  def get(self, key):
    return self.store.get(key)

  def incr(self, key):
    value = int(self.store.get(key, "0")) + 1
    self.store[key] = str(value)
    return value

  def expire(self, key, ttl):
    return True


@contextmanager
def _fake_auth_valkey():
  fake = _FakeRedis()
  with (
    patch.object(passkey_ops, "create_redis_client", return_value=fake),
    patch(
      "robosystems.routers.auth.mfa.create_redis_client",
      return_value=fake,
    ),
  ):
    yield fake


def _create_user(session, *, with_org_role=None):
  user = User.create(
    email=f"mfa+{uuid4().hex[:8]}@customer.example",
    name="MFA Tester",
    password_hash=PasswordSecurity.hash_password(PASSWORD),
    session=session,
  )
  if with_org_role is not None:
    org = Org.create(
      name=f"Org {uuid4().hex[:6]}", org_type=OrgType.ENTERPRISE, session=session
    )
    OrgUser.create(str(org.id), str(user.id), with_org_role, session)
  return user


def _enroll_passkey_row(session, user, credential_id=None):
  return UserPasskey.create(
    user_id=str(user.id),
    credential_id=credential_id or bytes_to_base64url(uuid4().bytes),
    public_key=b"cose-key",
    session=session,
    sign_count=0,
  )


def _login(client, user):
  return client.post("/v1/auth/login", json={"email": user.email, "password": PASSWORD})


def _assertion_for(options_json: dict, credential_id: str):
  """Build a wire-shaped assertion echoing the options' challenge."""
  challenge_b64 = options_json["challenge"]
  client_data = json.dumps(
    {"type": "webauthn.get", "challenge": challenge_b64}
  ).encode()
  return {
    "id": credential_id,
    "response": {"clientDataJSON": bytes_to_base64url(client_data)},
  }


def _verified_authentication(**overrides):
  defaults = {"new_sign_count": 1, "credential_backed_up": False}
  defaults.update(overrides)
  return SimpleNamespace(**defaults)


class TestLoginMfaBranch:
  def test_flag_off_is_byte_identical_even_with_passkey(self, client, test_db):
    user = _create_user(test_db)
    _enroll_passkey_row(test_db, user)
    with patch.object(env, "PASSKEYS_ENABLED", False):
      resp = _login(client, user)
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "authenticated"
    assert body["token"]
    assert body["mfa_token"] is None

  def test_enrolled_user_gets_mfa_required(self, client, test_db):
    user = _create_user(test_db)
    _enroll_passkey_row(test_db, user)
    with patch.object(env, "PASSKEYS_ENABLED", True):
      resp = _login(client, user)
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "mfa_required"
    assert body["token"] is None
    payload = decode_mfa_token(body["mfa_token"], "login")
    assert payload is not None and payload["user_id"] == str(user.id)

  def test_unenrolled_member_unaffected_by_enforcement(self, client, test_db):
    user = _create_user(test_db, with_org_role=OrgRole.MEMBER)
    with (
      patch.object(env, "PASSKEYS_ENABLED", True),
      patch.object(env, "MFA_ENFORCEMENT_ENABLED", True),
    ):
      resp = _login(client, user)
    assert resp.json()["status"] == "authenticated"

  @pytest.mark.parametrize("role", [OrgRole.OWNER, OrgRole.ADMIN])
  def test_unenrolled_privileged_forced_to_enroll(self, client, test_db, role):
    user = _create_user(test_db, with_org_role=role)
    with (
      patch.object(env, "PASSKEYS_ENABLED", True),
      patch.object(env, "MFA_ENFORCEMENT_ENABLED", True),
    ):
      resp = _login(client, user)
    body = resp.json()
    assert body["status"] == "mfa_enrollment_required"
    assert body["token"] is None
    # The token is enroll-purpose: it can never satisfy the login handshake.
    assert decode_mfa_token(body["mfa_token"], "enroll") is not None
    assert decode_mfa_token(body["mfa_token"], "login") is None

  def test_unenrolled_owner_normal_when_enforcement_off(self, client, test_db):
    user = _create_user(test_db, with_org_role=OrgRole.OWNER)
    with (
      patch.object(env, "PASSKEYS_ENABLED", True),
      patch.object(env, "MFA_ENFORCEMENT_ENABLED", False),
    ):
      resp = _login(client, user)
    assert resp.json()["status"] == "authenticated"


class TestMfaHandshakeSeam:
  """Producer→consumer: the token login mints is what /mfa/* redeems."""

  def _login_to_challenge(self, client, test_db):
    user = _create_user(test_db)
    passkey = _enroll_passkey_row(test_db, user)
    resp = _login(client, user)
    assert resp.json()["status"] == "mfa_required"
    return user, passkey, resp.json()["mfa_token"]

  def test_full_assertion_handshake(self, client, test_db):
    with patch.object(env, "PASSKEYS_ENABLED", True), _fake_auth_valkey():
      user, passkey, mfa_token = self._login_to_challenge(client, test_db)

      options_resp = client.post("/v1/auth/mfa/options", json={"mfa_token": mfa_token})
      assert options_resp.status_code == 200
      options = options_resp.json()["options"]
      assertion = _assertion_for(options, str(passkey.credential_id))

      with patch.object(
        passkey_ops.webauthn,
        "verify_authentication_response",
        return_value=_verified_authentication(),
      ):
        verify_resp = client.post(
          "/v1/auth/mfa/verify",
          json={"mfa_token": mfa_token, "assertion": assertion},
        )
      assert verify_resp.status_code == 200
      body = verify_resp.json()
      assert body["status"] == "authenticated"
      assert body["token"]

      # jti burned: the same mfa_token cannot mint a second session.
      replay = client.post(
        "/v1/auth/mfa/verify",
        json={"mfa_token": mfa_token, "recovery_code": "AAAAA-AAAAA"},
      )
      assert replay.status_code == 401

  def test_recovery_code_lane(self, client, test_db):
    with patch.object(env, "PASSKEYS_ENABLED", True), _fake_auth_valkey():
      user, _passkey, mfa_token = self._login_to_challenge(client, test_db)
      codes = UserMfaRecoveryCode.create_set(str(user.id), test_db)

      verify_resp = client.post(
        "/v1/auth/mfa/verify",
        json={"mfa_token": mfa_token, "recovery_code": codes[0]},
      )
      assert verify_resp.status_code == 200
      assert verify_resp.json()["token"]
      assert UserMfaRecoveryCode.remaining_count(str(user.id), test_db) == 9

  def test_wrong_recovery_codes_exhaust_the_token(self, client, test_db):
    with patch.object(env, "PASSKEYS_ENABLED", True), _fake_auth_valkey():
      user, _passkey, mfa_token = self._login_to_challenge(client, test_db)
      codes = UserMfaRecoveryCode.create_set(str(user.id), test_db)

      for _ in range(5):
        resp = client.post(
          "/v1/auth/mfa/verify",
          json={"mfa_token": mfa_token, "recovery_code": "WRONG-WRONG"},
        )
        assert resp.status_code == 401

      # Budget spent: even the correct code is refused on this token.
      resp = client.post(
        "/v1/auth/mfa/verify",
        json={"mfa_token": mfa_token, "recovery_code": codes[0]},
      )
      assert resp.status_code == 401

  def test_session_version_bump_kills_the_flow(self, client, test_db):
    with patch.object(env, "PASSKEYS_ENABLED", True), _fake_auth_valkey():
      user, _passkey, mfa_token = self._login_to_challenge(client, test_db)
      user.invalidate_sessions(test_db)

      resp = client.post("/v1/auth/mfa/options", json={"mfa_token": mfa_token})
      assert resp.status_code == 401

  def test_enroll_token_refused_at_mfa_endpoints(self, client, test_db):
    with patch.object(env, "PASSKEYS_ENABLED", True), _fake_auth_valkey():
      from robosystems.middleware.auth.jwt import create_mfa_token

      user = _create_user(test_db)
      _enroll_passkey_row(test_db, user)
      enroll_token, _ = create_mfa_token(str(user.id), "enroll", session=test_db)

      resp = client.post("/v1/auth/mfa/options", json={"mfa_token": enroll_token})
      assert resp.status_code == 401

  def test_garbage_token_refused(self, client, test_db):
    with patch.object(env, "PASSKEYS_ENABLED", True), _fake_auth_valkey():
      resp = client.post("/v1/auth/mfa/options", json={"mfa_token": "not-a-jwt"})
      assert resp.status_code == 401

  def test_verify_requires_exactly_one_factor(self, client, test_db):
    with patch.object(env, "PASSKEYS_ENABLED", True), _fake_auth_valkey():
      _user, _passkey, mfa_token = self._login_to_challenge(client, test_db)
      resp = client.post("/v1/auth/mfa/verify", json={"mfa_token": mfa_token})
      assert resp.status_code == 422
      resp = client.post(
        "/v1/auth/mfa/verify",
        json={
          "mfa_token": mfa_token,
          "assertion": {"id": "x"},
          "recovery_code": "AAAAA-AAAAA",
        },
      )
      assert resp.status_code == 422

  def test_malformed_assertion_is_401_not_500(self, client, test_db):
    """clientDataJSON is attacker-controlled; malformation must hit the
    failure accounting, never crash."""
    with patch.object(env, "PASSKEYS_ENABLED", True), _fake_auth_valkey():
      _user, _passkey, mfa_token = self._login_to_challenge(client, test_db)
      resp = client.post(
        "/v1/auth/mfa/verify",
        json={
          "mfa_token": mfa_token,
          "assertion": {"id": "x", "response": {"clientDataJSON": "!!!"}},
        },
      )
      assert resp.status_code == 401


class TestMfaTokenNotABearer:
  def test_mfa_token_refused_as_session_bearer(self, client, test_db):
    with patch.object(env, "PASSKEYS_ENABLED", True):
      user = _create_user(test_db)
      _enroll_passkey_row(test_db, user)
      resp = _login(client, user)
      mfa_token = resp.json()["mfa_token"]

    me = client.get("/v1/auth/me", headers={"Authorization": f"Bearer {mfa_token}"})
    assert me.status_code == 401


class TestOidcLaneNeverChallenged:
  def test_sso_session_completion_ignores_passkeys(self, test_db):
    """The OIDC/bridge lane mints via sso_complete, which must stay
    MFA-blind: an IdP-governed user with a passkey enrolled here (or on
    another deployment) is the IdP's MFA problem, not ours."""
    import robosystems.routers.auth.sso as sso_module

    assert not hasattr(sso_module, "user_requires_mfa_enrollment")
    with open(sso_module.__file__) as f:
      source = f.read()
    assert "mfa" not in source.lower(), (
      "sso.py must not grow MFA logic — OIDC-minted sessions are "
      "MFA-satisfied by definition (the IdP owns MFA policy)"
    )
