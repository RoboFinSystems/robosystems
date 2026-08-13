"""Passkey lifecycle and passwordless-login router tests.

HTTP-level with the shared client fixture. WebAuthn ceremony verification is
mocked at the kernel's webauthn binding (the library cannot generate
authenticator responses); the dual-principal enrollment resolution, recovery
codes, re-auth proofs, and passwordless resolution all run real.
"""

import json
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import patch
from uuid import uuid4

from webauthn.helpers import base64url_to_bytes, bytes_to_base64url
from webauthn.helpers.structs import CredentialDeviceType

from robosystems.config import env
from robosystems.middleware.auth.jwt import create_jwt_token, create_mfa_token
from robosystems.models.core import (
  Org,
  OrgRole,
  OrgType,
  OrgUser,
  User,
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
def _passkeys_on():
  fake = _FakeRedis()
  with (
    patch.object(env, "PASSKEYS_ENABLED", True),
    patch.object(passkey_ops, "create_redis_client", return_value=fake),
    patch(
      "robosystems.routers.auth.mfa.create_redis_client",
      return_value=fake,
    ),
  ):
    yield fake


def _create_user(session):
  return User.create(
    email=f"pkr+{uuid4().hex[:8]}@customer.example",
    name="Passkey Router Tester",
    password_hash=PasswordSecurity.hash_password(PASSWORD),
    session=session,
  )


def _bearer(user):
  return {"Authorization": f"Bearer {create_jwt_token(str(user.id))}"}


def _credential_for(options: dict, credential_id: str | None = None):
  client_data = json.dumps(
    {"type": "webauthn.create", "challenge": options["challenge"]}
  ).encode()
  return {
    "id": credential_id or bytes_to_base64url(uuid4().bytes),
    "response": {"clientDataJSON": bytes_to_base64url(client_data)},
  }


def _verified_registration(credential_id_b64: str):
  return SimpleNamespace(
    credential_id=base64url_to_bytes(credential_id_b64),
    credential_public_key=b"cose-public-key",
    sign_count=0,
    aaguid="test-aaguid",
    credential_device_type=CredentialDeviceType.MULTI_DEVICE,
    credential_backed_up=True,
  )


def _enroll_via_api(client, user, name="My Key"):
  """Full settings-lane ceremony through the HTTP surface."""
  options_resp = client.post(
    "/v1/auth/passkeys/register/options", json={}, headers=_bearer(user)
  )
  assert options_resp.status_code == 200
  options = options_resp.json()["options"]
  credential = _credential_for(options)
  with patch.object(
    passkey_ops.webauthn,
    "verify_registration_response",
    return_value=_verified_registration(credential["id"]),
  ):
    verify_resp = client.post(
      "/v1/auth/passkeys/register/verify",
      json={"credential": credential, "name": name},
      headers=_bearer(user),
    )
  assert verify_resp.status_code == 200
  return verify_resp.json()


class TestSettingsEnrollment:
  def test_first_enrollment_returns_codes_no_auth(self, client, test_db):
    user = _create_user(test_db)
    with _passkeys_on():
      body = _enroll_via_api(client, user)
    assert body["passkey"]["name"] == "My Key"
    assert body["recovery_codes"] is not None
    assert len(body["recovery_codes"]) == 10
    # Settings lane: already authenticated, no login completion.
    assert body["auth"] is None

  def test_second_enrollment_no_codes(self, client, test_db):
    user = _create_user(test_db)
    with _passkeys_on():
      _enroll_via_api(client, user)
      body = _enroll_via_api(client, user, name="Second Key")
    assert body["recovery_codes"] is None

  def test_anonymous_enrollment_refused(self, client, test_db):
    with _passkeys_on():
      resp = client.post("/v1/auth/passkeys/register/options", json={})
    assert resp.status_code == 401

  def test_options_carry_rp_and_uv(self, client, test_db):
    user = _create_user(test_db)
    with _passkeys_on():
      resp = client.post(
        "/v1/auth/passkeys/register/options", json={}, headers=_bearer(user)
      )
    options = resp.json()["options"]
    assert options["rp"]["id"] == "localhost"
    assert options["authenticatorSelection"]["userVerification"] == "required"
    assert options["authenticatorSelection"]["residentKey"] == "required"


class TestForcedEnrollment:
  def test_enroll_token_completes_ceremony_and_login(self, client, test_db):
    user = _create_user(test_db)
    org = Org.create(name="Forced", org_type=OrgType.ENTERPRISE, session=test_db)
    OrgUser.create(str(org.id), str(user.id), OrgRole.OWNER, test_db)

    with _passkeys_on(), patch.object(env, "MFA_ENFORCEMENT_ENABLED", True):
      login_resp = client.post(
        "/v1/auth/login", json={"email": user.email, "password": PASSWORD}
      )
      assert login_resp.json()["status"] == "mfa_enrollment_required"
      enroll_token = login_resp.json()["mfa_token"]

      options_resp = client.post(
        "/v1/auth/passkeys/register/options", json={"mfa_token": enroll_token}
      )
      assert options_resp.status_code == 200
      credential = _credential_for(options_resp.json()["options"])

      with patch.object(
        passkey_ops.webauthn,
        "verify_registration_response",
        return_value=_verified_registration(credential["id"]),
      ):
        verify_resp = client.post(
          "/v1/auth/passkeys/register/verify",
          json={"credential": credential, "mfa_token": enroll_token},
        )
      assert verify_resp.status_code == 200
      body = verify_resp.json()
      # Ceremony completion IS login completion in the forced lane.
      assert body["auth"] is not None
      assert body["auth"]["token"]
      assert body["recovery_codes"] is not None

      # jti burned: the enroll token cannot start another ceremony.
      replay = client.post(
        "/v1/auth/passkeys/register/options", json={"mfa_token": enroll_token}
      )
      assert replay.status_code == 401

  def test_login_purpose_token_cannot_enroll(self, client, test_db):
    user = _create_user(test_db)
    with _passkeys_on():
      login_token, _ = create_mfa_token(str(user.id), "login", session=test_db)
      resp = client.post(
        "/v1/auth/passkeys/register/options", json={"mfa_token": login_token}
      )
    assert resp.status_code == 401


class TestLifecycle:
  def test_list_passkeys(self, client, test_db):
    user = _create_user(test_db)
    with _passkeys_on():
      _enroll_via_api(client, user)
      resp = client.get("/v1/auth/passkeys", headers=_bearer(user))
    assert resp.status_code == 200
    passkeys = resp.json()["passkeys"]
    assert len(passkeys) == 1
    assert passkeys[0]["backup_eligible"] is True

  def test_delete_with_password(self, client, test_db):
    user = _create_user(test_db)
    with _passkeys_on():
      body = _enroll_via_api(client, user)
      resp = client.request(
        "DELETE",
        f"/v1/auth/passkeys/{body['passkey']['id']}",
        json={"password": PASSWORD},
        headers=_bearer(user),
      )
    assert resp.status_code == 200
    assert UserPasskey.count_for_user(str(user.id), test_db) == 0

  def test_delete_wrong_password_refused(self, client, test_db):
    user = _create_user(test_db)
    with _passkeys_on():
      body = _enroll_via_api(client, user)
      resp = client.request(
        "DELETE",
        f"/v1/auth/passkeys/{body['passkey']['id']}",
        json={"password": "wrong-password-1"},
        headers=_bearer(user),
      )
    assert resp.status_code == 401
    assert UserPasskey.count_for_user(str(user.id), test_db) == 1

  def test_delete_last_passkey_of_enforced_owner_conflicts(self, client, test_db):
    user = _create_user(test_db)
    org = Org.create(name="KeepKey", org_type=OrgType.ENTERPRISE, session=test_db)
    OrgUser.create(str(org.id), str(user.id), OrgRole.OWNER, test_db)
    with _passkeys_on(), patch.object(env, "MFA_ENFORCEMENT_ENABLED", True):
      body = _enroll_via_api(client, user)
      resp = client.request(
        "DELETE",
        f"/v1/auth/passkeys/{body['passkey']['id']}",
        json={"password": PASSWORD},
        headers=_bearer(user),
      )
    assert resp.status_code == 409

  def test_delete_unknown_passkey_404(self, client, test_db):
    user = _create_user(test_db)
    with _passkeys_on():
      _enroll_via_api(client, user)
      resp = client.request(
        "DELETE",
        "/v1/auth/passkeys/upk_nonexistent",
        json={"password": PASSWORD},
        headers=_bearer(user),
      )
    assert resp.status_code == 404

  def test_delete_requires_exactly_one_proof(self, client, test_db):
    user = _create_user(test_db)
    with _passkeys_on():
      body = _enroll_via_api(client, user)
      resp = client.request(
        "DELETE",
        f"/v1/auth/passkeys/{body['passkey']['id']}",
        json={},
        headers=_bearer(user),
      )
    assert resp.status_code == 422

  def test_reauth_options_require_enrollment(self, client, test_db):
    user = _create_user(test_db)
    with _passkeys_on():
      resp = client.post("/v1/auth/passkeys/reauth/options", headers=_bearer(user))
      assert resp.status_code == 400
      _enroll_via_api(client, user)
      resp = client.post("/v1/auth/passkeys/reauth/options", headers=_bearer(user))
      assert resp.status_code == 200

  def test_mfa_status_and_recovery_regeneration(self, client, test_db):
    user = _create_user(test_db)
    with _passkeys_on():
      body = _enroll_via_api(client, user)
      old_codes = body["recovery_codes"]

      status_resp = client.get("/v1/auth/mfa/status", headers=_bearer(user))
      assert status_resp.status_code == 200
      assert status_resp.json()["passkey_count"] == 1
      assert status_resp.json()["recovery_codes_remaining"] == 10

      regen_resp = client.post(
        "/v1/auth/mfa/recovery-codes/regenerate",
        json={"password": PASSWORD},
        headers=_bearer(user),
      )
      assert regen_resp.status_code == 200
      new_codes = regen_resp.json()["codes"]
      assert len(new_codes) == 10
      assert set(new_codes).isdisjoint(set(old_codes))

  def test_recovery_regeneration_wrong_password_refused(self, client, test_db):
    user = _create_user(test_db)
    with _passkeys_on():
      _enroll_via_api(client, user)
      resp = client.post(
        "/v1/auth/mfa/recovery-codes/regenerate",
        json={"password": "wrong-password-1"},
        headers=_bearer(user),
      )
    assert resp.status_code == 401


class TestPasswordlessLogin:
  def _assertion_from_options(self, options: dict, credential_id: str, user_id: str):
    client_data = json.dumps(
      {"type": "webauthn.get", "challenge": options["challenge"]}
    ).encode()
    return {
      "id": credential_id,
      "response": {
        "clientDataJSON": bytes_to_base64url(client_data),
        "userHandle": bytes_to_base64url(user_id.encode()),
      },
    }

  def test_passwordless_login_mints_session(self, client, test_db):
    user = _create_user(test_db)
    with _passkeys_on():
      enrolled = _enroll_via_api(client, user)

      options_resp = client.post("/v1/auth/passkeys/login/options")
      assert options_resp.status_code == 200
      options = options_resp.json()["options"]
      # Usernameless: no credential hints for enumeration.
      assert not options.get("allowCredentials")

      assert enrolled["passkey"]["id"]
      assertion = self._assertion_from_options(
        options, self._credential_id(test_db, user), str(user.id)
      )
      with patch.object(
        passkey_ops.webauthn,
        "verify_authentication_response",
        return_value=SimpleNamespace(new_sign_count=1, credential_backed_up=True),
      ):
        verify_resp = client.post(
          "/v1/auth/passkeys/login/verify", json={"assertion": assertion}
        )
      assert verify_resp.status_code == 200
      body = verify_resp.json()
      assert body["status"] == "authenticated"
      assert body["token"]

  @staticmethod
  def _credential_id(session, user) -> str:
    passkey = UserPasskey.get_all_for_user(str(user.id), session)[0]
    return str(passkey.credential_id)

  def test_unknown_credential_refused(self, client, test_db):
    with _passkeys_on():
      options_resp = client.post("/v1/auth/passkeys/login/options")
      options = options_resp.json()["options"]
      client_data = json.dumps(
        {"type": "webauthn.get", "challenge": options["challenge"]}
      ).encode()
      assertion = {
        "id": bytes_to_base64url(uuid4().bytes),
        "response": {"clientDataJSON": bytes_to_base64url(client_data)},
      }
      resp = client.post(
        "/v1/auth/passkeys/login/verify", json={"assertion": assertion}
      )
    assert resp.status_code == 401

  def test_replayed_challenge_refused(self, client, test_db):
    user = _create_user(test_db)
    with _passkeys_on():
      _enroll_via_api(client, user)
      options_resp = client.post("/v1/auth/passkeys/login/options")
      options = options_resp.json()["options"]
      assertion = self._assertion_from_options(
        options, self._credential_id(test_db, user), str(user.id)
      )
      with patch.object(
        passkey_ops.webauthn,
        "verify_authentication_response",
        return_value=SimpleNamespace(new_sign_count=1, credential_backed_up=True),
      ):
        first = client.post(
          "/v1/auth/passkeys/login/verify", json={"assertion": assertion}
        )
        replay = client.post(
          "/v1/auth/passkeys/login/verify", json={"assertion": assertion}
        )
      assert first.status_code == 200
      assert replay.status_code == 401
