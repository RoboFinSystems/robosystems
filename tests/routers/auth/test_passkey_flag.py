"""PASSKEYS_ENABLED must disable the whole passkey/MFA surface.

Same posture-drift regression shape as test_password_auth_flag.py: the flag
drives /auth/providers (what the UI renders) and require_passkeys_enabled
(what the API accepts). The routers mount unconditionally, so with the flag
off every route must exist and refuse with 403 — dark means refused, not
absent.
"""

from unittest.mock import patch

import pytest

from robosystems.config import env

# (method, path, request kwargs) for every passkey/MFA endpoint.
# Bodies are shape-valid so the guard is what answers, not model validation.
PASSKEY_ENDPOINTS = [
  (
    "post",
    "/v1/auth/passkeys/register/options",
    {"json": {}},
  ),
  (
    "post",
    "/v1/auth/passkeys/register/verify",
    {"json": {"credential": {"id": "x", "response": {}}}},
  ),
  (
    "get",
    "/v1/auth/passkeys",
    {},
  ),
  (
    "post",
    "/v1/auth/passkeys/reauth/options",
    {},
  ),
  (
    "delete",
    "/v1/auth/passkeys/upk_someid",
    {"json": {"password": "irrelevant-pw-1"}},
  ),
  (
    "post",
    "/v1/auth/passkeys/login/options",
    {},
  ),
  (
    "post",
    "/v1/auth/passkeys/login/verify",
    {"json": {"assertion": {"id": "x", "response": {}}}},
  ),
  (
    "post",
    "/v1/auth/mfa/options",
    {"json": {"mfa_token": "some-token"}},
  ),
  (
    "post",
    "/v1/auth/mfa/verify",
    {"json": {"mfa_token": "some-token", "recovery_code": "AAAAA-AAAAA"}},
  ),
  (
    "get",
    "/v1/auth/mfa/status",
    {},
  ),
  (
    "post",
    "/v1/auth/mfa/recovery-codes/regenerate",
    {"json": {"password": "irrelevant-pw-1"}},
  ),
]


class TestPasskeysDisabled:
  @pytest.mark.parametrize("method,path,kwargs", PASSKEY_ENDPOINTS)
  def test_endpoint_refuses_when_disabled(self, client, method, path, kwargs):
    # Default posture: PASSKEYS_ENABLED is false out of the box, but pin it
    # anyway so the table stays truthful if a future default flips.
    with patch.object(env, "PASSKEYS_ENABLED", False):
      # client.request rather than the method helpers: DELETE carries a
      # re-auth JSON body, which httpx's .delete() helper does not accept.
      resp = client.request(method.upper(), path, **kwargs)
    assert resp.status_code == 403, f"{path} returned {resp.status_code}"
    assert "disabled" in str(resp.json()["detail"]).lower()

  def test_login_path_untouched_when_disabled(self, client):
    """Flag off ⇒ login never emits an MFA discriminator (byte-identical
    contract: 401 for a bad credential, no mfa fields on the wire)."""
    with patch.object(env, "PASSKEYS_ENABLED", False):
      resp = client.post(
        "/v1/auth/login",
        json={"email": "nobody@example.com", "password": "irrelevant-pw-1"},
      )
    assert resp.status_code == 401
