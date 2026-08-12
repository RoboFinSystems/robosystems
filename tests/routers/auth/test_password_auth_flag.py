"""PASSWORD_AUTH_ENABLED must disable the whole password-credential surface.

Regression suite for posture drift: the flag drives /auth/providers (what the
UI renders) and these guards (what the API accepts) — if the two ever diverge,
a hidden-but-functional password login would bypass the IdP's MFA and
conditional-access policies on an SSO-primary deployment.
"""

from unittest.mock import patch

import pytest

from robosystems.config import env

# (method, path, request kwargs) for every password-credential endpoint.
# Bodies are shape-valid so the guard is what answers, not model validation.
PASSWORD_ENDPOINTS = [
  (
    "post",
    "/v1/auth/login",
    {"json": {"email": "someone@example.com", "password": "irrelevant-pw-1"}},
  ),
  (
    "post",
    "/v1/auth/register",
    {
      "json": {
        "name": "Someone",
        "email": "someone@example.com",
        "password": "irrelevant-pw-1",
      }
    },
  ),
  (
    "post",
    "/v1/auth/password/forgot",
    {"json": {"email": "someone@example.com"}},
  ),
  (
    "get",
    "/v1/auth/password/reset/validate",
    {"params": {"token": "some-reset-token"}},
  ),
  (
    "post",
    "/v1/auth/password/reset",
    {"json": {"token": "some-reset-token", "password": "irrelevant-pw-1"}},
  ),
]


class TestPasswordAuthDisabled:
  @pytest.mark.parametrize("method,path,kwargs", PASSWORD_ENDPOINTS)
  def test_endpoint_refuses_when_disabled(self, client, method, path, kwargs):
    with patch.object(env, "PASSWORD_AUTH_ENABLED", False):
      resp = getattr(client, method)(path, **kwargs)
    assert resp.status_code == 403
    assert "disabled" in str(resp.json()["detail"]).lower()

  def test_password_change_refuses_when_disabled(self, client, test_user):
    from robosystems.middleware.auth.jwt import create_jwt_token

    jwt = create_jwt_token(test_user.id)
    with patch.object(env, "PASSWORD_AUTH_ENABLED", False):
      resp = client.put(
        "/v1/user/password",
        headers={"Authorization": f"Bearer {jwt}"},
        json={
          "current_password": "irrelevant-pw-1",
          "new_password": "irrelevant-pw-2",
          "confirm_password": "irrelevant-pw-2",
        },
      )
    assert resp.status_code == 403

  def test_invitation_does_not_bypass_the_guard(self, client):
    """An invite token must not become a side door to a password account."""
    with patch.object(env, "PASSWORD_AUTH_ENABLED", False):
      resp = client.post(
        "/v1/auth/register",
        json={
          "name": "Invited",
          "email": "invited@example.com",
          "password": "irrelevant-pw-1",
          "invite_token": "some-invite-token",
        },
      )
    assert resp.status_code == 403


class TestPasswordAuthDefaultOpen:
  def test_forgot_password_open_by_default(self, client):
    resp = client.post("/v1/auth/password/forgot", json={"email": "nobody@example.com"})
    assert resp.status_code == 200
