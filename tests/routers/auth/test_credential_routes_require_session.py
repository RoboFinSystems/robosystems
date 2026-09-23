"""Credential-lifecycle routes accept an interactive session only, never an API key."""

from unittest.mock import Mock

import pytest

from robosystems.middleware.auth.dependencies import get_current_user
from robosystems.models.core import User


@pytest.fixture
def api_key_client(client):
  from main import app

  user = Mock(spec=User)
  user.id = "user_key_holder"
  app.dependency_overrides[get_current_user] = lambda: user
  try:
    yield client
  finally:
    app.dependency_overrides.pop(get_current_user, None)


@pytest.mark.parametrize(
  ("method", "path", "body"),
  [
    ("post", "/v1/auth/mfa/recovery-codes/regenerate", {"password": "x"}),
    ("post", "/v1/auth/passkeys/reauth/options", None),
    ("delete", "/v1/auth/passkeys/pk_1", {"password": "x"}),
    (
      "put",
      "/v1/user/password",
      {
        "current_password": "x",
        "new_password": "Tr0pic@lBreeze#99",
        "confirm_password": "Tr0pic@lBreeze#99",
      },
    ),
  ],
)
def test_api_key_caller_is_refused(api_key_client, monkeypatch, method, path, body):
  from robosystems.config import env

  monkeypatch.setattr(env, "PASSKEYS_ENABLED", True)
  response = api_key_client.request(
    method, path, json=body, headers={"X-API-Key": "rfs_not_a_session"}
  )
  assert response.status_code == 401
