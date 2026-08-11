"""
Tests for SSO token exchange endpoint.
"""

import json
from unittest.mock import AsyncMock, Mock, patch

import pytest
from fastapi import HTTPException, status

from robosystems.models.api.auth import SSOExchangeRequest
from robosystems.models.core import User
from robosystems.routers.auth.sso import sso_token_exchange
from robosystems.routers.auth.utils import SSO_TOKEN_EXPIRY_SECONDS

APP_URLS = {
  "roboledger": "https://roboledger.ai",
  "roboinvestor": "https://roboinvestor.ai",
  "robosystems": "https://robosystems.ai",
}

TOKEN_PAYLOAD = {
  "user_id": "user_123",
  "sso": True,
  "token_id": "token_id_123",
  "session_version": 0,
}


@pytest.fixture
def mock_user():
  user = Mock(spec=User)
  user.id = "user_123"
  user.is_active = True
  user.session_version = 0
  return user


def _redis_with_token():
  """AsyncMock redis holding the SSO token and no exchange marker."""
  redis_instance = AsyncMock()

  def _get(key):
    if key == "sso_token:token_id_123":
      return "user_123"
    return None

  redis_instance.get.side_effect = _get
  redis_instance.setex.return_value = True
  return redis_instance


class TestSSOTokenExchange:
  """Test the /sso-exchange endpoint."""

  async def test_invalid_target_app(self):
    request = SSOExchangeRequest(token="some_token", target_app="evilapp")

    with pytest.raises(HTTPException) as exc_info:
      await sso_token_exchange(request=request, session=Mock(), _rate_limit=None)

    assert exc_info.value.status_code == status.HTTP_400_BAD_REQUEST
    assert exc_info.value.detail == "Invalid target application"

  @pytest.mark.parametrize(
    "return_url",
    [
      "https://evil.com/phish",
      "//evil.com/phish",
      "/\\evil.com",
      "javascript:alert(1)",
      "relative-no-slash",
    ],
  )
  async def test_unsafe_return_url_rejected(self, return_url):
    request = SSOExchangeRequest(
      token="some_token", target_app="roboledger", return_url=return_url
    )

    with pytest.raises(HTTPException) as exc_info:
      await sso_token_exchange(request=request, session=Mock(), _rate_limit=None)

    assert exc_info.value.status_code == status.HTTP_400_BAD_REQUEST
    assert exc_info.value.detail == "Invalid return URL"

  @patch("robosystems.routers.auth.sso.Config.get_app_urls")
  @patch("robosystems.routers.auth.sso.Config.get_jwt_secret")
  @patch("robosystems.routers.auth.sso.jwt.decode")
  @patch("robosystems.routers.auth.sso.User.get_by_id")
  @patch("robosystems.routers.auth.sso.get_async_redis_client")
  @patch("robosystems.routers.auth.sso.get_sso_lock_manager")
  async def test_exchange_success(
    self,
    mock_lock_manager,
    mock_redis_client,
    mock_get_by_id,
    mock_jwt_decode,
    mock_get_secret,
    mock_get_app_urls,
    mock_user,
  ):
    mock_lock_manager.return_value = None
    redis_instance = _redis_with_token()
    mock_redis_client.return_value = redis_instance
    mock_get_by_id.return_value = mock_user
    mock_jwt_decode.return_value = dict(TOKEN_PAYLOAD)
    mock_get_secret.return_value = "secret"
    mock_get_app_urls.return_value = APP_URLS

    request = SSOExchangeRequest(
      token="sso_jwt", target_app="roboledger", return_url="/reports/x"
    )
    result = await sso_token_exchange(request=request, session=Mock(), _rate_limit=None)

    assert result.session_id
    assert result.redirect_url == "https://roboledger.ai/login"

    setex_calls = {call.args[0]: call for call in redis_instance.setex.call_args_list}

    # Replay marker must live as long as the token itself
    marker_call = setex_calls["sso_token_exchange:token_id_123"]
    assert marker_call.args[1] == SSO_TOKEN_EXPIRY_SECONDS

    # Session payload carries the version for re-validation at completion
    session_key = f"sso_session:{result.session_id}"
    session_payload = json.loads(setex_calls[session_key].args[2])
    assert session_payload["session_version"] == 0
    assert session_payload["return_url"] == "/reports/x"
    assert session_payload["target_app"] == "roboledger"

  @patch("robosystems.routers.auth.sso.Config.get_jwt_secret")
  @patch("robosystems.routers.auth.sso.jwt.decode")
  @patch("robosystems.routers.auth.sso.User.get_by_id")
  @patch("robosystems.routers.auth.sso.get_async_redis_client")
  @patch("robosystems.routers.auth.sso.get_sso_lock_manager")
  async def test_exchange_session_version_mismatch(
    self,
    mock_lock_manager,
    mock_redis_client,
    mock_get_by_id,
    mock_jwt_decode,
    mock_get_secret,
    mock_user,
  ):
    mock_lock_manager.return_value = None
    mock_redis_client.return_value = _redis_with_token()
    mock_user.session_version = 5
    mock_get_by_id.return_value = mock_user
    mock_jwt_decode.return_value = dict(TOKEN_PAYLOAD)
    mock_get_secret.return_value = "secret"

    request = SSOExchangeRequest(token="sso_jwt", target_app="roboledger")

    with pytest.raises(HTTPException) as exc_info:
      await sso_token_exchange(request=request, session=Mock(), _rate_limit=None)

    assert exc_info.value.status_code == status.HTTP_401_UNAUTHORIZED
    assert exc_info.value.detail == "SSO token session has been invalidated"

  @patch("robosystems.routers.auth.sso.Config.get_jwt_secret")
  @patch("robosystems.routers.auth.sso.jwt.decode")
  @patch("robosystems.routers.auth.sso.get_async_redis_client")
  @patch("robosystems.routers.auth.sso.get_sso_lock_manager")
  async def test_exchange_token_not_found(
    self,
    mock_lock_manager,
    mock_redis_client,
    mock_jwt_decode,
    mock_get_secret,
  ):
    mock_lock_manager.return_value = None
    redis_instance = AsyncMock()
    redis_instance.get.return_value = None
    mock_redis_client.return_value = redis_instance
    mock_jwt_decode.return_value = dict(TOKEN_PAYLOAD)
    mock_get_secret.return_value = "secret"

    request = SSOExchangeRequest(token="sso_jwt", target_app="roboledger")

    with pytest.raises(HTTPException) as exc_info:
      await sso_token_exchange(request=request, session=Mock(), _rate_limit=None)

    assert exc_info.value.status_code == status.HTTP_401_UNAUTHORIZED
    assert exc_info.value.detail == "SSO token expired or already used"
