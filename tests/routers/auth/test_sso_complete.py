"""
Tests for SSO completion endpoint.
"""

import json
from unittest.mock import AsyncMock, Mock, patch

import pytest
from fastapi import HTTPException, status

from robosystems.models.api.auth import SSOCompleteRequest
from robosystems.models.core import User
from robosystems.routers.auth.sso import sso_complete

SESSION_ID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"

SESSION_DATA = {
  "user_id": "user_123",
  "token_id": "token_id_123",
  "target_app": "roboledger",
  "return_url": "/reports/x",
  "session_version": 0,
  "created_at": "2026-08-11T00:00:00+00:00",
}


class _AsyncCM:
  async def __aenter__(self):
    return None

  async def __aexit__(self, *args):
    return False


@pytest.fixture
def mock_user():
  user = Mock(spec=User)
  user.id = "user_123"
  user.name = "Test User"
  user.email = "test@example.com"
  user.email_verified = True
  user.is_active = True
  user.session_version = 0
  return user


def _call_args():
  mock_request = Mock()
  mock_request.headers = {}
  return {
    "request": mock_request,
    "body": SSOCompleteRequest(session_id=SESSION_ID),
    "response": Mock(),
    "session": Mock(),
    "_rate_limit": None,
  }


class TestSSOComplete:
  """Test the /sso-complete endpoint."""

  @patch("robosystems.routers.auth.sso.create_jwt_token")
  @patch("robosystems.routers.auth.sso.extract_device_fingerprint")
  @patch("robosystems.routers.auth.sso.User.get_by_id")
  @patch("robosystems.routers.auth.sso.get_async_redis_client")
  @patch("robosystems.routers.auth.sso.get_sso_lock_manager")
  async def test_complete_success_locked(
    self,
    mock_lock_manager,
    mock_redis_client,
    mock_get_by_id,
    mock_fingerprint,
    mock_create_jwt,
    mock_user,
  ):
    lock = Mock()
    lock.lock_sso_session.return_value = _AsyncCM()
    mock_lock_manager.return_value = lock

    redis_instance = AsyncMock()
    redis_instance.get.return_value = json.dumps(SESSION_DATA)
    mock_redis_client.return_value = redis_instance
    mock_get_by_id.return_value = mock_user
    mock_fingerprint.return_value = "fp"
    mock_create_jwt.return_value = "new_jwt_token"

    result = await sso_complete(**_call_args())

    assert result.token == "new_jwt_token"
    assert result.user["id"] == "user_123"

    deleted_keys = {call.args[0] for call in redis_instance.delete.call_args_list}
    assert deleted_keys == {
      f"sso_session:{SESSION_ID}",
      "sso_token:token_id_123",
      "sso_token_exchange:token_id_123",
    }

  @patch("robosystems.routers.auth.sso.create_jwt_token")
  @patch("robosystems.routers.auth.sso.extract_device_fingerprint")
  @patch("robosystems.routers.auth.sso.User.get_by_id")
  @patch("robosystems.routers.auth.sso.get_async_redis_client")
  @patch("robosystems.routers.auth.sso.get_sso_lock_manager")
  async def test_complete_success_fallback_str_payload(
    self,
    mock_lock_manager,
    mock_redis_client,
    mock_get_by_id,
    mock_fingerprint,
    mock_create_jwt,
    mock_user,
  ):
    """Regression: the client decodes responses, so the fallback path receives
    str payloads and must not call .decode() on them."""
    mock_lock_manager.return_value = None

    redis_instance = AsyncMock()
    redis_instance.get.return_value = json.dumps(SESSION_DATA)
    mock_redis_client.return_value = redis_instance
    mock_get_by_id.return_value = mock_user
    mock_fingerprint.return_value = "fp"
    mock_create_jwt.return_value = "new_jwt_token"

    result = await sso_complete(**_call_args())

    assert result.token == "new_jwt_token"
    assert result.message == "SSO authentication completed successfully"

  @patch("robosystems.routers.auth.sso.User.get_by_id")
  @patch("robosystems.routers.auth.sso.get_async_redis_client")
  @patch("robosystems.routers.auth.sso.get_sso_lock_manager")
  async def test_complete_session_version_mismatch(
    self,
    mock_lock_manager,
    mock_redis_client,
    mock_get_by_id,
    mock_user,
  ):
    """A handoff whose session was invalidated between exchange and
    completion must not mint a JWT."""
    mock_lock_manager.return_value = None

    redis_instance = AsyncMock()
    redis_instance.get.return_value = json.dumps(SESSION_DATA)
    mock_redis_client.return_value = redis_instance
    mock_user.session_version = 1
    mock_get_by_id.return_value = mock_user

    with pytest.raises(HTTPException) as exc_info:
      await sso_complete(**_call_args())

    assert exc_info.value.status_code == status.HTTP_401_UNAUTHORIZED
    assert exc_info.value.detail == "Session has been invalidated"

  @patch("robosystems.routers.auth.sso.get_async_redis_client")
  @patch("robosystems.routers.auth.sso.get_sso_lock_manager")
  async def test_complete_missing_session(
    self,
    mock_lock_manager,
    mock_redis_client,
  ):
    mock_lock_manager.return_value = None

    redis_instance = AsyncMock()
    redis_instance.get.return_value = None
    mock_redis_client.return_value = redis_instance

    with pytest.raises(HTTPException) as exc_info:
      await sso_complete(**_call_args())

    assert exc_info.value.status_code == status.HTTP_401_UNAUTHORIZED
    assert exc_info.value.detail == "Session expired or invalid"

  @patch("robosystems.routers.auth.sso.get_async_redis_client")
  @patch("robosystems.routers.auth.sso.get_sso_lock_manager")
  async def test_complete_invalid_session_data(
    self,
    mock_lock_manager,
    mock_redis_client,
  ):
    mock_lock_manager.return_value = None

    redis_instance = AsyncMock()
    redis_instance.get.return_value = json.dumps({"target_app": "roboledger"})
    mock_redis_client.return_value = redis_instance

    with pytest.raises(HTTPException) as exc_info:
      await sso_complete(**_call_args())

    assert exc_info.value.status_code == status.HTTP_401_UNAUTHORIZED
    assert exc_info.value.detail == "Invalid session data"

  @patch("robosystems.routers.auth.sso.User.get_by_id")
  @patch("robosystems.routers.auth.sso.get_async_redis_client")
  @patch("robosystems.routers.auth.sso.get_sso_lock_manager")
  async def test_complete_inactive_user(
    self,
    mock_lock_manager,
    mock_redis_client,
    mock_get_by_id,
    mock_user,
  ):
    mock_lock_manager.return_value = None

    redis_instance = AsyncMock()
    redis_instance.get.return_value = json.dumps(SESSION_DATA)
    mock_redis_client.return_value = redis_instance
    mock_user.is_active = False
    mock_get_by_id.return_value = mock_user

    with pytest.raises(HTTPException) as exc_info:
      await sso_complete(**_call_args())

    assert exc_info.value.status_code == status.HTTP_401_UNAUTHORIZED
    assert exc_info.value.detail == "User not found or inactive"
