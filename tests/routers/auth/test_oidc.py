"""Tests for the OIDC login/callback endpoints.

Same style as the sso-bridge tests: direct coroutine calls with the
collaborators patched where the router binds them. The final class is the
producer→consumer seam test — the payload the callback writes must be
accepted by ``sso_complete`` verbatim, synthetic ``token_id`` and all.
"""

import json
from unittest.mock import AsyncMock, Mock, patch
from urllib.parse import parse_qs, urlparse

import pytest
from fastapi import HTTPException, status

from robosystems.models.api.auth import SSOCompleteRequest
from robosystems.models.core import User
from robosystems.operations.oidc import (
  OIDCConnection,
  OIDCResolution,
  OIDCState,
  OIDCTokenInvalidError,
  OIDCUserInactiveError,
  OIDCUserNotProvisionedError,
)
from robosystems.routers.auth.oidc import oidc_callback, oidc_login
from robosystems.routers.auth.sso import sso_complete

ISSUER = "https://test-org.okta.example"
CONNECTION = OIDCConnection(
  issuer=ISSUER,
  client_id="client-abc-123",
  client_secret="secret",
  redirect_uri="http://localhost:8000/v1/auth/oidc/callback",
)
METADATA = {
  "issuer": ISSUER,
  "authorization_endpoint": f"{ISSUER}/oauth2/v1/authorize",
  "token_endpoint": f"{ISSUER}/oauth2/v1/token",
  "jwks_uri": f"{ISSUER}/oauth2/v1/keys",
}
APP_URLS = {
  "roboledger": "https://ledger.example",
  "roboinvestor": "https://investor.example",
  "robosystems": "https://app.example",
}
BROWSER_STATE = "the-browser-binding"
STATE_DATA = {
  "nonce": "expected-nonce",
  "code_verifier": "the-verifier",
  "return_to": "roboledger:/reports/x",
  "browser_state": BROWSER_STATE,
}
CLAIMS = {
  "sub": "okta|user-1",
  "email": "staff@customer.example",
  "email_verified": True,
}


def _mock_request(cookies=None):
  request = Mock()
  request.client = Mock()
  request.client.host = "203.0.113.9"
  request.headers = {"user-agent": "pytest"}
  request.cookies = cookies if cookies is not None else {"oidc_flow": BROWSER_STATE}
  return request


@pytest.fixture
def mock_user():
  user = Mock(spec=User)
  user.id = "user_oidc_123"
  user.name = "IdP Staffer"
  user.email = "staff@customer.example"
  user.email_verified = True
  user.is_active = True
  user.session_version = 3
  return user


class TestOidcLogin:
  @patch("robosystems.routers.auth.oidc.Config.get_app_urls", return_value=APP_URLS)
  @patch.object(OIDCState, "create", return_value="fixed-state-token")
  @patch(
    "robosystems.routers.auth.oidc.fetch_server_metadata",
    new_callable=AsyncMock,
    return_value=METADATA,
  )
  @patch("robosystems.routers.auth.oidc.get_oidc_connection", return_value=CONNECTION)
  async def test_redirects_to_idp_with_code_flow_params(
    self, _conn, _metadata, _state, _urls
  ):
    response = await oidc_login(
      request=_mock_request(),
      iss=None,
      return_to="roboledger:/reports/x",
      _rate_limit=None,
    )

    assert response.status_code == 302
    location = response.headers["location"]
    parsed = urlparse(location)
    assert location.startswith(METADATA["authorization_endpoint"])
    params = parse_qs(parsed.query)
    assert params["response_type"] == ["code"]
    assert params["client_id"] == [CONNECTION.client_id]
    assert params["redirect_uri"] == [CONNECTION.redirect_uri]
    assert params["state"] == ["fixed-state-token"]
    assert params["code_challenge_method"] == ["S256"]
    assert params["code_challenge"]
    assert params["nonce"]
    assert "openid" in params["scope"][0]

    # The browser-binding cookie is set, HttpOnly, path-scoped to the callback.
    set_cookie = response.headers["set-cookie"]
    assert set_cookie.startswith("oidc_flow=")
    assert "HttpOnly" in set_cookie
    assert "Path=/v1/auth/oidc/callback" in set_cookie
    assert "SameSite=lax" in set_cookie

  @patch("robosystems.routers.auth.oidc.Config.get_app_urls", return_value=APP_URLS)
  @patch.object(OIDCState, "create", return_value="fixed-state-token")
  @patch(
    "robosystems.routers.auth.oidc.fetch_server_metadata",
    new_callable=AsyncMock,
    return_value=METADATA,
  )
  @patch("robosystems.routers.auth.oidc.get_oidc_connection", return_value=CONNECTION)
  async def test_matching_iss_is_accepted(self, _conn, _metadata, _state, _urls):
    """The Okta app tile sends iss; a matching one (modulo trailing slash)
    starts a normal flow."""
    response = await oidc_login(
      request=_mock_request(), iss=f"{ISSUER}/", return_to=None, _rate_limit=None
    )
    assert response.status_code == 302
    assert response.headers["location"].startswith(METADATA["authorization_endpoint"])

  @patch("robosystems.routers.auth.oidc.SecurityAuditLogger")
  @patch("robosystems.routers.auth.oidc.Config.get_app_urls", return_value=APP_URLS)
  @patch("robosystems.routers.auth.oidc.get_oidc_connection", return_value=CONNECTION)
  async def test_foreign_iss_is_refused(self, _conn, _urls, mock_audit):
    """An attacker-supplied iss must be refused, never followed."""
    response = await oidc_login(
      request=_mock_request(),
      iss="https://evil.example",
      return_to=None,
      _rate_limit=None,
    )

    assert response.status_code == 302
    assert (
      response.headers["location"] == "https://app.example/login?reason=oidc_failed"
    )
    details = mock_audit.log_security_event.call_args.kwargs["details"]
    assert details["failure_reason"] == "oidc_iss_mismatch"


def _callback_patches():
  """The full happy-path patch stack; tests mutate pieces as needed."""
  return {
    "conn": patch(
      "robosystems.routers.auth.oidc.get_oidc_connection", return_value=CONNECTION
    ),
    "metadata": patch(
      "robosystems.routers.auth.oidc.fetch_server_metadata",
      new_callable=AsyncMock,
      return_value=METADATA,
    ),
    "urls": patch(
      "robosystems.routers.auth.oidc.Config.get_app_urls", return_value=APP_URLS
    ),
    "state": patch.object(OIDCState, "validate", return_value=dict(STATE_DATA)),
    "exchange": patch(
      "robosystems.routers.auth.oidc.exchange_code",
      new_callable=AsyncMock,
      return_value="raw-id-token",
    ),
    "validate": patch(
      "robosystems.routers.auth.oidc.validate_id_token",
      new_callable=AsyncMock,
      return_value=dict(CLAIMS),
    ),
  }


def _callback_args(**overrides):
  args = {
    "request": _mock_request(),
    "code": "auth-code",
    "state": "state-token",
    "error": None,
    "session": Mock(),
    "_rate_limit": None,
  }
  args.update(overrides)
  return args


class TestOidcCallback:
  async def test_happy_path_writes_bridge_session_and_redirects(self, mock_user):
    patches = _callback_patches()
    redis_instance = AsyncMock()
    with (
      patches["conn"],
      patches["metadata"],
      patches["urls"],
      patches["state"],
      patches["exchange"],
      patches["validate"],
      patch(
        "robosystems.routers.auth.oidc.resolve_oidc_user",
        return_value=OIDCResolution(user=mock_user, linked=False),
      ),
      patch(
        "robosystems.routers.auth.oidc.get_async_redis_client",
        new_callable=AsyncMock,
        return_value=redis_instance,
      ),
    ):
      response = await oidc_callback(**_callback_args())

    assert response.status_code == 302
    location = response.headers["location"]
    assert location.startswith("https://app.example/login?session_id=")
    assert "return_to=roboledger%3A/reports/x" in location

    redis_instance.setex.assert_called_once()
    key, ttl, raw_payload = redis_instance.setex.call_args.args
    assert key.startswith("sso_session:")
    assert ttl == 30
    payload = json.loads(raw_payload)
    assert payload["user_id"] == mock_user.id
    assert payload["token_id"]  # synthetic, but present — sso_complete requires it
    assert payload["session_version"] == mock_user.session_version
    assert payload["target_app"] == "robosystems"

  async def test_idp_error_redirects_failed(self):
    patches = _callback_patches()
    with patches["conn"], patches["metadata"], patches["urls"]:
      response = await oidc_callback(**_callback_args(error="access_denied"))
    assert (
      response.headers["location"] == "https://app.example/login?reason=oidc_failed"
    )

  async def test_missing_state_redirects_failed(self):
    patches = _callback_patches()
    with patches["conn"], patches["metadata"], patches["urls"]:
      response = await oidc_callback(**_callback_args(state=None))
    assert (
      response.headers["location"] == "https://app.example/login?reason=oidc_failed"
    )

  async def test_replayed_state_redirects_failed(self):
    patches = _callback_patches()
    with (
      patches["conn"],
      patches["metadata"],
      patches["urls"],
      patch.object(OIDCState, "validate", return_value=None),
    ):
      response = await oidc_callback(**_callback_args())
    assert (
      response.headers["location"] == "https://app.example/login?reason=oidc_failed"
    )

  async def test_missing_browser_binding_cookie_redirects_failed(self):
    """Login-CSRF defense: a browser without the flow cookie (a victim handed
    an attacker's captured callback URL) is refused even with a valid state."""
    patches = _callback_patches()
    with patches["conn"], patches["metadata"], patches["urls"], patches["state"]:
      response = await oidc_callback(
        **_callback_args(request=_mock_request(cookies={}))
      )
    assert (
      response.headers["location"] == "https://app.example/login?reason=oidc_failed"
    )

  async def test_wrong_browser_binding_cookie_redirects_failed(self):
    patches = _callback_patches()
    with patches["conn"], patches["metadata"], patches["urls"], patches["state"]:
      response = await oidc_callback(
        **_callback_args(request=_mock_request(cookies={"oidc_flow": "nope"}))
      )
    assert (
      response.headers["location"] == "https://app.example/login?reason=oidc_failed"
    )

  async def test_invalid_token_redirects_failed(self):
    patches = _callback_patches()
    with (
      patches["conn"],
      patches["metadata"],
      patches["urls"],
      patches["state"],
      patches["exchange"],
      patch(
        "robosystems.routers.auth.oidc.validate_id_token",
        new_callable=AsyncMock,
        side_effect=OIDCTokenInvalidError("nonce mismatch"),
      ),
    ):
      response = await oidc_callback(**_callback_args())
    assert (
      response.headers["location"] == "https://app.example/login?reason=oidc_failed"
    )

  @pytest.mark.parametrize(
    "resolution_error", [OIDCUserNotProvisionedError, OIDCUserInactiveError]
  )
  async def test_unprovisioned_or_inactive_user_denied(self, resolution_error):
    """The IdP authenticated, but the platform must still refuse — audited as
    OIDC_LOGIN_DENIED for the not-provisioned reason code."""
    patches = _callback_patches()
    with (
      patches["conn"],
      patches["metadata"],
      patches["urls"],
      patches["state"],
      patches["exchange"],
      patches["validate"],
      patch(
        "robosystems.routers.auth.oidc.resolve_oidc_user",
        side_effect=resolution_error("refused"),
      ),
      patch("robosystems.routers.auth.oidc.SecurityAuditLogger") as mock_audit,
    ):
      response = await oidc_callback(**_callback_args())

    assert (
      response.headers["location"] == "https://app.example/login?reason=not_provisioned"
    )
    event_type = mock_audit.log_security_event.call_args.kwargs["event_type"]
    assert event_type.value == "oidc_login_denied"

  async def test_bridge_store_down_redirects_failed(self, mock_user):
    patches = _callback_patches()
    redis_instance = AsyncMock()
    redis_instance.setex.side_effect = ConnectionError("valkey down")
    with (
      patches["conn"],
      patches["metadata"],
      patches["urls"],
      patches["state"],
      patches["exchange"],
      patches["validate"],
      patch(
        "robosystems.routers.auth.oidc.resolve_oidc_user",
        return_value=OIDCResolution(user=mock_user, linked=False),
      ),
      patch(
        "robosystems.routers.auth.oidc.get_async_redis_client",
        new_callable=AsyncMock,
        return_value=redis_instance,
      ),
    ):
      response = await oidc_callback(**_callback_args())
    assert (
      response.headers["location"] == "https://app.example/login?reason=oidc_failed"
    )


class _AsyncCM:
  async def __aenter__(self):
    return None

  async def __aexit__(self, *args):
    return False


class TestCallbackToSsoCompleteSeam:
  """Producer→consumer: sso_complete must accept the exact payload the OIDC
  callback writes. sso_complete hard-requires token_id (sso.py's deletes),
  which an OIDC login doesn't naturally have — the synthetic token_id exists
  to satisfy that check, and this test is what keeps the two ends honest."""

  async def _payload_written_by_callback(self, mock_user):
    patches = _callback_patches()
    redis_instance = AsyncMock()
    with (
      patches["conn"],
      patches["metadata"],
      patches["urls"],
      patches["state"],
      patches["exchange"],
      patches["validate"],
      patch(
        "robosystems.routers.auth.oidc.resolve_oidc_user",
        return_value=OIDCResolution(user=mock_user, linked=True),
      ),
      patch(
        "robosystems.routers.auth.oidc.get_async_redis_client",
        new_callable=AsyncMock,
        return_value=redis_instance,
      ),
    ):
      response = await oidc_callback(**_callback_args())

    location = response.headers["location"]
    session_id = parse_qs(urlparse(location).query)["session_id"][0]
    _key, _ttl, raw_payload = redis_instance.setex.call_args.args
    return session_id, raw_payload

  @patch("robosystems.routers.auth.sso.create_jwt_token")
  @patch("robosystems.routers.auth.sso.extract_device_fingerprint")
  @patch("robosystems.routers.auth.sso.User.get_by_id")
  @patch("robosystems.routers.auth.sso.get_async_redis_client")
  @patch("robosystems.routers.auth.sso.get_sso_lock_manager")
  async def test_sso_complete_accepts_oidc_payload(
    self,
    mock_lock_manager,
    mock_redis_client,
    mock_get_by_id,
    mock_fingerprint,
    mock_create_jwt,
    mock_user,
  ):
    session_id, raw_payload = await self._payload_written_by_callback(mock_user)

    lock = Mock()
    lock.lock_sso_session.return_value = _AsyncCM()
    mock_lock_manager.return_value = lock

    redis_instance = AsyncMock()
    redis_instance.get.return_value = raw_payload
    mock_redis_client.return_value = redis_instance
    mock_get_by_id.return_value = mock_user
    mock_fingerprint.return_value = "fp"
    mock_create_jwt.return_value = "minted-jwt"

    request = Mock()
    request.headers = {}
    result = await sso_complete(
      request=request,
      body=SSOCompleteRequest(session_id=session_id),
      response=Mock(),
      session=Mock(),
      _rate_limit=None,
    )

    assert result.token == "minted-jwt"
    assert result.user["id"] == mock_user.id

  @patch("robosystems.routers.auth.sso.User.get_by_id")
  @patch("robosystems.routers.auth.sso.get_async_redis_client")
  @patch("robosystems.routers.auth.sso.get_sso_lock_manager")
  async def test_stale_session_version_still_fails_closed(
    self,
    mock_lock_manager,
    mock_redis_client,
    mock_get_by_id,
    mock_user,
  ):
    """An OIDC handoff written before a deactivation bump must die at
    completion — the offboarding guarantee spans this producer too."""
    session_id, raw_payload = await self._payload_written_by_callback(mock_user)
    mock_user.session_version = mock_user.session_version + 1  # bumped after write

    mock_lock_manager.return_value = None
    redis_instance = AsyncMock()
    redis_instance.get.return_value = raw_payload
    mock_redis_client.return_value = redis_instance
    mock_get_by_id.return_value = mock_user

    request = Mock()
    request.headers = {}
    with pytest.raises(HTTPException) as exc_info:
      await sso_complete(
        request=request,
        body=SSOCompleteRequest(session_id=session_id),
        response=Mock(),
        session=Mock(),
        _rate_limit=None,
      )

    assert exc_info.value.status_code == status.HTTP_401_UNAUTHORIZED
