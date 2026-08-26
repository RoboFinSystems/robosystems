"""The OAuth carriage on the MCP routes: audience binding, challenges,
the OAuth-only rule on /v1/mcp."""

from unittest.mock import Mock, patch

import pytest
from fastapi import HTTPException

from robosystems.config import env
from robosystems.middleware.auth import dependencies as deps
from robosystems.middleware.auth.oauth import OAuthPrincipal
from robosystems.models.core import User

KG = "kg19fb490f76871d22e835"
OTHER = "kg000000000000000000ff"
ISSUER = "https://api.test.example"
TOKEN = "rfso" + "a" * 64


def _request(headers=None, host="203.0.113.7", path=f"/v1/graphs/{KG}/mcp"):
  request = Mock()
  request.headers = headers or {}
  request.client = Mock()
  request.client.host = host
  request.url = Mock()
  request.url.path = path
  request.state = Mock()
  return request


def _principal(graph_id=KG, resource=f"{ISSUER}/v1/graphs/{KG}/mcp"):
  user = User(id="user-1", email="u@example.com", name="U", is_active=True)
  return OAuthPrincipal(
    user=user,
    token_id="oat_1",
    grant_id="oag_1",
    client_row_id="oac_1",
    graph_id=graph_id,
    resource=resource,
    scope="mcp",
  )


@pytest.fixture
def oauth_on():
  with (
    patch.object(env, "MCP_OAUTH_ENABLED", True),
    patch.object(env, "ROBOSYSTEMS_API_URL", ISSUER),
  ):
    yield


@pytest.fixture
def oauth_off():
  with patch.object(env, "MCP_OAUTH_ENABLED", False):
    yield


class TestBearerDetection:
  def test_opaque_token_is_detected(self):
    assert (
      deps._oauth_bearer_token(_request({"authorization": f"Bearer {TOKEN}"})) == TOKEN
    )

  def test_jwt_is_not(self):
    assert (
      deps._oauth_bearer_token(_request({"authorization": "Bearer eyJhb.x.y"})) is None
    )

  def test_absent(self):
    assert deps._oauth_bearer_token(_request({})) is None


@pytest.mark.usefixtures("oauth_on")
class TestPerGraphRoute:
  async def test_valid_token_bound_to_this_graph(self):
    request = _request({"authorization": f"Bearer {TOKEN}"})
    with (
      patch.object(deps, "validate_oauth_access_token", return_value=_principal()),
      patch.object(
        deps.api_key_cache, "get_cached_jwt_graph_access", return_value=True
      ),
      patch.object(deps, "publish_principal") as publish,
      patch.object(deps, "SecurityAuditLogger"),
    ):
      user = await deps.get_current_user_with_graph_or_url_token(
        request, KG, api_key=None, token=None
      )
    assert user.id == "user-1"
    publish.assert_called_once()
    assert publish.call_args.args[2] == "oauth"
    assert request.state.auth_graph_id == KG

  async def test_audience_mismatch_is_401_invalid_token(self):
    request = _request({"authorization": f"Bearer {TOKEN}"})
    with (
      patch.object(
        deps,
        "validate_oauth_access_token",
        return_value=_principal(graph_id=KG, resource=f"{ISSUER}/v1/mcp"),
      ),
      patch.object(deps, "SecurityAuditLogger"),
      pytest.raises(HTTPException) as exc,
    ):
      await deps.get_current_user_with_graph_or_url_token(
        request, KG, api_key=None, token=None
      )
    assert exc.value.status_code == 401
    challenge = exc.value.headers["WWW-Authenticate"]
    assert 'error="invalid_token"' in challenge
    assert f"oauth-protected-resource/v1/graphs/{KG}/mcp" in challenge

  async def test_token_for_another_graph_url_is_401(self):
    request = _request({"authorization": f"Bearer {TOKEN}"})
    with (
      patch.object(
        deps,
        "validate_oauth_access_token",
        return_value=_principal(
          graph_id=OTHER, resource=f"{ISSUER}/v1/graphs/{OTHER}/mcp"
        ),
      ),
      patch.object(deps, "SecurityAuditLogger"),
      pytest.raises(HTTPException) as exc,
    ):
      await deps.get_current_user_with_graph_or_url_token(
        request, KG, api_key=None, token=None
      )
    assert exc.value.status_code == 401

  async def test_invalid_token_is_401(self):
    request = _request({"authorization": f"Bearer {TOKEN}"})
    with (
      patch.object(deps, "validate_oauth_access_token", return_value=None),
      patch.object(deps, "SecurityAuditLogger"),
      pytest.raises(HTTPException) as exc,
    ):
      await deps.get_current_user_with_graph_or_url_token(
        request, KG, api_key=None, token=None
      )
    assert exc.value.status_code == 401
    assert 'error="invalid_token"' in exc.value.headers["WWW-Authenticate"]

  async def test_lost_membership_is_403_insufficient_scope(self):
    request = _request({"authorization": f"Bearer {TOKEN}"})
    with (
      patch.object(deps, "validate_oauth_access_token", return_value=_principal()),
      patch.object(
        deps.api_key_cache, "get_cached_jwt_graph_access", return_value=None
      ),
      patch.object(deps.api_key_cache, "cache_jwt_graph_access"),
      patch.object(deps, "_db_check_graph_access", return_value=False),
      patch.object(deps, "SecurityAuditLogger"),
      pytest.raises(HTTPException) as exc,
    ):
      await deps.get_current_user_with_graph_or_url_token(
        request, KG, api_key=None, token=None
      )
    assert exc.value.status_code == 403
    assert 'error="insufficient_scope"' in exc.value.headers["WWW-Authenticate"]

  async def test_no_credentials_carries_the_discovery_challenge(self):
    request = _request({})
    with (
      patch.object(deps, "SecurityAuditLogger"),
      pytest.raises(HTTPException) as exc,
    ):
      await deps.get_current_user_with_graph_or_url_token(
        request, KG, api_key=None, token=None
      )
    assert exc.value.status_code == 401
    challenge = exc.value.headers["WWW-Authenticate"]
    assert challenge.startswith("Bearer resource_metadata=")
    assert f"/.well-known/oauth-protected-resource/v1/graphs/{KG}/mcp" in challenge
    assert 'scope="mcp"' in challenge

  async def test_api_key_carriage_is_unchanged(self):
    """An X-API-Key still resolves through the header path (no OAuth involved)."""
    request = _request({})
    user = User(id="user-2", email="k@example.com", is_active=True)
    with patch.object(
      deps, "get_current_user_with_graph", return_value=user
    ) as header_path:
      resolved = await deps.get_current_user_with_graph_or_url_token(
        request, KG, api_key="rfsabc", token=None
      )
    assert resolved.id == "user-2"
    header_path.assert_awaited_once()


@pytest.mark.usefixtures("oauth_off")
class TestPerGraphRouteFlagOff:
  async def test_opaque_bearer_falls_through_to_the_jwt_path(self):
    request = _request({"authorization": f"Bearer {TOKEN}"})
    with (
      patch.object(deps, "validate_oauth_access_token") as validate,
      patch.object(deps, "verify_jwt_claims", return_value=None),
      patch.object(deps, "SecurityAuditLogger"),
      patch.object(deps, "extract_device_fingerprint", return_value=None),
      pytest.raises(HTTPException) as exc,
    ):
      await deps.get_current_user_with_graph_or_url_token(
        request, KG, api_key=None, token=None
      )
    validate.assert_not_called()
    assert exc.value.status_code == 401
    assert exc.value.headers["WWW-Authenticate"] == "Bearer, ApiKey"


@pytest.mark.usefixtures("oauth_on")
class TestAgnosticRoute:
  async def test_valid_token_yields_principal(self):
    request = _request({"authorization": f"Bearer {TOKEN}"}, path="/v1/mcp")
    principal = _principal(graph_id=KG, resource=f"{ISSUER}/v1/mcp")
    with (
      patch.object(deps, "validate_oauth_access_token", return_value=principal),
      patch.object(
        deps.api_key_cache, "get_cached_jwt_graph_access", return_value=True
      ),
      patch.object(deps, "publish_principal"),
      patch.object(deps, "SecurityAuditLogger"),
    ):
      resolved = await deps.get_oauth_mcp_principal(request, api_key=None, token=None)
    assert resolved.graph_id == KG
    assert request.state.auth_graph_id == KG

  async def test_per_graph_token_is_refused_here(self):
    request = _request({"authorization": f"Bearer {TOKEN}"}, path="/v1/mcp")
    with (
      patch.object(deps, "validate_oauth_access_token", return_value=_principal()),
      patch.object(deps, "SecurityAuditLogger"),
      pytest.raises(HTTPException) as exc,
    ):
      await deps.get_oauth_mcp_principal(request, api_key=None, token=None)
    assert exc.value.status_code == 401

  @pytest.mark.parametrize(
    "headers, api_key, token",
    [
      ({}, "rfsabc", None),
      ({}, None, "rfscabc"),
      ({"authorization": "Bearer eyJhb.x.y"}, None, None),
      ({}, None, None),
    ],
  )
  async def test_every_non_oauth_carriage_is_401_with_challenge(
    self, headers, api_key, token
  ):
    request = _request(headers, path="/v1/mcp")
    with (
      patch.object(deps, "SecurityAuditLogger"),
      patch.object(deps, "validate_oauth_access_token") as validate,
      pytest.raises(HTTPException) as exc,
    ):
      await deps.get_oauth_mcp_principal(request, api_key=api_key, token=token)
    validate.assert_not_called()
    assert exc.value.status_code == 401
    assert (
      "/.well-known/oauth-protected-resource/v1/mcp"
      in exc.value.headers["WWW-Authenticate"]
    )

  async def test_flag_off_is_404(self):
    request = _request({"authorization": f"Bearer {TOKEN}"}, path="/v1/mcp")
    with (
      patch.object(env, "MCP_OAUTH_ENABLED", False),
      pytest.raises(HTTPException) as exc,
    ):
      await deps.get_oauth_mcp_principal(request, api_key=None, token=None)
    assert exc.value.status_code == 404
