"""The OAuth wire surface, end to end through the app."""

from unittest.mock import AsyncMock, patch
from urllib.parse import parse_qs, urlsplit

import pytest
from authlib.oauth2.rfc7636 import create_s256_code_challenge

from robosystems.config import env
from robosystems.middleware.auth.dependencies import get_optional_jwt_user
from robosystems.models.core import OAuthGrant

KG = "kg19fb490f76871d22e835"
VERIFIER = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"
CHALLENGE = create_s256_code_challenge(VERIFIER)
VSCODE_BODY = {
  "client_name": "Visual Studio Code",
  "client_uri": "https://code.visualstudio.com",
  "response_types": ["code"],
  "redirect_uris": ["https://vscode.dev/redirect", "http://127.0.0.1/"],
  "token_endpoint_auth_method": "none",
  "application_type": "native",
}


class FakeRedis:
  def __init__(self):
    self.store = {}

  def setex(self, key, ttl, value):
    self.store[key] = value

  def get(self, key):
    return self.store.get(key)

  def getdel(self, key):
    return self.store.pop(key, None)


@pytest.fixture
def oauth_on():
  with (
    patch.object(env, "MCP_OAUTH_ENABLED", True),
    patch.object(env, "ROBOSYSTEMS_API_URL", "http://testserver"),
    patch(
      "robosystems.operations.oauth_server.authorization.create_redis_client",
      return_value=FakeRedis(),
    ),
  ):
    yield


def _q(url):
  return {k: v[0] for k, v in parse_qs(urlsplit(url).query).items()}


class TestFlagOff:
  @pytest.mark.parametrize(
    "method, path",
    [
      ("GET", "/.well-known/oauth-authorization-server"),
      ("GET", "/.well-known/oauth-protected-resource/v1/mcp"),
      ("GET", "/v1/oauth/authorize"),
      ("POST", "/v1/oauth/token"),
      ("POST", "/v1/oauth/register"),
      ("POST", "/v1/oauth/revoke"),
    ],
  )
  def test_surface_is_absent(self, client, method, path):
    with patch.object(env, "MCP_OAUTH_ENABLED", False):
      response = client.request(method, path)
    assert response.status_code == 404


@pytest.mark.usefixtures("oauth_on")
class TestDiscovery:
  def test_authorization_server_metadata(self, client):
    doc = client.get("/.well-known/oauth-authorization-server").json()
    assert doc["issuer"] == "http://testserver"
    assert doc["token_endpoint"] == "http://testserver/v1/oauth/token"
    assert doc["code_challenge_methods_supported"] == ["S256"]

  def test_protected_resource_documents(self, client):
    root = client.get("/.well-known/oauth-protected-resource").json()
    agnostic = client.get("/.well-known/oauth-protected-resource/v1/mcp").json()
    graph = client.get(
      f"/.well-known/oauth-protected-resource/v1/graphs/{KG}/mcp"
    ).json()
    assert root == agnostic
    assert agnostic["resource"] == "http://testserver/v1/mcp"
    assert graph["resource"] == f"http://testserver/v1/graphs/{KG}/mcp"
    assert graph["authorization_servers"] == ["http://testserver"]

  def test_malformed_graph_id_is_not_served(self, client):
    # (A ".." segment is normalized away by HTTP clients before it reaches
    # the server; a character outside the id alphabet is what survives.)
    response = client.get(
      "/.well-known/oauth-protected-resource/v1/graphs/kg%20bad!/mcp"
    )
    assert response.status_code in (404, 422)


@pytest.mark.usefixtures("oauth_on")
class TestRegistration:
  def test_public_client(self, client):
    response = client.post("/v1/oauth/register", json=VSCODE_BODY)
    assert response.status_code == 201, response.text
    body = response.json()
    assert body["client_id"].startswith("rfsoc_")
    assert "client_secret" not in body
    assert body["token_endpoint_auth_method"] == "none"
    assert body["redirect_uris"] == VSCODE_BODY["redirect_uris"]
    assert response.headers["cache-control"] == "no-store"

  def test_confidential_client(self, client):
    body = {**VSCODE_BODY, "token_endpoint_auth_method": "client_secret_post"}
    response = client.post("/v1/oauth/register", json=body)
    assert response.status_code == 201
    assert response.json()["client_secret"].startswith("rfsos")
    assert response.json()["client_secret_expires_at"] == 0

  def test_rejections(self, client):
    response = client.post(
      "/v1/oauth/register", content="x", headers={"content-type": "text/plain"}
    )
    assert response.status_code == 400
    assert response.json()["error"] == "invalid_client_metadata"
    response = client.post(
      "/v1/oauth/register", json={"redirect_uris": ["http://evil.example/cb"]}
    )
    assert response.status_code == 400
    assert response.json()["error"] == "invalid_redirect_uri"


@pytest.mark.usefixtures("oauth_on")
class TestTokenEndpointErrors:
  def test_wrong_content_type(self, client):
    response = client.post("/v1/oauth/token", json={"grant_type": "authorization_code"})
    assert response.status_code == 400
    assert response.json()["error"] == "invalid_request"

  def test_unknown_client(self, client):
    response = client.post(
      "/v1/oauth/token",
      data={"grant_type": "authorization_code", "client_id": "rfsoc_nope", "code": "x"},
    )
    assert response.status_code == 401
    assert response.json()["error"] == "invalid_client"

  def test_unsupported_grant(self, client):
    registered = client.post("/v1/oauth/register", json=VSCODE_BODY).json()
    response = client.post(
      "/v1/oauth/token",
      data={"grant_type": "client_credentials", "client_id": registered["client_id"]},
    )
    assert response.status_code == 400
    assert response.json()["error"] == "unsupported_grant_type"


@pytest.mark.usefixtures("oauth_on")
class TestAuthorizeEndpoint:
  def test_unknown_client_is_a_400_not_a_redirect(self, client):
    response = client.get(
      "/v1/oauth/authorize",
      params={"response_type": "code", "client_id": "rfsoc_nope"},
      follow_redirects=False,
    )
    assert response.status_code == 400
    assert response.json()["error"] == "invalid_client"

  def test_bad_request_redirects_back_to_the_client(self, client):
    registered = client.post("/v1/oauth/register", json=VSCODE_BODY).json()
    response = client.get(
      "/v1/oauth/authorize",
      params={
        "response_type": "code",
        "client_id": registered["client_id"],
        "redirect_uri": "https://vscode.dev/redirect",
        "state": "s1",
      },
      follow_redirects=False,
    )
    assert response.status_code == 302
    q = _q(response.headers["location"])
    assert response.headers["location"].startswith("https://vscode.dev/redirect?")
    assert q["error"] == "invalid_request" and q["state"] == "s1"
    assert q["iss"] == "http://testserver"


@pytest.mark.usefixtures("oauth_on")
class TestFullFlow:
  def test_register_authorize_consent_exchange_refresh_revoke(
    self, client, test_user, test_db
  ):
    from main import app

    registered = client.post("/v1/oauth/register", json=VSCODE_BODY).json()
    client_id = registered["client_id"]

    # 1. authorize → consent page on the login home
    response = client.get(
      "/v1/oauth/authorize",
      params={
        "response_type": "code",
        "client_id": client_id,
        "redirect_uri": "http://127.0.0.1:33418/",
        "state": "st4te",
        "code_challenge": CHALLENGE,
        "code_challenge_method": "S256",
        "scope": "mcp",
        "resource": "http://testserver/v1/mcp",
      },
      follow_redirects=False,
    )
    assert response.status_code == 302, response.text
    consent = response.headers["location"]
    assert "/oauth/consent?request_id=" in consent
    request_id = _q(consent)["request_id"]

    # 2. the consent page reads the pending request with the app session
    unauthenticated = client.get(f"/v1/oauth/authorize/{request_id}")
    assert unauthenticated.status_code == 401

    app.dependency_overrides[get_optional_jwt_user] = lambda: test_user
    try:
      pending = client.get(f"/v1/oauth/authorize/{request_id}")
      assert pending.status_code == 200, pending.text
      body = pending.json()
      assert body["client_name"] == "Visual Studio Code"
      assert body["redirect_host"] == "127.0.0.1"
      assert body["is_loopback_redirect"] is True
      assert body["is_trusted"] is False
      assert body["graph_id"] is None
      assert body["resource"] == "http://testserver/v1/mcp"

      # 3. approve with a graph → callback with code
      with patch(
        "robosystems.routers.graphs.mcp.handlers.validate_mcp_access", new=AsyncMock()
      ):
        decision = client.post(
          f"/v1/oauth/authorize/{request_id}/decision",
          json={"approved": True, "graph_id": KG},
        )
      assert decision.status_code == 200, decision.text
      redirect_to = decision.json()["redirect_to"]
      assert redirect_to.startswith("http://127.0.0.1:33418/?")
      q = _q(redirect_to)
      assert q["state"] == "st4te" and q["iss"] == "http://testserver"
      code = q["code"]

      # A second answer is gone.
      assert client.get(f"/v1/oauth/authorize/{request_id}").status_code == 404
    finally:
      app.dependency_overrides.pop(get_optional_jwt_user, None)

    # 4. exchange (public client: client_id in the body, PKCE proves it)
    exchanged = client.post(
      "/v1/oauth/token",
      data={
        "grant_type": "authorization_code",
        "code": code,
        "client_id": client_id,
        "code_verifier": VERIFIER,
        "redirect_uri": "http://127.0.0.1:33418/",
        "resource": "http://testserver/v1/mcp",
      },
    )
    assert exchanged.status_code == 200, exchanged.text
    tokens = exchanged.json()
    assert tokens["token_type"] == "Bearer"
    assert tokens["access_token"].startswith("rfso")
    assert tokens["refresh_token"].startswith("rfsr")
    assert tokens["expires_in"] == 3600
    assert exchanged.headers["cache-control"] == "no-store"

    grants = OAuthGrant.get_active_by_user_id(str(test_user.id), test_db)
    assert [g.graph_id for g in grants] == [KG]

    # 5. refresh rotates
    refreshed = client.post(
      "/v1/oauth/token",
      data={
        "grant_type": "refresh_token",
        "refresh_token": tokens["refresh_token"],
        "client_id": client_id,
        "resource": "http://testserver/v1/mcp",
      },
    )
    assert refreshed.status_code == 200, refreshed.text
    assert refreshed.json()["refresh_token"] != tokens["refresh_token"]

    # 6. the old refresh token is a replay → invalid_grant
    replay = client.post(
      "/v1/oauth/token",
      data={
        "grant_type": "refresh_token",
        "refresh_token": tokens["refresh_token"],
        "client_id": client_id,
      },
    )
    assert replay.status_code == 400
    assert replay.json()["error"] == "invalid_grant"

    # 7. revocation always 200s
    revoked = client.post(
      "/v1/oauth/revoke",
      data={"token": refreshed.json()["refresh_token"], "client_id": client_id},
    )
    assert revoked.status_code == 200
