"""The authorize → consent → code leg."""

from unittest.mock import AsyncMock, patch
from urllib.parse import parse_qs, urlsplit

import pytest
from fastapi import HTTPException

from robosystems.models.core import OAuthClient, OAuthGrant
from robosystems.operations.oauth_server.authorization import (
  AuthorizationCodeStore,
  AuthorizeError,
  AuthorizeParams,
  ConsentError,
  PendingAuthorizationStore,
  begin_authorization,
  client_callback,
  record_decision,
)

KG = "kg19fb490f76871d22e835"
CHALLENGE = "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"


def _params(**overrides):
  base = {
    "response_type": "code",
    "client_id": None,
    "redirect_uri": "https://claude.ai/api/mcp/auth_callback",
    "state": "xyz",
    "code_challenge": CHALLENGE,
    "code_challenge_method": "S256",
    "scope": "mcp",
    "resource": "https://api.test.example/v1/mcp",
  }
  base.update(overrides)
  return AuthorizeParams(**base)


@pytest.fixture
def claude_client(test_db):
  client, _ = OAuthClient.register_preregistered(
    client_name="Claude",
    redirect_uris=["https://claude.ai/api/mcp/auth_callback"],
    confidential=False,
    session=test_db,
  )
  return client


def _query(url: str) -> dict[str, str]:
  return {k: v[0] for k, v in parse_qs(urlsplit(url).query).items()}


@pytest.mark.usefixtures("oauth_env", "fake_redis")
class TestBeginAuthorization:
  def test_unknown_client_is_not_redirected(self, test_db):
    with pytest.raises(AuthorizeError) as exc:
      begin_authorization(_params(client_id="rfsoc_nope"), test_db)
    assert exc.value.error == "invalid_client"
    assert exc.value.redirect_uri is None

  def test_unregistered_redirect_is_not_redirected(self, test_db, claude_client):
    with pytest.raises(AuthorizeError) as exc:
      begin_authorization(
        _params(
          client_id=claude_client.client_id, redirect_uri="https://evil.example/cb"
        ),
        test_db,
      )
    assert exc.value.error == "invalid_request"
    assert exc.value.redirect_uri is None

  @pytest.mark.parametrize(
    "override, error",
    [
      ({"response_type": "token"}, "unsupported_response_type"),
      ({"code_challenge": None}, "invalid_request"),
      ({"code_challenge": "short"}, "invalid_request"),
      ({"code_challenge_method": "plain"}, "invalid_request"),
      ({"scope": "mcp admin"}, "invalid_scope"),
      ({"resource": "https://evil.example/v1/mcp"}, "invalid_target"),
      ({"state": "s" * 2000}, "invalid_request"),
    ],
  )
  def test_request_errors_go_back_to_the_client(
    self, test_db, claude_client, override, error
  ):
    with pytest.raises(AuthorizeError) as exc:
      begin_authorization(
        _params(client_id=claude_client.client_id, **override), test_db
      )
    assert exc.value.error == error
    if override.get("state") is None or len(override.get("state", "")) < 2000:
      assert exc.value.redirect_uri == "https://claude.ai/api/mcp/auth_callback"
      assert exc.value.state == "xyz"

  def test_success_parks_the_request(self, test_db, claude_client, fake_redis):
    location = begin_authorization(_params(client_id=claude_client.client_id), test_db)
    assert location.startswith("https://app.test.example/oauth/consent?request_id=")
    request_id = _query(location)["request_id"]
    pending = PendingAuthorizationStore.peek(request_id)
    assert pending is not None
    assert pending.client_row_id == claude_client.id
    assert pending.client_name == "Claude"
    assert pending.is_trusted
    assert pending.redirect_host == "claude.ai"
    assert not pending.is_loopback
    assert pending.resource == "https://api.test.example/v1/mcp"
    assert pending.graph_id is None
    assert pending.scope == "mcp"
    assert pending.code_challenge == CHALLENGE
    assert pending.state == "xyz"

  def test_per_graph_resource_fixes_the_graph(self, test_db, claude_client):
    location = begin_authorization(
      _params(
        client_id=claude_client.client_id,
        resource=f"https://api.test.example/v1/graphs/{KG}/mcp",
      ),
      test_db,
    )
    pending = PendingAuthorizationStore.peek(_query(location)["request_id"])
    assert pending.graph_id == KG

  def test_absent_resource_defaults_to_agnostic(self, test_db, claude_client):
    location = begin_authorization(
      _params(client_id=claude_client.client_id, resource=None), test_db
    )
    pending = PendingAuthorizationStore.peek(_query(location)["request_id"])
    assert pending.resource == "https://api.test.example/v1/mcp"


@pytest.mark.usefixtures("oauth_env")
class TestClientCallback:
  def test_preserves_registered_query_and_adds_iss(self):
    url = client_callback("https://a.example/cb?keep=1", {"code": "abc"}, "st")
    q = _query(url)
    assert q == {
      "keep": "1",
      "code": "abc",
      "state": "st",
      "iss": "https://api.test.example",
    }

  def test_no_state_when_client_sent_none(self):
    q = _query(client_callback("https://a.example/cb", {"code": "abc"}, None))
    assert "state" not in q and q["iss"] == "https://api.test.example"


@pytest.mark.usefixtures("oauth_env", "fake_redis")
class TestRecordDecision:
  @pytest.fixture
  def pending_id(self, test_db, claude_client):
    location = begin_authorization(_params(client_id=claude_client.client_id), test_db)
    return _query(location)["request_id"]

  async def test_expired_request(self, test_db, test_user):
    with pytest.raises(ConsentError) as exc:
      await record_decision(
        request_id="nope", user=test_user, approved=True, graph_id=KG, session=test_db
      )
    assert exc.value.status_code == 404

  async def test_deny_goes_back_with_access_denied(
    self, test_db, test_user, pending_id
  ):
    url = await record_decision(
      request_id=pending_id,
      user=test_user,
      approved=False,
      graph_id=None,
      session=test_db,
    )
    q = _query(url)
    assert q["error"] == "access_denied"
    assert q["state"] == "xyz"
    assert q["iss"] == "https://api.test.example"
    # Consumed: a second answer finds nothing.
    assert PendingAuthorizationStore.peek(pending_id) is None

  async def test_agnostic_requires_a_graph(self, test_db, test_user, pending_id):
    with pytest.raises(ConsentError) as exc:
      await record_decision(
        request_id=pending_id,
        user=test_user,
        approved=True,
        graph_id=None,
        session=test_db,
      )
    assert exc.value.status_code == 400

  async def test_malformed_graph_id(self, test_db, test_user, pending_id):
    with pytest.raises(ConsentError) as exc:
      await record_decision(
        request_id=pending_id,
        user=test_user,
        approved=True,
        graph_id="../etc",
        session=test_db,
      )
    assert exc.value.status_code == 400

  async def test_graph_without_access_is_refused(self, test_db, test_user, pending_id):
    with patch(
      "robosystems.routers.graphs.mcp.handlers.validate_mcp_access",
      new=AsyncMock(side_effect=HTTPException(status_code=403, detail="no")),
    ):
      with pytest.raises(ConsentError) as exc:
        await record_decision(
          request_id=pending_id,
          user=test_user,
          approved=True,
          graph_id=KG,
          session=test_db,
        )
    assert exc.value.status_code == 403
    assert OAuthGrant.get_active_by_user_id(str(test_user.id), test_db) == []

  async def test_approve_writes_grant_and_issues_code(
    self, test_db, test_user, claude_client, pending_id, fake_redis
  ):
    with patch(
      "robosystems.routers.graphs.mcp.handlers.validate_mcp_access", new=AsyncMock()
    ) as access:
      url = await record_decision(
        request_id=pending_id,
        user=test_user,
        approved=True,
        graph_id=KG,
        session=test_db,
      )
    access.assert_awaited_once()
    assert access.await_args.args[0] == KG
    assert access.await_args.args[3] == "read"

    q = _query(url)
    assert url.startswith("https://claude.ai/api/mcp/auth_callback?")
    assert q["state"] == "xyz" and q["iss"] == "https://api.test.example"

    grants = OAuthGrant.get_active_by_user_id(str(test_user.id), test_db)
    assert len(grants) == 1
    assert grants[0].graph_id == KG
    assert grants[0].oauth_client_id == claude_client.id
    assert grants[0].resource == "https://api.test.example/v1/mcp"

    payload = AuthorizationCodeStore.consume(q["code"])
    assert payload["grant_id"] == grants[0].id
    assert payload["client_row_id"] == claude_client.id
    assert payload["code_challenge"] == CHALLENGE
    assert payload["resource"] == "https://api.test.example/v1/mcp"
    # Single use.
    assert AuthorizationCodeStore.consume(q["code"]) is None

  async def test_per_graph_pending_rejects_a_different_graph(
    self, test_db, test_user, claude_client
  ):
    location = begin_authorization(
      _params(
        client_id=claude_client.client_id,
        resource=f"https://api.test.example/v1/graphs/{KG}/mcp",
      ),
      test_db,
    )
    request_id = _query(location)["request_id"]
    with pytest.raises(ConsentError) as exc:
      await record_decision(
        request_id=request_id,
        user=test_user,
        approved=True,
        graph_id="kg000000000000000000ff",
        session=test_db,
      )
    assert exc.value.status_code == 400
