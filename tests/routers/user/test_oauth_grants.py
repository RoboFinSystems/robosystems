"""Connected apps: a user sees only their own live grants, and revoking one
kills its tokens — the proportionate alternative to a password change."""

from uuid import uuid4

from robosystems.middleware.auth.jwt import create_jwt_token
from robosystems.models.core import Graph, OAuthClient, OAuthGrant, OAuthToken, User
from robosystems.models.core.user.oauth_token import TOKEN_TYPE_ACCESS

RESOURCE = "https://api.test.example/v1/mcp"


def _auth(user):
  return {"Authorization": f"Bearer {create_jwt_token(user.id)}"}


def _client_row(session, name="Claude", trusted=True):
  client, _ = OAuthClient.register_preregistered(
    client_name=name,
    redirect_uris=["https://claude.ai/api/mcp/auth_callback"],
    confidential=False,
    session=session,
  )
  if not trusted:
    client.is_trusted = False
    session.commit()
  return client


def _grant(session, user, client, graph_id="sec"):
  return OAuthGrant.create(
    user_id=str(user.id),
    oauth_client_id=str(client.id),
    graph_id=graph_id,
    resource=RESOURCE,
    scope="mcp offline_access",
    session=session,
  )


def _ensure_sec(session):
  if Graph.get_by_id("sec", session) is None:
    Graph.create(
      graph_id="sec",
      org_id=None,
      graph_name="SEC EDGAR Filings",
      graph_type="repository",
      session=session,
    )


def _other_user(session, like):
  user = User(
    email=f"other+{uuid4().hex[:8]}@example.com",
    name="Other",
    password_hash=like.password_hash,
  )
  session.add(user)
  session.commit()
  session.refresh(user)
  return user


class TestListConnectedApps:
  def test_lists_own_live_grants_with_client_and_graph(
    self, client, test_user, test_db
  ):
    _ensure_sec(test_db)
    trusted = _client_row(test_db, "Claude")
    untrusted = _client_row(test_db, "Some IDE", trusted=False)
    live = _grant(test_db, test_user, trusted)
    on_unknown_graph = _grant(
      test_db, test_user, untrusted, graph_id="kg00000000000000000abc"
    )
    revoked = _grant(test_db, test_user, trusted)
    revoked.revoke(test_db)
    other = _other_user(test_db, test_user)
    _grant(test_db, other, trusted)
    # Captured before the request: the endpoint's session handling detaches
    # instances created beforehand, so their attributes cannot refresh after.
    live_id, unknown_id = str(live.id), str(on_unknown_graph.id)

    resp = client.get("/v1/user/oauth/grants", headers=_auth(test_user))

    assert resp.status_code == 200
    rows = {row["id"]: row for row in resp.json()["grants"]}
    assert set(rows) == {live_id, unknown_id}
    sec = rows[live_id]
    assert sec["client_name"] == "Claude"
    assert sec["client_is_trusted"] is True
    assert sec["graph_id"] == "sec"
    assert sec["graph_name"] == "SEC EDGAR Filings"
    assert sec["resource"] == RESOURCE
    assert sec["scope"] == "mcp offline_access"
    unknown = rows[unknown_id]
    assert unknown["client_is_trusted"] is False
    assert unknown["graph_name"] is None

  def test_requires_authentication(self, client):
    assert client.get("/v1/user/oauth/grants").status_code == 401


class TestRevokeConnectedApp:
  def test_revokes_grant_and_its_tokens(self, client, test_user, test_db):
    grant = _grant(test_db, test_user, _client_row(test_db))
    access, _, refresh, _, _ = OAuthToken.mint_pair(
      grant_id=str(grant.id), user_id=str(test_user.id), session=test_db
    )
    grant_id, access_id, refresh_id = str(grant.id), str(access.id), str(refresh.id)

    resp = client.delete(f"/v1/user/oauth/grants/{grant_id}", headers=_auth(test_user))

    assert resp.status_code == 200
    assert resp.json()["data"] == {"grant_id": grant_id, "tokens_revoked": 2}
    assert OAuthGrant.get_by_id(grant_id, test_db).revoked_at is not None
    for token_id in (access_id, refresh_id):
      row = test_db.query(OAuthToken).filter(OAuthToken.id == token_id).one()
      assert row.revoked_at is not None
    assert (
      test_db.query(OAuthToken)
      .filter(
        OAuthToken.grant_id == grant_id, OAuthToken.token_type == TOKEN_TYPE_ACCESS
      )
      .one()
      .is_live
      is False
    )
    # It has left the list.
    listed = client.get("/v1/user/oauth/grants", headers=_auth(test_user)).json()
    assert grant_id not in {row["id"] for row in listed["grants"]}

  def test_revoking_twice_is_idempotent(self, client, test_user, test_db):
    grant_id = str(_grant(test_db, test_user, _client_row(test_db)).id)
    first = client.delete(f"/v1/user/oauth/grants/{grant_id}", headers=_auth(test_user))
    second = client.delete(
      f"/v1/user/oauth/grants/{grant_id}", headers=_auth(test_user)
    )
    assert first.status_code == 200
    assert second.status_code == 200
    assert second.json()["data"]["tokens_revoked"] == 0

  def test_someone_elses_grant_and_unknown_ids_are_both_404(
    self, client, test_user, test_db
  ):
    other = _other_user(test_db, test_user)
    theirs = str(_grant(test_db, other, _client_row(test_db)).id)

    resp = client.delete(f"/v1/user/oauth/grants/{theirs}", headers=_auth(test_user))
    assert resp.status_code == 404
    assert OAuthGrant.get_by_id(theirs, test_db).revoked_at is None

    resp = client.delete("/v1/user/oauth/grants/oag_nope", headers=_auth(test_user))
    assert resp.status_code == 404
