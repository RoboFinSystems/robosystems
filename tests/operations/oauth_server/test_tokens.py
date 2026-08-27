"""Code exchange, refresh rotation + replay, revocation, and validation."""

from datetime import UTC, datetime, timedelta
from unittest.mock import Mock, patch

import pytest
from authlib.oauth2.rfc7636 import create_s256_code_challenge

from robosystems.middleware.auth.oauth import validate_oauth_access_token
from robosystems.models.core import OAuthClient, OAuthGrant, OAuthToken
from robosystems.models.core.user.oauth_token import (
  TOKEN_TYPE_ACCESS,
  TOKEN_TYPE_REFRESH,
)
from robosystems.operations.oauth_server.authorization import AuthorizationCodeStore
from robosystems.operations.oauth_server.tokens import (
  TokenError,
  exchange_authorization_code,
  refresh_access_token,
  revoke_presented_token,
)

KG = "kg19fb490f76871d22e835"
VERIFIER = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"
CHALLENGE = create_s256_code_challenge(VERIFIER)
RESOURCE = "https://api.test.example/v1/mcp"
REDIRECT = "https://claude.ai/api/mcp/auth_callback"


@pytest.fixture
def client_row(test_db):
  client, _ = OAuthClient.register_preregistered(
    client_name="Claude", redirect_uris=[REDIRECT], confidential=False, session=test_db
  )
  return client


@pytest.fixture
def other_client(test_db):
  client, _ = OAuthClient.register_preregistered(
    client_name="Other", redirect_uris=[REDIRECT], confidential=False, session=test_db
  )
  return client


@pytest.fixture
def grant(test_db, test_user, client_row):
  return OAuthGrant.create(
    user_id=str(test_user.id),
    oauth_client_id=str(client_row.id),
    graph_id=KG,
    resource=RESOURCE,
    scope="mcp",
    session=test_db,
  )


def _issue_code(grant, client_row, user_id):
  return AuthorizationCodeStore.issue(
    {
      "user_id": user_id,
      "client_row_id": str(client_row.id),
      "client_id": str(client_row.client_id),
      "grant_id": str(grant.id),
      "redirect_uri": REDIRECT,
      "code_challenge": CHALLENGE,
      "resource": RESOURCE,
      "scope": "mcp",
    }
  )


@pytest.fixture
def issued(test_db, grant, client_row, test_user, fake_redis, oauth_env):
  code = _issue_code(grant, client_row, str(test_user.id))
  return exchange_authorization_code(
    code=code,
    client=client_row,
    code_verifier=VERIFIER,
    redirect_uri=REDIRECT,
    resource=RESOURCE,
    session=test_db,
  )


@pytest.mark.usefixtures("oauth_env", "fake_redis")
class TestExchange:
  def test_success(self, test_db, grant, client_row, test_user):
    code = _issue_code(grant, client_row, str(test_user.id))
    issued = exchange_authorization_code(
      code=code,
      client=client_row,
      code_verifier=VERIFIER,
      redirect_uri=REDIRECT,
      resource=RESOURCE,
      session=test_db,
    )
    assert issued.access_token.startswith("rfso")
    assert issued.refresh_token.startswith("rfsr")
    assert issued.expires_in == 3600
    assert issued.scope == "mcp"
    assert issued.as_dict()["token_type"] == "Bearer"
    # Code is single-use.
    with pytest.raises(TokenError) as exc:
      exchange_authorization_code(
        code=code,
        client=client_row,
        code_verifier=VERIFIER,
        redirect_uri=REDIRECT,
        resource=RESOURCE,
        session=test_db,
      )
    assert exc.value.error == "invalid_grant"

  def test_wrong_verifier_burns_the_code(self, test_db, grant, client_row, test_user):
    code = _issue_code(grant, client_row, str(test_user.id))
    with pytest.raises(TokenError) as exc:
      exchange_authorization_code(
        code=code,
        client=client_row,
        code_verifier="x" * 43,
        redirect_uri=REDIRECT,
        resource=RESOURCE,
        session=test_db,
      )
    assert exc.value.error == "invalid_grant"
    with pytest.raises(TokenError):
      exchange_authorization_code(
        code=code,
        client=client_row,
        code_verifier=VERIFIER,
        redirect_uri=REDIRECT,
        resource=RESOURCE,
        session=test_db,
      )

  def test_other_client_cannot_redeem(
    self, test_db, grant, client_row, other_client, test_user
  ):
    code = _issue_code(grant, client_row, str(test_user.id))
    with pytest.raises(TokenError) as exc:
      exchange_authorization_code(
        code=code,
        client=other_client,
        code_verifier=VERIFIER,
        redirect_uri=REDIRECT,
        resource=RESOURCE,
        session=test_db,
      )
    assert exc.value.error == "invalid_grant"

  def test_redirect_and_resource_must_match(
    self, test_db, grant, client_row, test_user
  ):
    code = _issue_code(grant, client_row, str(test_user.id))
    with pytest.raises(TokenError) as exc:
      exchange_authorization_code(
        code=code,
        client=client_row,
        code_verifier=VERIFIER,
        redirect_uri="https://claude.ai/other",
        resource=RESOURCE,
        session=test_db,
      )
    assert exc.value.error == "invalid_grant"
    code = _issue_code(grant, client_row, str(test_user.id))
    with pytest.raises(TokenError) as exc:
      exchange_authorization_code(
        code=code,
        client=client_row,
        code_verifier=VERIFIER,
        redirect_uri=None,
        resource=f"https://api.test.example/v1/graphs/{KG}/mcp",
        session=test_db,
      )
    assert exc.value.error == "invalid_target"

  def test_revoked_grant(self, test_db, grant, client_row, test_user):
    code = _issue_code(grant, client_row, str(test_user.id))
    grant.revoke(test_db)
    with pytest.raises(TokenError) as exc:
      exchange_authorization_code(
        code=code,
        client=client_row,
        code_verifier=VERIFIER,
        redirect_uri=None,
        resource=None,
        session=test_db,
      )
    assert exc.value.error == "invalid_grant"


@pytest.mark.usefixtures("oauth_env", "fake_redis")
class TestRefresh:
  def test_rotation(self, test_db, issued, client_row):
    rotated = refresh_access_token(
      refresh_token=issued.refresh_token,
      client=client_row,
      scope=None,
      resource=RESOURCE,
      session=test_db,
    )
    assert rotated.access_token != issued.access_token
    assert rotated.refresh_token != issued.refresh_token
    old_refresh = OAuthToken.get_by_plaintext(
      issued.refresh_token, TOKEN_TYPE_REFRESH, test_db
    )
    assert old_refresh.used_at is not None
    old_access = OAuthToken.get_by_plaintext(
      issued.access_token, TOKEN_TYPE_ACCESS, test_db
    )
    assert old_access.revoked_at is not None
    new_refresh = OAuthToken.get_by_plaintext(
      rotated.refresh_token, TOKEN_TYPE_REFRESH, test_db
    )
    assert new_refresh.family_id == old_refresh.family_id

  def test_replay_revokes_the_family(self, test_db, issued, client_row):
    rotated = refresh_access_token(
      refresh_token=issued.refresh_token,
      client=client_row,
      scope=None,
      resource=None,
      session=test_db,
    )
    with pytest.raises(TokenError) as exc:
      refresh_access_token(
        refresh_token=issued.refresh_token,
        client=client_row,
        scope=None,
        resource=None,
        session=test_db,
      )
    assert exc.value.error == "invalid_grant"
    # The successor pair is dead too.
    new_refresh = OAuthToken.get_by_plaintext(
      rotated.refresh_token, TOKEN_TYPE_REFRESH, test_db
    )
    assert new_refresh.revoked_at is not None
    new_access = OAuthToken.get_by_plaintext(
      rotated.access_token, TOKEN_TYPE_ACCESS, test_db
    )
    assert new_access.revoked_at is not None
    with pytest.raises(TokenError):
      refresh_access_token(
        refresh_token=rotated.refresh_token,
        client=client_row,
        scope=None,
        resource=None,
        session=test_db,
      )

  def test_other_client_and_garbage(self, test_db, issued, other_client, client_row):
    with pytest.raises(TokenError):
      refresh_access_token(
        refresh_token=issued.refresh_token,
        client=other_client,
        scope=None,
        resource=None,
        session=test_db,
      )
    with pytest.raises(TokenError):
      refresh_access_token(
        refresh_token="rfsr" + "0" * 64,
        client=client_row,
        scope=None,
        resource=None,
        session=test_db,
      )
    with pytest.raises(TokenError):
      refresh_access_token(
        refresh_token=issued.access_token,
        client=client_row,
        scope=None,
        resource=None,
        session=test_db,
      )

  def test_scope_cannot_widen_and_resource_must_match(
    self, test_db, issued, client_row
  ):
    with pytest.raises(TokenError) as exc:
      refresh_access_token(
        refresh_token=issued.refresh_token,
        client=client_row,
        scope="mcp offline_access",
        resource=None,
        session=test_db,
      )
    assert exc.value.error == "invalid_scope"
    with pytest.raises(TokenError) as exc:
      refresh_access_token(
        refresh_token=issued.refresh_token,
        client=client_row,
        scope=None,
        resource=f"https://api.test.example/v1/graphs/{KG}/mcp",
        session=test_db,
      )
    assert exc.value.error == "invalid_target"

  def test_expired_refresh_token(self, test_db, issued, client_row):
    row = OAuthToken.get_by_plaintext(issued.refresh_token, TOKEN_TYPE_REFRESH, test_db)
    row.expires_at = datetime.now(UTC) - timedelta(seconds=1)
    test_db.commit()
    with pytest.raises(TokenError) as exc:
      refresh_access_token(
        refresh_token=issued.refresh_token,
        client=client_row,
        scope=None,
        resource=None,
        session=test_db,
      )
    assert exc.value.error == "invalid_grant"

  def test_replay_of_an_expired_consumed_token_still_revokes_the_family(
    self, test_db, issued, client_row
  ):
    """A consumed refresh token replayed after its own expiry is still a
    replay — its successor may be live — so the family goes; the expiry
    must not mask it."""
    rotated = refresh_access_token(
      refresh_token=issued.refresh_token,
      client=client_row,
      scope=None,
      resource=None,
      session=test_db,
    )
    consumed = OAuthToken.get_by_plaintext(
      issued.refresh_token, TOKEN_TYPE_REFRESH, test_db
    )
    consumed.expires_at = datetime.now(UTC) - timedelta(days=1)
    test_db.commit()

    with pytest.raises(TokenError) as exc:
      refresh_access_token(
        refresh_token=issued.refresh_token,
        client=client_row,
        scope=None,
        resource=None,
        session=test_db,
      )
    assert exc.value.error == "invalid_grant"
    successor = OAuthToken.get_by_plaintext(
      rotated.refresh_token, TOKEN_TYPE_REFRESH, test_db
    )
    assert successor.revoked_at is not None


@pytest.mark.usefixtures("oauth_env", "fake_redis")
class TestRevocation:
  def test_refresh_revokes_family(self, test_db, issued, client_row):
    revoke_presented_token(
      token=issued.refresh_token, client=client_row, session=test_db
    )
    for plain, kind in (
      (issued.access_token, TOKEN_TYPE_ACCESS),
      (issued.refresh_token, TOKEN_TYPE_REFRESH),
    ):
      assert OAuthToken.get_by_plaintext(plain, kind, test_db).revoked_at is not None

  def test_access_revokes_only_itself(self, test_db, issued, client_row):
    revoke_presented_token(
      token=issued.access_token, client=client_row, session=test_db
    )
    assert OAuthToken.get_by_plaintext(
      issued.access_token, TOKEN_TYPE_ACCESS, test_db
    ).revoked_at
    assert (
      OAuthToken.get_by_plaintext(
        issued.refresh_token, TOKEN_TYPE_REFRESH, test_db
      ).revoked_at
      is None
    )

  def test_unknown_and_foreign_tokens_are_ignored(self, test_db, issued, other_client):
    revoke_presented_token(
      token="rfso" + "0" * 64, client=other_client, session=test_db
    )
    revoke_presented_token(
      token=issued.refresh_token, client=other_client, session=test_db
    )
    assert (
      OAuthToken.get_by_plaintext(
        issued.refresh_token, TOKEN_TYPE_REFRESH, test_db
      ).revoked_at
      is None
    )


@pytest.fixture
def no_cache():
  cache = Mock()
  cache.get_cached_api_key_validation.return_value = None
  with patch("robosystems.middleware.auth.oauth.api_key_cache", cache):
    yield cache


@pytest.mark.usefixtures("oauth_env", "fake_redis", "no_cache")
class TestValidateAccessToken:
  def test_valid_token_resolves_to_principal(
    self, test_db, issued, grant, test_user, no_cache
  ):
    with patch("robosystems.database.SessionFactory", return_value=test_db):
      principal = validate_oauth_access_token(issued.access_token)
    assert principal is not None
    assert principal.user_id == str(test_user.id)
    assert principal.graph_id == KG
    assert principal.resource == RESOURCE
    assert principal.grant_id == grant.id
    assert principal.scope == "mcp"
    no_cache.cache_api_key_validation.assert_called_once()
    payload = no_cache.cache_api_key_validation.call_args.args[1]
    assert payload["oauth_graph_id"] == KG and payload["is_active"] is True

  def test_cache_hit_short_circuits_the_db(
    self, test_db, issued, grant, test_user, no_cache
  ):
    with patch("robosystems.database.SessionFactory", return_value=test_db):
      validate_oauth_access_token(issued.access_token)
    payload = no_cache.cache_api_key_validation.call_args.args[1]
    no_cache.get_cached_api_key_validation.return_value = {"user_data": payload}
    with patch(
      "robosystems.database.SessionFactory", side_effect=AssertionError("db hit")
    ):
      principal = validate_oauth_access_token(issued.access_token)
    assert principal is not None and principal.graph_id == KG

  def test_expired_cache_payload_is_ignored(self, test_db, issued, no_cache):
    with patch("robosystems.database.SessionFactory", return_value=test_db):
      validate_oauth_access_token(issued.access_token)
    payload = dict(no_cache.cache_api_key_validation.call_args.args[1])
    payload["oauth_expires_at"] = (datetime.now(UTC) - timedelta(seconds=1)).isoformat()
    no_cache.get_cached_api_key_validation.return_value = {"user_data": payload}
    row = OAuthToken.get_by_plaintext(issued.access_token, TOKEN_TYPE_ACCESS, test_db)
    row.expires_at = datetime.now(UTC) - timedelta(seconds=1)
    test_db.commit()
    with patch("robosystems.database.SessionFactory", return_value=test_db):
      assert validate_oauth_access_token(issued.access_token) is None

  def test_revoked_grant_and_wrong_prefix(self, test_db, issued, grant):
    grant.revoke(test_db)
    with patch("robosystems.database.SessionFactory", return_value=test_db):
      assert validate_oauth_access_token(issued.access_token) is None
      assert validate_oauth_access_token(issued.refresh_token) is None
      assert validate_oauth_access_token("eyJhbGciOi.jwt.token") is None

  def test_deactivated_client(self, test_db, issued, client_row):
    client_row.deactivate(test_db)
    with patch("robosystems.database.SessionFactory", return_value=test_db):
      assert validate_oauth_access_token(issued.access_token) is None

  def test_user_session_invalidation_revokes_tokens(self, test_db, issued, test_user):
    from robosystems.models.core import User

    user = User.get_by_id(str(test_user.id), test_db)
    with patch.object(User, "_invalidate_auth_cache", return_value=True):
      user.invalidate_sessions(test_db)
    assert OAuthToken.get_by_plaintext(
      issued.access_token, TOKEN_TYPE_ACCESS, test_db
    ).revoked_at
    assert OAuthToken.get_by_plaintext(
      issued.refresh_token, TOKEN_TYPE_REFRESH, test_db
    ).revoked_at
    with patch("robosystems.database.SessionFactory", return_value=test_db):
      assert validate_oauth_access_token(issued.access_token) is None

  def test_client_deactivation_revokes_its_tokens(
    self, test_db, issued, client_row, grant
  ):
    grants, tokens = client_row.deactivate(test_db)
    assert (grants, tokens) == (1, 2)
    assert OAuthGrant.get_by_id(grant.id, test_db).is_revoked
    assert OAuthToken.get_by_plaintext(
      issued.access_token, TOKEN_TYPE_ACCESS, test_db
    ).revoked_at
    assert OAuthToken.get_by_plaintext(
      issued.refresh_token, TOKEN_TYPE_REFRESH, test_db
    ).revoked_at
    with patch("robosystems.database.SessionFactory", return_value=test_db):
      assert validate_oauth_access_token(issued.access_token) is None

  def test_user_deactivation_revokes_tokens(self, test_db, issued, test_user):
    from robosystems.models.core import User

    user = User.get_by_id(str(test_user.id), test_db)
    with patch.object(User, "_invalidate_auth_cache", return_value=True):
      user.deactivate(test_db)
    assert OAuthToken.get_by_plaintext(
      issued.access_token, TOKEN_TYPE_ACCESS, test_db
    ).revoked_at
