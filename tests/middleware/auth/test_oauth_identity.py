"""`cached_oauth_identity`: the pre-authentication read of the OAuth
validation cache that lets the rate limiter name a caller before the auth
dependency runs. It reads the cache only; the database is never touched."""

from datetime import UTC, datetime, timedelta
from unittest.mock import patch

from robosystems.middleware.auth import oauth
from robosystems.models.core.user.oauth_token import OAuthToken

TOKEN = "rfso" + "a" * 48


def _payload(**overrides):
  expires = datetime.now(UTC) + timedelta(minutes=30)
  payload = {
    "id": "user_01ABC",
    "email": "member@example.com",
    "name": "Member",
    "email_verified": True,
    "is_active": True,
    "session_version": 1,
    "oauth_token_id": "tok_1",
    "oauth_grant_id": "grant_1",
    "oauth_client_row_id": "client_1",
    "oauth_graph_id": "kg1a2b3c",
    "oauth_resource": "https://api.example.com/v1/graphs/kg1a2b3c/mcp",
    "oauth_scope": "mcp",
    "oauth_expires_at": expires.isoformat(),
  }
  payload.update(overrides)
  return {"user_data": payload, "is_active": True}


def test_a_live_cached_token_names_the_user_and_the_grant_graph():
  with patch.object(
    oauth.api_key_cache, "get_cached_api_key_validation", return_value=_payload()
  ) as lookup:
    assert oauth.cached_oauth_identity(TOKEN) == ("user_01ABC", "kg1a2b3c")
  lookup.assert_called_once_with(OAuthToken.fingerprint(TOKEN))


def test_a_cache_miss_is_none():
  with patch.object(
    oauth.api_key_cache, "get_cached_api_key_validation", return_value=None
  ):
    assert oauth.cached_oauth_identity(TOKEN) is None


def test_an_expired_entry_is_none():
  stale = (datetime.now(UTC) - timedelta(seconds=1)).isoformat()
  with patch.object(
    oauth.api_key_cache,
    "get_cached_api_key_validation",
    return_value=_payload(oauth_expires_at=stale),
  ):
    assert oauth.cached_oauth_identity(TOKEN) is None


def test_an_inactive_user_is_none():
  with patch.object(
    oauth.api_key_cache,
    "get_cached_api_key_validation",
    return_value=_payload(is_active=False),
  ):
    assert oauth.cached_oauth_identity(TOKEN) is None


def test_an_api_key_entry_under_the_same_digest_shape_is_not_an_oauth_identity():
  """The cache is shared with API keys; only an entry the OAuth validator
  wrote (it carries the token id) counts."""
  entry = _payload()
  del entry["user_data"]["oauth_token_id"]
  with patch.object(
    oauth.api_key_cache, "get_cached_api_key_validation", return_value=entry
  ):
    assert oauth.cached_oauth_identity(TOKEN) is None


def test_a_non_oauth_bearer_never_reaches_the_cache():
  with patch.object(oauth.api_key_cache, "get_cached_api_key_validation") as lookup:
    assert oauth.cached_oauth_identity("eyJhbGciOi.jwt.token") is None
    assert oauth.cached_oauth_identity("rfso" + "a" * 300) is None
  lookup.assert_not_called()


def test_a_cache_failure_is_none():
  with patch.object(
    oauth.api_key_cache,
    "get_cached_api_key_validation",
    side_effect=RuntimeError("valkey down"),
  ):
    assert oauth.cached_oauth_identity(TOKEN) is None
