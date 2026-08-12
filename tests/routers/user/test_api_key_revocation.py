"""User-facing API-key revocation must fail loud when revocation is partial.

Same contract as the SCIM deactivation path: the key row flips in the DB
either way, but a surviving cached validation entry keeps the key usable
until its TTL — so the endpoint must return a retryable failure rather than
"revoked successfully", and the retry must converge (the lookup includes
inactive keys, and deactivate re-asserts the cache invalidation).
"""

from unittest.mock import patch

from robosystems.middleware.auth.jwt import create_jwt_token
from robosystems.models.core.user.user_api_key import UserAPIKey


def _auth(user):
  return {"Authorization": f"Bearer {create_jwt_token(user.id)}"}


def _is_active(session, key_id: str) -> bool:
  # Re-query rather than refresh: the endpoint's error path closes the shared
  # test session, detaching instances created before the request.
  row = session.query(UserAPIKey).filter(UserAPIKey.id == key_id).one()
  return bool(row.is_active)


class TestRevocationCompleteness:
  def test_stale_cache_is_503_and_retry_converges(self, client, test_user, test_db):
    key, _plain = UserAPIKey.create(user_id=test_user.id, name="k", session=test_db)
    key_id = str(key.id)

    with patch.object(UserAPIKey, "invalidate_cache", return_value=False):
      resp = client.delete(f"/v1/user/api-keys/{key_id}", headers=_auth(test_user))
    assert resp.status_code == 503

    # The DB half applied regardless.
    assert _is_active(test_db, key_id) is False

    # The retry finds the now-inactive key and re-asserts the invalidation.
    resp = client.delete(f"/v1/user/api-keys/{key_id}", headers=_auth(test_user))
    assert resp.status_code == 200
    assert resp.json()["success"] is True

  def test_clean_revocation_succeeds(self, client, test_user, test_db):
    key, _plain = UserAPIKey.create(user_id=test_user.id, name="k2", session=test_db)
    key_id = str(key.id)

    resp = client.delete(f"/v1/user/api-keys/{key_id}", headers=_auth(test_user))
    assert resp.status_code == 200
    assert _is_active(test_db, key_id) is False
