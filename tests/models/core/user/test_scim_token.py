"""ScimToken model invariants: every token expires, enforced in the model.

The admin bootstrap layer bounds lifetimes too, but the invariant lives here
so no direct caller can mint a non-expiring bearer, and a pre-invariant NULL
row cannot keep authenticating forever.
"""

import uuid
from datetime import UTC, datetime, timedelta

from robosystems.models.core import Org, OrgType, ScimToken
from robosystems.models.core.user.scim_token import DEFAULT_TOKEN_LIFETIME_DAYS


def _org(test_db):
  return Org.create(
    name=f"Token Org {uuid.uuid4().hex[:6]}",
    org_type=OrgType.ENTERPRISE,
    session=test_db,
  )


class TestExpiryInvariant:
  def test_create_without_expiry_gets_the_default_lifetime(self, test_db):
    token, raw = ScimToken.create(_org(test_db).id, "default-lifetime", test_db)

    assert token.expires_at is not None
    expires = token.expires_at
    if expires.tzinfo is None:
      expires = expires.replace(tzinfo=UTC)
    delta = expires - datetime.now(UTC)
    assert (
      timedelta(days=DEFAULT_TOKEN_LIFETIME_DAYS - 1)
      < delta
      <= timedelta(days=DEFAULT_TOKEN_LIFETIME_DAYS)
    )
    assert ScimToken.validate_token(raw, test_db) is not None

  def test_explicit_expiry_is_honored(self, test_db):
    expires_at = datetime.now(UTC) + timedelta(days=30)
    token, _raw = ScimToken.create(
      _org(test_db).id, "short", test_db, expires_at=expires_at
    )
    assert token.expires_at is not None

  def test_null_expiry_row_is_refused(self, test_db):
    """A row predating the invariant must not authenticate forever."""
    token, raw = ScimToken.create(_org(test_db).id, "legacy", test_db)
    token.expires_at = None
    test_db.commit()

    assert ScimToken.validate_token(raw, test_db) is None

  def test_expired_token_is_refused(self, test_db):
    _token, raw = ScimToken.create(
      _org(test_db).id,
      "expired",
      test_db,
      expires_at=datetime.now(UTC) - timedelta(minutes=1),
    )
    assert ScimToken.validate_token(raw, test_db) is None
