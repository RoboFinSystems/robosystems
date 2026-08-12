"""End-to-end tests for the SCIM /scim/v2 surface.

Uses the real mounted router (SCIM_ENABLED=true in pytest.ini) through the
shared TestClient, so these exercise the bearer dependency, org-scoping,
envelope shapes, and the deactivate kill-switch together. A ScimToken is
minted in the test DB and presented as the bearer.
"""

import uuid
from unittest.mock import patch

import pytest

from robosystems.models.core import (
  Org,
  OrgRole,
  OrgType,
  OrgUser,
  ScimToken,
  User,
)


@pytest.fixture
def enterprise_org(test_db):
  org = Org.create(
    name=f"Enterprise {uuid.uuid4().hex[:6]}",
    org_type=OrgType.ENTERPRISE,
    session=test_db,
  )
  return org


@pytest.fixture
def scim_auth(test_db, enterprise_org):
  _token, raw = ScimToken.create(enterprise_org.id, "test-scim", test_db)
  return {"Authorization": f"Bearer {raw}"}


def _make_member(test_db, org, *, email=None, external_id="okta-ext", active=True):
  user = User.create(
    email=email or f"member+{uuid.uuid4().hex[:8]}@customer.example",
    name="Member",
    password_hash=None,
    session=test_db,
    external_id=external_id,
  )
  OrgUser.create(org.id, user.id, OrgRole.MEMBER, test_db)
  if not active:
    user.deactivate(test_db)
  return user


class TestScimAuth:
  def test_no_token_is_401_scim_envelope(self, client):
    resp = client.get("/scim/v2/Users")
    assert resp.status_code == 401
    body = resp.json()["detail"]
    assert "urn:ietf:params:scim:api:messages:2.0:Error" in body["schemas"]

  def test_bogus_token_is_401(self, client):
    resp = client.get(
      "/scim/v2/Users", headers={"Authorization": "Bearer rfssdeadbeef"}
    )
    assert resp.status_code == 401

  def test_user_jwt_is_not_accepted_at_scim(self, client, test_user):
    # A normal auth token must never authenticate against the SCIM surface.
    from robosystems.middleware.auth.jwt import create_jwt_token

    jwt = create_jwt_token(test_user.id)
    resp = client.get("/scim/v2/Users", headers={"Authorization": f"Bearer {jwt}"})
    assert resp.status_code == 401


class TestConformance:
  def test_service_provider_config(self, client, scim_auth):
    resp = client.get("/scim/v2/ServiceProviderConfig", headers=scim_auth)
    assert resp.status_code == 200
    body = resp.json()
    assert body["patch"]["supported"] is True
    assert body["filter"]["supported"] is True
    assert body["changePassword"]["supported"] is False

  def test_resource_types_and_schemas(self, client, scim_auth):
    assert client.get("/scim/v2/ResourceTypes", headers=scim_auth).status_code == 200
    assert client.get("/scim/v2/Schemas", headers=scim_auth).status_code == 200


class TestUsersCrud:
  def test_create_user_provisions_passwordless_into_org(
    self, client, scim_auth, enterprise_org, test_db
  ):
    email = f"new+{uuid.uuid4().hex[:8]}@customer.example"
    resp = client.post(
      "/scim/v2/Users",
      headers=scim_auth,
      json={
        "schemas": ["urn:ietf:params:scim:schemas:core:2.0:User"],
        "userName": email,
        "externalId": "okta-99",
        "name": {"givenName": "New", "familyName": "Hire"},
        "active": True,
      },
    )
    assert resp.status_code == 201
    body = resp.json()
    assert body["userName"] == email
    assert body["externalId"] == "okta-99"
    assert body["active"] is True
    assert body["id"]

    user = User.get_by_email(email, test_db)
    assert user is not None
    assert user.password_hash is None
    assert user.email_verified is True
    assert OrgUser.get_by_org_and_user(enterprise_org.id, user.id, test_db) is not None

  def test_duplicate_email_is_409_uniqueness(
    self, client, scim_auth, test_db, enterprise_org
  ):
    existing = _make_member(test_db, enterprise_org)
    resp = client.post(
      "/scim/v2/Users",
      headers=scim_auth,
      json={"userName": existing.email, "externalId": "dup"},
    )
    assert resp.status_code == 409
    assert resp.json()["detail"]["scimType"] == "uniqueness"

  def test_filter_by_username_finds_member(
    self, client, scim_auth, test_db, enterprise_org
  ):
    member = _make_member(test_db, enterprise_org)
    resp = client.get(
      "/scim/v2/Users",
      params={"filter": f'userName eq "{member.email}"'},
      headers=scim_auth,
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["totalResults"] == 1
    assert body["Resources"][0]["userName"] == member.email

  def test_unsupported_filter_is_empty_not_error(self, client, scim_auth):
    resp = client.get(
      "/scim/v2/Users",
      params={"filter": 'emails.value co "example.com"'},
      headers=scim_auth,
    )
    assert resp.status_code == 200
    assert resp.json()["totalResults"] == 0

  def test_get_user_by_id(self, client, scim_auth, test_db, enterprise_org):
    member = _make_member(test_db, enterprise_org)
    resp = client.get(f"/scim/v2/Users/{member.id}", headers=scim_auth)
    assert resp.status_code == 200
    assert resp.json()["id"] == member.id


class TestOrgScoping:
  def test_cannot_read_user_in_another_org(self, client, scim_auth, test_db):
    """A token for org A must not resolve a user in org B — 404, no leak."""
    other_org = Org.create(name="Other", org_type=OrgType.ENTERPRISE, session=test_db)
    foreign = _make_member(test_db, other_org)

    resp = client.get(f"/scim/v2/Users/{foreign.id}", headers=scim_auth)
    assert resp.status_code == 404

  def test_list_only_returns_own_org_members(
    self, client, scim_auth, test_db, enterprise_org
  ):
    mine = _make_member(test_db, enterprise_org)
    other_org = Org.create(name="Other2", org_type=OrgType.ENTERPRISE, session=test_db)
    _make_member(test_db, other_org)

    resp = client.get("/scim/v2/Users", headers=scim_auth)
    ids = {r["id"] for r in resp.json()["Resources"]}
    assert mine.id in ids
    assert len(ids) == 1


class TestDeactivation:
  def test_patch_active_false_kills_sessions_and_keys(
    self, client, scim_auth, test_db, enterprise_org
  ):
    from robosystems.models.core.user.user_api_key import UserAPIKey

    member = _make_member(test_db, enterprise_org)
    UserAPIKey.create(user_id=member.id, name="k", session=test_db)
    before = member.session_version or 0

    resp = client.patch(
      f"/scim/v2/Users/{member.id}",
      headers=scim_auth,
      json={
        "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
        "Operations": [{"op": "replace", "path": "active", "value": False}],
      },
    )
    assert resp.status_code == 200
    assert resp.json()["active"] is False

    test_db.refresh(member)
    assert member.is_active is False
    assert (member.session_version or 0) > before
    assert UserAPIKey.get_active_by_user_id(str(member.id), test_db) == []

  def test_patch_active_tolerates_entra_string_false(
    self, client, scim_auth, test_db, enterprise_org
  ):
    """Entra (without the compat flag) sends capitalized op and "False" —
    the most important operation must still deactivate."""
    member = _make_member(test_db, enterprise_org)
    resp = client.patch(
      f"/scim/v2/Users/{member.id}",
      headers=scim_auth,
      json={"Operations": [{"op": "Replace", "path": "active", "value": "False"}]},
    )
    assert resp.status_code == 200
    test_db.refresh(member)
    assert member.is_active is False

  def test_delete_deactivates_idempotently(
    self, client, scim_auth, test_db, enterprise_org
  ):
    member = _make_member(test_db, enterprise_org)
    assert (
      client.delete(f"/scim/v2/Users/{member.id}", headers=scim_auth).status_code == 204
    )
    test_db.refresh(member)
    assert member.is_active is False
    # Idempotent replay.
    assert (
      client.delete(f"/scim/v2/Users/{member.id}", headers=scim_auth).status_code == 204
    )

  def test_patch_reactivate(self, client, scim_auth, test_db, enterprise_org):
    member = _make_member(test_db, enterprise_org, active=False)
    resp = client.patch(
      f"/scim/v2/Users/{member.id}",
      headers=scim_auth,
      json={"Operations": [{"op": "replace", "path": "active", "value": True}]},
    )
    assert resp.status_code == 200
    test_db.refresh(member)
    assert member.is_active is True

  def test_incomplete_deactivation_is_503_and_retryable(
    self, client, scim_auth, test_db, enterprise_org
  ):
    """The offboarding kill switch must fail loud, not report success.

    When the revocation side effects don't all take (here: the auth-cache
    invalidation), the IdP must see a retryable failure — and the retry must
    re-run those side effects even though the DB flag already flipped.
    """
    member = _make_member(test_db, enterprise_org)
    deactivate_body = {
      "Operations": [{"op": "replace", "path": "active", "value": False}]
    }

    with patch.object(User, "_invalidate_auth_cache", return_value=False):
      resp = client.patch(
        f"/scim/v2/Users/{member.id}", headers=scim_auth, json=deactivate_body
      )
    assert resp.status_code == 503

    # The DB half still applied — the account is inactive either way.
    test_db.refresh(member)
    assert member.is_active is False
    version_after_failure = member.session_version or 0

    # The IdP retry re-asserts the side effects and now reports success.
    resp = client.patch(
      f"/scim/v2/Users/{member.id}", headers=scim_auth, json=deactivate_body
    )
    assert resp.status_code == 200
    test_db.refresh(member)
    assert (member.session_version or 0) > version_after_failure
