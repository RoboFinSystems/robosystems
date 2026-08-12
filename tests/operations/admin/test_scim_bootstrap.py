"""Tests for SCIM provisioning bootstrap."""

from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import pytest

from robosystems.config import env
from robosystems.models.core import Org, OrgLimits, OrgType, ScimToken
from robosystems.operations.admin import (
  OrgBoundaryError,
  OrgNotFoundError,
  bootstrap_scim,
  revoke_scim_token,
)


class TestBootstrapScim:
  def test_creates_enterprise_org_with_limits_and_token(self, test_db):
    result = bootstrap_scim(test_db, org_name="Acme Accounting")

    org = Org.get_by_id(result.org_id, test_db)
    assert org is not None
    assert org.org_type == OrgType.ENTERPRISE
    assert org.name == "Acme Accounting"
    assert OrgLimits.get_by_org_id(org.id, test_db) is not None

    # The raw token validates against the stored hash; only its hash persists.
    token = ScimToken.validate_token(result.raw_token, test_db)
    assert token is not None
    assert token.id == result.scim_token_id
    assert token.org_id == org.id

  def test_reuses_existing_org(self, test_db):
    org = Org.create(name="Existing", org_type=OrgType.ENTERPRISE, session=test_db)

    result = bootstrap_scim(test_db, org_id=org.id, token_name="second-token")

    assert result.org_id == org.id
    # No duplicate org created.
    assert len([o for o in Org.get_all(test_db) if o.name == "Existing"]) == 1
    assert ScimToken.validate_token(result.raw_token, test_db) is not None

  def test_unknown_org_raises(self, test_db):
    with pytest.raises(OrgNotFoundError):
      bootstrap_scim(test_db, org_id="org_does_not_exist")

  def test_two_tokens_can_coexist_for_rotation(self, test_db):
    first = bootstrap_scim(test_db, org_name="Rotation Co")
    second = bootstrap_scim(test_db, org_id=first.org_id, token_name="rotated")

    # Both live during the swap.
    assert ScimToken.validate_token(first.raw_token, test_db) is not None
    assert ScimToken.validate_token(second.raw_token, test_db) is not None

  def test_token_expires_by_default(self, test_db):
    """Every token expires — there is deliberately no non-expiring mint."""
    result = bootstrap_scim(test_db, org_name="Expiry Co")

    delta = result.expires_at - datetime.now(UTC)
    assert timedelta(days=364) < delta <= timedelta(days=365)
    # Still valid today, and the stored row carries the expiry.
    token = ScimToken.validate_token(result.raw_token, test_db)
    assert token is not None
    assert token.expires_at is not None

  def test_custom_expiry_honored(self, test_db):
    result = bootstrap_scim(test_db, org_name="Short Co", expires_in_days=30)

    delta = result.expires_at - datetime.now(UTC)
    assert timedelta(days=29) < delta <= timedelta(days=30)


class TestOrgBoundaryPin:
  """Once ENTERPRISE_ORG_ID pins the deployment, bootstrap can only mint
  tokens for that org — no new-org creation, no other-org targeting."""

  def test_pinned_refuses_org_name_creation(self, test_db):
    org = Org.create(name="Pinned", org_type=OrgType.ENTERPRISE, session=test_db)

    with patch.object(env, "ENTERPRISE_ORG_ID", str(org.id)):
      with pytest.raises(OrgBoundaryError):
        bootstrap_scim(test_db, org_name="Second Org")

  def test_pinned_refuses_other_org_id(self, test_db):
    org = Org.create(name="Pinned2", org_type=OrgType.ENTERPRISE, session=test_db)
    other = Org.create(name="Other", org_type=OrgType.ENTERPRISE, session=test_db)

    with patch.object(env, "ENTERPRISE_ORG_ID", str(org.id)):
      with pytest.raises(OrgBoundaryError):
        bootstrap_scim(test_db, org_id=other.id)

  def test_pinned_allows_matching_org_id(self, test_db):
    org = Org.create(name="Pinned3", org_type=OrgType.ENTERPRISE, session=test_db)

    with patch.object(env, "ENTERPRISE_ORG_ID", str(org.id)):
      result = bootstrap_scim(test_db, org_id=org.id, token_name="pinned-token")

    assert result.org_id == org.id
    assert ScimToken.validate_token(result.raw_token, test_db) is not None


class TestRevokeScimToken:
  def test_revoke_makes_token_invalid(self, test_db):
    result = bootstrap_scim(test_db, org_name="Revoke Co")
    assert ScimToken.validate_token(result.raw_token, test_db) is not None

    assert revoke_scim_token(test_db, result.scim_token_id) is True
    assert ScimToken.validate_token(result.raw_token, test_db) is None

  def test_revoke_unknown_returns_false(self, test_db):
    assert revoke_scim_token(test_db, "scim_nope") is False
