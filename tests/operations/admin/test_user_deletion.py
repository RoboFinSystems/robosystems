"""Tests for administrative user deletion.

The guards matter more than the deletes: this is the one path that removes an
account, and it must refuse while the org still holds anything of value.
"""

from uuid import uuid4

import pytest

from robosystems.models.core import (
  BillingAuditLog,
  BillingSubscription,
  Graph,
  OAuthClient,
  OAuthGrant,
  OAuthToken,
  Org,
  OrgRole,
  OrgType,
  OrgUser,
  User,
)
from robosystems.models.core.billing.audit_log import BillingEventType
from robosystems.models.core.user.user_repository import (
  RepositoryAccessLevel,
  RepositoryType,
  UserRepository,
)
from robosystems.operations.admin import (
  UserDeletionBlocked,
  execute_user_deletion,
  plan_user_deletion,
)


def _create_user(session, password_hash: str) -> User:
  suffix = uuid4().hex[:8]
  user = User(
    email=f"deletable+{suffix}@example.com",
    name="Deletable User",
    password_hash=password_hash,
  )
  session.add(user)
  session.commit()
  session.refresh(user)
  return user


def _personal_org(session, user: User) -> Org:
  org = Org.create(
    name=f"Personal {uuid4().hex[:6]}",
    org_type=OrgType.PERSONAL,
    session=session,
  )
  OrgUser.create(org_id=org.id, user_id=user.id, role=OrgRole.OWNER, session=session)
  return org


def _ensure_sec_repository(session) -> None:
  if Graph.get_by_id("sec", session) is None:
    Graph.create(
      graph_id="sec",
      org_id=None,
      graph_name="SEC",
      graph_type="repository",
      session=session,
    )


class TestDeletionGuards:
  def test_live_graph_blocks_deletion(self, test_db, test_user):
    user = _create_user(test_db, test_user.password_hash)
    org = _personal_org(test_db, user)
    Graph.create(
      graph_id=f"kg{uuid4().hex[:16]}",
      org_id=org.id,
      graph_name="Live Graph",
      graph_type="generic",
      session=test_db,
    )

    plan = plan_user_deletion(user.id, test_db)

    assert not plan.can_delete
    assert [b.code for b in plan.blockers] == ["org_has_live_graphs"]
    with pytest.raises(UserDeletionBlocked):
      execute_user_deletion(user.id, test_db)

  def test_subscription_in_force_blocks_deletion(self, test_db, test_user):
    user = _create_user(test_db, test_user.password_hash)
    org = _personal_org(test_db, user)
    subscription = BillingSubscription.create_subscription(
      org_id=org.id,
      resource_type="repository",
      resource_id="sec",
      plan_name="sec-starter",
      base_price_cents=4900,
      session=test_db,
      user_id=user.id,
    )
    subscription.activate(test_db)

    plan = plan_user_deletion(user.id, test_db)

    assert "org_has_active_subscriptions" in [b.code for b in plan.blockers]

  def test_active_repository_access_blocks_deletion(self, test_db, test_user):
    _ensure_sec_repository(test_db)
    user = _create_user(test_db, test_user.password_hash)
    _personal_org(test_db, user)
    UserRepository.create_access(
      user_id=user.id,
      repository_type=RepositoryType.SEC,
      repository_name="sec",
      access_level=RepositoryAccessLevel.READ,
      repository_plan="sec-starter",
      session=test_db,
    )

    plan = plan_user_deletion(user.id, test_db)

    assert "active_repository_access" in [b.code for b in plan.blockers]

  def test_owner_of_shared_org_blocks_deletion(self, test_db, test_user):
    owner = _create_user(test_db, test_user.password_hash)
    org = _personal_org(test_db, owner)
    colleague = _create_user(test_db, test_user.password_hash)
    OrgUser.create(
      org_id=org.id, user_id=colleague.id, role=OrgRole.MEMBER, session=test_db
    )

    plan = plan_user_deletion(owner.id, test_db)

    assert "org_has_other_members" in [b.code for b in plan.blockers]


class TestDeletionExecution:
  def test_clean_account_is_deleted_and_email_freed(self, test_db, test_user):
    user = _create_user(test_db, test_user.password_hash)
    org_id = _personal_org(test_db, user).id
    email = user.email

    plan = execute_user_deletion(user.id, test_db, actor="admin:test")

    assert plan.orgs_to_delete == [org_id]
    assert User.get_by_email(email, test_db) is None
    assert Org.get_by_id(org_id, test_db) is None

  def test_oauth_consents_go_with_the_account(self, test_db, test_user):
    """A grant is a FK into users with no cascade: the sweep must take the
    tokens and grants first, or the account row cannot be deleted at all."""
    user = _create_user(test_db, test_user.password_hash)
    _personal_org(test_db, user)
    client, _ = OAuthClient.register_preregistered(
      client_name="Claude",
      redirect_uris=["https://claude.ai/api/mcp/auth_callback"],
      confidential=False,
      session=test_db,
    )
    grant = OAuthGrant.create(
      user_id=str(user.id),
      oauth_client_id=str(client.id),
      graph_id="kg19fb490f76871d22e835",
      resource="https://api.test.example/v1/mcp",
      scope="mcp",
      session=test_db,
    )
    OAuthToken.mint_pair(grant_id=str(grant.id), user_id=str(user.id), session=test_db)
    # Captured before the rows go: a deleted instance cannot refresh its id.
    user_id, grant_id, client_id = str(user.id), str(grant.id), str(client.id)

    plan = execute_user_deletion(user_id, test_db, actor="admin:test")

    assert plan.removals["oauth_grants"] == 1
    assert User.get_by_id(user_id, test_db) is None
    assert OAuthGrant.get_by_id(grant_id, test_db) is None
    assert test_db.query(OAuthToken).filter_by(user_id=user_id).count() == 0
    # The client is not the user's — it outlives every grant it held.
    assert OAuthClient.get_by_id(client_id, test_db) is not None

  def test_invited_member_leaves_the_org_standing(self, test_db, test_user):
    """Deleting a member of a shared org removes them, not the organization."""
    owner = _create_user(test_db, test_user.password_hash)
    org = _personal_org(test_db, owner)
    member = _create_user(test_db, test_user.password_hash)
    OrgUser.create(
      org_id=org.id, user_id=member.id, role=OrgRole.MEMBER, session=test_db
    )

    plan = execute_user_deletion(member.id, test_db)

    assert plan.orgs_retained == [org.id]
    assert Org.get_by_id(org.id, test_db) is not None
    assert OrgUser.get_by_org_and_user(org.id, member.id, test_db) is None
    assert User.get_by_id(owner.id, test_db) is not None

  def test_billing_audit_history_survives_the_account(self, test_db, test_user):
    """Retention is a compliance requirement — the actor is de-referenced,
    the record is not deleted."""
    owner = _create_user(test_db, test_user.password_hash)
    org = _personal_org(test_db, owner)
    member = _create_user(test_db, test_user.password_hash)
    OrgUser.create(
      org_id=org.id, user_id=member.id, role=OrgRole.MEMBER, session=test_db
    )
    entry = BillingAuditLog.log_event(
      session=test_db,
      event_type=BillingEventType.SUBSCRIPTION_CANCELED,
      org_id=org.id,
      description="Canceled during off-boarding",
      actor_type="user",
      actor_user_id=member.id,
    )
    entry_id = entry.id

    execute_user_deletion(member.id, test_db)

    retained = test_db.query(BillingAuditLog).filter_by(id=entry_id).first()
    assert retained is not None
    assert retained.actor_user_id is None

  def test_org_with_billing_history_is_retained(self, test_db, test_user):
    """A canceled subscription is financial history — the org outlives its
    last member so the record keeps its home."""
    user = _create_user(test_db, test_user.password_hash)
    org = _personal_org(test_db, user)
    subscription = BillingSubscription.create_subscription(
      org_id=org.id,
      resource_type="repository",
      resource_id="sec",
      plan_name="sec-starter",
      base_price_cents=4900,
      session=test_db,
      user_id=user.id,
    )
    subscription.status = "canceled"
    test_db.commit()

    plan = execute_user_deletion(user.id, test_db)

    assert plan.orgs_retained == [org.id]
    assert Org.get_by_id(org.id, test_db) is not None
    test_db.refresh(subscription)
    assert subscription.user_id is None
