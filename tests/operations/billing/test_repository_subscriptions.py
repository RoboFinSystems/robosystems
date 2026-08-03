"""Tests for repository subscription cancellation.

The invariant under test: the payment provider is the first thing to change and
the gate on everything else. If it refuses, the customer is still being billed,
so local state and access must be untouched.
"""

from unittest.mock import MagicMock, patch

import pytest

from robosystems.models.core import BillingSubscription, Graph, OrgRole, OrgUser, User
from robosystems.models.core.user.user_repository import (
  RepositoryAccessLevel,
  RepositoryType,
  UserRepository,
)
from robosystems.operations.billing import (
  ProviderCancellationError,
  cancel_repository_subscription,
  cancel_user_repository_subscriptions,
)

PROVIDER = "robosystems.operations.providers.payment_provider.get_payment_provider"


@pytest.fixture
def subscribed_member(test_db, test_user, test_org):
  """An org member with an active, Stripe-linked repository subscription."""
  from uuid import uuid4

  if Graph.get_by_id("sec", test_db) is None:
    Graph.create(
      graph_id="sec",
      org_id=None,
      graph_name="SEC",
      graph_type="repository",
      session=test_db,
    )

  member = User(
    email=f"subscriber+{uuid4().hex[:8]}@example.com",
    name="Subscriber",
    password_hash=test_user.password_hash,
  )
  test_db.add(member)
  test_db.commit()
  OrgUser.create(
    org_id=test_org.id, user_id=member.id, role=OrgRole.MEMBER, session=test_db
  )

  subscription = BillingSubscription.create_subscription(
    org_id=test_org.id,
    resource_type="repository",
    resource_id="sec",
    plan_name="sec-starter",
    base_price_cents=4900,
    session=test_db,
    user_id=member.id,
  )
  subscription.activate(test_db)
  subscription.stripe_subscription_id = "sub_stripe_test"
  test_db.commit()

  access = UserRepository.create_access(
    user_id=member.id,
    repository_type=RepositoryType.SEC,
    repository_name="sec",
    access_level=RepositoryAccessLevel.READ,
    repository_plan="sec-starter",
    session=test_db,
  )

  return member, subscription, access


class TestCancelRepositorySubscription:
  def test_provider_failure_changes_nothing(self, test_db, subscribed_member):
    member, subscription, access = subscribed_member
    provider = MagicMock()
    provider.cancel_subscription_at_period_end.side_effect = RuntimeError("stripe down")

    with patch(PROVIDER, return_value=provider):
      with pytest.raises(ProviderCancellationError):
        cancel_repository_subscription(
          subscription, session=test_db, actor_user_id=member.id
        )

    test_db.refresh(subscription)
    test_db.refresh(access)
    assert subscription.status == "active"
    assert access.is_active is True

  def test_period_end_cancel_keeps_access_until_the_period_closes(
    self, test_db, subscribed_member
  ):
    member, subscription, access = subscribed_member

    with patch(PROVIDER, return_value=MagicMock()):
      cancel_repository_subscription(
        subscription, session=test_db, actor_user_id=member.id
      )

    test_db.refresh(subscription)
    test_db.refresh(access)
    assert subscription.status == "canceled"
    assert subscription.cancellation_type == "period_end"
    assert access.is_active is True
    # The access record is timezone-aware, the billing row is not.
    assert access.expires_at.replace(tzinfo=None) == subscription.ends_at

  def test_immediate_cancel_revokes_access(self, test_db, subscribed_member):
    member, subscription, access = subscribed_member

    with patch(PROVIDER, return_value=MagicMock()):
      cancel_repository_subscription(
        subscription, session=test_db, actor_user_id=member.id, immediate=True
      )

    test_db.refresh(subscription)
    test_db.refresh(access)
    assert subscription.cancellation_type == "immediate"
    assert access.is_active is False


class TestCancelAllForUser:
  def test_offboarding_cancels_every_live_subscription(
    self, test_db, subscribed_member, test_user
  ):
    member, subscription, access = subscribed_member

    with patch(PROVIDER, return_value=MagicMock()):
      canceled = cancel_user_repository_subscriptions(
        member.id, session=test_db, actor_user_id=test_user.id
      )

    assert canceled == [subscription.id]
    test_db.refresh(subscription)
    test_db.refresh(access)
    assert subscription.status == "canceled"
    assert access.is_active is False

  def test_no_subscriptions_is_a_no_op(self, test_db, test_user):
    assert (
      cancel_user_repository_subscriptions(
        test_user.id, session=test_db, actor_user_id=test_user.id
      )
      == []
    )
