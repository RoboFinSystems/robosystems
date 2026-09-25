"""A repository grant follows its subscription through every Stripe change.

Against a real Postgres session. Access to a shared repository reads the
grant (`UserRepository`), not the billing row, so a subscription that ends,
lapses or comes back on Stripe's side must move the grant and its credits.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest

from robosystems.dagster.jobs.billing import (
  _handle_subscription_deleted,
  _handle_subscription_updated,
  allocate_user_repository_credits,
)
from robosystems.models.core import Graph, User
from robosystems.models.core.billing import BillingSubscription
from robosystems.models.core.user.user_repository import (
  RepositoryAccessLevel,
  RepositoryType,
  UserRepository,
)
from robosystems.operations.billing.repository_subscriptions import (
  cancel_repository_subscription,
)

pytestmark = pytest.mark.unit

PROVIDER = "robosystems.operations.providers.payment_provider.get_payment_provider"


def _member(session, password_hash: str) -> User:
  user = User(
    email=f"repo+{uuid4().hex[:8]}@example.com",
    name="Repository Member",
    password_hash=password_hash,
  )
  session.add(user)
  session.commit()
  session.refresh(user)
  return user


def _subscribed(session, test_user, test_org):
  if Graph.get_by_id("sec", session) is None:
    Graph.create(
      graph_id="sec",
      org_id=None,
      graph_name="SEC",
      graph_type="repository",
      session=session,
    )
  member = _member(session, test_user.password_hash)
  subscription = BillingSubscription.create_subscription(
    org_id=test_org.id,
    resource_type="repository",
    resource_id="sec",
    plan_name="sec-starter",
    base_price_cents=4900,
    session=session,
    user_id=member.id,
  )
  subscription.activate(session)
  subscription.stripe_subscription_id = f"sub_test_{uuid4().hex[:10]}"
  session.commit()
  grant = UserRepository.create_access(
    user_id=member.id,
    repository_type=RepositoryType.SEC,
    repository_name="sec",
    access_level=RepositoryAccessLevel.READ,
    repository_plan="sec-starter",
    session=session,
    monthly_credits=100,
  )
  return member, subscription, grant


def _event(subscription, status: str, *, cancel_at_period_end=False, period_end=None):
  now = datetime.now(UTC)
  end = period_end or now + timedelta(days=20)
  return {
    "id": subscription.stripe_subscription_id,
    "object": "subscription",
    "status": status,
    "cancel_at_period_end": cancel_at_period_end,
    "metadata": {},
    "items": {
      "object": "list",
      "data": [
        {
          "id": "si_test_1",
          "current_period_start": int((end - timedelta(days=30)).timestamp()),
          "current_period_end": int(end.timestamp()),
        }
      ],
    },
  }


def _has_access(session, member) -> bool:
  session.expire_all()
  return UserRepository.user_has_access(member.id, "sec", session)


class TestStripeSideEnds:
  async def test_a_portal_cancel_ends_access_when_the_subscription_is_deleted(
    self, test_db, test_user, test_org
  ):
    member, subscription, grant = _subscribed(test_db, test_user, test_org)
    await _handle_subscription_updated(
      _event(subscription, "active", cancel_at_period_end=True), test_db, MagicMock()
    )
    test_db.refresh(grant)
    assert grant.expires_at is not None
    assert _has_access(test_db, member)

    past = datetime.now(UTC) - timedelta(minutes=1)
    await _handle_subscription_deleted(
      _event(subscription, "canceled", period_end=past), test_db, MagicMock()
    )
    assert not _has_access(test_db, member)

  async def test_an_immediate_stripe_deletion_revokes_access(
    self, test_db, test_user, test_org
  ):
    member, subscription, grant = _subscribed(test_db, test_user, test_org)
    await _handle_subscription_deleted(
      _event(subscription, "canceled"), test_db, MagicMock()
    )
    test_db.refresh(grant)
    assert not _has_access(test_db, member)
    assert grant.user_credits.is_active is False


class TestFailedPayments:
  async def test_past_due_keeps_access_while_stripe_retries(
    self, test_db, test_user, test_org
  ):
    member, subscription, _grant = _subscribed(test_db, test_user, test_org)
    await _handle_subscription_updated(
      _event(subscription, "past_due"), test_db, MagicMock()
    )
    assert _has_access(test_db, member)

  async def test_unpaid_suspends_access_and_a_payment_restores_it(
    self, test_db, test_user, test_org
  ):
    member, subscription, grant = _subscribed(test_db, test_user, test_org)
    await _handle_subscription_updated(
      _event(subscription, "unpaid"), test_db, MagicMock()
    )
    test_db.refresh(grant)
    assert not _has_access(test_db, member)
    assert grant.user_credits.is_active is False

    await _handle_subscription_updated(
      _event(subscription, "active"), test_db, MagicMock()
    )
    test_db.refresh(grant)
    assert _has_access(test_db, member)
    assert grant.user_credits.is_active is True

  async def test_a_payment_never_restores_a_grant_revoked_for_another_reason(
    self, test_db, test_user, test_org
  ):
    member, subscription, grant = _subscribed(test_db, test_user, test_org)
    grant.revoke_access(test_db, reason="org_offboarding")
    await _handle_subscription_updated(
      _event(subscription, "active"), test_db, MagicMock()
    )
    assert not _has_access(test_db, member)


class TestReactivation:
  async def test_a_portal_reactivation_clears_the_pending_expiry(
    self, test_db, test_user, test_org
  ):
    member, subscription, grant = _subscribed(test_db, test_user, test_org)
    with patch(PROVIDER, return_value=MagicMock()):
      cancel_repository_subscription(
        subscription, session=test_db, actor_user_id=member.id, immediate=False
      )
    test_db.refresh(grant)
    assert grant.expires_at is not None

    with patch(
      "robosystems.dagster.jobs.billing._stripe_confirms_reactivation",
      return_value=True,
    ):
      await _handle_subscription_updated(
        _event(subscription, "active"), test_db, MagicMock()
      )
    test_db.refresh(grant)
    assert grant.expires_at is None
    assert _has_access(test_db, member)


class TestCreditAllocation:
  def test_a_grant_that_ran_out_earns_no_more_credits(
    self, test_db, test_user, test_org
  ):
    _member_, _subscription, grant = _subscribed(test_db, test_user, test_org)
    pool = grant.user_credits
    grant.expires_at = datetime.now(UTC) - timedelta(days=1)
    pool.next_allocation_date = datetime.now(UTC) - timedelta(days=1)
    pool.last_allocation_date = datetime.now(UTC) - timedelta(days=40)
    test_db.commit()
    due_before = pool.next_allocation_date

    @contextmanager
    def session():
      yield test_db

    db = MagicMock()
    db.get_session = session
    from dagster import build_op_context

    allocate_user_repository_credits(build_op_context(), db, {})

    test_db.refresh(pool)
    assert pool.next_allocation_date == due_before


def _resubscribe(session, member, test_org):
  """A second subscription to the same repository, provisioned the way the
  provisioning service does it: the grant reactivated, then reconciled."""
  from robosystems.operations.billing.repository_subscriptions import (
    reconcile_repository_grant,
  )

  newer = BillingSubscription.create_subscription(
    org_id=test_org.id,
    resource_type="repository",
    resource_id="sec",
    plan_name="sec-starter",
    base_price_cents=4900,
    session=session,
    user_id=member.id,
  )
  newer.activate(session)
  newer.stripe_subscription_id = f"sub_test_{uuid4().hex[:10]}"
  session.commit()
  grant = UserRepository.get_by_user_and_repository(member.id, "sec", session)
  grant.is_active = True
  session.commit()
  reconcile_repository_grant(newer, session)
  return newer


class TestResubscribe:
  async def test_the_old_subscriptions_late_end_leaves_the_new_one_alone(
    self, test_db, test_user, test_org
  ):
    member, older, _grant = _subscribed(test_db, test_user, test_org)
    with patch(PROVIDER, return_value=MagicMock()):
      cancel_repository_subscription(
        older, session=test_db, actor_user_id=member.id, immediate=False
      )
    _resubscribe(test_db, member, test_org)
    assert _has_access(test_db, member)

    past = datetime.now(UTC) - timedelta(minutes=1)
    await _handle_subscription_deleted(
      _event(older, "canceled", period_end=past), test_db, MagicMock()
    )
    assert _has_access(test_db, member)

  async def test_a_resubscribe_restores_access_and_credits_an_ended_one_took(
    self, test_db, test_user, test_org
  ):
    member, older, grant = _subscribed(test_db, test_user, test_org)
    await _handle_subscription_deleted(_event(older, "canceled"), test_db, MagicMock())
    assert not _has_access(test_db, member)

    _resubscribe(test_db, member, test_org)
    test_db.refresh(grant)
    assert _has_access(test_db, member)
    assert grant.expires_at is None
    assert grant.user_credits.is_active is True
