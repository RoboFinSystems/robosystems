"""Keep Subscription takes back a cancellation scheduled for the end of the
period, in the database, with the same authority that cancels."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import patch
from uuid import uuid4

import pytest
from fastapi import HTTPException

from robosystems.models.core import OrgRole, OrgUser
from robosystems.models.core.billing import BillingSubscription
from robosystems.routers.billing.subscriptions import resume_subscription

pytestmark = pytest.mark.integration

_ROUTE = "robosystems.routers.billing.subscriptions"


def _subscription(session, org, user, *, resource_type="graph", immediate=False):
  subscription = BillingSubscription.create_subscription(
    org_id=org.id,
    resource_type=resource_type,
    resource_id="sec" if resource_type == "repository" else f"kg{uuid4().hex[:18]}",
    plan_name="starter" if resource_type == "repository" else "ladybug-standard",
    base_price_cents=2900,
    session=session,
    user_id=user.id if resource_type == "repository" else None,
  )
  subscription.activate(session)
  subscription.stripe_subscription_id = f"sub_test_{uuid4().hex[:10]}"
  session.commit()
  subscription.cancel(session, immediate=immediate)
  return subscription


async def _resume(session, org, subscription, user, *, stripe_error=None):
  with patch(f"{_ROUTE}.get_payment_provider") as provider:
    modify = provider.return_value.stripe.Subscription.modify
    if stripe_error:
      modify.side_effect = stripe_error
    response = await resume_subscription(
      org_id=org.id,
      subscription_id=subscription.id,
      current_user=user,
      db=session,
      _rate_limit=None,
    )
    return response, modify


@pytest.mark.asyncio
async def test_owner_keeps_a_graph_scheduled_to_end(test_db, test_user, test_org):
  subscription = _subscription(test_db, test_org, test_user)
  assert subscription.status == "canceled" and subscription.ends_at is not None

  response, modify = await _resume(test_db, test_org, subscription, test_user)

  modify.assert_called_once_with(
    subscription.stripe_subscription_id, cancel_at_period_end=False
  )
  test_db.refresh(subscription)
  assert response.status == "active"
  assert subscription.status == "active"
  assert subscription.ends_at is None and subscription.canceled_at is None
  assert subscription.cancellation_type is None


@pytest.mark.asyncio
async def test_an_immediate_cancel_cannot_be_taken_back(test_db, test_user, test_org):
  subscription = _subscription(test_db, test_org, test_user, immediate=True)

  with pytest.raises(HTTPException) as refused:
    await _resume(test_db, test_org, subscription, test_user)

  assert refused.value.status_code == 409


@pytest.mark.asyncio
async def test_a_period_already_over_cannot_be_taken_back(test_db, test_user, test_org):
  subscription = _subscription(test_db, test_org, test_user)
  subscription.ends_at = datetime.now(UTC) - timedelta(minutes=1)
  test_db.commit()

  with pytest.raises(HTTPException) as refused:
    await _resume(test_db, test_org, subscription, test_user)

  assert refused.value.status_code == 409


@pytest.mark.asyncio
async def test_stripe_refusing_changes_nothing(test_db, test_user, test_org):
  subscription = _subscription(test_db, test_org, test_user)

  with pytest.raises(HTTPException) as refused:
    await _resume(
      test_db, test_org, subscription, test_user, stripe_error=RuntimeError("down")
    )

  assert refused.value.status_code == 502
  test_db.refresh(subscription)
  assert subscription.status == "canceled"


@pytest.mark.asyncio
@pytest.mark.parametrize(
  ("resource_type", "role", "owns_seat", "allowed"),
  [
    ("graph", OrgRole.ADMIN, False, False),
    ("graph", OrgRole.MEMBER, False, False),
    ("repository", OrgRole.MEMBER, True, True),
    ("repository", OrgRole.MEMBER, False, False),
    ("repository", OrgRole.ADMIN, False, True),
  ],
  ids=[
    "graph-admin",
    "graph-member",
    "own-seat",
    "another-members-seat",
    "admin-on-a-seat",
  ],
)
async def test_the_authority_that_cancels_is_the_one_that_keeps(
  test_db, test_user, test_org, other_user, resource_type, role, owns_seat, allowed
):
  OrgUser.create(org_id=test_org.id, user_id=other_user.id, role=role, session=test_db)
  holder = other_user if owns_seat else test_user
  subscription = _subscription(test_db, test_org, holder, resource_type=resource_type)

  if allowed:
    response, _ = await _resume(test_db, test_org, subscription, other_user)
    assert response.status == "active"
  else:
    with pytest.raises(HTTPException) as refused:
      await _resume(test_db, test_org, subscription, other_user)
    assert refused.value.status_code == 403
