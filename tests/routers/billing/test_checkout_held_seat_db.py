"""Checkout refuses a repository seat the user already holds, in the database:
a trial taken without a card leaves no card on file, which was the only thing
checkout asked about."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import patch
from uuid import uuid4

import pytest
from fastapi import HTTPException

from robosystems.models.api.billing.checkout import CreateCheckoutRequest
from robosystems.models.core.billing import BillingCustomer, BillingSubscription
from robosystems.routers.billing.checkout import create_checkout_session

pytestmark = pytest.mark.integration

_CHECKOUT = "robosystems.routers.billing.checkout"


def _seat(session, org, user, *, status: str = "active"):
  """A subscription to the SEC repository as provisioning leaves it."""
  seat = BillingSubscription.create_subscription(
    org_id=org.id,
    resource_type="repository",
    resource_id="sec",
    plan_name="starter",
    base_price_cents=2900,
    session=session,
    user_id=user.id,
  )
  seat.activate(session)
  seat.stripe_subscription_id = f"sub_test_{uuid4().hex[:10]}"
  seat.trial_ends_at = datetime.now(UTC) + timedelta(days=14)
  seat.status = status
  session.commit()
  return seat


async def _checkout(session, user):
  with (
    patch(f"{_CHECKOUT}.env.BILLING_ENABLED", True),
    patch(f"{_CHECKOUT}.env.EMAIL_VERIFICATION_ENABLED", False),
    patch(f"{_CHECKOUT}.get_payment_provider") as provider,
  ):
    provider.return_value.create_customer.return_value = "cus_test"
    provider.return_value.create_checkout_session.return_value = {
      "checkout_url": "https://checkout.example/c/cs_test",
      "session_id": "cs_test",
    }
    response = await create_checkout_session(
      CreateCheckoutRequest(
        resource_type="repository",
        plan_name="starter",
        resource_config={"repository_name": "sec"},
      ),
      current_user=user,
      db=session,
      _rate_limit=None,
    )
    return response, provider


def _pending(session, org) -> int:
  return (
    session.query(BillingSubscription)
    .filter_by(org_id=org.id, status="pending_payment")
    .count()
  )


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["active", "provisioning", "past_due", "unpaid"])
async def test_a_seat_in_force_is_not_sold_again(test_db, test_user, test_org, status):
  BillingCustomer.get_or_create(test_org.id, test_db)
  _seat(test_db, test_org, test_user, status=status)

  with pytest.raises(HTTPException) as refused:
    await _checkout(test_db, test_user)

  assert refused.value.status_code == 409
  assert _pending(test_db, test_org) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["canceled", "failed"])
async def test_an_ended_seat_can_be_bought_again(test_db, test_user, test_org, status):
  BillingCustomer.get_or_create(test_org.id, test_db)
  _seat(test_db, test_org, test_user, status=status)

  response, provider = await _checkout(test_db, test_user)

  assert response.checkout_url
  provider.return_value.create_checkout_session.assert_called_once()
  assert _pending(test_db, test_org) == 1
