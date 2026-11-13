"""A repository trial driven through its Stripe lifecycle.

Against a real Postgres session, with the webhook handlers called in the order
Stripe delivers. The invariant: a trial grant has the views and nothing that
costs more (raw queries, the AI analyst, credits) until a paid invoice lands,
and only a subscriber's first subscription opens with one.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from fastapi import HTTPException

from robosystems.config import env
from robosystems.config.shared_repositories import TRIAL_PLAN, get_rate_limits
from robosystems.dagster.jobs.billing import (
  _as_utc,
  _handle_payment_succeeded,
  _handle_subscription_updated,
)
from robosystems.models.core import BillingSubscription, OrgRole, OrgUser
from robosystems.models.core.billing.subscription import SubscriptionStatus
from robosystems.models.core.user.user_repository import RepositoryType, UserRepository
from robosystems.operations.billing.repository_subscriptions import (
  convert_repository_trial,
  repository_trial_days,
)
from robosystems.operations.graph.repository_subscription_service import (
  RepositorySubscriptionService,
)
from robosystems.routers.graphs.query.execute import _check_shared_repository_limits
from tests.dagster.test_billing_repository_lifecycle_db import (
  _event,
  _has_access,
  _member,
  _subscribed,
)

pytestmark = pytest.mark.unit


def _on_trial(session, test_user, test_org, plan: str = "starter"):
  """A member whose first subscription was provisioned as a trial."""
  member = _member(session, test_user.password_hash)
  OrgUser.create(
    org_id=test_org.id, user_id=member.id, role=OrgRole.MEMBER, session=session
  )
  subscription = BillingSubscription.create_subscription(
    org_id=test_org.id,
    resource_type="repository",
    resource_id="sec",
    plan_name=plan,
    base_price_cents=2900,
    session=session,
    user_id=member.id,
  )
  subscription.activate(session)
  subscription.stripe_subscription_id = f"sub_test_{uuid4().hex[:10]}"
  subscription.trial_ends_at = datetime.now(UTC) + timedelta(days=14)
  subscription.current_period_end = subscription.trial_ends_at
  session.commit()

  RepositorySubscriptionService(session).grant_access(
    repository_type=RepositoryType.SEC,
    user_id=member.id,
    repository_plan=plan,
    trial=True,
  )
  grant = UserRepository.get_by_user_and_repository(member.id, "sec", session)
  return member, subscription, grant


def _invoice(subscription, *, reason: str, amount: int) -> dict:
  now = datetime.now(UTC)
  end = now + timedelta(days=30)
  return {
    "id": f"in_test_{uuid4().hex[:10]}",
    "object": "invoice",
    "subscription": subscription.stripe_subscription_id,
    "status": "paid",
    "billing_reason": reason,
    "amount_due": amount,
    "amount_paid": amount,
    "subtotal": amount,
    "total": amount,
    "period_start": int(now.timestamp()),
    "period_end": int(end.timestamp()),
    "lines": {
      "data": [
        {
          "amount": amount,
          "quantity": 1,
          "period": {"start": int(now.timestamp()), "end": int(end.timestamp())},
        }
      ]
    },
  }


class TestTrialGrant:
  def test_a_trial_grant_has_no_credits_and_reads_the_trial_limits(
    self, test_db, test_user, test_org
  ):
    _member_, _subscription, grant = _on_trial(test_db, test_user, test_org, "advanced")

    assert grant.is_trial is True
    assert grant.repository_plan == "advanced"
    assert grant.limits_plan == TRIAL_PLAN
    assert grant.monthly_credit_allocation == 0
    assert grant.user_credits is None

  def test_the_trial_row_is_below_every_plan(self):
    trial = get_rate_limits("sec", TRIAL_PLAN)
    starter = get_rate_limits("sec", "starter")
    assert trial is not None and starter is not None
    assert trial["agent_calls_per_day"] == 0
    assert trial["mcp_queries_per_day"] < starter["mcp_queries_per_day"]
    assert trial["searches_per_day"] < starter["searches_per_day"]
    # Decided with the trial: the download is not held back.
    assert trial["downloads_per_month"] == starter["downloads_per_month"]


class TestTrialLifecycle:
  async def test_the_opening_invoice_leaves_the_trial_running(
    self, test_db, test_user, test_org
  ):
    _member_, subscription, grant = _on_trial(test_db, test_user, test_org)

    await _handle_payment_succeeded(
      _invoice(subscription, reason="subscription_create", amount=0),
      test_db,
      MagicMock(),
    )

    test_db.refresh(grant)
    assert grant.is_trial is True
    assert grant.user_credits is None

  async def test_the_opening_invoice_can_beat_the_checkout_event(
    self, test_db, test_user, test_org
  ):
    """Stripe sends the zero invoice before checkout.session.completed; the
    subscription must carry its Stripe id by the time it is provisioned."""
    member = _member(test_db, test_user.password_hash)
    subscription = BillingSubscription.create_subscription(
      org_id=test_org.id,
      resource_type="repository",
      resource_id=None,
      plan_name="starter",
      base_price_cents=2900,
      session=test_db,
      user_id=member.id,
    )
    subscription.status = "pending_payment"
    subscription.provider_subscription_id = f"cs_test_{uuid4().hex[:10]}"
    subscription.subscription_metadata = {"trial_days": 14, "repository_name": "sec"}
    test_db.commit()

    stripe_sub_id = f"sub_test_{uuid4().hex[:10]}"
    invoice = _invoice(subscription, reason="subscription_create", amount=0)
    invoice["subscription"] = stripe_sub_id
    invoice["metadata"] = {"subscription_id": subscription.id}
    seen: dict = {}

    async def _provision(sub, _session, _context):
      seen["stripe_subscription_id"] = sub.stripe_subscription_id

    with patch(
      "robosystems.dagster.jobs.billing._trigger_resource_provisioning", _provision
    ):
      await _handle_payment_succeeded(invoice, test_db, MagicMock())

    assert seen["stripe_subscription_id"] == stripe_sub_id

  async def test_stripe_restates_the_trial_end(self, test_db, test_user, test_org):
    _member_, subscription, _grant = _on_trial(test_db, test_user, test_org)
    trial_end = datetime.now(UTC) + timedelta(days=13, hours=23)

    event = _event(subscription, "trialing", period_end=trial_end)
    event["trial_end"] = int(trial_end.timestamp())
    await _handle_subscription_updated(event, test_db, MagicMock())

    test_db.refresh(subscription)
    assert subscription.status == "active"
    assert abs((_as_utc(subscription.trial_ends_at) - trial_end).total_seconds()) < 1

  async def test_the_first_charge_lifts_the_trial_and_grants_the_credits(
    self, test_db, test_user, test_org
  ):
    member, subscription, grant = _on_trial(test_db, test_user, test_org)

    await _handle_payment_succeeded(
      _invoice(subscription, reason="subscription_cycle", amount=2900),
      test_db,
      MagicMock(),
    )

    test_db.refresh(grant)
    assert grant.is_trial is False
    assert grant.limits_plan == "starter"
    assert grant.monthly_credit_allocation == 5000
    assert grant.user_credits is not None
    assert grant.user_credits.monthly_allocation == 5000
    assert _has_access(test_db, member)
    # The record that this subscriber has had a trial outlives it.
    test_db.refresh(subscription)
    assert subscription.trial_ends_at is not None

  async def test_ending_the_trial_early_converts_on_its_invoice(
    self, test_db, test_user, test_org
  ):
    _member_, subscription, grant = _on_trial(test_db, test_user, test_org)

    await _handle_payment_succeeded(
      _invoice(subscription, reason="subscription_update", amount=2900),
      test_db,
      MagicMock(),
    )

    test_db.refresh(grant)
    assert grant.is_trial is False

  async def test_a_zero_invoice_inside_the_trial_does_not_lift_it(
    self, test_db, test_user, test_org
  ):
    """A plan change mid-trial raises a zero `subscription_update` invoice.
    Nothing was paid and the trial has not ended, so the trial stands."""
    _member_, subscription, grant = _on_trial(test_db, test_user, test_org)

    await _handle_payment_succeeded(
      _invoice(subscription, reason="subscription_update", amount=0),
      test_db,
      MagicMock(),
    )

    test_db.refresh(grant)
    assert grant.is_trial is True
    assert grant.user_credits is None

  async def test_a_fully_discounted_first_invoice_lifts_an_ended_trial(
    self, test_db, test_user, test_org
  ):
    _member_, subscription, grant = _on_trial(test_db, test_user, test_org)
    subscription.trial_ends_at = datetime.now(UTC) - timedelta(seconds=30)
    test_db.commit()

    await _handle_payment_succeeded(
      _invoice(subscription, reason="subscription_cycle", amount=0),
      test_db,
      MagicMock(),
    )

    test_db.refresh(grant)
    assert grant.is_trial is False

  @pytest.mark.parametrize(
    "trial_ends_in",
    [timedelta(minutes=4), timedelta(seconds=-1), timedelta(hours=-1)],
    ids=["in-its-last-minutes", "just-ended-early", "ended"],
  )
  async def test_a_zero_plan_change_invoice_never_lifts_the_trial(
    self, test_db, test_user, test_org, trial_ends_in
  ):
    """Stripe raises the same paid zero invoice for a plan change at any point,
    so it says nothing about the first charge: that one can still decline."""
    _member_, subscription, grant = _on_trial(test_db, test_user, test_org)
    subscription.trial_ends_at = datetime.now(UTC) + trial_ends_in
    test_db.commit()

    await _handle_payment_succeeded(
      _invoice(subscription, reason="subscription_update", amount=0),
      test_db,
      MagicMock(),
    )

    test_db.refresh(grant)
    assert grant.is_trial is True
    assert grant.limits_plan == TRIAL_PLAN
    assert grant.user_credits is None

  async def test_a_cancelled_trial_can_be_resumed(self, test_db, test_user, test_org):
    """Stripe reports a resumed trial as `trialing`, not `active`."""
    member, subscription, grant = _on_trial(test_db, test_user, test_org)
    period_end = datetime.now(UTC) + timedelta(days=10)
    await _handle_subscription_updated(
      _event(
        subscription, "trialing", cancel_at_period_end=True, period_end=period_end
      ),
      test_db,
      MagicMock(),
    )
    test_db.refresh(subscription)
    assert subscription.status == "canceled"

    provider = MagicMock()
    provider.get_subscription_state.return_value = {
      "status": "trialing",
      "cancel_at_period_end": False,
    }
    with patch(
      "robosystems.operations.providers.payment_provider.get_payment_provider",
      return_value=provider,
    ):
      await _handle_subscription_updated(
        _event(subscription, "trialing", period_end=period_end), test_db, MagicMock()
      )

    test_db.refresh(subscription)
    test_db.refresh(grant)
    assert subscription.status == "active"
    assert subscription.ends_at is None
    assert grant.expires_at is None
    assert grant.is_trial is True
    assert _has_access(test_db, member)

  async def test_a_plan_change_mid_trial_keeps_the_trial(
    self, test_db, test_user, test_org
  ):
    """The seat moves to the new plan; the trial, its limits and its empty
    credit pool stay until that plan's first charge."""
    from robosystems.models.api.billing.subscription import (
      UpgradeSubscriptionRequest,
    )
    from robosystems.routers.graphs.subscriptions import _change_repository_plan

    member, subscription, grant = _on_trial(test_db, test_user, test_org)

    with patch(
      "robosystems.operations.providers.payment_provider.get_payment_provider",
      return_value=MagicMock(),
    ):
      await _change_repository_plan(
        "sec", UpgradeSubscriptionRequest(new_plan_name="advanced"), member, test_db
      )
    # What Stripe sends for the price swap inside a trial.
    await _handle_payment_succeeded(
      _invoice(subscription, reason="subscription_update", amount=0),
      test_db,
      MagicMock(),
    )

    test_db.refresh(grant)
    assert grant.repository_plan == "advanced"
    assert grant.is_trial is True
    assert grant.limits_plan == TRIAL_PLAN
    assert not grant.monthly_credit_allocation
    assert grant.user_credits is None

  async def test_a_cancel_keeps_the_rest_of_the_trial_and_never_converts(
    self, test_db, test_user, test_org
  ):
    member, subscription, grant = _on_trial(test_db, test_user, test_org)

    await _handle_subscription_updated(
      _event(
        subscription,
        "trialing",
        cancel_at_period_end=True,
        period_end=datetime.now(UTC) + timedelta(days=10),
      ),
      test_db,
      MagicMock(),
    )

    test_db.refresh(subscription)
    test_db.refresh(grant)
    assert subscription.status == "canceled"
    assert _as_utc(subscription.ends_at) > datetime.now(UTC)
    assert grant.is_trial is True
    assert _has_access(test_db, member)

  def test_converting_twice_grants_once(self, test_db, test_user, test_org):
    _member_, subscription, grant = _on_trial(test_db, test_user, test_org)

    assert convert_repository_trial(subscription, test_db) is True
    assert convert_repository_trial(subscription, test_db) is False

    test_db.refresh(grant)
    assert grant.user_credits.monthly_allocation == 5000

  def test_a_paid_subscription_has_nothing_to_convert(
    self, test_db, test_user, test_org
  ):
    _member_, subscription, _grant = _subscribed(test_db, test_user, test_org)
    assert convert_repository_trial(subscription, test_db) is False


class TestTrialEligibility:
  def test_a_first_subscription_opens_with_the_trial(
    self, test_db, test_user, test_org
  ):
    member = _member(test_db, test_user.password_hash)
    assert repository_trial_days("sec", member.id, test_org.id, test_db) == 14

  def test_an_abandoned_checkout_does_not_spend_it(self, test_db, test_user, test_org):
    member = _member(test_db, test_user.password_hash)
    checkout = BillingSubscription.create_subscription(
      org_id=test_org.id,
      resource_type="repository",
      resource_id=None,
      plan_name="starter",
      base_price_cents=2900,
      session=test_db,
      user_id=member.id,
    )
    checkout.status = "pending_payment"
    test_db.commit()

    assert repository_trial_days("sec", member.id, test_org.id, test_db) == 14

  def test_a_failed_attempt_does_not_spend_it(self, test_db, test_user, test_org):
    member = _member(test_db, test_user.password_hash)
    failed = BillingSubscription.create_subscription(
      org_id=test_org.id,
      resource_type="repository",
      resource_id="sec",
      plan_name="starter",
      base_price_cents=2900,
      session=test_db,
      user_id=member.id,
    )
    failed.status = SubscriptionStatus.FAILED.value
    failed.ends_at = datetime.now(UTC)
    test_db.commit()

    assert repository_trial_days("sec", member.id, test_org.id, test_db) == 14

  def test_a_canceled_trial_is_spent(self, test_db, test_user, test_org):
    member, subscription, _grant = _on_trial(test_db, test_user, test_org)
    subscription.cancel(test_db, immediate=True)

    assert repository_trial_days("sec", member.id, test_org.id, test_db) == 0

  def test_one_trial_per_org(self, test_db, test_user, test_org):
    _on_trial(test_db, test_user, test_org)
    colleague = _member(test_db, test_user.password_hash)

    assert repository_trial_days("sec", colleague.id, test_org.id, test_db) == 0

  def test_a_repository_without_a_trial_offers_none(self, test_db, test_user, test_org):
    with patch("robosystems.config.shared_repositories.get_trial_days", return_value=0):
      assert repository_trial_days("sec", test_user.id, test_org.id, test_db) == 0


class TestTrialGate:
  async def test_a_raw_query_is_refused_with_the_way_to_open_it(
    self, test_db, test_user, test_org
  ):
    member, _subscription, _grant = _on_trial(test_db, test_user, test_org)

    with pytest.raises(HTTPException) as exc:
      await _check_shared_repository_limits("sec", member, test_db, raw_query=True)

    assert exc.value.status_code == 403
    assert "Raw queries are not part of the SEC trial" in exc.value.detail
    assert "organization?tab=subscriptions" in exc.value.detail

  async def test_the_ai_analyst_is_refused(self, test_db, test_user, test_org):
    member, _subscription, _grant = _on_trial(test_db, test_user, test_org)

    with pytest.raises(HTTPException) as exc:
      await _check_shared_repository_limits(
        "sec", member, test_db, endpoint="agent", operation="agent"
      )

    assert exc.value.status_code == 403
    assert "The AI analyst is not part of the SEC trial" in exc.value.detail

  async def test_a_view_read_is_open(self, test_db, test_user, test_org):
    member, _subscription, _grant = _on_trial(test_db, test_user, test_org)

    with patch.object(env, "RATE_LIMIT_ENABLED", False):
      await _check_shared_repository_limits("sec", member, test_db)

  async def test_a_paid_subscriber_is_not_gated(self, test_db, test_user, test_org):
    member, _subscription, _grant = _subscribed(test_db, test_user, test_org)

    with patch.object(env, "RATE_LIMIT_ENABLED", False):
      await _check_shared_repository_limits("sec", member, test_db, raw_query=True)
