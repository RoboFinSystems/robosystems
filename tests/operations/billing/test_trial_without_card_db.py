"""A repository trial taken without a card, against a real Postgres session.

Checkout opens the trial with no card. While none is on file, every place the
trial speaks says so and gives the end date; Stripe cancels the trial when it
ends without one, and access ends with a refusal that says why. A card added
on the way records itself, and a later paid subscription replaces the ended
trial's grant with the plan.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from robosystems.config import env
from robosystems.dagster.jobs.billing import (
  _handle_subscription_deleted,
  _handle_subscription_updated,
)
from robosystems.middleware.rate_limits.repository_rate_limits import (
  limit_message,
  trial_ended_refusal,
)
from robosystems.models.core import BillingCustomer, BillingSubscription
from robosystems.models.core.user.user_repository import RepositoryType, UserRepository
from robosystems.operations.billing.repository_subscriptions import (
  org_has_payment_method,
  reconcile_repository_grant,
  trial_without_card_ends_at,
)
from robosystems.operations.graph.repository_subscription_service import (
  RepositorySubscriptionService,
)
from robosystems.routers.graphs.query.execute import _check_shared_repository_limits
from robosystems.routers.graphs.subscriptions import end_repository_trial
from tests.dagster.test_billing_repository_lifecycle_db import _subscribed
from tests.dagster.test_repository_trial_db import _on_trial

pytestmark = pytest.mark.unit


def _card(session, org, on_file: bool = True) -> BillingCustomer:
  customer = BillingCustomer.get_or_create(org.id, session)
  customer.has_payment_method = on_file
  session.commit()
  return customer


def _grant(session, member) -> UserRepository:
  session.expire_all()
  grant = UserRepository.get_by_user_and_repository(member.id, "sec", session)
  assert grant is not None
  return grant


def _stripe_subscription(subscription, **fields) -> dict:
  return {
    "id": subscription.stripe_subscription_id,
    "object": "subscription",
    "customer": "cus_test_trial",
    **fields,
  }


async def _trial_ends_without_a_card(session, subscription) -> None:
  """What Stripe does with `missing_payment_method: cancel`."""
  await _handle_subscription_deleted(
    _stripe_subscription(subscription, status="canceled"), session, MagicMock()
  )


class TestWhileNoCardIsOnFile:
  def test_the_trial_end_is_known(self, test_db, test_user, test_org):
    member, subscription, _ = _on_trial(test_db, test_user, test_org)
    _card(test_db, test_org, on_file=False)

    ends = trial_without_card_ends_at("sec", member.id, test_db)

    assert ends == subscription.trial_ends_at.replace(tzinfo=UTC)

  def test_a_card_on_file_starts_the_plan_so_there_is_no_end(
    self, test_db, test_user, test_org
  ):
    member, _subscription, _ = _on_trial(test_db, test_user, test_org)
    _card(test_db, test_org)

    assert trial_without_card_ends_at("sec", member.id, test_db) is None

  async def test_a_raw_query_is_refused_with_the_way_to_add_a_card(
    self, test_db, test_user, test_org
  ):
    member, _subscription, _ = _on_trial(test_db, test_user, test_org)
    _card(test_db, test_org, on_file=False)

    with pytest.raises(HTTPException) as exc:
      await _check_shared_repository_limits("sec", member, test_db, raw_query=True)

    assert exc.value.status_code == 403
    assert "Raw queries are not part of the SEC trial" in exc.value.detail
    assert "No card is on file" in exc.value.detail
    assert f"{env.ROBOSYSTEMS_URL}/organization?tab=billing" in exc.value.detail
    assert "nothing is charged" in exc.value.detail
    assert "start your plan now" not in exc.value.detail

  def test_the_limit_message_says_the_trial_ends_without_a_card(
    self, test_db, test_user, test_org
  ):
    member, subscription, _ = _on_trial(test_db, test_user, test_org)
    _card(test_db, test_org, on_file=False)
    ends = subscription.trial_ends_at

    message = limit_message(
      "sec",
      "mcp",
      {"window": "hour", "limit": 50},
      "starter",
      trial=True,
      user_id=member.id,
    )

    assert "SEC trial's limit of 50 tool calls per hour" in message
    assert f"trial ends on {ends:%B} {ends.day}, {ends.year}" in message
    assert "No card is on file" in message

  def test_a_cancelled_trial_is_told_it_runs_out(self, test_db, test_user, test_org):
    member, subscription, _ = _on_trial(test_db, test_user, test_org)
    _card(test_db, test_org, on_file=False)
    subscription.status = "canceled"
    subscription.ends_at = subscription.trial_ends_at
    test_db.commit()

    message = limit_message(
      "sec",
      "mcp",
      {"window": "hour", "limit": 50},
      "starter",
      trial=True,
      user_id=member.id,
    )

    assert "cancelled and keeps these limits" in message
    assert "No card is on file" not in message


class TestTheTrialEndsWithoutACard:
  async def test_access_ends_and_the_refusal_says_why(
    self, test_db, test_user, test_org
  ):
    member, subscription, _ = _on_trial(test_db, test_user, test_org)
    _card(test_db, test_org, on_file=False)

    await _trial_ends_without_a_card(test_db, subscription)

    test_db.expire_all()
    assert not UserRepository.user_has_access(member.id, "sec", test_db)
    message = trial_ended_refusal("sec", member.id)
    assert message is not None
    assert "Your SEC trial ended on" in message
    assert f"{env.ROBOSYSTEMS_URL}/repositories/browse" in message

  def test_a_running_trial_or_a_paid_plan_gets_no_ended_message(
    self, test_db, test_user, test_org
  ):
    trialing, _subscription, _ = _on_trial(test_db, test_user, test_org)
    paying, _subscription, _grant = _subscribed(test_db, test_user, test_org)

    assert trial_ended_refusal("sec", trialing.id) is None
    assert trial_ended_refusal("sec", paying.id) is None

  async def test_a_lapsed_trial_leaves_the_graph_list(
    self, test_db, test_user, test_org
  ):
    from robosystems.routers.graphs.main import get_graphs

    member, subscription, _ = _on_trial(test_db, test_user, test_org)

    async def listed() -> list[str]:
      with patch("robosystems.routers.graphs.main.session", test_db):
        response = await get_graphs(current_user=member, _rate_limit=None)
      return [g.graphId for g in response.graphs]

    assert "sec" in await listed()
    # Cancelled during the trial: access runs to its end, and Stripe's
    # deletion that revokes the grant may land after it.
    subscription.status = "canceled"
    subscription.ends_at = datetime.now(UTC) + timedelta(days=1)
    test_db.commit()
    reconcile_repository_grant(subscription, test_db)
    grant = _grant(test_db, member)
    grant.expires_at = datetime.now(UTC) - timedelta(minutes=1)
    test_db.commit()
    assert "sec" not in await listed()


class TestAddingACard:
  async def test_a_card_on_the_subscription_is_recorded(
    self, test_db, test_user, test_org
  ):
    member, subscription, _ = _on_trial(test_db, test_user, test_org)
    _card(test_db, test_org, on_file=False)

    await _handle_subscription_updated(
      _stripe_subscription(
        subscription, status="trialing", default_payment_method="pm_test_1"
      ),
      test_db,
      MagicMock(),
    )

    test_db.expire_all()
    assert BillingCustomer.get_by_org_id(test_org.id, test_db).has_payment_method
    assert trial_without_card_ends_at("sec", member.id, test_db) is None

  async def test_a_trial_end_moved_in_stripe_moves_ours(
    self, test_db, test_user, test_org
  ):
    member, subscription, _ = _on_trial(test_db, test_user, test_org)
    _card(test_db, test_org, on_file=False)
    later = (datetime.now(UTC) + timedelta(days=30)).replace(microsecond=0)

    await _handle_subscription_updated(
      _stripe_subscription(
        subscription, status="trialing", trial_end=int(later.timestamp())
      ),
      test_db,
      MagicMock(),
    )

    assert trial_without_card_ends_at("sec", member.id, test_db) == later
    assert _grant(test_db, member).is_trial
    assert UserRepository.user_has_access(member.id, "sec", test_db)

  async def test_starting_the_plan_early_needs_a_card(
    self, test_db, test_user, test_org
  ):
    member, _subscription, _ = _on_trial(test_db, test_user, test_org)
    _card(test_db, test_org, on_file=False).stripe_customer_id = "cus_test_none"
    test_db.commit()
    provider = MagicMock()
    provider.ensure_default_payment_method.return_value = False

    with patch(
      "robosystems.operations.providers.payment_provider.get_payment_provider",
      return_value=provider,
    ):
      with pytest.raises(HTTPException) as exc:
        await end_repository_trial(
          graph_id="sec", current_user=member, db=test_db, _rate_limit=None
        )

    assert exc.value.status_code == 400
    assert "Add a card first" in exc.value.detail
    provider.end_trial.assert_not_called()

  async def test_starting_the_plan_early_with_a_card(
    self, test_db, test_user, test_org
  ):
    member, subscription, _ = _on_trial(test_db, test_user, test_org)
    _card(test_db, test_org).stripe_customer_id = "cus_test_card"
    test_db.commit()
    provider = MagicMock()
    provider.ensure_default_payment_method.return_value = True

    with patch(
      "robosystems.operations.providers.payment_provider.get_payment_provider",
      return_value=provider,
    ):
      await end_repository_trial(
        graph_id="sec", current_user=member, db=test_db, _rate_limit=None
      )

    provider.end_trial.assert_called_once_with(subscription.stripe_subscription_id)

  def test_a_card_just_added_counts_before_its_webhook(
    self, test_db, test_user, test_org
  ):
    customer = _card(test_db, test_org, on_file=False)
    customer.stripe_customer_id = "cus_test_card"
    test_db.commit()
    provider = MagicMock()
    provider.ensure_default_payment_method.return_value = True

    with patch(
      "robosystems.operations.providers.payment_provider.get_payment_provider",
      return_value=provider,
    ):
      assert org_has_payment_method(test_org.id, test_db)

    test_db.expire_all()
    assert BillingCustomer.get_by_org_id(test_org.id, test_db).has_payment_method


class TestSubscribingAfterTheTrialEnded:
  async def test_the_paid_plan_replaces_the_trial_grant(
    self, test_db, test_user, test_org
  ):
    member, ended, _ = _on_trial(test_db, test_user, test_org)
    _card(test_db, test_org, on_file=False)
    await _trial_ends_without_a_card(test_db, ended)

    subscription = BillingSubscription.create_subscription(
      org_id=test_org.id,
      resource_type="repository",
      resource_id="sec",
      plan_name="advanced",
      base_price_cents=9900,
      session=test_db,
      user_id=member.id,
    )
    service = RepositorySubscriptionService(test_db)
    service.grant_access(
      repository_type=RepositoryType.SEC,
      user_id=member.id,
      repository_plan="advanced",
      trial=False,
    )
    service.allocate_credits(
      repository_type=RepositoryType.SEC, repository_plan="advanced", user_id=member.id
    )
    subscription.activate(test_db)
    reconcile_repository_grant(subscription, test_db)

    grant = _grant(test_db, member)
    assert grant.is_active and not grant.is_trial
    assert grant.expires_at is None
    assert grant.limits_plan == "advanced"
    assert grant.user_credits is not None
    assert grant.monthly_credit_allocation == grant.user_credits.monthly_allocation
    assert UserRepository.user_has_access(member.id, "sec", test_db)
    assert trial_ended_refusal("sec", member.id) is None
