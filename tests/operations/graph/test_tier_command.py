# pyright: reportGeneralTypeIssues=false, reportArgumentType=false
"""Tests for change_graph_tier_cmd.

All lazy imports inside change_graph_tier_cmd are patched at their source
modules. Module-level imports in tier.py are patched at the tier module
namespace.
"""

from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

# Module-level patch targets (imported at top of tier.py)
_TIER = "robosystems.operations.graph.commands.tier"

# Lazy-import patch targets (imported inside function body)
_CORE = "robosystems.models.core"
_GRAPH = "robosystems.models.core.graph.Graph"
_GRAPH_CREDITS = "robosystems.models.core.graph.graph_credits.GraphCredits"
_BILLING_CFG = "robosystems.config.billing"
_PAYMENT = "robosystems.operations.providers.payment_provider.get_payment_provider"
_WORKER = "robosystems.worker.client.enqueue_task"
_CACHE_INVALIDATE = (
  "robosystems.middleware.billing.enforcement.invalidate_subscription_cache"
)


def _make_user(org_id="org_123"):
  user = MagicMock()
  user.id = "user_abc"
  return user


def _make_membership(org_id="org_123", role="OWNER"):
  m = MagicMock()
  m.org_id = org_id
  m.role = role
  return m


def _make_subscription(
  status="active",
  plan_name="ladybug-standard",
  base_price_cents=9900,
  org_id="org_123",
  stripe_sub_id="sub_stripe_abc",
):
  sub = MagicMock()
  sub.id = "bsub_001"
  sub.plan_name = plan_name
  sub.base_price_cents = base_price_cents
  sub.status = status
  sub.org_id = org_id
  sub.stripe_subscription_id = stripe_sub_id
  return sub


def _make_graph(tier="ladybug-standard"):
  g = MagicMock()
  g.graph_id = "kg1a2b3c4d5e6f78"
  g.graph_tier = tier
  return g


def _make_graph_credits(allocation=8000):
  gc = MagicMock()
  gc.monthly_allocation = Decimal(str(allocation))
  return gc


GRAPH_ID = "kg1a2b3c4d5e6f78"


@pytest.mark.asyncio
@pytest.mark.unit
class TestChangeGraphTierCmd:
  """Tests for the change_graph_tier_cmd ops-layer command."""

  async def _call(self, new_tier="ladybug-large", user=None, db=None):
    from robosystems.operations.graph.commands.tier import change_graph_tier_cmd

    return await change_graph_tier_cmd(
      graph_id=GRAPH_ID,
      new_tier=new_tier,
      current_user=user or _make_user(),
      db=db or MagicMock(),
    )

  async def test_non_member_raises_403(self):
    """User with no org memberships is rejected."""
    with patch(f"{_CORE}.OrgUser") as mock_org_user:
      mock_org_user.get_user_orgs.return_value = []
      with pytest.raises(HTTPException) as exc:
        await self._call()
    assert exc.value.status_code == 403

  async def test_non_owner_raises_403(self):
    """Org member who is not OWNER is rejected."""
    membership = _make_membership(role="MEMBER")
    with (
      patch(f"{_CORE}.OrgUser") as mock_org_user,
      patch(f"{_CORE}.OrgRole") as mock_org_role,
    ):
      mock_org_user.get_user_orgs.return_value = [membership]
      mock_org_role.OWNER = "OWNER"
      with pytest.raises(HTTPException) as exc:
        await self._call()
    assert exc.value.status_code == 403

  async def test_graph_not_found_raises_404(self):
    """Missing graph returns 404."""
    membership = _make_membership()
    with (
      patch(f"{_CORE}.OrgUser") as mock_org_user,
      patch(f"{_CORE}.OrgRole") as mock_org_role,
      patch(_GRAPH) as mock_graph_cls,
    ):
      mock_org_user.get_user_orgs.return_value = [membership]
      mock_org_role.OWNER = "OWNER"
      membership.role = mock_org_role.OWNER
      mock_graph_cls.get_by_id.return_value = None
      with pytest.raises(HTTPException) as exc:
        await self._call()
    assert exc.value.status_code == 404

  async def test_no_subscription_raises_404(self):
    """Graph with no billing subscription returns 404."""
    membership = _make_membership()
    with (
      patch(f"{_CORE}.OrgUser") as mock_org_user,
      patch(f"{_CORE}.OrgRole") as mock_org_role,
      patch(_GRAPH) as mock_graph_cls,
      patch(f"{_TIER}.BillingSubscription") as mock_sub_cls,
    ):
      mock_org_user.get_user_orgs.return_value = [membership]
      mock_org_role.OWNER = "OWNER"
      membership.role = mock_org_role.OWNER
      mock_graph_cls.get_by_id.return_value = _make_graph()
      mock_sub_cls.get_by_resource.return_value = None
      with pytest.raises(HTTPException) as exc:
        await self._call()
    assert exc.value.status_code == 404

  async def test_wrong_org_raises_403(self):
    """Subscription belonging to a different org is rejected."""
    membership = _make_membership(org_id="org_mine")
    subscription = _make_subscription(org_id="org_other")
    with (
      patch(f"{_CORE}.OrgUser") as mock_org_user,
      patch(f"{_CORE}.OrgRole") as mock_org_role,
      patch(_GRAPH) as mock_graph_cls,
      patch(f"{_TIER}.BillingSubscription") as mock_sub_cls,
    ):
      mock_org_user.get_user_orgs.return_value = [membership]
      mock_org_role.OWNER = "OWNER"
      membership.role = mock_org_role.OWNER
      mock_graph_cls.get_by_id.return_value = _make_graph()
      mock_sub_cls.get_by_resource.return_value = subscription
      with pytest.raises(HTTPException) as exc:
        await self._call()
    assert exc.value.status_code == 403

  async def test_non_active_subscription_raises_400(self):
    """Canceled or suspended subscription is rejected."""
    membership = _make_membership()
    subscription = _make_subscription(status="canceled")
    with (
      patch(f"{_CORE}.OrgUser") as mock_org_user,
      patch(f"{_CORE}.OrgRole") as mock_org_role,
      patch(_GRAPH) as mock_graph_cls,
      patch(f"{_TIER}.BillingSubscription") as mock_sub_cls,
    ):
      mock_org_user.get_user_orgs.return_value = [membership]
      mock_org_role.OWNER = "OWNER"
      membership.role = mock_org_role.OWNER
      mock_graph_cls.get_by_id.return_value = _make_graph()
      mock_sub_cls.get_by_resource.return_value = subscription
      with pytest.raises(HTTPException) as exc:
        await self._call()
    assert exc.value.status_code == 400
    assert "canceled" in exc.value.detail

  async def test_invalid_tier_raises_400(self):
    """Unrecognized tier name is rejected."""
    membership = _make_membership()
    subscription = _make_subscription()
    with (
      patch(f"{_CORE}.OrgUser") as mock_org_user,
      patch(f"{_CORE}.OrgRole") as mock_org_role,
      patch(_GRAPH) as mock_graph_cls,
      patch(f"{_TIER}.BillingSubscription") as mock_sub_cls,
      patch(f"{_TIER}.BillingConfig") as mock_billing_cfg,
    ):
      mock_org_user.get_user_orgs.return_value = [membership]
      mock_org_role.OWNER = "OWNER"
      membership.role = mock_org_role.OWNER
      mock_graph_cls.get_by_id.return_value = _make_graph()
      mock_sub_cls.get_by_resource.return_value = subscription
      mock_billing_cfg.get_subscription_plan.return_value = None
      with pytest.raises(HTTPException) as exc:
        await self._call(new_tier="ladybug-mega")
    assert exc.value.status_code == 400
    assert "Invalid tier" in exc.value.detail

  async def test_same_tier_raises_400(self):
    """Requesting the current tier is rejected."""
    membership = _make_membership()
    subscription = _make_subscription(plan_name="ladybug-standard")
    graph = _make_graph(tier="ladybug-standard")
    with (
      patch(f"{_CORE}.OrgUser") as mock_org_user,
      patch(f"{_CORE}.OrgRole") as mock_org_role,
      patch(_GRAPH) as mock_graph_cls,
      patch(f"{_TIER}.BillingSubscription") as mock_sub_cls,
      patch(f"{_TIER}.BillingConfig") as mock_billing_cfg,
    ):
      mock_org_user.get_user_orgs.return_value = [membership]
      mock_org_role.OWNER = "OWNER"
      membership.role = mock_org_role.OWNER
      mock_graph_cls.get_by_id.return_value = graph
      mock_sub_cls.get_by_resource.return_value = subscription
      mock_billing_cfg.get_subscription_plan.return_value = {
        "name": "ladybug-standard",
        "base_price_cents": 9900,
      }
      with pytest.raises(HTTPException) as exc:
        await self._call(new_tier="ladybug-standard")
    assert exc.value.status_code == 400
    assert "Already on this tier" in exc.value.detail

  async def test_successful_upgrade_returns_operation_id(self):
    """Happy-path upgrade returns the operation_id string."""
    membership = _make_membership()
    subscription = _make_subscription()
    graph = _make_graph()
    graph_credits = _make_graph_credits()

    with (
      patch(f"{_CORE}.OrgUser") as mock_org_user,
      patch(f"{_CORE}.OrgRole") as mock_org_role,
      patch(_GRAPH) as mock_graph_cls,
      patch(f"{_TIER}.BillingSubscription") as mock_sub_cls,
      patch(f"{_TIER}.BillingConfig") as mock_billing_cfg,
      patch(f"{_TIER}.BillingAuditLog"),
      patch(_GRAPH_CREDITS) as mock_gc_cls,
      patch(f"{_BILLING_CFG}.get_tier_credit_allocation", return_value=32000),
      patch(_PAYMENT) as mock_provider_factory,
      patch(_WORKER, new_callable=AsyncMock) as mock_enqueue,
      patch(_CACHE_INVALIDATE),
      patch(f"{_TIER}.env") as mock_env,
    ):
      mock_org_user.get_user_orgs.return_value = [membership]
      mock_org_role.OWNER = "OWNER"
      membership.role = mock_org_role.OWNER
      mock_graph_cls.get_by_id.return_value = graph
      mock_sub_cls.get_by_resource.return_value = subscription
      mock_billing_cfg.get_subscription_plan.return_value = {
        "name": "ladybug-large",
        "base_price_cents": 24900,
      }
      mock_gc_cls.get_by_graph_id.return_value = graph_credits
      mock_env.BILLING_ENABLED = True
      provider = MagicMock()
      provider.get_or_create_price.return_value = "price_new"
      provider.change_subscription_price.return_value = {}
      mock_provider_factory.return_value = provider
      mock_enqueue.return_value = {"operation_id": "op_upgrade_001"}

      result = await self._call(new_tier="ladybug-large")

    assert result == "op_upgrade_001"
    assert subscription.plan_name == "ladybug-large"
    assert graph.graph_tier == "ladybug-large"

  async def test_stripe_failure_rolls_back(self):
    """Stripe error reverts PG changes and raises 500."""
    membership = _make_membership()
    subscription = _make_subscription()
    graph = _make_graph()
    graph_credits = _make_graph_credits()

    with (
      patch(f"{_CORE}.OrgUser") as mock_org_user,
      patch(f"{_CORE}.OrgRole") as mock_org_role,
      patch(_GRAPH) as mock_graph_cls,
      patch(f"{_TIER}.BillingSubscription") as mock_sub_cls,
      patch(f"{_TIER}.BillingConfig") as mock_billing_cfg,
      patch(_GRAPH_CREDITS) as mock_gc_cls,
      patch(f"{_BILLING_CFG}.get_tier_credit_allocation", side_effect=[32000, 8000]),
      patch(_PAYMENT) as mock_provider_factory,
      patch(_CACHE_INVALIDATE),
      patch(f"{_TIER}.env") as mock_env,
    ):
      mock_org_user.get_user_orgs.return_value = [membership]
      mock_org_role.OWNER = "OWNER"
      membership.role = mock_org_role.OWNER
      mock_graph_cls.get_by_id.return_value = graph
      mock_sub_cls.get_by_resource.return_value = subscription
      mock_billing_cfg.get_subscription_plan.return_value = {
        "name": "ladybug-large",
        "base_price_cents": 24900,
      }
      mock_gc_cls.get_by_graph_id.return_value = graph_credits
      mock_env.BILLING_ENABLED = True
      provider = MagicMock()
      provider.get_or_create_price.return_value = "price_new"
      provider.change_subscription_price.side_effect = Exception("Stripe down")
      mock_provider_factory.return_value = provider

      with pytest.raises(HTTPException) as exc:
        await self._call(new_tier="ladybug-large")

    assert exc.value.status_code == 500
    assert "rolled back" in exc.value.detail.lower()
    # PG rolled back
    assert subscription.status == "active"
    assert subscription.plan_name == "ladybug-standard"
    assert graph.graph_tier == "ladybug-standard"

  async def test_enqueue_failure_rolls_back(self):
    """Worker enqueue failure reverts both PG and Stripe, raises 500."""
    membership = _make_membership()
    subscription = _make_subscription()
    graph = _make_graph()
    graph_credits = _make_graph_credits()

    with (
      patch(f"{_CORE}.OrgUser") as mock_org_user,
      patch(f"{_CORE}.OrgRole") as mock_org_role,
      patch(_GRAPH) as mock_graph_cls,
      patch(f"{_TIER}.BillingSubscription") as mock_sub_cls,
      patch(f"{_TIER}.BillingConfig") as mock_billing_cfg,
      patch(_GRAPH_CREDITS) as mock_gc_cls,
      patch(
        f"{_BILLING_CFG}.get_tier_credit_allocation",
        side_effect=[32000, 8000, 8000],
      ),
      patch(_PAYMENT) as mock_provider_factory,
      patch(_WORKER, new_callable=AsyncMock) as mock_enqueue,
      patch(_CACHE_INVALIDATE),
      patch(f"{_TIER}.env") as mock_env,
    ):
      mock_org_user.get_user_orgs.return_value = [membership]
      mock_org_role.OWNER = "OWNER"
      membership.role = mock_org_role.OWNER
      mock_graph_cls.get_by_id.return_value = graph
      mock_sub_cls.get_by_resource.return_value = subscription
      mock_billing_cfg.get_subscription_plan.return_value = {
        "name": "ladybug-large",
        "base_price_cents": 24900,
      }
      mock_gc_cls.get_by_graph_id.return_value = graph_credits
      mock_env.BILLING_ENABLED = True
      provider = MagicMock()
      provider.get_or_create_price.return_value = "price_new"
      provider.change_subscription_price.return_value = {}
      mock_provider_factory.return_value = provider
      mock_enqueue.side_effect = Exception("Queue unavailable")

      with pytest.raises(HTTPException) as exc:
        await self._call(new_tier="ladybug-large")

    assert exc.value.status_code == 500
    assert "rolled back" in exc.value.detail.lower()
    assert subscription.status == "active"
    assert graph.graph_tier == "ladybug-standard"
