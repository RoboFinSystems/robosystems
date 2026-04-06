# pyright: reportGeneralTypeIssues=false, reportArgumentType=false
"""Tests for graph tier upgrade/downgrade via _change_graph_tier.

All imports inside _change_graph_tier are lazy (inside function body),
so patches target the source modules, not the subscriptions module.
"""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

# Patch targets at source locations (lazy imports inside function body)
_CORE = "robosystems.models.core"
_BILLING_CFG = "robosystems.config.billing"
_GRAPH = "robosystems.models.core.graph"
_GRAPH_CREDITS = "robosystems.models.core.graph.graph_credits"
_PROVIDERS = "robosystems.operations.providers.payment_provider"
_WORKER = "robosystems.worker.client"
_SUBSCRIPTIONS = "robosystems.routers.graphs.subscriptions"
_VALIDATION = "robosystems.operations.graph.tier_validation"


def _make_mock_subscription(
  status="active",
  plan_name="ladybug-standard",
  base_price_cents=9900,
  org_id="org_123",
  stripe_subscription_id="sub_stripe_abc",
):
  sub = MagicMock()
  sub.id = "bsub_abc123"
  sub.resource_type = "graph"
  sub.resource_id = "kg1a2b3c4d5e6f78"
  sub.plan_name = plan_name
  sub.billing_interval = "monthly"
  sub.status = status
  sub.base_price_cents = base_price_cents
  sub.org_id = org_id
  sub.stripe_subscription_id = stripe_subscription_id
  sub.current_period_start = datetime(2024, 6, 15, tzinfo=UTC)
  sub.current_period_end = datetime(2024, 7, 15, tzinfo=UTC)
  sub.started_at = datetime(2024, 6, 15, tzinfo=UTC)
  sub.canceled_at = None
  sub.ends_at = None
  sub.created_at = datetime(2024, 6, 15, tzinfo=UTC)
  sub.updated_at = datetime(2024, 6, 15, tzinfo=UTC)
  return sub


def _make_mock_graph(graph_tier="ladybug-standard"):
  graph = MagicMock()
  graph.graph_id = "kg1a2b3c4d5e6f78"
  graph.graph_tier = graph_tier
  graph.updated_at = datetime(2024, 6, 15, tzinfo=UTC)
  return graph


def _make_request(new_plan_name="ladybug-large"):
  req = MagicMock()
  req.new_plan_name = new_plan_name
  return req


@pytest.fixture
def mock_db():
  return MagicMock()


@pytest.fixture
def mock_user():
  user = MagicMock()
  user.id = "user_123"
  return user


def _patch_org_owner(org_id="org_123"):
  """Create patches for OrgUser/OrgRole that return an OWNER membership."""
  membership = MagicMock()
  membership.org_id = org_id

  mock_org_user = MagicMock()
  mock_org_user.get_user_orgs.return_value = [membership]

  mock_org_role = MagicMock()
  membership.role = mock_org_role.OWNER

  return (
    patch(f"{_CORE}.OrgUser", mock_org_user),
    patch(f"{_CORE}.OrgRole", mock_org_role),
  )


def _patch_org_member(org_id="org_123"):
  """Create patches for OrgUser/OrgRole that return a non-OWNER membership."""
  membership = MagicMock()
  membership.org_id = org_id
  membership.role = "MEMBER"

  mock_org_user = MagicMock()
  mock_org_user.get_user_orgs.return_value = [membership]

  mock_org_role = MagicMock()
  mock_org_role.OWNER = "OWNER"

  return (
    patch(f"{_CORE}.OrgUser", mock_org_user),
    patch(f"{_CORE}.OrgRole", mock_org_role),
  )


@pytest.mark.unit
@pytest.mark.asyncio
class TestChangeGraphTier:
  """Tests for _change_graph_tier function."""

  async def test_upgrade_returns_operation_id(self, mock_db, mock_user):
    """Successful upgrade returns response with operation_id."""
    subscription = _make_mock_subscription()
    graph = _make_mock_graph()
    graph_credits = MagicMock()
    graph_credits.monthly_allocation = Decimal("8000")
    request = _make_request("ladybug-large")

    org_user_patch, org_role_patch = _patch_org_owner()

    with (
      org_user_patch,
      org_role_patch,
      patch(f"{_GRAPH}.Graph.get_by_id", return_value=graph),
      patch(
        f"{_SUBSCRIPTIONS}.BillingSubscription.get_by_resource",
        return_value=subscription,
      ),
      patch(
        f"{_GRAPH_CREDITS}.GraphCredits.get_by_graph_id", return_value=graph_credits
      ),
      patch(
        f"{_BILLING_CFG}.BillingConfig.get_subscription_plan",
        return_value={"name": "ladybug-large", "base_price_cents": 29900},
      ),
      patch(f"{_BILLING_CFG}.get_tier_credit_allocation", return_value=32000),
      patch(f"{_PROVIDERS}.get_payment_provider") as mock_provider,
      patch(f"{_WORKER}.enqueue_task", new_callable=AsyncMock) as mock_enqueue,
      patch(f"{_SUBSCRIPTIONS}.BillingAuditLog"),
      patch(f"{_SUBSCRIPTIONS}.env") as mock_env,
      patch(f"{_SUBSCRIPTIONS}._get_plan_display_name", return_value="LadybugDB Large"),
    ):
      mock_env.BILLING_ENABLED = True
      provider = MagicMock()
      provider.get_or_create_price.return_value = "price_new"
      provider.change_subscription_price.return_value = {}
      mock_provider.return_value = provider
      mock_enqueue.return_value = {"operation_id": "op_xyz789"}

      from robosystems.routers.graphs.subscriptions import _change_graph_tier

      response = await _change_graph_tier(
        "kg1a2b3c4d5e6f78", request, mock_user, mock_db
      )

    assert response.operation_id == "op_xyz789"
    assert response.status == "upgrading"
    assert response.plan_name == "ladybug-large"
    mock_enqueue.assert_called_once()

  async def test_same_tier_rejected(self, mock_db, mock_user):
    """Rejects upgrade to the same tier."""
    subscription = _make_mock_subscription()
    graph = _make_mock_graph()
    request = _make_request("ladybug-standard")

    org_user_patch, org_role_patch = _patch_org_owner()

    with (
      org_user_patch,
      org_role_patch,
      patch(f"{_GRAPH}.Graph.get_by_id", return_value=graph),
      patch(
        f"{_SUBSCRIPTIONS}.BillingSubscription.get_by_resource",
        return_value=subscription,
      ),
      patch(
        f"{_BILLING_CFG}.BillingConfig.get_subscription_plan",
        return_value={"name": "ladybug-standard", "base_price_cents": 9900},
      ),
    ):
      from robosystems.routers.graphs.subscriptions import _change_graph_tier

      with pytest.raises(HTTPException) as exc_info:
        await _change_graph_tier("kg1a2b3c4d5e6f78", request, mock_user, mock_db)

    assert exc_info.value.status_code == 400
    assert "Already on this tier" in exc_info.value.detail

  async def test_canceled_subscription_rejected(self, mock_db, mock_user):
    """Rejects upgrade on a canceled subscription."""
    subscription = _make_mock_subscription(status="canceled")
    graph = _make_mock_graph()
    request = _make_request("ladybug-large")

    org_user_patch, org_role_patch = _patch_org_owner()

    with (
      org_user_patch,
      org_role_patch,
      patch(f"{_GRAPH}.Graph.get_by_id", return_value=graph),
      patch(
        f"{_SUBSCRIPTIONS}.BillingSubscription.get_by_resource",
        return_value=subscription,
      ),
    ):
      from robosystems.routers.graphs.subscriptions import _change_graph_tier

      with pytest.raises(HTTPException) as exc_info:
        await _change_graph_tier("kg1a2b3c4d5e6f78", request, mock_user, mock_db)

    assert exc_info.value.status_code == 400
    assert "canceled" in exc_info.value.detail

  async def test_already_upgrading_rejected(self, mock_db, mock_user):
    """Rejects upgrade when already in upgrading state."""
    subscription = _make_mock_subscription(status="upgrading")
    graph = _make_mock_graph()
    request = _make_request("ladybug-large")

    org_user_patch, org_role_patch = _patch_org_owner()

    with (
      org_user_patch,
      org_role_patch,
      patch(f"{_GRAPH}.Graph.get_by_id", return_value=graph),
      patch(
        f"{_SUBSCRIPTIONS}.BillingSubscription.get_by_resource",
        return_value=subscription,
      ),
    ):
      from robosystems.routers.graphs.subscriptions import _change_graph_tier

      with pytest.raises(HTTPException) as exc_info:
        await _change_graph_tier("kg1a2b3c4d5e6f78", request, mock_user, mock_db)

    assert exc_info.value.status_code == 400
    assert "upgrading" in exc_info.value.detail

  async def test_non_owner_rejected(self, mock_db, mock_user):
    """Rejects upgrade from non-owner org member."""
    request = _make_request("ladybug-large")

    org_user_patch, org_role_patch = _patch_org_member()

    with org_user_patch, org_role_patch:
      from robosystems.routers.graphs.subscriptions import _change_graph_tier

      with pytest.raises(HTTPException) as exc_info:
        await _change_graph_tier("kg1a2b3c4d5e6f78", request, mock_user, mock_db)

    assert exc_info.value.status_code == 403

  async def test_invalid_tier_rejected(self, mock_db, mock_user):
    """Rejects upgrade to a non-existent tier."""
    subscription = _make_mock_subscription()
    graph = _make_mock_graph()
    request = _make_request("ladybug-mega")

    org_user_patch, org_role_patch = _patch_org_owner()

    with (
      org_user_patch,
      org_role_patch,
      patch(f"{_GRAPH}.Graph.get_by_id", return_value=graph),
      patch(
        f"{_SUBSCRIPTIONS}.BillingSubscription.get_by_resource",
        return_value=subscription,
      ),
      patch(f"{_BILLING_CFG}.BillingConfig.get_subscription_plan", return_value=None),
    ):
      from robosystems.routers.graphs.subscriptions import _change_graph_tier

      with pytest.raises(HTTPException) as exc_info:
        await _change_graph_tier("kg1a2b3c4d5e6f78", request, mock_user, mock_db)

    assert exc_info.value.status_code == 400
    assert "Invalid tier" in exc_info.value.detail

  async def test_no_subscription_returns_404(self, mock_db, mock_user):
    """Returns 404 when graph has no subscription."""
    graph = _make_mock_graph()
    request = _make_request("ladybug-large")

    org_user_patch, org_role_patch = _patch_org_owner()

    with (
      org_user_patch,
      org_role_patch,
      patch(f"{_GRAPH}.Graph.get_by_id", return_value=graph),
      patch(f"{_SUBSCRIPTIONS}.BillingSubscription.get_by_resource", return_value=None),
    ):
      from robosystems.routers.graphs.subscriptions import _change_graph_tier

      with pytest.raises(HTTPException) as exc_info:
        await _change_graph_tier("kg1a2b3c4d5e6f78", request, mock_user, mock_db)

    assert exc_info.value.status_code == 404

  async def test_wrong_org_returns_403(self, mock_db, mock_user):
    """Returns 403 when subscription belongs to a different org."""
    subscription = _make_mock_subscription(org_id="org_other")
    graph = _make_mock_graph()
    request = _make_request("ladybug-large")

    org_user_patch, org_role_patch = _patch_org_owner(org_id="org_123")

    with (
      org_user_patch,
      org_role_patch,
      patch(f"{_GRAPH}.Graph.get_by_id", return_value=graph),
      patch(
        f"{_SUBSCRIPTIONS}.BillingSubscription.get_by_resource",
        return_value=subscription,
      ),
    ):
      from robosystems.routers.graphs.subscriptions import _change_graph_tier

      with pytest.raises(HTTPException) as exc_info:
        await _change_graph_tier("kg1a2b3c4d5e6f78", request, mock_user, mock_db)

    assert exc_info.value.status_code == 403

  async def test_downgrade_blocked_by_subgraphs(self, mock_db, mock_user):
    """Downgrade is blocked when subgraph count exceeds target tier limit."""
    subscription = _make_mock_subscription(
      plan_name="ladybug-large", base_price_cents=29900
    )
    graph = _make_mock_graph(graph_tier="ladybug-large")
    request = _make_request("ladybug-standard")

    org_user_patch, org_role_patch = _patch_org_owner()

    with (
      org_user_patch,
      org_role_patch,
      patch(f"{_GRAPH}.Graph.get_by_id", return_value=graph),
      patch(
        f"{_SUBSCRIPTIONS}.BillingSubscription.get_by_resource",
        return_value=subscription,
      ),
      patch(
        f"{_BILLING_CFG}.BillingConfig.get_subscription_plan",
        return_value={"name": "ladybug-standard", "base_price_cents": 9900},
      ),
      patch(
        f"{_VALIDATION}.validate_subgraph_count",
        side_effect=HTTPException(
          status_code=400,
          detail="Cannot downgrade: 7 active subgraphs exceeds ladybug-standard limit of 3.",
        ),
      ),
    ):
      from robosystems.routers.graphs.subscriptions import _change_graph_tier

      with pytest.raises(HTTPException) as exc_info:
        await _change_graph_tier("kg1a2b3c4d5e6f78", request, mock_user, mock_db)

    assert exc_info.value.status_code == 400
    assert "subgraphs" in exc_info.value.detail

  async def test_downgrade_blocked_by_storage(self, mock_db, mock_user):
    """Downgrade is blocked when storage exceeds target tier limit."""
    subscription = _make_mock_subscription(
      plan_name="ladybug-large", base_price_cents=29900
    )
    graph = _make_mock_graph(graph_tier="ladybug-large")
    request = _make_request("ladybug-standard")

    org_user_patch, org_role_patch = _patch_org_owner()

    with (
      org_user_patch,
      org_role_patch,
      patch(f"{_GRAPH}.Graph.get_by_id", return_value=graph),
      patch(
        f"{_SUBSCRIPTIONS}.BillingSubscription.get_by_resource",
        return_value=subscription,
      ),
      patch(
        f"{_BILLING_CFG}.BillingConfig.get_subscription_plan",
        return_value={"name": "ladybug-standard", "base_price_cents": 9900},
      ),
      patch(f"{_VALIDATION}.validate_subgraph_count"),
      patch(
        f"{_VALIDATION}.validate_storage_capacity",
        new_callable=AsyncMock,
        side_effect=HTTPException(
          status_code=400,
          detail="Cannot downgrade: 45.0 GB storage exceeds ladybug-standard limit of 20 GB.",
        ),
      ),
    ):
      from robosystems.routers.graphs.subscriptions import _change_graph_tier

      with pytest.raises(HTTPException) as exc_info:
        await _change_graph_tier("kg1a2b3c4d5e6f78", request, mock_user, mock_db)

    assert exc_info.value.status_code == 400
    assert "storage" in exc_info.value.detail

  async def test_stripe_failure_rolls_back(self, mock_db, mock_user):
    """Stripe failure rolls back all PostgreSQL changes."""
    subscription = _make_mock_subscription()
    graph = _make_mock_graph()
    graph_credits = MagicMock()
    graph_credits.monthly_allocation = Decimal("8000")
    request = _make_request("ladybug-large")

    org_user_patch, org_role_patch = _patch_org_owner()

    with (
      org_user_patch,
      org_role_patch,
      patch(f"{_GRAPH}.Graph.get_by_id", return_value=graph),
      patch(
        f"{_SUBSCRIPTIONS}.BillingSubscription.get_by_resource",
        return_value=subscription,
      ),
      patch(
        f"{_GRAPH_CREDITS}.GraphCredits.get_by_graph_id", return_value=graph_credits
      ),
      patch(
        f"{_BILLING_CFG}.BillingConfig.get_subscription_plan",
        return_value={"name": "ladybug-large", "base_price_cents": 29900},
      ),
      patch(f"{_BILLING_CFG}.get_tier_credit_allocation", side_effect=[32000, 8000]),
      patch(f"{_PROVIDERS}.get_payment_provider") as mock_prov,
      patch(f"{_SUBSCRIPTIONS}.env") as mock_env,
    ):
      mock_env.BILLING_ENABLED = True
      provider = MagicMock()
      provider.get_or_create_price.return_value = "price_new"
      provider.change_subscription_price.side_effect = Exception("Stripe error")
      mock_prov.return_value = provider

      from robosystems.routers.graphs.subscriptions import _change_graph_tier

      with pytest.raises(HTTPException) as exc_info:
        await _change_graph_tier("kg1a2b3c4d5e6f78", request, mock_user, mock_db)

    assert exc_info.value.status_code == 500
    assert "rolled back" in exc_info.value.detail.lower()
    assert subscription.status == "active"
    assert subscription.plan_name == "ladybug-standard"
    assert graph.graph_tier == "ladybug-standard"

  async def test_enqueue_failure_rolls_back(self, mock_db, mock_user):
    """Worker queue failure rolls back all PostgreSQL and Stripe changes."""
    subscription = _make_mock_subscription()
    graph = _make_mock_graph()
    graph_credits = MagicMock()
    graph_credits.monthly_allocation = Decimal("8000")
    request = _make_request("ladybug-large")

    org_user_patch, org_role_patch = _patch_org_owner()

    with (
      org_user_patch,
      org_role_patch,
      patch(f"{_GRAPH}.Graph.get_by_id", return_value=graph),
      patch(
        f"{_SUBSCRIPTIONS}.BillingSubscription.get_by_resource",
        return_value=subscription,
      ),
      patch(
        f"{_GRAPH_CREDITS}.GraphCredits.get_by_graph_id", return_value=graph_credits
      ),
      patch(
        f"{_BILLING_CFG}.BillingConfig.get_subscription_plan",
        return_value={"name": "ladybug-large", "base_price_cents": 29900},
      ),
      patch(f"{_BILLING_CFG}.get_tier_credit_allocation", side_effect=[32000, 8000]),
      patch(f"{_PROVIDERS}.get_payment_provider"),
      patch(
        f"{_WORKER}.enqueue_task",
        new_callable=AsyncMock,
        side_effect=RuntimeError("queue down"),
      ),
      patch(f"{_SUBSCRIPTIONS}.env") as mock_env,
    ):
      mock_env.BILLING_ENABLED = False

      from robosystems.routers.graphs.subscriptions import _change_graph_tier

      with pytest.raises(HTTPException) as exc_info:
        await _change_graph_tier("kg1a2b3c4d5e6f78", request, mock_user, mock_db)

    assert exc_info.value.status_code == 500
    assert "rolled back" in exc_info.value.detail.lower()
    assert subscription.status == "active"
    assert subscription.plan_name == "ladybug-standard"
    assert subscription.base_price_cents == 9900
    assert graph.graph_tier == "ladybug-standard"
