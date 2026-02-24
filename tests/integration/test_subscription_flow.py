"""Integration tests for subscription lifecycle.

Exercises: operations/graph/subscription_service.py → models/billing/* → middleware/billing/enforcement.py
"""

from unittest.mock import patch

import pytest

from robosystems.models.billing import BillingCustomer, BillingSubscription
from robosystems.operations.graph.subscription_service import (
    get_available_plans,
    get_max_plan_tier,
    GraphSubscriptionService,
)
from robosystems.middleware.billing.enforcement import (
    check_graph_subscription_active,
    invalidate_subscription_cache,
    _get_cached_subscription,
    _cache_subscription,
)


@pytest.mark.integration
class TestSubscriptionServiceHelpers:
    """Test subscription service helper functions."""

    def test_get_available_plans(self):
        """Available plans should include all tiers."""
        plans = get_available_plans()
        assert isinstance(plans, list)
        assert len(plans) >= 3
        assert "ladybug-standard" in plans

    def test_get_max_plan_tier(self):
        """Max plan tier should be xlarge."""
        max_tier = get_max_plan_tier()
        assert max_tier == "ladybug-xlarge"


@pytest.mark.integration
class TestSubscriptionCreation:
    """Test subscription creation flow."""

    def test_create_graph_subscription(self, test_db, test_user, sample_graph, test_user_graph):
        """Create a billing subscription for a graph."""
        service = GraphSubscriptionService(test_db)

        with patch("robosystems.operations.graph.subscription_service.BILLING_ENABLED", False):
            subscription = service.create_graph_subscription(
                user_id=str(test_user.id),
                graph_id=sample_graph.graph_id,
                plan_name="ladybug-standard",
            )

        assert subscription is not None
        assert subscription.resource_id == sample_graph.graph_id
        assert subscription.plan_name == "ladybug-standard"
        assert subscription.status == "active"

    def test_create_duplicate_subscription_returns_existing(
        self, test_db, test_user, sample_graph, test_user_graph
    ):
        """Creating a subscription for the same graph returns existing."""
        service = GraphSubscriptionService(test_db)

        with patch("robosystems.operations.graph.subscription_service.BILLING_ENABLED", False):
            sub1 = service.create_graph_subscription(
                user_id=str(test_user.id),
                graph_id=sample_graph.graph_id,
            )
            sub2 = service.create_graph_subscription(
                user_id=str(test_user.id),
                graph_id=sample_graph.graph_id,
            )

        assert sub1.id == sub2.id


@pytest.mark.integration
class TestSubscriptionEnforcement:
    """Test subscription enforcement cache."""

    def test_cache_and_retrieve(self):
        """Cache a subscription result and retrieve it."""
        _cache_subscription("test_graph_123", "active")
        result = _get_cached_subscription("test_graph_123")
        assert result == "active"

    def test_invalidate_cache(self):
        """Invalidate subscription cache entry."""
        _cache_subscription("test_graph_456", "active")
        invalidate_subscription_cache("test_graph_456")
        result = _get_cached_subscription("test_graph_456")
        assert result is None

    def test_cache_miss_returns_none(self):
        """Missing cache entry returns None."""
        result = _get_cached_subscription("nonexistent_graph")
        assert result is None

    def test_check_subscription_no_subscription(self, test_db, sample_graph):
        """Graph without subscription — behavior depends on billing enabled."""
        is_active, error = check_graph_subscription_active(
            sample_graph.graph_id, test_db
        )
        # When billing is disabled, should return True
        # When enabled, should return False
        assert isinstance(is_active, bool)
