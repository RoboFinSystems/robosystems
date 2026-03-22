"""Tests for subscription router helper functions."""

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

from robosystems.routers.graphs.subscriptions import (
  _get_plan_display_name,
  is_shared_repository,
  subscription_to_response,
)

MODULE = "robosystems.routers.graphs.subscriptions"


class TestIsSharedRepository:
  """Tests for is_shared_repository helper."""

  @pytest.mark.unit
  def test_shared_repo_returns_true(self):
    with patch(f"{MODULE}._is_shared_repo_or_sub", return_value=True):
      assert is_shared_repository("sec") is True

  @pytest.mark.unit
  def test_user_graph_returns_false(self):
    with patch(f"{MODULE}._is_shared_repo_or_sub", return_value=False):
      assert is_shared_repository("kg1a2b3c") is False


class TestGetPlanDisplayName:
  """Tests for _get_plan_display_name helper."""

  @pytest.mark.unit
  def test_graph_plan_with_display_name(self):
    with patch(
      f"{MODULE}.BillingConfig.get_subscription_plan",
      return_value={"display_name": "LadybugDB Standard"},
    ):
      result = _get_plan_display_name("ladybug-standard", "graph", "kg1a2b3c")
      assert result == "LadybugDB Standard"

  @pytest.mark.unit
  def test_graph_plan_no_display_name(self):
    with patch(
      f"{MODULE}.BillingConfig.get_subscription_plan",
      return_value={"name": "ladybug-standard"},
    ):
      result = _get_plan_display_name("ladybug-standard", "graph", "kg1a2b3c")
      assert result == "ladybug-standard"

  @pytest.mark.unit
  def test_repository_plan_with_display_name(self):
    with patch(
      f"{MODULE}.BillingConfig.get_repository_plan",
      return_value={"display_name": "SEC Professional"},
    ):
      result = _get_plan_display_name("sec-professional", "repository", "sec")
      assert result == "SEC Professional"

  @pytest.mark.unit
  def test_unknown_type_returns_plan_name(self):
    result = _get_plan_display_name("some-plan", "unknown", "whatever")
    assert result == "some-plan"


class TestSubscriptionToResponse:
  """Tests for subscription_to_response helper."""

  @pytest.mark.unit
  def test_full_subscription(self):
    now = datetime(2024, 6, 15, 10, 30, 0, tzinfo=UTC)
    period_end = datetime(2024, 7, 15, 10, 30, 0, tzinfo=UTC)

    subscription = MagicMock()
    subscription.id = "bsub_abc123"
    subscription.resource_type = "graph"
    subscription.resource_id = "kg1a2b3c"
    subscription.plan_name = "ladybug-standard"
    subscription.billing_interval = "monthly"
    subscription.status = "active"
    subscription.base_price_cents = 4999
    subscription.current_period_start = now
    subscription.current_period_end = period_end
    subscription.started_at = now
    subscription.canceled_at = None
    subscription.ends_at = None
    subscription.created_at = now

    with patch(
      f"{MODULE}._get_plan_display_name",
      return_value="LadybugDB Standard",
    ):
      response = subscription_to_response(subscription)

    assert response.id == "bsub_abc123"
    assert response.resource_type == "graph"
    assert response.resource_id == "kg1a2b3c"
    assert response.plan_name == "ladybug-standard"
    assert response.plan_display_name == "LadybugDB Standard"
    assert response.billing_interval == "monthly"
    assert response.status == "active"
    assert response.base_price_cents == 4999
    assert response.current_period_start == now.isoformat()
    assert response.current_period_end == period_end.isoformat()
    assert response.started_at == now.isoformat()
    assert response.canceled_at is None
    assert response.ends_at is None
    assert response.created_at == now.isoformat()

  @pytest.mark.unit
  def test_minimal_subscription(self):
    created = datetime(2024, 6, 15, 10, 30, 0, tzinfo=UTC)

    subscription = MagicMock()
    subscription.id = "bsub_xyz789"
    subscription.resource_type = "repository"
    subscription.resource_id = "sec"
    subscription.plan_name = "sec-starter"
    subscription.billing_interval = "monthly"
    subscription.status = "provisioning"
    subscription.base_price_cents = 0
    subscription.current_period_start = None
    subscription.current_period_end = None
    subscription.started_at = None
    subscription.canceled_at = None
    subscription.ends_at = None
    subscription.created_at = created

    with patch(
      f"{MODULE}._get_plan_display_name",
      return_value="SEC Starter",
    ):
      response = subscription_to_response(subscription)

    assert response.id == "bsub_xyz789"
    assert response.resource_type == "repository"
    assert response.resource_id == "sec"
    assert response.current_period_start is None
    assert response.current_period_end is None
    assert response.started_at is None
    assert response.canceled_at is None
    assert response.ends_at is None
    assert response.created_at == created.isoformat()
