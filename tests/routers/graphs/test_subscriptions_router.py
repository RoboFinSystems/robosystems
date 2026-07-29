"""Tests for subscription router helper functions."""

from datetime import UTC, datetime
from unittest.mock import MagicMock, Mock, patch

import pytest
from fastapi import HTTPException

from robosystems.models.api.billing.subscription import CancelSubscriptionRequest
from robosystems.models.core.billing import BillingSubscription
from robosystems.routers.graphs.subscriptions import (
  _get_plan_display_name,
  cancel_repository_subscription,
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


class TestCancelRepositorySubscription:
  """Tests for the graph-scoped repository cancel endpoint."""

  def _build_active_repo_subscription(self) -> Mock:
    sub = Mock(spec=BillingSubscription)
    sub.id = "sub_123"
    sub.resource_type = "repository"
    sub.resource_id = "sec"
    sub.plan_name = "starter"
    sub.billing_interval = "monthly"
    sub.status = "active"
    sub.base_price_cents = 999
    sub.current_period_start = datetime(2026, 1, 1, tzinfo=UTC)
    sub.current_period_end = datetime(2026, 2, 1, tzinfo=UTC)
    sub.started_at = datetime(2026, 1, 1, tzinfo=UTC)
    sub.canceled_at = None
    sub.ends_at = None
    sub.stripe_subscription_id = None
    sub.created_at = datetime(2026, 1, 1, tzinfo=UTC)
    return sub

  @pytest.fixture
  def mock_user(self):
    user = MagicMock()
    user.id = "user_123"
    return user

  @pytest.mark.asyncio
  async def test_rejects_user_graph(self, mock_user):
    """Calling cancel with a user-graph id is rejected and points at
    the delete-graph operation."""
    with patch(f"{MODULE}.is_shared_repository", return_value=False):
      with pytest.raises(HTTPException) as exc:
        await cancel_repository_subscription(
          graph_id="kg_user",
          body=CancelSubscriptionRequest(),
          current_user=mock_user,
          db=MagicMock(),
          _rate_limit=None,
        )
    assert exc.value.status_code == 400
    assert "delete-graph" in exc.value.detail
    assert "kg_user" in exc.value.detail

  @pytest.mark.asyncio
  @patch(f"{MODULE}.BillingAuditLog")
  @patch(f"{MODULE}.BillingSubscription.get_by_resource_and_user")
  async def test_period_end_repo_cancel_success(
    self, mock_get_sub, _mock_audit, mock_user
  ):
    """Period-end cancel of a repo sub calls subscription.cancel(immediate=False)."""
    from robosystems.models.core import OrgRole

    mock_org = Mock()
    mock_org.org_id = "org_1"
    mock_org.role = OrgRole.OWNER

    db = MagicMock()
    sub = self._build_active_repo_subscription()
    mock_get_sub.return_value = sub

    with (
      patch(f"{MODULE}.is_shared_repository", return_value=True),
      patch(f"{MODULE}.resolve_shared_repository_parent", return_value="sec"),
      patch(
        "robosystems.models.core.OrgUser.get_user_orgs",
        return_value=[mock_org],
      ),
    ):
      result = await cancel_repository_subscription(
        graph_id="sec",
        body=CancelSubscriptionRequest(),
        current_user=mock_user,
        db=db,
        _rate_limit=None,
      )

    sub.cancel.assert_called_once_with(db, immediate=False)
    assert result.id == "sub_123"

  @pytest.mark.asyncio
  @patch("robosystems.operations.providers.payment_provider.get_payment_provider")
  @patch(f"{MODULE}.BillingAuditLog")
  @patch(f"{MODULE}.BillingSubscription.get_by_resource_and_user")
  async def test_immediate_repo_cancel_calls_stripe_full_cancel(
    self, mock_get_sub, _mock_audit, mock_get_provider, mock_user
  ):
    """Immediate cancel of a Stripe-linked repo sub calls
    provider.cancel_subscription (full Stripe cancel, not period-end modify)."""
    from robosystems.models.core import OrgRole

    mock_org = Mock()
    mock_org.org_id = "org_1"
    mock_org.role = OrgRole.OWNER

    db = MagicMock()
    sub = self._build_active_repo_subscription()
    sub.stripe_subscription_id = "sub_stripe_xyz"
    mock_get_sub.return_value = sub

    provider = MagicMock()
    mock_get_provider.return_value = provider

    with (
      patch(f"{MODULE}.is_shared_repository", return_value=True),
      patch(f"{MODULE}.resolve_shared_repository_parent", return_value="sec"),
      patch(
        "robosystems.models.core.OrgUser.get_user_orgs",
        return_value=[mock_org],
      ),
    ):
      await cancel_repository_subscription(
        graph_id="sec",
        body=CancelSubscriptionRequest(immediate=True, confirm="sec"),
        current_user=mock_user,
        db=db,
        _rate_limit=None,
      )

    provider.cancel_subscription.assert_called_once_with("sub_stripe_xyz")
    provider.stripe.Subscription.modify.assert_not_called()
    sub.cancel.assert_called_once_with(db, immediate=True)

  @pytest.mark.asyncio
  @patch(f"{MODULE}.BillingAuditLog")
  @patch(f"{MODULE}.BillingSubscription.get_by_resource_and_user")
  async def test_immediate_repo_cancel_requires_confirm(
    self, mock_get_sub, _mock_audit, mock_user
  ):
    """Immediate cancel without confirm token returns 400."""
    from robosystems.models.core import OrgRole

    mock_org = Mock()
    mock_org.org_id = "org_1"
    mock_org.role = OrgRole.OWNER

    db = MagicMock()
    sub = self._build_active_repo_subscription()
    mock_get_sub.return_value = sub

    with (
      patch(f"{MODULE}.is_shared_repository", return_value=True),
      patch(f"{MODULE}.resolve_shared_repository_parent", return_value="sec"),
      patch(
        "robosystems.models.core.OrgUser.get_user_orgs",
        return_value=[mock_org],
      ),
    ):
      with pytest.raises(HTTPException) as exc:
        await cancel_repository_subscription(
          graph_id="sec",
          body=CancelSubscriptionRequest(immediate=True),  # no confirm
          current_user=mock_user,
          db=db,
          _rate_limit=None,
        )

    assert exc.value.status_code == 400
    assert "confirm" in exc.value.detail.lower()
    sub.cancel.assert_not_called()

  @pytest.mark.asyncio
  @patch(f"{MODULE}.BillingAuditLog")
  @patch(f"{MODULE}.BillingSubscription.get_by_resource_and_user")
  async def test_immediate_repo_cancel_wrong_confirm(
    self, mock_get_sub, _mock_audit, mock_user
  ):
    """Immediate cancel with mismatched confirm returns 400."""
    from robosystems.models.core import OrgRole

    mock_org = Mock()
    mock_org.org_id = "org_1"
    mock_org.role = OrgRole.OWNER

    db = MagicMock()
    sub = self._build_active_repo_subscription()
    mock_get_sub.return_value = sub

    with (
      patch(f"{MODULE}.is_shared_repository", return_value=True),
      patch(f"{MODULE}.resolve_shared_repository_parent", return_value="sec"),
      patch(
        "robosystems.models.core.OrgUser.get_user_orgs",
        return_value=[mock_org],
      ),
    ):
      with pytest.raises(HTTPException) as exc:
        await cancel_repository_subscription(
          graph_id="sec",
          body=CancelSubscriptionRequest(immediate=True, confirm="industry"),
          current_user=mock_user,
          db=db,
          _rate_limit=None,
        )

    assert exc.value.status_code == 400
    assert "confirm" in exc.value.detail.lower()
    sub.cancel.assert_not_called()

  @pytest.mark.asyncio
  @patch(f"{MODULE}.BillingSubscription.get_by_resource_and_user")
  async def test_no_subscription_returns_404(self, mock_get_sub, mock_user):
    from robosystems.models.core import OrgRole

    mock_org = Mock()
    mock_org.org_id = "org_1"
    mock_org.role = OrgRole.OWNER
    mock_get_sub.return_value = None

    with (
      patch(f"{MODULE}.is_shared_repository", return_value=True),
      patch(f"{MODULE}.resolve_shared_repository_parent", return_value="sec"),
      patch(
        "robosystems.models.core.OrgUser.get_user_orgs",
        return_value=[mock_org],
      ),
    ):
      with pytest.raises(HTTPException) as exc:
        await cancel_repository_subscription(
          graph_id="sec",
          body=CancelSubscriptionRequest(),
          current_user=mock_user,
          db=MagicMock(),
          _rate_limit=None,
        )

    assert exc.value.status_code == 404

  @pytest.mark.asyncio
  async def test_rejects_non_owner(self, mock_user):
    from robosystems.models.core import OrgRole

    mock_org = Mock()
    mock_org.role = OrgRole.ADMIN

    with (
      patch(f"{MODULE}.is_shared_repository", return_value=True),
      patch(f"{MODULE}.resolve_shared_repository_parent", return_value="sec"),
      patch(
        "robosystems.models.core.OrgUser.get_user_orgs",
        return_value=[mock_org],
      ),
    ):
      with pytest.raises(HTTPException) as exc:
        await cancel_repository_subscription(
          graph_id="sec",
          body=CancelSubscriptionRequest(),
          current_user=mock_user,
          db=MagicMock(),
          _rate_limit=None,
        )
    assert exc.value.status_code == 403
    assert "owner" in exc.value.detail.lower()


class TestChangeRepositoryPlan:
  """Plan changes must persist the canonical plan key and never write partially."""

  def _active_subscription(self) -> Mock:
    sub = Mock(spec=BillingSubscription)
    sub.id = "sub_123"
    sub.resource_type = "repository"
    sub.resource_id = "sec"
    sub.plan_name = "starter"
    sub.billing_interval = "monthly"
    sub.status = "active"
    sub.base_price_cents = 2900
    sub.current_period_start = datetime(2026, 1, 1, tzinfo=UTC)
    sub.current_period_end = datetime(2026, 2, 1, tzinfo=UTC)
    sub.started_at = datetime(2026, 1, 1, tzinfo=UTC)
    sub.canceled_at = None
    sub.ends_at = None
    sub.stripe_subscription_id = None
    sub.created_at = datetime(2026, 1, 1, tzinfo=UTC)
    return sub

  def _owner_org(self):
    from robosystems.models.core import OrgRole

    org = Mock()
    org.org_id = "org_1"
    org.role = OrgRole.OWNER
    return org

  @pytest.mark.asyncio
  @patch(f"{MODULE}.BillingAuditLog")
  @patch(f"{MODULE}.UserRepository.get_by_user_and_repository")
  @patch(f"{MODULE}.BillingSubscription.get_by_resource_and_user")
  async def test_prefixed_plan_name_persists_the_canonical_form(
    self, mock_get_sub, mock_get_repo, _mock_audit
  ):
    """new_plan_name='sec-advanced' must persist 'advanced' everywhere.

    The raw string used to be written to both records; every lookup keyed on
    the plan (rate limits, credits, price) then returned empty, which reads
    as "no access" — a 429 on every query until the row was hand-fixed.
    """
    from robosystems.models.api.billing.subscription import (
      UpgradeSubscriptionRequest,
    )
    from robosystems.routers.graphs.subscriptions import _change_repository_plan

    db = MagicMock()
    user = MagicMock()
    user.id = "user_123"
    sub = self._active_subscription()
    mock_get_sub.return_value = sub
    user_repo = Mock()
    mock_get_repo.return_value = user_repo

    with patch(
      "robosystems.models.core.OrgUser.get_user_orgs",
      return_value=[self._owner_org()],
    ):
      await _change_repository_plan(
        "sec",
        UpgradeSubscriptionRequest(new_plan_name="sec-advanced"),
        user,
        db,
      )

    sub.update_plan.assert_called_once_with("advanced", 9900, db)
    assert mock_get_repo.call_args.args[1] == "sec"
    assert user_repo.upgrade_tier.call_args.kwargs["new_plan"] == "advanced"

  @pytest.mark.asyncio
  @patch(f"{MODULE}.BillingAuditLog")
  @patch(f"{MODULE}.UserRepository.get_by_user_and_repository")
  @patch(f"{MODULE}.BillingSubscription.get_by_resource_and_user")
  async def test_missing_access_record_makes_no_changes(
    self, mock_get_sub, mock_get_repo, _mock_audit
  ):
    """The access record is checked before any write: a missing record must
    error out with the subscription untouched, not after mutating it."""
    from robosystems.models.api.billing.subscription import (
      UpgradeSubscriptionRequest,
    )
    from robosystems.routers.graphs.subscriptions import _change_repository_plan

    db = MagicMock()
    user = MagicMock()
    user.id = "user_123"
    sub = self._active_subscription()
    mock_get_sub.return_value = sub
    mock_get_repo.return_value = None

    with patch(
      "robosystems.models.core.OrgUser.get_user_orgs",
      return_value=[self._owner_org()],
    ):
      with pytest.raises(HTTPException) as exc:
        await _change_repository_plan(
          "sec",
          UpgradeSubscriptionRequest(new_plan_name="advanced"),
          user,
          db,
        )

    assert exc.value.status_code == 500
    sub.update_plan.assert_not_called()
