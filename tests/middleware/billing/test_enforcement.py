"""
Tests for subscription enforcement middleware.

These tests cover the billing enforcement functions for graph operations,
including provisioning checks and subscription status validation.
"""

from datetime import UTC, datetime, timedelta
from unittest.mock import Mock, patch

import pytest
from fastapi import HTTPException

from robosystems.config.graph_tier import GraphTier
from robosystems.middleware.billing.enforcement import (
  check_can_provision_graph,
  check_graph_subscription_active,
  require_graph_access,
)
from robosystems.models.core.billing import SubscriptionStatus


class TestCheckCanProvisionGraph:
  """Test check_can_provision_graph function."""

  @pytest.fixture
  def mock_session(self):
    """Create a mock database session."""
    return Mock()

  @patch("robosystems.middleware.billing.enforcement.BillingCustomer")
  @patch("robosystems.middleware.billing.enforcement.OrgUser")
  @patch("robosystems.middleware.billing.enforcement.env")
  def test_can_provision_with_valid_subscription(
    self, mock_env, mock_org_user_class, mock_billing_customer_class, mock_session
  ):
    """Test user can provision when they have valid subscription."""
    user_id = "user123"
    requested_tier = GraphTier.LADYBUG_STANDARD

    mock_env.ENVIRONMENT = "prod"
    mock_env.BILLING_ENABLED = True

    mock_org_user = Mock()
    mock_org_user.org_id = "org456"
    mock_org_user_class.return_value = mock_org_user
    mock_session.query.return_value.filter.return_value.first.return_value = (
      mock_org_user
    )

    mock_customer = Mock()
    mock_customer.can_provision_resources.return_value = (True, None)
    mock_billing_customer_class.get_or_create.return_value = mock_customer

    can_provision, error = check_can_provision_graph(
      user_id=user_id,
      requested_tier=requested_tier,
      session=mock_session,
    )

    assert can_provision is True
    assert error is None
    mock_billing_customer_class.get_or_create.assert_called_once_with(
      "org456", mock_session
    )
    mock_customer.can_provision_resources.assert_called_once_with(
      environment="prod", billing_enabled=True
    )

  @patch("robosystems.middleware.billing.enforcement.OrgUser")
  def test_cannot_provision_without_organization(
    self, mock_org_user_class, mock_session
  ):
    """Test user cannot provision without organization membership."""
    user_id = "user123"
    requested_tier = GraphTier.LADYBUG_LARGE

    mock_session.query.return_value.filter.return_value.first.return_value = None

    can_provision, error = check_can_provision_graph(
      user_id=user_id,
      requested_tier=requested_tier,
      session=mock_session,
    )

    assert can_provision is False
    assert "not a member of any organization" in error

  @patch("robosystems.middleware.billing.enforcement.BillingCustomer")
  @patch("robosystems.middleware.billing.enforcement.OrgUser")
  @patch("robosystems.middleware.billing.enforcement.env")
  def test_cannot_provision_billing_restriction(
    self, mock_env, mock_org_user_class, mock_billing_customer_class, mock_session
  ):
    """Test user cannot provision due to billing restrictions."""
    user_id = "user123"
    requested_tier = GraphTier.LADYBUG_XLARGE

    mock_env.ENVIRONMENT = "prod"
    mock_env.BILLING_ENABLED = True

    mock_org_user = Mock()
    mock_org_user.org_id = "org456"
    mock_session.query.return_value.filter.return_value.first.return_value = (
      mock_org_user
    )

    mock_customer = Mock()
    mock_customer.can_provision_resources.return_value = (
      False,
      "Payment method required",
    )
    mock_billing_customer_class.get_or_create.return_value = mock_customer

    can_provision, error = check_can_provision_graph(
      user_id=user_id,
      requested_tier=requested_tier,
      session=mock_session,
    )

    assert can_provision is False
    assert error == "Payment method required"

  @patch("robosystems.middleware.billing.enforcement.BillingCustomer")
  @patch("robosystems.middleware.billing.enforcement.OrgUser")
  @patch("robosystems.middleware.billing.enforcement.env")
  def test_provisioning_in_dev_environment(
    self, mock_env, mock_org_user_class, mock_billing_customer_class, mock_session
  ):
    """Test provisioning check passes correct environment."""
    user_id = "user123"
    requested_tier = GraphTier.LADYBUG_STANDARD

    mock_env.ENVIRONMENT = "dev"
    mock_env.BILLING_ENABLED = False

    mock_org_user = Mock()
    mock_org_user.org_id = "org456"
    mock_session.query.return_value.filter.return_value.first.return_value = (
      mock_org_user
    )

    mock_customer = Mock()
    mock_customer.can_provision_resources.return_value = (True, None)
    mock_billing_customer_class.get_or_create.return_value = mock_customer

    can_provision, error = check_can_provision_graph(
      user_id=user_id,
      requested_tier=requested_tier,
      session=mock_session,
    )

    assert can_provision is True
    mock_customer.can_provision_resources.assert_called_once_with(
      environment="dev", billing_enabled=False
    )

  @patch("robosystems.middleware.billing.enforcement.BillingCustomer")
  @patch("robosystems.middleware.billing.enforcement.OrgUser")
  @patch("robosystems.middleware.billing.enforcement.env")
  def test_all_graph_tiers_can_be_requested(
    self, mock_env, mock_org_user_class, mock_billing_customer_class, mock_session
  ):
    """Test all graph tiers can be requested."""
    user_id = "user123"

    mock_env.ENVIRONMENT = "prod"
    mock_env.BILLING_ENABLED = True

    mock_org_user = Mock()
    mock_org_user.org_id = "org456"
    mock_session.query.return_value.filter.return_value.first.return_value = (
      mock_org_user
    )

    mock_customer = Mock()
    mock_customer.can_provision_resources.return_value = (True, None)
    mock_billing_customer_class.get_or_create.return_value = mock_customer

    for tier in [
      GraphTier.LADYBUG_STANDARD,
      GraphTier.LADYBUG_LARGE,
      GraphTier.LADYBUG_XLARGE,
    ]:
      can_provision, error = check_can_provision_graph(
        user_id=user_id,
        requested_tier=tier,
        session=mock_session,
      )

      assert can_provision is True
      assert error is None


class TestCheckGraphSubscriptionActive:
  """Test check_graph_subscription_active function."""

  @pytest.fixture
  def mock_session(self):
    """Create a mock database session."""
    return Mock()

  @patch("robosystems.middleware.billing.enforcement.BillingSubscription")
  def test_active_subscription(self, mock_subscription_class, mock_session):
    """Test active subscription is recognized."""
    graph_id = "kg1234567890abcdef"

    mock_subscription = Mock()
    mock_subscription.status = SubscriptionStatus.ACTIVE.value
    mock_subscription_class.get_by_resource.return_value = mock_subscription

    is_active, error = check_graph_subscription_active(
      graph_id=graph_id,
      session=mock_session,
    )

    assert is_active is True
    assert error is None
    mock_subscription_class.get_by_resource.assert_called_once_with(
      resource_type="graph", resource_id=graph_id, session=mock_session
    )

  @patch("robosystems.middleware.billing.enforcement.BillingSubscription")
  @patch("robosystems.middleware.billing.enforcement.env")
  def test_no_subscription_billing_enabled(
    self, mock_env, mock_subscription_class, mock_session
  ):
    """Test no subscription with billing enabled returns error."""
    graph_id = "kg1234567890abcdef"

    mock_env.BILLING_ENABLED = True
    mock_subscription_class.get_by_resource.return_value = None

    is_active, error = check_graph_subscription_active(
      graph_id=graph_id,
      session=mock_session,
    )

    assert is_active is False
    assert "No active subscription" in error

  @patch("robosystems.middleware.billing.enforcement.BillingSubscription")
  @patch("robosystems.middleware.billing.enforcement.env")
  def test_no_subscription_billing_disabled(
    self, mock_env, mock_subscription_class, mock_session
  ):
    """Test no subscription with billing disabled passes."""
    graph_id = "kg1234567890abcdef"

    mock_env.BILLING_ENABLED = False
    mock_subscription_class.get_by_resource.return_value = None

    is_active, error = check_graph_subscription_active(
      graph_id=graph_id,
      session=mock_session,
    )

    assert is_active is True
    assert error is None

  @patch("robosystems.middleware.billing.enforcement.BillingSubscription")
  def test_pending_subscription(self, mock_subscription_class, mock_session):
    """Test pending subscription status."""
    graph_id = "kg1234567890abcdef"

    mock_subscription = Mock()
    mock_subscription.status = SubscriptionStatus.PENDING.value
    mock_subscription_class.get_by_resource.return_value = mock_subscription

    is_active, error = check_graph_subscription_active(
      graph_id=graph_id,
      session=mock_session,
    )

    assert is_active is False
    assert "pending activation" in error

  @patch("robosystems.middleware.billing.enforcement.BillingSubscription")
  def test_paused_subscription(self, mock_subscription_class, mock_session):
    """Test paused subscription status."""
    graph_id = "kg1234567890abcdef"

    mock_subscription = Mock()
    mock_subscription.status = SubscriptionStatus.PAUSED.value
    mock_subscription_class.get_by_resource.return_value = mock_subscription

    is_active, error = check_graph_subscription_active(
      graph_id=graph_id,
      session=mock_session,
    )

    assert is_active is False
    assert "paused" in error
    assert "reactivate" in error

  @patch("robosystems.middleware.billing.enforcement.BillingSubscription")
  def test_canceled_subscription(self, mock_subscription_class, mock_session):
    """Test canceled subscription status."""
    graph_id = "kg1234567890abcdef"

    mock_subscription = Mock()
    mock_subscription.status = SubscriptionStatus.CANCELED.value
    mock_subscription_class.get_by_resource.return_value = mock_subscription

    is_active, error = check_graph_subscription_active(
      graph_id=graph_id,
      session=mock_session,
    )

    assert is_active is False
    assert "canceled" in error

  @patch("robosystems.middleware.billing.enforcement.BillingSubscription")
  def test_past_due_subscription(self, mock_subscription_class, mock_session):
    """Test past due subscription status."""
    graph_id = "kg1234567890abcdef"

    mock_subscription = Mock()
    mock_subscription.status = SubscriptionStatus.PAST_DUE.value
    mock_subscription_class.get_by_resource.return_value = mock_subscription

    is_active, error = check_graph_subscription_active(
      graph_id=graph_id,
      session=mock_session,
    )

    assert is_active is False
    assert "past due" in error
    assert "payment" in error

  @patch("robosystems.middleware.billing.enforcement.BillingSubscription")
  def test_unpaid_subscription(self, mock_subscription_class, mock_session):
    """Test unpaid subscription status."""
    graph_id = "kg1234567890abcdef"

    mock_subscription = Mock()
    mock_subscription.status = SubscriptionStatus.UNPAID.value
    mock_subscription_class.get_by_resource.return_value = mock_subscription

    is_active, error = check_graph_subscription_active(
      graph_id=graph_id,
      session=mock_session,
    )

    assert is_active is False
    assert "unpaid" in error
    assert "payment" in error

  @patch("robosystems.middleware.billing.enforcement.BillingSubscription")
  def test_unknown_subscription_status(self, mock_subscription_class, mock_session):
    """Test handling of unknown subscription status."""
    graph_id = "kg1234567890abcdef"

    mock_subscription = Mock()
    mock_subscription.status = "unknown_status"
    mock_subscription_class.get_by_resource.return_value = mock_subscription

    is_active, error = check_graph_subscription_active(
      graph_id=graph_id,
      session=mock_session,
    )

    assert is_active is False
    assert "unknown_status" in error


class TestEdgeCases:
  """Test edge cases and error handling."""

  @pytest.fixture
  def mock_session(self):
    """Create a mock database session."""
    return Mock()

  @patch("robosystems.middleware.billing.enforcement.BillingCustomer")
  @patch("robosystems.middleware.billing.enforcement.OrgUser")
  @patch("robosystems.middleware.billing.enforcement.env")
  def test_logging_on_authorization_failure(
    self, mock_env, mock_org_user_class, mock_billing_customer_class, mock_session
  ):
    """Test that authorization failures are logged."""
    user_id = "user123"
    requested_tier = GraphTier.LADYBUG_LARGE

    mock_env.ENVIRONMENT = "prod"
    mock_env.BILLING_ENABLED = True

    mock_org_user = Mock()
    mock_org_user.org_id = "org456"
    mock_session.query.return_value.filter.return_value.first.return_value = (
      mock_org_user
    )

    mock_customer = Mock()
    mock_customer.can_provision_resources.return_value = (
      False,
      "Exceeded graph limit",
    )
    mock_billing_customer_class.get_or_create.return_value = mock_customer

    with patch("robosystems.middleware.billing.enforcement.logger") as mock_logger:
      can_provision, error = check_can_provision_graph(
        user_id=user_id,
        requested_tier=requested_tier,
        session=mock_session,
      )

      assert can_provision is False
      mock_logger.warning.assert_called_once()
      # Verify the log message contains relevant info
      log_call = mock_logger.warning.call_args
      assert "user123" in str(log_call) or user_id in str(log_call)

  @patch("robosystems.middleware.billing.enforcement.BillingCustomer")
  @patch("robosystems.middleware.billing.enforcement.OrgUser")
  @patch("robosystems.middleware.billing.enforcement.env")
  def test_logging_on_authorization_success(
    self, mock_env, mock_org_user_class, mock_billing_customer_class, mock_session
  ):
    """Test that successful authorizations are logged."""
    user_id = "user123"
    requested_tier = GraphTier.LADYBUG_STANDARD

    mock_env.ENVIRONMENT = "prod"
    mock_env.BILLING_ENABLED = True

    mock_org_user = Mock()
    mock_org_user.org_id = "org456"
    mock_session.query.return_value.filter.return_value.first.return_value = (
      mock_org_user
    )

    mock_customer = Mock()
    mock_customer.can_provision_resources.return_value = (True, None)
    mock_billing_customer_class.get_or_create.return_value = mock_customer

    with patch("robosystems.middleware.billing.enforcement.logger") as mock_logger:
      can_provision, error = check_can_provision_graph(
        user_id=user_id,
        requested_tier=requested_tier,
        session=mock_session,
      )

      assert can_provision is True
      mock_logger.info.assert_called_once()


class TestRequireGraphAccessBillingDisabled:
  """require_graph_access with billing off — the dedicated-tenant posture.

  Read and write access must both bypass the subscription machinery
  entirely: no subscription row exists on those deployments, and the
  lookup should never even run.
  """

  @pytest.fixture
  def mock_session(self):
    return Mock()

  @pytest.fixture(autouse=True)
  def clear_subscription_cache(self):
    from robosystems.middleware.billing import enforcement

    enforcement._subscription_cache.clear()
    yield
    enforcement._subscription_cache.clear()

  def _active_graph(self):
    graph = Mock()
    graph.status = "active"
    graph.is_repository = False
    graph.deleted_at = None
    graph.is_subgraph = False
    graph.parent_graph_id = None
    return graph

  @patch("robosystems.middleware.billing.enforcement.env")
  @patch("robosystems.middleware.billing.enforcement.BillingSubscription")
  @patch("robosystems.middleware.billing.enforcement.Graph")
  def test_read_access_without_subscription(
    self, mock_graph_class, mock_subscription_class, mock_env, mock_session
  ):
    mock_env.BILLING_ENABLED = False
    mock_graph_class.get_by_id.return_value = self._active_graph()
    mock_subscription_class.get_by_resource.return_value = None

    graph = require_graph_access("kg_billoff1", mock_session)

    assert graph is mock_graph_class.get_by_id.return_value
    mock_subscription_class.get_by_resource.assert_not_called()

  @patch("robosystems.middleware.billing.enforcement.env")
  @patch("robosystems.middleware.billing.enforcement.BillingSubscription")
  @patch("robosystems.middleware.billing.enforcement.Graph")
  def test_write_access_without_subscription(
    self, mock_graph_class, mock_subscription_class, mock_env, mock_session
  ):
    mock_env.BILLING_ENABLED = False
    mock_graph_class.get_by_id.return_value = self._active_graph()
    mock_subscription_class.get_by_resource.return_value = None

    graph = require_graph_access("kg_billoff2", mock_session, require_write=True)

    assert graph is mock_graph_class.get_by_id.return_value
    mock_subscription_class.get_by_resource.assert_not_called()


class TestRequireGraphAccessGracePeriod:
  """Canceled-subscription grace period in require_graph_access.

  `BillingSubscription.ends_at` is a timezone-naive DateTime column while
  the comparison side is `datetime.now(UTC)` — without tzinfo
  normalization, every request to a graph in its cancellation grace
  period raised TypeError (a 500) for the entire remaining paid period.
  """

  @pytest.fixture
  def mock_session(self):
    return Mock()

  @pytest.fixture(autouse=True)
  def clear_subscription_cache(self):
    from robosystems.middleware.billing import enforcement

    enforcement._subscription_cache.clear()
    yield
    enforcement._subscription_cache.clear()

  def _active_graph(self):
    graph = Mock()
    graph.status = "active"
    graph.is_repository = False
    graph.deleted_at = None
    graph.is_subgraph = False
    graph.parent_graph_id = None
    return graph

  def _canceled_subscription(self, ends_at):
    subscription = Mock()
    subscription.status = "canceled"
    subscription.ends_at = ends_at
    return subscription

  @patch("robosystems.middleware.billing.enforcement.env")
  @patch("robosystems.middleware.billing.enforcement.BillingSubscription")
  @patch("robosystems.middleware.billing.enforcement.Graph")
  def test_naive_future_ends_at_allows_reads(
    self, mock_graph_class, mock_subscription_class, mock_env, mock_session
  ):
    """A naive (column-typed) future ends_at must grant read access, not 500."""
    mock_env.BILLING_ENABLED = True
    mock_graph_class.get_by_id.return_value = self._active_graph()
    mock_subscription_class.get_by_resource.return_value = self._canceled_subscription(
      datetime.now(UTC).replace(tzinfo=None) + timedelta(days=10)
    )

    graph = require_graph_access("kg_gracetest1", mock_session)

    assert graph is mock_graph_class.get_by_id.return_value

  @patch("robosystems.middleware.billing.enforcement.env")
  @patch("robosystems.middleware.billing.enforcement.BillingSubscription")
  @patch("robosystems.middleware.billing.enforcement.Graph")
  def test_naive_future_ends_at_blocks_writes(
    self, mock_graph_class, mock_subscription_class, mock_env, mock_session
  ):
    """Grace period allows reads only — writes must 403, not 500."""
    mock_env.BILLING_ENABLED = True
    mock_graph_class.get_by_id.return_value = self._active_graph()
    mock_subscription_class.get_by_resource.return_value = self._canceled_subscription(
      datetime.now(UTC).replace(tzinfo=None) + timedelta(days=10)
    )

    with pytest.raises(HTTPException) as exc:
      require_graph_access("kg_gracetest2", mock_session, require_write=True)

    assert exc.value.status_code == 403
    assert "canceled" in exc.value.detail.lower()

  @patch("robosystems.middleware.billing.enforcement.env")
  @patch("robosystems.middleware.billing.enforcement.BillingSubscription")
  @patch("robosystems.middleware.billing.enforcement.Graph")
  def test_naive_past_ends_at_denies_access(
    self, mock_graph_class, mock_subscription_class, mock_env, mock_session
  ):
    """A naive past ends_at means the grace period is over: 403."""
    mock_env.BILLING_ENABLED = True
    mock_graph_class.get_by_id.return_value = self._active_graph()
    mock_subscription_class.get_by_resource.return_value = self._canceled_subscription(
      datetime.now(UTC).replace(tzinfo=None) - timedelta(days=1)
    )

    with pytest.raises(HTTPException) as exc:
      require_graph_access("kg_gracetest3", mock_session)

    assert exc.value.status_code == 403
    assert "ended" in exc.value.detail.lower()

  @patch("robosystems.middleware.billing.enforcement.env")
  @patch("robosystems.middleware.billing.enforcement.BillingSubscription")
  @patch("robosystems.middleware.billing.enforcement.Graph")
  def test_aware_future_ends_at_still_works(
    self, mock_graph_class, mock_subscription_class, mock_env, mock_session
  ):
    """Timezone-aware ends_at (future-proofing) passes through unchanged."""
    mock_env.BILLING_ENABLED = True
    mock_graph_class.get_by_id.return_value = self._active_graph()
    mock_subscription_class.get_by_resource.return_value = self._canceled_subscription(
      datetime.now(UTC) + timedelta(days=10)
    )

    graph = require_graph_access("kg_gracetest4", mock_session)

    assert graph is mock_graph_class.get_by_id.return_value


class TestRequireGraphAccessLifecycle:
  """Lifecycle layer of require_graph_access: gone graphs and subgraphs.

  ``deleted_at`` is stamped first in teardown and ``status`` flipped last, so
  the stamp alone must read as gone. Subgraphs carry no subscription and no
  infrastructure of their own: they are held to the parent's lifecycle and
  billed through the parent.
  """

  @pytest.fixture
  def mock_session(self):
    return Mock()

  @pytest.fixture(autouse=True)
  def clear_subscription_cache(self):
    from robosystems.middleware.billing import enforcement

    enforcement._subscription_cache.clear()
    yield
    enforcement._subscription_cache.clear()

  @staticmethod
  def _graph(graph_id, *, status="active", deleted_at=None, parent=None):
    graph = Mock()
    graph.graph_id = graph_id
    graph.status = status
    graph.is_repository = False
    graph.deleted_at = deleted_at
    graph.is_subgraph = parent is not None
    graph.parent_graph_id = parent
    return graph

  @patch("robosystems.middleware.billing.enforcement.env")
  @patch("robosystems.middleware.billing.enforcement.Graph")
  def test_deleted_at_stamp_is_gone(self, mock_graph_class, mock_env, mock_session):
    mock_env.BILLING_ENABLED = False
    mock_graph_class.get_by_id.return_value = self._graph(
      "kg_stranded", status="suspended", deleted_at=datetime.now(UTC)
    )

    with pytest.raises(HTTPException) as exc:
      require_graph_access("kg_stranded", mock_session)
    assert exc.value.status_code == 404

  @patch("robosystems.middleware.billing.enforcement.env")
  @patch("robosystems.middleware.billing.enforcement.BillingSubscription")
  @patch("robosystems.middleware.billing.enforcement.Graph")
  def test_subgraph_billed_through_parent(
    self, mock_graph_class, mock_subscription_class, mock_env, mock_session
  ):
    """The subscription lookup and its cache entry key on the parent, and the
    subgraph's own row is what is returned."""
    mock_env.BILLING_ENABLED = True
    parent = self._graph("kg_parent")
    child = self._graph("kg_parent_dev", parent="kg_parent")
    mock_graph_class.get_by_id.side_effect = lambda gid, *_a, **_k: {
      "kg_parent": parent,
      "kg_parent_dev": child,
    }[gid]
    active = Mock()
    active.status = SubscriptionStatus.ACTIVE.value
    mock_subscription_class.get_by_resource.return_value = active

    graph = require_graph_access("kg_parent_dev", mock_session, require_write=True)

    assert graph is child
    mock_subscription_class.get_by_resource.assert_called_once_with(
      resource_type="graph", resource_id="kg_parent", session=mock_session
    )
    from robosystems.middleware.billing import enforcement

    assert "kg_parent" in enforcement._subscription_cache
    assert "kg_parent_dev" not in enforcement._subscription_cache

  @patch("robosystems.middleware.billing.enforcement.env")
  @patch("robosystems.middleware.billing.enforcement.Graph")
  def test_subgraph_of_suspended_parent_is_suspended(
    self, mock_graph_class, mock_env, mock_session
  ):
    mock_env.BILLING_ENABLED = False
    parent = self._graph("kg_parent", status="suspended")
    child = self._graph("kg_parent_dev", parent="kg_parent")
    mock_graph_class.get_by_id.side_effect = lambda gid, *_a, **_k: {
      "kg_parent": parent,
      "kg_parent_dev": child,
    }[gid]

    with pytest.raises(HTTPException) as exc:
      require_graph_access("kg_parent_dev", mock_session)
    assert exc.value.status_code == 403

  @patch("robosystems.middleware.billing.enforcement.env")
  @patch("robosystems.middleware.billing.enforcement.Graph")
  def test_subgraph_of_deleted_parent_is_gone(
    self, mock_graph_class, mock_env, mock_session
  ):
    mock_env.BILLING_ENABLED = False
    parent = self._graph("kg_parent", deleted_at=datetime.now(UTC))
    child = self._graph("kg_parent_dev", parent="kg_parent")
    mock_graph_class.get_by_id.side_effect = lambda gid, *_a, **_k: {
      "kg_parent": parent,
      "kg_parent_dev": child,
    }[gid]

    with pytest.raises(HTTPException) as exc:
      require_graph_access("kg_parent_dev", mock_session)
    assert exc.value.status_code == 404
