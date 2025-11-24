"""Tests for graph provisioning Celery task."""

import pytest
from unittest.mock import Mock, patch
from robosystems.tasks.graph_operations.provision_graph import provision_graph_task
from robosystems.models.billing import BillingSubscription
from robosystems.models.iam import User


class TestProvisionGraphTask:
  """Tests for provision_graph_task."""

  @pytest.fixture
  def mock_user(self):
    user = Mock(spec=User)
    user.id = "user_123"
    user.email = "test@example.com"
    return user

  @pytest.fixture
  def mock_subscription(self):
    sub = Mock(spec=BillingSubscription)
    sub.id = "sub_456"
    sub.org_id = "org_123"
    sub.resource_type = "graph"
    sub.plan_name = "standard"
    sub.status = "provisioning"
    sub.subscription_metadata = {}
    return sub

  @patch("robosystems.tasks.graph_operations.provision_graph.get_db_session")
  @patch("robosystems.tasks.graph_operations.create_graph.create_graph_task")
  def test_provision_graph_success(
    self, mock_create_graph, mock_get_db, mock_user, mock_subscription
  ):
    """Test successful graph provisioning."""
    mock_session = Mock()
    mock_get_db.return_value = iter([mock_session])
    mock_session.query.return_value.filter.return_value.first.return_value = (
      mock_subscription
    )

    mock_create_graph.return_value = {"graph_id": "kg_789"}

    provision_graph_task(  # type: ignore[call-arg]
      user_id="user_123",
      subscription_id="sub_456",
      graph_config={"tier": "standard"},
    )

    mock_create_graph.assert_called_once()
    assert mock_subscription.resource_id == "kg_789"
    mock_subscription.activate.assert_called_once_with(mock_session)

  @patch("robosystems.tasks.graph_operations.provision_graph.get_db_session")
  def test_provision_graph_subscription_not_found(self, mock_get_db):
    """Test handling when subscription is not found."""
    mock_session = Mock()
    mock_get_db.return_value = iter([mock_session])
    mock_session.query.return_value.filter.return_value.first.return_value = None

    with pytest.raises(Exception, match="Subscription sub_123 not found"):
      provision_graph_task(  # type: ignore[call-arg]
        user_id="user_999", subscription_id="sub_123", graph_config={}
      )

  @patch("robosystems.tasks.graph_operations.provision_graph.get_db_session")
  @patch("robosystems.tasks.graph_operations.create_graph.create_graph_task")
  def test_provision_graph_failure_updates_subscription(
    self, mock_create_graph, mock_get_db, mock_user, mock_subscription
  ):
    """Test that provisioning failure updates subscription."""
    mock_session = Mock()
    mock_get_db.return_value = iter([mock_session])
    mock_session.query.return_value.filter.return_value.first.return_value = (
      mock_subscription
    )

    mock_create_graph.side_effect = Exception("Allocation failed")

    with pytest.raises(Exception):
      provision_graph_task(  # type: ignore[call-arg]
        user_id="user_123", subscription_id="sub_456", graph_config={}
      )

    assert mock_subscription.status == "failed"
    assert "error" in mock_subscription.subscription_metadata

  @patch("robosystems.tasks.graph_operations.provision_graph.get_db_session")
  @patch(
    "robosystems.tasks.graph_operations.create_entity_graph.create_entity_with_new_graph_task"
  )
  def test_provision_entity_graph_success(
    self, mock_create_entity, mock_get_db, mock_user, mock_subscription
  ):
    """Test successful entity graph provisioning."""
    mock_session = Mock()
    mock_get_db.return_value = iter([mock_session])
    mock_subscription.plan_name = "ladybug-standard"
    mock_session.query.return_value.filter.return_value.first.return_value = (
      mock_subscription
    )

    mock_create_entity.return_value = {"graph_id": "kg_entity_123"}

    graph_config = {
      "graph_type": "entity",
      "entity_name": "Test Entity",
      "entity_identifier": "12-3456789",
      "entity_identifier_type": "ein",
      "graph_name": "Test Entity Graph",
      "description": "Test entity graph",
      "schema_extensions": ["roboledger"],
      "tags": ["test", "entity"],
      "tier": "ladybug-standard",
      "create_entity": True,
    }

    result = provision_graph_task(  # type: ignore[call-arg]
      user_id="user_123",
      subscription_id="sub_456",
      graph_config=graph_config,
    )

    assert result["graph_id"] == "kg_entity_123"
    mock_create_entity.assert_called_once()

    call_args = mock_create_entity.call_args[0]
    entity_data = call_args[1]
    assert entity_data["name"] == "Test Entity"
    assert entity_data["ein"] == "12-3456789"
    assert entity_data["extensions"] == ["roboledger"]
    assert entity_data["graph_name"] == "Test Entity Graph"
    assert entity_data["skip_billing"] is True
    assert mock_subscription.resource_id == "kg_entity_123"

  @patch("robosystems.tasks.graph_operations.provision_graph.get_db_session")
  @patch(
    "robosystems.tasks.graph_operations.create_entity_graph.create_entity_with_new_graph_task"
  )
  def test_provision_company_graph_success(
    self, mock_create_entity, mock_get_db, mock_user, mock_subscription
  ):
    """Test successful company graph provisioning."""
    mock_session = Mock()
    mock_get_db.return_value = iter([mock_session])
    mock_subscription.plan_name = "ladybug-large"
    mock_session.query.return_value.filter.return_value.first.return_value = (
      mock_subscription
    )

    mock_create_entity.return_value = {"graph_id": "kg_company_456"}

    graph_config = {
      "graph_type": "company",
      "company_name": "Test Company",
      "company_identifier": "0001234567",
      "company_identifier_type": "cik",
      "graph_name": "Test Company Graph",
      "tier": "ladybug-large",
    }

    result = provision_graph_task(  # type: ignore[call-arg]
      user_id="user_123",
      subscription_id="sub_456",
      graph_config=graph_config,
    )

    assert result["graph_id"] == "kg_company_456"
    call_args = mock_create_entity.call_args[0]
    entity_data = call_args[1]
    assert entity_data["name"] == "Test Company"
    assert entity_data["cik"] == "0001234567"

  @patch("robosystems.tasks.graph_operations.provision_graph.get_db_session")
  @patch("robosystems.tasks.graph_operations.create_graph.create_graph_task")
  def test_provision_graph_tier_mismatch_uses_subscription_tier(
    self, mock_create_graph, mock_get_db, mock_user, mock_subscription
  ):
    """Test tier mismatch detection and correction."""
    mock_session = Mock()
    mock_get_db.return_value = iter([mock_session])
    mock_subscription.plan_name = "ladybug-standard"
    mock_session.query.return_value.filter.return_value.first.return_value = (
      mock_subscription
    )

    mock_create_graph.return_value = {"graph_id": "kg_789"}

    graph_config = {
      "tier": "ladybug-xlarge",
    }

    provision_graph_task(  # type: ignore[call-arg]
      user_id="user_123",
      subscription_id="sub_456",
      graph_config=graph_config,
    )

    call_args = mock_create_graph.call_args[0]
    task_data = call_args[1]
    assert task_data["tier"] == "ladybug-standard"
    assert task_data["graph_tier"] == "ladybug-standard"

  @patch("robosystems.tasks.graph_operations.provision_graph.get_db_session")
  @patch("robosystems.tasks.graph_operations.create_graph.create_graph_task")
  def test_provision_graph_with_custom_schema(
    self, mock_create_graph, mock_get_db, mock_user, mock_subscription
  ):
    """Test provisioning with custom schema."""
    mock_session = Mock()
    mock_get_db.return_value = iter([mock_session])
    mock_subscription.plan_name = "ladybug-standard"
    mock_session.query.return_value.filter.return_value.first.return_value = (
      mock_subscription
    )

    mock_create_graph.return_value = {"graph_id": "kg_custom"}

    custom_schema = {
      "nodes": {"Product": {"properties": {"name": "STRING"}}},
      "edges": {},
    }

    graph_config = {
      "tier": "ladybug-standard",
      "graph_name": "Custom Graph",
      "custom_schema": custom_schema,
    }

    provision_graph_task(  # type: ignore[call-arg]
      user_id="user_123",
      subscription_id="sub_456",
      graph_config=graph_config,
    )

    call_args = mock_create_graph.call_args[0]
    task_data = call_args[1]
    assert task_data["custom_schema"] == custom_schema

  @patch("robosystems.tasks.graph_operations.provision_graph.get_db_session")
  @patch("robosystems.tasks.graph_operations.create_graph.create_graph_task")
  @patch(
    "robosystems.operations.graph.subscription_service.generate_subscription_invoice"
  )
  def test_provision_graph_generates_invoice_for_manual_billing(
    self, mock_generate_invoice, mock_create_graph, mock_get_db, mock_subscription
  ):
    """Test invoice generation for manual billing customers."""
    mock_session = Mock()
    mock_get_db.return_value = iter([mock_session])
    mock_subscription.plan_name = "ladybug-standard"
    mock_subscription.stripe_subscription_id = None
    mock_session.query.return_value.filter.return_value.first.return_value = (
      mock_subscription
    )

    mock_create_graph.return_value = {"graph_id": "kg_invoice"}

    from robosystems.models.billing import BillingCustomer

    mock_customer = Mock(spec=BillingCustomer)
    mock_customer.invoice_billing_enabled = True
    with patch.object(BillingCustomer, "get_by_user_id", return_value=mock_customer):
      provision_graph_task(  # type: ignore[call-arg]
        user_id="user_123",
        subscription_id="sub_456",
        graph_config={"tier": "ladybug-standard"},
      )

    mock_generate_invoice.assert_called_once()
    call_kwargs = mock_generate_invoice.call_args[1]
    assert call_kwargs["subscription"] == mock_subscription
    assert call_kwargs["customer"] == mock_customer
    assert "Graph Database Subscription" in call_kwargs["description"]

  @patch("robosystems.tasks.graph_operations.provision_graph.get_db_session")
  @patch("robosystems.tasks.graph_operations.create_graph.create_graph_task")
  def test_provision_graph_skips_invoice_for_stripe_subscription(
    self, mock_create_graph, mock_get_db, mock_subscription
  ):
    """Test no invoice generation for Stripe-managed subscriptions."""
    mock_session = Mock()
    mock_get_db.return_value = iter([mock_session])
    mock_subscription.plan_name = "ladybug-standard"
    mock_subscription.stripe_subscription_id = "sub_stripe_123"
    mock_session.query.return_value.filter.return_value.first.return_value = (
      mock_subscription
    )

    mock_create_graph.return_value = {"graph_id": "kg_stripe"}

    with patch(
      "robosystems.operations.graph.subscription_service.generate_subscription_invoice"
    ) as mock_generate:
      provision_graph_task(  # type: ignore[call-arg]
        user_id="user_123",
        subscription_id="sub_456",
        graph_config={"tier": "ladybug-standard"},
      )

      mock_generate.assert_not_called()

  @patch("robosystems.tasks.graph_operations.provision_graph.get_db_session")
  @patch("robosystems.tasks.graph_operations.create_graph.create_graph_task")
  def test_provision_graph_wrong_status_warning(
    self, mock_create_graph, mock_get_db, mock_subscription
  ):
    """Test warning when subscription is not in provisioning status."""
    mock_session = Mock()
    mock_get_db.return_value = iter([mock_session])
    mock_subscription.plan_name = "ladybug-standard"
    mock_subscription.status = "active"
    mock_session.query.return_value.filter.return_value.first.return_value = (
      mock_subscription
    )

    mock_create_graph.return_value = {"graph_id": "kg_789"}

    provision_graph_task(  # type: ignore[call-arg]
      user_id="user_123",
      subscription_id="sub_456",
      graph_config={"tier": "ladybug-standard"},
    )

    mock_create_graph.assert_called_once()
    assert mock_subscription.resource_id == "kg_789"

  @patch("robosystems.tasks.graph_operations.provision_graph.get_db_session")
  @patch("robosystems.tasks.graph_operations.create_graph.create_graph_task")
  def test_provision_graph_error_handling_subscription_update_fails(
    self, mock_create_graph, mock_get_db, mock_subscription
  ):
    """Test error handling when subscription status update fails."""
    mock_session = Mock()
    mock_get_db.return_value = iter([mock_session])
    mock_subscription.plan_name = "ladybug-standard"

    call_count = {"count": 0}

    def query_side_effect(*args):
      mock_query = Mock()
      mock_filter = Mock()
      if call_count["count"] == 0:
        mock_filter.first.return_value = mock_subscription
      else:
        mock_filter.first.return_value = None
      mock_query.filter.return_value = mock_filter
      call_count["count"] += 1
      return mock_query

    mock_session.query.side_effect = query_side_effect
    mock_create_graph.side_effect = Exception("Graph creation failed")

    with pytest.raises(Exception, match="Graph creation failed"):
      provision_graph_task(  # type: ignore[call-arg]
        user_id="user_123",
        subscription_id="sub_456",
        graph_config={"tier": "ladybug-standard"},
      )

  @patch("robosystems.tasks.graph_operations.provision_graph.get_db_session")
  @patch("robosystems.tasks.graph_operations.create_graph.create_graph_task")
  def test_provision_graph_default_tier(
    self, mock_create_graph, mock_get_db, mock_subscription
  ):
    """Test default tier is used when not specified."""
    mock_session = Mock()
    mock_get_db.return_value = iter([mock_session])
    mock_subscription.plan_name = "ladybug-standard"
    mock_session.query.return_value.filter.return_value.first.return_value = (
      mock_subscription
    )

    mock_create_graph.return_value = {"graph_id": "kg_default"}

    provision_graph_task(  # type: ignore[call-arg]
      user_id="user_123",
      subscription_id="sub_456",
      graph_config={},
    )

    call_args = mock_create_graph.call_args[0]
    task_data = call_args[1]
    assert task_data["tier"] == "ladybug-standard"
