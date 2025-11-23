"""Integration tests for the credit-based system."""

import pytest
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from unittest.mock import Mock, MagicMock, patch

from robosystems.operations.graph.credit_service import CreditService
from robosystems.models.iam import (
  User,
  GraphCredits,
  GraphCreditTransaction,
)
from robosystems.config.graph_tier import GraphTier
from robosystems.models.iam.graph_credits import CreditTransactionType


class TestCreditSystemIntegration:
  """Test cases for credit system integration."""

  @pytest.fixture
  def mock_session(self):
    """Create a mock database session."""
    session = MagicMock()
    return session

  @pytest.fixture
  def credit_service(self, mock_session):
    """Create a CreditService instance with mocked session."""
    with patch("robosystems.middleware.billing.cache.credit_cache"):
      return CreditService(mock_session)

  @pytest.fixture
  def sample_user(self):
    """Create a sample user for testing."""
    user = Mock(spec=User)
    user.id = "user123"
    user.email = "test@example.com"
    user.is_admin = False
    return user

  @pytest.fixture
  def sample_billing_plans(self):
    """Create sample billing plans with credit allocations."""
    plans = {}
    for name, price, credits in [
      ("standard", 4999, 1000),
      ("enterprise", 14999, 5000),
      ("premium", 29999, 20000),
    ]:
      # Use config-based plan instead of database model
      plan_config = {
        "name": name,
        "monthly_credit_allocation": credits,
        "base_price_cents": price,
        "backup_retention_days": 30,
        "priority_support": True,
      }
      plans[name] = plan_config
    return plans

  def test_create_graph_with_credits(
    self, credit_service, mock_session, sample_billing_plans
  ):
    """Test creating a graph automatically creates credit pool."""
    user_id = "user123"
    graph_id = "entity_456"

    # Mock GraphCredits.create_for_graph directly
    mock_credits = Mock(spec=GraphCredits)
    mock_credits.id = "gc_123"
    mock_credits.graph_id = graph_id
    mock_credits.current_balance = Decimal("1000.0")
    mock_credits.monthly_allocation = Decimal("1000.0")
    mock_credits.graph_tier = GraphTier.LADYBUG_STANDARD.value

    with patch.object(GraphCredits, "create_for_graph", return_value=mock_credits):
      # Create credits for graph
      credits = credit_service.create_graph_credits(
        graph_id=graph_id,
        user_id=user_id,
        billing_admin_id=user_id,
        subscription_tier="ladybug-standard",
        graph_tier=GraphTier.LADYBUG_STANDARD,
      )

      # Verify credits were created
      assert credits.graph_id == graph_id
      assert credits.monthly_allocation == Decimal("1000.0")

  def test_ai_credit_consumption(self, credit_service, mock_session):
    """Test AI credit consumption based on actual token usage."""
    graph_id = "kg1a2b3c"

    # Create mock credits with enterprise tier
    mock_credits = Mock(spec=GraphCredits)
    mock_credits.graph_id = graph_id
    mock_credits.current_balance = Decimal("1000.0")
    mock_credits.monthly_allocation = Decimal("5000.0")
    mock_credits.graph_tier = GraphTier.LADYBUG_LARGE.value

    # Mock cache
    with patch("robosystems.middleware.billing.cache.credit_cache") as mock_cache:
      mock_cache.get_cached_graph_credit_balance.return_value = (
        Decimal("1000.0"),
        "enterprise",
      )

      # Mock queries
      mock_session.query().filter_by().first.return_value = mock_credits

      # Mock GraphCredits.get_by_graph_id to return our mock credits
      with patch.object(GraphCredits, "get_by_graph_id", return_value=mock_credits):
        # Mock consume_credits_atomic method to return success response
        mock_credits.consume_credits_atomic = Mock(
          return_value={
            "success": True,
            "credits_consumed": 15.0,  # 10 * 1.5 multiplier
            "new_balance": 985.0,
            "transaction_id": "test-123",
            "base_cost": 10.0,
            "multiplier": 1.5,
            "reservation_id": "res-123",
          }
        )

        # Mock get_operation_cost to return base_cost 10.0
        with patch(
          "robosystems.operations.graph.credit_service.get_operation_cost",
          return_value=Decimal("10.0"),
        ):
          # Mock _get_consumed_this_month to return consumed credits
          with patch.object(
            credit_service, "_get_consumed_this_month", return_value=Decimal("100.0")
          ):
            # Consume AI credits
            result = credit_service.consume_credits(
              graph_id=graph_id,
              operation_type="agent_call",
              base_cost=Decimal("100.0"),
            )

        # Verify credits consumed (no multipliers in simplified model)
        assert result["success"] is True
        assert result["credits_consumed"] == 15.0  # Mocked value
        assert result["base_cost"] == 10.0

  def test_monthly_credit_allocation(self, credit_service, mock_session):
    """Test monthly credit allocation process."""
    # Create mock credits that need allocation
    mock_credits = []
    for i in range(3):
      credit = Mock(spec=GraphCredits)
      credit.id = f"gc_{i}"
      credit.graph_id = f"entity_{i}"
      credit.current_balance = Decimal("100.0")  # Low balance
      credit.monthly_allocation = Decimal("1000.0")
      credit.is_active = True
      credit.last_allocation_date = datetime.now(timezone.utc) - timedelta(days=35)
      credit.allocate_monthly_credits = Mock(return_value=True)
      mock_credits.append(credit)

    # Mock query for credits needing allocation
    mock_query = mock_session.query.return_value
    mock_query.filter.return_value = mock_query
    mock_query.all.return_value = mock_credits

    # Run allocation
    result = credit_service.bulk_allocate_monthly_credits()

    # Verify all credits were allocated
    assert result["allocated_graphs"] == 3
    assert result["total_credits_allocated"] == 3000.0
    for credit in mock_credits:
      credit.allocate_monthly_credits.assert_called_once_with(mock_session)

  def test_credit_balance_checking(self, credit_service, mock_session):
    """Test checking credit balance before operations."""
    graph_id = "kg1a2b3c"
    required_credits = Decimal("50.0")

    # Create mock credits
    mock_credits = Mock(spec=GraphCredits)
    mock_credits.graph_id = graph_id
    mock_credits.current_balance = Decimal("100.0")
    mock_credits.graph_tier = GraphTier.LADYBUG_XLARGE.value

    # Mock cache
    with patch("robosystems.middleware.billing.cache.credit_cache") as mock_cache:
      mock_cache.get_cached_graph_credit_balance.return_value = (
        Decimal("100.0"),
        "premium",
      )

      # Mock query
      mock_session.query().filter_by().first.return_value = mock_credits

      # Check balance
      result = credit_service.check_credit_balance(
        graph_id=graph_id, required_credits=required_credits
      )

      # Verify check results
      assert result["has_sufficient_credits"] is True
      assert result["available_credits"] == 100.0
      assert result["required_credits"] == 50.0  # No multiplier in simplified model

  def test_credit_transaction_history(self, credit_service, mock_session):
    """Test retrieving credit transaction history."""
    graph_id = "kg1a2b3c"

    # Create mock transactions
    mock_transactions = []
    for i, (type_val, amount, desc) in enumerate(
      [
        (CreditTransactionType.ALLOCATION, 1000.0, "Monthly allocation"),
        (CreditTransactionType.CONSUMPTION, -10.0, "API call"),
        (CreditTransactionType.BONUS, 100.0, "Support credit"),
      ]
    ):
      transaction = Mock(spec=GraphCreditTransaction)
      transaction.id = f"tx_{i}"
      transaction.transaction_type = type_val.value
      transaction.amount = Decimal(str(amount))
      transaction.description = desc
      transaction.metadata = {}
      transaction.get_metadata = Mock(return_value={})
      transaction.created_at = datetime.now(timezone.utc)
      mock_transactions.append(transaction)

    # Mock GraphCredits
    mock_credits = Mock(spec=GraphCredits)
    mock_credits.id = "gc_123"

    # Mock GraphCredits.get_by_graph_id
    with patch.object(GraphCredits, "get_by_graph_id", return_value=mock_credits):
      # Mock GraphCreditTransaction.get_transactions_for_graph
      with patch.object(
        GraphCreditTransaction,
        "get_transactions_for_graph",
        return_value=mock_transactions,
      ):
        # Get transactions
        transactions = credit_service.get_credit_transactions(
          graph_id=graph_id, limit=10
        )

    # Verify transaction format
    assert len(transactions) == 3
    assert transactions[0]["type"] == CreditTransactionType.ALLOCATION.value
    assert transactions[0]["amount"] == 1000.0
    assert transactions[1]["type"] == CreditTransactionType.CONSUMPTION.value
    assert transactions[1]["amount"] == -10.0

  def test_credit_summary_with_caching(self, credit_service, mock_session):
    """Test credit summary retrieval with caching."""
    graph_id = "kg1a2b3c"

    # Create mock credits
    mock_credits = Mock(spec=GraphCredits)
    mock_credits.graph_id = graph_id
    mock_credits.current_balance = Decimal("750.0")
    mock_credits.monthly_allocation = Decimal("1000.0")
    mock_credits.graph_tier = GraphTier.LADYBUG_STANDARD.value
    mock_credits.last_allocation_date = datetime.now(timezone.utc).date()

    # Mock cache miss then hit
    with patch("robosystems.middleware.billing.cache.credit_cache") as mock_cache:
      mock_cache.get_cached_credit_summary.return_value = None  # Cache miss

      # Mock GraphCredits.get_by_graph_id
      with patch.object(GraphCredits, "get_by_graph_id", return_value=mock_credits):
        # Mock get_usage_summary method
        mock_credits.get_usage_summary = Mock(
          return_value={
            "graph_id": graph_id,
            "graph_tier": GraphTier.LADYBUG_STANDARD.value,
            "current_balance": 750.0,
            "monthly_allocation": 1000.0,
            "consumed_this_month": 250.0,
            "transaction_count": 10,
            "usage_percentage": 25.0,
            "last_allocation_date": mock_credits.last_allocation_date.isoformat(),
          }
        )

        # Get summary
        summary = credit_service.get_credit_summary(graph_id)

        # Verify summary
        assert summary["current_balance"] == 750.0
        assert summary["monthly_allocation"] == 1000.0
        assert summary["consumed_this_month"] == 250.0
        assert summary["usage_percentage"] == 25.0

  def test_credit_enforcement_prevents_overuse(self, credit_service, mock_session):
    """Test that credit system prevents operations when balance is insufficient."""
    graph_id = "kg1a2b3c"

    # Create mock credits with low balance
    mock_credits = Mock(spec=GraphCredits)
    mock_credits.graph_id = graph_id
    mock_credits.current_balance = Decimal("5.0")  # Only 5 credits left
    mock_credits.graph_tier = GraphTier.LADYBUG_STANDARD.value

    # Mock cache
    with patch("robosystems.middleware.billing.cache.credit_cache") as mock_cache:
      mock_cache.get_cached_graph_credit_balance.return_value = (
        Decimal("5.0"),
        "standard",
      )

      # Mock query
      mock_session.query().filter_by().first.return_value = mock_credits

      # Try to consume more AI credits than available
      result = credit_service.consume_credits(
        graph_id=graph_id,
        operation_type="agent_call",  # AI operation
        base_cost=Decimal("100.0"),
      )

      # Verify operation was blocked
      assert result["success"] is False
      assert "Insufficient credits" in result["error"]
      assert result["required_credits"] == 100.0
      assert result["available_credits"] == 5.0
