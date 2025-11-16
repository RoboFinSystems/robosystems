"""Comprehensive tests for the credit management service."""

import pytest
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from unittest.mock import Mock, MagicMock, patch

from robosystems.operations.graph.credit_service import (
  CreditService,
  get_operation_cost,
)
from robosystems.models.iam import (
  User,
  GraphCredits,
)
from robosystems.config.graph_tier import GraphTier


class TestCreditService:
  """Test cases for CreditService class."""

  @pytest.fixture
  def mock_session(self):
    """Create a mock database session."""
    session = MagicMock()
    return session

  @pytest.fixture
  def credit_service(self, mock_session):
    """Create a CreditService instance with mocked session."""
    # Mock the credit_cache at the module level where it's imported
    with patch("robosystems.middleware.billing.cache.credit_cache") as mock_cache:
      # Setup default mock behavior
      mock_cache.warmup_operation_costs.return_value = None
      mock_cache.get_cached_graph_credit_balance.return_value = None
      mock_cache.get_cached_credit_summary.return_value = None
      mock_cache.get_cached_operation_cost.return_value = None
      return CreditService(mock_session)

  @pytest.fixture
  def sample_user(self):
    """Create a sample user for testing."""
    user = Mock(spec=User)
    user.id = "user123"
    user.email = "test@example.com"
    user.is_active = True
    return user

  @pytest.fixture
  def sample_billing_plan(self):
    """Create a sample billing plan."""
    # Create a mock plan object
    plan = Mock()
    plan.name = "standard"
    plan.tier = "standard"
    plan.monthly_credit_allocation = 1000.0
    plan.base_price = 2900
    plan.is_active = True
    return plan

  @pytest.fixture
  def sample_graph_credits(self):
    """Create sample graph credits."""
    credits = Mock(spec=GraphCredits)
    credits.graph_id = "graph123"
    credits.user_id = "user123"
    credits.billing_admin_id = "user123"
    credits.current_balance = Decimal("1000.0")
    credits.monthly_allocation = Decimal("1000.0")
    credits.graph_tier = GraphTier.KUZU_STANDARD.value
    credits.is_active = True
    credits.last_allocation_date = datetime.now(timezone.utc)
    return credits

  def test_create_graph_credits(
    self, credit_service, mock_session, sample_billing_plan
  ):
    """Test creating graph credits for a new graph."""
    # Mock the billing plan query
    mock_session.query().filter().first.return_value = sample_billing_plan

    # Mock GraphCredits.create_for_graph
    mock_credits = Mock(spec=GraphCredits)
    with patch.object(GraphCredits, "create_for_graph", return_value=mock_credits):
      # Create graph credits
      result = credit_service.create_graph_credits(
        graph_id="graph123",
        user_id="user123",
        billing_admin_id="user123",
        subscription_tier="kuzu-standard",
        graph_tier=GraphTier.KUZU_STANDARD,
      )

      # Verify the result
      assert result == mock_credits
      GraphCredits.create_for_graph.assert_called_once_with(
        graph_id="graph123",
        user_id="user123",
        billing_admin_id="user123",
        monthly_allocation=Decimal("100"),
        session=mock_session,
      )

  def test_create_graph_credits_invalid_tier(self, credit_service, mock_session):
    """Test creating graph credits with invalid subscription tier."""
    # Should raise ValueError because the subscription tier is not valid
    with pytest.raises(ValueError, match="No billing plan found for subscription tier"):
      credit_service.create_graph_credits(
        graph_id="graph123",
        user_id="user123",
        billing_admin_id="user123",
        subscription_tier="invalid",
        graph_tier=GraphTier.KUZU_STANDARD,
      )

  def test_consume_ai_credits_success(
    self, credit_service, mock_session, sample_graph_credits
  ):
    """Test successful AI credit consumption."""
    # Mock cache import
    with patch("robosystems.middleware.billing.cache.credit_cache") as mock_cache:
      # Setup mock cache to return cached data
      mock_cache.get_cached_graph_credit_balance.return_value = (
        Decimal("1000.0"),
        "standard",
      )

      # Mock GraphCredits.get_by_graph_id to return our sample
      with patch.object(
        GraphCredits, "get_by_graph_id", return_value=sample_graph_credits
      ):
        # Mock the consume_credits_atomic method to return success
        sample_graph_credits.consume_credits_atomic = Mock(
          return_value={
            "success": True,
            "credits_consumed": 10.0,
            "new_balance": 990.0,
            "transaction_id": "test-123",
            "base_cost": 10.0,
            "multiplier": 1.0,
            "reservation_id": "res-123",
          }
        )

        # Mock _get_consumed_this_month to return 0
        with patch.object(
          credit_service, "_get_consumed_this_month", return_value=Decimal("0")
        ):
          # Consume AI credits
          result = credit_service.consume_credits(
            graph_id="graph123",
            operation_type="agent_call",
            base_cost=Decimal("100.0"),
            metadata={"test": "data"},
          )

        # Verify result
        assert result["success"] is True
        assert result["credits_consumed"] == 10.0
        assert result["remaining_balance"] == 990.0  # 1000 - 10 consumed

        # Verify consume_credits_atomic was called on the instance
        # Note: The amount should be the base_cost (100.0), not the multiplied value
        sample_graph_credits.consume_credits_atomic.assert_called_once_with(
          amount=Decimal("100.0"),
          operation_type="agent_call",
          operation_description="agent_call operation on graph graph123",
          session=mock_session,
          request_id=None,
          user_id=None,
        )

        # Verify cache invalidation was called
        mock_cache.invalidate_graph_credit_balance.assert_called_once_with("graph123")

  def test_free_operations_dont_consume_credits(
    self, credit_service, mock_session, sample_graph_credits
  ):
    """Test that included operations (queries, imports, etc.) don't consume credits."""
    # Mock cache import
    with patch("robosystems.middleware.billing.cache.credit_cache") as mock_cache:
      # Setup mock cache to return cached data
      mock_cache.get_cached_graph_credit_balance.return_value = (
        Decimal("1000.0"),
        "standard",
      )

      # Mock GraphCredits.get_by_graph_id to return our sample
      with patch.object(
        GraphCredits, "get_by_graph_id", return_value=sample_graph_credits
      ):
        # Mock the consume_credits_atomic method
        sample_graph_credits.consume_credits_atomic = Mock(
          return_value={
            "success": True,
            "transaction_id": "test_txn",
            "credits_consumed": Decimal("0"),
            "new_balance": Decimal("1000.0"),
            "base_cost": Decimal("0"),
            "multiplier": Decimal("1.0"),
            "reservation_id": None,
          }
        )

        # Test various included operations
        free_operations = ["query", "import", "backup", "analytics", "sync", "api_call"]

        for operation in free_operations:
          # These operations should have 0 cost
          result = credit_service.consume_credits(
            graph_id="graph123",
            operation_type=operation,
            base_cost=Decimal("0"),  # Included operations have 0 cost
          )

          # Should return success but consume 0 credits
          assert result["success"] is True
          assert result.get("credits_consumed", 0) == 0
          assert result.get("remaining_balance", 1000) == 1000  # Balance unchanged

  def test_consume_credits_insufficient_balance(self, credit_service, mock_session):
    """Test credit consumption with insufficient balance."""
    # Create graph credits with low balance
    mock_credits = Mock(spec=GraphCredits)
    mock_credits.current_balance = Decimal("5.0")
    mock_credits.graph_tier = GraphTier.KUZU_STANDARD

    # Mock cache import
    with patch("robosystems.middleware.billing.cache.credit_cache") as mock_cache:
      # Setup mock cache to return low balance
      mock_cache.get_cached_graph_credit_balance.return_value = (
        Decimal("5.0"),
        "standard",
      )

      # Mock GraphCredits.get_by_graph_id to return our mock credits
      with patch.object(GraphCredits, "get_by_graph_id", return_value=mock_credits):
        # Mock the consume_credits_atomic method to return insufficient balance
        mock_credits.consume_credits_atomic = Mock(
          return_value={
            "success": False,
            "error": "Insufficient credits",
            "required_credits": 10.0,
            "available_credits": 5.0,
          }
        )

        # Try to consume more than available
        result = credit_service.consume_credits(
          graph_id="graph123", operation_type="query", base_cost=Decimal("10.0")
        )

        # Verify failure
        assert result["success"] is False
        assert result["error"] == "Insufficient credits"
        assert result["required_credits"] == 10.0
        assert result["available_credits"] == 5.0

  def test_check_credit_balance(
    self, credit_service, mock_session, sample_graph_credits
  ):
    """Test checking credit balance."""
    # Mock cache import
    with patch("robosystems.middleware.billing.cache.credit_cache") as mock_cache:
      # Setup mock cache
      mock_cache.get_cached_graph_credit_balance.return_value = (
        Decimal("1000.0"),
        "standard",
      )

      # Check balance (should use cached data)
      result = credit_service.check_credit_balance("graph123", Decimal("100.0"))

      # Verify result
      assert result["has_sufficient_credits"] is True
      assert result["available_credits"] == 1000.0
      assert result["required_credits"] == 100.0
      assert result["cached"] is True

  def test_get_credit_summary(self, credit_service, mock_session, sample_graph_credits):
    """Test getting credit summary."""
    # Mock cache import
    with patch("robosystems.middleware.billing.cache.credit_cache") as mock_cache:
      # Setup mock cache to return None (cache miss)
      mock_cache.get_cached_credit_summary.return_value = None

      # Mock GraphCredits.get_by_graph_id to return our sample
      with patch.object(
        GraphCredits, "get_by_graph_id", return_value=sample_graph_credits
      ):
        # Mock the get_usage_summary method
        sample_graph_credits.get_usage_summary = Mock(
          return_value={
            "graph_id": "graph123",
            "graph_tier": "kuzu-standard",
            "credit_multiplier": 1.0,
            "current_balance": 1000.0,
            "monthly_allocation": 1000.0,
            "consumed_this_month": 150.0,
            "transaction_count": 7,
            "usage_percentage": 15.0,
            "last_allocation_date": "2024-01-01T00:00:00",
          }
        )

        # Get summary
        result = credit_service.get_credit_summary("graph123")

        # Verify result
        assert result["graph_id"] == "graph123"
        assert result["current_balance"] == 1000.0
        assert result["monthly_allocation"] == 1000.0

        # Verify get_usage_summary was called
        sample_graph_credits.get_usage_summary.assert_called_once_with(mock_session)

        # Verify cache was set
        mock_cache.cache_credit_summary.assert_called_once()

  def test_allocate_monthly_credits(
    self, credit_service, mock_session, sample_graph_credits
  ):
    """Test monthly credit allocation."""
    # Mock GraphCredits.get_by_graph_id to return our sample
    with patch.object(
      GraphCredits, "get_by_graph_id", return_value=sample_graph_credits
    ):
      # Mock the allocate_monthly_credits method to return True
      sample_graph_credits.allocate_monthly_credits = Mock(return_value=True)

      # Mock cache import
      with patch("robosystems.middleware.billing.cache.credit_cache") as mock_cache:
        # Allocate credits
        result = credit_service.allocate_monthly_credits("graph123")

        # Verify result
        assert result["success"] is True
        assert result["allocated_credits"] == 1000.0

        # Verify allocate_monthly_credits was called
        sample_graph_credits.allocate_monthly_credits.assert_called_once_with(
          mock_session
        )

        # Verify cache was invalidated
        mock_cache.invalidate_graph_credit_balance.assert_called_once_with("graph123")

  def test_get_operation_cost(self):
    """Test getting operation costs."""
    # Clear cache to ensure we get fresh values from configuration
    try:
      from robosystems.middleware.billing.cache import credit_cache

      credit_cache._redis.flushdb()
    except Exception:
      pass  # Cache might not be available in test environment

    # Test AI operations (consume credits)
    assert get_operation_cost("agent_call") == Decimal("100")
    assert get_operation_cost("ai_analysis") == Decimal("100")
    assert get_operation_cost("mcp_call") == Decimal("0")  # MCP calls are included

    # Test included operations (all database operations)
    assert get_operation_cost("query") == Decimal("0")
    assert get_operation_cost("cypher_query") == Decimal("0")
    assert get_operation_cost("analytics") == Decimal("0")
    assert get_operation_cost("import") == Decimal("0")
    assert get_operation_cost("backup") == Decimal("0")
    assert get_operation_cost("sync") == Decimal("0")
    assert get_operation_cost("api_call") == Decimal("0")
    assert get_operation_cost("schema_query") == Decimal("0")

    # Test unknown operation (should return 0 in simplified model)
    assert get_operation_cost("unknown_op") == Decimal("0")

  def test_upgrade_graph_tier(self, credit_service, mock_session):
    """Test that graph tier upgrades are not supported."""
    # Attempt to upgrade tier
    result = credit_service.upgrade_graph_tier(
      graph_id="graph123",
      new_tier=GraphTier.KUZU_LARGE,
      user_subscription_tier="enterprise",
    )

    # Verify result shows it's not supported
    assert result["success"] is False
    assert result["error"] == "Graph tier upgrades are not supported"
    assert "architecturally optimized" in result["message"]

  def test_get_operation_cost_with_unknown_type(self):
    """Test get_operation_cost with unknown operation type."""
    cost = get_operation_cost("unknown_operation_type")
    assert isinstance(cost, (int, float, Decimal))
    assert cost >= 0  # Should return a default cost

  def test_get_credit_summary_with_cache(self, credit_service, mock_session):
    """Test getting credit summary with cached value."""
    # Mock the cache module's get_cached_credit_summary function
    with patch(
      "robosystems.middleware.billing.cache.credit_cache.get_cached_credit_summary"
    ) as mock_get_summary:
      mock_get_summary.return_value = {
        "current_balance": 500.0,
        "monthly_allocation": 1000.0,
      }

      result = credit_service.get_credit_summary("graph123")

      assert result["current_balance"] == 500.0
      assert result["monthly_allocation"] == 1000.0
      # Verify cache was checked
      mock_get_summary.assert_called_once_with("graph123")

  def test_get_credit_summary_without_cache(self, credit_service, mock_session):
    """Test getting credit summary without cached value."""
    mock_credits = Mock(spec=GraphCredits)
    mock_credits.current_balance = Decimal("750.0")
    mock_credits.monthly_allocation = Decimal("1000.0")
    mock_credits.graph_tier = GraphTier.KUZU_LARGE.value
    mock_credits.last_allocation_date = datetime.now(timezone.utc)
    mock_credits.get_usage_summary = Mock(
      return_value={
        "current_balance": 750.0,
        "monthly_allocation": 1000.0,
        "graph_tier": "kuzu-large",
        "credit_multiplier": 0.9,
      }
    )

    with patch(
      "robosystems.middleware.billing.cache.credit_cache.get_cached_credit_summary",
      return_value=None,
    ):
      with patch(
        "robosystems.middleware.billing.cache.credit_cache.cache_credit_summary"
      ):
        with patch.object(GraphCredits, "get_by_graph_id", return_value=mock_credits):
          result = credit_service.get_credit_summary("graph123")

    assert result["current_balance"] == 750.0
    assert result["monthly_allocation"] == 1000.0
    assert result["graph_tier"] == "kuzu-large"

  def test_allocate_monthly_credits_recent(self, credit_service, mock_session):
    """Test monthly allocation when already allocated recently."""
    mock_credits = Mock(spec=GraphCredits)
    mock_credits.last_allocation_date = datetime.now(timezone.utc) - timedelta(days=5)
    mock_credits.graph_id = "graph123"
    mock_credits.allocate_monthly_credits = Mock(return_value=False)

    with patch.object(GraphCredits, "get_by_graph_id", return_value=mock_credits):
      result = credit_service.allocate_monthly_credits("graph123")

    assert result["success"] is False
    assert "not due yet" in result["message"]
    mock_session.commit.assert_not_called()

  def test_allocate_monthly_credits_overdue(self, credit_service, mock_session):
    """Test monthly allocation for overdue credits."""
    mock_credits = Mock(spec=GraphCredits)
    mock_credits.last_allocation_date = datetime.now(timezone.utc) - timedelta(days=35)
    mock_credits.monthly_allocation = Decimal("1000.0")
    mock_credits.current_balance = Decimal("1100.0")  # After allocation
    mock_credits.graph_id = "graph123"
    mock_credits.is_active = True
    mock_credits.allocate_monthly_credits = Mock(return_value=True)

    with patch.object(GraphCredits, "get_by_graph_id", return_value=mock_credits):
      result = credit_service.allocate_monthly_credits("graph123")

    assert result["success"] is True
    assert result["allocated_credits"] == 1000.0
    assert result["new_balance"] == 1100.0
    mock_session.commit.assert_called_once()


class TestCreditCaching:
  """Test cases for credit caching functionality."""

  @pytest.fixture
  def mock_redis(self):
    """Create a mock Redis client."""
    mock_redis = MagicMock()
    return mock_redis

  def test_cache_balance_and_retrieve(self, mock_redis):
    """Test caching and retrieving credit balance."""
    from robosystems.middleware.billing.cache import CreditCache

    cache = CreditCache()
    cache._redis = mock_redis

    # Mock Redis get to return cached data
    mock_redis.get.return_value = (
      '{"balance": "1000.0", "multiplier": "2.0", "graph_tier": "enterprise"}'
    )

    # Retrieve cached balance
    result = cache.get_cached_graph_credit_balance("graph123")

    # Verify result
    assert result is not None
    balance, tier = result
    assert balance == Decimal("1000.0")
    assert tier == "enterprise"

    # Verify Redis was called correctly
    mock_redis.get.assert_called_once_with("graph_credit:graph123")

  def test_cache_invalidation(self, mock_redis):
    """Test cache invalidation."""
    from robosystems.middleware.billing.cache import CreditCache

    cache = CreditCache()
    cache._redis = mock_redis

    # Invalidate cache
    cache.invalidate_graph_credit_balance("graph123")

    # Verify Redis delete was called
    # The method deletes both the credit balance and summary
    assert mock_redis.delete.call_count == 2
    delete_calls = mock_redis.delete.call_args_list
    assert delete_calls[0][0][0] == "graph_credit:graph123"
    assert delete_calls[1][0][0] == "credit_summary:graph123"

  def test_optimistic_balance_update(self, mock_redis):
    """Test optimistic balance update after consumption."""
    from robosystems.middleware.billing.cache import CreditCache

    cache = CreditCache()
    cache._redis = mock_redis

    # Mock existing cached balance
    mock_redis.get.return_value = (
      '{"balance": "1000.0", "multiplier": "1.0", "graph_tier": "standard"}'
    )
    mock_redis.ttl.return_value = 300  # Mock TTL

    # Update balance after consumption
    cache.update_cached_balance_after_consumption("graph123", Decimal("50.0"))

    # Verify balance was updated
    mock_redis.setex.assert_called_once()
    call_args = mock_redis.setex.call_args
    assert call_args[0][0] == "graph_credit:graph123"
    assert '"balance": "950.0"' in call_args[0][2]
