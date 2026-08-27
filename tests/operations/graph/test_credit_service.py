"""Comprehensive tests for the credit management service."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import MagicMock, Mock, patch

import pytest

from robosystems.config.graph_tier import GraphTier
from robosystems.models.core import (
  GraphCredits,
  GraphUsage,
  User,
)
from robosystems.operations.graph.credit_service import (
  CreditService,
  get_operation_cost,
)


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
    credits.graph_tier = GraphTier.LADYBUG_STANDARD.value
    credits.is_active = True
    credits.last_allocation_date = datetime.now(UTC)
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
        subscription_tier="ladybug-standard",
        graph_tier=GraphTier.LADYBUG_STANDARD,
      )

      # Verify the result
      assert result == mock_credits
      GraphCredits.create_for_graph.assert_called_once_with(
        graph_id="graph123",
        user_id="user123",
        billing_admin_id="user123",
        monthly_allocation=Decimal(
          "8000"
        ),  # ladybug-standard allocation (~200 agent calls)
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
        graph_tier=GraphTier.LADYBUG_STANDARD,
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
          metadata={"test": "data"},
          drain_on_shortfall=False,
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
    mock_credits.graph_tier = GraphTier.LADYBUG_STANDARD

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
            "graph_tier": "ladybug-standard",
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
    """Test getting operation costs.

    Note: AI operations (agent_call) use token-based pricing via consume_ai_tokens(),
    not fixed costs from get_operation_cost().
    """
    # Clear cache to ensure we get fresh values from configuration
    try:
      from robosystems.middleware.billing.cache import credit_cache

      credit_cache._redis.flushdb()
    except Exception:
      pass  # Cache might not be available in test environment

    # All database operations are free (storage billing removed)
    # MCP calls and other operations are included (0 credits)
    assert get_operation_cost("mcp_call") == Decimal("0")
    assert get_operation_cost("mcp_tool_call") == Decimal("0")

    # Test included operations (all database operations)
    assert get_operation_cost("query") == Decimal("0")
    assert get_operation_cost("analytics") == Decimal("0")
    assert get_operation_cost("import") == Decimal("0")
    assert get_operation_cost("backup") == Decimal("0")
    assert get_operation_cost("sync") == Decimal("0")
    assert get_operation_cost("api_call") == Decimal("0")

    # Test unknown operation (should return 0 in simplified model)
    assert get_operation_cost("unknown_op") == Decimal("0")

  def test_upgrade_graph_tier(self, credit_service, mock_session):
    """Test that graph tier upgrades are not supported."""
    # Attempt to upgrade tier
    result = credit_service.upgrade_graph_tier(
      graph_id="graph123",
      new_tier=GraphTier.LADYBUG_LARGE,
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
    mock_credits.graph_tier = GraphTier.LADYBUG_LARGE.value
    mock_credits.last_allocation_date = datetime.now(UTC)
    mock_credits.get_usage_summary = Mock(
      return_value={
        "current_balance": 750.0,
        "monthly_allocation": 1000.0,
        "graph_tier": "ladybug-large",
        "credit_multiplier": 0.9,
      }
    )

    with (
      patch(
        "robosystems.middleware.billing.cache.credit_cache.get_cached_credit_summary",
        return_value=None,
      ),
      patch("robosystems.middleware.billing.cache.credit_cache.cache_credit_summary"),
      patch.object(GraphCredits, "get_by_graph_id", return_value=mock_credits),
    ):
      result = credit_service.get_credit_summary("graph123")

    assert result["current_balance"] == 750.0
    assert result["monthly_allocation"] == 1000.0
    assert result["graph_tier"] == "ladybug-large"

  def test_allocate_monthly_credits_recent(self, credit_service, mock_session):
    """Test monthly allocation when already allocated recently."""
    mock_credits = Mock(spec=GraphCredits)
    mock_credits.last_allocation_date = datetime.now(UTC) - timedelta(days=5)
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
    mock_credits.last_allocation_date = datetime.now(UTC) - timedelta(days=35)
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


class TestResetGraphPool:
  """reset_graph_pool wires the model reset to commit + cache invalidation.

  The ledger/balance mechanics are covered against a real database in
  tests/models/core/graph/test_graph_credits.py; this pins the service
  half the model test cannot see — the commit and the cache eviction.
  """

  def test_reset_delegates_commits_and_invalidates_cache(self):
    session = MagicMock()
    with patch("robosystems.middleware.billing.cache.credit_cache") as mock_cache:
      service = CreditService(session)
      credits = MagicMock()
      credits.reset_pool.return_value = Decimal("400")
      credits.current_balance = Decimal("1000")
      with patch(
        "robosystems.operations.graph.credit_service.GraphCredits"
      ) as MockCredits:
        MockCredits.get_by_graph_id.return_value = credits
        result = service.reset_graph_pool(
          "kg0123456789abcdef", initiated_by="admin:key", reason="mid-month refresh"
        )

    credits.reset_pool.assert_called_once_with(
      session, initiated_by="admin:key", reason="mid-month refresh"
    )
    session.commit.assert_called_once()
    mock_cache.invalidate_graph_credit_balance.assert_called_once_with(
      "kg0123456789abcdef"
    )
    assert result == {
      "success": True,
      "credits_forfeited": 400.0,
      "new_balance": 1000.0,
    }

  def test_missing_pool_returns_error(self):
    session = MagicMock()
    with patch("robosystems.middleware.billing.cache.credit_cache"):
      service = CreditService(session)
      with patch(
        "robosystems.operations.graph.credit_service.GraphCredits"
      ) as MockCredits:
        MockCredits.get_by_graph_id.return_value = None
        result = service.reset_graph_pool(
          "kg0123456789abcdef", initiated_by="admin:key"
        )

    assert result == {"error": "No credit pool found for graph"}
    session.commit.assert_not_called()


class TestCreditCaching:
  """Test cases for credit caching functionality."""

  def test_cache_balance_and_retrieve(self):
    """Test caching and retrieving credit balance."""
    from robosystems.middleware.billing.cache import CreditCache

    cache = CreditCache()

    # Cache a balance
    cache.cache_graph_credit_balance("graph123", Decimal("1000.0"), "enterprise")

    # Retrieve cached balance
    result = cache.get_cached_graph_credit_balance("graph123")

    # Verify result
    assert result is not None
    balance, tier = result
    assert balance == Decimal("1000.0")
    assert tier == "enterprise"

  def test_cache_invalidation(self):
    """Test cache invalidation."""
    from robosystems.middleware.billing.cache import CreditCache

    cache = CreditCache()

    # Cache balance and summary
    cache.cache_graph_credit_balance("graph123", Decimal("500.0"), "standard")
    cache.cache_credit_summary("graph123", {"current_balance": 500.0})

    # Verify both exist
    assert cache.get_cached_graph_credit_balance("graph123") is not None
    assert cache.get_cached_credit_summary("graph123") is not None

    # Invalidate cache
    cache.invalidate_graph_credit_balance("graph123")

    # Both should be gone
    assert cache.get_cached_graph_credit_balance("graph123") is None
    assert cache.get_cached_credit_summary("graph123") is None

  def test_optimistic_balance_update(self):
    """Test optimistic balance update after consumption."""
    from robosystems.middleware.billing.cache import CreditCache

    cache = CreditCache()

    # Cache initial balance
    cache.cache_graph_credit_balance("graph123", Decimal("1000.0"), "standard")

    # Update balance after consumption
    cache.update_cached_balance_after_consumption("graph123", Decimal("50.0"))

    # Verify balance was updated
    result = cache.get_cached_graph_credit_balance("graph123")
    assert result is not None
    balance, tier = result
    assert balance == Decimal("950.0")
    assert tier == "standard"


@pytest.mark.unit
class TestSharedRepositorySubgraphBilling:
  """AI operations on a shared repository's subgraph bill the parent's pool.

  The exact-only shared-repository check sent sec_historical down the
  user-graph path, where GraphCredits.get_by_graph_id("sec") finds nothing —
  so the operation ran and was never billed. The pool is keyed by the
  repository id, so routing must resolve the parent first.
  """

  @pytest.fixture
  def credit_service(self):
    with patch("robosystems.middleware.billing.cache.credit_cache"):
      return CreditService(MagicMock())

  def test_consume_credits_routes_subgraph_to_parent_repository_pool(
    self, credit_service
  ):
    with patch.object(
      credit_service,
      "consume_shared_repository_credits",
      return_value={"success": True, "credits_consumed": 5.0},
    ) as mock_consume:
      result = credit_service.consume_credits(
        graph_id="sec_historical",
        operation_type="agent_call",
        base_cost=Decimal("5"),
        user_id="user123",
      )

    assert result["success"] is True
    assert mock_consume.call_args.kwargs["repository_name"] == "sec"

  def test_consume_credits_on_the_repository_itself_is_unchanged(self, credit_service):
    with patch.object(
      credit_service,
      "consume_shared_repository_credits",
      return_value={"success": True, "credits_consumed": 1.0},
    ) as mock_consume:
      credit_service.consume_credits(
        graph_id="sec",
        operation_type="query",
        base_cost=Decimal("1"),
        user_id="user123",
      )

    assert mock_consume.call_args.kwargs["repository_name"] == "sec"

  def test_subgraph_without_user_id_is_rejected_not_misrouted(self, credit_service):
    """The shared path requires a user; the failure must say so rather than
    fall through to a nonexistent GraphCredits pool."""
    result = credit_service.consume_credits(
      graph_id="sec_historical",
      operation_type="query",
      base_cost=Decimal("1"),
    )

    assert result["success"] is False
    assert "User ID required" in result["error"]

  def test_check_credit_balance_routes_subgraph_to_parent(self, credit_service):
    with patch.object(
      credit_service,
      "check_shared_repository_access",
      return_value={"has_access": True, "has_sufficient_credits": True},
    ) as mock_check:
      credit_service.check_credit_balance(
        "sec_historical", Decimal("1"), user_id="user123"
      )

    assert mock_check.call_args.kwargs["repository_name"] == "sec"


@pytest.mark.unit
class TestSharedRepositoryPlanIsAPlainString:
  """`UserRepository.repository_plan` is a plain `String` column, but the
  shared-repository read paths kept reading `.repository_plan.value` from the
  enum era. Nothing called them until the operator credit pre-flight made
  `check_credit_balance` their first caller — after which every operator run
  on `sec` was refused with `'str' object has no attribute 'value'`, fail-closed
  and rendered to the user as "Insufficient credits".

  The fixtures carry a real `str`, never a MagicMock attribute: a MagicMock
  answers `.value` happily, which is exactly how the admin router tests hid
  this. `access_level` stays a real enum, so its `.value` is legitimate.
  """

  @pytest.fixture
  def credit_service(self):
    with patch("robosystems.middleware.billing.cache.credit_cache"):
      return CreditService(MagicMock())

  @staticmethod
  def _pool(is_active: bool = True, balance: str = "500"):
    user_repo = MagicMock()
    user_repo.is_active = is_active
    user_repo.repository_type = "sec"
    user_repo.repository_plan = "starter"  # plain str, as the column is
    pool = MagicMock()
    pool.user_repository = user_repo
    pool.current_balance = Decimal(balance)
    return pool

  def test_operator_preflight_on_sec_is_funded(self, credit_service):
    with patch(
      "robosystems.operations.graph.credit_service.UserRepositoryCredits"
    ) as mock_urc:
      mock_urc.get_user_repository_credits.return_value = self._pool()
      result = credit_service.check_credit_balance(
        "sec", Decimal("14"), user_id="user123", operation_type="agent_call"
      )

    assert result["has_sufficient_credits"] is True
    assert result["available_credits"] == 500.0
    assert "error" not in result

  def test_access_check_reports_the_plan_string(self, credit_service):
    with patch(
      "robosystems.operations.graph.credit_service.UserRepositoryCredits"
    ) as mock_urc:
      mock_urc.get_user_repository_credits.return_value = self._pool()
      funded = credit_service.check_shared_repository_access(
        user_id="user123",
        repository_name="sec",
        operation_type="agent_call",
        required_credits=Decimal("14"),
      )
      mock_urc.get_user_repository_credits.return_value = self._pool(is_active=False)
      inactive = credit_service.check_shared_repository_access(
        user_id="user123",
        repository_name="sec",
        operation_type="agent_call",
        required_credits=Decimal("14"),
      )

    assert funded["has_access"] is True
    assert funded["addon_tier"] == "starter"
    assert inactive["has_access"] is False
    assert inactive["addon_tier"] == "starter"

  def test_zero_cost_operation_is_included(self, credit_service):
    with (
      patch(
        "robosystems.operations.graph.credit_service.UserRepositoryCredits"
      ) as mock_urc,
      patch(
        "robosystems.operations.graph.credit_service._get_credit_costs",
        return_value={"query": Decimal("0.0")},
      ),
    ):
      mock_urc.get_user_repository_credits.return_value = self._pool()
      result = credit_service.check_shared_repository_access(
        user_id="user123", repository_name="sec", operation_type="query"
      )

    assert result["operation_included"] is True
    assert result["addon_tier"] == "starter"

  def test_repository_summary_reports_the_plan_string(self, credit_service):
    from robosystems.models.core.user.user_repository import RepositoryAccessLevel

    record = MagicMock()
    record.id = "ur_1"
    record.repository_type = "sec"
    record.repository_plan = "starter"
    record.access_level = RepositoryAccessLevel.READ
    record.user_credits.get_summary.return_value = {"balance": 1.0}
    with patch("robosystems.operations.graph.credit_service.UserRepository") as mock_ur:
      mock_ur.get_user_repositories.return_value = [record]
      summary = credit_service.get_shared_repository_summary("user123")

    assert summary["sec"]["subscription_tier"] == "starter"
    assert summary["sec"]["access_level"] == "read"


class TestCreditConsumptionWritesUsageLedger:
  """Consuming credits records a `graph_usage` row, not just a credit ledger
  transaction.

  These are two different tables. `graph_credit_transactions` is authoritative
  for billing and was always written; `graph_usage` is what the org and
  per-graph usage dashboards read, and only `record_storage_usage` was ever
  wired to it. In production that left `graph_usage` holding storage snapshots
  and nothing else, so an org that had genuinely run AI operations and spent
  credits saw zero of both on its usage view.
  """

  @pytest.fixture
  def mock_session(self):
    return MagicMock()

  @pytest.fixture
  def credit_service(self, mock_session):
    with patch("robosystems.middleware.billing.cache.credit_cache"):
      return CreditService(mock_session)

  def _credits(self, result: dict):
    credits = MagicMock()
    credits.graph_tier = "ladybug-standard"
    credits.consume_credits_atomic = Mock(return_value=result)
    return credits

  def _consume(self, credit_service, credits, **kwargs):
    with (
      patch("robosystems.middleware.billing.cache.credit_cache") as cache,
      patch.object(GraphCredits, "get_by_graph_id", return_value=credits),
      patch.object(GraphUsage, "record_credit_consumption") as record,
    ):
      # Real tuple, not a MagicMock: consume_credits unpacks this.
      cache.get_cached_graph_credit_balance.return_value = (
        Decimal("1000.0"),
        "ladybug-standard",
      )
      result = credit_service.consume_credits(
        graph_id="graph123",
        operation_type="agent_call",
        base_cost=Decimal("10.0"),
        **kwargs,
      )
    return result, record

  def test_successful_consumption_records_a_usage_row(self, credit_service):
    credits = self._credits(
      {
        "success": True,
        "credits_consumed": 10.0,
        "new_balance": 990.0,
        "transaction_id": "t-1",
        "base_cost": 10.0,
      }
    )

    result, record = self._consume(credit_service, credits, user_id="usr_abc")

    assert result["success"] is True
    record.assert_called_once()
    kwargs = record.call_args.kwargs
    assert kwargs["user_id"] == "usr_abc"
    assert kwargs["graph_id"] == "graph123"
    assert kwargs["operation_type"] == "agent_call"
    assert kwargs["credits_consumed"] == Decimal("10.0")

  def test_failed_consumption_records_nothing(self, credit_service):
    """A refusal that deducted nothing (empty pool) writes no usage row."""
    credits = self._credits(
      {
        "success": False,
        "error": "Insufficient credits",
        "available_credits": 0.0,
        "credits_consumed": 0.0,
      }
    )

    result, record = self._consume(credit_service, credits, user_id="usr_abc")

    assert result["success"] is False
    record.assert_not_called()

  def test_drain_to_zero_records_the_partial_debit_and_pins_the_cache(
    self, credit_service
  ):
    """A shortfall drain is real spend: the usage ledger records the drained
    amount, the result reports it, and the cached balance is pinned at zero
    rather than merely invalidated — the pre-flight prefers the cache, and a
    stale positive figure there would re-admit the request the drain stops."""
    credits = self._credits(
      {
        "success": False,
        "error": "Insufficient credits",
        "credits_consumed": 40.0,
        "shortfall": 160.0,
        "drained_to_zero": True,
        "available_credits": 0.0,
        "required_credits": 200.0,
        "base_cost": 200.0,
        "transaction_id": "t-drain",
      }
    )

    with (
      patch("robosystems.middleware.billing.cache.credit_cache") as cache,
      patch.object(GraphCredits, "get_by_graph_id", return_value=credits),
      patch.object(GraphUsage, "record_credit_consumption") as record,
    ):
      cache.get_cached_graph_credit_balance.return_value = (
        Decimal("40.0"),
        "ladybug-standard",
      )
      result = credit_service.consume_credits(
        graph_id="graph123",
        operation_type="ai_tokens",
        base_cost=Decimal("200.0"),
        user_id="usr_abc",
        drain_on_shortfall=True,
      )

      cache.invalidate_graph_credit_balance.assert_called_once_with("graph123")
      cache.cache_graph_credit_balance.assert_called_once()
      pinned = cache.cache_graph_credit_balance.call_args.kwargs
      assert pinned["graph_id"] == "graph123"
      assert pinned["balance"] == Decimal("0")

    assert result["success"] is False
    assert result["drained_to_zero"] is True
    assert result["credits_consumed"] == 40.0
    assert result["shortfall"] == 160.0

    record.assert_called_once()
    kwargs = record.call_args.kwargs
    assert kwargs["credits_consumed"] == Decimal("40.0")
    assert kwargs["operation_type"] == "ai_tokens"

  def test_unattributed_consumption_is_skipped_not_forged(self, credit_service):
    """`GraphUsage.user_id` is NOT NULL and usage is attributed per user, so a
    spend with no user is dropped rather than written against a placeholder."""
    credits = self._credits(
      {
        "success": True,
        "credits_consumed": 10.0,
        "new_balance": 990.0,
        "transaction_id": "t-1",
        "base_cost": 10.0,
      }
    )

    result, record = self._consume(credit_service, credits)

    assert result["success"] is True
    record.assert_not_called()

  def test_a_failing_usage_write_never_breaks_consumption(self, credit_service):
    """The credits are already committed by the time this runs — a reporting
    row must not turn a successful spend into an error."""
    credits = self._credits(
      {
        "success": True,
        "credits_consumed": 10.0,
        "new_balance": 990.0,
        "transaction_id": "t-1",
        "base_cost": 10.0,
      }
    )

    with (
      patch("robosystems.middleware.billing.cache.credit_cache") as cache,
      patch.object(GraphCredits, "get_by_graph_id", return_value=credits),
      patch.object(
        GraphUsage,
        "record_credit_consumption",
        side_effect=RuntimeError("usage table unavailable"),
      ),
    ):
      cache.get_cached_graph_credit_balance.return_value = (
        Decimal("1000.0"),
        "ladybug-standard",
      )
      result = credit_service.consume_credits(
        graph_id="graph123",
        operation_type="agent_call",
        base_cost=Decimal("10.0"),
        user_id="usr_abc",
      )

    assert result["success"] is True
    assert result["credits_consumed"] == 10.0
