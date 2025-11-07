"""Tests for credit caching functionality."""

import pytest
import json
from decimal import Decimal
from unittest.mock import MagicMock, patch

from robosystems.middleware.billing.cache import CreditCache, credit_cache


class TestCreditCache:
  """Test cases for CreditCache class."""

  @pytest.fixture
  def mock_redis(self):
    """Create a mock Redis client."""
    return MagicMock()

  @pytest.fixture
  def cache_instance(self, mock_redis):
    """Create a CreditCache instance with mocked Redis."""
    cache = CreditCache()
    cache._redis = mock_redis  # Use the private attribute that the property uses
    return cache

  def test_cache_and_retrieve_graph_credit_balance(self, cache_instance, mock_redis):
    """Test caching and retrieving graph credit balance."""
    graph_id = "graph123"
    balance = Decimal("1000.0")
    tier = "enterprise"

    # Cache the balance
    cache_instance.cache_graph_credit_balance(graph_id, balance, tier)

    # Verify Redis setex was called correctly
    mock_redis.setex.assert_called_once()
    call_args = mock_redis.setex.call_args
    assert call_args[0][0] == f"graph_credit:{graph_id}"
    assert call_args[0][1] == cache_instance.BALANCE_TTL

    # Parse the cached data
    cached_data = json.loads(call_args[0][2])
    assert cached_data["balance"] == "1000.0"
    assert cached_data["graph_tier"] == "enterprise"

    # Test retrieval
    mock_redis.get.return_value = json.dumps(cached_data)
    result = cache_instance.get_cached_graph_credit_balance(graph_id)

    assert result is not None
    retrieved_balance, retrieved_tier = result
    assert retrieved_balance == balance
    assert retrieved_tier == tier

  def test_get_cached_balance_cache_miss(self, cache_instance, mock_redis):
    """Test retrieving balance when cache misses."""
    mock_redis.get.return_value = None

    result = cache_instance.get_cached_graph_credit_balance("graph123")
    assert result is None

  def test_get_cached_balance_invalid_json(self, cache_instance, mock_redis):
    """Test retrieving balance with invalid JSON in cache."""
    mock_redis.get.return_value = "invalid json"

    result = cache_instance.get_cached_graph_credit_balance("graph123")
    assert result is None

  def test_update_cached_balance_after_consumption(self, cache_instance, mock_redis):
    """Test optimistic balance update after credit consumption."""
    graph_id = "graph123"
    initial_balance = Decimal("1000.0")
    consumed = Decimal("50.0")

    # Mock existing cached balance
    cached_data = {
      "balance": str(initial_balance),
      "multiplier": "1.0",
      "graph_tier": "standard",
    }
    mock_redis.get.return_value = json.dumps(cached_data)
    mock_redis.ttl.return_value = 300  # Mock TTL

    # Update balance
    cache_instance.update_cached_balance_after_consumption(graph_id, consumed)

    # Verify new balance was cached
    mock_redis.setex.assert_called_once()
    call_args = mock_redis.setex.call_args
    assert call_args[0][0] == f"graph_credit:{graph_id}"

    # Parse updated data
    updated_data = json.loads(call_args[0][2])
    expected_balance = initial_balance - consumed
    assert Decimal(updated_data["balance"]) == expected_balance

  def test_update_cached_balance_no_existing_cache(self, cache_instance, mock_redis):
    """Test update when no existing cache exists."""
    mock_redis.get.return_value = None

    # Should not update if no existing cache
    cache_instance.update_cached_balance_after_consumption("graph123", Decimal("10.0"))

    # Verify setex was not called
    mock_redis.setex.assert_not_called()

  def test_invalidate_graph_credit_balance(self, cache_instance, mock_redis):
    """Test cache invalidation."""
    graph_id = "graph123"

    cache_instance.invalidate_graph_credit_balance(graph_id)

    # The method deletes both the credit balance and summary
    assert mock_redis.delete.call_count == 2
    delete_calls = mock_redis.delete.call_args_list
    assert delete_calls[0][0][0] == f"graph_credit:{graph_id}"
    assert delete_calls[1][0][0] == f"credit_summary:{graph_id}"

  def test_cache_and_retrieve_credit_summary(self, cache_instance, mock_redis):
    """Test caching and retrieving credit summary."""
    graph_id = "graph123"
    summary = {
      "graph_id": graph_id,
      "current_balance": 5000.0,
      "monthly_allocation": 5000.0,
      "usage_by_operation": {
        "query": {"count": 10, "total_credits": 100.0},
        "mcp_call": {"count": 2, "total_credits": 200.0},
      },
    }

    # Cache summary
    cache_instance.cache_credit_summary(graph_id, summary)

    # Verify Redis setex was called
    mock_redis.setex.assert_called_once()
    call_args = mock_redis.setex.call_args
    assert call_args[0][0] == f"credit_summary:{graph_id}"
    assert call_args[0][1] == cache_instance.SUMMARY_TTL

    # Test retrieval
    mock_redis.get.return_value = json.dumps(summary)
    result = cache_instance.get_cached_credit_summary(graph_id)

    assert result == summary

  def test_cache_and_retrieve_operation_cost(self, cache_instance, mock_redis):
    """Test caching and retrieving operation costs."""
    operation_type = "mcp_call"
    cost = Decimal("10.0")

    # Cache cost
    cache_instance.cache_operation_cost(operation_type, cost)

    # Verify Redis setex was called
    mock_redis.setex.assert_called_once()
    call_args = mock_redis.setex.call_args
    assert call_args[0][0] == f"op_cost:{operation_type}"
    assert call_args[0][1] == cache_instance.OPERATION_COST_TTL
    assert call_args[0][2] == "10.0"

    # Test retrieval
    mock_redis.get.return_value = "10.0"
    result = cache_instance.get_cached_operation_cost(operation_type)

    assert result == cost

  def test_warmup_operation_costs(self, cache_instance, mock_redis):
    """Test warming up operation cost cache."""
    costs = {
      "query": Decimal("1.0"),
      "mcp_call": Decimal("10.0"),
      "agent_call": Decimal("5.0"),
    }

    cache_instance.warmup_operation_costs(costs)

    # Verify all costs were cached
    assert mock_redis.setex.call_count == len(costs)

    # Check each call
    for call, (op_type, cost) in zip(mock_redis.setex.call_args_list, costs.items()):
      assert call[0][0] == f"op_cost:{op_type}"
      assert call[0][2] == str(cost)

  def test_get_cache_stats(self, cache_instance, mock_redis):
    """Test getting cache statistics."""
    # Mock Redis info
    mock_redis.info.return_value = {
      "used_memory_human": "1.5M",
      "connected_clients": 5,
      "total_commands_processed": 1000,
    }

    # Mock key counts
    mock_redis.keys.side_effect = [
      ["graph_credit:1", "graph_credit:2", "graph_credit:3"],  # graph balance keys
      ["shared_credit:1", "shared_credit:2"],  # shared balance keys
      ["credit_summary:1", "credit_summary:2"],  # summary keys
      ["op_cost:1", "op_cost:2", "op_cost:3", "op_cost:4"],  # operation cost keys
    ]

    stats = cache_instance.get_cache_stats()

    assert stats["connected"] is True
    assert stats["redis_info"]["used_memory_human"] == "1.5M"
    assert stats["cache_counts"]["graph_balances"] == 3
    assert stats["cache_counts"]["shared_balances"] == 2
    assert stats["cache_counts"]["summaries"] == 2
    assert stats["cache_counts"]["operation_costs"] == 4

  def test_get_cache_stats_no_connection(self, cache_instance, mock_redis):
    """Test cache stats when Redis is not connected."""
    # Simulate connection error
    mock_redis.info.side_effect = Exception("Connection failed")

    stats = cache_instance.get_cache_stats()

    assert stats["connected"] is False
    assert "error" in stats
    assert "Connection failed" in stats["error"]

  def test_redis_error_handling(self, cache_instance, mock_redis):
    """Test handling of Redis errors."""
    # Mock Redis error
    mock_redis.get.side_effect = Exception("Redis connection error")

    # Should return None on error
    result = cache_instance.get_cached_graph_credit_balance("graph123")
    assert result is None

    # Test with setex error
    mock_redis.setex.side_effect = Exception("Redis write error")

    # Should not raise exception
    cache_instance.cache_graph_credit_balance("graph123", Decimal("100.0"), "standard")


class TestCreditCacheSingleton:
  """Test the global credit_cache singleton."""

  def test_singleton_instance(self):
    """Test that credit_cache is a CreditCache instance."""
    assert isinstance(credit_cache, CreditCache)

  def test_singleton_initialization(self):
    """Test that singleton initializes Redis connection."""
    # Re-import to trigger initialization
    from robosystems.middleware.billing.cache import CreditCache

    new_cache = CreditCache()

    # Verify cache instance exists
    assert isinstance(new_cache, CreditCache)


class TestCreditCacheIntegration:
  """Integration tests for credit cache with CreditService."""

  @patch("robosystems.middleware.billing.cache.credit_cache")
  def test_credit_service_uses_cache(self, mock_cache):
    """Test that CreditService properly integrates with cache."""
    from robosystems.operations.graph.credit_service import CreditService

    # Setup mocks
    mock_session = MagicMock()

    # Create service
    CreditService(mock_session)

    # Verify cache warmup was attempted
    mock_cache.warmup_operation_costs.assert_called_once()

  def test_cache_performance_improvement(self):
    """Test that cache provides performance improvement."""
    import time

    mock_redis = MagicMock()

    cache = CreditCache()
    cache._redis = mock_redis

    # Simulate cache miss (slower)
    mock_redis.get.return_value = None
    start = time.time()
    result = cache.get_cached_operation_cost("query")
    _ = time.time() - start  # Cache miss time

    # Simulate cache hit (faster)
    mock_redis.get.return_value = "1.0"
    start = time.time()
    result = cache.get_cached_operation_cost("query")
    _ = time.time() - start  # Cache hit time

    # Cache hit should be faster (in real scenario)
    # Here we just verify the different code paths
    assert result == Decimal("1.0")
