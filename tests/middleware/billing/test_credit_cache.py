"""Tests for credit caching functionality (in-memory backend)."""

import json
import time
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from robosystems.middleware.billing.cache import CreditCache, credit_cache


class TestCreditCache:
  """Test cases for CreditCache class."""

  @pytest.fixture
  def cache_instance(self):
    """Create a fresh CreditCache instance."""
    return CreditCache()

  def test_cache_and_retrieve_graph_credit_balance(self, cache_instance):
    """Test caching and retrieving graph credit balance."""
    graph_id = "graph123"
    balance = Decimal("1000.0")
    tier = "ladybug-large"

    # Cache the balance
    cache_instance.cache_graph_credit_balance(graph_id, balance, tier)

    # Retrieve it
    result = cache_instance.get_cached_graph_credit_balance(graph_id)

    assert result is not None
    retrieved_balance, retrieved_tier = result
    assert retrieved_balance == balance
    assert retrieved_tier == tier

  def test_get_cached_balance_cache_miss(self, cache_instance):
    """Test retrieving balance when cache misses."""
    result = cache_instance.get_cached_graph_credit_balance("graph123")
    assert result is None

  def test_get_cached_balance_expired(self, cache_instance):
    """Test retrieving balance after TTL expiry."""
    graph_id = "graph123"
    cache_key = cache_instance._get_graph_credit_key(graph_id)

    # Set with very short TTL (already expired)
    cache_instance._store[cache_key] = (
      json.dumps({"balance": "100.0", "graph_tier": "standard", "cached_at": "now"}),
      time.time() - 1,  # already expired
    )

    result = cache_instance.get_cached_graph_credit_balance(graph_id)
    assert result is None

  def test_update_cached_balance_after_consumption(self, cache_instance):
    """Test optimistic balance update after credit consumption."""
    graph_id = "graph123"
    initial_balance = Decimal("1000.0")
    consumed = Decimal("50.0")

    # Cache initial balance
    cache_instance.cache_graph_credit_balance(graph_id, initial_balance, "standard")

    # Update balance
    cache_instance.update_cached_balance_after_consumption(graph_id, consumed)

    # Verify new balance
    result = cache_instance.get_cached_graph_credit_balance(graph_id)
    assert result is not None
    expected_balance = initial_balance - consumed
    assert result[0] == expected_balance

  def test_update_cached_balance_no_existing_cache(self, cache_instance):
    """Test update when no existing cache exists (no-op)."""
    cache_instance.update_cached_balance_after_consumption("graph123", Decimal("10.0"))
    # Should not raise, just silently do nothing
    result = cache_instance.get_cached_graph_credit_balance("graph123")
    assert result is None

  def test_invalidate_graph_credit_balance(self, cache_instance):
    """Test cache invalidation."""
    graph_id = "graph123"

    # Cache balance and summary
    cache_instance.cache_graph_credit_balance(graph_id, Decimal("500.0"), "standard")
    cache_instance.cache_credit_summary(graph_id, {"current_balance": 500.0})

    # Verify both exist
    assert cache_instance.get_cached_graph_credit_balance(graph_id) is not None
    assert cache_instance.get_cached_credit_summary(graph_id) is not None

    # Invalidate
    cache_instance.invalidate_graph_credit_balance(graph_id)

    # Both should be gone
    assert cache_instance.get_cached_graph_credit_balance(graph_id) is None
    assert cache_instance.get_cached_credit_summary(graph_id) is None

  def test_cache_and_retrieve_credit_summary(self, cache_instance):
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

    # Retrieve it
    result = cache_instance.get_cached_credit_summary(graph_id)
    assert result is not None
    assert result["graph_id"] == graph_id
    assert result["current_balance"] == 5000.0

  def test_cache_and_retrieve_operation_cost(self, cache_instance):
    """Test caching and retrieving operation costs."""
    operation_type = "mcp_call"
    cost = Decimal("10.0")

    # Cache cost
    cache_instance.cache_operation_cost(operation_type, cost)

    # Retrieve it
    result = cache_instance.get_cached_operation_cost(operation_type)
    assert result == cost

  def test_warmup_operation_costs(self, cache_instance):
    """Test warming up operation cost cache."""
    costs = {
      "query": Decimal("1.0"),
      "mcp_call": Decimal("10.0"),
      "agent_call": Decimal("5.0"),
    }

    cache_instance.warmup_operation_costs(costs)

    # Verify all costs were cached
    for op_type, expected_cost in costs.items():
      result = cache_instance.get_cached_operation_cost(op_type)
      assert result == expected_cost

  def test_get_cache_stats(self, cache_instance):
    """Test getting cache statistics."""
    # Populate some entries
    cache_instance.cache_graph_credit_balance("g1", Decimal("100"), "standard")
    cache_instance.cache_graph_credit_balance("g2", Decimal("200"), "standard")
    cache_instance.cache_graph_credit_balance("g3", Decimal("300"), "standard")
    cache_instance.cache_credit_summary("g1", {"current_balance": 100.0})
    cache_instance.cache_credit_summary("g2", {"current_balance": 200.0})
    cache_instance.cache_operation_cost("query", Decimal("1.0"))
    cache_instance.cache_operation_cost("mcp_call", Decimal("10.0"))
    cache_instance.cache_operation_cost("agent_call", Decimal("5.0"))
    cache_instance.cache_operation_cost("embedding", Decimal("2.0"))

    stats = cache_instance.get_cache_stats()

    assert stats["connected"] is True
    assert stats["cache_type"] == "in-memory"
    assert stats["cache_counts"]["graph_balances"] == 3
    assert stats["cache_counts"]["summaries"] == 2
    assert stats["cache_counts"]["operation_costs"] == 4

  def test_invalidate_all_graph_credits(self, cache_instance):
    """Test invalidating all graph credit caches."""
    # Cache several balances and summaries
    for i in range(5):
      cache_instance.cache_graph_credit_balance(
        f"graph{i}", Decimal(str(i * 100)), "standard"
      )
      cache_instance.cache_credit_summary(
        f"graph{i}", {"current_balance": float(i * 100)}
      )

    # Also cache an operation cost (should NOT be invalidated)
    cache_instance.cache_operation_cost("query", Decimal("1.0"))

    # Invalidate all graph credits
    cache_instance.invalidate_all_graph_credits()

    # All graph credits and summaries should be gone
    for i in range(5):
      assert cache_instance.get_cached_graph_credit_balance(f"graph{i}") is None
      assert cache_instance.get_cached_credit_summary(f"graph{i}") is None

    # Operation cost should still be there
    assert cache_instance.get_cached_operation_cost("query") == Decimal("1.0")

  def test_ttl_expiry(self, cache_instance, monkeypatch):
    """Test that entries expire based on TTL."""
    import time as _t
    from types import SimpleNamespace

    cache_key = "test_key"
    # Set with 1-second TTL
    cache_instance._setex(cache_key, 1, "test_value")

    # Should exist immediately
    assert cache_instance._get(cache_key) == "test_value"

    # Advance the cache's clock past expiry instead of sleeping.
    future = _t.time() + 2
    monkeypatch.setattr(
      "robosystems.middleware.billing.cache.time",
      SimpleNamespace(time=lambda: future),
    )

    # Should be gone
    assert cache_instance._get(cache_key) is None

  def test_thread_safety(self, cache_instance):
    """Test that concurrent access doesn't corrupt data."""
    import threading

    errors = []

    def writer(thread_id):
      try:
        for i in range(100):
          cache_instance.cache_graph_credit_balance(
            f"graph_{thread_id}_{i}", Decimal(str(i)), "standard"
          )
      except Exception as e:
        errors.append(e)

    def reader(thread_id):
      try:
        for i in range(100):
          cache_instance.get_cached_graph_credit_balance(f"graph_{thread_id}_{i}")
      except Exception as e:
        errors.append(e)

    threads = []
    for t_id in range(4):
      threads.append(threading.Thread(target=writer, args=(t_id,)))
      threads.append(threading.Thread(target=reader, args=(t_id,)))

    for t in threads:
      t.start()
    for t in threads:
      t.join()

    assert errors == [], f"Thread safety errors: {errors}"


class TestCreditCacheSingleton:
  """Test the global credit_cache singleton."""

  def test_singleton_instance(self):
    """Test that credit_cache is a CreditCache instance."""
    assert isinstance(credit_cache, CreditCache)

  def test_singleton_initialization(self):
    """Test that singleton initializes correctly."""
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
    cache = CreditCache()

    # Simulate cache miss (no data)
    result = cache.get_cached_operation_cost("query")
    assert result is None

    # Simulate cache hit (data present)
    cache.cache_operation_cost("query", Decimal("1.0"))
    result = cache.get_cached_operation_cost("query")
    assert result == Decimal("1.0")
