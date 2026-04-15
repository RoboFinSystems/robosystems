"""Tests for credit consumption flow.

Exercises: routers/graphs/credits.py → operations/graph/credit_service.py → models/core/graph/graph_credits.py
"""

from decimal import Decimal

from robosystems.operations.graph.credit_service import (
  CreditService,
  get_operation_cost,
)


class TestCreditServiceFlow:
  """Test credit service operations with real database."""

  def test_get_operation_cost_known_types(self):
    """All configured operation types should return their cost."""
    for op_type in ["query", "mcp_call", "mcp_tool_call", "backup", "analytics"]:
      cost = get_operation_cost(op_type)
      assert isinstance(cost, Decimal)

  def test_get_operation_cost_unknown_returns_zero(self):
    """Unknown operations (database ops) should cost 0 credits."""
    cost = get_operation_cost("unknown_operation_type")
    assert cost == Decimal("0")

  def test_create_graph_credits(self, test_db, test_user, sample_graph):
    """Create credit pool for a new graph."""
    service = CreditService(test_db)
    credits = service.create_graph_credits(
      graph_id=sample_graph.graph_id,
      user_id=str(test_user.id),
      billing_admin_id=str(test_user.id),
      subscription_tier="ladybug-standard",
    )
    assert credits is not None
    assert credits.graph_id == sample_graph.graph_id
    assert credits.current_balance > 0

  def test_get_credit_summary(self, test_db, test_graph_with_credits):
    """Get credit summary for a graph with credits."""
    graph = test_graph_with_credits["graph"]
    service = CreditService(test_db)
    summary = service.get_credit_summary(graph.graph_id)
    assert "error" not in summary
    assert "current_balance" in summary or "graph_id" in summary

  def test_consume_credits_cached_operation(self, test_db, test_graph_with_credits):
    """Cached operations should not consume credits."""
    graph = test_graph_with_credits["graph"]
    service = CreditService(test_db)
    result = service.consume_credits(
      graph_id=graph.graph_id,
      operation_type="query",
      base_cost=Decimal("5"),
      cached=True,
    )
    assert result["success"] is True
    assert result["credits_consumed"] == 0
    assert result["cached"] is True

  def test_consume_credits_real_operation(self, test_db, test_graph_with_credits):
    """Real operations should consume credits."""
    graph = test_graph_with_credits["graph"]
    credits = test_graph_with_credits["credits"]

    service = CreditService(test_db)
    result = service.consume_credits(
      graph_id=graph.graph_id,
      operation_type="query",
      base_cost=Decimal("5"),
      user_id=str(credits.user_id),
    )
    assert result["success"] is True
    assert result["credits_consumed"] > 0

  def test_check_credit_balance_sufficient(self, test_db, test_graph_with_credits):
    """Check balance when sufficient credits exist."""
    graph = test_graph_with_credits["graph"]
    service = CreditService(test_db)
    result = service.check_credit_balance(
      graph_id=graph.graph_id,
      required_credits=Decimal("5"),
    )
    assert result["has_sufficient_credits"] is True

  def test_check_credit_balance_no_pool(self, test_db):
    """Check balance for graph without credit pool."""
    service = CreditService(test_db)
    result = service.check_credit_balance(
      graph_id="kg_nonexistent_graph_id",
      required_credits=Decimal("5"),
    )
    assert result["has_sufficient_credits"] is False

  def test_get_subscription_tier_limits(self, test_db):
    """Get limits for each subscription tier."""
    service = CreditService(test_db)
    for tier in ["ladybug-standard", "ladybug-large", "ladybug-xlarge"]:
      limits = service.get_subscription_tier_limits(tier)
      assert limits["subscription_tier"] == tier
      assert limits["monthly_credits"] > 0
      assert len(limits["allowed_graph_tiers"]) > 0

  def test_upgrade_graph_tier_not_supported(self, test_db, test_graph_with_credits):
    """Graph tier upgrades are not supported."""
    from robosystems.config.graph_tier import GraphTier

    graph = test_graph_with_credits["graph"]
    service = CreditService(test_db)
    result = service.upgrade_graph_tier(
      graph.graph_id, GraphTier.LADYBUG_LARGE, "ladybug-large"
    )
    assert result["success"] is False

  def test_can_create_graph_tier_validation(self, test_db):
    """Subscription tier restricts which graph tiers can be created."""
    from robosystems.config.graph_tier import GraphTier

    service = CreditService(test_db)
    # Standard can only create standard
    assert (
      service._can_create_graph_tier("ladybug-standard", GraphTier.LADYBUG_STANDARD)
      is True
    )
    assert (
      service._can_create_graph_tier("ladybug-standard", GraphTier.LADYBUG_LARGE)
      is False
    )
    # Large can create standard and large
    assert (
      service._can_create_graph_tier("ladybug-large", GraphTier.LADYBUG_LARGE) is True
    )
    # XLarge can create all
    assert (
      service._can_create_graph_tier("ladybug-xlarge", GraphTier.LADYBUG_XLARGE) is True
    )

  def test_add_bonus_credits(self, test_db, test_graph_with_credits):
    """Add bonus credits to a graph."""
    graph = test_graph_with_credits["graph"]

    service = CreditService(test_db)
    result = service.add_bonus_credits(
      graph_id=graph.graph_id,
      amount=Decimal("500"),
      description="Test bonus",
    )
    assert result["success"] is True
    assert result["credits_added"] == 500.0

  def test_get_credit_transactions(self, test_db, test_graph_with_credits):
    """Get credit transactions for a graph."""
    graph = test_graph_with_credits["graph"]
    service = CreditService(test_db)
    transactions = service.get_credit_transactions(graph.graph_id)
    assert isinstance(transactions, list)

  def test_consume_ai_tokens(self, test_db, test_graph_with_credits):
    """Consume credits based on AI token usage."""
    graph = test_graph_with_credits["graph"]
    credits = test_graph_with_credits["credits"]

    service = CreditService(test_db)
    result = service.consume_ai_tokens(
      graph_id=graph.graph_id,
      input_tokens=1000,
      output_tokens=500,
      model="claude-4-sonnet",
      operation_description="Test AI operation",
      user_id=str(credits.user_id),
    )
    assert result["success"] is True
    assert result["credits_consumed"] > 0
