"""Tests for credit system configuration."""

from decimal import Decimal

from robosystems.config.credits import CreditConfig


class TestCreditConfig:
  """Test CreditConfig class."""

  def test_operation_costs_constants(self):
    """Test that operation costs are defined correctly.

    Note: AI operations (agent_call) use token-based pricing via AIBillingConfig,
    not fixed costs in OPERATION_COSTS.
    """
    # Fixed-cost operations
    assert CreditConfig.OPERATION_COSTS["storage_per_gb_day"] == Decimal("1")
    assert CreditConfig.OPERATION_COSTS["connection_sync"] == Decimal("20")

    # All other operations should be free (0 credits)
    free_operations = [
      "mcp_call",
      "mcp_tool_call",
      "api_call",
      "query",
      "cypher_query",
      "analytics",
      "analytics_query",
      "backup",
      "backup_restore",
      "backup_export",
      "sync",
      "import",
      "data_transfer_in",
      "data_transfer_out",
      "schema_query",
      "schema_validation",
      "schema_export",
      "connection_create",
      "connection_test",
      "connection_delete",
      "database_query",
      "database_write",
    ]

    for operation in free_operations:
      assert CreditConfig.OPERATION_COSTS[operation] == Decimal("0")

  def test_alert_thresholds_constants(self):
    """Test that alert thresholds are defined correctly."""
    assert CreditConfig.ALERT_THRESHOLDS["low_balance"] == 0.2
    assert CreditConfig.ALERT_THRESHOLDS["critical_balance"] == 0.05
    assert CreditConfig.ALERT_THRESHOLDS["exhausted"] == 0.0

  def test_monthly_allocations_structure(self):
    """Test that monthly allocations have the correct structure."""
    # Test that allocations exist for expected tiers
    standard_allocation = CreditConfig.get_monthly_allocation("standard")
    enterprise_allocation = CreditConfig.get_monthly_allocation("enterprise")
    premium_allocation = CreditConfig.get_monthly_allocation("premium")

    # All should be integers >= 0
    assert isinstance(standard_allocation, int)
    assert isinstance(enterprise_allocation, int)
    assert isinstance(premium_allocation, int)
    assert standard_allocation >= 0
    assert enterprise_allocation >= 0
    assert premium_allocation >= 0

    # Higher tiers should typically have higher allocations
    # Note: This is a business logic test, not a strict requirement

  def test_get_operation_cost_known_operations(self):
    """Test getting costs for known operations.

    Note: AI operations use token-based pricing via AIBillingConfig.TOKEN_PRICING.
    """
    # Fixed-cost operations
    assert CreditConfig.get_operation_cost("storage_per_gb_day") == Decimal("1")
    assert CreditConfig.get_operation_cost("connection_sync") == Decimal("20")

    # Free operations should return 0
    assert CreditConfig.get_operation_cost("query") == Decimal("0")
    assert CreditConfig.get_operation_cost("backup") == Decimal("0")
    assert CreditConfig.get_operation_cost("mcp_call") == Decimal("0")

  def test_get_operation_cost_unknown_operation(self):
    """Test getting cost for unknown operation returns 0."""
    assert CreditConfig.get_operation_cost("unknown_operation") == Decimal("0")
    assert CreditConfig.get_operation_cost("") == Decimal("0")
    assert CreditConfig.get_operation_cost("nonexistent") == Decimal("0")

  def test_get_monthly_allocation_valid_tiers(self):
    """Test getting monthly allocation for valid tiers."""
    # Test that valid tiers return positive allocations
    lbug_standard_allocation = CreditConfig.get_monthly_allocation("ladybug-standard")
    lbug_large_allocation = CreditConfig.get_monthly_allocation("ladybug-large")
    lbug_xlarge_allocation = CreditConfig.get_monthly_allocation("ladybug-xlarge")

    assert lbug_standard_allocation > 0
    assert lbug_large_allocation > 0
    assert lbug_xlarge_allocation > 0

  def test_get_monthly_allocation_invalid_tier(self):
    """Test getting monthly allocation for invalid tier."""
    assert CreditConfig.get_monthly_allocation("invalid") == 0
    assert CreditConfig.get_monthly_allocation("") == 0
    assert CreditConfig.get_monthly_allocation("nonexistent") == 0

  def test_should_alert_exhausted(self):
    """Test alert when credits are exhausted."""
    assert CreditConfig.should_alert(0, 1000) == "exhausted"
    assert CreditConfig.should_alert(-10, 1000) == "exhausted"

  def test_should_alert_critical(self):
    """Test critical alert when credits are very low."""
    # 5% threshold = 0.05
    assert CreditConfig.should_alert(49, 1000) == "critical"  # 4.9%
    assert CreditConfig.should_alert(50, 1000) == "critical"  # 5.0%
    assert CreditConfig.should_alert(25, 1000) == "critical"  # 2.5%

  def test_should_alert_low(self):
    """Test low alert when credits are below 20%."""
    # Between 5% and 20%
    assert CreditConfig.should_alert(51, 1000) == "low"  # 5.1%
    assert CreditConfig.should_alert(100, 1000) == "low"  # 10%
    assert CreditConfig.should_alert(199, 1000) == "low"  # 19.9%
    assert CreditConfig.should_alert(200, 1000) == "low"  # 20%

  def test_should_alert_none(self):
    """Test no alert when credits are sufficient."""
    assert CreditConfig.should_alert(201, 1000) == "none"  # 20.1%
    assert CreditConfig.should_alert(500, 1000) == "none"  # 50%
    assert CreditConfig.should_alert(1000, 1000) == "none"  # 100%
    assert CreditConfig.should_alert(1500, 1000) == "none"  # 150%

  def test_should_alert_zero_allocation(self):
    """Test alert with zero allocation returns none."""
    assert CreditConfig.should_alert(0, 0) == "none"
    assert CreditConfig.should_alert(100, 0) == "none"
    assert CreditConfig.should_alert(-50, 0) == "none"

  def test_should_alert_edge_cases(self):
    """Test alert edge cases at threshold boundaries."""
    allocation = 1000

    # Exactly at thresholds
    assert CreditConfig.should_alert(0, allocation) == "exhausted"  # 0%
    assert CreditConfig.should_alert(50, allocation) == "critical"  # 5%
    assert CreditConfig.should_alert(200, allocation) == "low"  # 20%

    # Just above thresholds
    assert CreditConfig.should_alert(1, allocation) == "critical"  # 0.1%
    assert CreditConfig.should_alert(51, allocation) == "low"  # 5.1%
    assert CreditConfig.should_alert(201, allocation) == "none"  # 20.1%

  def test_decimal_precision_in_costs(self):
    """Test that costs maintain decimal precision."""
    cost = CreditConfig.get_operation_cost("storage_per_gb_day")
    assert isinstance(cost, Decimal)
    assert cost == Decimal("1")

    # Test arithmetic operations maintain precision
    double_cost = cost * 2
    assert isinstance(double_cost, Decimal)
    assert double_cost == Decimal("2")

  def test_ai_vs_free_operation_categorization(self):
    """Test that operations are correctly categorized.

    Note: AI operations use token-based pricing via AIBillingConfig.TOKEN_PRICING,
    not fixed costs in OPERATION_COSTS.
    """
    # Storage and connection operations (configurable costs)
    configurable_ops = ["storage_per_gb_day", "connection_sync"]
    for op in configurable_ops:
      assert CreditConfig.get_operation_cost(op) > Decimal("0")

    # Free operations (should have zero costs)
    free_operations = [
      "query",
      "backup",
      "import",
      "mcp_call",
      "api_call",
      "schema_query",
      "analytics",
      "data_transfer_in",
    ]
    for op in free_operations:
      assert CreditConfig.get_operation_cost(op) == Decimal("0")

  def test_operation_costs_coverage(self):
    """Test that all expected operation types are covered.

    Note: AI operations use token-based pricing via AIBillingConfig.TOKEN_PRICING.
    """
    required_operations = [
      # Storage operations
      "storage_per_gb_day",
      # Connection operations
      "connection_sync",
      # Free operations
      "mcp_call",
      "mcp_tool_call",
      "api_call",
      "query",
      "cypher_query",
      "analytics",
      "analytics_query",
      "backup",
      "backup_restore",
      "backup_export",
      "sync",
      "import",
      "data_transfer_in",
      "data_transfer_out",
      "schema_query",
      "schema_validation",
      "schema_export",
      "connection_create",
      "connection_test",
      "connection_delete",
      "database_query",
      "database_write",
    ]

    for operation in required_operations:
      assert operation in CreditConfig.OPERATION_COSTS

  def test_monthly_allocations_structure_detailed(self):
    """Test that monthly allocations have the expected structure."""
    required_tiers = ["standard", "enterprise", "premium"]

    for tier in required_tiers:
      allocation = CreditConfig.get_monthly_allocation(tier)
      assert isinstance(allocation, int)
      assert allocation >= 0

  def test_alert_level_progression(self):
    """Test that alert levels progress correctly as balance decreases."""
    allocation = 1000

    # Test progression from none -> low -> critical -> exhausted
    balances_and_expected = [
      (1000, "none"),  # 100%
      (500, "none"),  # 50%
      (200, "low"),  # 20%
      (100, "low"),  # 10%
      (50, "critical"),  # 5%
      (25, "critical"),  # 2.5%
      (0, "exhausted"),  # 0%
    ]

    for balance, expected_alert in balances_and_expected:
      assert CreditConfig.should_alert(balance, allocation) == expected_alert

  def test_class_methods_are_accessible(self):
    """Test that all class methods are accessible without instantiation."""
    # Should be able to call all methods without creating an instance
    assert callable(CreditConfig.get_operation_cost)
    assert callable(CreditConfig.get_monthly_allocation)
    assert callable(CreditConfig.should_alert)

    # Methods should work when called on the class
    cost = CreditConfig.get_operation_cost("storage_per_gb_day")
    allocation = CreditConfig.get_monthly_allocation("standard")
    alert = CreditConfig.should_alert(100, 1000)

    assert isinstance(cost, Decimal)
    assert isinstance(allocation, int)
    assert isinstance(alert, str)

  def test_operation_cost_consistency(self):
    """Test that operation costs are consistent with documentation.

    Note: AI operations use token-based pricing via AIBillingConfig.TOKEN_PRICING.
    """
    # Storage should be 1 credit per GB per day
    assert CreditConfig.get_operation_cost("storage_per_gb_day") == Decimal("1")

    # Connection sync should be 20 credits
    assert CreditConfig.get_operation_cost("connection_sync") == Decimal("20")

  def test_tier_dependency_handling(self):
    """Test handling of tier configuration dependency."""
    # Test that the module can handle various edge cases gracefully

    # Should still be able to get operation costs
    cost = CreditConfig.get_operation_cost("storage_per_gb_day")
    assert cost == Decimal("1")

    # Alert functionality should work with manual allocations
    alert = CreditConfig.should_alert(100, 1000)
    assert alert == "low"

    # Invalid tiers should return 0
    invalid_allocation = CreditConfig.get_monthly_allocation("invalid_tier")
    assert invalid_allocation == 0
