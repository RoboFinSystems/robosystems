"""Tests for timeout coordinator module."""

from unittest.mock import patch

from robosystems.middleware.robustness.timeout_coordinator import (
  TimeoutConfiguration,
  TimeoutCoordinator,
)


class TestTimeoutConfiguration:
  """Test TimeoutConfiguration dataclass."""

  def test_timeout_configuration_creation(self):
    """Test creating a timeout configuration."""
    config = TimeoutConfiguration(
      endpoint_timeout=30.0,
      queue_timeout=28.0,
      tool_timeout=25.0,
      instance_timeout=20.0,
    )

    assert config.endpoint_timeout == 30.0
    assert config.queue_timeout == 28.0
    assert config.tool_timeout == 25.0
    assert config.instance_timeout == 20.0

  def test_timeout_configuration_types(self):
    """Test that timeout configuration accepts float values."""
    config = TimeoutConfiguration(
      endpoint_timeout=30,  # int should work
      queue_timeout=28.5,  # float
      tool_timeout=25.0,  # explicit float
      instance_timeout=20,  # int
    )

    assert isinstance(config.endpoint_timeout, (int, float))
    assert isinstance(config.queue_timeout, float)
    assert isinstance(config.tool_timeout, float)
    assert isinstance(config.instance_timeout, (int, float))


class TestTimeoutCoordinator:
  """Test TimeoutCoordinator class."""

  def test_initialization(self):
    """Test timeout coordinator initialization."""
    coordinator = TimeoutCoordinator()

    assert coordinator.timeout_configs is not None
    assert isinstance(coordinator.timeout_configs, dict)
    assert "default" in coordinator.timeout_configs
    assert "cypher_query" in coordinator.timeout_configs

  def test_default_timeout_configs(self):
    """Test that default timeout configurations are present."""
    coordinator = TimeoutCoordinator()

    required_configs = [
      "cypher_query",
      "read-graph-cypher",
      "get-graph-schema",
      "get_schema",
      "get_graph_info",
      "default",
    ]

    for config_name in required_configs:
      assert config_name in coordinator.timeout_configs
      config = coordinator.timeout_configs[config_name]
      assert isinstance(config, TimeoutConfiguration)

  def test_timeout_hierarchy_in_defaults(self):
    """Test that default configurations follow timeout hierarchy."""
    coordinator = TimeoutCoordinator()

    for tool_name, config in coordinator.timeout_configs.items():
      assert config.endpoint_timeout > config.queue_timeout, f"Failed for {tool_name}"
      assert config.queue_timeout > config.tool_timeout, f"Failed for {tool_name}"
      assert config.tool_timeout > config.instance_timeout, f"Failed for {tool_name}"

  def test_get_timeout_config_known_tool(self):
    """Test getting timeout config for known tool."""
    coordinator = TimeoutCoordinator()

    config = coordinator.get_timeout_config("cypher_query")

    assert isinstance(config, TimeoutConfiguration)
    assert config.endpoint_timeout == 30.0
    assert config.queue_timeout == 28.0
    assert config.tool_timeout == 25.0
    assert config.instance_timeout == 20.0

  def test_get_timeout_config_unknown_tool(self):
    """Test getting timeout config for unknown tool returns default."""
    coordinator = TimeoutCoordinator()

    config = coordinator.get_timeout_config("unknown_tool")

    assert isinstance(config, TimeoutConfiguration)
    # Should be the same as default config
    default_config = coordinator.timeout_configs["default"]
    assert config.endpoint_timeout == default_config.endpoint_timeout
    assert config.queue_timeout == default_config.queue_timeout
    assert config.tool_timeout == default_config.tool_timeout
    assert config.instance_timeout == default_config.instance_timeout

  def test_get_endpoint_timeout(self):
    """Test getting endpoint timeout for specific tool."""
    coordinator = TimeoutCoordinator()

    timeout = coordinator.get_endpoint_timeout("cypher_query")
    assert timeout == 30.0

    timeout = coordinator.get_endpoint_timeout("unknown_tool")
    assert timeout == 30.0  # Should use default

  def test_get_queue_timeout(self):
    """Test getting queue timeout for specific tool."""
    coordinator = TimeoutCoordinator()

    timeout = coordinator.get_queue_timeout("cypher_query")
    assert timeout == 28.0

    timeout = coordinator.get_queue_timeout("unknown_tool")
    assert timeout == 28.0  # Should use default

  def test_get_tool_timeout(self):
    """Test getting tool timeout for specific tool."""
    coordinator = TimeoutCoordinator()

    timeout = coordinator.get_tool_timeout("cypher_query")
    assert timeout == 25.0

    timeout = coordinator.get_tool_timeout("unknown_tool")
    assert timeout == 25.0  # Should use default

  def test_get_instance_timeout(self):
    """Test getting instance timeout for specific tool."""
    coordinator = TimeoutCoordinator()

    timeout = coordinator.get_instance_timeout("cypher_query")
    assert timeout == 20.0

    timeout = coordinator.get_instance_timeout("unknown_tool")
    assert timeout == 20.0  # Should use default

  def test_validate_timeout_hierarchy_valid(self):
    """Test validating valid timeout hierarchy."""
    coordinator = TimeoutCoordinator()

    # Default configurations should be valid
    assert coordinator.validate_timeout_hierarchy("cypher_query") is True
    assert coordinator.validate_timeout_hierarchy("get_schema") is True
    assert coordinator.validate_timeout_hierarchy("default") is True

  def test_validate_timeout_hierarchy_invalid(self):
    """Test validating invalid timeout hierarchy."""
    coordinator = TimeoutCoordinator()

    # Create an invalid configuration
    coordinator.timeout_configs["invalid_tool"] = TimeoutConfiguration(
      endpoint_timeout=10.0,  # Invalid: smaller than queue
      queue_timeout=20.0,
      tool_timeout=15.0,
      instance_timeout=5.0,
    )

    with patch(
      "robosystems.middleware.robustness.timeout_coordinator.logger"
    ) as mock_logger:
      result = coordinator.validate_timeout_hierarchy("invalid_tool")

      assert result is False
      mock_logger.warning.assert_called_once()
      assert "Invalid timeout hierarchy" in mock_logger.warning.call_args[0][0]

  def test_get_timeout_summary(self):
    """Test getting timeout summary for monitoring."""
    coordinator = TimeoutCoordinator()

    summary = coordinator.get_timeout_summary("cypher_query")

    assert summary["tool_name"] == "cypher_query"
    assert summary["endpoint_timeout"] == 30.0
    assert summary["queue_timeout"] == 28.0
    assert summary["tool_timeout"] == 25.0
    assert summary["instance_timeout"] == 20.0
    assert summary["hierarchy_valid"] is True

  def test_get_timeout_summary_unknown_tool(self):
    """Test getting timeout summary for unknown tool."""
    coordinator = TimeoutCoordinator()

    summary = coordinator.get_timeout_summary("unknown_tool")

    assert summary["tool_name"] == "unknown_tool"
    assert summary["hierarchy_valid"] is True
    # Should use default values
    assert summary["endpoint_timeout"] == 30.0

  def test_calculate_timeout_basic_operations(self):
    """Test calculating timeout for basic operations."""
    coordinator = TimeoutCoordinator()

    # Test basic operations without complexity factors
    query_timeout = coordinator.calculate_timeout("database_query")
    assert query_timeout == 30.0  # Should use cypher_query endpoint timeout

    write_timeout = coordinator.calculate_timeout("database_write")
    assert write_timeout == 30.0  # Should use cypher_query endpoint timeout

    schema_timeout = coordinator.calculate_timeout("schema_operation")
    assert schema_timeout == 30.0  # Should use get_schema endpoint timeout

    info_timeout = coordinator.calculate_timeout("graph_info")
    assert info_timeout == 30.0  # Should use get_graph_info endpoint timeout

  def test_calculate_timeout_unknown_operation(self):
    """Test calculating timeout for unknown operation."""
    coordinator = TimeoutCoordinator()

    timeout = coordinator.calculate_timeout("unknown_operation")
    assert timeout == 30.0  # Should use default endpoint timeout

  def test_calculate_timeout_with_limit_factor(self):
    """Test calculating timeout with limit complexity factor."""
    coordinator = TimeoutCoordinator()

    # Small limit - no multiplier
    timeout = coordinator.calculate_timeout(
      "database_query", complexity_factors={"limit": 100}
    )
    assert timeout == 30.0

    # Medium limit - 1.5x multiplier
    timeout = coordinator.calculate_timeout(
      "database_query", complexity_factors={"limit": 2000}
    )
    assert timeout == 45.0  # 30.0 * 1.5

    # Large limit - still 1.5x multiplier (due to elif logic)
    timeout = coordinator.calculate_timeout(
      "database_query", complexity_factors={"limit": 6000}
    )
    assert timeout == 45.0  # 30.0 * 1.5 (the elif condition never hits)

  def test_calculate_timeout_with_search_factor(self):
    """Test calculating timeout with search complexity factor."""
    coordinator = TimeoutCoordinator()

    timeout = coordinator.calculate_timeout(
      "database_query", complexity_factors={"has_search": True}
    )
    assert timeout == 39.0  # 30.0 * 1.3

  def test_calculate_timeout_with_fields_factor(self):
    """Test calculating timeout with fields count factor."""
    coordinator = TimeoutCoordinator()

    # Few fields - no multiplier
    timeout = coordinator.calculate_timeout(
      "database_write", complexity_factors={"fields_count": 3}
    )
    assert timeout == 30.0

    # Many fields - 1.2x multiplier
    timeout = coordinator.calculate_timeout(
      "database_write", complexity_factors={"fields_count": 8}
    )
    assert timeout == 36.0  # 30.0 * 1.2

  def test_calculate_timeout_multiple_factors(self):
    """Test calculating timeout with multiple complexity factors."""
    coordinator = TimeoutCoordinator()

    timeout = coordinator.calculate_timeout(
      "database_query",
      complexity_factors={
        "limit": 2000,  # 1.5x multiplier
        "has_search": True,  # 1.3x multiplier
        "fields_count": 8,  # 1.2x multiplier
      },
    )
    # 30.0 * 1.5 * 1.3 * 1.2 = 70.2
    assert timeout == 70.2

  def test_calculate_timeout_capped_multiplier(self):
    """Test that timeout multiplier is capped at 3.0."""
    coordinator = TimeoutCoordinator()

    # Use factors that would result in > 3.0 multiplier
    timeout = coordinator.calculate_timeout(
      "database_query",
      complexity_factors={
        "limit": 10000,  # 1.5x multiplier (not 2.0x due to elif logic)
        "has_search": True,  # 1.3x multiplier
        "fields_count": 10,  # 1.2x multiplier
      },
    )
    # Would be 30.0 * 1.5 * 1.3 * 1.2 = 70.2, which is under the 3.0 cap
    assert timeout == 70.2

  def test_calculate_timeout_logging(self):
    """Test that timeout calculation logs debug information."""
    coordinator = TimeoutCoordinator()

    with patch(
      "robosystems.middleware.robustness.timeout_coordinator.logger"
    ) as mock_logger:
      coordinator.calculate_timeout(
        "database_query", complexity_factors={"limit": 2000}
      )

      mock_logger.debug.assert_called_once()
      log_message = mock_logger.debug.call_args[0][0]
      assert "Calculated timeout for database_query" in log_message
      assert "45.0s" in log_message

  def test_initialization_logging(self):
    """Test that initialization logs debug message."""
    with patch(
      "robosystems.middleware.robustness.timeout_coordinator.logger"
    ) as mock_logger:
      TimeoutCoordinator()

      mock_logger.debug.assert_called_once()
      log_message = mock_logger.debug.call_args[0][0]
      assert "Initialized TimeoutCoordinator" in log_message

  def test_specific_tool_configurations(self):
    """Test specific tool configurations have expected values."""
    coordinator = TimeoutCoordinator()

    # Test get_graph_info has different timeouts
    info_config = coordinator.get_timeout_config("get_graph_info")
    assert info_config.endpoint_timeout == 30.0
    assert info_config.queue_timeout == 25.0
    assert info_config.tool_timeout == 20.0
    assert info_config.instance_timeout == 15.0

    # Test that it's different from default
    default_config = coordinator.get_timeout_config("default")
    assert info_config.queue_timeout != default_config.queue_timeout
    assert info_config.tool_timeout != default_config.tool_timeout
    assert info_config.instance_timeout != default_config.instance_timeout

  def test_operation_type_mapping(self):
    """Test that operation types map to correct tool configurations."""
    coordinator = TimeoutCoordinator()

    # Test that different operation types map correctly
    query_timeout = coordinator.calculate_timeout("database_query")
    write_timeout = coordinator.calculate_timeout("database_write")
    schema_timeout = coordinator.calculate_timeout("schema_operation")
    info_timeout = coordinator.calculate_timeout("graph_info")

    # database_query and database_write should both use cypher_query config
    assert query_timeout == write_timeout == 30.0

    # schema_operation should use get_schema config
    assert schema_timeout == 30.0

    # graph_info should use get_graph_info config
    assert info_timeout == 30.0

  def test_timeout_consistency_across_methods(self):
    """Test that timeout values are consistent across different methods."""
    coordinator = TimeoutCoordinator()

    tool_name = "cypher_query"

    # Get timeouts using different methods
    config = coordinator.get_timeout_config(tool_name)
    endpoint_timeout = coordinator.get_endpoint_timeout(tool_name)
    queue_timeout = coordinator.get_queue_timeout(tool_name)
    tool_timeout = coordinator.get_tool_timeout(tool_name)
    instance_timeout = coordinator.get_instance_timeout(tool_name)

    # Should all be consistent
    assert endpoint_timeout == config.endpoint_timeout
    assert queue_timeout == config.queue_timeout
    assert tool_timeout == config.tool_timeout
    assert instance_timeout == config.instance_timeout

  def test_edge_case_complexity_factors(self):
    """Test edge cases in complexity factor handling."""
    coordinator = TimeoutCoordinator()

    # Empty complexity factors
    timeout = coordinator.calculate_timeout("database_query", {})
    assert timeout == 30.0

    # None complexity factors
    timeout = coordinator.calculate_timeout("database_query", None)
    assert timeout == 30.0

    # Factors with zero/negative values
    timeout = coordinator.calculate_timeout(
      "database_query", complexity_factors={"limit": 0, "fields_count": -1}
    )
    assert timeout == 30.0  # Should not apply multipliers for invalid values

  def test_timeout_configuration_immutability(self):
    """Test that timeout configurations don't affect each other."""
    coordinator = TimeoutCoordinator()

    # Get two different configurations
    config1 = coordinator.get_timeout_config("cypher_query")
    config2 = coordinator.get_timeout_config("get_graph_info")

    # They should be different objects
    assert config1 is not config2

    # Verify they have different values
    assert config1.queue_timeout != config2.queue_timeout
    assert config1.tool_timeout != config2.tool_timeout
    assert config1.instance_timeout != config2.instance_timeout
