"""
Timeout coordination system for hierarchical timeout management.

Provides coordinated timeout management across multiple layers to prevent
timeout conflicts and ensure proper error handling.
"""

from typing import Dict, Union, Optional
from dataclasses import dataclass

from robosystems.logger import logger


@dataclass
class TimeoutConfiguration:
  """Timeout configuration for different layers."""

  endpoint_timeout: float
  queue_timeout: float
  tool_timeout: float
  instance_timeout: float


class TimeoutCoordinator:
  """Coordinate timeouts across endpoint, queue, tool, and instance layers."""

  def __init__(self):
    """Initialize timeout coordinator with default configurations."""
    # Default timeout configurations by tool type
    self.timeout_configs: Dict[str, TimeoutConfiguration] = {
      "cypher_query": TimeoutConfiguration(
        endpoint_timeout=30.0,  # 30s API limit
        queue_timeout=28.0,  # 2s buffer for response
        tool_timeout=25.0,  # 3s buffer for queue overhead
        instance_timeout=20.0,  # 5s buffer for HTTP/network overhead
      ),
      "read-graph-cypher": TimeoutConfiguration(
        endpoint_timeout=30.0,  # 30s API limit
        queue_timeout=28.0,  # 2s buffer for response
        tool_timeout=25.0,  # 3s buffer for queue overhead
        instance_timeout=20.0,  # 5s buffer for HTTP/network overhead
      ),
      "get-graph-schema": TimeoutConfiguration(
        endpoint_timeout=30.0,  # 30s API limit
        queue_timeout=28.0,  # 2s buffer
        tool_timeout=25.0,  # 3s buffer
        instance_timeout=20.0,  # 5s buffer
      ),
      "get_schema": TimeoutConfiguration(
        endpoint_timeout=30.0,  # 30s API limit
        queue_timeout=28.0,  # 2s buffer
        tool_timeout=25.0,  # 3s buffer
        instance_timeout=20.0,  # 5s buffer
      ),
      "get_graph_info": TimeoutConfiguration(
        endpoint_timeout=30.0,  # 30 seconds total
        queue_timeout=25.0,  # 25 seconds for queue processing
        tool_timeout=20.0,  # 20 seconds for tool execution
        instance_timeout=15.0,  # 15 seconds for Kuzu instance
      ),
      "default": TimeoutConfiguration(
        endpoint_timeout=30.0,  # 30s API limit
        queue_timeout=28.0,  # 2s buffer
        tool_timeout=25.0,  # 3s buffer
        instance_timeout=20.0,  # 5s buffer
      ),
    }

    logger.debug("Initialized TimeoutCoordinator with hierarchical timeout management")

  def get_timeout_config(self, tool_name: str) -> TimeoutConfiguration:
    """
    Get timeout configuration for a specific tool.

    Args:
        tool_name: Name of the tool (e.g., 'cypher_query', 'get_schema')

    Returns:
        TimeoutConfiguration for the tool
    """
    return self.timeout_configs.get(tool_name, self.timeout_configs["default"])

  def get_endpoint_timeout(self, tool_name: str) -> float:
    """Get endpoint timeout for a tool."""
    config = self.get_timeout_config(tool_name)
    return config.endpoint_timeout

  def get_queue_timeout(self, tool_name: str) -> float:
    """Get queue timeout for a tool."""
    config = self.get_timeout_config(tool_name)
    return config.queue_timeout

  def get_tool_timeout(self, tool_name: str) -> float:
    """Get tool execution timeout."""
    config = self.get_timeout_config(tool_name)
    return config.tool_timeout

  def get_instance_timeout(self, tool_name: str) -> float:
    """Get Kuzu instance timeout."""
    config = self.get_timeout_config(tool_name)
    return config.instance_timeout

  def validate_timeout_hierarchy(self, tool_name: str) -> bool:
    """
    Validate that timeout hierarchy is properly configured.

    Args:
        tool_name: Tool to validate timeouts for

    Returns:
        True if hierarchy is valid
    """
    config = self.get_timeout_config(tool_name)

    # Check that timeouts decrease down the hierarchy
    hierarchy_valid = (
      config.endpoint_timeout
      > config.queue_timeout
      > config.tool_timeout
      > config.instance_timeout
    )

    if not hierarchy_valid:
      logger.warning(
        f"Invalid timeout hierarchy for {tool_name}: "
        f"endpoint={config.endpoint_timeout}, queue={config.queue_timeout}, "
        f"tool={config.tool_timeout}, instance={config.instance_timeout}"
      )

    return hierarchy_valid

  def get_timeout_summary(self, tool_name: str) -> Dict[str, Union[str, float, bool]]:
    """Get timeout summary for monitoring/debugging."""
    config = self.get_timeout_config(tool_name)

    return {
      "tool_name": tool_name,
      "endpoint_timeout": config.endpoint_timeout,
      "queue_timeout": config.queue_timeout,
      "tool_timeout": config.tool_timeout,
      "instance_timeout": config.instance_timeout,
      "hierarchy_valid": self.validate_timeout_hierarchy(tool_name),
    }

  def calculate_timeout(
    self, operation_type: str, complexity_factors: Optional[Dict] = None
  ) -> float:
    """
    Calculate timeout based on operation type and complexity factors.

    Args:
        operation_type: Type of operation (database_query, database_write, etc.)
        complexity_factors: Optional dict of factors that affect timeout

    Returns:
        Calculated timeout in seconds
    """
    # Map operation types to timeout configurations
    operation_mapping = {
      "database_query": "cypher_query",
      "database_write": "cypher_query",
      "schema_operation": "get_schema",
      "graph_info": "get_graph_info",
    }

    tool_name = operation_mapping.get(operation_type, "default")
    base_timeout = self.get_endpoint_timeout(tool_name)

    # Apply complexity factors if provided
    if complexity_factors:
      multiplier = 1.0

      # Adjust based on limit/row count
      if "limit" in complexity_factors:
        limit = complexity_factors["limit"]
        if limit > 1000:
          multiplier *= 1.5
        elif limit > 5000:
          multiplier *= 2.0

      # Adjust for search operations
      if complexity_factors.get("has_search", False):
        multiplier *= 1.3

      # Adjust for write operations with multiple fields
      if "fields_count" in complexity_factors:
        fields = complexity_factors["fields_count"]
        if fields > 5:
          multiplier *= 1.2

      # Cap the multiplier to prevent excessive timeouts
      multiplier = min(multiplier, 3.0)
      base_timeout *= multiplier

    logger.debug(
      f"Calculated timeout for {operation_type}: {base_timeout}s "
      f"(factors: {complexity_factors})"
    )

    return base_timeout
