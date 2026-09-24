"""Per-tool timeouts for the endpoint, queue, tool and instance layers.

Each layer is shorter than the one it is nested in, so the innermost failure
is the one reported.
"""

from dataclasses import dataclass

from robosystems.logger import logger


@dataclass
class TimeoutConfiguration:
  endpoint_timeout: float
  queue_timeout: float
  tool_timeout: float
  instance_timeout: float


class TimeoutCoordinator:
  """Coordinate timeouts across endpoint, queue, tool, and instance layers."""

  def __init__(self):
    self.timeout_configs: dict[str, TimeoutConfiguration] = {
      "cypher_query": TimeoutConfiguration(
        endpoint_timeout=30.0,
        queue_timeout=28.0,
        tool_timeout=25.0,
        instance_timeout=20.0,
      ),
      "read-graph-cypher": TimeoutConfiguration(
        endpoint_timeout=30.0,
        queue_timeout=28.0,
        tool_timeout=25.0,
        instance_timeout=20.0,
      ),
      "get-graph-schema": TimeoutConfiguration(
        endpoint_timeout=30.0,
        queue_timeout=28.0,
        tool_timeout=25.0,
        instance_timeout=20.0,
      ),
      "get-graph-info": TimeoutConfiguration(
        endpoint_timeout=30.0,
        queue_timeout=25.0,
        tool_timeout=20.0,
        instance_timeout=15.0,
      ),
      "default": TimeoutConfiguration(
        endpoint_timeout=30.0,
        queue_timeout=28.0,
        tool_timeout=25.0,
        instance_timeout=20.0,
      ),
    }

    logger.debug("Initialized TimeoutCoordinator with hierarchical timeout management")

  def get_timeout_config(self, tool_name: str) -> TimeoutConfiguration:
    return self.timeout_configs.get(tool_name, self.timeout_configs["default"])

  def get_endpoint_timeout(self, tool_name: str) -> float:
    config = self.get_timeout_config(tool_name)
    return config.endpoint_timeout

  def get_queue_timeout(self, tool_name: str) -> float:
    config = self.get_timeout_config(tool_name)
    return config.queue_timeout

  def get_tool_timeout(self, tool_name: str) -> float:
    config = self.get_timeout_config(tool_name)
    return config.tool_timeout

  def get_instance_timeout(self, tool_name: str) -> float:
    config = self.get_timeout_config(tool_name)
    return config.instance_timeout

  def validate_timeout_hierarchy(self, tool_name: str) -> bool:
    """Whether timeouts strictly decrease down the layers."""
    config = self.get_timeout_config(tool_name)

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

  def get_timeout_summary(self, tool_name: str) -> dict[str, str | float | bool]:
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
    self, operation_type: str, complexity_factors: dict | None = None
  ) -> float:
    """Endpoint timeout for an operation type, scaled by complexity factors
    (capped at 3x)."""
    operation_mapping = {
      "database_query": "cypher_query",
      "database_write": "cypher_query",
      "schema_operation": "get-graph-schema",
      "graph_info": "get-graph-info",
    }

    tool_name = operation_mapping.get(operation_type, "default")
    base_timeout = self.get_endpoint_timeout(tool_name)

    if complexity_factors:
      multiplier = 1.0

      if "limit" in complexity_factors:
        limit = complexity_factors["limit"]
        if limit > 5000:
          multiplier *= 2.0
        elif limit > 1000:
          multiplier *= 1.5

      if complexity_factors.get("has_search", False):
        multiplier *= 1.3

      if "fields_count" in complexity_factors:
        fields = complexity_factors["fields_count"]
        if fields > 5:
          multiplier *= 1.2

      multiplier = min(multiplier, 3.0)
      base_timeout *= multiplier

    logger.debug(
      f"Calculated timeout for {operation_type}: {base_timeout}s "
      f"(factors: {complexity_factors})"
    )

    return base_timeout
