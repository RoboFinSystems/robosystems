"""
Base tool class for MCP tools.

Provides common functionality and interface for all MCP tools.
"""

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
  from ..client import GraphMCPClient

from robosystems.logger import logger
from robosystems.middleware.mcp.query_validator import GraphQueryValidator


class BaseTool(ABC):
  """
  Base class for all MCP tools.

  Provides common functionality like client access, validation, and logging.
  """

  def __init__(self, client: "GraphMCPClient"):
    self.client = client
    self.validator = GraphQueryValidator()

  @abstractmethod
  def get_tool_definition(self) -> dict[str, Any]:
    """
    Get the tool definition for this tool.

    Returns:
        Tool definition dictionary for MCP protocol
    """
    pass

  @abstractmethod
  async def execute(self, arguments: dict[str, Any]) -> Any:
    """
    Execute the tool with the given arguments.

    Args:
        arguments: Tool-specific arguments

    Returns:
        Tool execution result
    """
    pass

  def _log_tool_execution(self, tool_name: str, arguments: dict[str, Any]) -> None:
    """Log tool execution for debugging."""
    logger.info(f"Executing MCP tool: {tool_name} with args: {list(arguments.keys())}")
