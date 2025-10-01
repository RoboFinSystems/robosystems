"""
Base tool class for MCP tools.

Provides common functionality and interface for all MCP tools.
"""

from typing import Any, Dict, TYPE_CHECKING
from abc import ABC, abstractmethod

if TYPE_CHECKING:
  from ..client import KuzuMCPClient

from robosystems.logger import logger
from robosystems.middleware.mcp.query_validator import KuzuQueryValidator


class BaseTool(ABC):
  """
  Base class for all MCP tools.

  Provides common functionality like client access, validation, and logging.
  """

  def __init__(self, client: "KuzuMCPClient"):
    self.client = client
    self.validator = KuzuQueryValidator()

  @abstractmethod
  def get_tool_definition(self) -> Dict[str, Any]:
    """
    Get the tool definition for this tool.

    Returns:
        Tool definition dictionary for MCP protocol
    """
    pass

  @abstractmethod
  async def execute(self, arguments: Dict[str, Any]) -> Any:
    """
    Execute the tool with the given arguments.

    Args:
        arguments: Tool-specific arguments

    Returns:
        Tool execution result
    """
    pass

  def _log_tool_execution(self, tool_name: str, arguments: Dict[str, Any]) -> None:
    """Log tool execution for debugging."""
    logger.info(f"Executing MCP tool: {tool_name} with args: {list(arguments.keys())}")
