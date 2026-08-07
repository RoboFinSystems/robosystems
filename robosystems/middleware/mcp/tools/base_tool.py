"""Base class for hand-written MCP tools."""

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
  from ..client import GraphMCPClient

from robosystems.logger import logger
from robosystems.middleware.mcp.query_validator import GraphQueryValidator


class BaseTool(ABC):
  """Shared client access, query validation, and logging for MCP tools."""

  def __init__(self, client: "GraphMCPClient"):
    self.client = client
    self.validator = GraphQueryValidator()

  @abstractmethod
  def get_tool_definition(self) -> dict[str, Any]:
    """Return this tool's MCP definition: name, description, inputSchema."""
    pass

  @abstractmethod
  async def execute(self, arguments: dict[str, Any]) -> Any:
    """Run the tool against the arguments an MCP client supplied."""
    pass

  def _log_tool_execution(self, tool_name: str, arguments: dict[str, Any]) -> None:
    """Log tool execution for debugging."""
    logger.info(f"Executing MCP tool: {tool_name} with args: {list(arguments.keys())}")
