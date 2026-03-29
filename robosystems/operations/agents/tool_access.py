"""Tool access implementations for different execution contexts.

ToolAccess is a protocol (defined in agent_context.py). Each execution
context provides an implementation:

- HttpToolAccess: Full MCP HTTP client (for agents that query the graph)
- DirectToolAccess: Direct tool class instantiation (for agents that use
  taxonomy/mapping tools without HTTP overhead)
"""

from __future__ import annotations

from typing import Any

from robosystems.logger import logger


class HttpToolAccess:
  """MCP tool access via HTTP client.

  Uses the full GraphMCPTools pipeline (HTTP → Graph API). Best for
  agents that execute Cypher queries or read graph schema.
  """

  def __init__(self, graph_id: str) -> None:
    self._graph_id = graph_id
    self._client = None
    self._tools = None

  @property
  def graph_id(self) -> str:
    return self._graph_id

  async def initialize(self) -> None:
    """Lazily initialize the MCP HTTP client and tools."""
    if self._tools is not None:
      return

    from robosystems.middleware.mcp import GraphMCPTools, create_graph_mcp_client
    from robosystems.middleware.mcp.tools.manager import resolve_schema_extensions

    self._client = await create_graph_mcp_client(graph_id=self._graph_id)
    schema_extensions = resolve_schema_extensions(self._graph_id)
    self._tools = GraphMCPTools(self._client, schema_extensions=schema_extensions)
    logger.info(
      f"Initialized HTTP tool access for graph {self._graph_id} "
      f"(extensions={schema_extensions})"
    )

  async def call_tool(
    self,
    tool_name: str,
    arguments: dict[str, Any],
    return_raw: bool = False,
  ) -> Any:
    """Call an MCP tool by name via HTTP."""
    if self._tools is None:
      await self.initialize()
    return await self._tools.call_tool(tool_name, arguments, return_raw=return_raw)

  async def close(self) -> None:
    """Clean up HTTP client connection."""
    if self._client:
      try:
        await self._client.close()
        logger.debug("Closed HTTP tool access connection")
      except Exception as e:
        logger.error(f"Error closing HTTP tool access: {e}")


class DirectToolAccess:
  """Direct MCP tool class instantiation.

  Instantiates tool classes in-process with a minimal client that
  provides .graph_id. No HTTP overhead. Best for agents that use
  taxonomy/mapping tools in the worker context.

  Usage::

      tools = DirectToolAccess(graph_id)
      unmapped_tool = tools.get_tool_instance(GetUnmappedElementsTool)
      result = await unmapped_tool.execute({"mapping_id": "..."})
  """

  def __init__(self, graph_id: str) -> None:
    self._graph_id = graph_id
    self._tool_instances: dict[str, Any] = {}

  @property
  def graph_id(self) -> str:
    return self._graph_id

  def get_tool_instance(self, tool_class: type) -> Any:
    """Get or create a tool instance by class.

    Tool classes expect a client with a .graph_id attribute.
    DirectToolAccess provides exactly that (itself).
    """
    key = tool_class.__name__
    if key not in self._tool_instances:
      self._tool_instances[key] = tool_class(self)
    return self._tool_instances[key]

  async def call_tool(
    self,
    tool_name: str,
    arguments: dict[str, Any],
    return_raw: bool = False,
  ) -> Any:
    """Call a tool by name from registered instances."""
    for tool in self._tool_instances.values():
      defn = tool.get_tool_definition()
      if defn.get("name") == tool_name:
        return await tool.execute(arguments)
    raise ValueError(
      f"Tool '{tool_name}' not registered in DirectToolAccess. "
      f"Use get_tool_instance() to register tool classes first."
    )

  async def close(self) -> None:
    """No-op — no connections to clean up."""
    pass
