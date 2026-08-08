"""Tool access implementations for different execution contexts.

ToolAccess is a protocol (defined in operator_context.py). Each execution
context provides an implementation:

- HttpToolAccess: Full MCP HTTP client (for operators that query the graph)
- DirectToolAccess: Direct tool class instantiation (for operators that use
  taxonomy/mapping tools without HTTP overhead)
"""

from __future__ import annotations

from typing import Any

from robosystems.logger import logger


class _RemoteToolHandle:
  """A tool-shaped object whose ``execute`` goes over the MCP HTTP surface.

  Duck-types the one method imperative operators use, so the same operator
  body runs against either access implementation.

  ``return_raw=True`` is not incidental: ``GraphMCPTools.call_tool`` returns
  a JSON *string* by default, while a direct tool returns its native dict.
  Callers test results with ``"error" in result``, which silently degrades
  to substring matching on a string — passing where it should fail.
  """

  def __init__(self, access: HttpToolAccess, tool_name: str) -> None:
    self._access = access
    self._tool_name = tool_name

  async def execute(self, arguments: dict[str, Any]) -> Any:
    return await self._access.call_tool(self._tool_name, arguments, return_raw=True)


class HttpToolAccess:
  """MCP tool access via HTTP client.

  Uses the full GraphMCPTools pipeline (HTTP → Graph API). Best for
  operators that execute Cypher queries or read graph schema.

  ``read_only`` decides whether GraphMCPTools wires its write tools at all.
  It must mirror the operator spec's ``read_only`` flag: the write-role gate
  in the adapters is skipped for read-only operators, so a read-only
  operator handed a write-capable tool surface would have an ungated write
  path. Defaults to read-only so a caller has to opt in to writes.
  """

  def __init__(self, graph_id: str, read_only: bool = True) -> None:
    self._graph_id = graph_id
    self._read_only = read_only
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
    self._tools = GraphMCPTools(
      self._client,
      schema_extensions=schema_extensions,
      read_only=self._read_only,
    )
    logger.info(
      f"Initialized HTTP tool access for graph {self._graph_id} "
      f"(extensions={schema_extensions}, read_only={self._read_only})"
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

  def get_tool_instance(self, tool_class: type) -> Any:
    """Return a call-by-name handle for a tool class.

    Operators that drive tools imperatively hold a tool *object* and call
    ``.execute(args)`` on it. On the direct path that object is the tool
    itself; here it is a handle that routes back through
    :meth:`call_tool`, so execution still goes through ``GraphMCPTools`` —
    which is what applies the ``read_only`` gating and the registrar
    dispatch. Instantiating the tool class and executing it in-process
    would work and would bypass both, so the handle is the point.

    The class is instantiated once here purely to read its declared name;
    that instance is discarded and never executed.
    """
    tool_name = tool_class(self).get_tool_definition()["name"]
    return _RemoteToolHandle(self, tool_name)

  async def get_tool_schemas(self, names: list[str]) -> list[dict[str, Any]]:
    """Return Anthropic-shaped tool definitions for the requested names.

    The requested `names` are intersected with the tools actually available
    on this graph (GraphMCPTools gates by schema extension + feature flag),
    so an operator can ask for a broad read-only allowlist and safely get
    back only what exists. Remaps the MCP `inputSchema` key to Anthropic's
    `input_schema`.
    """
    if self._tools is None:
      await self.initialize()
    wanted = set(names)
    return [
      {
        "name": defn["name"],
        "description": defn["description"],
        "input_schema": defn["inputSchema"],
      }
      for defn in self._tools.get_tool_definitions_as_dict()
      if defn["name"] in wanted
    ]

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
  provides .graph_id. No HTTP overhead. Best for operators that use
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

  async def get_tool_schemas(self, names: list[str]) -> list[dict[str, Any]]:
    """Return Anthropic-shaped definitions for registered tool instances.

    DirectToolAccess is used by operators that drive tools imperatively
    (e.g. MappingOperator), not by the model-driven tool loop. It only
    knows about tool classes explicitly registered via get_tool_instance,
    so it reports those; the loop-based operators use HttpToolAccess.
    """
    wanted = set(names)
    schemas: list[dict[str, Any]] = []
    for tool in self._tool_instances.values():
      defn = tool.get_tool_definition()
      if defn.get("name") in wanted:
        schemas.append(
          {
            "name": defn["name"],
            "description": defn["description"],
            "input_schema": defn["inputSchema"],
          }
        )
    return schemas

  async def close(self) -> None:
    """No-op — no connections to clean up."""
    pass
