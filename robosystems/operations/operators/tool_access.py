"""`ToolAccess` implementations: `HttpToolAccess` (the GraphMCPTools pipeline,
used by both adapters) and `DirectToolAccess` (in-process tool classes)."""

from __future__ import annotations

from typing import Any

from robosystems.logger import logger


class _RemoteToolHandle:
  """Tool-shaped handle whose ``execute`` goes through ``call_tool``.

  ``return_raw=True`` matters: the default JSON string would turn callers'
  ``"error" in result`` checks into substring matches.
  """

  def __init__(self, access: HttpToolAccess, tool_name: str) -> None:
    self._access = access
    self._tool_name = tool_name

  async def execute(self, arguments: dict[str, Any]) -> Any:
    return await self._access.call_tool(self._tool_name, arguments, return_raw=True)


class HttpToolAccess:
  """MCP tool access through the full GraphMCPTools pipeline.

  ``read_only`` must mirror the operator spec's flag: read-only operators skip
  the write-role gate, so a write-capable surface would be an ungated write
  path.
  """

  def __init__(
    self, graph_id: str, read_only: bool = True, user_id: str | None = None
  ) -> None:
    self._graph_id = graph_id
    self._read_only = read_only
    self._user_id = user_id
    self._client = None
    self._tools = None

  @property
  def graph_id(self) -> str:
    return self._graph_id

  async def initialize(self) -> None:
    if self._tools is not None:
      return

    from robosystems.middleware.mcp import GraphMCPTools, create_graph_mcp_client
    from robosystems.middleware.mcp.tools.manager import resolve_schema_extensions

    self._client = await create_graph_mcp_client(graph_id=self._graph_id)
    # Registrar tools stamp `created_by` from this; unset, writes are
    # attributed to `mcp:{graph_id}`.
    if self._user_id:
      self._client.user_id = self._user_id
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
    if self._tools is None:
      await self.initialize()
    return await self._tools.call_tool(tool_name, arguments, return_raw=return_raw)

  def get_tool_instance(self, tool_class: type) -> Any:
    """Return a handle that routes back through :meth:`call_tool`, so the
    ``read_only`` gating and registrar dispatch always apply. The class is
    instantiated only to read its name; never execute a tool object here."""
    tool_name = tool_class(self).get_tool_definition()["name"]
    return _RemoteToolHandle(self, tool_name)

  async def get_tool_schemas(self, names: list[str]) -> list[dict[str, Any]]:
    """MCP-shaped definitions for `names`, intersected with what this graph
    exposes."""
    if self._tools is None:
      await self.initialize()
    wanted = set(names)
    return [
      {
        "name": defn["name"],
        "description": defn["description"],
        "inputSchema": defn["inputSchema"],
      }
      for defn in self._tools.get_tool_definitions_as_dict()
      if defn["name"] in wanted
    ]

  async def close(self) -> None:
    if self._client:
      try:
        await self._client.close()
        logger.debug("Closed HTTP tool access connection")
      except Exception as e:
        logger.error(f"Error closing HTTP tool access: {e}")


class DirectToolAccess:
  """Instantiates tool classes in-process, acting as their client. Bypasses
  GraphMCPTools, so no read-only gating or registrar dispatch; neither adapter
  uses it."""

  def __init__(self, graph_id: str, user_id: str | None = None) -> None:
    self._graph_id = graph_id
    self._user_id = user_id
    self._tool_instances: dict[str, Any] = {}

  @property
  def graph_id(self) -> str:
    return self._graph_id

  @property
  def user_id(self) -> str | None:
    """Read by tools as ``client.user_id`` for ``created_by``."""
    return self._user_id

  def get_tool_instance(self, tool_class: type) -> Any:
    """Memoized per class; this object is the tool's client."""
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
    """Only tools already registered via ``get_tool_instance``."""
    for tool in self._tool_instances.values():
      defn = tool.get_tool_definition()
      if defn.get("name") == tool_name:
        return await tool.execute(arguments)
    raise ValueError(
      f"Tool '{tool_name}' not registered in DirectToolAccess. "
      f"Use get_tool_instance() to register tool classes first."
    )

  async def get_tool_schemas(self, names: list[str]) -> list[dict[str, Any]]:
    """Only registered instances, so a model-driven loop on this sees none."""
    wanted = set(names)
    schemas: list[dict[str, Any]] = []
    for tool in self._tool_instances.values():
      defn = tool.get_tool_definition()
      if defn.get("name") in wanted:
        schemas.append(
          {
            "name": defn["name"],
            "description": defn["description"],
            "inputSchema": defn["inputSchema"],
          }
        )
    return schemas

  async def close(self) -> None:
    pass
