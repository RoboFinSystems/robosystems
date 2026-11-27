"""The multi-graph MCP transport: ``POST /v1/mcp/graphs``.

One connection for every graph an account-wide API key reaches. It serves four
fixed tools; three take the graph as an argument and run against that graph's
own tool layer, exactly as a call to ``/v1/graphs/{graph_id}/mcp`` would. A
graph's tools are data returned by ``get-tools``, never part of this route's
``tools/list``, so the list is the same on every request and a subgraph is
usable the moment it exists. There is no current graph and no state between
requests.

The graph arrives in the request body, after the route's dependencies have
run, so the per-graph checks those dependencies make elsewhere run here per
call: ``_enter_graph`` is the one way into a graph, and every tool that names
one goes through it.
"""

import json
import re
from dataclasses import dataclass
from importlib.metadata import version as pkg_version
from typing import Any

from fastapi import APIRouter, Depends, Request, Response, Security
from fastapi import status as http_status
from fastapi.exceptions import HTTPException

from robosystems.logger import logger
from robosystems.middleware.auth.dependencies import (
  API_KEY_HEADER,
  authorize_api_key_on_graph,
  get_account_api_key_user,
)
from robosystems.middleware.graph import types as graph_types
from robosystems.middleware.mcp.tools.annotations import tool_hints
from robosystems.middleware.operations import run_off_loop
from robosystems.middleware.otel.metrics import endpoint_metrics_decorator
from robosystems.middleware.rate_limits import (
  subscription_aware_rate_limit_dependency,
)
from robosystems.models.core import User

from .remote import (
  INTERNAL_ERROR,
  INVALID_PARAMS,
  MCP_LATEST_PROTOCOL_VERSION,
  MCP_SUPPORTED_PROTOCOL_VERSIONS,
  METHOD_NOT_FOUND,
  _handle_initialize,
  _handle_tools_call,
  _handle_tools_list,
  _rpc_error,
  _rpc_result,
  _tool_error_result,
  _transport_gate,
  read_jsonrpc_request,
)

graphs_router = APIRouter()

# A reference the model passes as `graph`: an id or an alias.
_GRAPH_REF = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")
_READ_ONLY_SUFFIX = ":ro"

SUMMARY_CHARS = 180
MAX_DEFINITIONS = 8
MAX_QUERY_HITS = 25

_GRAPH_ARGUMENT = {
  "type": "string",
  "description": (
    "The graph to act on: a graph id, a subgraph id (`{parent}_{name}`) or an "
    "alias, as `list-graphs` returns them"
  ),
}

_CALL_INPUT_SCHEMA = {
  "type": "object",
  "properties": {
    "graph": _GRAPH_ARGUMENT,
    "name": {"type": "string", "description": "The tool to run"},
    "arguments": {
      "type": "object",
      "description": "The tool's arguments, per its input schema",
      "additionalProperties": True,
    },
  },
  "required": ["graph", "name"],
  "additionalProperties": False,
}


def _read_annotations(title: str) -> dict[str, Any]:
  return {
    "title": title,
    "readOnlyHint": True,
    "destructiveHint": False,
    "idempotentHint": True,
    "openWorldHint": False,
  }


LIST_GRAPHS = "list-graphs"
GET_TOOLS = "get-tools"
CALL_READ_TOOL = "call-read-tool"
CALL_WRITE_TOOL = "call-write-tool"

CATALOG_TOOLS: list[dict[str, Any]] = [
  {
    "name": LIST_GRAPHS,
    "title": "List graphs",
    "description": (
      "List the graphs this connection can reach, with the id or alias to pass "
      "as `graph` to the other tools. Subgraphs are graphs: a subgraph id works "
      "as `graph` as soon as it exists."
    ),
    "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False},
    "annotations": _read_annotations("List graphs"),
  },
  {
    "name": GET_TOOLS,
    "title": "Get a graph's tools",
    "description": (
      "Look up the tools one graph serves. Each graph has its own list: a "
      "ledger graph, a shared repository such as `sec` and a subgraph differ.\n\n"
      "- With `graph` alone: the index (every tool's name and one-line summary, "
      "split into read and write tools) and the graph's own instructions. Call "
      "this first for a graph you have not worked on in this conversation, and "
      "read the instructions.\n"
      "- With `names`: the full description and input schema of those tools. "
      "Read a tool's definition before the first time you call it; never guess "
      "its arguments.\n"
      "- With `query`: the index filtered to tools matching the words given."
    ),
    "inputSchema": {
      "type": "object",
      "properties": {
        "graph": _GRAPH_ARGUMENT,
        "names": {
          "type": "array",
          "items": {"type": "string"},
          "maxItems": MAX_DEFINITIONS,
          "description": (
            f"Tool names to return in full, at most {MAX_DEFINITIONS} per call"
          ),
        },
        "query": {
          "type": "string",
          "description": (
            "Words to filter the index by, matched against names and descriptions"
          ),
        },
      },
      "required": ["graph"],
      "additionalProperties": False,
    },
    "annotations": _read_annotations("Get a graph's tools"),
  },
  {
    "name": CALL_READ_TOOL,
    "title": "Call a read tool",
    "description": (
      "Run one of a graph's read tools (listed under `read_tools` by "
      "`get-tools`). `name` is the tool and `arguments` is its input, exactly "
      "as its definition describes. The result is the tool's own. A write tool "
      "is refused here: use `call-write-tool`."
    ),
    "inputSchema": _CALL_INPUT_SCHEMA,
    "annotations": _read_annotations("Call a read tool"),
  },
  {
    "name": CALL_WRITE_TOOL,
    "title": "Call a write tool",
    "description": (
      "Run one of a graph's write tools (listed under `write_tools` by "
      "`get-tools`). `name` is the tool and `arguments` is its input, exactly "
      "as its definition describes. This changes the graph named in `graph`: "
      "check it is the one you mean. `create-subgraph` on a parent returns a "
      "subgraph id that is usable as `graph` on the next call. A read tool is "
      "refused here: use `call-read-tool`."
    ),
    "inputSchema": _CALL_INPUT_SCHEMA,
    # Any graph write runs through this tool, so it takes the most cautious
    # claim on every hint.
    "annotations": {
      "title": "Call a write tool",
      "readOnlyHint": False,
      "destructiveHint": True,
      "idempotentHint": False,
      "openWorldHint": True,
    },
  },
]


class _Refusal(Exception):
  """A call this connection will not make, worded for the model to act on."""


@dataclass(frozen=True)
class ScopeEntry:
  graph_id: str
  alias: str | None = None
  read_only: bool = False


@dataclass(frozen=True)
class ConnectionScope:
  """What one connection's URL narrows it to.

  ``entries`` empty means every graph the key reaches. These are settings the
  key's holder chose, not enforcement: the key itself bounds access.
  """

  entries: tuple[ScopeEntry, ...] = ()
  read_only: bool = False

  @property
  def is_subset(self) -> bool:
    return bool(self.entries)

  def resolve(self, ref: Any) -> str:
    """The graph id for the model's ``graph`` argument, or a refusal."""
    if not isinstance(ref, str) or not _GRAPH_REF.match(ref):
      raise _Refusal("`graph` must be a graph id or alias from `list-graphs`")
    graph_id = ref
    if self.entries:
      by_alias = {e.alias: e.graph_id for e in self.entries if e.alias}
      graph_id = by_alias.get(ref, ref)
      if not any(_covers(e.graph_id, graph_id) for e in self.entries):
        reach = ", ".join(e.alias or e.graph_id for e in self.entries)
        raise _Refusal(
          f'Graph "{ref}" is outside this connection. It reaches: {reach}, '
          "and their subgraphs"
        )
    if not re.match(graph_types.GRAPH_OR_SUBGRAPH_ID_PATTERN, graph_id):
      raise _Refusal(
        f'"{ref}" is not a graph id. `list-graphs` returns the ids this '
        "connection reaches."
      )
    return graph_id

  def writes_withheld(self, graph_id: str) -> bool:
    return self.read_only or any(
      e.read_only and _covers(e.graph_id, graph_id) for e in self.entries
    )


def _covers(entry_graph_id: str, graph_id: str) -> bool:
  """A configured graph covers itself and its subgraphs.

  Relies on a subgraph id being ``{parent}_{name}`` and no top-level id
  containing an underscore.
  """
  return graph_id == entry_graph_id or graph_id.startswith(f"{entry_graph_id}_")


def parse_connection_scope(
  graphs: str | None, read_only: str | None
) -> ConnectionScope:
  """Read ``?graphs=`` and ``?read_only=`` off the route URL.

  ``graphs`` is a comma-separated list of graph ids, each optionally named
  (``books=kg1a…``) and optionally read-only (``sec:ro``). Raises
  ``ValueError`` on anything else: a malformed setting must not quietly widen
  the connection to every graph.
  """
  flag = (read_only or "").strip().lower()
  if flag not in ("", "true", "false", "1", "0"):
    raise ValueError("read_only must be true or false")
  entries: list[ScopeEntry] = []
  if graphs is not None:
    seen: set[str] = set()
    for part in graphs.split(","):
      item = part.strip()
      if not item:
        continue
      entry_read_only = item.endswith(_READ_ONLY_SUFFIX)
      pair = item[: -len(_READ_ONLY_SUFFIX)].strip() if entry_read_only else item
      alias, eq, graph_id = pair.rpartition("=")
      alias = alias.strip() if eq else None
      graph_id = graph_id.strip()
      if not re.match(graph_types.GRAPH_OR_SUBGRAPH_ID_PATTERN, graph_id) or (
        alias is not None and not _GRAPH_REF.match(alias)
      ):
        raise ValueError(
          f'graphs: "{item}" is not a graph id or alias=id pair '
          f'(add "{_READ_ONLY_SUFFIX}" for read-only)'
        )
      if graph_id in seen:
        continue
      seen.add(graph_id)
      entries.append(ScopeEntry(graph_id, alias, entry_read_only))
    if not entries:
      raise ValueError("graphs is set but names no graph")
    aliases = {e.alias for e in entries if e.alias}
    if len(aliases) != sum(1 for e in entries if e.alias):
      raise ValueError("graphs: an alias is used twice")
    clash = aliases & seen
    if clash:
      raise ValueError(f'graphs: alias "{sorted(clash)[0]}" is also a graph id')
  return ConnectionScope(tuple(entries), flag in ("true", "1"))


def get_connection_scope(request: Request) -> ConnectionScope:
  try:
    return parse_connection_scope(
      request.query_params.get("graphs"), request.query_params.get("read_only")
    )
  except ValueError as e:
    raise HTTPException(
      status_code=http_status.HTTP_400_BAD_REQUEST, detail=str(e)
    ) from e


def _instructions(scope: ConnectionScope) -> str:
  if scope.is_subset:

    def label(entry: ScopeEntry) -> str:
      name = (
        f"`{entry.alias}` (`{entry.graph_id}`)"
        if entry.alias
        else f"`{entry.graph_id}`"
      )
      return f"{name}, read-only" if entry.read_only and not scope.read_only else name

    reach = (
      "This connection is limited to these graphs and their subgraphs: "
      + ", ".join(label(e) for e in scope.entries)
      + "."
    )
  else:
    reach = "This connection reaches every graph the API key can."

  lead = (
    "Each graph is its own MCP server with its own tools and instructions; this "
    "connection looks them up and calls them for you."
  )
  list_graphs = (
    "- `list-graphs` says what you can reach. Every other tool takes `graph`: "
    "there is no current or default graph, so name it on every call."
  )
  if scope.read_only:
    return "\n".join(
      [
        f"RoboSystems, through one read-only connection for many graphs. {lead}",
        reach,
        "This connection cannot change anything: write tools are neither listed "
        "nor callable.",
        "HOW TO WORK",
        list_graphs,
        "- Before working on a graph, call `get-tools` with `graph` alone and "
        "read the instructions it returns. They are that graph's own and they "
        "differ from graph to graph. Where they describe a write, it is not "
        "available here.",
        "- Before the first call to a tool, call `get-tools` with its name in "
        "`names` and follow its description and input schema, then run it with "
        "`call-read-tool`.",
        "When several graphs are in play, say which graph a figure belongs to.",
      ]
    )
  return "\n".join(
    [
      f"RoboSystems, through one connection for many graphs. {lead}",
      reach,
      "HOW TO WORK",
      list_graphs,
      "- Before working on a graph, call `get-tools` with `graph` alone and read "
      "the instructions it returns. They are that graph's own and they differ "
      "from graph to graph.",
      "- Before the first call to a tool, call `get-tools` with its name in "
      "`names` and follow its description and input schema.",
      "- Run it with `call-read-tool` or `call-write-tool`, whichever list the "
      "index put it in.",
      "SUBGRAPHS",
      "- A subgraph is a graph with its own, smaller tool list. Create one with "
      "the parent's `create-subgraph` (a write tool); the id it returns works as "
      "`graph` on the next call. The parent's `list-subgraphs` lists them, and "
      "`delete-subgraph` removes one.",
      "- A result or instruction that says to add a connector for a "
      "`connector_url` is written for a connection to a single graph. Here, "
      "pass the subgraph id as `graph` instead.",
      "When several graphs are in play, say which graph a figure or a write "
      "belongs to.",
    ]
  )


def _served_tools(scope: ConnectionScope) -> list[dict[str, Any]]:
  if scope.read_only:
    return [t for t in CATALOG_TOOLS if t["name"] != CALL_WRITE_TOOL]
  return CATALOG_TOOLS


def _json_result(msg_id: Any, value: dict[str, Any]) -> Response:
  return _rpc_result(
    msg_id,
    {"content": [{"type": "text", "text": json.dumps(value)}], "isError": False},
  )


async def _charge(request: Request) -> None:
  """Charge this request to a rate-limit bucket, once.

  The limiter draws on a graph's budget when the caller has been authorized
  on one (``_enter_graph``), and on the caller's own otherwise. Raises 429.
  """
  await run_off_loop(subscription_aware_rate_limit_dependency, request)


async def _enter_graph(
  request: Request,
  current_user: User,
  api_key: str,
  scope: ConnectionScope,
  ref: Any,
) -> str:
  """The one way into a graph on this route. Returns its id, or refuses.

  In order: the connection's own bounds, the key's access to the graph, then
  the rate limit, charged to that graph. A refused call is charged to the
  caller, so probing for graphs is limited like any other request.
  """
  try:
    graph_id = scope.resolve(ref)
    try:
      # The route already validated the key, so a refusal here is this
      # user lacking the graph, and is audited as that.
      await authorize_api_key_on_graph(
        request, api_key, graph_id, known_user_id=str(current_user.id)
      )
    except HTTPException as e:
      if e.status_code != http_status.HTTP_403_FORBIDDEN:
        raise
      raise _Refusal(
        f"This API key cannot reach graph {graph_id}. `list-graphs` returns "
        "the graphs it can."
      ) from e
  except _Refusal, HTTPException:
    await _charge(request)
    raise
  await _charge(request)
  return graph_id


def _summarize(description: Any) -> str:
  """The first paragraph of a tool description that says what the tool does."""
  paragraphs = [
    " ".join(p.split()) for p in re.split(r"\n\s*\n", str(description or ""))
  ]
  lead = next((p for p in paragraphs if p and not p.startswith("**GRAPH:**")), "")
  if len(lead) > SUMMARY_CHARS:
    return lead[: SUMMARY_CHARS - 1].rstrip() + "…"
  return lead


def _is_read(tool: dict[str, Any]) -> bool:
  return (tool.get("annotations") or {}).get("readOnlyHint") is True


def _search(tools: list[dict[str, Any]], query: str) -> list[dict[str, Any]]:
  words = query.lower().split()
  scored: list[tuple[int, int, dict[str, Any]]] = []
  for position, tool in enumerate(tools):
    name = tool["name"].lower()
    title = str(tool.get("title") or "").lower()
    description = str(tool.get("description") or "").lower()
    score = 0
    for word in words:
      if word in name:
        score += 3
      elif word in title:
        score += 2
      elif word in description:
        score += 1
    if score:
      scored.append((-score, position, tool))
  return [tool for _, _, tool in sorted(scored, key=lambda s: s[:2])[:MAX_QUERY_HITS]]


async def _list_graphs(
  msg_id: Any, current_user: User, scope: ConnectionScope
) -> Response:
  from robosystems.database import SessionFactory
  from robosystems.operations.graph.listing import list_user_graphs

  def load() -> list[Any]:
    sess = SessionFactory()
    try:
      return list_user_graphs(str(current_user.id), sess).graphs
    finally:
      sess.close()

  listed = await run_off_loop(load)
  alias_of = {e.graph_id: e.alias for e in scope.entries}
  configured = set(alias_of)

  subgraph_count: dict[str, int] = {}
  for graph in listed:
    if graph.parentGraphId:
      subgraph_count[graph.parentGraphId] = (
        subgraph_count.get(graph.parentGraphId, 0) + 1
      )

  rows: list[dict[str, Any]] = []
  for graph in listed:
    if scope.is_subset and not (
      graph.graphId in configured or graph.parentGraphId in configured
    ):
      continue
    row: dict[str, Any] = {"graph": graph.graphId}
    if alias_of.get(graph.graphId):
      row["alias"] = alias_of[graph.graphId]
    if graph.graphName:
      row["name"] = graph.graphName
    if graph.description and graph.description != graph.graphName:
      row["description"] = graph.description
    if graph.graphType:
      row["type"] = graph.graphType
    if graph.schemaExtensions:
      row["extensions"] = graph.schemaExtensions
    if graph.repositoryType:
      row["repository"] = graph.repositoryType
    if graph.parentGraphId:
      row["parent"] = graph.parentGraphId
    if subgraph_count.get(graph.graphId):
      row["subgraphs"] = subgraph_count[graph.graphId]
    if graph.role:
      row["role"] = graph.role
    if graph.status:
      row["status"] = graph.status
    if scope.writes_withheld(graph.graphId):
      row["read_only"] = True
    rows.append(row)

  # A configured graph the account listing does not show is still named, so
  # the model sees the whole of what the URL set; access is decided on use.
  seen = {row["graph"] for row in rows}
  for entry in scope.entries:
    if entry.graph_id in seen:
      continue
    row = {"graph": entry.graph_id}
    if entry.alias:
      row["alias"] = entry.alias
    if scope.writes_withheld(entry.graph_id):
      row["read_only"] = True
    row["note"] = "Configured for this connection; not in the account listing"
    rows.append(row)

  return _json_result(
    msg_id,
    {
      "graphs": rows,
      "scope": "the configured graphs and their subgraphs"
      if scope.is_subset
      else "every graph the key reaches",
      "next": "Pass `graph` (the alias when there is one) to `get-tools`.",
    },
  )


async def _get_tools(
  msg_id: Any,
  graph_id: str,
  current_user: User,
  scope: ConnectionScope,
  args: dict[str, Any],
) -> Response:
  served = (await _handle_tools_list(graph_id, current_user))["tools"]
  withheld = scope.writes_withheld(graph_id)
  tools = [t for t in served if _is_read(t)] if withheld else served

  names = args.get("names")
  if isinstance(names, list) and names:
    asked = [n for n in names if isinstance(n, str)]
    wanted = asked[:MAX_DEFINITIONS]
    by_name = {t["name"]: t for t in tools}
    served_names = {t["name"] for t in served}
    payload: dict[str, Any] = {
      "graph": graph_id,
      "tools": [
        {
          "name": name,
          "call_with": CALL_READ_TOOL if _is_read(by_name[name]) else CALL_WRITE_TOOL,
          "description": by_name[name].get("description", ""),
          "inputSchema": by_name[name].get("inputSchema"),
        }
        for name in wanted
        if name in by_name
      ],
    }
    unknown = [n for n in wanted if n not in served_names]
    writes = [n for n in wanted if n in served_names and n not in by_name]
    if unknown:
      payload["not_on_this_graph"] = unknown
    if writes:
      payload["withheld_read_only"] = writes
    if len(asked) > MAX_DEFINITIONS:
      payload["omitted"] = asked[MAX_DEFINITIONS:]
    return _json_result(msg_id, payload)

  raw_query = args.get("query")
  query = raw_query.strip() if isinstance(raw_query, str) else ""
  listed = _search(tools, query) if query else tools

  def row(tool: dict[str, Any]) -> dict[str, str]:
    return {"name": tool["name"], "summary": _summarize(tool.get("description"))}

  payload = {
    "graph": graph_id,
    "tool_count": len(tools),
    "read_tools": [row(t) for t in listed if _is_read(t)],
  }
  if withheld:
    payload["read_only"] = True
    payload["write_tools_withheld"] = len(served) - len(tools)
  else:
    payload["write_tools"] = [row(t) for t in listed if not _is_read(t)]
  payload["next"] = (
    "Call `get-tools` with `names` for the full definition before calling a tool."
  )
  if query:
    payload["query"] = query
  else:
    # The graph's own instructions, as its initialize would send them.
    # Guidance only: a graph without them is still usable.
    try:
      instructions = (await _handle_initialize(graph_id, current_user, {})).get(
        "instructions"
      )
    except Exception as e:
      logger.warning(f"Failed to read MCP instructions for graph {graph_id}: {e}")
      instructions = None
    if instructions:
      payload["instructions"] = instructions
  return _json_result(msg_id, payload)


async def _call_graph_tool(
  request: Request,
  msg_id: Any,
  graph_id: str,
  current_user: User,
  scope: ConnectionScope,
  args: dict[str, Any],
  meta: Any,
  *,
  want_read: bool,
) -> Response:
  name = args.get("name")
  if not isinstance(name, str) or not name:
    return _tool_error_result(
      msg_id, "`name` is required: the tool to run, from `get-tools`"
    )
  withheld = scope.writes_withheld(graph_id)
  if not want_read and withheld:
    return _tool_error_result(
      msg_id, f"Graph {graph_id} is read-only on this connection."
    )
  # A name no table lists is a write here, as it is in the annotations
  # `get-tools` splits its index on, so the graph itself answers for it.
  is_read = tool_hints(name).read_only
  if is_read != want_read:
    if want_read and withheld:
      text = (
        f'"{name}" is a write tool, and graph {graph_id} is read-only on this '
        "connection."
      )
    elif want_read:
      text = f'"{name}" is a write tool: call it with `{CALL_WRITE_TOOL}`.'
    else:
      text = f'"{name}" is a read tool: call it with `{CALL_READ_TOOL}`.'
    return _tool_error_result(msg_id, text)

  # From here it is an ordinary tools/call on the graph: authorization, write
  # classification, the breaker, streaming and audit all run as they do on
  # the per-graph route.
  params: dict[str, Any] = {"name": name, "arguments": args.get("arguments") or {}}
  if meta is not None:
    params["_meta"] = meta
  return await _handle_tools_call(request, graph_id, current_user, msg_id, params)


async def _handle_catalog_call(
  request: Request,
  current_user: User,
  api_key: str,
  scope: ConnectionScope,
  msg_id: Any,
  params: dict[str, Any],
) -> Response:
  name = params.get("name")
  if not isinstance(name, str) or not name:
    await _charge(request)
    return _rpc_error(msg_id, INVALID_PARAMS, "Invalid params: 'name' is required")
  raw_args = params.get("arguments")
  args: dict[str, Any] = raw_args if isinstance(raw_args, dict) else {}

  if scope.read_only and name == CALL_WRITE_TOOL:
    await _charge(request)
    return _tool_error_result(
      msg_id, "This connection is read-only: no write tool can be called."
    )
  if name not in (LIST_GRAPHS, GET_TOOLS, CALL_READ_TOOL, CALL_WRITE_TOOL):
    await _charge(request)
    return _tool_error_result(
      msg_id,
      f"Unknown tool: {name}. A graph's own tools run through "
      f"`{CALL_READ_TOOL}` or `{CALL_WRITE_TOOL}`.",
    )
  if name == LIST_GRAPHS:
    await _charge(request)
    return await _list_graphs(msg_id, current_user, scope)

  try:
    graph_id = await _enter_graph(
      request, current_user, api_key, scope, args.get("graph")
    )
  except _Refusal as refusal:
    return _tool_error_result(msg_id, str(refusal))

  if name == GET_TOOLS:
    try:
      return await _get_tools(msg_id, graph_id, current_user, scope, args)
    except HTTPException as e:
      # The graph refused the read (a suspended graph, an ended trial).
      detail = e.detail if isinstance(e.detail, str) else json.dumps(e.detail)
      return _tool_error_result(msg_id, detail)
  return await _call_graph_tool(
    request,
    msg_id,
    graph_id,
    current_user,
    scope,
    args,
    params.get("_meta"),
    want_read=name == CALL_READ_TOOL,
  )


async def dispatch_catalog_jsonrpc(
  request: Request,
  current_user: User,
  api_key: str,
  scope: ConnectionScope,
) -> Response:
  """Parse and dispatch one JSON-RPC message on the multi-graph route.

  Every request is charged to one rate-limit bucket: the target graph's when
  a call enters one, the caller's own otherwise.
  """
  parsed = await read_jsonrpc_request(request)
  if isinstance(parsed, Response):
    await _charge(request)
    return parsed
  msg_id, method, params = parsed

  try:
    if method == "tools/call":
      return await _handle_catalog_call(
        request, current_user, api_key, scope, msg_id, params
      )
    await _charge(request)
    if method == "initialize":
      requested = params.get("protocolVersion")
      return _rpc_result(
        msg_id,
        {
          "protocolVersion": requested
          if isinstance(requested, str) and requested in MCP_SUPPORTED_PROTOCOL_VERSIONS
          else MCP_LATEST_PROTOCOL_VERSION,
          "capabilities": {"tools": {"listChanged": False}},
          "serverInfo": {
            "name": "robosystems",
            "title": "RoboSystems",
            "version": pkg_version("robosystems"),
          },
          "instructions": _instructions(scope),
        },
      )
    if method == "ping":
      return _rpc_result(msg_id, {})
    if method == "tools/list":
      return _rpc_result(msg_id, {"tools": _served_tools(scope)})
    return _rpc_error(msg_id, METHOD_NOT_FOUND, f"Method not found: {method}")
  except HTTPException:
    # A rate-limit refusal keeps its status and headers.
    raise
  except Exception as e:
    logger.error(
      f"Multi-graph MCP transport error on {method}: {e}",
      extra={"user_id": str(current_user.id)},
    )
    return _rpc_error(msg_id, INTERNAL_ERROR, "Internal error")


@graphs_router.post("", include_in_schema=False, response_model=None)
@endpoint_metrics_decorator("/v1/mcp/graphs", business_event_type="mcp_remote_request")
async def mcp_graphs_transport(
  request: Request,
  _transport: None = Depends(_transport_gate),
  scope: ConnectionScope = Depends(get_connection_scope),
  current_user: User = Depends(get_account_api_key_user),
  api_key: str = Security(API_KEY_HEADER),
) -> Response:
  """Streamable-HTTP MCP endpoint (JSON-RPC 2.0) for every graph an
  account-wide API key reaches. Each call names its graph."""
  return await dispatch_catalog_jsonrpc(request, current_user, api_key, scope)
