"""The multi-graph MCP transport (`POST /v1/mcp/graphs`).

The graph arrives in the request body, so the checks a per-graph route makes
in its dependencies run here per call. These tests hold that every tool that
names a graph goes through `_enter_graph`, and what `_enter_graph` does.
"""

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import pytest
from fastapi import HTTPException

from robosystems.routers.graphs.mcp import catalog
from robosystems.routers.graphs.mcp.catalog import (
  CALL_READ_TOOL,
  CALL_WRITE_TOOL,
  CATALOG_TOOLS,
  GET_TOOLS,
  LIST_GRAPHS,
  ConnectionScope,
  ScopeEntry,
  dispatch_catalog_jsonrpc,
  parse_connection_scope,
)

KG = "kg1a2b3c4d5e6f7a8b9c0d"
KG2 = "kg9f8e7d6c5b4a39281706"
KEY = "rfs" + "a" * 64
EVERY_GRAPH = ConnectionScope()


def _body(response):
  return json.loads(response.body)


def _text(response):
  return _body(response)["result"]["content"][0]["text"]


def _payload(response):
  return json.loads(_text(response))


def _request(message):
  request = Mock()
  request.json = AsyncMock(return_value=message)
  request.headers = {"accept": "application/json, text/event-stream"}
  request.state = SimpleNamespace()
  return request


def _user():
  user = Mock()
  user.id = "user-123"
  return user


def _call(name, arguments=None, meta=None):
  params = {"name": name, "arguments": arguments or {}}
  if meta is not None:
    params["_meta"] = meta
  return {"jsonrpc": "2.0", "id": 7, "method": "tools/call", "params": params}


GRAPH_TOOLS = [
  {
    "name": "get-graph-schema",
    "title": "Get graph schema",
    "description": "**GRAPH:** kg…\n\nReturn the schema.\n\nMore detail.",
    "inputSchema": {"type": "object", "properties": {}},
    "annotations": {"readOnlyHint": True},
  },
  {
    "name": "create-subgraph",
    "title": "Create subgraph",
    "description": "Create a subgraph.",
    "inputSchema": {"type": "object", "properties": {"name": {"type": "string"}}},
    "annotations": {"readOnlyHint": False},
  },
]


class _Gate:
  """Patches the per-graph seams and records the order they ran in."""

  def __init__(self, *, deny=False, limited=False):
    self.events: list[tuple[str, str | None]] = []
    self.deny = deny
    self.limited = limited

  async def authorize(self, request, api_key, graph_id, **_):
    self.events.append(("authorize", graph_id))
    if self.deny:
      raise HTTPException(status_code=403, detail="denied")
    request.state.auth_graph_id = graph_id

  async def charge(self, request):
    self.events.append(("charge", getattr(request.state, "auth_graph_id", None)))
    if self.limited:
      raise HTTPException(status_code=429, detail="Rate limit exceeded")

  def __enter__(self):
    self._patches = [
      patch.object(catalog, "authorize_api_key_on_graph", self.authorize),
      patch.object(catalog, "_charge", self.charge),
      patch.object(
        catalog, "_handle_tools_list", AsyncMock(return_value={"tools": GRAPH_TOOLS})
      ),
      patch.object(
        catalog,
        "_handle_initialize",
        AsyncMock(return_value={"instructions": "graph guidance"}),
      ),
      patch.object(catalog, "_handle_tools_call", AsyncMock(return_value="ran")),
    ]
    mocks = [p.start() for p in self._patches]
    self.tools_list, self.initialize, self.tools_call = mocks[2:]
    return self

  def __exit__(self, *exc):
    for p in self._patches:
      p.stop()


async def _dispatch(message, scope=EVERY_GRAPH, request=None):
  return await dispatch_catalog_jsonrpc(
    request or _request(message), _user(), KEY, scope
  )


class TestConnectionScope:
  def test_unset_reaches_every_graph(self):
    scope = parse_connection_scope(None, None)
    assert scope == ConnectionScope()
    assert scope.resolve(KG) == KG
    assert not scope.writes_withheld(KG)

  def test_ids_aliases_and_read_only_marks(self):
    scope = parse_connection_scope(f"books={KG}, sec:ro", None)
    assert scope.entries == (
      ScopeEntry(KG, "books", False),
      ScopeEntry("sec", None, True),
    )
    assert scope.resolve("books") == KG
    assert scope.resolve(f"{KG}_dev") == f"{KG}_dev"
    assert scope.writes_withheld("sec")
    assert scope.writes_withheld("sec_dev")
    assert not scope.writes_withheld(KG)

  def test_a_graph_outside_the_set_is_refused(self):
    scope = parse_connection_scope(KG, None)
    with pytest.raises(catalog._Refusal, match="outside this connection"):
      scope.resolve(KG2)

  def test_a_longer_id_is_not_a_subgraph_of_a_configured_one(self):
    scope = parse_connection_scope(KG, None)
    with pytest.raises(catalog._Refusal):
      scope.resolve(KG + "ff")

  @pytest.mark.parametrize("ref", ["", "../kg1", "kg1/..", "a b", None, 5, "x" * 80])
  def test_an_unsafe_reference_is_refused(self, ref):
    with pytest.raises(catalog._Refusal):
      EVERY_GRAPH.resolve(ref)

  def test_a_reference_that_is_not_a_graph_id_is_refused(self):
    with pytest.raises(catalog._Refusal, match="not a graph id"):
      EVERY_GRAPH.resolve("mcp")

  def test_read_only_connection_withholds_every_write(self):
    scope = parse_connection_scope(None, "true")
    assert scope.read_only
    assert scope.writes_withheld(KG)

  @pytest.mark.parametrize(
    "graphs",
    ["", " , ", "not-a-graph", f"bad alias={KG}", f"a={KG},a={KG2}", f"sec={KG},sec"],
  )
  def test_a_malformed_setting_never_widens_the_connection(self, graphs):
    with pytest.raises(ValueError):
      parse_connection_scope(graphs, None)

  def test_read_only_must_be_a_boolean(self):
    with pytest.raises(ValueError):
      parse_connection_scope(None, "yes please")

  def test_a_bad_setting_on_the_url_is_a_400(self):
    request = Mock()
    request.query_params = {"graphs": "nope"}
    with pytest.raises(HTTPException) as exc:
      catalog.get_connection_scope(request)
    assert exc.value.status_code == 400


@pytest.mark.asyncio
class TestHandshake:
  async def test_initialize_names_no_graph(self):
    with _Gate() as gate:
      response = await _dispatch(
        {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}}
      )
    result = _body(response)["result"]
    assert result["serverInfo"]["name"] == "robosystems"
    assert result["capabilities"] == {"tools": {"listChanged": False}}
    assert "every graph the API key can" in result["instructions"]
    # No graph was entered; the request drew on the caller's own budget.
    assert gate.events == [("charge", None)]

  async def test_instructions_name_the_configured_set(self):
    scope = parse_connection_scope(f"books={KG},sec:ro", None)
    with _Gate():
      response = await _dispatch(
        {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}}, scope
      )
    instructions = _body(response)["result"]["instructions"]
    assert f"`books` (`{KG}`)" in instructions
    assert "`sec`, read-only" in instructions

  async def test_the_tool_list_is_fixed(self):
    listings = []
    for scope in (EVERY_GRAPH, parse_connection_scope(f"{KG},sec:ro", None)):
      with _Gate():
        response = await _dispatch(
          {"jsonrpc": "2.0", "id": 1, "method": "tools/list"}, scope
        )
      listings.append(_body(response)["result"]["tools"])
    assert listings[0] == listings[1] == CATALOG_TOOLS
    assert [t["name"] for t in CATALOG_TOOLS] == [
      LIST_GRAPHS,
      GET_TOOLS,
      CALL_READ_TOOL,
      CALL_WRITE_TOOL,
    ]

  async def test_a_read_only_connection_does_not_list_the_write_tool(self):
    with _Gate():
      response = await _dispatch(
        {"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
        ConnectionScope(read_only=True),
      )
    names = [t["name"] for t in _body(response)["result"]["tools"]]
    assert names == [LIST_GRAPHS, GET_TOOLS, CALL_READ_TOOL]

  async def test_a_notification_is_accepted_and_charged(self):
    with _Gate() as gate:
      response = await _dispatch(
        {"jsonrpc": "2.0", "method": "notifications/initialized"}
      )
    assert response.status_code == 202
    assert gate.events == [("charge", None)]

  async def test_unknown_method(self):
    with _Gate():
      response = await _dispatch({"jsonrpc": "2.0", "id": 1, "method": "nope"})
    assert _body(response)["error"]["code"] == catalog.METHOD_NOT_FOUND

  async def test_a_rate_limit_refusal_keeps_its_status(self):
    with _Gate(limited=True), pytest.raises(HTTPException) as exc:
      await _dispatch({"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
    assert exc.value.status_code == 429


@pytest.mark.asyncio
class TestEnterGraph:
  """Every tool that names a graph: access first, then that graph's budget."""

  @pytest.mark.parametrize(
    "tool,arguments",
    [
      (GET_TOOLS, {}),
      (GET_TOOLS, {"names": ["get-graph-schema"]}),
      (CALL_READ_TOOL, {"name": "get-graph-schema"}),
      (CALL_WRITE_TOOL, {"name": "create-subgraph"}),
    ],
  )
  async def test_access_is_checked_then_the_graph_is_charged(self, tool, arguments):
    with _Gate() as gate:
      await _dispatch(_call(tool, {"graph": KG, **arguments}))
    assert gate.events == [("authorize", KG), ("charge", KG)]

  @pytest.mark.parametrize(
    "tool,arguments",
    [
      (GET_TOOLS, {}),
      (GET_TOOLS, {"names": ["get-graph-schema"]}),
      (GET_TOOLS, {"query": "schema"}),
      (CALL_READ_TOOL, {"name": "get-graph-schema"}),
      (CALL_WRITE_TOOL, {"name": "create-subgraph"}),
    ],
  )
  async def test_a_graph_the_key_cannot_reach_is_refused(self, tool, arguments):
    with _Gate(deny=True) as gate:
      response = await _dispatch(_call(tool, {"graph": KG2, **arguments}))
    result = _body(response)["result"]
    assert result["isError"] is True
    assert "cannot reach graph" in result["content"][0]["text"]
    # Nothing of the graph was read or run, and the refusal drew on the
    # caller's budget, never the graph's.
    gate.tools_list.assert_not_awaited()
    gate.initialize.assert_not_awaited()
    gate.tools_call.assert_not_awaited()
    assert gate.events == [("authorize", KG2), ("charge", None)]

  @pytest.mark.parametrize("tool", [GET_TOOLS, CALL_READ_TOOL, CALL_WRITE_TOOL])
  async def test_a_graph_outside_the_set_never_reaches_the_access_check(self, tool):
    scope = parse_connection_scope(KG, None)
    with _Gate() as gate:
      response = await _dispatch(_call(tool, {"graph": KG2, "name": "x"}), scope)
    assert _body(response)["result"]["isError"] is True
    assert gate.events == [("charge", None)]
    gate.tools_call.assert_not_awaited()

  @pytest.mark.parametrize("tool", [GET_TOOLS, CALL_READ_TOOL, CALL_WRITE_TOOL])
  async def test_a_missing_graph_is_refused(self, tool):
    with _Gate() as gate:
      response = await _dispatch(_call(tool, {"name": "get-graph-schema"}))
    assert _body(response)["result"]["isError"] is True
    assert ("authorize", KG) not in gate.events
    gate.tools_call.assert_not_awaited()

  async def test_an_alias_enters_its_graph(self):
    scope = parse_connection_scope(f"books={KG}", None)
    with _Gate() as gate:
      await _dispatch(_call(GET_TOOLS, {"graph": "books"}), scope)
    assert gate.events[0] == ("authorize", KG)

  async def test_a_rate_limited_graph_call_runs_nothing(self):
    with _Gate(limited=True) as gate, pytest.raises(HTTPException) as exc:
      await _dispatch(_call(CALL_WRITE_TOOL, {"graph": KG, "name": "create-subgraph"}))
    assert exc.value.status_code == 429
    gate.tools_call.assert_not_awaited()

  async def test_the_key_checked_is_the_request_key(self):
    authorize = AsyncMock()
    with (
      patch.object(catalog, "authorize_api_key_on_graph", authorize),
      patch.object(catalog, "_charge", AsyncMock()),
      patch.object(
        catalog, "_handle_tools_list", AsyncMock(return_value={"tools": []})
      ),
      patch.object(catalog, "_handle_initialize", AsyncMock(return_value={})),
    ):
      request = _request(_call(GET_TOOLS, {"graph": KG}))
      await _dispatch(None, request=request)
    authorize.assert_awaited_once_with(request, KEY, KG)


@pytest.mark.asyncio
class TestGetTools:
  async def test_the_index_splits_reads_and_writes_and_carries_instructions(self):
    with _Gate():
      payload = _payload(await _dispatch(_call(GET_TOOLS, {"graph": KG})))
    assert payload["graph"] == KG
    assert payload["tool_count"] == 2
    assert payload["read_tools"] == [
      {"name": "get-graph-schema", "summary": "Return the schema."}
    ]
    assert payload["write_tools"] == [
      {"name": "create-subgraph", "summary": "Create a subgraph."}
    ]
    assert payload["instructions"] == "graph guidance"

  async def test_names_return_full_definitions(self):
    with _Gate() as gate:
      payload = _payload(
        await _dispatch(
          _call(GET_TOOLS, {"graph": KG, "names": ["create-subgraph", "no-such-tool"]})
        )
      )
    assert payload["tools"] == [
      {
        "name": "create-subgraph",
        "call_with": CALL_WRITE_TOOL,
        "description": "Create a subgraph.",
        "inputSchema": GRAPH_TOOLS[1]["inputSchema"],
      }
    ]
    assert payload["not_on_this_graph"] == ["no-such-tool"]
    gate.initialize.assert_not_awaited()

  async def test_names_past_the_limit_are_reported(self):
    names = [f"tool-{i}" for i in range(catalog.MAX_DEFINITIONS + 2)]
    with _Gate():
      payload = _payload(
        await _dispatch(_call(GET_TOOLS, {"graph": KG, "names": names}))
      )
    assert payload["omitted"] == names[catalog.MAX_DEFINITIONS :]

  async def test_a_query_filters_the_index(self):
    with _Gate():
      payload = _payload(
        await _dispatch(_call(GET_TOOLS, {"graph": KG, "query": "subgraph"}))
      )
    assert payload["read_tools"] == []
    assert [t["name"] for t in payload["write_tools"]] == ["create-subgraph"]
    assert "instructions" not in payload

  async def test_a_read_only_graph_leaves_write_tools_out(self):
    scope = parse_connection_scope(f"{KG}:ro", None)
    with _Gate():
      index = _payload(await _dispatch(_call(GET_TOOLS, {"graph": KG}), scope))
      named = _payload(
        await _dispatch(
          _call(GET_TOOLS, {"graph": KG, "names": ["create-subgraph"]}), scope
        )
      )
    assert "write_tools" not in index
    assert index["read_only"] is True
    assert index["write_tools_withheld"] == 1
    assert named["tools"] == []
    assert named["withheld_read_only"] == ["create-subgraph"]

  async def test_the_graph_refusing_the_read_is_a_tool_error(self):
    with _Gate() as gate:
      gate.tools_list.side_effect = HTTPException(status_code=403, detail="suspended")
      response = await _dispatch(_call(GET_TOOLS, {"graph": KG}))
    result = _body(response)["result"]
    assert result["isError"] is True
    assert result["content"][0]["text"] == "suspended"

  async def test_missing_instructions_do_not_fail_the_index(self):
    with _Gate() as gate:
      gate.initialize.side_effect = RuntimeError("boom")
      payload = _payload(await _dispatch(_call(GET_TOOLS, {"graph": KG})))
    assert "instructions" not in payload
    assert payload["tool_count"] == 2


@pytest.mark.asyncio
class TestCallTool:
  async def test_a_call_is_an_ordinary_tools_call_on_the_graph(self):
    request = _request(
      _call(
        CALL_WRITE_TOOL,
        {"graph": KG, "name": "create-subgraph", "arguments": {"name": "dev"}},
        meta={"progressToken": "p1"},
      )
    )
    with _Gate() as gate:
      response = await _dispatch(None, request=request)
    assert response == "ran"
    args = gate.tools_call.await_args.args
    assert args[0] is request
    assert args[1] == KG
    assert args[3] == 7
    assert args[4] == {
      "name": "create-subgraph",
      "arguments": {"name": "dev"},
      "_meta": {"progressToken": "p1"},
    }

  async def test_a_write_is_refused_through_the_read_tool(self):
    with _Gate() as gate:
      response = await _dispatch(
        _call(CALL_READ_TOOL, {"graph": KG, "name": "create-subgraph"})
      )
    assert CALL_WRITE_TOOL in _text(response)
    gate.tools_call.assert_not_awaited()

  async def test_a_read_is_refused_through_the_write_tool(self):
    with _Gate() as gate:
      response = await _dispatch(
        _call(CALL_WRITE_TOOL, {"graph": KG, "name": "get-graph-schema"})
      )
    assert CALL_READ_TOOL in _text(response)
    gate.tools_call.assert_not_awaited()

  async def test_an_unknown_tool_is_not_called(self):
    for tool in (CALL_READ_TOOL, CALL_WRITE_TOOL):
      with _Gate() as gate:
        response = await _dispatch(_call(tool, {"graph": KG, "name": "no-such-tool"}))
      assert "no tool named" in _text(response)
      gate.tools_call.assert_not_awaited()

  async def test_a_missing_tool_name_is_refused(self):
    with _Gate() as gate:
      response = await _dispatch(_call(CALL_READ_TOOL, {"graph": KG}))
    assert _body(response)["result"]["isError"] is True
    gate.tools_call.assert_not_awaited()

  async def test_a_read_only_graph_refuses_writes_on_either_tool(self):
    scope = parse_connection_scope(f"{KG}:ro,{KG2}", None)
    with _Gate() as gate:
      for tool in (CALL_READ_TOOL, CALL_WRITE_TOOL):
        response = await _dispatch(
          _call(tool, {"graph": f"{KG}_dev", "name": "create-subgraph"}), scope
        )
        assert "read-only on this connection" in _text(response)
      gate.tools_call.assert_not_awaited()
      # The other graph on the same connection still takes writes.
      await _dispatch(
        _call(CALL_WRITE_TOOL, {"graph": KG2, "name": "create-subgraph"}), scope
      )
      gate.tools_call.assert_awaited_once()

  async def test_a_read_only_connection_refuses_the_write_tool_outright(self):
    with _Gate() as gate:
      response = await _dispatch(
        _call(CALL_WRITE_TOOL, {"graph": KG, "name": "create-subgraph"}),
        ConnectionScope(read_only=True),
      )
    assert "read-only" in _text(response)
    assert gate.events == [("charge", None)]
    gate.tools_call.assert_not_awaited()

  async def test_a_graph_tool_called_by_its_own_name_is_refused(self):
    with _Gate() as gate:
      response = await _dispatch(_call("create-subgraph", {"graph": KG}))
    assert "Unknown tool" in _text(response)
    assert gate.events == [("charge", None)]
    gate.tools_call.assert_not_awaited()


def _graph(graph_id, **fields):
  defaults = {
    "graphId": graph_id,
    "graphName": graph_id,
    "description": "",
    "graphType": "entity",
    "schemaExtensions": [],
    "repositoryType": None,
    "parentGraphId": None,
    "role": "admin",
    "status": "active",
  }
  return SimpleNamespace(**{**defaults, **fields})


@pytest.mark.asyncio
class TestListGraphs:
  LISTED = [
    _graph(KG, graphName="Books", schemaExtensions=["roboledger"]),
    _graph(f"{KG}_dev", parentGraphId=KG),
    _graph(KG2),
    _graph("sec", repositoryType="sec", role="read"),
  ]

  async def _list(self, scope=EVERY_GRAPH):
    with (
      _Gate() as gate,
      patch(
        "robosystems.operations.graph.listing.list_user_graphs",
        return_value=SimpleNamespace(graphs=self.LISTED),
      ),
      patch("robosystems.database.SessionFactory", Mock()),
    ):
      response = await _dispatch(_call(LIST_GRAPHS), scope)
    assert gate.events == [("charge", None)]
    return _payload(response)

  async def test_every_graph_the_user_reaches(self):
    payload = await self._list()
    assert [row["graph"] for row in payload["graphs"]] == [
      KG,
      f"{KG}_dev",
      KG2,
      "sec",
    ]
    books = payload["graphs"][0]
    assert books["name"] == "Books"
    assert books["extensions"] == ["roboledger"]
    assert books["subgraphs"] == 1
    assert payload["graphs"][1]["parent"] == KG
    assert payload["graphs"][3]["repository"] == "sec"

  async def test_a_configured_set_narrows_and_names_the_listing(self):
    scope = parse_connection_scope(f"books={KG},sec:ro,{'kg' + 'b' * 16}", None)
    payload = await self._list(scope)
    rows = {row["graph"]: row for row in payload["graphs"]}
    assert set(rows) == {KG, f"{KG}_dev", "sec", "kg" + "b" * 16}
    assert rows[KG]["alias"] == "books"
    assert rows["sec"]["read_only"] is True
    assert "note" in rows["kg" + "b" * 16]


@pytest.mark.asyncio
class TestRouteCredential:
  def _request(self):
    request = Mock()
    request.client.host = "203.0.113.9"
    request.headers = {"user-agent": "test"}
    request.url.path = "/v1/mcp/graphs"
    request.state = SimpleNamespace()
    return request

  async def test_an_account_wide_key_is_accepted(self):
    from robosystems.middleware.auth import dependencies

    user = _user()
    with (
      patch.object(dependencies, "validate_api_key", return_value=user),
      patch.object(dependencies, "_stash_api_key_identity") as stash,
    ):
      result = await dependencies.get_account_api_key_user(self._request(), KEY)
    assert result is user
    stash.assert_called_once()

  async def test_no_key_is_a_401(self):
    from robosystems.middleware.auth import dependencies

    with pytest.raises(HTTPException) as exc:
      await dependencies.get_account_api_key_user(self._request(), None)
    assert exc.value.status_code == 401

  async def test_a_graph_scoped_key_is_pointed_at_its_own_url(self):
    from robosystems.middleware.auth import dependencies

    scoped = "rfsc" + "a" * 64
    with (
      patch.object(dependencies, "validate_api_key", return_value=None),
      pytest.raises(HTTPException) as exc,
    ):
      await dependencies.get_account_api_key_user(self._request(), scoped)
    assert exc.value.status_code == 401
    assert "/v1/graphs/{graph_id}/mcp" in exc.value.detail

  async def test_key_access_to_a_graph_is_published_only_when_granted(self):
    from robosystems.middleware.auth import dependencies

    request = self._request()
    with (
      patch.object(dependencies, "validate_api_key_with_graph", return_value=None),
      pytest.raises(HTTPException) as exc,
    ):
      await dependencies.authorize_api_key_on_graph(request, KEY, KG)
    assert exc.value.status_code == 403
    assert not hasattr(request.state, "auth_graph_id")

    user = _user()
    with (
      patch.object(dependencies, "validate_api_key_with_graph", return_value=user),
      patch.object(dependencies, "_stash_api_key_identity"),
    ):
      assert await dependencies.authorize_api_key_on_graph(request, KEY, KG) is user
    assert request.state.auth_graph_id == KG


def test_the_route_is_mounted_key_only_and_out_of_the_openapi_schema():
  from fastapi import FastAPI
  from starlette.testclient import TestClient

  from robosystems.routers import mcp_agnostic_router_v1

  app = FastAPI()
  app.include_router(mcp_agnostic_router_v1)
  client = TestClient(app)
  ping = {"jsonrpc": "2.0", "id": 1, "method": "ping"}

  assert client.post("/v1/mcp/graphs", json=ping).status_code == 401
  # A bearer token is not this route's credential.
  bearer = {"Authorization": "Bearer rfso" + "a" * 40}
  assert client.post("/v1/mcp/graphs", json=ping, headers=bearer).status_code == 401
  assert "/v1/mcp/graphs" not in app.openapi()["paths"]
