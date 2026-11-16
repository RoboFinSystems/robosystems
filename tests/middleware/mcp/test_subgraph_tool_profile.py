"""The tool surface a tenant subgraph advertises.

A subgraph inherits its parent's ``schema_extensions``, so without an explicit
cut every roboledger tool registers on it — and roughly seventy percent of them
cannot run. The OLTP ones fail on a schema-name validator (`{parent}_{name}`
can't match ``^kg[0-9a-f]{16,}$``); the platform-lifecycle ones are worse
because they *succeed*, answering confidently about connections and
materialization the subgraph does not have.
"""

from unittest.mock import MagicMock

import pytest

from robosystems.middleware.mcp.tools.manager import (
  SUBGRAPH_TOOL_PROFILE,
  GraphMCPTools,
)

PARENT = "kg19ed34f81c37ba3f31fa"
SUBGRAPH = f"{PARENT}_entities"


def _tools_for(graph_id: str) -> GraphMCPTools:
  client = MagicMock()
  client.graph_id = graph_id
  return GraphMCPTools(client, schema_extensions=("roboledger",))


@pytest.fixture
def all_flags_on(monkeypatch):
  """Register every optional family, so the cut is measured at full surface.

  Without this the parent registers a partial surface and the filter looks
  more effective than it is.
  """
  from robosystems.config import env

  for flag in (
    "MCP_WORKSPACE_ENABLED",
    "MCP_SUBGRAPH_OPS_ENABLED",
    "MCP_GRAPHQL_ENABLED",
    "EXTENSIONS_GRAPHQL_ENABLED",
    "ROBOLEDGER_ENABLED",
    "FACT_GRID_ENABLED",
    "SEMANTIC_MEMORY_ENABLED",
    "MCP_SEMANTIC_MEMORY_ENABLED",
    "SEMANTIC_SEARCH_ENABLED",
  ):
    monkeypatch.setattr(env, flag, True, raising=False)


@pytest.mark.unit
class TestSubgraphToolProfile:
  def test_parent_advertises_far_more_than_the_subgraph(self, all_flags_on):
    parent_names = {
      t["name"] for t in _tools_for(PARENT).get_tool_definitions_as_dict()
    }
    subgraph_names = {
      t["name"] for t in _tools_for(SUBGRAPH).get_tool_definitions_as_dict()
    }

    # The magnitude is the finding, not just the direction: a subgraph used
    # to advertise the parent's whole surface with most of it broken.
    assert len(parent_names) > 60
    assert len(subgraph_names) < 20
    assert subgraph_names <= SUBGRAPH_TOOL_PROFILE

  def test_no_profile_entry_is_a_typo(self, all_flags_on):
    """Every profile name must be a real tool, or the allowlist silently lies.

    A misspelled entry withholds a tool it was meant to keep, and nothing
    fails — the filter just quietly drops it. `get-graph-info` is the one
    deliberate exception: it registers outside this manager and is listed
    only so the call_tool guard admits it.
    """
    real = {t["name"] for t in _tools_for(PARENT).get_tool_definitions_as_dict()}
    unmatched = SUBGRAPH_TOOL_PROFILE - real
    assert unmatched == {"get-graph-info"}, (
      f"profile names matching no real tool: {sorted(unmatched)}"
    )

  def test_the_retired_navigation_tools_are_gone(self, all_flags_on):
    """Neither `switch-workspace` nor `resolve-subgraph` may resurface.

    The first advertised a switch the URL-anchored transport never performed;
    the second only formatted a URL from an id `list-subgraphs` already
    returns. `list-subgraphs` now carries `connector_url` per row, which is
    the whole job in the place you were already looking.
    """
    names = {t["name"] for t in _tools_for(PARENT).get_tool_definitions_as_dict()}
    assert "list-subgraphs" in names
    assert "switch-workspace" not in names
    assert "resolve-subgraph" not in names

  def test_the_surface_that_makes_a_subgraph_useful_survives(self, all_flags_on):
    names = {t["name"] for t in _tools_for(SUBGRAPH).get_tool_definitions_as_dict()}

    # A subgraph is a live, writable graph database — schema plus Cypher —
    # and it's the only such surface, since raw writes are blocked on the
    # parent. Losing any of these would make the cut pointless.
    for essential in (
      "read-graph-cypher",
      "get-graph-schema",
      "add-node-table",
      "add-relationship-table",
    ):
      assert essential in names, f"{essential} must survive the subgraph cut"

  def test_oltp_and_lifecycle_tools_are_withheld(self, all_flags_on):
    names = {t["name"] for t in _tools_for(SUBGRAPH).get_tool_definitions_as_dict()}

    for withheld in (
      "get-fiscal-calendar",  # OLTP — fails on the schema validator
      "list-information-blocks",  # OLTP — same
      "get-graph-sync-status",  # platform — answers about a relationship it lacks
      "materialize",  # platform — nothing materializes into a subgraph
      "sync-connection",  # platform — a subgraph has no connections
    ):
      assert withheld not in names, f"{withheld} must not be offered on a subgraph"

  def test_parent_keeps_everything(self, all_flags_on):
    """The cut must be scoped to subgraphs, not leak onto the parent."""
    names = {t["name"] for t in _tools_for(PARENT).get_tool_definitions_as_dict()}
    assert "get-fiscal-calendar" in names
    assert "materialize" in names


@pytest.mark.unit
class TestNotApplicableOnSubgraph:
  @pytest.mark.asyncio
  async def test_a_withheld_tool_answers_instead_of_dead_ending(self):
    """A stale client session gets a typed answer, not a validator string.

    A client that listed tools before switching — or that ignored
    `tools/list_changed` — will call tools this graph no longer advertises.
    Letting that reach `extensions_session` surfaces "Invalid graph_id for
    schema name: kg…_entities", which describes an internal invariant the
    caller cannot act on.
    """
    result = await _tools_for(SUBGRAPH).call_tool(
      "get-fiscal-calendar", {}, return_raw=True
    )

    assert result["error"] == "not_applicable_on_subgraph"
    assert "subgraph" in result["message"].lower()
    # The answer has to carry the way forward, not just the refusal.
    assert "parent" in result["message"].lower()
    assert "read-graph-cypher" in result["available_tools"]

  @pytest.mark.asyncio
  async def test_the_guard_does_not_fire_on_the_parent(self):
    """Same call on the parent must reach the real tool, not the guard."""
    tools = _tools_for(PARENT)
    result = await tools.call_tool("get-fiscal-calendar", {}, return_raw=True)
    assert (
      not isinstance(result, dict)
      or result.get("error") != "not_applicable_on_subgraph"
    )


@pytest.mark.unit
class TestRoboLedgerRouteToolProfile:
  def test_no_exclusion_is_a_typo(self, all_flags_on):
    """A misspelled exclusion lists a tool it was meant to withhold, and
    nothing fails. Every name must be a real tool on a RoboLedger graph."""
    from robosystems.middleware.mcp.tools.manager import (
      ROBOLEDGER_ROUTE_TOOL_EXCLUSIONS,
    )

    real = {t["name"] for t in _tools_for(PARENT).get_tool_definitions_as_dict()}
    unmatched = ROBOLEDGER_ROUTE_TOOL_EXCLUSIONS - real
    assert unmatched == set(), f"exclusions matching no real tool: {sorted(unmatched)}"

  def test_the_report_bundle_link_stays_off_the_chat_route(self, all_flags_on):
    from robosystems.middleware.mcp.tools.manager import (
      ROBOLEDGER_ROUTE_TOOL_EXCLUSIONS,
    )

    real = {t["name"] for t in _tools_for(PARENT).get_tool_definitions_as_dict()}
    assert "get-report-bundle" in real
    assert "get-report-bundle" in ROBOLEDGER_ROUTE_TOOL_EXCLUSIONS

  def test_the_accounting_surface_survives_the_cut(self, all_flags_on):
    from robosystems.middleware.mcp.tools.manager import (
      ROBOLEDGER_ROUTE_TOOL_EXCLUSIONS,
    )

    real = {t["name"] for t in _tools_for(PARENT).get_tool_definitions_as_dict()}
    kept = real - ROBOLEDGER_ROUTE_TOOL_EXCLUSIONS
    for tool in (
      "get-close-playbook",
      "get-fiscal-calendar",
      "close-period",
      "promote-obligations",
      "list-period-drafts",
      "create-event-block",
      "live-financial-statement",
      "read-graph-cypher",
    ):
      assert tool in kept, tool

  # Withheld because a directory review needs every operation a model can
  # call to be its own reviewed tool.
  _NEWLY_WITHHELD = (
    "query-graphql",
    "get-graphql-schema",
    "create-event-handler",
    "update-event-handler",
    "execute-event-block",
  )
  # The reads tool text routed through `query-graphql`, each its own tool.
  _TYPED_READS = (
    "get-account-register",
    "list-flow-candidates",
    "get-classification-stats",
    "list-discharge-candidates",
    "list-bank-accounts",
    "list-reports",
    "list-element-arcs",
  )

  @staticmethod
  def _route(all_tools):
    from robosystems.middleware.mcp.tools.route_profile import ROBOLEDGER_ROUTE

    return ROBOLEDGER_ROUTE.serve(all_tools)

  def _withheld(self) -> set[str]:
    """Every tool the graph serves elsewhere that the route does not."""
    from robosystems.middleware.mcp.tools.manager import (
      ROBOLEDGER_ROUTE_TOOL_EXCLUSIONS,
    )

    every = _tools_for(PARENT).get_tool_definitions_as_dict()
    route = {t["name"] for t in self._route(every)}
    return {t["name"] for t in every} - route | ROBOLEDGER_ROUTE_TOOL_EXCLUSIONS

  @staticmethod
  def _naming(text: str, names: set[str]) -> list[str]:
    import re

    return sorted(
      name
      for name in names
      if re.search(rf"(?<![\w-]){re.escape(name)}(?![\w-])", text)
    )

  def test_generic_and_rule_tools_are_withheld_only_on_the_route(self, all_flags_on):
    real = {t["name"] for t in _tools_for(PARENT).get_tool_definitions_as_dict()}
    route = {
      t["name"] for t in self._route(_tools_for(PARENT).get_tool_definitions_as_dict())
    }
    for name in self._NEWLY_WITHHELD:
      assert name in real, f"{name} must stay on the universal and per-graph routes"
      assert name not in route, f"{name} must not be offered on the route"

  def test_the_named_reads_are_typed_tools_on_both(self, all_flags_on):
    real = {t["name"] for t in _tools_for(PARENT).get_tool_definitions_as_dict()}
    route = {
      t["name"] for t in self._route(_tools_for(PARENT).get_tool_definitions_as_dict())
    }
    for name in self._TYPED_READS:
      assert name in real, name
      assert name in route, name

  def test_no_route_tool_text_points_at_a_withheld_tool(self, all_flags_on):
    """A description or schema naming a tool the route lacks sends the model
    after an operation it cannot call. Every withheld tool, not only the
    latest: a stale reference outlives the exclusion that made it one."""
    import json

    withheld = self._withheld()
    assert self._NEWLY_WITHHELD and set(self._NEWLY_WITHHELD) <= withheld
    for tool in self._route(_tools_for(PARENT).get_tool_definitions_as_dict()):
      text = tool["description"] + json.dumps(tool.get("inputSchema", {}))
      named = self._naming(text, withheld)
      assert not named, f"{tool['name']} names {named}"

  def test_route_instructions_name_no_withheld_tool(self, all_flags_on):
    from robosystems.middleware.mcp.tools.instructions import build_instructions

    names = {
      t["name"] for t in self._route(_tools_for(PARENT).get_tool_definitions_as_dict())
    } | {"get-graph-info"}
    group = [
      {"id": "ent_p", "name": "Parent", "is_group_parent": True},
      {"id": "ent_s", "name": "Sub", "consolidation_method": "equity"},
      {"id": "ent_c", "name": "Group", "entity_role": "consolidation"},
    ]
    out = build_instructions(
      graph_id=PARENT,
      tool_names=names,
      is_shared_repo=False,
      read_only=False,
      entities=group,
    )
    assert out is not None
    assert self._naming(out, self._withheld()) == []
    assert "graphql" not in out.lower()
    for read in (
      "get-account-register",
      "list-flow-candidates",
      "get-classification-stats",
      "list-discharge-candidates",
    ):
      assert f"`{read}(" in out, read

  @pytest.mark.parametrize("mode", ["overview", "initiate", "recurring"])
  def test_the_close_playbook_names_no_withheld_tool(self, all_flags_on, mode):
    import json

    from robosystems.middleware.mcp.tools.playbook_tools import _build_playbook

    text = json.dumps(_build_playbook(mode))
    assert self._naming(text, self._withheld()) == []

  @pytest.mark.asyncio
  async def test_a_typed_read_dispatches_by_name(self, all_flags_on):
    from unittest.mock import patch

    from robosystems.models.api.extensions.taxonomies import FlowCandidate

    flow = FlowCandidate(
      element_id="el_debt",
      qname="rs-gaap:ProceedsFromIssuanceOfLongTermDebt",
      name="Proceeds from long-term debt",
      activity="financing",
    )
    session_cm = MagicMock()
    session_cm.__enter__ = MagicMock(return_value=MagicMock())
    session_cm.__exit__ = MagicMock(return_value=False)
    with (
      patch(
        "robosystems.middleware.mcp.tools.ledger_read_tools.extensions_session",
        return_value=session_cm,
      ),
      patch(
        "robosystems.operations.roboledger.reads.taxonomies.list_entity_flow_candidates",
        return_value=[flow],
      ),
    ):
      result = await _tools_for(PARENT).call_tool(
        "list-flow-candidates", {}, return_raw=True
      )
    assert result["flows"][0]["qname"] == flow.qname

  def test_the_real_event_tools_carry_the_enum_on_the_route_only(self, all_flags_on):
    from robosystems.middleware.mcp.tools.route_profile import BUILT_IN_EVENT_TYPES

    every = _tools_for(PARENT).get_tool_definitions_as_dict()
    universal = {t["name"]: t for t in every}
    route = {t["name"]: t for t in self._route(every)}
    for name in ("create-event-block", "preview-event-block"):
      held = route[name]["inputSchema"]["properties"]["event_type"]
      assert held["enum"] == list(BUILT_IN_EVENT_TYPES)
      assert "enum" not in universal[name]["inputSchema"]["properties"]["event_type"]
    # The inbox writes still take what they took.
    update = route["update-event-block"]["inputSchema"]["properties"]
    assert {"event_id", "transition_to", "metadata_patch"} <= set(update)
