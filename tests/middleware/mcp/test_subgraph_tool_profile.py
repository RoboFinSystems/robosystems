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
