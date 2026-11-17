"""A route's cut of the tool list: the RoboLedger route withholds its
exclusions and serves every other tool exactly as the universal route does."""

from __future__ import annotations

import copy

import pytest

from robosystems.middleware.mcp.tools.manager import ROBOLEDGER_ROUTE_TOOL_EXCLUSIONS
from robosystems.middleware.mcp.tools.route_profile import (
  ROBOLEDGER_ROUTE,
  UNIVERSAL_ROUTE,
)


def _tools() -> list[dict]:
  names = (
    "create-event-block",
    "update-event-block",
    "create-event-handler",
    "execute-event-block",
    "query-graphql",
    "write-graph-cypher",
  )
  return [
    {
      "name": name,
      "description": f"{name} description",
      "inputSchema": {
        "type": "object",
        "properties": {"event_type": {"type": "string"}},
      },
    }
    for name in names
  ]


@pytest.mark.unit
class TestRouteProfile:
  def test_the_route_withholds_only_its_exclusions(self):
    served = {t["name"] for t in ROBOLEDGER_ROUTE.serve(_tools())}
    assert served == {t["name"] for t in _tools()} - ROBOLEDGER_ROUTE_TOOL_EXCLUSIONS
    assert "write-graph-cypher" not in served
    assert ROBOLEDGER_ROUTE.withholds("write-graph-cypher")

  @pytest.mark.parametrize(
    "name",
    [
      "create-event-block",
      "update-event-block",
      "create-event-handler",
      "execute-event-block",
      "query-graphql",
    ],
  )
  def test_the_event_block_design_is_served_whole(self, name):
    assert not ROBOLEDGER_ROUTE.withholds(name)

  def test_served_tools_are_the_definitions_unchanged(self):
    tools = _tools()
    before = copy.deepcopy(tools)
    served = ROBOLEDGER_ROUTE.serve(tools)
    assert tools == before
    assert served == [t for t in before if t["name"] != "write-graph-cypher"]

  def test_also_excluded_adds_to_the_route_s_own(self):
    served = ROBOLEDGER_ROUTE.serve(_tools(), frozenset({"query-graphql"}))
    assert "query-graphql" not in {t["name"] for t in served}

  def test_the_universal_route_serves_the_tools_as_they_are(self):
    assert UNIVERSAL_ROUTE.serve(_tools()) == _tools()
    assert not UNIVERSAL_ROUTE.withholds("write-graph-cypher")
