"""A route's cut of the tool list. The RoboLedger route names the event kinds
its event-block tools run, and refuses any other; the universal route serves
the tools as they are."""

from __future__ import annotations

import copy
from unittest.mock import patch

import pytest

from robosystems.middleware.mcp.tools.route_profile import (
  BUILT_IN_EVENT_TYPES,
  ROBOLEDGER_ROUTE,
  UNIVERSAL_ROUTE,
)
from robosystems.operations.event_block.python_handlers import (
  EVENT_BLOCK_PYTHON_REGISTRY,
)

_STORED = "robosystems.middleware.mcp.tools.route_profile._stored_event_type"
KG = "kg1a2b3c4d5e6f7a8b9c"


def _event_tools() -> list[dict]:
  return [
    {
      "name": name,
      "description": f"{name} description",
      "inputSchema": {
        "type": "object",
        "properties": {
          "event_type": {"type": "string", "description": "Open vocabulary"},
          "event_category": {"type": "string"},
        },
        "required": ["event_type", "event_category"],
      },
    }
    for name in ("create-event-block", "preview-event-block")
  ] + [
    {
      "name": "update-event-block",
      "description": "update description",
      "inputSchema": {
        "type": "object",
        "properties": {
          "event_id": {"type": "string"},
          "transition_to": {"type": "string"},
        },
        "required": ["event_id"],
      },
    },
    {"name": "query-graphql", "description": "q", "inputSchema": {"type": "object"}},
  ]


@pytest.mark.unit
def test_every_platform_handler_kind_is_described():
  """The enum is the handler registry: a new platform handler cannot ship
  without the line that tells a model what it does."""
  assert set(BUILT_IN_EVENT_TYPES) == set(EVENT_BLOCK_PYTHON_REGISTRY)
  assert all(meaning.strip() for meaning in BUILT_IN_EVENT_TYPES.values())


@pytest.mark.unit
class TestTheRouteSchema:
  def test_event_type_is_the_enum_of_built_in_kinds(self):
    served = {t["name"]: t for t in ROBOLEDGER_ROUTE.serve(_event_tools())}
    for name in ("create-event-block", "preview-event-block"):
      event_type = served[name]["inputSchema"]["properties"]["event_type"]
      assert event_type["enum"] == list(BUILT_IN_EVENT_TYPES)
      for kind, meaning in BUILT_IN_EVENT_TYPES.items():
        assert f"- `{kind}`: {meaning}" in event_type["description"]
      assert served[name]["inputSchema"]["required"] == [
        "event_type",
        "event_category",
      ]

  def test_update_says_which_events_it_takes(self):
    served = {t["name"]: t for t in ROBOLEDGER_ROUTE.serve(_event_tools())}
    assert "built-in kind" in served["update-event-block"]["description"]
    assert "event_type" not in served["update-event-block"]["inputSchema"]["properties"]

  def test_the_route_withholds_its_exclusions(self):
    names = {t["name"] for t in ROBOLEDGER_ROUTE.serve(_event_tools())}
    assert "query-graphql" not in names

  def test_the_caller_s_definitions_are_left_as_they_were(self):
    tools = _event_tools()
    before = copy.deepcopy(tools)
    ROBOLEDGER_ROUTE.serve(tools)
    assert tools == before

  def test_the_universal_route_serves_the_tools_as_they_are(self):
    assert UNIVERSAL_ROUTE.serve(_event_tools()) == _event_tools()


@pytest.mark.unit
class TestTheRouteRefuses:
  @pytest.mark.parametrize("tool", ["create-event-block", "preview-event-block"])
  @pytest.mark.parametrize("event_type", ["consignment_shipped", "", None, 7])
  def test_an_event_type_outside_the_enum(self, tool, event_type):
    assert ROBOLEDGER_ROUTE.screens(tool)
    refusal = ROBOLEDGER_ROUTE.refusal(KG, tool, {"event_type": event_type})
    assert refusal is not None
    assert "not available on this connection" in refusal

  @pytest.mark.parametrize("kind", sorted(BUILT_IN_EVENT_TYPES))
  def test_no_built_in_kind(self, kind):
    assert (
      ROBOLEDGER_ROUTE.refusal(KG, "create-event-block", {"event_type": kind}) is None
    )

  def test_an_update_to_an_event_of_another_kind(self):
    with patch(_STORED, return_value="consignment_shipped") as stored:
      refusal = ROBOLEDGER_ROUTE.refusal(
        KG, "update-event-block", {"event_id": "evt_1", "transition_to": "committed"}
      )
    stored.assert_called_once_with(KG, "evt_1")
    assert refusal is not None and "'consignment_shipped'" in refusal

  @pytest.mark.parametrize(
    "kind", ["bank_transaction", "bank_fee", "external_transfer", "internal_transfer"]
  )
  def test_no_update_to_a_bank_feed_line(self, kind):
    with patch(_STORED, return_value=kind):
      assert (
        ROBOLEDGER_ROUTE.refusal(
          KG, "update-event-block", {"event_id": "evt_1", "transition_to": "committed"}
        )
        is None
      )

  def test_an_unknown_event_is_left_to_the_operation(self):
    with patch(_STORED, return_value=None):
      assert (
        ROBOLEDGER_ROUTE.refusal(KG, "update-event-block", {"event_id": "evt_x"})
        is None
      )

  def test_an_unreadable_kind_is_refused(self):
    with patch(_STORED, side_effect=RuntimeError("database down")):
      refusal = ROBOLEDGER_ROUTE.refusal(KG, "update-event-block", {"event_id": "e"})
    assert refusal == "The event's kind could not be checked; try again."

  def test_the_universal_route_screens_nothing(self):
    for tool in ("create-event-block", "preview-event-block", "update-event-block"):
      assert not UNIVERSAL_ROUTE.screens(tool)
      assert UNIVERSAL_ROUTE.refusal(KG, tool, {"event_type": "anything"}) is None
    assert not ROBOLEDGER_ROUTE.screens("list-event-blocks")
