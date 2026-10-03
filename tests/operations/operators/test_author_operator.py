"""AuthorOperator — the analyst's reads plus a narrow write allowlist.

The allowlist is the safety boundary: a write tool the graph exposes but the
list omits is never advertised to the model, and the tool loop refuses any
name it did not advertise. Every successful write is reported, so the console
can show a receipt and refresh the page it changed.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from robosystems.config.operators import ModelProfile, OperatorConfig
from robosystems.operations.operators.ai_client import AIResponse
from robosystems.operations.operators.base import OperatorMode
from robosystems.operations.operators.implementations.analyst import AnalystOperator
from robosystems.operations.operators.implementations.author import AuthorOperator
from robosystems.operations.operators.operator_context import OperatorContext
from robosystems.operations.operators.operator_registry import get_operator
from robosystems.operations.operators.progress import NoOpProgress
from robosystems.operations.operators.tool_loop import (
  ToolLoopResult,
  WriteRefusedError,
  run_tool_loop,
  write_record,
)

pytestmark = pytest.mark.asyncio

# Writes a graph exposes that must never reach the author's model.
GATED_WRITES = [
  "delete-information-block",
  "delete-journal-entry",
  "update-journal-entry",
  "close-period",
  "reopen-period",
  "write-graph-cypher",
  "set-write-policy",
  "sync-connection",
  "delete-taxonomy-block",
  "create-event-handler",
  "update-taxonomy-block",
]


def _tools(available: list[str], call_results: dict | None = None) -> MagicMock:
  tools = MagicMock()
  tools.get_tool_schemas = AsyncMock(
    side_effect=lambda names: [
      {"name": n, "description": "d", "inputSchema": {"type": "object"}}
      for n in names
      if n in available
    ]
  )
  results = call_results or {}

  async def call_tool(name, arguments, return_raw=False):
    return results.get(name, {"error": "unavailable"})

  tools.call_tool = AsyncMock(side_effect=call_tool)
  return tools


def _ctx(
  tools: MagicMock, ai: MagicMock | None = None, extra: dict | None = None
) -> OperatorContext:
  return OperatorContext(
    graph_id="kg_test",
    user_id="u",
    query="Create a metric block for revenue per customer",
    mode=OperatorMode.STANDARD,
    history=[],
    extra=extra or {},
    ai=ai or MagicMock(),
    tools=tools,
    progress=NoOpProgress(),
  )


async def _loop_kwargs(operator, tools: MagicMock, extra: dict | None = None) -> dict:
  with patch(
    "robosystems.operations.operators.implementations.analyst.run_tool_loop",
    AsyncMock(return_value=ToolLoopResult(text="done", iterations=2)),
  ) as loop:
    await operator.run(_ctx(tools, extra=extra))
  return loop.await_args.kwargs


class TestDeclaration:
  def test_registered_by_name_and_never_auto_routed(self):
    operator = get_operator("author")
    assert isinstance(operator, AuthorOperator)
    assert operator.can_handle("create a metric block") == 0.0

  def test_allowlist_holds_no_destructive_or_gated_tool(self):
    assert set(AuthorOperator.WRITE_TOOLS).isdisjoint(GATED_WRITES)
    assert not any(t.startswith("delete-") for t in AuthorOperator.WRITE_TOOLS)

  def test_runs_on_the_quality_tier(self):
    assert OperatorConfig.OPERATOR_MODEL_OVERRIDES["author"] is ModelProfile.QUALITY
    assert (
      OperatorConfig.resolve_model(operator_type="author").model_id
      == OperatorConfig.resolve_model(ModelProfile.QUALITY).model_id
    )

  def test_analyst_writes_nothing(self):
    assert AnalystOperator.WRITE_TOOLS == ()

  def test_it_reads_what_the_analyst_reads_and_no_dry_run(self):
    # The previews take the locks of the writes they preview, so they stay
    # behind the write classification and off both operators.
    assert AuthorOperator.READ_ONLY_TOOLS == AnalystOperator.READ_ONLY_TOOLS
    assert not [t for t in AuthorOperator.READ_ONLY_TOOLS if t.startswith("preview-")]

  def test_ledger_bound_writes_are_the_ones_a_person_still_approves(self):
    # A classified line waits on its commit; a drafted entry on close-period.
    assert {"update-event-block", "promote-obligations", "create-report"} <= set(
      AuthorOperator.WRITE_TOOLS
    )
    assert "update-event-block" in AuthorOperator.WRITE_GUARDS


class TestRun:
  async def test_advertises_allowed_writes_and_never_gated_ones(self):
    tools = _tools(
      ["read-graph-cypher", "create-information-block", "assert-metrics", *GATED_WRITES]
    )
    kwargs = await _loop_kwargs(AuthorOperator(), tools)

    assert "create-information-block" in kwargs["tool_names"]
    assert "assert-metrics" in kwargs["tool_names"]
    assert not set(kwargs["tool_names"]) & set(GATED_WRITES)
    assert kwargs["write_tools"] == frozenset(
      {"create-information-block", "assert-metrics"}
    )

  async def test_step_cap_is_a_backstop_not_the_mode_budget(self):
    kwargs = await _loop_kwargs(AuthorOperator(), _tools(["read-graph-cypher"]))
    assert kwargs["max_iterations"] == 25
    assert kwargs["operator_type"] == "author"

  async def test_prompt_names_the_writes_and_the_rules(self):
    tools = _tools(["read-graph-cypher", "create-information-block"])
    kwargs = await _loop_kwargs(AuthorOperator(), tools)
    assert "AUTHORING" in kwargs["system"]
    assert "`create-information-block`" in kwargs["system"]
    assert "never invent" in kwargs["system"]

  async def test_prompt_states_the_inbox_rule_only_where_the_tool_is(self):
    with_inbox = await _loop_kwargs(
      AuthorOperator(), _tools(["read-graph-cypher", "update-event-block"])
    )
    assert "only classifies" in with_inbox["system"]
    without = await _loop_kwargs(
      AuthorOperator(), _tools(["read-graph-cypher", "create-agent"])
    )
    assert "only classifies" not in without["system"]

  async def test_prompt_says_so_when_the_graph_has_no_write_tools(self):
    kwargs = await _loop_kwargs(AuthorOperator(), _tools(["read-graph-cypher"]))
    assert "no write tools are available" in kwargs["system"]
    assert kwargs["write_tools"] == frozenset()

  async def test_analyst_is_unchanged_on_a_graph_with_write_tools(self):
    tools = _tools(["read-graph-cypher", "create-information-block"])
    kwargs = await _loop_kwargs(AnalystOperator(), tools)
    assert "create-information-block" not in kwargs["tool_names"]
    assert kwargs["write_tools"] == frozenset()
    assert "AUTHORING" not in kwargs["system"]
    assert kwargs["operator_type"] == "analyst"

  async def test_a_write_run_is_bounded_by_default(self):
    kwargs = await _loop_kwargs(AuthorOperator(), _tools(["read-graph-cypher"]))
    assert kwargs["max_credits"] == 750

  @pytest.mark.parametrize("requested", [50, 1200])
  async def test_the_request_ceiling_wins_either_way(self, requested):
    kwargs = await _loop_kwargs(
      AuthorOperator(), _tools(["read-graph-cypher"]), {"max_credits": requested}
    )
    assert kwargs["max_credits"] == requested

  async def test_a_non_positive_request_falls_back_to_the_default(self):
    kwargs = await _loop_kwargs(
      AuthorOperator(), _tools(["read-graph-cypher"]), {"max_credits": 0}
    )
    assert kwargs["max_credits"] == 750

  async def test_the_analyst_keeps_no_default_ceiling(self):
    kwargs = await _loop_kwargs(AnalystOperator(), _tools(["read-graph-cypher"]))
    assert kwargs["max_credits"] is None

  async def test_writes_reach_the_result_metadata(self):
    written = [{"operation": "create-information-block", "id": "blk_1", "name": "ARPC"}]
    with patch(
      "robosystems.operations.operators.implementations.analyst.run_tool_loop",
      AsyncMock(return_value=ToolLoopResult(text="done", writes=written)),
    ):
      result = await AuthorOperator().run(_ctx(_tools(["read-graph-cypher"])))
    assert result.metadata["writes"] == written


def _turn(*calls, text: str = "") -> AIResponse:
  blocks = [
    {"toolUse": {"toolUseId": f"t{i}", "name": name, "input": args}}
    for i, (name, args) in enumerate(calls)
  ]
  return AIResponse(
    content=text,
    model="m",
    input_tokens=1,
    output_tokens=1,
    stop_reason="tool_use" if calls else "end_turn",
    content_blocks=blocks or [{"text": text}],
  )


class TestLoopRecordsWrites:
  async def test_only_successful_writes_are_recorded(self):
    tools = _tools(
      ["create-information-block", "assert-metrics", "read-graph-cypher"],
      {
        "create-information-block": {
          "id": "blk_9",
          "name": "Revenue per customer",
          "block_type": "metric",
        },
        "assert-metrics": {"error": "invalid_arguments", "message": "bad"},
        "read-graph-cypher": [{"n": 1}],
      },
    )
    ai = MagicMock()
    ai.create_message = AsyncMock(
      side_effect=[
        _turn(
          ("create-information-block", {"block_type": "metric"}),
          ("assert-metrics", {"structure_id": "s1"}),
          ("read-graph-cypher", {"query": "MATCH (n) RETURN n LIMIT 1"}),
        ),
        _turn(text="Created it."),
      ]
    )
    result = await run_tool_loop(
      _ctx(tools, ai),
      system="s",
      tool_names=["create-information-block", "assert-metrics", "read-graph-cypher"],
      max_iterations=3,
      max_tokens=100,
      write_tools=frozenset({"create-information-block", "assert-metrics"}),
    )
    assert result.writes == [
      {
        "operation": "create-information-block",
        "id": "blk_9",
        "name": "Revenue per customer",
        "block_type": "metric",
      }
    ]

  async def test_writes_survive_a_run_stopped_by_the_step_cap(self):
    tools = _tools(
      ["create-information-block"], {"create-information-block": {"id": "b1"}}
    )
    ai = MagicMock()
    ai.create_message = AsyncMock(
      side_effect=[
        _turn(("create-information-block", {})),
        _turn(text="Partial."),
      ]
    )
    result = await run_tool_loop(
      _ctx(tools, ai),
      system="s",
      tool_names=["create-information-block"],
      max_iterations=1,
      max_tokens=100,
      write_tools=frozenset({"create-information-block"}),
    )
    assert result.hit_cap is True
    assert [w["id"] for w in result.writes] == ["b1"]


class TestWriteGuards:
  async def test_a_refused_call_is_never_dispatched_or_recorded(self):
    tools = _tools(["create-taxonomy-block"], {"create-taxonomy-block": {"id": "t1"}})
    ai = MagicMock()
    ai.create_message = AsyncMock(
      side_effect=[
        _turn(("create-taxonomy-block", {"taxonomy_type": "chart_of_accounts"})),
        _turn(text="I can't do that here."),
      ]
    )

    def refuse(arguments):
      raise WriteRefusedError("not from the console")

    result = await run_tool_loop(
      _ctx(tools, ai),
      system="s",
      tool_names=["create-taxonomy-block"],
      max_iterations=3,
      max_tokens=100,
      write_tools=frozenset({"create-taxonomy-block"}),
      write_guards={"create-taxonomy-block": refuse},
    )

    tools.call_tool.assert_not_awaited()
    assert result.writes == []
    # The model is told why, as the tool's own error.
    answered = ai.create_message.await_args_list[1].kwargs["messages"][-1].content
    block = answered[0]["toolResult"]
    assert block["status"] == "error"
    assert "not from the console" in block["content"][0]["text"]

  async def test_a_guard_can_pin_an_argument(self):
    tools = _tools(["update-agent"], {"update-agent": {"id": "agt_1"}})
    ai = MagicMock()
    ai.create_message = AsyncMock(
      side_effect=[_turn(("update-agent", {"agent_id": "agt_1"})), _turn(text="Done.")]
    )

    result = await run_tool_loop(
      _ctx(tools, ai),
      system="s",
      tool_names=["update-agent"],
      max_iterations=3,
      max_tokens=100,
      write_tools=frozenset({"update-agent"}),
      write_guards={"update-agent": lambda args: {**args, "pinned": True}},
    )

    tools.call_tool.assert_awaited_once_with(
      "update-agent", {"agent_id": "agt_1", "pinned": True}, return_raw=True
    )
    assert [w["id"] for w in result.writes] == ["agt_1"]

  async def test_the_author_hands_its_guards_to_the_loop(self):
    kwargs = await _loop_kwargs(AuthorOperator(), _tools(["read-graph-cypher"]))
    assert kwargs["write_guards"] is AuthorOperator.WRITE_GUARDS
    analyst = await _loop_kwargs(AnalystOperator(), _tools(["read-graph-cypher"]))
    assert analyst["write_guards"] == {}

  def test_every_guard_narrows_an_allowlisted_tool(self):
    assert set(AuthorOperator.WRITE_GUARDS) <= set(AuthorOperator.WRITE_TOOLS)

  def test_a_chart_of_accounts_is_never_created_from_the_console(self):
    # Creating one would demote the chart the books run on.
    guard = AuthorOperator.WRITE_GUARDS["create-taxonomy-block"]
    with pytest.raises(WriteRefusedError, match="primary chart"):
      guard({"name": "New chart", "taxonomy_type": "chart_of_accounts"})

  @pytest.mark.parametrize("taxonomy_type", ["custom_ontology", "reporting_extension"])
  def test_other_taxonomy_blocks_pass_unchanged(self, taxonomy_type):
    guard = AuthorOperator.WRITE_GUARDS["create-taxonomy-block"]
    arguments = {"name": "Metrics", "taxonomy_type": taxonomy_type}
    assert guard(arguments) == arguments


class TestClassifyGuard:
  guard = staticmethod(AuthorOperator.WRITE_GUARDS["update-event-block"])

  def test_a_classification_passes_and_is_stamped_as_the_ai(self):
    out = self.guard(
      {
        "event_id": "evt_1",
        "transition_to": "classified",
        "metadata_patch": {"classified_element_id": "el_1", "basis": "Stripe fee"},
      }
    )
    assert out["metadata_patch"] == {
      "classified_element_id": "el_1",
      "basis": "Stripe fee",
      "classified_by": "ai",
    }

  def test_it_cannot_claim_a_person_classified_the_line(self):
    out = self.guard(
      {
        "event_id": "evt_1",
        "transition_to": "classified",
        "metadata_patch": {"accept_suggestion": True, "classified_by": "user"},
      }
    )
    assert out["metadata_patch"]["classified_by"] == "ai"

  def test_a_split_passes(self):
    allocations = [
      {"element_id": "el_1", "amount": 600},
      {"element_id": "el_2", "amount": 400},
    ]
    out = self.guard(
      {
        "event_id": "evt_1",
        "transition_to": "classified",
        "metadata_patch": {"classified_allocations": allocations},
      }
    )
    assert out["metadata_patch"]["classified_allocations"] == allocations

  @pytest.mark.parametrize(
    "transition", ["committed", "voided", "superseded", "pending", "fulfilled", None]
  )
  def test_every_other_transition_is_refused(self, transition):
    arguments = {"event_id": "evt_1", "metadata_patch": {"accept_suggestion": True}}
    if transition is not None:
      arguments["transition_to"] = transition
    with pytest.raises(WriteRefusedError, match="only classifies"):
      self.guard(arguments)

  @pytest.mark.parametrize(
    "extra",
    [
      {"description": "edited"},
      {"effective_at": "2026-04-30T23:59:59Z"},
      {"superseded_by_id": "evt_2"},
    ],
  )
  def test_a_field_correction_riding_along_is_refused(self, extra):
    with pytest.raises(WriteRefusedError):
      self.guard({"event_id": "evt_1", "transition_to": "classified", **extra})

  def test_metadata_outside_the_classification_is_refused(self):
    with pytest.raises(WriteRefusedError):
      self.guard(
        {
          "event_id": "evt_1",
          "transition_to": "classified",
          "metadata_patch": {"classified_element_id": "el_1", "source_amount": 1},
        }
      )

  def test_the_caller_arguments_are_not_mutated(self):
    arguments = {
      "event_id": "evt_1",
      "transition_to": "classified",
      "metadata_patch": {"accept_suggestion": True},
    }
    self.guard(arguments)
    assert arguments["metadata_patch"] == {"accept_suggestion": True}


class TestWriteRecord:
  def test_id_and_name_come_from_the_result(self):
    assert write_record(
      "create-agent", {"name": "Acme"}, {"id": "agt_1", "name": "Acme Corp"}
    ) == {"operation": "create-agent", "id": "agt_1", "name": "Acme Corp"}

  def test_an_update_falls_back_to_the_target_in_its_arguments(self):
    record = write_record("assert-metrics", {"structure_id": "s_7"}, {"facts": 3})
    assert record["id"] == "s_7"

  def test_a_wrapped_object_is_read_one_level_down(self):
    result = {"success": True, "memory": {"id": "mem_1", "text": "x"}}
    assert write_record("remember", {"text": "x"}, result)["id"] == "mem_1"

  def test_a_result_with_no_id_records_none(self):
    assert write_record("remember", {"text": "x"}, "ok")["id"] is None
