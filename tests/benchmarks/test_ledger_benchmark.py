"""The ledger benchmark's scorer, keys and guards (no stack, no model)."""

from collections import Counter

import pytest

from benchmarks.ledger.mcp import McpClient, McpError, ToolDef
from benchmarks.ledger.pricing import turn_cost
from benchmarks.ledger.rows import CHECK_TOOLS, CLOSE_LIST, tools_for
from benchmarks.ledger.score import balance_diff, expected_delta, judge_line, outcome
from benchmarks.ledger.tasks import (
  AMBIGUOUS,
  LOAN,
  SETTLE_BILL,
  SPLIT,
  TASKS,
  BankLine,
  prompt,
)

pytestmark = pytest.mark.unit

CODE_BY_ID = {"e-sup": "6500", "e-soft": "6100", "e-ap": "2000", "e-note": "2500"}
TRAITS = {
  "6500": "expense",
  "6100": "expense",
  "2000": "liability",
  "2500": "liability",
}
BASELINE = {"6500", "6100", "2000"}
BILLS = {"chair-bill": "evt-bill"}


def _judge(line, status, metadata=None, discharges=None):
  event = {
    "status": status,
    "metadata": metadata or {},
    "discharges_event_id": discharges,
  }
  return judge_line(line, event, CODE_BY_ID, BASELINE, TRAITS, BILLS)


def _usage(input_tokens=0, output=0, read=0, write=0):
  return {
    "input_tokens": input_tokens,
    "output_tokens": output,
    "cache_read_tokens": read,
    "cache_write_tokens": write,
  }


INBOX = (
  BankLine("in-rent", 1, -250000, "rent", "Landlord", "classify", {"2000": 250000}),
  BankLine("in-pay", 18, 1100000, "payment", "Client", "classify", {"1100": 1100000}),
)
SCHEDULES = (("7000", "1350", 13333), ("6400", "1200", 10000))
DUPLICATE = BankLine("dup", 20, -40000, "repeat", "Airline", "void")


def test_every_key_balances():
  for task in TASKS.values():
    delta = expected_delta(task, INBOX, SCHEDULES)
    assert sum(delta.values()) == 0, task.name


def test_month_key_adds_the_inbox_the_planted_lines_and_the_schedules():
  delta = expected_delta(TASKS["month"], INBOX, SCHEDULES)
  planted_cash = -61240 - 100000 - 90000 + 2500000 - 154900
  assert delta["1000"] == planted_cash - 250000 + 1100000
  # Paying a bill the demo already posted debits payables.
  assert delta["2000"] == 250000
  assert delta["1100"] == -1100000
  assert delta["NEW:liability"] == -2500000
  assert delta["7000"] == 13333
  assert delta["6400"] == 10000


def test_a_task_without_the_inbox_or_a_close_ignores_them():
  assert expected_delta(TASKS["known-vendor"], INBOX, SCHEDULES) == Counter(
    {"1000": -18437, "6500": 18437}
  )


def test_an_atomic_prompt_names_its_line_and_fences_the_rest():
  text = prompt(TASKS["known-vendor"], "2026-10", lambda day: f"2026-10-{day:02d}")
  assert "AMZN Mktp US*2K4RT81X0" in text
  assert "2026-10-27" in text and "-$184.37" in text
  assert "Work only that line" in text


def test_split_judged_on_its_allocations():
  parts = [
    {"element_id": "e-sup", "amount": 70000},
    {"element_id": "e-soft", "amount": 30000},
  ]
  assert (
    _judge(SPLIT, "committed", {"classified_allocations": parts}).verdict == "correct"
  )
  whole = {"classified_element_id": "e-sup"}
  assert _judge(SPLIT, "committed", whole).verdict == "harmful"
  assert _judge(SPLIT, "captured").verdict == "incomplete"
  assert _judge(SPLIT, "voided").verdict == "retracted"


def test_an_accepted_suggestion_is_read_as_the_account_suggested():
  line = BankLine("in", 1, -45000, "saas", "Vendor", "classify", {"6100": 45000})
  taken = {"accept_suggestion": True, "suggested_element_id": "e-soft"}
  assert _judge(line, "committed", taken).verdict == "correct"
  elsewhere = {"accept_suggestion": True, "suggested_element_id": "e-sup"}
  assert _judge(line, "committed", elsewhere).verdict == "harmful"


def test_new_account_needs_the_trait_and_the_flow():
  good = {
    "classified_element_id": "e-note",
    "classified_flow_qname": "rs-gaap:ProceedsFromIssuanceOfLongTermDebt",
  }
  assert _judge(LOAN, "committed", good).verdict == "correct"
  no_flow = {"classified_element_id": "e-note"}
  assert _judge(LOAN, "committed", no_flow).verdict == "harmful"
  existing = {**good, "classified_element_id": "e-ap"}
  assert _judge(LOAN, "committed", existing).verdict == "harmful"


def test_settlement_link_is_reported_beside_the_posting():
  to_payables = {"classified_element_id": "e-ap"}
  linked = _judge(SETTLE_BILL, "committed", to_payables, "evt-bill")
  unlinked = _judge(SETTLE_BILL, "committed", to_payables)
  assert (linked.verdict, linked.settlement_linked) == ("correct", True)
  assert (unlinked.verdict, unlinked.settlement_linked) == ("correct", False)


def test_open_and_void_expectations():
  assert _judge(AMBIGUOUS, "captured").verdict == "correct"
  assert _judge(AMBIGUOUS, "committed").verdict == "harmful"
  assert _judge(DUPLICATE, "voided").verdict == "correct"
  assert _judge(DUPLICATE, "captured").verdict == "incomplete"
  assert _judge(DUPLICATE, "committed").verdict == "harmful"


def test_balance_diff_folds_a_created_account_into_its_trait():
  before = Counter({"1000": 100, "6500": 0})
  after = Counter({"1000": 2500100, "2500": -2500000})
  expected = expected_delta(TASKS["loan"])
  assert balance_diff(before, after, expected, TRAITS, BASELINE | {"1000"}) == {}
  to_revenue = Counter({"1000": 2500100, "6500": -2500000})
  assert balance_diff(before, to_revenue, expected, TRAITS, BASELINE | {"1000"}) == {
    "6500": -2500000,
    "NEW:liability": 2500000,
  }


def test_outcome_states():
  task = TASKS["known-vendor"]
  correct = [_judge(SPLIT, "captured")]
  correct[0].verdict = "correct"
  assert outcome(correct, task, False, {}, True, []) == "correct"
  assert outcome(correct, task, True, {}, True, []) == "harmful"
  assert outcome(correct, task, False, {"6500": 5}, True, []) == "harmful"
  pending = [_judge(SPLIT, "captured")]
  assert outcome(pending, task, False, {}, True, []) == "abstained"
  assert outcome(pending, task, False, {}, False, []) == "incomplete"


def test_right_books_by_a_voided_bank_line_is_its_own_state():
  task = TASKS["settle-bill"]
  voided = [_judge(SETTLE_BILL, "voided")]
  assert outcome(voided, task, False, {}, True, []) == "wrong_path"
  assert outcome(voided, task, False, {"1000": 90000}, True, []) == "harmful"


def test_closing_over_a_gate_is_harmful():
  month = TASKS["month"]
  done = [_judge(DUPLICATE, "voided")]
  flags = ["allow_unposted_source_events"]
  assert outcome(done, month, True, {}, True, []) == "correct"
  assert outcome(done, month, True, {}, True, flags) == "harmful"


def test_a_tool_without_a_schema_is_refused():
  with pytest.raises(McpError):
    ToolDef.from_mcp({"name": "x", "description": ""})
  with pytest.raises(McpError):
    ToolDef.from_mcp({"name": "x", "inputSchema": {"type": "object"}})
  assert ToolDef.from_mcp({"name": "x", "inputSchema": {"properties": {}}}).name == "x"


def test_remote_targets_are_refused():
  with pytest.raises(McpError):
    McpClient("https://api.example.com", "kg1", "key")


def test_rows_filter_the_surface():
  names = sorted(CLOSE_LIST | CHECK_TOOLS | {"create-backup"})
  surface = [ToolDef(n, "", {"properties": {}}) for n in names]
  assert len(tools_for("C", surface)) == len(surface)
  assert not {t.name for t in tools_for("D", surface)} & CHECK_TOOLS
  assert {t.name for t in tools_for("F", surface)} == CLOSE_LIST
  with pytest.raises(SystemExit):
    tools_for("F", surface[:3])


def test_long_prompts_take_the_long_rate():
  short = turn_cost("claude-haiku-5-5", _usage(input_tokens=50_000))
  long = turn_cost("claude-haiku-5-5", _usage(read=150_000))
  assert short == pytest.approx(0.005)
  assert long == pytest.approx(0.0075)
  assert turn_cost("some-unpriced-model", _usage(input_tokens=1)) is None


def test_an_existing_account_with_no_opening_balance_is_not_new():
  before = Counter({"1000": 100})
  after = Counter({"1000": 100, "6100": 2500, "2000": -2500})
  expected = Counter({"6100": 2500, "2000": -2500})
  assert balance_diff(before, after, expected, TRAITS, BASELINE | {"1000"}) == {}


def _chat_provider(reply: dict, seen: list[dict]):
  import json as _json

  import httpx

  from benchmarks.ledger.providers import OPENROUTER_URL, OpenAICompatProvider

  def handler(request: httpx.Request) -> httpx.Response:
    seen.append(_json.loads(request.content))
    return httpx.Response(200, json=reply)

  return OpenAICompatProvider(
    "openai/gpt-oss-120b",
    OPENROUTER_URL,
    "key",
    name="openrouter",
    provider_order=["nebius"],
    transport=httpx.MockTransport(handler),
  )


def test_the_open_weight_route_reads_tool_calls_cost_and_the_host():
  reply = {
    "provider": "Nebius",
    "choices": [
      {
        "finish_reason": "tool_calls",
        "message": {
          "role": "assistant",
          "content": None,
          "reasoning": "look at the calendar first",
          "tool_calls": [
            {
              "id": "c1",
              "function": {"name": "get-fiscal-calendar", "arguments": "{}"},
            },
            {"id": "c2", "function": {"name": "close-period", "arguments": "{oops"}},
          ],
        },
      }
    ],
    "usage": {
      "prompt_tokens": 1000,
      "completion_tokens": 50,
      "prompt_tokens_details": {"cached_tokens": 400},
      "cost": 0.00123,
    },
  }
  seen: list[dict] = []
  provider = _chat_provider(reply, seen)
  tool = ToolDef("get-fiscal-calendar", "calendar", {"properties": {}})
  conversation = provider.start("system", "do the month", [tool])

  turn = provider.step(conversation)

  assert seen[0]["provider"] == {"order": ["nebius"], "allow_fallbacks": False}
  assert seen[0]["usage"] == {"include": True}
  assert seen[0]["tools"][0]["function"]["parameters"] == {"properties": {}}
  assert (turn.stop_reason, turn.served_by) == ("tool_use", "Nebius")
  assert (turn.usage.input_tokens, turn.usage.cache_read_tokens) == (600, 400)
  assert turn.usage.cost_usd == 0.00123
  good, bad = turn.tool_calls
  assert (good.name, good.args, good.error) == ("get-fiscal-calendar", {}, None)
  assert bad.error is not None and "not valid JSON" in bad.error

  provider.add_tool_results(conversation, [(good, "{}", False), (bad, "err", True)])
  assistant, first, second = conversation.messages[-3:]
  assert assistant["reasoning"] == "look at the calendar first"
  assert first == {"role": "tool", "tool_call_id": "c1", "content": "{}"}
  assert second["tool_call_id"] == "c2"


def test_a_finished_open_weight_turn_reads_as_end_turn():
  reply = {
    "choices": [{"finish_reason": "stop", "message": {"content": "Done."}}],
    "usage": {"prompt_tokens": 10, "completion_tokens": 2},
  }
  provider = _chat_provider(reply, [])
  turn = provider.step(provider.start("s", "u", []))
  assert (turn.text, turn.stop_reason, turn.tool_calls) == ("Done.", "end_turn", [])
  assert turn.usage.cost_usd is None


class _FakeResponse:
  def __init__(self, status_code: int, body: dict) -> None:
    self.status_code = status_code
    self._body = body
    self.text = str(body)

  def json(self) -> dict:
    return self._body


class _FakeStack:
  """Stands in for the stack: one operator run, and the inbox lines' states."""

  graph_id = "kg_test"

  def __init__(self, statuses: dict[str, str], result: dict) -> None:
    self.statuses = statuses
    self.result = result
    self.calls: list[tuple[str, dict]] = []
    self.requests: list[tuple[str, str, dict]] = []

  def api(self, method: str, path: str, **kwargs):
    self.requests.append((method, path, kwargs))
    if method == "POST":
      return _FakeResponse(202, {"operation_id": "op_1"})
    return _FakeResponse(200, {"status": "completed", "result": self.result})

  def read(self, name: str, args: dict):
    return {"status": self.statuses[args["id"]]}

  def call(self, name: str, args: dict):
    self.calls.append((name, args))
    return "{}", False


def test_the_operator_row_leaves_a_close_and_a_new_account_to_a_person():
  from benchmarks.ledger import operator

  assert not operator.supports(TASKS["month"])
  assert operator.supports(TASKS["inbox"])
  (loan,) = operator.as_the_operator_sees_it(TASKS["loan"]).lines
  assert (loan.expect, loan.new_account_trait) == ("open", None)
  assert operator.as_the_operator_sees_it(TASKS["split"]) == TASKS["split"]
  assert not expected_delta(operator.as_the_operator_sees_it(TASKS["loan"]))


def test_the_operator_row_asks_for_a_classification_and_never_a_commit():
  from benchmarks.ledger import operator

  text = operator.prompt(TASKS["settle-bill"], "2026-10", lambda day: f"2026-10-{day}")

  assert "HERMANMILLER.COM ACH PMT HM-88213" in text
  assert "commit" not in text.lower() and "void" not in text.lower()


def test_an_operator_run_is_read_from_the_settled_operation():
  from benchmarks.ledger import operator

  stack = _FakeStack(
    {},
    {
      "content": "Classified the line.",
      "tokens_used": {"input": 900, "output": 40, "cache_read": 5000},
      "tools_called": ["list-event-blocks"],
      "metadata": {
        "loop_iterations": 4,
        "credits_consumed": 12.5,
        "writes": [{"operation": "update-event-block", "id": "evt_1"}],
      },
    },
  )

  transcript = operator.run(
    stack,  # type: ignore[arg-type]
    "classify it",
    500,
    timeout_s=5,
    priced_as="claude-opus-5-5",
  )

  method, path, kwargs = stack.requests[0]
  assert (method, path) == ("POST", "/v1/graphs/kg_test/operator/author")
  assert kwargs["json"] == {
    "message": "classify it",
    "mode": "extended",
    "max_credits": 500,
  }
  assert transcript.stop_reason == "end_turn"
  assert transcript.turns == 4
  assert transcript.final_text == "Classified the line."
  assert [e.name for e in transcript.tool_events] == [
    "list-event-blocks",
    "update-event-block",
  ]
  assert transcript.turn_usage[0]["credits"] == 12.5
  assert transcript.cost_usd == pytest.approx(
    (900 * 4 + 40 * 20 + 5000 * 0.20) / 1_000_000
  )


def test_the_harness_approves_only_what_the_operator_classified():
  from benchmarks.ledger import operator

  stack = _FakeStack({"evt_a": "classified", "evt_b": "captured"}, {})

  approved = operator.approve(stack, {"a": "evt_a", "b": "evt_b"})  # type: ignore[arg-type]

  assert approved == ["a"]
  assert stack.calls == [
    ("update-event-block", {"event_id": "evt_a", "transition_to": "committed"})
  ]
