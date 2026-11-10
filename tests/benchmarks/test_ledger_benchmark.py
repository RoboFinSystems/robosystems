"""The ledger benchmark's scorer, keys and guards (no stack, no model)."""

from collections import Counter

import pytest

from benchmarks.ledger.mcp import McpClient, McpError, ToolDef
from benchmarks.ledger.pricing import turn_cost
from benchmarks.ledger.rows import CHECK_TOOLS, CLOSE_LIST, tools_for
from benchmarks.ledger.score import balance_diff, expected_delta, judge_line, outcome
from benchmarks.ledger.tasks import (
  AMBIGUOUS,
  DUPLICATE,
  LOAN,
  SETTLE_BILL,
  SPLIT,
  TASKS,
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


def test_every_key_balances():
  for task in TASKS.values():
    assert sum(expected_delta(task).values()) == 0, task.name


def test_month_key_moves_cash_by_every_committed_line():
  delta = expected_delta(TASKS["month"])
  assert delta["1000"] == -18437 - 61240 - 100000 - 90000 + 2500000
  assert delta["2000"] == 0
  assert delta["6500"] == 18437 + 70000 + 90000
  assert delta["NEW:liability"] == -2500000
  assert delta["7000"] == 15833


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
