"""The operator row: the task goes to the stack's own author operator, which
runs its own prompt, tool list and limits on whatever model the stack is
configured with. The operator classifies; committing is a person's step, so
the harness approves what it classified before the books are scored."""

from __future__ import annotations

import time
from dataclasses import replace
from typing import Any

from .loop import ToolEvent, Transcript
from .mcp import McpClient
from .pricing import trial_cost
from .tasks import BankLine, Task, _dollars

OPERATOR = "author"
_SETTLED = frozenset({"completed", "failed", "cancelled"})
_POLL_SECONDS = 3


def supports(task: Task) -> bool:
  """A close is a person's action, not the operator's."""
  return not task.close


def as_the_operator_sees_it(task: Task) -> Task:
  """The task with each line's expected result set to what the operator is
  allowed to do: it cannot add an account, so a line that needs one is left
  for a person."""
  return replace(task, lines=tuple(_operator_line(line) for line in task.lines))


def _operator_line(line: BankLine) -> BankLine:
  if not line.new_account_trait:
    return line
  return replace(line, expect="open", new_account_trait=None, flows=frozenset())


def prompt(task: Task, period: str, day: Any) -> str:
  rules = (
    "Classify each to the account the company's history and policies support. "
    "Leave unclassified any line that needs a person, and say why."
  )
  if not task.inbox:
    named = "; ".join(
      f'"{line.description}" on {day(line.day)} for {_dollars(line.cents)}'
      for line in task.lines
    )
    return (
      f"A new bank line has arrived in the inbox for {period}: {named}. {rules} "
      "Work only that line and leave the rest of the inbox as it is."
    )
  return (
    f"Work the captured bank lines in the inbox that are dated in {period}. "
    f"{rules} Leave lines dated in a later month alone."
  )


def run(
  mcp: McpClient,
  message: str,
  max_credits: float | None,
  timeout_s: int,
  priced_as: str,
) -> Transcript:
  """`priced_as` is the model the stack's author tier runs on: the run does
  not report it, so the dollar cost is worked out from its tokens."""
  transcript = Transcript(f"operator:{OPERATOR}")
  started = time.monotonic()
  body: dict[str, Any] = {"message": message, "mode": "extended"}
  if max_credits:
    body["max_credits"] = max_credits
  response = mcp.api(
    "POST",
    f"/v1/graphs/{mcp.graph_id}/operator/{OPERATOR}",
    params={"mode": "sync"},
    json=body,
  )
  if response.status_code not in (200, 202):
    transcript.stop_reason = "error"
    transcript.error = f"HTTP {response.status_code}: {response.text[:500]}"
    return transcript
  answer = response.json()
  operation_id = answer.get("operation_id")
  status: dict[str, Any] = {}
  while response.status_code == 202 or not status:
    status = mcp.api("GET", f"/v1/operations/{operation_id}/status").json()
    if str(status.get("status")) in _SETTLED:
      break
    if time.monotonic() - started > timeout_s:
      transcript.stop_reason, transcript.error = "timeout", f"after {timeout_s}s"
      transcript.wall_s = round(time.monotonic() - started, 1)
      return transcript
    time.sleep(_POLL_SECONDS)

  result = status.get("result") or status.get("result_data") or {}
  metadata = result.get("metadata") or {}
  transcript.wall_s = round(time.monotonic() - started, 1)
  transcript.final_text = result.get("content") or status.get("error") or ""
  transcript.turns = int(metadata.get("loop_iterations") or 0)
  tokens = result.get("tokens_used") or {}
  transcript.turn_usage = [
    {
      "input_tokens": tokens.get("input", 0),
      "output_tokens": tokens.get("output", 0),
      "cache_read_tokens": tokens.get("cache_read", 0),
      "cache_write_tokens": tokens.get("cache_write", 0),
      "credits": metadata.get("credits_consumed"),
      "priced_as": priced_as,
    }
  ]
  transcript.cost_usd = trial_cost(priced_as, transcript.turn_usage)
  # The run reports which tools it used and each write, not every call.
  transcript.tool_events = [
    ToolEvent(0, name, {}, 0, False, 0.0) for name in result.get("tools_called") or []
  ] + [
    ToolEvent(0, write.get("operation", "?"), write, 0, False, 0.0)
    for write in metadata.get("writes") or []
  ]
  transcript.messages = [status]
  if str(status.get("status")) != "completed":
    transcript.stop_reason = str(status.get("status") or "error")
    transcript.error = str(status.get("error") or status.get("error_message") or "")
  elif metadata.get("hit_step_limit"):
    transcript.stop_reason = "max_turns"
  elif metadata.get("hit_credit_ceiling"):
    transcript.stop_reason = "max_credits"
  else:
    transcript.stop_reason = "end_turn"
  return transcript


def approve(mcp: McpClient, line_ids: dict[str, str]) -> list[str]:
  """Commit every task line the operator classified, as the person reviewing
  the inbox would. Returns the keys approved; a refusal is left for scoring."""
  approved = []
  for key, event_id in line_ids.items():
    if mcp.read("get-event-block", {"id": event_id})["status"] != "classified":
      continue
    _text, is_error = mcp.call(
      "update-event-block", {"event_id": event_id, "transition_to": "committed"}
    )
    if not is_error:
      approved.append(key)
  return approved
