"""The tool loop: one trial, one model, every tool call and token recorded."""

from __future__ import annotations

import json
import time
import traceback
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from typing import Any

from .mcp import ToolDef
from .pricing import trial_cost
from .providers import CannotAttempt, Provider

ToolRunner = Callable[[str, dict[str, Any]], tuple[str, bool]]


@dataclass
class ToolEvent:
  turn: int
  name: str
  args: dict[str, Any]
  result_chars: int
  is_error: bool
  elapsed_s: float


@dataclass
class Transcript:
  model: str
  turns: int = 0
  tool_errors: int = 0
  turn_usage: list[dict[str, Any]] = field(default_factory=list)
  served_by: list[str] = field(default_factory=list)
  tool_events: list[ToolEvent] = field(default_factory=list)
  wall_s: float = 0.0
  cost_usd: float | None = None
  stop_reason: str = ""
  final_text: str = ""
  error: str | None = None
  messages: list[dict[str, Any]] = field(default_factory=list)

  def as_dict(self) -> dict[str, Any]:
    return asdict(self)


def _cost(model: str, turn_usage: list[dict[str, Any]]) -> float | None:
  """What the provider charged, where every turn says; else the price table."""
  charged = [usage.get("cost_usd") for usage in turn_usage]
  if all(cost is not None for cost in charged):
    return round(sum(cost for cost in charged if cost is not None), 6)
  return trial_cost(model, turn_usage)


def run_trial(
  provider: Provider,
  system: str,
  user_text: str,
  tools: list[ToolDef],
  tool_runner: ToolRunner,
  max_turns: int,
  max_usd: float,
) -> Transcript:
  t = Transcript(provider.model)
  allowed = {tool.name for tool in tools}
  started = time.monotonic()
  conversation: Any = None
  try:
    conversation = provider.start(system, user_text, tools)
    for turn_index in range(1, max_turns + 1):
      t.turns = turn_index
      turn = provider.step(conversation)
      t.turn_usage.append(turn.usage.as_dict())
      t.cost_usd = _cost(provider.model, t.turn_usage)
      if turn.served_by and turn.served_by not in t.served_by:
        t.served_by.append(turn.served_by)
      if not turn.tool_calls:
        t.final_text, t.stop_reason = turn.text, turn.stop_reason
        break
      results = []
      for call in turn.tool_calls:
        call_started = time.monotonic()
        if call.error:
          text, is_error = json.dumps({"error": call.error}), True
        elif call.name in allowed:
          text, is_error = tool_runner(call.name, call.args)
        else:
          text, is_error = f'{{"error": "no tool named {call.name}"}}', True
        t.tool_errors += int(is_error)
        t.tool_events.append(
          ToolEvent(
            turn_index,
            call.name,
            call.args,
            len(text),
            is_error,
            round(time.monotonic() - call_started, 3),
          )
        )
        results.append((call, text, is_error))
      provider.add_tool_results(conversation, results)
      if t.cost_usd is not None and t.cost_usd > max_usd:
        t.stop_reason = "budget"
        break
    else:
      t.stop_reason = "max_turns"
  except CannotAttempt as exc:
    t.error, t.stop_reason = str(exc), "cannot_attempt"
  except Exception as exc:
    t.error = f"{type(exc).__name__}: {exc}\n{traceback.format_exc()[-1500:]}"
    t.stop_reason = "error"
  if conversation is not None:
    t.messages = provider.serialize(conversation)
  t.wall_s = round(time.monotonic() - started, 3)
  return t
