"""Bounded, model-driven tool-use loop for read/analysis operators. Tool errors
go back to the model as error results so it can correct itself."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from robosystems.logger import logger
from robosystems.operations.operators.ai_client import (
  AIMessage,
  text_block,
  tool_result_block,
)

if TYPE_CHECKING:
  from robosystems.operations.operators.operator_context import OperatorContext

# Caps what the model sees; the caller gets the full rows separately.
_MAX_TOOL_RESULT_CHARS = 12000

# A truncated schema is worse than none (the model plans against relationships
# it never saw), so orientation tools get a cap above any real payload.
_ORIENTATION_TOOL_RESULT_CHARS = 48000
_TOOL_RESULT_CHAR_CAPS: dict[str, int] = {
  "get-graph-schema": _ORIENTATION_TOOL_RESULT_CHARS,
  "get-example-queries": _ORIENTATION_TOOL_RESULT_CHARS,
}

# Uncharged turns per run for a turn whose tool calls ALL failed, so quick mode
# keeps its one retry after a syntax error.
DEFAULT_MAX_ERROR_RETRIES = 2

_ANSWER_NOW = (
  "You've reached the step limit. Answer the original question now using the "
  "results gathered so far. Do not request any more tools."
)

_ANSWER_NOW_CREDITS = (
  "You've reached the credit limit set for this question. Answer the original "
  "question now using the results gathered so far. Do not request any more "
  "tools."
)

_NO_ANSWER = (
  "I gathered results but couldn't compose a final answer within the step limit."
)


@dataclass
class ToolLoopResult:
  text: str
  rows: list[dict[str, Any]] | None = None  # last read-graph-cypher result set
  cypher: str | None = None  # the query that produced ``rows``
  tools_called: list[str] = field(default_factory=list)
  iterations: int = 0  # model calls made, including the nudge if any
  hit_cap: bool = False  # stopped at max_iterations rather than by the model
  hit_credit_ceiling: bool = False  # stopped by the caller's max_credits
  cancelled: bool = False  # the operation was cancelled; no answer was composed
  error_retries: int = 0  # uncharged turns granted for all-error tool results


def _serialize_tool_result(result: Any, tool_name: str | None = None) -> str:
  cap = _TOOL_RESULT_CHAR_CAPS.get(tool_name or "", _MAX_TOOL_RESULT_CHARS)
  text = json.dumps(result, default=str)
  if len(text) > cap:
    text = text[:cap] + f"\n… [truncated; {len(text)} chars total]"
  return text


def _seed_history(ctx: OperatorContext) -> list[AIMessage]:
  messages: list[AIMessage] = []
  for msg in ctx.history[-5:]:
    if isinstance(msg, dict):
      role = msg.get("role", "user")
      content = msg.get("content", "")
    else:
      role = getattr(msg, "role", "user")
      content = getattr(msg, "content", "")
    messages.append(AIMessage(role=role, content=content))
  return messages


async def run_tool_loop(
  ctx: OperatorContext,
  *,
  system: str,
  tool_names: list[str],
  max_iterations: int,
  max_tokens: int,
  temperature: float = 0.3,
  operator_type: str | None = None,
  operation_description: str = "Tool-use loop",
  max_error_retries: int = DEFAULT_MAX_ERROR_RETRIES,
  max_credits: float | None = None,
  user_message: str | None = None,
) -> ToolLoopResult:
  """Run a bounded tool-use loop and return the model's final answer.

  ``tool_names`` is intersected with what the graph exposes, and only that set
  is dispatched. At most ``max_iterations + max_error_retries + 1`` model
  calls: all-error turns are uncharged up to ``max_error_retries``, and on the
  cap one wrap-up turn asks for an answer.

  ``max_credits`` is a soft ceiling checked between calls; the wrap-up turn
  can carry spend somewhat past it. ``user_message`` replaces ``ctx.query``
  as the opening turn, for per-request context that must stay out of the
  cached system prefix.
  """
  tools = await ctx.tools.get_tool_schemas(tool_names)
  if not tools:
    logger.warning(
      "run_tool_loop: none of %s available on graph %s", tool_names, ctx.graph_id
    )
  # Enforced at dispatch, not just advertised: call_tool would run any name.
  advertised = {t["name"] for t in tools}

  messages: list[AIMessage] = _seed_history(ctx)
  messages.append(AIMessage(role="user", content=user_message or ctx.query))

  tools_called: list[str] = []
  last_rows: list[dict[str, Any]] | None = None
  last_cypher: str | None = None

  tool_turns = 0
  error_retries = 0
  model_calls = 0
  hit_ceiling = False
  step = 60 // max(max_iterations, 1)

  while tool_turns < max_iterations:
    # Before every call, including the first, so a run cancelled while
    # queued spends nothing. No wrap-up call on cancel.
    if await ctx.progress.is_cancelled():
      return ToolLoopResult(
        text="Cancelled before an answer was reached.",
        rows=last_rows,
        cypher=last_cypher,
        tools_called=tools_called,
        iterations=model_calls,
        cancelled=True,
        error_retries=error_retries,
      )

    # The first call always runs; the pre-flight already gated it.
    if (
      max_credits is not None
      and model_calls > 0
      and getattr(ctx.ai, "total_credits", 0.0) >= max_credits
    ):
      hit_ceiling = True
      break

    await ctx.progress.report(
      "Thinking..." if model_calls == 0 else f"Working (step {model_calls + 1})...",
      percent=min(20 + tool_turns * step, 85),
    )

    response = await ctx.ai.create_message(
      messages=messages,
      system=system,
      max_tokens=max_tokens,
      temperature=temperature,
      operator_type=operator_type,
      operation_description=operation_description,
      tools=tools,
      cache_conversation=True,
    )
    model_calls += 1

    if response.stop_reason != "tool_use":
      return ToolLoopResult(
        text=response.content,
        rows=last_rows,
        cypher=last_cypher,
        tools_called=tools_called,
        iterations=model_calls,
        error_retries=error_retries,
      )

    # Verbatim: a reasoning block's signature is checked on replay.
    messages.append(AIMessage(role="assistant", content=response.content_blocks))

    tool_results: list[dict[str, Any]] = []
    turn_succeeded = False
    for call in response.tool_calls:
      name = call.name
      args = call.input
      tools_called.append(name)

      is_error = False
      if name not in advertised:
        logger.warning(
          "run_tool_loop: model requested unadvertised tool %s on graph %s",
          name,
          ctx.graph_id,
        )
        tool_results.append(
          tool_result_block(
            call.id,
            _serialize_tool_result({"error": f"Tool '{name}' is not available"}),
            is_error=True,
          )
        )
        continue
      try:
        result = await ctx.tools.call_tool(name, args, return_raw=True)
        # Some tools return {"error": ...} instead of raising.
        if isinstance(result, dict) and "error" in result:
          is_error = True
        elif name == "read-graph-cypher" and isinstance(result, list) and result:
          # Last NON-EMPTY set, so a later zero-row probe can't wipe it.
          last_rows = result
          last_cypher = args.get("query")
      except Exception as e:  # cypher_tool raises ValueError on bad queries
        logger.info("run_tool_loop tool %s errored: %s", name, e)
        result = {"error": str(e)}
        is_error = True

      if not is_error:
        turn_succeeded = True
      tool_results.append(
        tool_result_block(call.id, _serialize_tool_result(result, name), is_error)
      )

    messages.append(AIMessage(role="user", content=tool_results))

    if turn_succeeded or error_retries >= max_error_retries:
      tool_turns += 1
    else:
      error_retries += 1

  if await ctx.progress.is_cancelled():
    return ToolLoopResult(
      text="Cancelled before an answer was reached.",
      rows=last_rows,
      cypher=last_cypher,
      tools_called=tools_called,
      iterations=model_calls,
      cancelled=True,
      error_retries=error_retries,
    )

  # Appended to the trailing user turn (consecutive user messages are
  # rejected). `tools` must stay set while the transcript has tool blocks, so
  # the nudge forbids tools in words and a stray tool call is not executed.
  answer_now = _ANSWER_NOW_CREDITS if hit_ceiling else _ANSWER_NOW
  final_messages = list(messages)
  last = final_messages[-1]
  if last.role == "user":
    if isinstance(last.content, list):
      final_messages[-1] = AIMessage(
        role="user", content=[*last.content, text_block(answer_now)]
      )
    else:
      final_messages[-1] = AIMessage(
        role="user", content=f"{last.content}\n\n{answer_now}"
      )
  else:
    final_messages.append(AIMessage(role="user", content=answer_now))

  final = await ctx.ai.create_message(
    messages=final_messages,
    system=system,
    max_tokens=max_tokens,
    temperature=temperature,
    operator_type=operator_type,
    operation_description=operation_description,
    tools=tools,
    cache_conversation=True,
  )
  if final.tool_calls and not final.content:
    logger.info(
      "run_tool_loop: model requested %d more tool(s) on the wrap-up turn; "
      "answering from gathered results",
      len(final.tool_calls),
    )
  return ToolLoopResult(
    text=final.content or _NO_ANSWER,
    rows=last_rows,
    cypher=last_cypher,
    tools_called=tools_called,
    iterations=model_calls + 1,
    hit_cap=not hit_ceiling,
    hit_credit_ceiling=hit_ceiling,
    error_retries=error_retries,
  )
