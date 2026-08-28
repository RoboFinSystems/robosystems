"""Bounded, model-driven tool-use loop for operators.

The model chooses which read-only MCP tools to call; every tool error comes
back as an ``is_error`` tool_result so it can correct itself and retry. The
loop is the shared harness behind `CypherOperator` and any other read/analysis
operator — the Claude-via-MCP tool loop run in-process on Bedrock, with
per-call credit tracking supplied by `TrackedAIClient`.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from robosystems.logger import logger
from robosystems.operations.operators.ai_client import AIMessage

if TYPE_CHECKING:
  from robosystems.operations.operators.operator_context import OperatorContext

# Tool results fed back to the model are capped so a large query result can't
# blow the context window or run up credit spend — the model only needs
# enough rows to reason about. The full result is captured separately (rows)
# for the caller/frontend, which paginates.
_MAX_TOOL_RESULT_CHARS = 12000

# Orientation tools are the exception: a truncated schema or example set is
# worse than none, because the model then plans queries against
# relationships it never saw. A tenant schema with the ledger spine runs
# 20-30k chars, so these get a cap well above the largest real payload and
# only a pathological graph truncates.
_ORIENTATION_TOOL_RESULT_CHARS = 48000
_TOOL_RESULT_CHAR_CAPS: dict[str, int] = {
  "get-graph-schema": _ORIENTATION_TOOL_RESULT_CHARS,
  "get-example-queries": _ORIENTATION_TOOL_RESULT_CHARS,
}

# A turn whose tool calls ALL fail is not charged against the tool budget, up
# to this many times per run. Error feedback is the point of the loop, and
# charging a budgeted turn for a syntax error starves quick mode of the one
# retry it needs to recover.
DEFAULT_MAX_ERROR_RETRIES = 2

_ANSWER_NOW = (
  "You've reached the step limit. Answer the original question now using the "
  "results gathered so far. Do not request any more tools."
)

# The nudge keeps the tool definitions (the transcript carries tool_use
# blocks, which the API requires tools for) but forbids further calls, so
# the final turn is guaranteed to be text rather than a dropped tool_use.
_NO_MORE_TOOLS: dict[str, Any] = {"type": "none"}


@dataclass
class ToolLoopResult:
  """Outcome of a tool-use loop."""

  text: str
  rows: list[dict[str, Any]] | None = None  # last read-graph-cypher result set
  cypher: str | None = None  # the query that produced ``rows``
  tools_called: list[str] = field(default_factory=list)
  iterations: int = 0  # model calls made, including the nudge if any
  hit_cap: bool = False  # stopped at max_iterations rather than by the model
  error_retries: int = 0  # uncharged turns granted for all-error tool results


def _serialize_tool_result(result: Any, tool_name: str | None = None) -> str:
  """JSON-encode a tool result for feedback, capped in size per tool."""
  cap = _TOOL_RESULT_CHAR_CAPS.get(tool_name or "", _MAX_TOOL_RESULT_CHARS)
  text = json.dumps(result, default=str)
  if len(text) > cap:
    text = text[:cap] + f"\n… [truncated; {len(text)} chars total]"
  return text


def _seed_history(ctx: OperatorContext) -> list[AIMessage]:
  """Convert the last few conversation turns into plain-text messages."""
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
) -> ToolLoopResult:
  """Run a bounded tool-use loop and return the model's final answer.

  The model gets the read-only tools named by ``tool_names``, intersected with
  what the graph actually exposes, and iterates: call tools → observe results
  (errors included) → answer in natural language. ``max_iterations`` caps the
  round-trips that may call tools. A round-trip whose tool calls all failed is
  not charged against that cap, up to ``max_error_retries`` times, so a bad
  query costs the model a correction rather than a step. On hitting the cap
  one further turn nudges the model to answer from what it has with tool use
  disabled, so the loop always costs at most
  ``max_iterations + max_error_retries + 1`` model calls.
  """
  tools = await ctx.tools.get_tool_schemas(tool_names)
  if not tools:
    logger.warning(
      "run_tool_loop: none of %s available on graph %s", tool_names, ctx.graph_id
    )
  # The dispatch below must enforce this set, not just advertise it: the
  # model can emit any tool name, and call_tool dispatches whatever the
  # underlying tool manager exposes.
  advertised = {t["name"] for t in tools}

  messages: list[AIMessage] = _seed_history(ctx)
  messages.append(AIMessage(role="user", content=ctx.query))

  tools_called: list[str] = []
  last_rows: list[dict[str, Any]] | None = None
  last_cypher: str | None = None

  tool_turns = 0  # round-trips charged against max_iterations
  error_retries = 0  # uncharged round-trips granted so far
  model_calls = 0
  step = 60 // max(max_iterations, 1)

  while tool_turns < max_iterations:
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
      # The transcript grows monotonically across iterations, so a breakpoint
      # on the trailing turn means every call from the second onward reads the
      # previous call's cache entry and extends it.
      cache_conversation=True,
    )
    model_calls += 1

    # No tool call means the model is answering — done.
    if response.stop_reason != "tool_use":
      return ToolLoopResult(
        text=response.content,
        rows=last_rows,
        cypher=last_cypher,
        tools_called=tools_called,
        iterations=model_calls,
        error_retries=error_retries,
      )

    # Replay the assistant turn (text + tool_use blocks) verbatim.
    messages.append(AIMessage(role="assistant", content=response.content_blocks))

    tool_results: list[dict[str, Any]] = []
    turn_succeeded = False
    for block in response.content_blocks:
      if block.get("type") != "tool_use":
        continue
      name = block.get("name", "")
      tool_use_id = block.get("id", "")
      args = block.get("input") or {}
      tools_called.append(name)

      is_error = False
      if name not in advertised:
        logger.warning(
          "run_tool_loop: model requested unadvertised tool %s on graph %s",
          name,
          ctx.graph_id,
        )
        tool_results.append(
          {
            "type": "tool_result",
            "tool_use_id": tool_use_id,
            "content": _serialize_tool_result(
              {"error": f"Tool '{name}' is not available"}
            ),
            "is_error": True,
          }
        )
        continue
      try:
        result = await ctx.tools.call_tool(name, args, return_raw=True)
        # Registrar/domain tools report failure as {"error": ...} instead of
        # raising; treat both paths as errors so the model gets the feedback.
        if isinstance(result, dict) and "error" in result:
          is_error = True
        elif name == "read-graph-cypher" and isinstance(result, list) and result:
          # Capture the last NON-EMPTY result set so a later exploratory or
          # zero-row query doesn't wipe the rows that back the answer. If every
          # query returns empty, last_rows stays None and the console shows no
          # table — correct for a genuinely empty answer.
          last_rows = result
          last_cypher = args.get("query")
      except Exception as e:  # cypher_tool raises ValueError on bad queries
        logger.info("run_tool_loop tool %s errored: %s", name, e)
        result = {"error": str(e)}
        is_error = True

      if not is_error:
        turn_succeeded = True
      tool_results.append(
        {
          "type": "tool_result",
          "tool_use_id": tool_use_id,
          "content": _serialize_tool_result(result, name),
          "is_error": is_error,
        }
      )

    messages.append(AIMessage(role="user", content=tool_results))

    if turn_succeeded or error_retries >= max_error_retries:
      tool_turns += 1
    else:
      error_retries += 1

  # Iteration cap reached. Nudge for a final answer, appending the nudge to
  # the trailing user turn (avoids a second consecutive user message). Keep
  # `tools` defined so the tool_use/tool_result transcript stays valid, but
  # disable tool choice so the answer can't come back as a tool_use block
  # that nobody would execute.
  final_messages = list(messages)
  last = final_messages[-1]
  if last.role == "user" and isinstance(last.content, list):
    final_messages[-1] = AIMessage(
      role="user",
      content=[*last.content, {"type": "text", "text": _ANSWER_NOW}],
    )
  else:
    final_messages.append(AIMessage(role="user", content=_ANSWER_NOW))

  final = await ctx.ai.create_message(
    messages=final_messages,
    system=system,
    max_tokens=max_tokens,
    temperature=temperature,
    operator_type=operator_type,
    operation_description=operation_description,
    tools=tools,
    tool_choice=_NO_MORE_TOOLS if tools else None,
    cache_conversation=True,
  )
  return ToolLoopResult(
    text=final.content
    or "I gathered results but couldn't compose a final answer within the step limit.",
    rows=last_rows,
    cypher=last_cypher,
    tools_called=tools_called,
    iterations=model_calls + 1,
    hit_cap=True,
    error_retries=error_retries,
  )
