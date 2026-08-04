"""Bounded, model-driven tool-use loop for operators.

Lets the model choose and call read-only MCP tools, feeds tool errors back
so it can self-correct, and stops at a bounded iteration count. This is the
shared harness behind CypherOperator (its first caller) and any future
read/analysis operator — the same tool-use loop that makes Claude-via-MCP
robust, run in-process on Bedrock with automatic per-call credit tracking.

Contrast with the old single-shot pipeline: there, Python called two fixed
tools and the model never saw a query error. Here the model drives the tools
and every tool error comes back as an ``is_error`` tool_result it can react to.
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

_ANSWER_NOW = (
  "You've reached the step limit. Answer the original question now using the "
  "results gathered so far. Do not request any more tools."
)


@dataclass
class ToolLoopResult:
  """Outcome of a tool-use loop."""

  text: str
  rows: list[dict[str, Any]] | None = None  # last read-graph-cypher result set
  cypher: str | None = None  # the query that produced ``rows``
  tools_called: list[str] = field(default_factory=list)
  iterations: int = 0
  hit_cap: bool = False  # stopped at max_iterations rather than by the model


def _serialize_tool_result(result: Any) -> str:
  """JSON-encode a tool result for feedback, capped in size."""
  text = json.dumps(result, default=str)
  if len(text) > _MAX_TOOL_RESULT_CHARS:
    text = text[:_MAX_TOOL_RESULT_CHARS] + f"\n… [truncated; {len(text)} chars total]"
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
) -> ToolLoopResult:
  """Run a bounded tool-use loop and return the model's final answer.

  The model is given the read-only tools named by ``tool_names`` (intersected
  with what the graph actually exposes) and iterates: call tools → observe
  results, or errors fed back as ``is_error`` tool_results so it can retry →
  answer in natural language. Bounded by ``max_iterations``; on hitting the
  cap, one final turn nudges the model to answer from what it gathered.

  Args:
      ctx: Operator context (ai, tools, progress, history, query).
      system: System prompt.
      tool_names: Read-only tool allowlist to request. Only names actually
          available on the graph are passed to the model.
      max_iterations: Maximum model round-trips that may call tools.
      max_tokens: Max output tokens per model call.
      temperature: Sampling temperature.
      operator_type: For model-override lookup + credit audit.
      operation_description: Credit audit description.

  Returns:
      ToolLoopResult with the final text, the last Cypher result set (for a
      table), and loop diagnostics.
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

  step = 60 // max(max_iterations, 1)

  for iteration in range(max_iterations):
    await ctx.progress.report(
      "Thinking..." if iteration == 0 else f"Working (step {iteration + 1})...",
      percent=min(20 + iteration * step, 85),
    )

    response = await ctx.ai.create_message(
      messages=messages,
      system=system,
      max_tokens=max_tokens,
      temperature=temperature,
      operator_type=operator_type,
      operation_description=operation_description,
      tools=tools,
    )

    # Model produced a final answer (no tool call) — done.
    if response.stop_reason != "tool_use":
      return ToolLoopResult(
        text=response.content,
        rows=last_rows,
        cypher=last_cypher,
        tools_called=tools_called,
        iterations=iteration + 1,
      )

    # Replay the assistant turn (text + tool_use blocks) verbatim.
    messages.append(AIMessage(role="assistant", content=response.content_blocks))

    tool_results: list[dict[str, Any]] = []
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

      tool_results.append(
        {
          "type": "tool_result",
          "tool_use_id": tool_use_id,
          "content": _serialize_tool_result(result),
          "is_error": is_error,
        }
      )

    messages.append(AIMessage(role="user", content=tool_results))

  # Iteration cap reached. Nudge for a final answer, appending the nudge to
  # the trailing user turn (avoids a second consecutive user message). Keep
  # `tools` defined so the tool_use/tool_result transcript stays valid.
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
  )
  return ToolLoopResult(
    text=final.content
    or "I gathered results but couldn't compose a final answer within the step limit.",
    rows=last_rows,
    cypher=last_cypher,
    tools_called=tools_called,
    iterations=max_iterations,
    hit_cap=True,
  )
