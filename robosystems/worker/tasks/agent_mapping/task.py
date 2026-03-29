"""MappingAgent task handler — autonomous CoA → US GAAP mapping.

Iterates through unmapped Chart of Accounts elements, calls Bedrock to
classify and match each to a US GAAP reporting concept, and writes
confirmed mappings via MCP tool classes (direct instantiation).

Uses the same MCP tool classes as cowork (Claude Desktop) — instantiated
in-process with the validated graph_id. Auth was checked at the API
boundary (POST /auto-map) before the task was enqueued.
"""

import json
import logging
from collections import defaultdict
from typing import Any

from robosystems.operations.agents.ai_client import AIClient, AIMessage
from robosystems.worker.tasks import register_task
from robosystems.worker.tasks.agent_mapping.prompt import (
  MAPPING_SYSTEM_PROMPT,
  build_mapping_prompt,
)
from robosystems.worker.tasks.base import BaseTask

logger = logging.getLogger(__name__)

# Confidence thresholds
CONFIDENCE_AUTO_APPROVE = 0.90
CONFIDENCE_MIN_MAP = 0.70

# Max elements per Bedrock call (batched by classification)
BATCH_SIZE = 10


class _ToolClient:
  """Minimal adapter to satisfy MCP tool constructors in worker context.

  MCP tool classes expect a graph_client with a .graph_id attribute.
  This provides exactly that — no HTTP, no auth, just the graph_id
  that was already validated at the API boundary.
  """

  def __init__(self, graph_id: str) -> None:
    self.graph_id = graph_id


@register_task("agent_mapping")
class AgentMappingTask(BaseTask):
  """Autonomous CoA → GAAP mapping via Bedrock AI and MCP tools."""

  async def execute(self) -> dict[str, Any]:
    mapping_id = self.params["mapping_id"]
    tool_client = _ToolClient(self.graph_id)
    ai_client = AIClient()

    # Import MCP tool classes (same ones cowork uses)
    from robosystems.middleware.mcp.tools.taxonomy_tools import (
      CreateMappingAssociationTool,
      GetMappingSummaryTool,
      GetUnmappedElementsTool,
      SuggestMappingTool,
    )

    unmapped_tool = GetUnmappedElementsTool(tool_client)
    suggest_tool = SuggestMappingTool(tool_client)
    create_tool = CreateMappingAssociationTool(tool_client)
    summary_tool = GetMappingSummaryTool(tool_client)

    # 1. Get unmapped elements
    unmapped_result = await unmapped_tool.execute({"mapping_id": mapping_id})
    if "error" in unmapped_result:
      return {
        "error": unmapped_result["error"],
        "mapped": 0,
        "flagged": 0,
        "skipped": 0,
      }

    elements = unmapped_result.get("elements", [])
    total = unmapped_result.get("unmapped_count", len(elements))

    if total == 0:
      await self.report_progress("All elements already mapped", percent=100)
      return {
        "mapped": 0,
        "flagged": 0,
        "skipped": 0,
        "coverage_percent": 100.0,
        "total_credits_consumed": 0.0,
      }

    await self.report_progress(f"Found {total} unmapped elements", percent=0)

    # 2. Group by classification for efficient candidate lookup
    by_classification: dict[str, list[dict]] = defaultdict(list)
    for elem in elements:
      cls = elem.get("classification", "expense")
      by_classification[cls].append(elem)

    # 3. Get candidates per classification via suggest-mapping tool
    candidates_by_cls: dict[str, list[dict]] = {}
    for cls, cls_elements in by_classification.items():
      suggest_result = await suggest_tool.execute(
        {"element_id": cls_elements[0]["id"], "classification": cls}
      )
      candidates_by_cls[cls] = suggest_result.get("candidates", [])

    # 4. Process elements in batches per classification
    mapped, flagged, skipped = 0, 0, 0
    total_credits = 0.0
    processed = 0

    for cls, cls_elements in by_classification.items():
      candidates = candidates_by_cls.get(cls, [])
      if not candidates:
        skipped += len(cls_elements)
        processed += len(cls_elements)
        continue

      for batch_start in range(0, len(cls_elements), BATCH_SIZE):
        if await self.is_cancelled():
          break

        batch = cls_elements[batch_start : batch_start + BATCH_SIZE]

        try:
          mappings, credits = await self._map_batch(
            ai_client, batch, candidates, mapping_id, create_tool
          )
          total_credits += credits

          for m in mappings:
            if m.get("target_id") and m["confidence"] >= CONFIDENCE_AUTO_APPROVE:
              mapped += 1
            elif m.get("target_id") and m["confidence"] >= CONFIDENCE_MIN_MAP:
              flagged += 1
            else:
              skipped += 1

        except Exception as e:
          logger.warning(f"Batch mapping failed for {cls}: {e}")
          skipped += len(batch)

        processed += len(batch)
        await self.report_progress(
          f"Processed {processed}/{total} elements ({cls})",
          percent=(processed / total) * 100,
        )

    # 5. Get final coverage
    try:
      summary = await summary_tool.execute({"mapping_id": mapping_id})
      coverage_percent = summary.get("coverage_percent", 0)
    except Exception:
      coverage_percent = ((mapped + flagged) / total * 100) if total > 0 else 0

    return {
      "mapped": mapped,
      "flagged": flagged,
      "skipped": skipped,
      "coverage_percent": coverage_percent,
      "total_credits_consumed": total_credits,
    }

  async def _map_batch(
    self,
    ai_client: AIClient,
    elements: list[dict],
    candidates: list[dict],
    mapping_id: str,
    create_tool: Any,
  ) -> tuple[list[dict], float]:
    """Map a batch of elements via Bedrock and write results.

    Returns (mapping_results, credits_consumed).
    """
    prompt = build_mapping_prompt(elements, candidates)

    response = await ai_client.create_message(
      messages=[AIMessage(role="user", content=prompt)],
      system=MAPPING_SYSTEM_PROMPT,
      max_tokens=4000,
      temperature=0.3,
      agent_type="mapping",
    )

    credits = await self._consume_credits(
      response.input_tokens,
      response.output_tokens,
      response.model,
    )

    mappings = self._parse_response(response.content, elements)

    # Write confirmed mappings via MCP tool
    for m in mappings:
      if m.get("target_id") and m["confidence"] >= CONFIDENCE_MIN_MAP:
        try:
          await create_tool.execute(
            {
              "mapping_id": mapping_id,
              "from_element_id": m["element_id"],
              "to_element_id": m["target_id"],
              "confidence": m["confidence"],
            }
          )
        except Exception as e:
          logger.warning(f"Failed to create association for {m['element_id']}: {e}")
          m["target_id"] = None
          m["confidence"] = 0

    return mappings, credits

  async def _consume_credits(
    self, input_tokens: int, output_tokens: int, model: str
  ) -> float:
    """Consume credits for a Bedrock call. Returns credits consumed."""
    try:
      from robosystems.db.platform import SessionFactory
      from robosystems.operations.graph.credit_service import CreditService

      session = SessionFactory()
      try:
        service = CreditService(session)
        result = service.consume_ai_tokens(
          graph_id=self.graph_id,
          input_tokens=input_tokens,
          output_tokens=output_tokens,
          model=model,
          operation_description="agent_mapping",
          user_id=self.user_id,
        )
        return float(result.get("credits_consumed", 0))
      finally:
        session.close()
    except Exception as e:
      logger.warning(f"Credit consumption failed: {e}")
      return 0.0

  def _parse_response(self, content: str, elements: list[dict]) -> list[dict]:
    """Parse Bedrock JSON response into mapping results.

    Falls back gracefully on malformed responses.
    """
    try:
      text = content.strip()
      if text.startswith("```"):
        text = text.split("\n", 1)[1] if "\n" in text else text[3:]
        if text.endswith("```"):
          text = text[:-3]
        text = text.strip()

      mappings = json.loads(text)
      if not isinstance(mappings, list):
        mappings = [mappings]

      valid = []
      for m in mappings:
        valid.append(
          {
            "element_id": m.get("element_id", ""),
            "target_id": m.get("target_id"),
            "target_qname": m.get("target_qname"),
            "confidence": float(m.get("confidence", 0)),
            "reasoning": m.get("reasoning", ""),
          }
        )
      return valid

    except (json.JSONDecodeError, KeyError, TypeError) as e:
      logger.warning(f"Failed to parse Bedrock response: {e}")
      return [
        {
          "element_id": elem.get("id", ""),
          "target_id": None,
          "target_qname": None,
          "confidence": 0,
          "reasoning": "Failed to parse AI response",
        }
        for elem in elements
      ]
