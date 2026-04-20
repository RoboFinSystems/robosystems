"""MappingAgent — autonomous CoA → FAC mapping.

Iterates through unmapped Chart of Accounts elements, calls Bedrock to
match each to a FAC (Fundamental Accounting Concepts) concept, and writes
confirmed mappings via MCP tool classes (direct instantiation). FAC is
the primary semantic target; filing-specific rs-gaap / us-gaap variants
are derived from the FAC match via deterministic equivalence-arc
expansion downstream.

Uses the same MCP tool classes as cowork (Claude Desktop) — instantiated
in-process via DirectToolAccess.
"""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from typing import Any

from robosystems.operations.agents.agent_context import AgentContext
from robosystems.operations.agents.agent_registry import register_agent
from robosystems.operations.agents.ai_client import AIMessage
from robosystems.operations.agents.base import (
  Agent,
  AgentCapability,
  AgentMode,
  AgentResult,
  AgentSpec,
  ExecutionProfile,
  GraphScope,
)
from robosystems.operations.agents.implementations.mapping.prompt import (
  MAPPING_SYSTEM_PROMPT,
  build_mapping_prompt,
)

logger = logging.getLogger(__name__)

# Confidence thresholds
CONFIDENCE_AUTO_APPROVE = 0.90
CONFIDENCE_MIN_MAP = 0.70

# Max elements per Bedrock call (batched by classification)
BATCH_SIZE = 10


@register_agent("mapping")
class MappingAgent(Agent):
  """Autonomous CoA → FAC mapping via Bedrock AI and MCP tools."""

  spec = AgentSpec(
    name="Mapping Agent",
    description="Autonomous Chart of Accounts to FAC (Fundamental Accounting Concepts) mapping",
    capabilities=[AgentCapability.FINANCIAL_ANALYSIS],
    version="1.0.0",
    requires_credits=True,
    supported_modes=[AgentMode.EXTENDED],
    graph_scope=GraphScope(schema_extension="roboledger"),
    execution_profile={
      AgentMode.EXTENDED: ExecutionProfile(
        min_time=30, max_time=600, avg_time=120, tool_calls=50
      ),
    },
  )

  async def run(self, ctx: AgentContext) -> AgentResult:
    mapping_id = ctx.extra["mapping_id"]

    # Import and instantiate MCP tool classes via DirectToolAccess
    from robosystems.middleware.mcp.tools.taxonomy_tools import (
      CreateMappingAssociationTool,
      GetMappingSummaryTool,
      GetUnmappedElementsTool,
      SuggestMappingTool,
    )

    unmapped_tool = ctx.tools.get_tool_instance(GetUnmappedElementsTool)
    suggest_tool = ctx.tools.get_tool_instance(SuggestMappingTool)
    create_tool = ctx.tools.get_tool_instance(CreateMappingAssociationTool)
    summary_tool = ctx.tools.get_tool_instance(GetMappingSummaryTool)

    # 1. Get unmapped elements
    unmapped_result = await unmapped_tool.execute({"mapping_id": mapping_id})
    if "error" in unmapped_result:
      return AgentResult(
        content=f"Failed to get unmapped elements: {unmapped_result['error']}",
        metadata={
          "error": unmapped_result["error"],
          "mapped": 0,
          "flagged": 0,
          "skipped": 0,
        },
      )

    elements = unmapped_result.get("elements", [])
    total = unmapped_result.get("unmapped_count", len(elements))

    if total == 0:
      await ctx.progress.report("All elements already mapped", percent=100)
      return AgentResult(
        content="All elements already mapped",
        metadata={"mapped": 0, "flagged": 0, "skipped": 0, "coverage_percent": 100.0},
      )

    await ctx.progress.report(f"Found {total} unmapped elements", percent=0)

    mapped, flagged, skipped = 0, 0, 0
    processed = 0

    # 2. Group by classification for efficient candidate lookup. Elements
    # without a classification can't be narrowed structurally — skip them
    # rather than invent a default, since the old 'expense' fallback no
    # longer matches any enum value (the vocab moved to inflow/outflow).
    by_classification: dict[str, list[dict]] = defaultdict(list)
    for elem in elements:
      cls = elem.get("classification")
      if cls is None:
        skipped += 1
        continue
      by_classification[cls].append(elem)

    # 3. Get candidates per classification via suggest-mapping tool
    candidates_by_cls: dict[str, list[dict]] = {}
    for cls, cls_elements in by_classification.items():
      suggest_result = await suggest_tool.execute(
        {"element_id": cls_elements[0]["id"], "classification": cls}
      )
      candidates_by_cls[cls] = suggest_result.get("candidates", [])

    # 4. Process elements in batches per classification

    for cls, cls_elements in by_classification.items():
      candidates = candidates_by_cls.get(cls, [])
      if not candidates:
        skipped += len(cls_elements)
        processed += len(cls_elements)
        continue

      for batch_start in range(0, len(cls_elements), BATCH_SIZE):
        if await ctx.progress.is_cancelled():
          break

        batch = cls_elements[batch_start : batch_start + BATCH_SIZE]

        try:
          mappings = await self._map_batch(
            ctx, batch, candidates, mapping_id, create_tool
          )

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
        await ctx.progress.report(
          f"Processed {processed}/{total} elements ({cls})",
          percent=(processed / total) * 100,
        )

    # 5. Get final coverage
    try:
      summary = await summary_tool.execute({"mapping_id": mapping_id})
      coverage_percent = summary.get("coverage_percent", 0)
    except Exception:
      coverage_percent = ((mapped + flagged) / total * 100) if total > 0 else 0

    return AgentResult(
      content=f"Mapped {mapped} elements, flagged {flagged} for review, skipped {skipped}",
      metadata={
        "mapped": mapped,
        "flagged": flagged,
        "skipped": skipped,
        "coverage_percent": coverage_percent,
      },
    )

  async def _map_batch(
    self,
    ctx: AgentContext,
    elements: list[dict],
    candidates: list[dict],
    mapping_id: str,
    create_tool: Any,
  ) -> list[dict]:
    """Map a batch of elements via Bedrock and write results."""
    prompt = build_mapping_prompt(elements, candidates)

    response = await ctx.ai.create_message(
      messages=[AIMessage(role="user", content=prompt)],
      system=MAPPING_SYSTEM_PROMPT,
      max_tokens=4000,
      temperature=0.3,
      agent_type="mapping",
      operation_description="CoA to FAC mapping",
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

    return mappings

  def _parse_response(self, content: str, elements: list[dict]) -> list[dict]:
    """Parse Bedrock JSON response into mapping results."""
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
