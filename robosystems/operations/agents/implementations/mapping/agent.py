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
  RS_GAAP_REFINEMENT_SYSTEM_PROMPT,
  build_mapping_prompt,
  build_rs_gaap_refinement_prompt,
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
      ExpandToRsGaapCandidatesTool,
      GetMappingSummaryTool,
      GetUnmappedElementsTool,
      SuggestMappingTool,
    )

    unmapped_tool = ctx.tools.get_tool_instance(GetUnmappedElementsTool)
    suggest_tool = ctx.tools.get_tool_instance(SuggestMappingTool)
    create_tool = ctx.tools.get_tool_instance(CreateMappingAssociationTool)
    expand_tool = ctx.tools.get_tool_instance(ExpandToRsGaapCandidatesTool)
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

    # Accumulate confirmed FAC mappings for the rs-gaap refinement pass:
    # [(coa_element_dict, fac_element_id)]
    confirmed_fac: list[tuple[dict, str]] = []

    # 2. Group by trait for efficient candidate lookup. Elements
    # without a trait can't be narrowed structurally — skip them
    # rather than invent a default.
    by_classification: dict[str, list[dict]] = defaultdict(list)
    for elem in elements:
      cls = elem.get("trait")
      if cls is None:
        skipped += 1
        continue
      by_classification[cls].append(elem)

    # Build an id→element lookup for the refinement pass.
    elem_by_id: dict[str, dict] = {e["id"]: e for e in elements}

    # 3. Get candidates per trait via suggest-mapping tool
    candidates_by_cls: dict[str, list[dict]] = {}
    for cls, cls_elements in by_classification.items():
      suggest_result = await suggest_tool.execute(
        {"element_id": cls_elements[0]["id"], "classification": cls}
      )
      candidates_by_cls[cls] = suggest_result.get("candidates", [])

    # 4. Process elements in batches per classification. Each confirmed /
    # flagged FAC mapping is persisted to coa_mapping as
    # association_type='mapping' — this is the primary rollup target that
    # fact_grid, trial balance, and coverage readers all consume. The
    # rs-gaap refinement pass in step 4b writes a second arc per element
    # with association_type='equivalence' so the filing-specific tag is
    # captured without double-counting line items in the 'mapping' rollup.

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
          mappings = await self._map_batch(ctx, batch, candidates)

          for m in mappings:
            fac_target = m.get("target_id")
            confidence = m["confidence"]
            if fac_target and confidence >= CONFIDENCE_AUTO_APPROVE:
              mapped += 1
            elif fac_target and confidence >= CONFIDENCE_MIN_MAP:
              flagged += 1
            else:
              skipped += 1
              continue

            # Persist the CoA → FAC arc as the primary mapping target.
            try:
              await create_tool.execute(
                {
                  "mapping_id": mapping_id,
                  "from_element_id": m["element_id"],
                  "to_element_id": fac_target,
                  "confidence": confidence,
                  "association_type": "mapping",
                }
              )
              confirmed_fac.append((elem_by_id[m["element_id"]], fac_target))
            except Exception as e:
              logger.warning(f"FAC create failed for {m['element_id']}: {e}")

        except Exception as e:
          logger.warning(f"Batch mapping failed for {cls}: {e}")
          skipped += len(batch)

        processed += len(batch)
        await ctx.progress.report(
          f"Processed {processed}/{total} elements ({cls})",
          percent=(processed / total) * 100,
        )

    # 4b. rs-gaap refinement pass — follow FAC → rs-gaap equivalence + type-subtype
    # for every confirmed FAC mapping, grouped by FAC element to minimise AI calls.
    if confirmed_fac and not await ctx.progress.is_cancelled():
      await ctx.progress.report("Running rs-gaap refinement pass…", percent=90)
      await self._refine_to_rs_gaap(
        ctx, confirmed_fac, mapping_id, expand_tool, create_tool
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
  ) -> list[dict]:
    """Classify a batch of CoA elements against FAC candidates.

    FAC is the primary semantic anchor: the AI picks among ~7-40 clean
    FAC concepts rather than ~2,000 rs-gaap variants. The orchestrator
    persists each accepted result as a CoA → FAC arc with
    ``association_type='mapping'`` (the primary rollup target), and the
    second pass (``_refine_to_rs_gaap``) layers a CoA → rs-gaap arc with
    ``association_type='equivalence'`` so filing-specific tags are
    captured without inflating trial-balance rows.
    """
    prompt = build_mapping_prompt(elements, candidates)

    response = await ctx.ai.create_message(
      messages=[AIMessage(role="user", content=prompt)],
      system=MAPPING_SYSTEM_PROMPT,
      max_tokens=4000,
      temperature=0.3,
      agent_type="mapping",
      operation_description="CoA to FAC mapping",
    )

    return self._parse_response(response.content, elements)

  async def _refine_to_rs_gaap(
    self,
    ctx: AgentContext,
    confirmed_fac: list[tuple[dict, str]],
    mapping_id: str,
    expand_tool: Any,
    create_tool: Any,
  ) -> None:
    """Second pass: write CoA → rs-gaap arcs as `association_type='equivalence'`.

    The FAC arc from pass 1 is the primary rollup target
    (`association_type='mapping'`); this pass layers a second arc per CoA
    element that names the specific rs-gaap filing tag. Readers that walk
    `association_type='mapping'` see only the FAC level (no double-count);
    readers that want filing granularity walk
    `association_type='equivalence'`.

    Groups confirmed mappings by FAC element ID so one expand_tool call
    (and one AI call) serves all CoA accounts that share the same FAC parent.
    """
    # Group CoA elements by their FAC target
    by_fac: dict[str, list[dict]] = defaultdict(list)
    for coa_elem, fac_id in confirmed_fac:
      by_fac[fac_id].append(coa_elem)

    for fac_id, coa_elements in by_fac.items():
      if await ctx.progress.is_cancelled():
        break

      # Expand FAC → rs-gaap parent + type-subtype children
      expand_result = await expand_tool.execute({"fac_element_id": fac_id})
      if "error" in expand_result:
        logger.debug(f"No rs-gaap expansion for {fac_id}: {expand_result['error']}")
        continue

      rs_gaap_parent = expand_result["rs_gaap_parent"]
      candidates = expand_result["candidates"]
      fac_qname = expand_result.get("fac_qname", fac_id)

      # One AI call per CoA element in this FAC group.
      for coa_elem in coa_elements:
        rs_gaap_id: str | None = None
        rs_gaap_conf: float = 0.0
        try:
          prompt = build_rs_gaap_refinement_prompt(
            coa_elem, fac_qname, rs_gaap_parent, candidates
          )
          response = await ctx.ai.create_message(
            messages=[AIMessage(role="user", content=prompt)],
            system=RS_GAAP_REFINEMENT_SYSTEM_PROMPT,
            max_tokens=500,
            temperature=0.2,
            agent_type="mapping",
            operation_description="CoA to rs-gaap refinement",
          )

          result = self._parse_rs_gaap_response(response.content)
          # Use a lower threshold here — the FAC pass already confirmed the
          # semantic bucket; the refinement is just picking the specific tag.
          if (
            result and result.get("rs_gaap_id") and result.get("confidence", 0) >= 0.50
          ):
            rs_gaap_id = result["rs_gaap_id"]
            rs_gaap_conf = result["confidence"]

        except Exception as e:
          logger.warning(f"rs-gaap refinement failed for {coa_elem['id']}: {e}")

        # Fall back to the rs-gaap parent when the AI returned nothing useful
        # (only possible in the narrow case where a parent exists).
        if not rs_gaap_id and rs_gaap_parent:
          rs_gaap_id = rs_gaap_parent["id"]
          rs_gaap_conf = 0.60

        if rs_gaap_id:
          try:
            await create_tool.execute(
              {
                "mapping_id": mapping_id,
                "from_element_id": coa_elem["id"],
                "to_element_id": rs_gaap_id,
                "confidence": rs_gaap_conf,
                "association_type": "equivalence",
              }
            )
          except Exception as e:
            logger.warning(f"rs-gaap create failed for {coa_elem['id']}: {e}")

  def _parse_rs_gaap_response(self, content: str) -> dict | None:
    """Parse the single-object JSON from the rs-gaap refinement pass."""
    try:
      text = content.strip()
      if text.startswith("```"):
        text = text.split("\n", 1)[1] if "\n" in text else text[3:]
        if text.endswith("```"):
          text = text[:-3]
        text = text.strip()
      return json.loads(text)
    except (json.JSONDecodeError, TypeError) as e:
      logger.warning(f"Failed to parse rs-gaap refinement response: {e}")
      return None

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
