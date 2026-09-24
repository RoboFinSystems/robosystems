"""MappingOperator — maps unmapped CoA elements to rs-gaap concepts with the
model, writing ``association_type='mapping'`` arcs through the MCP tools. The
FAC view is derived from those arcs via the fac-to-rs-gaap bridge, never stored
per tenant. Candidates are narrowed by EFS trait and liquidity first."""

from __future__ import annotations

import asyncio
import json
import logging
import re
from collections import defaultdict

from robosystems.operations.operators.ai_client import AIMessage, AIProviderError
from robosystems.operations.operators.base import (
  ExecutionProfile,
  GraphScope,
  Operator,
  OperatorCapability,
  OperatorMode,
  OperatorResult,
  OperatorSpec,
)
from robosystems.operations.operators.implementations.mapping.constants import (
  RS_GAAP_NAME_PATTERN_OVERRIDES,
)
from robosystems.operations.operators.implementations.mapping.prompt import (
  MAPPING_SYSTEM_PROMPT,
  build_mapping_prompt,
)
from robosystems.operations.operators.operator_context import OperatorContext
from robosystems.operations.operators.operator_registry import register_operator

logger = logging.getLogger(__name__)

CONFIDENCE_AUTO_APPROVE = 0.90
CONFIDENCE_MIN_MAP = 0.70

BATCH_SIZE = 10

# Safety net on top of the credit and no-progress stops, so a fast-failing
# pass can't spin indefinitely.
MAX_MAPPING_PASSES = 6

_NAME_PATTERN_OVERRIDES: tuple[tuple[re.Pattern[str], str], ...] = tuple(
  (re.compile(pat, re.IGNORECASE), qname)
  for pat, qname in RS_GAAP_NAME_PATTERN_OVERRIDES
)


def _deterministic_rs_gaap_override(coa_elem: dict) -> str | None:
  """Force an rs-gaap qname for synthesized-detail accounts (e.g. accumulated
  depreciation), which the model otherwise collapses into the net parent."""
  text = f"{coa_elem.get('name') or ''} {coa_elem.get('code') or ''}"
  for pattern, qname in _NAME_PATTERN_OVERRIDES:
    if pattern.search(text):
      return qname
  return None


@register_operator("mapping")
class MappingOperator(Operator):
  spec = OperatorSpec(
    name="Mapping Operator",
    description="Autonomous Chart of Accounts to rs-gaap reporting-concept mapping",
    capabilities=[OperatorCapability.FINANCIAL_ANALYSIS],
    version="1.0.0",
    requires_credits=True,
    supported_modes=[OperatorMode.EXTENDED],
    graph_scope=GraphScope(schema_extension="roboledger"),
    execution_profile={
      OperatorMode.EXTENDED: ExecutionProfile(
        min_time=30, max_time=600, avg_time=120, tool_calls=50
      ),
    },
  )

  async def run(self, ctx: OperatorContext) -> OperatorResult:
    """Repeat single passes until fully mapped, no progress, out of credits,
    or the pass cap. Each pass fetches only still-unmapped elements and
    persists as it goes, so an interrupted run resumes rather than repeats."""
    mapped_total = 0
    flagged_total = 0
    last_skipped = 0
    coverage_percent = 0.0
    passes = 0
    stop_reason = "pass_cap_reached"

    for attempt in range(1, MAX_MAPPING_PASSES + 1):
      if await ctx.progress.is_cancelled():
        stop_reason = "cancelled"
        break
      # Credits are debited after each call, so this is the only gate before
      # spend. Sync DB read, hence the thread.
      if not await asyncio.to_thread(self._has_credit_budget, ctx):
        stop_reason = "insufficient_credits"
        break

      md = (await self._run_single_pass(ctx)).metadata
      passes = attempt
      coverage_percent = md.get("coverage_percent", coverage_percent)
      pass_mapped = md.get("mapped", 0)
      pass_flagged = md.get("flagged", 0)
      # mapped/flagged leave the unmapped set, so they sum across passes;
      # skipped elements are retried, so only the last pass counts.
      mapped_total += pass_mapped
      flagged_total += pass_flagged
      last_skipped = md.get("skipped", 0)

      if coverage_percent >= 100:
        stop_reason = "complete"
        break
      if pass_mapped == 0 and pass_flagged == 0:
        stop_reason = "no_progress"
        break

    return OperatorResult(
      content=(
        f"Mapping stopped ({stop_reason}) after {passes} pass(es): "
        f"{mapped_total} mapped, {flagged_total} flagged for review, "
        f"{coverage_percent:.0f}% coverage"
      ),
      metadata={
        "mapped": mapped_total,
        "flagged": flagged_total,
        "skipped": last_skipped,
        "coverage_percent": coverage_percent,
        "passes": passes,
        "stop_reason": stop_reason,
      },
    )

  def _has_credit_budget(self, ctx: OperatorContext) -> bool:
    """Fails open on lookup error: ``MAX_MAPPING_PASSES`` still bounds spend."""
    try:
      from robosystems.database import SessionFactory
      from robosystems.operations.graph.credit_service import CreditService

      with SessionFactory() as session:
        summary = CreditService(session).get_credit_summary(ctx.graph_id, ctx.user_id)
      if "error" in summary:
        return True
      return float(summary.get("current_balance", 0)) > 0
    except Exception as e:
      logger.warning(
        "Credit pre-check failed for %s: %s; allowing pass (pass cap bounds spend)",
        ctx.graph_id,
        e,
      )
      return True

  async def _run_single_pass(self, ctx: OperatorContext) -> OperatorResult:
    mapping_id = ctx.extra["mapping_id"]

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

    unmapped_result = await unmapped_tool.execute({"mapping_id": mapping_id})
    if "error" in unmapped_result:
      return OperatorResult(
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
      return OperatorResult(
        content="All elements already mapped",
        metadata={"mapped": 0, "flagged": 0, "skipped": 0, "coverage_percent": 100.0},
      )

    await ctx.progress.report(f"Found {total} unmapped elements", percent=0)

    mapped, flagged, skipped = 0, 0, 0
    # Arcs refused because the account has history in a closed month; they
    # need the months reopened, so they are not counted as ``skipped``.
    refused_closed_history = 0
    processed = 0

    # Group by (EFS trait, liquidity); liquidity is None outside assets and
    # liabilities. Elements with no EFS trait are reported as ``unclassified``.
    unclassified: list[dict] = []
    by_group: dict[tuple[str, str | None], list[dict]] = defaultdict(list)
    for elem in elements:
      cls = elem.get("trait")
      if cls is None:
        unclassified.append(elem)
        continue
      by_group[(cls, elem.get("liquidity"))].append(elem)

    elem_by_id: dict[str, dict] = {e["id"]: e for e in elements}

    # Candidates depend only on (trait, liquidity): one call per group.
    candidates_by_group: dict[tuple[str, str | None], list[dict]] = {}
    for (cls, liq), group_elements in by_group.items():
      suggest_result = await suggest_tool.execute(
        {
          "element_id": group_elements[0]["id"],
          "classification": cls,
          **({"liquidity": liq} if liq else {}),
        }
      )
      candidates_by_group[(cls, liq)] = suggest_result.get("candidates", [])

    for (cls, liq), cls_elements in by_group.items():
      candidates = candidates_by_group.get((cls, liq), [])
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

          # The model sometimes repeats an element; keep the most confident.
          seen_in_batch: dict[str, dict] = {}
          for m in mappings:
            eid = m.get("element_id")
            if not eid:
              continue
            existing = seen_in_batch.get(eid)
            if existing is None or m.get("confidence", 0) > existing.get(
              "confidence", 0
            ):
              seen_in_batch[eid] = m

          # Elements the model dropped must still show up in the totals.
          batch_ids = {e["id"] for e in batch}
          for missing_id in batch_ids - seen_in_batch.keys():
            logger.warning(
              "Bedrock omitted element %s from %s mapping batch — counting as skipped",
              missing_id,
              cls,
            )
            skipped += 1

          for m in seen_in_batch.values():
            target = m.get("target_id")
            confidence = m["confidence"]

            override_qname = _deterministic_rs_gaap_override(
              elem_by_id[m["element_id"]]
            )
            if override_qname:
              override_id = await self._resolve_qname_to_id(ctx, override_qname)
              if override_id:
                target = override_id
                # A name match always auto-approves, however thresholds move.
                confidence = max(confidence, CONFIDENCE_AUTO_APPROVE)

            if target and confidence >= CONFIDENCE_AUTO_APPROVE:
              outcome = "mapped"
            elif target and confidence >= CONFIDENCE_MIN_MAP:
              outcome = "flagged"
            else:
              skipped += 1
              continue

            # Count only once the write lands: the tool returns
            # `{"error": ...}` on rejection rather than raising.
            try:
              written = await create_tool.execute(
                {
                  "mapping_id": mapping_id,
                  "from_element_id": m["element_id"],
                  "to_element_id": target,
                  "confidence": confidence,
                  "association_type": "mapping",
                }
              )
            except Exception as e:
              logger.warning(
                f"rs-gaap mapping create failed for {m['element_id']}: {e}"
              )
              skipped += 1
              continue
            if isinstance(written, dict) and written.get("error"):
              logger.warning(
                f"rs-gaap mapping create rejected for {m['element_id']}: "
                f"{written.get('error')}"
              )
              if written.get("error") == "protected_history":
                refused_closed_history += 1
              else:
                skipped += 1
              continue
            if outcome == "mapped":
              mapped += 1
            else:
              flagged += 1

        except AIProviderError:
          # Provider-down must fail the run, not report 0% coverage.
          raise
        except Exception as e:
          logger.warning(f"Batch mapping failed for {cls}: {e}")
          skipped += len(batch)

        processed += len(batch)
        await ctx.progress.report(
          f"Processed {processed}/{total} elements ({cls})",
          percent=(processed / total) * 100,
        )

    try:
      summary = await summary_tool.execute({"mapping_id": mapping_id})
      coverage_percent = summary.get("coverage_percent", 0)
    except Exception:
      coverage_percent = ((mapped + flagged) / total * 100) if total > 0 else 0

    content = (
      f"Mapped {mapped} elements, flagged {flagged} for review, skipped {skipped}"
    )
    if unclassified:
      content += (
        f"; {len(unclassified)} unclassified (no EFS trait — needs classification)"
      )
    if refused_closed_history:
      content += (
        f"; {refused_closed_history} refused (landed history in a closed month "
        "— reopen those months latest-first, then re-run)"
      )

    return OperatorResult(
      content=content,
      metadata={
        "mapped": mapped,
        "flagged": flagged,
        "skipped": skipped,
        "refused_closed_history": refused_closed_history,
        "unclassified": len(unclassified),
        "unclassified_elements": [
          {"id": e["id"], "name": e.get("name")} for e in unclassified[:50]
        ],
        "coverage_percent": coverage_percent,
      },
    )

  async def _resolve_qname_to_id(self, ctx: OperatorContext, qname: str) -> str | None:
    """Tenant element id for ``qname``, cached per instance; ``None`` (logged)
    when the qname isn't seeded."""
    if not hasattr(self, "_qname_cache"):
      self._qname_cache: dict[str, str | None] = {}
    if qname in self._qname_cache:
      return self._qname_cache[qname]

    from sqlalchemy import text

    from robosystems.db.extensions import extensions_session

    with extensions_session(ctx.graph_id) as session:
      row = session.execute(
        text("SELECT id FROM elements WHERE qname = :qname LIMIT 1"),
        {"qname": qname},
      ).fetchone()
    elem_id = row.id if row else None
    self._qname_cache[qname] = elem_id
    if elem_id is None:
      logger.warning(
        "rs-gaap override target %r not found in graph %s — override skipped",
        qname,
        ctx.graph_id,
      )
    return elem_id

  async def _map_batch(
    self,
    ctx: OperatorContext,
    elements: list[dict],
    candidates: list[dict],
  ) -> list[dict]:
    """One result per element when all goes well; callers must tolerate a
    short list (see `_parse_response`)."""
    prompt = build_mapping_prompt(elements, candidates)

    # Sized for a full batch with `reasoning`; a tighter ceiling truncates the
    # JSON and drops accounts into "skipped".
    response = await ctx.ai.create_message(
      messages=[AIMessage(role="user", content=prompt)],
      system=MAPPING_SYSTEM_PROMPT,
      max_tokens=8000,
      temperature=0.3,
      operator_type="mapping",
      operation_description="CoA to rs-gaap mapping",
    )

    return self._parse_response(response.content, elements)

  def _parse_response(self, content: str, elements: list[dict]) -> list[dict]:
    """Parse the model's JSON, tolerating fences, back-to-back values and prose
    between them; zero-confidence placeholders for the batch if nothing parses."""
    try:
      text = self._strip_markdown_fences(content.strip())
      mappings = self._parse_concatenated_json(text)

      if not mappings:
        raise json.JSONDecodeError("no JSON values found in response", text, 0)

      valid = []
      for m in mappings:
        if not isinstance(m, dict):
          continue
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

    except (json.JSONDecodeError, KeyError, TypeError, ValueError) as e:
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

  @staticmethod
  def _strip_markdown_fences(text: str) -> str:
    """Strip a fence only when it brackets the whole payload; mid-stream fences
    are left for `_parse_concatenated_json`."""
    if not text.startswith("```"):
      return text
    after_open = text.split("\n", 1)[1] if "\n" in text else text[3:]
    if after_open.endswith("```"):
      after_open = after_open[:-3]
    return after_open.strip()

  @staticmethod
  def _parse_concatenated_json(text: str) -> list:
    """Decode every top-level JSON value in ``text``, flattening lists. On a
    truncated value (max_tokens cutoff) keep what was already decoded, plus any
    complete objects inside a truncated array."""
    decoder = json.JSONDecoder()
    out: list = []
    i = 0
    n = len(text)
    while i < n:
      while i < n and text[i] not in "[{":
        i += 1
      if i >= n:
        break
      try:
        value, end = decoder.raw_decode(text, i)
      except json.JSONDecodeError:
        if text[i] == "[":
          inner = i + 1
          while inner < n:
            while inner < n and text[inner] != "{":
              inner += 1
            if inner >= n:
              break
            try:
              obj, obj_end = decoder.raw_decode(text, inner)
            except json.JSONDecodeError:
              break
            if isinstance(obj, dict):
              out.append(obj)
            inner = obj_end
        break
      if isinstance(value, list):
        out.extend(value)
      else:
        out.append(value)
      i = end
    return out
