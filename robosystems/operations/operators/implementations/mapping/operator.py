"""MappingOperator — autonomous CoA → rs-gaap mapping.

Iterates through unmapped Chart of Accounts elements, calls Bedrock to
match each to an rs-gaap reporting concept, and writes confirmed mappings
(``association_type='mapping'``) via MCP tool classes (direct
instantiation). This is the rs-gaap-anchored model: rs-gaap is the
canonical reporting target the renderer consumes, and the FAC view is
*derived* from each CoA → rs-gaap arc through the fac-to-rs-gaap
equivalence bridge — never stored as a separate per-tenant CoA → FAC arc.

Candidates are narrowed by the CoA element's EFS trait and (for
assets/liabilities) its liquidity trait, so the AI chooses among a tight,
section-correct candidate set rather than guessing current-vs-noncurrent.

Uses the same MCP tool classes as cowork (Claude Desktop) — instantiated
in-process via DirectToolAccess.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from collections import defaultdict

from robosystems.operations.operators.ai_client import AIMessage
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

# Confidence thresholds
CONFIDENCE_AUTO_APPROVE = 0.90
CONFIDENCE_MIN_MAP = 0.70

# Max elements per Bedrock call (batched by classification)
BATCH_SIZE = 10

# Max re-invocation passes the bounded mapping loop will make. Each pass
# re-fetches ONLY the still-unmapped elements (idempotent) and persists
# each mapping as it goes, so passes resume rather than repeat work — a
# pass interrupted by the worker timeout just continues on the next. The
# loop is additionally bounded by the graph's credit balance (checked
# before each pass) and stops early when a pass maps nothing new (the
# remaining elements are unmappable by the operator). The cap is a final
# safety net so a fast-failing pass can't spin indefinitely.
MAX_MAPPING_PASSES = 6

# Compiled once: deterministic CoA-name → rs-gaap qname overrides for
# synthesized-detail concepts the AI tends to collapse into the net parent.
_NAME_PATTERN_OVERRIDES: tuple[tuple[re.Pattern[str], str], ...] = tuple(
  (re.compile(pat, re.IGNORECASE), qname)
  for pat, qname in RS_GAAP_NAME_PATTERN_OVERRIDES
)


def _deterministic_rs_gaap_override(coa_elem: dict) -> str | None:
  """Return a forced rs-gaap qname when the CoA element's name/code is an
  unambiguous synthesized-detail concept (e.g. accumulated depreciation),
  else None. The name is a stronger signal than the AI's semantic match,
  which otherwise collapses the contra into the net parent."""
  text = f"{coa_elem.get('name') or ''} {coa_elem.get('code') or ''}"
  for pattern, qname in _NAME_PATTERN_OVERRIDES:
    if pattern.search(text):
      return qname
  return None


@register_operator("mapping")
class MappingOperator(Operator):
  """Autonomous CoA → rs-gaap mapping via Bedrock AI and MCP tools."""

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
    """Bounded, credit-constrained mapping loop.

    Re-invokes the single-pass mapper until the CoA is fully mapped, a
    pass makes no further progress (remaining elements are unmappable by
    the operator), the graph runs out of credits, or the pass cap is hit
    — whichever comes first. Each pass only touches still-unmapped
    elements and persists each mapping as it goes, so the loop never
    re-maps confirmed elements and a pass interrupted by the worker
    timeout simply resumes on the next run.
    """
    mapped_total = 0
    flagged_total = 0
    last_skipped = 0
    coverage_percent = 0.0
    passes = 0
    stop_reason = "pass_cap_reached"

    for attempt in range(1, MAX_MAPPING_PASSES + 1):
      # Stop before starting another (paid) pass if cancelled.
      # ``_run_single_pass`` also checks mid-pass, but gating here avoids
      # kicking off a fresh pass at all.
      if await ctx.progress.is_cancelled():
        stop_reason = "cancelled"
        break
      # Credit pre-check — the graph's own balance bounds the loop. AI
      # credits are consumed post-call (and lookup failures are swallowed
      # downstream), so this is the only place spend is gated before a
      # pass rather than after it. The check does a sync DB read, so run it
      # off the event loop.
      if not await asyncio.to_thread(self._has_credit_budget, ctx):
        stop_reason = "insufficient_credits"
        break

      md = (await self._run_single_pass(ctx)).metadata
      passes = attempt
      coverage_percent = md.get("coverage_percent", coverage_percent)
      pass_mapped = md.get("mapped", 0)
      pass_flagged = md.get("flagged", 0)
      # mapped + flagged both persist an association, so they leave the
      # unmapped set and are never recounted across passes — safe to sum.
      mapped_total += pass_mapped
      flagged_total += pass_flagged
      # skipped elements get no association and are re-attempted next pass,
      # so reflect the LAST pass only rather than summing (avoids double count).
      last_skipped = md.get("skipped", 0)

      if coverage_percent >= 100:
        stop_reason = "complete"
        break
      # A pass that confirmed/flagged nothing new means the rest are
      # unmappable by the operator — stop instead of paying for the same
      # no-op pass every iteration.
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
    """Whether the graph has a positive credit balance for another pass.

    Fails open on lookup error — the ``MAX_MAPPING_PASSES`` cap still
    bounds worst-case spend, so a transient platform-DB hiccup shouldn't
    strand a mapping run.
    """
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
    processed = 0

    # 2. Group by (EFS trait, liquidity) for candidate lookup. Liquidity
    # (current/noncurrent) narrows asset/liability candidates to the right
    # balance-sheet section; it is None for equity/revenue/expense and for
    # accounts that never received a liquidity trait (→ EFS-only candidates,
    # backward compatible). Elements without an EFS trait can't be narrowed
    # at all — collect them as ``unclassified`` and surface them to the
    # caller rather than silently folding them into ``skipped``.
    unclassified: list[dict] = []
    by_group: dict[tuple[str, str | None], list[dict]] = defaultdict(list)
    for elem in elements:
      cls = elem.get("trait")
      if cls is None:
        unclassified.append(elem)
        continue
      by_group[(cls, elem.get("liquidity"))].append(elem)

    # Build an id→element lookup for the deterministic-override path.
    elem_by_id: dict[str, dict] = {e["id"]: e for e in elements}

    # 3. Get candidates per (trait, liquidity) via suggest-mapping tool.
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

    # 4. Process elements in batches per (trait, liquidity) group. Each
    # confirmed / flagged mapping is persisted to coa_mapping as
    # association_type='mapping' — the CoA → rs-gaap arc that fact_grid,
    # trial balance, and coverage readers all consume (the rs-gaap-anchored
    # model: the FAC view is derived from this via the fac-to-rs-gaap
    # bridge, never stored per-tenant).

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

          # Dedupe by element_id — keep the highest-confidence pick per
          # element. Claude occasionally returns the same element_id
          # twice within a batch (or in a duplicated wrapping array
          # that the tolerant parser correctly recovers). Without this,
          # ``confirmed_fac`` would carry duplicates that re-fire the
          # rs-gaap refinement AI call N times for the same CoA → same
          # equivalence-arc insert → an N-fold loop of duplicate-key
          # warnings until the worker timeout fires.
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

          # Account for elements Bedrock dropped from the response
          # entirely. Without this they disappear from the counters and
          # the user sees no signal that an element needs attention —
          # this was how Notes Payable silently went unmapped on the
          # demo graph and stranded $25k of QB-balanced credit.
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

            # Deterministic override for unambiguous synthesized-detail
            # accounts (e.g. "Accumulated Depreciation") that the AI tends
            # to collapse into the net parent (PP&E Net). The account name
            # is a stronger signal than the semantic match; resolve the
            # rs-gaap qname directly, independent of the candidate set.
            override_qname = _deterministic_rs_gaap_override(
              elem_by_id[m["element_id"]]
            )
            if override_qname:
              override_id = await self._resolve_qname_to_id(ctx, override_qname)
              if override_id:
                target = override_id
                # Guarantee the override auto-approves regardless of how the
                # threshold is tuned — the deterministic name match is a
                # stronger signal than any AI confidence score.
                confidence = max(confidence, CONFIDENCE_AUTO_APPROVE)

            if target and confidence >= CONFIDENCE_AUTO_APPROVE:
              mapped += 1
            elif target and confidence >= CONFIDENCE_MIN_MAP:
              flagged += 1
            else:
              skipped += 1
              continue

            # Persist the CoA → rs-gaap arc as the primary mapping target.
            try:
              await create_tool.execute(
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

    # Surface accounts that carry no EFS trait — they can't be narrowed or
    # mapped until classified. Distinct from ``skipped`` (which the agent
    # could classify but didn't auto-approve); these need a classification
    # upstream (QB AccountType gap, or a manual element created trait-less).
    content = (
      f"Mapped {mapped} elements, flagged {flagged} for review, skipped {skipped}"
    )
    if unclassified:
      content += (
        f"; {len(unclassified)} unclassified (no EFS trait — needs classification)"
      )

    return OperatorResult(
      content=content,
      metadata={
        "mapped": mapped,
        "flagged": flagged,
        "skipped": skipped,
        "unclassified": len(unclassified),
        "unclassified_elements": [
          {"id": e["id"], "name": e.get("name")} for e in unclassified[:50]
        ],
        "coverage_percent": coverage_percent,
      },
    )

  async def _resolve_qname_to_id(self, ctx: OperatorContext, qname: str) -> str | None:
    """Resolve an rs-gaap qname → element_id within the operator's tenant
    schema.

    Backs the deterministic name-pattern override (e.g. "Accumulated
    Depreciation" → ``rs-gaap:AccumulatedDepreciation…``): the override
    names a target qname directly, independent of the AI candidate set, so
    it has to be resolved to the tenant's element_id before persisting the
    arc. Cached on the operator instance for the duration of a single
    ``run()`` so repeated overrides don't re-query for the same qname.
    Returns ``None`` when the qname isn't seeded — surfaces missing taxonomy
    data instead of silently falling through.
    """
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
    """Classify a batch of CoA elements against rs-gaap candidates.

    ``candidates`` is the EFS- (+ liquidity-) narrowed rs-gaap set from
    ``suggest-mapping`` — a tight, section-correct slate rather than the
    full ~2,000 rs-gaap variants. The orchestrator persists each accepted
    result as a CoA → rs-gaap arc with ``association_type='mapping'`` — the
    primary rollup target the renderer consumes. The FAC view is derived
    from that arc via the fac-to-rs-gaap bridge, not stored separately.
    """
    prompt = build_mapping_prompt(elements, candidates)

    # 8000 tokens accommodates a full BATCH_SIZE=10 response with verbose
    # `reasoning` fields. The previous 4000 ceiling truncated liability
    # batches mid-string ("Unterminated string starting at: line 241")
    # so the JSON parser raised and the entire batch was dropped — every
    # liability CoA fell to "skipped" and the BS imbalance traced back
    # to Notes Payable being unmapped (carrying a $25k QB credit).
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
    """Parse Bedrock JSON response into mapping results.

    Tolerant of three failure modes Claude exhibits in the wild:

    1. Markdown fences (```json … ```) — stripped before parsing.
    2. Multiple JSON values back-to-back (one array per element, or an
       array followed by a trailing object). We scan with
       ``json.JSONDecoder.raw_decode`` and accumulate every top-level
       value, flattening lists and unwrapping single objects. This is
       why ``json.loads`` was failing with "Extra data" on the asset
       and liability batches — Claude was emitting one mapping object
       per line instead of a single wrapping array.
    3. Lines of explanatory prose between values — skipped by advancing
       past whitespace and any non-`{`/`[` prefix between values.
    """
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
    """Strip a single ```json … ``` (or bare ```) wrapper if present.

    Claude follows the prompt's "Respond ONLY with the JSON" rule
    inconsistently — sometimes the response arrives wrapped in a
    fenced code block, sometimes not, and very occasionally the
    fences land mid-stream after a leading explanation. We only
    strip when fences bracket the entire payload.
    """
    if not text.startswith("```"):
      return text
    after_open = text.split("\n", 1)[1] if "\n" in text else text[3:]
    if after_open.endswith("```"):
      after_open = after_open[:-3]
    return after_open.strip()

  @staticmethod
  def _parse_concatenated_json(text: str) -> list:
    """Decode every top-level JSON value in ``text`` and flatten lists.

    Uses ``json.JSONDecoder().raw_decode`` to walk the buffer one
    value at a time so a response like
    ``[{...}, {...}]\\n{...}`` or ``{...}\\n{...}\\n{...}``
    yields all of the contained objects rather than failing the whole
    parse on the first leftover byte.

    When raw_decode raises mid-stream (truncated array, unterminated
    string from a max_tokens cutoff, malformed trailing object), we
    keep whatever objects we already extracted instead of dropping
    the whole batch. Partial recovery is the right behavior here: a
    Bedrock truncation that yields 6 of 7 mappings is still better
    than 0, and the orchestrator's per-element loop tolerates a
    short result list (missing element_ids fall through to "skipped"
    rather than corrupt the rest).

    A list value parses fine but its trailing `]` may be missing on
    truncation; in that case we attempt an inner-element scan over
    its body so the partial array still contributes whatever complete
    objects it has.
    """
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
        # Truncated value at this position. If we're inside a `[...]`
        # whose contents are object literals, scan forward and
        # recover each complete `{...}` we can find — typical
        # max_tokens cutoff shape. Otherwise stop here with whatever
        # we've accumulated.
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
