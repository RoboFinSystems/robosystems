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
  """Force an rs-gaap qname when the element's name/code is unambiguous.

  Applies only to synthesized-detail concepts such as accumulated
  depreciation, where the account name is a stronger signal than the model's
  semantic match — which otherwise collapses the contra into the net parent.
  """
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
    processed = 0

    # Group by (EFS trait, liquidity) for candidate lookup. Liquidity
    # (current/noncurrent) narrows asset/liability candidates to the right
    # balance-sheet section; it is None for equity/revenue/expense and for
    # accounts carrying no liquidity trait, which fall back to EFS-only
    # candidates. Elements without an EFS trait can't be narrowed at all —
    # collect them as ``unclassified`` and surface them to the caller rather
    # than silently folding them into ``skipped``.
    unclassified: list[dict] = []
    by_group: dict[tuple[str, str | None], list[dict]] = defaultdict(list)
    for elem in elements:
      cls = elem.get("trait")
      if cls is None:
        unclassified.append(elem)
        continue
      by_group[(cls, elem.get("liquidity"))].append(elem)

    # Backs the deterministic-override path, which needs the element's name.
    elem_by_id: dict[str, dict] = {e["id"]: e for e in elements}

    # One suggest-mapping call per group, not per element: candidates depend
    # only on (trait, liquidity), so every element in a group shares a slate.
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

    # Each confirmed / flagged mapping is persisted to coa_mapping as
    # association_type='mapping' — the CoA → rs-gaap arc that fact_grid, trial
    # balance, and coverage readers all consume. The FAC view is derived from
    # this arc via the fac-to-rs-gaap bridge, never stored per-tenant.
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

          # Dedupe by element_id, keeping the highest-confidence pick. The
          # model occasionally returns the same element twice in a batch (or
          # in a duplicated wrapping array the tolerant parser recovers), and
          # each duplicate would re-run the write path for the same CoA arc.
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

          # Count elements the model dropped from its response entirely.
          # Uncounted, they vanish from the totals and nothing tells the user
          # an account still needs attention — an unmapped account strands its
          # balance out of the financial statements.
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
    """Ask the model to match a batch of CoA elements to rs-gaap candidates.

    ``candidates`` is the EFS- (+ liquidity-) narrowed set from
    ``suggest-mapping``: a tight, section-correct slate rather than the full
    ~2,000 rs-gaap variants. Returns one result per element on the happy path,
    but callers must tolerate a short list — see `_parse_response`.
    """
    prompt = build_mapping_prompt(elements, candidates)

    # Sized to fit a full BATCH_SIZE=10 response including verbose `reasoning`
    # fields. A tighter ceiling truncates the JSON mid-string, and a batch that
    # only partially parses silently drops those accounts into "skipped" —
    # unmapped accounts strand real balances out of the statements.
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
    """Parse the model's JSON into mapping results, or return zero-confidence
    placeholders for the whole batch if nothing can be recovered.

    Deliberately tolerant of three shapes the model produces despite the
    prompt, each of which plain ``json.loads`` rejects outright:

    1. Markdown fences (```json … ```) — stripped before parsing.
    2. Several JSON values back to back (one object per line instead of one
       wrapping array, or an array followed by a stray object). Scanned with
       ``json.JSONDecoder.raw_decode``, accumulating every top-level value.
    3. Explanatory prose between values — skipped by advancing past anything
       that isn't `{` or `[`.
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

    The model follows the prompt's "Respond ONLY with the JSON" rule
    inconsistently. Only a fence that brackets the entire payload is stripped;
    fences appearing mid-stream after a leading explanation are left for the
    tolerant scan in `_parse_concatenated_json`.
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

    Walks the buffer one value at a time with ``json.JSONDecoder().raw_decode``
    so ``[{...}, {...}]\\n{...}`` or ``{...}\\n{...}\\n{...}`` yields all the
    contained objects instead of failing on the first leftover byte.

    On a mid-stream decode error (truncated array, unterminated string from a
    max_tokens cutoff, malformed trailing object) whatever was already
    extracted is kept. Partial recovery is correct here: 6 of 7 mappings beats
    0, and the caller tolerates a short list — missing element_ids fall through
    to "skipped" rather than corrupting the rest. A truncated array is scanned
    for its complete inner objects for the same reason.
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
        # Truncated value. Inside a `[...]` of object literals — the typical
        # max_tokens cutoff shape — scan forward for each complete `{...}`.
        # Otherwise stop with whatever has accumulated.
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
