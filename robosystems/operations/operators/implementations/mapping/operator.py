"""MappingOperator — autonomous CoA → FAC mapping.

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
  FAC_TO_RS_GAAP_FALLBACK,
  FALLBACK_CONFIDENCE,
  RS_GAAP_SUBTOTAL_DENYLIST,
)
from robosystems.operations.operators.implementations.mapping.prompt import (
  MAPPING_SYSTEM_PROMPT,
  RS_GAAP_REFINEMENT_SYSTEM_PROMPT,
  build_mapping_prompt,
  build_rs_gaap_refinement_prompt,
)
from robosystems.operations.operators.operator_context import OperatorContext
from robosystems.operations.operators.operator_registry import register_operator

logger = logging.getLogger(__name__)

# Confidence thresholds
CONFIDENCE_AUTO_APPROVE = 0.90
CONFIDENCE_MIN_MAP = 0.70

# Max elements per Bedrock call (batched by classification)
BATCH_SIZE = 10


@register_operator("mapping")
class MappingOperator(Operator):
  """Autonomous CoA → FAC mapping via Bedrock AI and MCP tools."""

  spec = OperatorSpec(
    name="Mapping Operator",
    description="Autonomous Chart of Accounts to FAC (Fundamental Accounting Concepts) mapping",
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

    return OperatorResult(
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
    ctx: OperatorContext,
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
      operation_description="CoA to FAC mapping",
    )

    return self._parse_response(response.content, elements)

  async def _refine_to_rs_gaap(
    self,
    ctx: OperatorContext,
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
    # Group CoA elements by their FAC target — deduped on (coa_id,
    # fac_id) so even if an upstream caller passes the same pair
    # twice, the refinement runs once per element. The operator's own
    # FAC pass dedupes too; this is belt-and-suspenders against the
    # 5-minute insert loop we hit when Claude returned a duplicated
    # batch and pre-dedupe ``confirmed_fac`` carried each pair N times.
    by_fac: dict[str, dict[str, dict]] = defaultdict(dict)
    for coa_elem, fac_id in confirmed_fac:
      by_fac[fac_id].setdefault(coa_elem["id"], coa_elem)
    by_fac_list: dict[str, list[dict]] = {
      fid: list(elems.values()) for fid, elems in by_fac.items()
    }

    # Pre-fetch FAC qnames so the fallback path can route each CoA to
    # its per-FAC Other bucket even when ``expand_to_rs_gaap_candidates``
    # returns an error (e.g. the FAC concept's wide equivalence set was
    # entirely filtered out by the presentation-set + denylist filters).
    # Without this, every CoA in such a group silently misses its
    # rs-gaap mapping.
    fac_qnames = await self._fetch_fac_qnames(ctx, list(by_fac_list.keys()))

    for fac_id, coa_elements in by_fac_list.items():
      if await ctx.progress.is_cancelled():
        break

      # Expand FAC → rs-gaap parent + type-subtype children
      expand_result = await expand_tool.execute({"fac_element_id": fac_id})
      expand_failed = "error" in expand_result
      if expand_failed:
        logger.debug(f"No rs-gaap expansion for {fac_id}: {expand_result['error']}")
        rs_gaap_parent = None
        candidates: list[dict] = []
        fac_qname = fac_qnames.get(fac_id, fac_id)
      else:
        rs_gaap_parent = expand_result["rs_gaap_parent"]
        candidates = expand_result["candidates"]
        fac_qname = expand_result.get("fac_qname", fac_id)

      # One AI call per CoA element in this FAC group. Skip the AI
      # entirely when no candidates exist (expand failed) — the
      # fallback path below routes the CoA to its per-FAC Other bucket
      # without burning a Bedrock call.
      for coa_elem in coa_elements:
        rs_gaap_id: str | None = None
        rs_gaap_conf: float = 0.0
        if candidates:
          try:
            prompt = build_rs_gaap_refinement_prompt(
              coa_elem, fac_qname, rs_gaap_parent, candidates
            )
            response = await ctx.ai.create_message(
              messages=[AIMessage(role="user", content=prompt)],
              system=RS_GAAP_REFINEMENT_SYSTEM_PROMPT,
              max_tokens=500,
              temperature=0.2,
              operator_type="mapping",
              operation_description="CoA to rs-gaap refinement",
            )

            result = self._parse_rs_gaap_response(response.content)
            # Use a lower threshold here — the FAC pass already confirmed the
            # semantic bucket; the refinement is just picking the specific tag.
            if (
              result
              and result.get("rs_gaap_id")
              and result.get("confidence", 0) >= 0.50
              # Belt-and-suspenders: even if the prompt instructed
              # "no rollups", filter the AI's pick against the denylist
              # in case it ignored the instruction.
              and result.get("rs_gaap_qname") not in RS_GAAP_SUBTOTAL_DENYLIST
            ):
              rs_gaap_id = result["rs_gaap_id"]
              rs_gaap_conf = result["confidence"]
            else:
              logger.info(
                "rs-gaap refinement REJECTED for %s (FAC=%s): result=%s — falling back",
                coa_elem.get("name", coa_elem["id"]),
                fac_qname,
                result,
              )

          except Exception as e:
            logger.warning(f"rs-gaap refinement failed for {coa_elem['id']}: {e}")

        # Fallback chain when the AI returned nothing usable:
        #   1. The narrow-case parent equivalent (if not denylisted).
        #      Already filtered upstream in expand_to_rs_gaap_candidates,
        #      but the explicit guard here is belt-and-suspenders for
        #      callers that bypass the filter.
        #   2. The per-FAC "Other" bucket (FAC_TO_RS_GAAP_FALLBACK) at
        #      a low ``FALLBACK_CONFIDENCE`` so the user sees it as a
        #      placeholder mapping in the CoA UI and can correct it.
        if not rs_gaap_id and rs_gaap_parent:
          parent_qname = rs_gaap_parent.get("qname", "")
          if parent_qname not in RS_GAAP_SUBTOTAL_DENYLIST:
            rs_gaap_id = rs_gaap_parent["id"]
            rs_gaap_conf = 0.60

        if not rs_gaap_id:
          fallback_qname = FAC_TO_RS_GAAP_FALLBACK.get(fac_qname)
          if fallback_qname:
            fallback_id = await self._resolve_qname_to_id(ctx, fallback_qname)
            if fallback_id:
              rs_gaap_id = fallback_id
              rs_gaap_conf = FALLBACK_CONFIDENCE
              logger.info(
                "rs-gaap fallback for %s (FAC=%s): %s @ confidence=%.2f",
                coa_elem.get("qname", coa_elem["id"]),
                fac_qname,
                fallback_qname,
                FALLBACK_CONFIDENCE,
              )

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

  async def _fetch_fac_qnames(
    self, ctx: OperatorContext, fac_ids: list[str]
  ) -> dict[str, str]:
    """Bulk-resolve a list of FAC element ids → qnames in one query.

    Used by ``_refine_to_rs_gaap`` so the per-FAC fallback (look up
    ``FAC_TO_RS_GAAP_FALLBACK[fac_qname]``) works even when
    ``expand_to_rs_gaap_candidates`` returns an error and we don't have
    a ``fac_qname`` from the expand result.
    """
    if not fac_ids:
      return {}

    from sqlalchemy import text

    from robosystems.db.extensions import extensions_session

    with extensions_session(ctx.graph_id) as session:
      rows = session.execute(
        text("SELECT id, qname FROM elements WHERE id = ANY(:ids)"),
        {"ids": fac_ids},
      ).fetchall()
    return {r.id: r.qname for r in rows}

  async def _resolve_qname_to_id(self, ctx: OperatorContext, qname: str) -> str | None:
    """Resolve an rs-gaap qname → element_id within the operator's tenant
    schema.

    Cached on the operator instance for the duration of a single ``run()``
    call so the fallback path doesn't re-query for every CoA element
    that lands on the same Other bucket. Returns ``None`` when the
    qname isn't seeded — surfaces missing taxonomy data instead of
    silently falling through.
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
        "rs-gaap fallback target %r not found in graph %s — fallback skipped",
        qname,
        ctx.graph_id,
      )
    return elem_id

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
