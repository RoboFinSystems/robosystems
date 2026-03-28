"""MappingAgent task handler — autonomous CoA → US GAAP mapping.

Iterates through unmapped Chart of Accounts elements, calls Bedrock to
classify and match each to a US GAAP reporting concept, and writes
confirmed mappings via the API. Progress is streamed via SSE.

Uses the same REST API endpoints as the frontend — the worker is just
another API client, calling through the internal ALB.
"""

import json
import logging
from collections import defaultdict
from typing import Any

from robosystems.operations.agents.ai_client import AIClient, AIMessage
from robosystems.worker.api_client import WorkerAPIClient
from robosystems.worker.tasks import register_task
from robosystems.worker.tasks.base import BaseTask
from robosystems.worker.tasks.mapping_prompt import (
  MAPPING_SYSTEM_PROMPT,
  build_mapping_prompt,
)

logger = logging.getLogger(__name__)

# Confidence thresholds
CONFIDENCE_AUTO_APPROVE = 0.90
CONFIDENCE_MIN_MAP = 0.70

# Max elements per Bedrock call (batched by classification)
BATCH_SIZE = 10


@register_task("agent_mapping")
class AgentMappingTask(BaseTask):
  """Autonomous CoA → GAAP mapping via Bedrock AI."""

  async def execute(self) -> dict[str, Any]:
    mapping_id = self.params["mapping_id"]
    api = WorkerAPIClient(self.graph_id)
    ai_client = AIClient()

    # 1. Get unmapped elements via API
    unmapped_response = await api.get(
      f"/v1/ledger/{self.graph_id}/elements/unmapped",
      params={"mapping_id": mapping_id},
    )
    elements = unmapped_response if isinstance(unmapped_response, list) else []
    total = len(elements)

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

    # 3. Get reporting taxonomy candidates per classification via API
    candidates_by_cls: dict[str, list[dict]] = {}
    for cls in by_classification:
      candidates_response = await api.get(
        f"/v1/ledger/{self.graph_id}/elements",
        params={
          "source": "us-gaap,sfac6",
          "classification": cls,
          "is_abstract": "false",
          "limit": "200",
        },
      )
      candidates_by_cls[cls] = (
        candidates_response if isinstance(candidates_response, list) else []
      )

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

      # Process in batches
      for batch_start in range(0, len(cls_elements), BATCH_SIZE):
        if await self.is_cancelled():
          break

        batch = cls_elements[batch_start : batch_start + BATCH_SIZE]

        # Call Bedrock to map this batch
        try:
          mappings, credits = await self._map_batch(
            ai_client, batch, candidates, mapping_id, api
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

    # 5. Get final coverage via API
    try:
      coverage = await api.get(
        f"/v1/ledger/{self.graph_id}/mappings/{mapping_id}/coverage"
      )
      coverage_percent = coverage.get("coverage_percent", 0)
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
    api: WorkerAPIClient,
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

    # Track credits
    credits = await self._consume_credits(
      response.input_tokens,
      response.output_tokens,
      response.model,
    )

    # Parse AI response
    mappings = self._parse_response(response.content, elements)

    # Write confirmed mappings via API
    for m in mappings:
      if m.get("target_id") and m["confidence"] >= CONFIDENCE_MIN_MAP:
        try:
          await api.post(
            f"/v1/ledger/{self.graph_id}/mappings/{mapping_id}/associations",
            json={
              "from_element_id": m["element_id"],
              "to_element_id": m["target_id"],
              "association_type": "mapping",
              "confidence": m["confidence"],
              "suggested_by": "bedrock",
            },
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

    Returns list of dicts with element_id, target_id, target_qname, confidence.
    Falls back gracefully on malformed responses.
    """
    try:
      # Strip markdown code fences if present
      text = content.strip()
      if text.startswith("```"):
        text = text.split("\n", 1)[1] if "\n" in text else text[3:]
        if text.endswith("```"):
          text = text[:-3]
        text = text.strip()

      mappings = json.loads(text)
      if not isinstance(mappings, list):
        mappings = [mappings]

      # Validate each mapping has required fields
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
      # Return all elements as skipped
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
