"""
Semantic enrichment for XBRL elements, labels, and structures.

Provides embedding-based canonical concept matching. The fastembed model
is loaded lazily to avoid import-time overhead (critical because
adapters/__init__.py eagerly imports all adapter code).
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

import numpy as np

from robosystems.logger import logger

if TYPE_CHECKING:
  from robosystems.adapters.sec.taxonomy.concepts import CanonicalConcept

# ---------------------------------------------------------------------------
# Utility functions (no heavy imports, safe at module level)
# ---------------------------------------------------------------------------


def camel_case_to_words(name: str) -> str:
  """Convert CamelCase XBRL element name to human-readable words.

  >>> camel_case_to_words("RevenueFromContractWithCustomerExcludingAssessedTax")
  'Revenue From Contract With Customer Excluding Assessed Tax'
  """
  # Insert space before each uppercase letter that follows a lowercase letter
  result = re.sub(r"([a-z])([A-Z])", r"\1 \2", name)
  # Insert space before a sequence of uppercase followed by lowercase (e.g., "HTMLParser" -> "HTML Parser")
  result = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1 \2", result)
  return result


def compose_element_text(parsed_name: str, element_data: dict) -> str:
  """Compose a text representation of an element for embedding.

  Combines the parsed name with metadata fields to produce a richer
  text representation that captures the element's financial semantics.
  """
  parts = [f"Name: {parsed_name}"]
  balance = element_data.get("balance")
  if balance:
    parts.append(f"Balance: {balance}")
  period_type = element_data.get("period_type")
  if period_type:
    parts.append(f"Period: {period_type}")
  classification = element_data.get("classification")
  if classification:
    parts.append(f"Classification: {classification}")
  return " | ".join(parts)


def compose_structure_text(name: str | None, definition: str | None) -> str:
  """Compose a text representation of a structure for embedding."""
  parts = []
  if name:
    parts.append(name)
  if definition:
    parts.append(definition)
  return " | ".join(parts) if parts else ""


def parse_structure_definition(
  definition: str,
) -> tuple[str | None, str | None, str | None]:
  """Parse an XBRL structure definition string into (number, type, name).

  Handles various formats:
    "0001001 - Statement - CONSOLIDATED BALANCE SHEETS"
    "0001003 - Disclosure - Organization"
    "995410 - Disclosure - Disclosure - Supplemental Balance Sheet ..."  (doubled type)
    "0001001 - Statement - CONSOLIDATED BALANCE SHEETS [Parenthetical]"
    ""  (empty)

  Returns:
      (number, type, name) — any may be None if parsing fails.
  """
  if not definition or not definition.strip():
    return (None, None, None)

  # Split on " - " (space-dash-space) to avoid splitting on hyphens inside names
  parts = definition.split(" - ")
  if len(parts) < 3:
    # Can't parse — return definition as name
    return (None, None, definition.strip() or None)

  number = parts[0].strip() or None
  type_part = parts[1].strip() or None

  # Everything after the first two splits is the name.
  # If the type is repeated (doubled), skip it.
  remaining = parts[2:]
  if remaining and type_part and remaining[0].strip() == type_part:
    remaining = remaining[1:]

  name = " - ".join(remaining).strip() or None
  return (number, type_part, name)


def classify_structure_heuristic(
  name: str | None,
  definition: str | None,
  structure_type: str | None = None,
) -> tuple[str | None, float]:
  """Classify a structure into a canonical statement type using keyword heuristics.

  Only classifies structures with type "Statement" (or unknown type).
  Disclosure sections like "Balance Sheet Components (Details)" are skipped
  to avoid false positives.

  Returns:
      (canonical_type, confidence) — canonical_type may be None if unclassifiable.
      Confidence is capped at 0.85 for heuristic matches.
  """
  # Only classify Statement structures — Disclosures are not primary financial statements
  if structure_type and structure_type.lower() not in ("statement", ""):
    return (None, 0.0)

  text = ""
  if name:
    text += name.lower()
  if definition:
    text += " " + definition.lower()
  text = text.strip()

  if not text:
    return (None, 0.0)

  # Check for parenthetical variants (lower confidence)
  is_parenthetical = "[parenthetical]" in text or "(parenthetical)" in text

  # Income statement keywords
  income_keywords = [
    "income",
    "operations",
    "earnings",
    "profit and loss",
    "profit or loss",
  ]
  # Balance sheet keywords
  balance_keywords = [
    "balance sheet",
    "financial position",
    "financial condition",
  ]
  # Cash flow keywords
  cash_flow_keywords = [
    "cash flow",
    "cash flows",
  ]
  # Equity keywords
  equity_keywords = [
    "stockholders equity",
    "stockholders' equity",
    "shareholders equity",
    "shareholders' equity",
    "changes in equity",
  ]
  # Comprehensive income keywords
  comprehensive_keywords = [
    "comprehensive income",
    "comprehensive loss",
  ]

  # Check comprehensive income BEFORE income (more specific first)
  for kw in comprehensive_keywords:
    if kw in text:
      conf = 0.75 if is_parenthetical else 0.85
      return ("comprehensive_income", conf)

  for kw in balance_keywords:
    if kw in text:
      conf = 0.75 if is_parenthetical else 0.85
      return ("balance_sheet", conf)

  for kw in cash_flow_keywords:
    if kw in text:
      conf = 0.75 if is_parenthetical else 0.85
      return ("cash_flow_statement", conf)

  for kw in equity_keywords:
    if kw in text:
      conf = 0.75 if is_parenthetical else 0.85
      return ("equity_statement", conf)

  # Income statement checked last (broad keywords)
  # Only match if "statement" context is present to avoid matching disclosures about income
  if "statement" in text:
    for kw in income_keywords:
      if kw in text:
        conf = 0.75 if is_parenthetical else 0.85
        return ("income_statement", conf)

  return (None, 0.0)


# ---------------------------------------------------------------------------
# SemanticEnricher
# ---------------------------------------------------------------------------


class SemanticEnricher:
  """Embedding-based canonical concept matching for XBRL elements and structures.

  The fastembed model is loaded lazily on first use. This is critical because
  ``adapters/__init__.py`` eagerly imports all adapter code — loading a 130 MB
  model at import time would be unacceptable.
  """

  def __init__(self) -> None:
    self._model = None
    self._element_taxonomy = None
    self._structure_taxonomy = None

  # -- Lazy model loading ---------------------------------------------------

  @property
  def model(self):
    """Lazy-load the fastembed model."""
    if self._model is None:
      from fastembed import TextEmbedding

      logger.info("Loading fastembed model BAAI/bge-small-en-v1.5")
      self._model = TextEmbedding("BAAI/bge-small-en-v1.5")
    return self._model

  @property
  def element_taxonomy(self):
    if self._element_taxonomy is None:
      from robosystems.adapters.sec.taxonomy import get_element_taxonomy

      self._element_taxonomy = get_element_taxonomy(model=self.model)
    return self._element_taxonomy

  @property
  def structure_taxonomy(self):
    if self._structure_taxonomy is None:
      from robosystems.adapters.sec.taxonomy import get_structure_taxonomy

      self._structure_taxonomy = get_structure_taxonomy(model=self.model)
    return self._structure_taxonomy

  # -- Embedding ------------------------------------------------------------

  def embed_batch(self, texts: list[str]) -> list[list[float]]:
    """Embed a batch of texts, returning list of float vectors."""
    embeddings = list(self.model.embed(texts))
    return [emb.tolist() for emb in embeddings]

  # -- Canonical matching (elements) ----------------------------------------

  def match_canonical(
    self, embedding: list[float], element_metadata: dict
  ) -> tuple[str | None, float]:
    """Match an element embedding to the best canonical concept.

    Algorithm:
    1. Cosine similarity between embedding and all taxonomy embeddings
    2. Metadata boost: +0.10 for matching period_type, +0.10 for matching balance
    3. Known-element override: floor at 0.95 if qname in expected_elements
    4. Threshold: minimum 0.70 confidence

    Returns:
        (concept_id, confidence) or (None, 0.0) if below threshold.
    """
    taxonomy = self.element_taxonomy
    if not taxonomy:
      return (None, 0.0)

    qname = element_metadata.get("qname", "")
    elem_period_type = element_metadata.get("period_type", "")
    elem_balance = element_metadata.get("balance", "")

    query_vec = np.array(embedding, dtype=np.float32)
    query_norm = np.linalg.norm(query_vec)
    if query_norm == 0:
      return (None, 0.0)

    best_id = None
    best_score = 0.0

    for concept in taxonomy:
      if concept.embedding is None:
        continue

      # Known-element shortcut
      if qname and qname in concept.expected_elements:
        return (concept.id, 0.95)

      tax_vec = np.array(concept.embedding, dtype=np.float32)
      tax_norm = np.linalg.norm(tax_vec)
      if tax_norm == 0:
        continue

      cos_sim = float(np.dot(query_vec, tax_vec) / (query_norm * tax_norm))

      # Metadata boosts
      if elem_period_type and concept.period_type == elem_period_type:
        cos_sim += 0.10
      if elem_balance and concept.balance == elem_balance:
        cos_sim += 0.10

      if cos_sim > best_score:
        best_score = cos_sim
        best_id = concept.id

    if best_score >= 0.80:
      return (best_id, round(min(best_score, 1.0), 4))
    return (None, 0.0)

  # -- Canonical matching (query-time, for MCP tools) -----------------------

  def match_canonical_from_query(
    self, query_embedding: list[float]
  ) -> CanonicalConcept | None:
    """Match a free-text query embedding to the best element taxonomy concept.

    Used by MCP tools at query time (no metadata boosts).
    """

    taxonomy = self.element_taxonomy
    if not taxonomy:
      return None

    query_vec = np.array(query_embedding, dtype=np.float32)
    query_norm = np.linalg.norm(query_vec)
    if query_norm == 0:
      return None

    best: CanonicalConcept | None = None
    best_score = 0.0

    for concept in taxonomy:
      if concept.embedding is None:
        continue
      tax_vec = np.array(concept.embedding, dtype=np.float32)
      tax_norm = np.linalg.norm(tax_vec)
      if tax_norm == 0:
        continue
      cos_sim = float(np.dot(query_vec, tax_vec) / (query_norm * tax_norm))
      if cos_sim > best_score:
        best_score = cos_sim
        best = concept

    if best_score >= 0.50:
      return best
    return None

  def match_structure_canonical(
    self, embedding: list[float]
  ) -> tuple[str | None, float]:
    """Match a structure embedding to the best canonical structure type.

    Returns:
        (canonical_type, confidence) or (None, 0.0) if below threshold.
    """
    taxonomy = self.structure_taxonomy
    if not taxonomy:
      return (None, 0.0)

    query_vec = np.array(embedding, dtype=np.float32)
    query_norm = np.linalg.norm(query_vec)
    if query_norm == 0:
      return (None, 0.0)

    best_id = None
    best_score = 0.0

    for concept in taxonomy:
      if concept.embedding is None:
        continue
      tax_vec = np.array(concept.embedding, dtype=np.float32)
      tax_norm = np.linalg.norm(tax_vec)
      if tax_norm == 0:
        continue
      cos_sim = float(np.dot(query_vec, tax_vec) / (query_norm * tax_norm))
      if cos_sim > best_score:
        best_score = cos_sim
        best_id = concept.id

    if best_score >= 0.70:
      return (best_id, round(min(best_score, 1.0), 4))
    return (None, 0.0)
