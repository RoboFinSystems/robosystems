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

  _UNSET = object()

  def __init__(self) -> None:
    self._model = None
    self._element_taxonomy = None
    self._structure_taxonomy = None
    self._element_knowledge = self._UNSET
    self._structure_profiles = self._UNSET
    self._structure_consensus = self._UNSET

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

  # -- Artifact lazy loading (graph-based refinement) -----------------------

  @property
  def element_knowledge(self) -> dict[str, dict] | None:
    """Lazy-load element knowledge artifact from Parquet."""
    if self._element_knowledge is self._UNSET:
      self._element_knowledge = self._load_element_knowledge()
    return self._element_knowledge

  @property
  def structure_profiles(self) -> dict[str, dict[str, float]] | None:
    """Lazy-load structure profiles artifact."""
    if self._structure_profiles is self._UNSET:
      self._structure_profiles = self._load_structure_profiles()
    return self._structure_profiles

  @property
  def structure_consensus(self) -> dict[str, dict] | None:
    """Lazy-load structure consensus artifact."""
    if self._structure_consensus is self._UNSET:
      self._structure_consensus = self._load_structure_consensus()
    return self._structure_consensus

  def _load_element_knowledge(self) -> dict[str, dict] | None:
    """Load element_knowledge.parquet into a qname-keyed dict."""
    try:
      from robosystems.config.storage.shared import get_artifact_path

      path = get_artifact_path("element_knowledge")

      import os

      if not os.path.exists(path):
        return None

      import pyarrow.parquet as pq

      table = pq.read_table(path)
      result = {}

      # Read columnar and transpose to row dicts
      columns = table.to_pydict()
      qnames = columns["qname"]
      for i, qname in enumerate(qnames):
        result[qname] = {
          "primary_statement": columns["primary_statement"][i],
          "bfs_depth": columns["bfs_depth"][i],
          "pagerank": columns["pagerank"][i],
          "core_number": columns["core_number"][i],
          "neighborhood_agreement": columns["neighborhood_agreement"][i],
          "filing_count": columns["filing_count"][i],
        }

      logger.info(f"Loaded element knowledge artifact: {len(result)} elements")
      return result
    except Exception as e:
      logger.debug(f"Element knowledge artifact not available: {e}")
      return None

  def _load_structure_profiles(self) -> dict[str, dict[str, float]] | None:
    """Load structure_profiles.parquet into canonical_type → {qname → frequency}."""
    try:
      from robosystems.config.storage.shared import get_artifact_path

      path = get_artifact_path("structure_profiles")

      import os

      if not os.path.exists(path):
        return None

      import pyarrow.parquet as pq

      table = pq.read_table(path)
      columns = table.to_pydict()

      result: dict[str, dict[str, float]] = {}
      for i, ct in enumerate(columns["canonical_type"]):
        if ct not in result:
          result[ct] = {}
        result[ct][columns["qname"][i]] = columns["frequency"][i]

      logger.info(f"Loaded structure profiles artifact: {len(result)} types")
      return result
    except Exception as e:
      logger.debug(f"Structure profiles artifact not available: {e}")
      return None

  def _load_structure_consensus(self) -> dict[str, dict] | None:
    """Load structure_consensus.parquet into definition_hash-keyed dict."""
    try:
      from robosystems.config.storage.shared import get_artifact_path

      path = get_artifact_path("structure_consensus")

      import os

      if not os.path.exists(path):
        return None

      import pyarrow.parquet as pq

      table = pq.read_table(path)
      columns = table.to_pydict()

      result = {}
      for i, def_hash in enumerate(columns["definition_hash"]):
        result[def_hash] = {
          "canonical_type": columns["canonical_type"][i],
          "consensus_ratio": columns["consensus_ratio"][i],
          "filing_count": columns["filing_count"][i],
        }

      logger.info(f"Loaded structure consensus artifact: {len(result)} entries")
      return result
    except Exception as e:
      logger.debug(f"Structure consensus artifact not available: {e}")
      return None

  # -- Embedding ------------------------------------------------------------

  def embed_batch(self, texts: list[str]) -> list[list[float]]:
    """Embed a batch of texts, returning list of float vectors."""
    embeddings = list(self.model.embed(texts))
    return [emb.tolist() for emb in embeddings]

  # -- Canonical matching (elements) ----------------------------------------

  # Top-N candidate selection parameters
  _CANDIDATE_THRESHOLD = 0.80  # Only consider candidates at or above match threshold
  _MAX_CANDIDATES = 3  # Cap to avoid unnecessary refinement work

  def match_canonical(
    self, embedding: list[float], element_metadata: dict
  ) -> tuple[str | None, float]:
    """Match an element embedding to the best canonical concept.

    Algorithm:
    1. Cosine similarity between embedding and all taxonomy embeddings
    2. Metadata boost: +0.10 for matching period_type, +0.10 for matching balance
    3. Known-element override: floor at 0.95 if qname in expected_elements
    4. Top-N candidates collected above threshold
    5. Concept-aware refinement applied to each candidate (if enabled)
    6. Winner selected after refinement

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

    candidates: list[tuple[float, str, CanonicalConcept]] = []

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

      if cos_sim >= self._CANDIDATE_THRESHOLD:
        candidates.append((cos_sim, concept.id, concept))

    if not candidates:
      return (None, 0.0)

    # Sort descending by score, cap at MAX_CANDIDATES
    candidates.sort(reverse=True)
    candidates = candidates[: self._MAX_CANDIDATES]

    from robosystems.adapters.sec.config import XBRL_GRAPH_REFINEMENT

    if XBRL_GRAPH_REFINEMENT:
      # Refine each candidate with concept-aware signals, pick winner
      refined = [
        (self._refine_element_confidence(score, qname, concept), concept_id)
        for score, concept_id, concept in candidates
      ]
      refined.sort(reverse=True)
      best_score, best_id = refined[0]
    else:
      # No refinement — use raw best
      best_score, best_id = candidates[0][0], candidates[0][1]

    if best_score >= self._CANDIDATE_THRESHOLD:
      return (best_id, round(min(best_score, 1.0), 4))
    return (None, 0.0)

  # Mapping from element knowledge primary_statement to concept category
  _STATEMENT_TO_CATEGORY = {
    "IncomeStatement": "income_statement",
    "BalanceSheet": "balance_sheet",
    "CashFlow": "cash_flow",
  }

  def _refine_element_confidence(
    self, raw_conf: float, qname: str, concept: CanonicalConcept | None = None
  ) -> float:
    """Apply graph-structural refinement to a semantic confidence score.

    If element knowledge artifact is not available, returns raw_conf unchanged.
    When a concept is provided, applies statement-alignment boost/penalty
    before the quadratic penalty.
    """
    ek = self.element_knowledge
    if ek is None:
      return raw_conf

    element_info = ek.get(qname)
    if element_info is None:
      return raw_conf  # Element not in artifact (new/rare element)

    agreement = element_info["neighborhood_agreement"]
    pagerank = element_info["pagerank"]

    # PageRank boost: structurally central elements get up to 15% boost
    pr_boost = 1.0 + 0.15 * (pagerank if pagerank is not None else 0.0)

    base = raw_conf * pr_boost

    # Agreement adjustment
    if agreement is not None and agreement >= 0.8:
      refined = base * (1.0 + 0.10 * agreement)  # Modest boost
    elif agreement is not None and agreement >= 0.5:
      refined = base  # Neutral zone
    elif agreement is not None:
      penalty = 0.6 + 0.4 * agreement  # Heavy penalty
      refined = base * penalty
    else:
      refined = base

    # Statement alignment: compare element's primary_statement to concept category
    if concept is not None:
      primary_stmt = element_info.get("primary_statement")
      if primary_stmt:
        mapped_category = self._STATEMENT_TO_CATEGORY.get(primary_stmt)
        if mapped_category == concept.category:
          refined *= 1.08  # +8% boost for matching statement
        elif mapped_category is not None:
          refined *= 0.88  # -12% penalty for mismatched statement

    # Quadratic penalty below threshold
    threshold = 0.90
    if refined < threshold:
      refined *= (refined / threshold) ** 2.0

    return round(min(1.0, max(0.0, refined)), 4)

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

  # -- Structure refinement ------------------------------------------------

  def refine_structure_confidence(
    self,
    raw_type: str | None,
    raw_conf: float,
    structure_elements: list[str],
    definition_hash: str,
  ) -> tuple[str | None, float]:
    """Refine structure classification using element composition and consensus.

    Three layers (each additive):
    1. Element composition voting — what do the elements inside say this is?
    2. Cross-filing consensus — what did other filings call this structure?
    3. Element knowledge cross-check — do the elements' primary_statements agree?

    Returns adjusted (type, confidence). Returns (raw_type, raw_conf) unchanged
    if artifacts are unavailable or structure has no elements.
    """
    from robosystems.adapters.sec.config import XBRL_GRAPH_REFINEMENT

    if not XBRL_GRAPH_REFINEMENT:
      return (raw_type, raw_conf)

    if not structure_elements:
      return (raw_type, raw_conf)

    # Layer 1: Element composition voting
    composition_type, composition_score = self._classify_by_composition(
      structure_elements
    )

    # Layer 2: Cross-filing consensus
    consensus_type, consensus_conf = self._lookup_structure_consensus(definition_hash)

    # Layer 3: Element knowledge cross-check
    element_vote_type = self._element_statement_vote(structure_elements)

    # Combine signals
    adjusted_type = raw_type
    adjusted_conf = raw_conf

    # If composition voting has a strong opinion
    if composition_type and composition_score > 0.3:
      if composition_type == raw_type:
        # Agreement — boost confidence
        adjusted_conf = min(1.0, adjusted_conf + 0.05 * composition_score)
      elif raw_type is None:
        # No heuristic match — use composition
        adjusted_type = composition_type
        adjusted_conf = composition_score * 0.85  # Scale down from raw score
      elif composition_score > 0.6 and adjusted_conf < 0.75:
        # Strong composition disagrees with weak heuristic — override
        adjusted_type = composition_type
        adjusted_conf = composition_score * 0.80

    # Consensus can boost or penalize
    if consensus_type and consensus_conf > 0.7:
      if consensus_type == adjusted_type:
        # Agreement — boost
        adjusted_conf = min(1.0, adjusted_conf + 0.05 * consensus_conf)
      elif adjusted_type is None:
        adjusted_type = consensus_type
        adjusted_conf = consensus_conf * 0.80
      elif consensus_conf > 0.9 and adjusted_conf < 0.80:
        # Very strong consensus disagrees — override
        adjusted_type = consensus_type
        adjusted_conf = consensus_conf * 0.85

    # Element vote cross-check (validation, not override)
    if element_vote_type and adjusted_type:
      # Map element statement types to canonical structure types
      stmt_to_structure = {
        "IncomeStatement": "income_statement",
        "BalanceSheet": "balance_sheet",
        "CashFlow": "cash_flow_statement",
        "Equity": "equity_statement",
      }
      mapped = stmt_to_structure.get(element_vote_type)
      if mapped == adjusted_type:
        adjusted_conf = min(1.0, adjusted_conf + 0.03)
      elif mapped and mapped != adjusted_type:
        adjusted_conf = max(0.0, adjusted_conf - 0.05)

    return (adjusted_type, round(adjusted_conf, 4))

  def _classify_by_composition(
    self, structure_elements: list[str]
  ) -> tuple[str | None, float]:
    """Score each canonical type by element overlap with its profile."""
    profiles = self.structure_profiles
    if profiles is None:
      return (None, 0.0)

    element_set = set(structure_elements)
    best_type = None
    best_score = 0.0

    for canonical_type, profile in profiles.items():
      overlap_score = sum(profile[q] for q in element_set if q in profile)
      score = overlap_score / max(len(element_set), 1)

      if score > best_score:
        best_score = score
        best_type = canonical_type

    return (best_type, best_score)

  def _lookup_structure_consensus(
    self, definition_hash: str
  ) -> tuple[str | None, float]:
    """Look up cross-filing consensus for a structure definition hash."""
    consensus = self.structure_consensus
    if consensus is None:
      return (None, 0.0)

    entry = consensus.get(definition_hash)
    if entry is None:
      return (None, 0.0)

    return (entry["canonical_type"], entry["consensus_ratio"])

  def _element_statement_vote(self, structure_elements: list[str]) -> str | None:
    """Majority vote of elements' primary_statement from element knowledge."""
    ek = self.element_knowledge
    if ek is None:
      return None

    votes: dict[str, int] = {}
    for qname in structure_elements:
      info = ek.get(qname)
      if info and info.get("primary_statement"):
        stmt = info["primary_statement"]
        votes[stmt] = votes.get(stmt, 0) + 1

    if not votes:
      return None
    return max(votes, key=votes.get)
