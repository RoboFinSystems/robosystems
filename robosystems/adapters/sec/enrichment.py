"""Embedding-based canonical concept matching for XBRL elements and structures."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

import numpy as np
from xbrlkit.serialize.lpg import (
  parse_structure_definition as parse_structure_definition,
)

from robosystems.logger import logger

if TYPE_CHECKING:
  from robosystems.adapters.sec.taxonomy.concepts import CanonicalConcept


def camel_case_to_words(name: str) -> str:
  """Convert CamelCase XBRL element name to human-readable words.

  >>> camel_case_to_words("RevenueFromContractWithCustomerExcludingAssessedTax")
  'Revenue From Contract With Customer Excluding Assessed Tax'
  """
  result = re.sub(r"([a-z])([A-Z])", r"\1 \2", name)
  # "HTMLParser" -> "HTML Parser"
  result = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1 \2", result)
  return result


def compose_element_text(parsed_name: str, element_data: dict) -> str:
  """Element text for embedding: parsed name plus balance, period type, classification."""
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


def classify_structure_heuristic(
  name: str | None,
  definition: str | None,
  block_type: str | None = None,
) -> tuple[str | None, float]:
  """Keyword-classify a Statement structure into a canonical statement type.

  Non-Statement blocks are skipped so disclosures like "Balance Sheet
  Components (Details)" don't match. Confidence is at most 0.85.
  """
  if block_type and block_type.lower() not in ("statement", ""):
    return (None, 0.0)

  text = ""
  if name:
    text += name.lower()
  if definition:
    text += " " + definition.lower()
  text = text.strip()

  if not text:
    return (None, 0.0)

  is_parenthetical = "[parenthetical]" in text or "(parenthetical)" in text

  income_keywords = [
    "income",
    "operations",
    "earnings",
    "profit and loss",
    "profit or loss",
  ]
  balance_keywords = [
    "balance sheet",
    "financial position",
    "financial condition",
  ]
  cash_flow_keywords = [
    "cash flow",
    "cash flows",
  ]
  equity_keywords = [
    "stockholders equity",
    "stockholders' equity",
    "shareholders equity",
    "shareholders' equity",
    "changes in equity",
  ]
  comprehensive_keywords = [
    "comprehensive income",
    "comprehensive loss",
  ]

  # Most specific first; comprehensive income before income.
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

  # Income keywords are broad, so they also require "statement".
  if "statement" in text:
    for kw in income_keywords:
      if kw in text:
        conf = 0.75 if is_parenthetical else 0.85
        return ("income_statement", conf)

  return (None, 0.0)


class SemanticEnricher:
  """Embedding-based canonical concept matching for XBRL elements and structures.

  The fastembed model loads on first use: ``adapters/__init__.py`` imports all
  adapter code eagerly.
  """

  _UNSET = object()

  def __init__(self) -> None:
    self._model = None
    self._element_taxonomy = None
    self._structure_taxonomy = None
    self._element_knowledge = self._UNSET
    self._structure_profiles = self._UNSET
    self._structure_consensus = self._UNSET
    self._disclosure_profiles = self._UNSET
    self._disclosure_consensus = self._UNSET

  # -- Lazy model loading ---------------------------------------------------

  @property
  def model(self):
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
    if self._element_knowledge is self._UNSET:
      self._element_knowledge = self._load_element_knowledge()
    return self._element_knowledge

  @property
  def structure_profiles(self) -> dict[str, dict[str, float]] | None:
    if self._structure_profiles is self._UNSET:
      self._structure_profiles = self._load_structure_profiles()
    return self._structure_profiles

  @property
  def structure_consensus(self) -> dict[str, dict] | None:
    if self._structure_consensus is self._UNSET:
      self._structure_consensus = self._load_structure_consensus()
    return self._structure_consensus

  def _ensure_artifact_local(self, name: str) -> str | None:
    """Local path to an artifact parquet, downloading it if missing (R2 in dev, S3 otherwise)."""
    import os

    from robosystems.config.storage.shared import get_artifact_path

    path = get_artifact_path(name)

    if os.path.exists(path):
      return path

    from robosystems.config import env

    os.makedirs(os.path.dirname(path), exist_ok=True)

    if env.is_development():
      return self._download_artifact_r2(name, path)

    return self._download_artifact_s3(name, path)

  @staticmethod
  def _download_artifact_s3(name: str, path: str) -> str | None:
    try:
      from robosystems.config import env
      from robosystems.config.storage.shared import DataSourceType, get_processed_key
      from robosystems.operations.aws.s3 import S3Client

      s3_key = get_processed_key(DataSourceType.SEC, "artifacts", f"{name}.parquet")
      bucket = env.SHARED_PROCESSED_BUCKET

      s3 = S3Client()
      if s3.download_file(bucket, s3_key, path):
        logger.info(f"Downloaded artifact from s3://{bucket}/{s3_key}")
        return path

      logger.debug(f"Artifact not available on S3: {name}")
      return None
    except Exception as e:
      logger.debug(f"S3 artifact download failed for {name}: {e}")
      return None

  @staticmethod
  def _download_artifact_r2(name: str, path: str) -> str | None:
    try:
      import shutil
      from urllib.request import Request, urlopen

      from robosystems.config import env
      from robosystems.config.storage.shared import get_artifact_r2_key

      if not env.R2_PUBLIC_URL:
        logger.debug("R2_PUBLIC_URL not configured, skipping artifact download")
        return None

      r2_key = get_artifact_r2_key(name)
      url = f"{env.R2_PUBLIC_URL}/{r2_key}"

      logger.info(f"Downloading artifact from {url}")
      req = Request(url, headers={"User-Agent": "RoboSystems/1.0"})
      with urlopen(req, timeout=30) as resp, open(path, "wb") as f:
        shutil.copyfileobj(resp, f)
      return path
    except Exception as e:
      logger.debug(f"R2 artifact download failed for {name}: {e}")
      return None

  def _load_element_knowledge(self) -> dict[str, dict] | None:
    """Load element_knowledge.parquet into a qname-keyed dict."""
    try:
      path = self._ensure_artifact_local("element_knowledge")

      if path is None:
        return None

      import pyarrow.parquet as pq

      with open(path, "rb") as f:
        table = pq.read_table(f)
      result = {}

      columns = table.to_pydict()
      qnames = columns["qname"]
      # Older artifacts lack this column.
      disclosure_types = columns.get("disclosure_type")
      for i, qname in enumerate(qnames):
        result[qname] = {
          "primary_statement": columns["primary_statement"][i],
          "bfs_depth": columns["bfs_depth"][i],
          "pagerank": columns["pagerank"][i],
          "core_number": columns["core_number"][i],
          "neighborhood_agreement": columns["neighborhood_agreement"][i],
          "filing_count": columns["filing_count"][i],
          "disclosure_type": disclosure_types[i] if disclosure_types else None,
        }

      logger.info(f"Loaded element knowledge artifact: {len(result)} elements")
      return result
    except Exception as e:
      logger.warning(f"Element knowledge artifact failed to load: {e}")
      return None

  def _load_structure_profiles(self) -> dict[str, dict[str, float]] | None:
    """Load structure_profiles.parquet into canonical_type → {qname → frequency}."""
    try:
      path = self._ensure_artifact_local("structure_profiles")

      if path is None:
        return None

      import pyarrow.parquet as pq

      with open(path, "rb") as f:
        table = pq.read_table(f)
      columns = table.to_pydict()

      result: dict[str, dict[str, float]] = {}
      for i, ct in enumerate(columns["canonical_type"]):
        if ct not in result:
          result[ct] = {}
        result[ct][columns["qname"][i]] = columns["frequency"][i]

      logger.info(f"Loaded structure profiles artifact: {len(result)} types")
      return result
    except Exception as e:
      logger.warning(f"Structure profiles artifact failed to load: {e}")
      return None

  def _load_structure_consensus(self) -> dict[str, dict] | None:
    """Load structure_consensus.parquet into definition_hash-keyed dict."""
    try:
      path = self._ensure_artifact_local("structure_consensus")

      if path is None:
        return None

      import pyarrow.parquet as pq

      with open(path, "rb") as f:
        table = pq.read_table(f)
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
      logger.warning(f"Structure consensus artifact failed to load: {e}")
      return None

  @property
  def disclosure_profiles(self) -> dict[str, dict[str, float]] | None:
    if self._disclosure_profiles is self._UNSET:
      self._disclosure_profiles = self._load_disclosure_profiles()
    return self._disclosure_profiles

  @property
  def disclosure_consensus(self) -> dict[str, dict] | None:
    if self._disclosure_consensus is self._UNSET:
      self._disclosure_consensus = self._load_disclosure_consensus()
    return self._disclosure_consensus

  def _load_disclosure_profiles(self) -> dict[str, dict[str, float]] | None:
    """Load disclosure_profiles.parquet into disclosure_type -> {qname -> weighted_score}."""
    try:
      path = self._ensure_artifact_local("disclosure_profiles")

      if path is None:
        return None

      import pyarrow.parquet as pq

      with open(path, "rb") as f:
        table = pq.read_table(f)
      columns = table.to_pydict()

      result: dict[str, dict[str, float]] = {}
      for i, dt in enumerate(columns["disclosure_type"]):
        if dt not in result:
          result[dt] = {}
        result[dt][columns["qname"][i]] = columns["weighted_score"][i]

      logger.info(f"Loaded disclosure profiles artifact: {len(result)} types")
      return result
    except Exception as e:
      logger.warning(f"Disclosure profiles artifact failed to load: {e}")
      return None

  def _load_disclosure_consensus(self) -> dict[str, dict] | None:
    """Load disclosure_consensus.parquet into definition_hash-keyed dict."""
    try:
      path = self._ensure_artifact_local("disclosure_consensus")

      if path is None:
        return None

      import pyarrow.parquet as pq

      with open(path, "rb") as f:
        table = pq.read_table(f)
      columns = table.to_pydict()

      result = {}
      for i, def_hash in enumerate(columns["definition_hash"]):
        result[def_hash] = {
          "disclosure_type": columns["disclosure_type"][i],
          "consensus_ratio": columns["consensus_ratio"][i],
          "filing_count": columns["filing_count"][i],
        }

      logger.info(f"Loaded disclosure consensus artifact: {len(result)} entries")
      return result
    except Exception as e:
      logger.warning(f"Disclosure consensus artifact failed to load: {e}")
      return None

  # -- Embedding ------------------------------------------------------------

  def embed_batch(self, texts: list[str]) -> list[list[float]]:
    embeddings = list(self.model.embed(texts))
    return [emb.tolist() for emb in embeddings]

  # -- Canonical matching (elements) ----------------------------------------

  _CANDIDATE_THRESHOLD = 0.80
  _MAX_CANDIDATES = 3

  def match_canonical(
    self, embedding: list[float], element_metadata: dict
  ) -> tuple[str | None, float]:
    """Best canonical concept for an element embedding, or (None, 0.0).

    A qname listed in a concept's expected_elements returns that concept at
    0.95 immediately. Otherwise cosine similarity, +0.10 each for matching
    period_type and balance; the top candidates are refined (when
    XBRL_GRAPH_REFINEMENT is on) before picking the winner.
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

      if qname and qname in concept.expected_elements:
        return (concept.id, 0.95)

      tax_vec = np.array(concept.embedding, dtype=np.float32)
      tax_norm = np.linalg.norm(tax_vec)
      if tax_norm == 0:
        continue

      cos_sim = float(np.dot(query_vec, tax_vec) / (query_norm * tax_norm))

      if elem_period_type and concept.period_type == elem_period_type:
        cos_sim += 0.10
      if elem_balance and concept.balance == elem_balance:
        cos_sim += 0.10

      if cos_sim >= self._CANDIDATE_THRESHOLD:
        candidates.append((cos_sim, concept.id, concept))

    if not candidates:
      return (None, 0.0)

    candidates.sort(reverse=True)
    candidates = candidates[: self._MAX_CANDIDATES]

    from robosystems.adapters.sec.config import XBRL_GRAPH_REFINEMENT

    if XBRL_GRAPH_REFINEMENT:
      refined = [
        (self._refine_element_confidence(score, qname, concept), concept_id)
        for score, concept_id, concept in candidates
      ]
      refined.sort(reverse=True)
      best_score, best_id = refined[0]
    else:
      best_score, best_id = candidates[0][0], candidates[0][1]

    if best_score >= self._CANDIDATE_THRESHOLD:
      return (best_id, round(min(best_score, 1.0), 4))
    return (None, 0.0)

  # element knowledge primary_statement → CanonicalConcept.category
  _STATEMENT_TO_CATEGORY = {
    "IncomeStatement": "income_statement",
    "BalanceSheet": "balance_sheet",
    "CashFlow": "cash_flow",
  }

  @staticmethod
  def _disclosure_type_to_category(disclosure_type: str) -> str | None:
    """Disclosure type → CanonicalConcept.category, via its statement type."""
    from robosystems.adapters.sec.knowledge.classifiers import DISCLOSURE_TO_STATEMENT

    stmt_type = DISCLOSURE_TO_STATEMENT.get(disclosure_type)
    if stmt_type is None:
      return None
    return {
      "IncomeStatement": "income_statement",
      "BalanceSheet": "balance_sheet",
      "CashFlow": "cash_flow",
      "Equity": "equity",
    }.get(stmt_type.value)

  def _refine_element_confidence(
    self, raw_conf: float, qname: str, concept: CanonicalConcept | None = None
  ) -> float:
    """Adjust a semantic score by graph structure (PageRank, neighborhood
    agreement, statement alignment with ``concept``); unchanged without the
    element knowledge artifact."""
    ek = self.element_knowledge
    if ek is None:
      return raw_conf

    element_info = ek.get(qname)
    if element_info is None:
      return raw_conf

    agreement = element_info["neighborhood_agreement"]
    pagerank = element_info["pagerank"]

    pr_boost = 1.0 + 0.15 * (pagerank if pagerank is not None else 0.0)

    base = raw_conf * pr_boost

    if agreement is not None and agreement >= 0.8:
      refined = base * (1.0 + 0.10 * agreement)
    elif agreement is not None and agreement >= 0.5:
      refined = base
    elif agreement is not None:
      penalty = 0.6 + 0.4 * agreement
      refined = base * penalty
    else:
      refined = base

    if concept is not None:
      primary_stmt = element_info.get("primary_statement")
      disclosure_type = element_info.get("disclosure_type")

      if primary_stmt:
        mapped_category = self._STATEMENT_TO_CATEGORY.get(primary_stmt)
        if mapped_category == concept.category:
          refined *= 1.08
        elif mapped_category is not None:
          refined *= 0.88
      elif disclosure_type:
        # Weaker signal than primary_statement, so smaller adjustments.
        mapped_category = self._disclosure_type_to_category(disclosure_type)
        if mapped_category == concept.category:
          refined *= 1.05
        elif mapped_category is not None:
          refined *= 0.92

    # Quadratic penalty below threshold
    threshold = 0.90
    if refined < threshold:
      refined *= (refined / threshold) ** 2.0

    return round(min(1.0, max(0.0, refined)), 4)

  # -- Canonical matching (query-time, for MCP tools) -----------------------

  def match_canonical_from_query(
    self, query_embedding: list[float]
  ) -> CanonicalConcept | None:
    """Best element concept for a free-text query embedding (no metadata boosts)."""
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

    if best_score >= 0.75:
      return best
    return None

  def match_structure_canonical(
    self, embedding: list[float]
  ) -> tuple[str | None, float]:
    """Best canonical structure type for an embedding, or (None, 0.0) below 0.70."""
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
    """Refine a structure classification by element composition, cross-filing
    consensus, and the elements' primary_statement vote, in that order.

    Unchanged when refinement is off or the structure has no elements.
    """
    from robosystems.adapters.sec.config import XBRL_GRAPH_REFINEMENT

    if not XBRL_GRAPH_REFINEMENT:
      return (raw_type, raw_conf)

    if not structure_elements:
      return (raw_type, raw_conf)

    composition_type, composition_score = self._classify_by_composition(
      structure_elements
    )

    consensus_type, consensus_conf = self._lookup_structure_consensus(definition_hash)

    element_vote_type = self._element_statement_vote(structure_elements)

    adjusted_type = raw_type
    adjusted_conf = raw_conf

    if composition_type and composition_score > 0.3:
      if composition_type == raw_type:
        adjusted_conf = min(1.0, adjusted_conf + 0.05 * composition_score)
      elif raw_type is None:
        adjusted_type = composition_type
        adjusted_conf = composition_score * 0.85
      elif composition_score > 0.6 and adjusted_conf < 0.75:
        # Strong composition overrides a weak heuristic.
        adjusted_type = composition_type
        adjusted_conf = composition_score * 0.80

    if consensus_type and consensus_conf > 0.7:
      if consensus_type == adjusted_type:
        adjusted_conf = min(1.0, adjusted_conf + 0.05 * consensus_conf)
      elif adjusted_type is None:
        adjusted_type = consensus_type
        adjusted_conf = consensus_conf * 0.80
      elif consensus_conf > 0.9 and adjusted_conf < 0.80:
        adjusted_type = consensus_type
        adjusted_conf = consensus_conf * 0.85

    # The element vote only nudges confidence; it never changes the type.
    if element_vote_type and adjusted_type:
      stmt_to_structure = {
        "IncomeStatement": "income_statement",
        "BalanceSheet": "balance_sheet",
        "CashFlow": "cash_flow_statement",
        "Equity": "equity_statement",
      }
      mapped = stmt_to_structure.get(element_vote_type)
      if mapped == adjusted_type:
        adjusted_conf = min(1.0, adjusted_conf + 0.03)
      elif mapped:
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

  # -- Disclosure classification ---------------------------------------------

  def classify_disclosure_by_composition(
    self, structure_elements: list[str]
  ) -> tuple[str | None, float]:
    """Best disclosure type by PageRank-weighted element overlap, normalized by size."""
    profiles = self.disclosure_profiles
    if profiles is None:
      return (None, 0.0)

    element_set = set(structure_elements)
    best_type = None
    best_score = 0.0

    for disclosure_type, profile in profiles.items():
      overlap_score = sum(profile[q] for q in element_set if q in profile)
      score = overlap_score / max(len(element_set), 1)

      if score > best_score:
        best_score = score
        best_type = disclosure_type

    return (best_type, best_score)

  @staticmethod
  def detect_dei_structure(
    structure_elements: list[str],
  ) -> tuple[str | None, float]:
    """Any dei: element makes a structure DocumentInformation or EntityInformation."""
    dei_elements = [q for q in structure_elements if q.startswith("dei:")]
    if not dei_elements:
      return (None, 0.0)

    entity_indicators = {
      "dei:EntityRegistrantName",
      "dei:EntityCentralIndexKey",
      "dei:EntityFileNumber",
      "dei:EntityTaxIdentificationNumber",
      "dei:EntityIncorporationStateCountryCode",
    }
    document_indicators = {
      "dei:DocumentType",
      "dei:DocumentPeriodEndDate",
      "dei:DocumentFiscalYearFocus",
      "dei:DocumentFiscalPeriodFocus",
      "dei:AmendmentFlag",
    }

    dei_set = set(dei_elements)
    entity_count = len(dei_set & entity_indicators)
    document_count = len(dei_set & document_indicators)

    if entity_count > document_count:
      return ("EntityInformation", 0.95)
    elif document_count > 0 or len(dei_elements) >= 2:
      return ("DocumentInformation", 0.95)

    return ("DocumentInformation", 0.85)

  @staticmethod
  def detect_balance_sheet_rollup(
    structure_elements: list[str],
  ) -> tuple[str | None, float]:
    """Detect AssetsRollUp or LiabilitiesAndEquityRollUp by root elements.

    At least two structural companions (AssetsCurrent, LiabilitiesCurrent, ...)
    are required, so VIE or segment disclosures that merely cite the totals
    don't match.
    """
    element_set = set(structure_elements)

    assets_roots = {
      "us-gaap:Assets",
    }
    liab_equity_roots = {
      "us-gaap:LiabilitiesAndStockholdersEquity",
    }
    balance_sheet_companions = {
      "us-gaap:AssetsCurrent",
      "us-gaap:LiabilitiesCurrent",
      "us-gaap:AssetsAbstract",
      "us-gaap:RetainedEarningsAccumulatedDeficit",
      "us-gaap:CommitmentsAndContingencies",
      "us-gaap:AccumulatedOtherComprehensiveIncomeLossNetOfTax",
    }

    has_assets = bool(element_set & assets_roots)
    has_liab_equity = bool(element_set & liab_equity_roots)
    companion_count = len(element_set & balance_sheet_companions)

    if companion_count < 2:
      return (None, 0.0)

    if has_assets and has_liab_equity:
      # Both totals: prefer assets, which maps to canonical total_assets.
      return ("AssetsRollUp", 0.95)
    elif has_assets:
      return ("AssetsRollUp", 0.95)
    elif has_liab_equity:
      return ("LiabilitiesAndEquityRollUp", 0.95)

    return (None, 0.0)

  def _lookup_disclosure_consensus(
    self, definition_hash: str
  ) -> tuple[str | None, float]:
    consensus = self.disclosure_consensus
    if consensus is None:
      return (None, 0.0)

    entry = consensus.get(definition_hash)
    if entry is None:
      return (None, 0.0)

    return (entry["disclosure_type"], entry["consensus_ratio"])

  def refine_disclosure_confidence(
    self,
    raw_type: str | None,
    raw_conf: float,
    structure_elements: list[str],
    definition_hash: str,
  ) -> tuple[str | None, float]:
    """Refine a disclosure classification by cross-filing consensus, then the
    elements' disclosure_type vote.

    Unchanged when refinement is off or the structure has no elements.
    """
    from robosystems.adapters.sec.config import XBRL_GRAPH_REFINEMENT

    if not XBRL_GRAPH_REFINEMENT:
      return (raw_type, raw_conf)

    if not structure_elements:
      return (raw_type, raw_conf)

    adjusted_type = raw_type
    adjusted_conf = raw_conf

    consensus_type, consensus_conf = self._lookup_disclosure_consensus(definition_hash)
    if consensus_type and consensus_conf > 0.7:
      if consensus_type == adjusted_type:
        adjusted_conf = min(1.0, adjusted_conf + 0.05 * consensus_conf)
      elif adjusted_type is None:
        adjusted_type = consensus_type
        adjusted_conf = consensus_conf * 0.80
      elif consensus_conf > 0.9 and adjusted_conf < 0.80:
        adjusted_type = consensus_type
        adjusted_conf = consensus_conf * 0.85

    ek = self.element_knowledge
    if ek is not None:
      votes: dict[str, int] = {}
      for qname in structure_elements:
        info = ek.get(qname)
        if info and info.get("disclosure_type"):
          dt = info["disclosure_type"]
          votes[dt] = votes.get(dt, 0) + 1

      if votes:
        top_vote = max(votes, key=votes.get)
        if top_vote == adjusted_type:
          adjusted_conf = min(1.0, adjusted_conf + 0.03)
        elif top_vote and adjusted_type:
          adjusted_conf = max(0.0, adjusted_conf - 0.05)

    return (adjusted_type, round(adjusted_conf, 4))
