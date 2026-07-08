"""SEC-specific natural-language → XBRL element resolution.

Backing logic for the ``resolve-element`` MCP tool. Lives in the SEC
adapter because it relies on the **canonical concept taxonomy** curated
inside ``adapters/sec/enrichment`` (us-gaap-oriented) — a small, in-memory
set of concept embeddings, not a persisted per-element vector index.

Resolution is canonical-first with a text-label fallback (the LanceDB
element-vector semantic search was retired — it indexed ~8M elements
dominated by single-filing filer extensions and returned low-signal
noise for non-canonical concepts). The tool is manifest-gated to
``has_semantic_enrichment`` repos, which only SEC sets.

Takes a duck-typed ``graph_client`` that exposes an ``execute_query``
coroutine (the MCP client today). Returns a dict — variable number of
matches + optional canonical / query-hint fields.
"""

from __future__ import annotations

from typing import Any, Protocol

from robosystems.logger import logger


class _GraphClientLike(Protocol):
  """Duck-typed interface for clients this module accepts.

  Just the methods we need; implementations don't have to subclass.
  """

  graph_id: str

  async def execute_query(
    self, query: str, parameters: dict[str, Any] | None = None
  ) -> list[dict[str, Any]]: ...


_enricher: Any = None


def _get_enricher():
  """Lazy-load the SemanticEnricher (avoids import cost at module load)."""
  global _enricher
  if _enricher is None:
    from robosystems.adapters.sec.enrichment import SemanticEnricher

    _enricher = SemanticEnricher()
  return _enricher


async def resolve_sec_element(
  graph_client: _GraphClientLike,
  *,
  concept: str,
  ticker: str | None = None,
  report_id: str | None = None,
) -> dict[str, Any]:
  """Resolve a natural-language concept to matching SEC XBRL elements.

  Resolution order:

  1. Canonical concept matching — curated concepts, deterministic.
  2. Text search on element labels — final fallback.

  Args:
      graph_client: Duck-typed client exposing ``execute_query``.
        Usually a ``GraphMCPClient``.
      concept: Natural-language concept to resolve (e.g. ``"revenue"``).
      ticker: Optional entity ticker filter (e.g. ``"NVDA"``).
      report_id: Optional report identifier filter.

  Returns:
      A dict with keys: ``concept``, ``ticker``, ``report_id``,
      ``canonical_id``, ``canonical_name``, ``matches`` (list of match
      dicts), ``query_hint`` (Cypher template), ``query_hint_params``
      (dict to pass alongside the hint).
  """
  enricher = _get_enricher()
  result: dict[str, Any] = {
    "concept": concept,
    "ticker": ticker,
    "report_id": report_id,
    "canonical_id": None,
    "canonical_name": None,
    "matches": [],
    "query_hint": None,
  }

  # Embed the query once for canonical concept matching (small curated set).
  query_embedding: list[float] | None = None
  try:
    query_embedding = enricher.embed_batch([concept])[0]
  except Exception as e:
    logger.warning(f"Embedding failed: {e}")

  # Try canonical concept matching first (curated, deterministic).
  if query_embedding:
    try:
      canonical = enricher.match_canonical_from_query(query_embedding)
      if canonical:
        result["canonical_id"] = canonical.id
        result["canonical_name"] = canonical.display_name
    except Exception as e:
      logger.warning(f"Canonical matching failed: {e}")

  canonical_id = result["canonical_id"]

  # No canonical match → fall back to text search on element labels.
  if not canonical_id:
    return await _resolve_text_fallback(
      graph_client, result, concept, ticker, report_id
    )

  rows = await _fetch_canonical_elements(graph_client, canonical_id, ticker, report_id)

  if not rows:
    return await _resolve_text_fallback(
      graph_client, result, concept, ticker, report_id
    )

  qnames = [r["qname"] for r in rows if r.get("qname")]
  labels = await _fetch_labels_by_qname(graph_client, qnames)

  for row in rows:
    qname = row.get("qname")
    if not qname:
      continue
    result["matches"].append(
      {
        "qname": qname,
        "confidence": row.get("confidence"),
        "label": labels.get(qname),
        "fact_count": row.get("fact_count"),
        "score": row.get("confidence"),
      }
    )

  result["matches"] = result["matches"][:10]
  _build_query_hint(result, ticker, report_id)
  return result


# ── Canonical concept path ────────────────────────────────────────────────


async def _fetch_canonical_elements(
  graph_client: _GraphClientLike,
  canonical_id: str,
  ticker: str | None,
  report_id: str | None,
) -> list[dict[str, Any]]:
  """Fetch elements matching a canonical concept id, optionally scoped."""
  try:
    params: dict[str, Any] = {"canonical_id": canonical_id}
    if report_id:
      query = (
        "MATCH (r:Report)-[:REPORT_HAS_FACT]->(f:Fact)-[:FACT_HAS_ELEMENT]->(e:Element) "
        "WHERE e.canonical_concept = $canonical_id "
        "AND r.identifier = $report_id "
        "AND f.has_dimensions = false "
        "RETURN DISTINCT e.qname AS qname, e.canonical_confidence AS confidence, "
        "count(f) AS fact_count "
        "ORDER BY fact_count DESC LIMIT 20"
      )
      params["report_id"] = report_id
    elif ticker:
      query = (
        "MATCH (f:Fact)-[:FACT_HAS_ELEMENT]->(e:Element), "
        "(f)-[:FACT_HAS_ENTITY]->(ent:Entity) "
        "WHERE e.canonical_concept = $canonical_id "
        "AND ent.ticker = $ticker "
        "AND f.has_dimensions = false "
        "RETURN DISTINCT e.qname AS qname, e.canonical_confidence AS confidence, "
        "count(f) AS fact_count "
        "ORDER BY fact_count DESC LIMIT 20"
      )
      params["ticker"] = ticker
    else:
      query = (
        "MATCH (f:Fact)-[:FACT_HAS_ELEMENT]->(e:Element) "
        "WHERE e.canonical_concept = $canonical_id "
        "AND f.has_dimensions = false "
        "RETURN DISTINCT e.qname AS qname, e.canonical_confidence AS confidence, "
        "count(f) AS fact_count "
        "ORDER BY fact_count DESC LIMIT 20"
      )

    return await graph_client.execute_query(query, parameters=params) or []
  except Exception as e:
    logger.warning(f"Canonical element query failed: {e}")
    return []


# ── Text fallback path ────────────────────────────────────────────────────


async def _resolve_text_fallback(
  graph_client: _GraphClientLike,
  result: dict[str, Any],
  concept: str,
  ticker: str | None,
  report_id: str | None,
) -> dict[str, Any]:
  """Final fallback: search element labels by text when embedding paths miss."""
  search_term = concept.lower()
  try:
    if ticker:
      query = (
        "MATCH (f:Fact)-[:FACT_HAS_ELEMENT]->(e:Element)-[:ELEMENT_HAS_LABEL]->(l:Label), "
        "(f)-[:FACT_HAS_ENTITY]->(ent:Entity) "
        "WHERE l.value CONTAINS $search_term "
        'AND l.type = "http://www.xbrl.org/2003/role/label" '
        "AND ent.ticker = $ticker "
        "AND f.has_dimensions = false "
        "RETURN DISTINCT e.qname AS qname, l.value AS label, "
        "e.canonical_concept AS concept, e.canonical_confidence AS confidence, "
        "count(f) AS fact_count "
        "ORDER BY fact_count DESC LIMIT 10"
      )
      params = {"search_term": search_term, "ticker": ticker}
    else:
      query = (
        "MATCH (e:Element)-[:ELEMENT_HAS_LABEL]->(l:Label) "
        "WHERE l.value CONTAINS $search_term "
        'AND l.type = "http://www.xbrl.org/2003/role/label" '
        "RETURN DISTINCT e.qname AS qname, l.value AS label, "
        "e.canonical_concept AS concept, e.canonical_confidence AS confidence "
        "ORDER BY e.canonical_confidence DESC LIMIT 10"
      )
      params = {"search_term": search_term}
    rows = await graph_client.execute_query(query, parameters=params) or []
    for row in rows:
      result["matches"].append(
        {
          "qname": row.get("qname"),
          "confidence": row.get("confidence"),
          "label": row.get("label"),
          "fact_count": row.get("fact_count"),
          "score": row.get("confidence"),
        }
      )
  except Exception as e:
    logger.warning(f"Text fallback search failed: {e}")

  _build_query_hint(result, ticker, report_id)
  return result


# ── Shared helpers ────────────────────────────────────────────────────────


async def _fetch_labels_by_qname(
  graph_client: _GraphClientLike, qnames: list[str]
) -> dict[str, str]:
  """Fetch element labels by qname, returning a ``{qname: label}`` map."""
  if not qnames:
    return {}
  try:
    label_query = (
      "MATCH (e:Element)-[:ELEMENT_HAS_LABEL]->(l:Label) "
      "WHERE e.qname IN $qnames "
      'AND l.type = "http://www.xbrl.org/2003/role/label" '
      "RETURN e.qname AS qname, l.value AS label"
    )
    label_rows = (
      await graph_client.execute_query(label_query, parameters={"qnames": qnames}) or []
    )
    return {r["qname"]: r["label"] for r in label_rows if r.get("label")}
  except Exception as e:
    logger.debug(f"Label enrichment failed: {e}")
    return {}


def _build_query_hint(
  result: dict[str, Any], ticker: str | None, report_id: str | None
) -> None:
  """Build a ready-to-use Cypher query hint from the top match.

  Uses ``$param`` syntax so the hint is safe to pass directly to
  ``read-graph-cypher`` (or any parameterized Cypher executor).
  """
  if not result["matches"]:
    return
  top = result["matches"][0]
  qname = top["qname"]
  hint_params: dict[str, str] = {"qname": qname}

  if report_id:
    hint_params["report_id"] = report_id
    result["query_hint"] = (
      "MATCH (r:Report)-[:REPORT_HAS_FACT]->(f:Fact)-[:FACT_HAS_ELEMENT]->(e:Element), "
      "(f)-[:FACT_HAS_PERIOD]->(p:Period) "
      "WHERE e.qname = $qname AND r.identifier = $report_id "
      "AND f.has_dimensions = false "
      "RETURN e.qname AS element, f.numeric_value AS value, "
      "p.end_date AS date, p.duration_type AS period_type "
      "ORDER BY p.end_date DESC LIMIT 20"
    )
  elif ticker:
    hint_params["ticker"] = ticker
    result["query_hint"] = (
      "MATCH (f:Fact)-[:FACT_HAS_ELEMENT]->(e:Element), "
      "(f)-[:FACT_HAS_PERIOD]->(p:Period), "
      "(f)-[:FACT_HAS_ENTITY]->(ent:Entity) "
      "WHERE e.qname = $qname AND ent.ticker = $ticker "
      "AND f.has_dimensions = false "
      "RETURN ent.ticker AS ticker, e.qname AS element, "
      "f.numeric_value AS value, p.end_date AS date, "
      "p.duration_type AS period_type "
      "ORDER BY p.end_date DESC LIMIT 20"
    )
  else:
    result["query_hint"] = (
      "MATCH (f:Fact)-[:FACT_HAS_ELEMENT]->(e:Element), "
      "(f)-[:FACT_HAS_PERIOD]->(p:Period) "
      "WHERE e.qname = $qname "
      "AND f.has_dimensions = false "
      "RETURN e.qname AS element, f.numeric_value AS value, "
      "p.end_date AS date, p.duration_type AS period_type "
      "ORDER BY p.end_date DESC LIMIT 20"
    )
  result["query_hint_params"] = hint_params
