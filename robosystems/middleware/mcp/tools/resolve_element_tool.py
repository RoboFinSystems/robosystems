"""
Resolve Element Tool — Maps natural-language financial concepts to XBRL elements.

Primary: DuckDB vector search on staging Element table. Uses HNSW-accelerated
query (array_cosine_distance) when DUCKDB_HNSW_INDEX_ENABLED is true, otherwise
brute-force list_cosine_similarity. Falls back to canonical concept lookup on the
graph if DuckDB staging is unavailable.
"""

import time
from typing import Any

from robosystems.logger import logger

from .base_tool import BaseTool

# Cache for DUCKDB_HNSW_INDEX_ENABLED feature flag
_hnsw_enabled_cache: tuple[bool, float] | None = None
_HNSW_CACHE_TTL = 60.0  # seconds


def _is_hnsw_index_enabled() -> bool:
  """Check if HNSW vector indexes are available on DuckDB staging tables.

  Controlled by SSM: /robosystems/{env}/features/DUCKDB_HNSW_INDEX_ENABLED
  Default: false (vector search uses brute-force list_cosine_similarity).

  Set to true after staging with build_hnsw_index=True and replicas
  have synced the indexed DuckDB file.
  """
  global _hnsw_enabled_cache

  now = time.time()
  if _hnsw_enabled_cache is not None:
    cached_result, cached_time = _hnsw_enabled_cache
    if now - cached_time < _HNSW_CACHE_TTL:
      return cached_result

  try:
    from robosystems.config.parameter_store import get_parameter_value

    value = get_parameter_value("DUCKDB_HNSW_INDEX_ENABLED", default="false")
    result = value.lower() == "true"
  except Exception:
    result = False

  _hnsw_enabled_cache = (result, now)
  return result


class ResolveElementTool(BaseTool):
  """MCP tool that resolves a natural-language concept to matching XBRL elements."""

  def __init__(self, client):
    super().__init__(client)
    self._enricher = None

  @property
  def enricher(self):
    """Lazy-load the SemanticEnricher."""
    if self._enricher is None:
      from robosystems.adapters.sec.enrichment import SemanticEnricher

      self._enricher = SemanticEnricher()
    return self._enricher

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "resolve-element",
      "description": """Resolve a financial concept (like "revenue" or "total assets") to the actual XBRL element names used in the graph.

**WHEN TO USE:**
- BEFORE writing Cypher queries that filter by element qname
- When you know the business concept but not the XBRL tag name
- To discover which companies report a given metric and how

**RETURNS:**
- Canonical concept match (if found in taxonomy)
- Top matching XBRL elements with qnames, labels, and fact counts
- A ready-to-use Cypher query hint

**EXAMPLES:**
- concept: "revenue" → finds us-gaap:Revenues, us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax, etc.
- concept: "total debt" → finds us-gaap:LongTermDebt, us-gaap:DebtAndCapitalLeaseObligations, etc.

**TIP:**
Use the returned query_hint directly in read-graph-cypher for immediate results.""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "concept": {
            "type": "string",
            "description": "Natural-language financial concept to resolve (e.g. 'revenue', 'total assets', 'earnings per share')",
          },
          "ticker": {
            "type": "string",
            "description": "Optional: filter to elements reported by a specific company ticker (e.g. 'NVDA', 'AAPL')",
          },
          "report_id": {
            "type": "string",
            "description": "Optional: filter to elements used in a specific report by its identifier",
          },
        },
        "required": ["concept"],
        "additionalProperties": False,
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> dict[str, Any]:
    self._log_tool_execution("resolve-element", arguments)

    concept = arguments.get("concept", "").strip()
    ticker = (
      arguments.get("ticker", "").strip().upper() if arguments.get("ticker") else None
    )
    report_id = (
      arguments.get("report_id", "").strip()
      if arguments.get("report_id")
      else None
    )

    if not concept:
      return {"error": "concept is required"}

    # Try DuckDB vector search first, fall back to canonical lookup
    return await self._resolve_vector(concept, ticker, report_id)

  # ---------------------------------------------------------------------------
  # Canonical concept lookup (default) — no vector index required
  # ---------------------------------------------------------------------------

  async def _resolve_canonical(
    self, concept: str, ticker: str | None, report_id: str | None = None
  ) -> dict[str, Any]:
    """Resolve using canonical_concept property on Element nodes."""
    enricher = self.enricher
    result: dict[str, Any] = {
      "concept": concept,
      "ticker": ticker,
      "report_id": report_id,
      "canonical_id": None,
      "canonical_name": None,
      "matches": [],
      "query_hint": None,
    }

    # Step 1: Embed query and match to canonical taxonomy in-memory
    try:
      query_embedding = enricher.embed_batch([concept])[0]
      canonical = enricher.match_canonical_from_query(query_embedding)
      if canonical:
        result["canonical_id"] = canonical.id
        result["canonical_name"] = canonical.display_name
    except Exception as e:
      logger.warning(f"Canonical matching failed, falling back to text: {e}")

    # Step 2: Query elements by canonical_concept property
    canonical_id = result["canonical_id"]
    if not canonical_id:
      # No canonical match — try direct text search on element qname/name
      return await self._resolve_text_fallback(
        result, concept, ticker, report_id
      )

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

      rows = await self.client.execute_query(query, parameters=params) or []
    except Exception as e:
      logger.warning(f"Canonical element query failed: {e}")
      rows = []

    if not rows:
      return await self._resolve_text_fallback(
        result, concept, ticker, report_id
      )

    # Step 3: Enrich with labels
    qnames = [r["qname"] for r in rows if r.get("qname")]
    labels = await self._fetch_labels_by_qname(qnames)

    # Build matches
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
    self._build_query_hint(result, ticker, report_id)
    return result

  async def _resolve_text_fallback(
    self,
    result: dict[str, Any],
    concept: str,
    ticker: str | None,
    report_id: str | None,
  ) -> dict[str, Any]:
    """Fallback: search element labels by text when no canonical match."""
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
      rows = await self.client.execute_query(query, parameters=params) or []
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

    self._build_query_hint(result, ticker, report_id)
    return result

  # ---------------------------------------------------------------------------
  # Vector search via DuckDB staging (feature-flagged)
  # ---------------------------------------------------------------------------

  async def _resolve_vector(
    self, concept: str, ticker: str | None, report_id: str | None = None
  ) -> dict[str, Any]:
    """Resolve using DuckDB vector similarity search on staging Element table."""
    enricher = self.enricher
    result: dict[str, Any] = {
      "concept": concept,
      "ticker": ticker,
      "report_id": report_id,
      "canonical_id": None,
      "canonical_name": None,
      "matches": [],
      "query_hint": None,
    }

    # Step 1: Embed the query
    try:
      query_embedding = enricher.embed_batch([concept])[0]
    except Exception as e:
      logger.error(f"Failed to embed concept query: {e}")
      result["error"] = f"Embedding failed: {e}"
      return result

    # Step 2: Match to canonical taxonomy in-memory
    canonical = enricher.match_canonical_from_query(query_embedding)
    if canonical:
      result["canonical_id"] = canonical.id
      result["canonical_name"] = canonical.display_name

    # Step 3: DuckDB vector similarity search on staging Element table.
    # Uses HNSW-accelerated query when index is available (SSM flag),
    # otherwise brute-force list_cosine_similarity.
    graph_id = self._get_graph_id()
    try:
      if _is_hnsw_index_enabled():
        search_sql = (
          "WITH ranked AS ("
          "  SELECT qname, canonical_concept, canonical_confidence, "
          "    array_cosine_distance(embedding, $1::FLOAT[384]) AS dist "
          "  FROM Element "
          "  WHERE embedding IS NOT NULL "
          "  ORDER BY dist ASC LIMIT 40"
          ") SELECT qname, canonical_concept, canonical_confidence, "
          "  1.0 - dist AS score FROM ranked"
        )
      else:
        search_sql = (
          "SELECT qname, canonical_concept, canonical_confidence, "
          "  list_cosine_similarity(embedding, $1) AS score "
          "FROM Element "
          "WHERE embedding IS NOT NULL "
          "ORDER BY score DESC LIMIT 40"
        )
      search_response = await self.client.query_table(
        graph_id=graph_id,
        sql=search_sql,
        parameters=[query_embedding],
      )
      raw_rows = self._table_rows_to_dicts(search_response)
    except Exception as e:
      logger.warning(f"DuckDB vector search failed, falling back to canonical: {e}")
      return await self._resolve_canonical(concept, ticker, report_id)

    if not raw_rows:
      return await self._resolve_canonical(concept, ticker, report_id)

    # Deduplicate by qname in Python (avoids expensive GROUP BY on FLOAT[384])
    seen: set[str] = set()
    search_rows = []
    for row in raw_rows:
      qname = row.get("qname")
      if qname and qname not in seen:
        seen.add(qname)
        search_rows.append(row)
      if len(search_rows) >= 20:
        break

    # Step 4: Enrich with fact counts and labels from graph
    qnames = [r["qname"] for r in search_rows if r.get("qname")]
    fact_counts = await self._fetch_fact_counts(qnames, ticker, report_id)
    labels = await self._fetch_labels_by_qname(qnames)

    for row in search_rows:
      qname = row.get("qname")
      if not qname:
        continue
      fc = fact_counts.get(qname, 0)
      if (ticker or report_id) and fc == 0:
        continue
      result["matches"].append(
        {
          "qname": qname,
          "confidence": row.get("canonical_confidence"),
          "label": labels.get(qname),
          "fact_count": fc if fc > 0 else None,
          "score": round(row.get("score", 0), 4),
        }
      )

    # Sort: fact_count desc, then score desc
    result["matches"].sort(
      key=lambda m: (m.get("fact_count") or 0, m.get("score") or 0),
      reverse=True,
    )
    result["matches"] = result["matches"][:10]
    self._build_query_hint(result, ticker, report_id)
    return result

  # ---------------------------------------------------------------------------
  # Shared helpers
  # ---------------------------------------------------------------------------

  def _get_graph_id(self) -> str:
    """Get the current graph ID from the client."""
    return getattr(self.client, "_database_name", None) or self.client.graph_id or "sec"

  @staticmethod
  def _table_rows_to_dicts(response: dict[str, Any]) -> list[dict[str, Any]]:
    """Convert query_table response to list of dicts."""
    columns = response.get("columns", [])
    rows = response.get("rows", [])
    return [dict(zip(columns, row, strict=False)) for row in rows]

  async def _fetch_labels_by_qname(self, qnames: list[str]) -> dict[str, str]:
    """Fetch labels for elements by qname."""
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
        await self.client.execute_query(label_query, parameters={"qnames": qnames})
        or []
      )
      return {r["qname"]: r["label"] for r in label_rows if r.get("label")}
    except Exception as e:
      logger.debug(f"Label enrichment failed: {e}")
      return {}

  async def _fetch_fact_counts(
    self,
    qnames: list[str],
    ticker: str | None,
    report_id: str | None,
  ) -> dict[str, int]:
    """Fetch fact counts for elements by qname."""
    if not qnames:
      return {}
    try:
      params: dict[str, Any] = {"qnames": qnames}
      if report_id:
        query = (
          "MATCH (r:Report)-[:REPORT_HAS_FACT]->(f:Fact)-[:FACT_HAS_ELEMENT]->(e:Element) "
          "WHERE e.qname IN $qnames AND r.identifier = $report_id "
          "RETURN e.qname AS qname, count(f) AS fact_count"
        )
        params["report_id"] = report_id
      elif ticker:
        query = (
          "MATCH (f:Fact)-[:FACT_HAS_ELEMENT]->(e:Element), "
          "(f)-[:FACT_HAS_ENTITY]->(ent:Entity) "
          "WHERE e.qname IN $qnames AND ent.ticker = $ticker "
          "RETURN e.qname AS qname, count(f) AS fact_count"
        )
        params["ticker"] = ticker
      else:
        query = (
          "MATCH (f:Fact)-[:FACT_HAS_ELEMENT]->(e:Element) "
          "WHERE e.qname IN $qnames "
          "RETURN e.qname AS qname, count(f) AS fact_count"
        )
      rows = await self.client.execute_query(query, parameters=params) or []
      return {r["qname"]: r["fact_count"] for r in rows}
    except Exception as e:
      logger.debug(f"Fact count enrichment failed: {e}")
      return {}

  @staticmethod
  def _build_query_hint(
    result: dict[str, Any], ticker: str | None, report_id: str | None
  ) -> None:
    """Build a ready-to-use Cypher query hint from the top match.

    Uses $param syntax so the hint is safe to pass directly to read-graph-cypher.
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
