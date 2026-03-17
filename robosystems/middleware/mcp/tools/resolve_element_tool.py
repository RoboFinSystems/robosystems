"""
Resolve Element Tool — Maps natural-language financial concepts to XBRL elements.

Uses LanceDB vector search (when available) over 2.6M+ element embeddings for
fast semantic matching. Falls back to in-memory canonical concept matching via
SemanticEnricher, then to graph-based text search.
"""

from typing import Any

from robosystems.logger import logger

from .base_tool import BaseTool


class ResolveElementTool(BaseTool):
  """MCP tool that resolves a natural-language concept to matching XBRL elements."""

  def __init__(self, client):
    super().__init__(client)
    self._enricher = None
    self._lance_search = None
    self._lance_checked = False

  @property
  def enricher(self):
    """Lazy-load the SemanticEnricher."""
    if self._enricher is None:
      from robosystems.adapters.sec.enrichment import SemanticEnricher

      self._enricher = SemanticEnricher()
    return self._enricher

  @property
  def lance_search(self):
    """Lazy-load the LanceDB search instance. Returns None if unavailable or disabled."""
    if not self._lance_checked:
      self._lance_checked = True
      try:
        from robosystems.config import env

        if not env.MCP_VECTOR_SEARCH_ENABLED:
          logger.debug(
            "LanceDB vector search disabled (MCP_VECTOR_SEARCH_ENABLED=false)"
          )
          return None

        from robosystems.adapters.sec.knowledge.lance_search import LanceElementSearch

        self._lance_search = LanceElementSearch.get_instance()
      except Exception as e:
        logger.debug(f"LanceDB search not available: {e}")
    return self._lance_search

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
      arguments.get("report_id", "").strip() if arguments.get("report_id") else None
    )

    if not concept:
      return {"error": "concept is required"}

    return await self._resolve_canonical(concept, ticker, report_id)

  # ---------------------------------------------------------------------------
  # Resolution — LanceDB vector search → canonical fallback → text fallback
  # ---------------------------------------------------------------------------

  async def _resolve_canonical(
    self, concept: str, ticker: str | None, report_id: str | None = None
  ) -> dict[str, Any]:
    """Resolve a concept to XBRL elements.

    Resolution order:
    1. LanceDB vector search (2.6M+ elements, ~5ms) — if index available
    2. Canonical concept matching (~40 concepts, in-memory) — fallback
    3. Text search on element labels — final fallback
    """
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

    # Step 1: Embed query
    query_embedding = None
    try:
      query_embedding = enricher.embed_batch([concept])[0]
    except Exception as e:
      logger.warning(f"Embedding failed: {e}")

    # Step 2: Try LanceDB vector search first (covers all elements)
    if query_embedding and self.lance_search:
      lance_result = await self._resolve_via_lance(
        result, query_embedding, ticker, report_id
      )
      if lance_result["matches"]:
        return lance_result

    # Step 3: Fall back to canonical concept matching
    if query_embedding:
      try:
        canonical = enricher.match_canonical_from_query(query_embedding)
        if canonical:
          result["canonical_id"] = canonical.id
          result["canonical_name"] = canonical.display_name
      except Exception as e:
        logger.warning(f"Canonical matching failed: {e}")

    canonical_id = result["canonical_id"]
    if not canonical_id:
      return await self._resolve_text_fallback(result, concept, ticker, report_id)

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
      return await self._resolve_text_fallback(result, concept, ticker, report_id)

    # Enrich with labels
    qnames = [r["qname"] for r in rows if r.get("qname")]
    labels = await self._fetch_labels_by_qname(qnames)

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

  # ---------------------------------------------------------------------------
  # LanceDB vector search path
  # ---------------------------------------------------------------------------

  async def _resolve_via_lance(
    self,
    result: dict[str, Any],
    query_embedding: list[float],
    ticker: str | None,
    report_id: str | None,
  ) -> dict[str, Any]:
    """Resolve using LanceDB vector search over all element embeddings.

    LanceDB provides semantic candidates (fast ~5ms ANN search), then the graph
    filters and enriches with fact counts, labels, and optional ticker/report scope.
    """
    try:
      lance_results = self.lance_search.search(query_embedding, limit=20)
      if not lance_results:
        return result

      # Deduplicate qnames (same qname may appear from different filings)
      seen_qnames: set[str] = set()
      unique_results = []
      for r in lance_results:
        qname = r.get("qname")
        if qname and qname not in seen_qnames:
          seen_qnames.add(qname)
          unique_results.append(r)

      qnames = [r["qname"] for r in unique_results]

      # Build a similarity lookup for scoring
      similarity_by_qname = {}
      for r in unique_results:
        similarity_by_qname[r["qname"]] = round(1.0 - r.get("_distance", 0.0), 4)

      # Query graph with lance candidates + optional ticker/report filter
      rows = await self._fetch_lance_candidates(qnames, ticker, report_id)
      labels = await self._fetch_labels_by_qname(
        [r["qname"] for r in rows] if rows else qnames
      )

      if rows:
        # Graph returned scoped results — use those
        for row in rows:
          qname = row["qname"]
          result["matches"].append(
            {
              "qname": qname,
              "confidence": row.get("confidence"),
              "label": labels.get(qname),
              "fact_count": row.get("fact_count", 0),
              "score": similarity_by_qname.get(qname, 0.0),
            }
          )
      else:
        # No graph results (no ticker/report filter, or no matches in scope)
        # Fall back to lance results directly
        for r in unique_results:
          qname = r["qname"]
          result["matches"].append(
            {
              "qname": qname,
              "confidence": r.get("canonical_confidence"),
              "label": labels.get(qname),
              "fact_count": 0,
              "score": similarity_by_qname.get(qname, 0.0),
            }
          )

      # Set canonical info from top match if available
      if result["matches"] and unique_results[0].get("canonical_concept"):
        result["canonical_id"] = unique_results[0]["canonical_concept"]

      result["matches"] = result["matches"][:10]
      self._build_query_hint(result, ticker, report_id)
    except Exception as e:
      logger.warning(f"LanceDB search failed, falling back: {e}")

    return result

  async def _fetch_lance_candidates(
    self,
    qnames: list[str],
    ticker: str | None,
    report_id: str | None,
  ) -> list[dict]:
    """Query graph for lance candidates, filtered by optional ticker/report scope."""
    if not qnames:
      return []
    try:
      params: dict[str, Any] = {"qnames": qnames}
      if report_id:
        query = (
          "MATCH (r:Report)-[:REPORT_HAS_FACT]->(f:Fact)-[:FACT_HAS_ELEMENT]->(e:Element) "
          "WHERE e.qname IN $qnames AND r.identifier = $report_id "
          "AND f.has_dimensions = false "
          "RETURN DISTINCT e.qname AS qname, e.canonical_confidence AS confidence, "
          "count(f) AS fact_count "
          "ORDER BY fact_count DESC"
        )
        params["report_id"] = report_id
      elif ticker:
        query = (
          "MATCH (f:Fact)-[:FACT_HAS_ELEMENT]->(e:Element), "
          "(f)-[:FACT_HAS_ENTITY]->(ent:Entity) "
          "WHERE e.qname IN $qnames AND ent.ticker = $ticker "
          "AND f.has_dimensions = false "
          "RETURN DISTINCT e.qname AS qname, e.canonical_confidence AS confidence, "
          "count(f) AS fact_count "
          "ORDER BY fact_count DESC"
        )
        params["ticker"] = ticker
      else:
        query = (
          "MATCH (f:Fact)-[:FACT_HAS_ELEMENT]->(e:Element) "
          "WHERE e.qname IN $qnames "
          "AND f.has_dimensions = false "
          "RETURN DISTINCT e.qname AS qname, e.canonical_confidence AS confidence, "
          "count(f) AS fact_count "
          "ORDER BY fact_count DESC"
        )
      return await self.client.execute_query(query, parameters=params) or []
    except Exception as e:
      logger.warning(f"Lance candidate graph query failed: {e}")
      return []

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
  # Shared helpers
  # ---------------------------------------------------------------------------

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
