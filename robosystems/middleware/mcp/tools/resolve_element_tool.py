"""
Resolve Element Tool — Maps natural-language financial concepts to XBRL elements.

Vector-search-first architecture:
  1. Taxonomy match: embed query → match canonical concept in-memory (<1ms)
  2. Element vector search: HNSW index on Element embeddings — O(log N) at any scale
  3. Fact count enrichment: primary key + CSR traversal (indexed)
  4. Label vector search fallback: HNSW index on Label embeddings
"""

from typing import Any

from robosystems.logger import logger

from .base_tool import BaseTool


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
          "accession_number": {
            "type": "string",
            "description": "Optional: filter to elements used in a specific filing (e.g. '0001045810-25-000023')",
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
    accession_number = (
      arguments.get("accession_number", "").strip()
      if arguments.get("accession_number")
      else None
    )

    if not concept:
      return {"error": "concept is required"}

    return await self._resolve(concept, ticker, accession_number)

  async def _resolve(
    self, concept: str, ticker: str | None, accession_number: str | None = None
  ) -> dict[str, Any]:
    enricher = self.enricher
    result: dict[str, Any] = {
      "concept": concept,
      "ticker": ticker,
      "accession_number": accession_number,
      "canonical_id": None,
      "canonical_name": None,
      "matches": [],
      "query_hint": None,
    }

    # Step 1: Embed the query (~10ms)
    try:
      query_embedding = enricher.embed_batch([concept])[0]
    except Exception as e:
      logger.error(f"Failed to embed concept query: {e}")
      result["error"] = f"Embedding failed: {e}"
      return result

    # Step 2: Match to canonical taxonomy in-memory (<1ms for ~40 concepts)
    canonical = enricher.match_canonical_from_query(query_embedding)
    if canonical:
      result["canonical_id"] = canonical.id
      result["canonical_name"] = canonical.display_name

    # Step 3: HNSW vector search on Element embeddings — O(log N)
    # LadybugDB QUERY_VECTOR_INDEX returns (node, distance) where distance is
    # cosine distance (0 = identical, 2 = opposite). Lower = more similar.
    vec_str = str(query_embedding)
    search_results = []
    try:
      search_query = (
        f"CALL QUERY_VECTOR_INDEX('Element', 'element_vec_index', {vec_str}, 20) "
        f"RETURN node.identifier AS id, node.qname AS qname, "
        f"node.canonical_concept AS canonical, "
        f"node.canonical_confidence AS confidence, distance "
        f"ORDER BY distance"
      )
      search_results = await self.client.execute_query(search_query) or []
    except Exception as e:
      logger.warning(f"Element vector search failed: {e}")

    # Deduplicate by qname (same element may appear from different filings)
    seen_qnames: set[str] = set()
    unique_results = []
    for row in search_results:
      qname = row.get("qname")
      if qname and qname not in seen_qnames:
        seen_qnames.add(qname)
        unique_results.append(row)

    # Step 4: Enrich with fact counts (primary key hash index + CSR traversal)
    fact_counts: dict[str, int] = {}
    labels: dict[str, str] = {}
    if unique_results:
      element_ids = [r["id"] for r in unique_results if r.get("id")]
      if element_ids:
        ids_str = ", ".join(f'"{eid}"' for eid in element_ids)

        # Fact count query — uses primary key index for element lookup,
        # CSR index for relationship traversal
        try:
          if accession_number:
            fact_query = (
              f"MATCH (r:Report)-[:REPORT_HAS_FACT]->(f:Fact)-[:FACT_HAS_ELEMENT]->(e:Element) "
              f'WHERE e.identifier IN [{ids_str}] AND r.accession_number = "{accession_number}" '
              f"RETURN e.identifier AS id, count(f) AS fact_count"
            )
          elif ticker:
            fact_query = (
              f"MATCH (f:Fact)-[:FACT_HAS_ELEMENT]->(e:Element), "
              f"(f)-[:FACT_HAS_ENTITY]->(ent:Entity) "
              f'WHERE e.identifier IN [{ids_str}] AND ent.ticker = "{ticker}" '
              f"RETURN e.identifier AS id, count(f) AS fact_count"
            )
          else:
            fact_query = (
              f"MATCH (f:Fact)-[:FACT_HAS_ELEMENT]->(e:Element) "
              f"WHERE e.identifier IN [{ids_str}] "
              f"RETURN e.identifier AS id, count(f) AS fact_count"
            )
          fact_rows = await self.client.execute_query(fact_query) or []
          fact_counts = {r["id"]: r["fact_count"] for r in fact_rows}
        except Exception as e:
          logger.debug(f"Fact count enrichment failed: {e}")

        # Label query — uses primary key index + CSR traversal
        try:
          label_query = (
            f"MATCH (e:Element)-[:ELEMENT_HAS_LABEL]->(l:Label) "
            f"WHERE e.identifier IN [{ids_str}] "
            f'AND l.type = "http://www.xbrl.org/2003/role/label" '
            f"RETURN e.identifier AS id, l.value AS label"
          )
          label_rows = await self.client.execute_query(label_query) or []
          labels = {r["id"]: r["label"] for r in label_rows if r.get("label")}
        except Exception as e:
          logger.debug(f"Label enrichment failed: {e}")

    # Build matches list (convert cosine distance to similarity: 1 - distance)
    for row in unique_results:
      eid = row.get("id")
      fc = fact_counts.get(eid, 0)

      # When filtering by ticker or accession, skip elements not in that scope
      if (ticker or accession_number) and fc == 0:
        continue

      distance = row.get("distance", 1.0)
      similarity = round(1.0 - distance, 4)

      result["matches"].append(
        {
          "qname": row.get("qname"),
          "confidence": row.get("confidence"),
          "label": labels.get(eid),
          "fact_count": fc if fc > 0 else None,
          "score": similarity,
        }
      )

    # Sort: fact_count (descending) first, then similarity score
    result["matches"].sort(
      key=lambda m: (m.get("fact_count") or 0, m.get("score") or 0),
      reverse=True,
    )
    result["matches"] = result["matches"][:10]

    # Step 5: Label vector search fallback if too few results
    if len(result["matches"]) < 3:
      try:
        label_search_query = (
          f"CALL QUERY_VECTOR_INDEX('Label', 'label_vec_index', {vec_str}, 20) "
          f"WITH node, distance "
          f"MATCH (e:Element)-[:ELEMENT_HAS_LABEL]->(node) "
          f"RETURN DISTINCT e.qname AS qname, node.value AS label, distance "
          f"ORDER BY distance LIMIT 10"
        )
        label_search_results = await self.client.execute_query(label_search_query) or []
        existing_qnames = {m["qname"] for m in result["matches"]}
        for row in label_search_results:
          qname = row.get("qname")
          if qname and qname not in existing_qnames:
            dist = row.get("distance", 1.0)
            sim = round(1.0 - dist, 4)
            result["matches"].append(
              {
                "qname": qname,
                "confidence": sim,
                "label": row.get("label"),
                "fact_count": None,
                "score": sim,
              }
            )
            existing_qnames.add(qname)
      except Exception as e:
        logger.debug(f"Label vector search fallback failed: {e}")

    # Build query_hint from top match
    if result["matches"]:
      top = result["matches"][0]
      qname = top["qname"]
      if accession_number:
        result["query_hint"] = (
          f"MATCH (r:Report)-[:REPORT_HAS_FACT]->(f:Fact)-[:FACT_HAS_ELEMENT]->(e:Element), "
          f"(f)-[:FACT_HAS_PERIOD]->(p:Period) "
          f'WHERE e.qname = "{qname}" AND r.accession_number = "{accession_number}" '
          f"AND f.has_dimensions = false "
          f"RETURN e.qname AS element, f.numeric_value AS value, "
          f"p.end_date AS date, p.duration_type AS period_type "
          f"ORDER BY p.end_date DESC LIMIT 20"
        )
      elif ticker:
        result["query_hint"] = (
          f"MATCH (f:Fact)-[:FACT_HAS_ELEMENT]->(e:Element), "
          f"(f)-[:FACT_HAS_PERIOD]->(p:Period), "
          f"(f)-[:FACT_HAS_ENTITY]->(ent:Entity) "
          f'WHERE e.qname = "{qname}" AND ent.ticker = "{ticker}" '
          f"AND f.has_dimensions = false "
          f"RETURN ent.ticker AS ticker, e.qname AS element, "
          f"f.numeric_value AS value, p.end_date AS date, "
          f"p.duration_type AS period_type "
          f"ORDER BY p.end_date DESC LIMIT 20"
        )
      else:
        result["query_hint"] = (
          f"MATCH (f:Fact)-[:FACT_HAS_ELEMENT]->(e:Element), "
          f"(f)-[:FACT_HAS_PERIOD]->(p:Period) "
          f'WHERE e.qname = "{qname}" '
          f"AND f.has_dimensions = false "
          f"RETURN e.qname AS element, f.numeric_value AS value, "
          f"p.end_date AS date, p.duration_type AS period_type "
          f"ORDER BY p.end_date DESC LIMIT 20"
        )

    return result
