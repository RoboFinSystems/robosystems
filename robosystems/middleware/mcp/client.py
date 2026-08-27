"""MCP client for graph databases, backed by the Graph API."""

import asyncio
import json
import os
import time
from typing import Any

import httpx
from httpx import HTTPError, TimeoutException

from robosystems.config import env
from robosystems.config.tuning import TuningConfig
from robosystems.graph_api.client import GraphClient
from robosystems.logger import logger

from .exceptions import (
  GraphAPIError,
  GraphQueryComplexityError,
  GraphQueryTimeoutError,
)


class GraphMCPClient:
  """MCP client that reaches graph databases over the Graph API.

  Access is HTTP rather than an embedded database handle, so the client runs
  in processes that have no local database volume.
  """

  # Class-level cached configuration
  _config_cache = None
  _config_cache_time = 0

  @classmethod
  def _get_config_cache_ttl(cls) -> int:
    """Get config cache TTL from TuningConfig (runtime tunable via SSM)."""
    return TuningConfig.get_cache_schema_ttl()

  def __init__(
    self,
    api_base_url: str,
    timeout: int | None = None,
    query_timeout: int = 120,
    max_query_length: int = 50000,
    graph_id: str = "sec",
    **kwargs,
  ):
    """
    Initialize Graph MCP client.

    Args:
        api_base_url: Base URL for Graph API (e.g., http://graph-api:8001)
        timeout: HTTP request timeout in seconds
        query_timeout: Maximum query execution time in seconds
        max_query_length: Maximum allowed query length in characters
        graph_id: Graph/database identifier
    """
    self.api_base_url = api_base_url.rstrip("/")
    self.timeout = (
      timeout if timeout is not None else TuningConfig.get_graph_http_timeout()
    )
    self.query_timeout = query_timeout
    self.max_query_length = max_query_length
    self.graph_id = graph_id

    self._load_cached_config()

    # Read through centralized config so production resolves the key from
    # Secrets Manager rather than the environment.
    api_key = env.GRAPH_API_KEY

    self.graph_client = GraphClient(base_url=api_base_url, api_key=api_key)
    self.graph_client.graph_id = graph_id  # Set the graph_id for queries

    # Keep the httpx client for any non-graph HTTP operations if needed
    timeout_config = httpx.Timeout(
      connect=10.0,  # Connection timeout
      read=max(self.timeout, query_timeout + 10),  # Read timeout (query + buffer)
      write=10.0,  # Write timeout
      pool=5.0,  # Pool timeout
    )
    self.client = httpx.AsyncClient(timeout=timeout_config)

    logger.info(
      f"Initialized Graph MCP client for graph '{graph_id}' at {api_base_url} "
      f"(query_timeout={query_timeout}s, max_length={max_query_length})"
    )

  def _load_cached_config(self):
    """Load configuration with caching to avoid repeated env var reads."""
    # For testing, don't use cache - always read fresh values
    if os.getenv("PYTEST_CURRENT_TEST"):
      self.max_result_rows = TuningConfig.get_mcp_max_result_rows()
      self.auto_limit_enabled = env.MCP_AUTO_LIMIT_ENABLED
      return

    current_time = time.time()

    if (
      GraphMCPClient._config_cache is None
      or current_time - GraphMCPClient._config_cache_time
      > GraphMCPClient._get_config_cache_ttl()
    ):
      # max_result_rows comes from TuningConfig so it stays SSM-tunable.
      GraphMCPClient._config_cache = {
        "max_result_rows": TuningConfig.get_mcp_max_result_rows(),
        "auto_limit_enabled": env.MCP_AUTO_LIMIT_ENABLED,
      }
      GraphMCPClient._config_cache_time = current_time
      logger.debug("Refreshed MCP configuration cache")

    self.max_result_rows = GraphMCPClient._config_cache["max_result_rows"]
    self.auto_limit_enabled = GraphMCPClient._config_cache["auto_limit_enabled"]

  async def close(self):
    """Close HTTP clients in proper order."""
    # Close the Graph client first (handles database connections, may need httpx for cleanup)
    try:
      await self.graph_client.close()
    except Exception as e:
      logger.error(f"Error closing Graph client: {e}")

    # Then close the httpx client (after Graph client is done with any cleanup requests)
    try:
      await self.client.aclose()
    except Exception as e:
      logger.warning(f"Error closing httpx client: {e}")

  def _validate_query_complexity(self, cypher: str) -> None:
    """Reject queries whose size or shape risks exhausting server or client.

    Query length and subquery count are hard limits. Unfiltered scan patterns
    are only logged — a legitimate query can look the same — so they stay
    diagnostic rather than blocking.

    Args:
        cypher: Cypher query string to validate

    Raises:
        GraphQueryComplexityError: If the query exceeds a hard limit.
    """
    if len(cypher) > self.max_query_length:
      raise GraphQueryComplexityError(
        f"Query length {len(cypher)} exceeds maximum {self.max_query_length} characters"
      )

    cypher_upper = cypher.upper()

    # Check for potentially expensive operations
    risky_patterns = [
      ("MATCH ()", "Queries matching all nodes without filters"),
      ("MATCH ()-[]->()", "Queries matching all relationships without filters"),
      ("CARTESIAN", "Cartesian products can be very expensive"),
    ]

    for pattern, reason in risky_patterns:
      if pattern in cypher_upper:
        logger.warning(f"Potentially expensive query detected: {reason}")
        # For now just warn, could make this configurable

    # Count nested subqueries (basic heuristic)
    subquery_count = cypher_upper.count("CALL {") + cypher_upper.count("WITH ")
    if subquery_count > 10:
      raise GraphQueryComplexityError(
        f"Query has {subquery_count} subqueries/WITH clauses, which may be too complex"
      )

    logger.debug(
      f"Query complexity validation passed for {len(cypher)} character query"
    )

  async def execute_query(
    self, cypher: str, parameters: dict[str, Any] | None = None
  ) -> list[dict[str, Any]]:
    """
    Execute a Cypher query via Graph API with timeout and complexity controls.

    Args:
        cypher: Cypher query string
        parameters: Optional query parameters

    Returns:
        List of result dictionaries

    Raises:
        GraphAPIError: If query execution fails
        GraphQueryTimeoutError: If query times out
        GraphQueryComplexityError: If query is too complex
    """
    logger.info(f"MCP execute_query called with: {cypher[:100]}...")
    self._validate_query_complexity(cypher)

    # Auto-append LIMIT for MCP context safety.
    original_query = cypher
    cypher_upper = cypher.strip().upper()

    max_rows = self.max_result_rows
    auto_limit_enabled = self.auto_limit_enabled

    has_limit = "LIMIT" in cypher_upper
    has_return = "RETURN" in cypher_upper
    has_aggregation = self._has_aggregation_function(cypher_upper)

    if auto_limit_enabled and has_return and not has_limit and not has_aggregation:
      cypher = self._inject_limit_intelligently(cypher, max_rows)
      logger.info(
        f"MCP safety: Auto-injected LIMIT {max_rows} to prevent context exhaustion"
      )
    elif has_aggregation:
      logger.debug("MCP: Skipping auto-LIMIT for aggregation query")

    try:
      logger.info(
        f"MCP: Executing graph query (timeout={self.query_timeout}s): {cypher[:200]}..."
      )

      # Use GraphClient instead of direct HTTP calls
      try:
        result = await asyncio.wait_for(
          self.graph_client.query(
            cypher=cypher, graph_id=self.graph_id, parameters=parameters
          ),
          timeout=self.query_timeout,
        )
      except TimeoutError:
        error_msg = f"Query execution timed out after {self.query_timeout} seconds"
        logger.error(error_msg)
        raise GraphQueryTimeoutError(error_msg)

      # Ensure result is a dictionary (non-streaming mode)
      if not isinstance(result, dict):
        raise GraphAPIError(
          "Expected dictionary result from query, got async generator"
        )

      data = result.get("data", [])
      execution_time = result.get("execution_time_ms", 0)

      logger.info(f"MCP: Query returned {len(data)} rows in {execution_time:.2f}ms")
      logger.info(f"MCP: Full result object keys: {list(result.keys())}")
      logger.info(f"MCP: Result data preview: {data[:2] if data else 'No data'}")

      # Check if results were likely truncated
      if (
        auto_limit_enabled
        and len(data) == max_rows
        and "LIMIT" not in original_query.upper()
      ):
        logger.warning(
          f"MCP query results truncated at {max_rows} rows for context safety"
        )
        # Add truncation marker to help the LLM understand
        data.append(
          {
            "_mcp_note": "RESULTS_TRUNCATED",
            "_mcp_message": f"Results limited to {max_rows} rows for LLM context safety. Add explicit LIMIT to your query to control result size.",
            "_mcp_total_rows": f">={max_rows}",
          }
        )

      # Also check total result size to prevent memory issues
      max_size_mb = TuningConfig.get_mcp_max_result_size_mb()
      result_size_mb = len(json.dumps(data)) / (1024 * 1024)
      if result_size_mb > max_size_mb:
        logger.warning(
          f"MCP query results too large: {result_size_mb:.1f}MB exceeds {max_size_mb}MB limit"
        )
        # Truncate to first N rows that fit within size limit
        truncated_data = []
        current_size = 0
        for row in data:
          row_size = len(json.dumps(row))
          if (current_size + row_size) / (1024 * 1024) > max_size_mb:
            break
          truncated_data.append(row)
          current_size += row_size

        truncated_data.append(
          {
            "_mcp_note": "RESULTS_TRUNCATED_BY_SIZE",
            "_mcp_message": f"Results truncated due to size limit ({max_size_mb}MB). Retrieved {len(truncated_data)} of {len(data)} rows.",
            "_mcp_size_mb": f"{result_size_mb:.1f}",
          }
        )
        return truncated_data

      return data

    except GraphQueryTimeoutError:
      raise
    except GraphQueryComplexityError:
      raise
    except TimeoutException as e:
      error_msg = f"HTTP timeout executing query after {self.timeout}s: {e}"
      logger.error(error_msg)
      raise GraphQueryTimeoutError(error_msg)
    except HTTPError as e:
      # Log full error for debugging
      logger.error(f"HTTP error executing query: {e}")

      # Extract user-friendly message
      user_msg = "Query execution failed"
      try:
        if hasattr(e, "response") and e.response is not None:
          status_code = e.response.status_code
          if status_code == 400:
            user_msg = "Invalid query. Please check your syntax."
          elif status_code == 401:
            user_msg = "Authentication failed. Please check your credentials."
          elif status_code == 403:
            user_msg = "Access denied. You don't have permission for this operation."
          elif status_code == 404:
            user_msg = "Resource not found. Please check your graph ID."
          elif status_code == 429:
            user_msg = "Rate limit exceeded. Please try again later."
          elif status_code == 500:
            user_msg = "Server error. Please try again later."
          elif status_code == 503:
            user_msg = "Service temporarily unavailable. Please try again later."
          else:
            # Try to get detail from response
            try:
              error_detail = e.response.json()
              detail = error_detail.get("detail", "")
              # Sanitize the detail message
              if detail:
                user_msg = self._sanitize_error_message(
                  Exception(detail), "query execution"
                )
            except Exception:
              user_msg = f"Request failed with status {status_code}"
      except Exception:
        # If we can't parse the response, use generic message
        user_msg = self._sanitize_error_message(e, "query execution")

      raise GraphAPIError(user_msg)

    except Exception as e:
      # Log full error for debugging
      logger.error(f"Unexpected error executing query: {e}")
      # Return sanitized error to user
      user_msg = self._sanitize_error_message(e, "query execution")
      raise GraphAPIError(user_msg)

  async def get_schema(self) -> list[dict[str, Any]]:
    """
    Get database schema information (optimized for large databases).

    Returns schema structure without counts to ensure fast response times
    even on databases with 100M+ nodes.

    Returns:
        List of schema information dictionaries with structure details
    """
    try:
      # Get table information with explicit column names
      tables_query = "CALL SHOW_TABLES() RETURN id, name, type, comment"
      tables_result = await self.execute_query(tables_query)

      nodes: list[dict[str, Any]] = []
      relationships: list[dict[str, Any]] = []

      for table in tables_result:
        table_name = table.get("name", "")
        table_type = table.get("type", "")
        table_comment = table.get("comment", "")

        if not table_name:
          continue

        if table_type.upper() == "NODE":
          # Prefer the curated property map for well-known nodes (nice
          # ordering); for everything else introspect the real columns from
          # the catalog so the schema isn't reported as a misleading guess.
          properties = self._get_common_properties(table_name)
          if properties is None:
            properties = await self._introspect_node_properties(table_name)

          nodes.append(
            {
              "label": table_name,
              "type": "node",
              "comment": table_comment,
              "description": self._get_node_description(table_name),
              "properties": properties,
            }
          )

        elif table_type.upper() == "REL":
          # Infer relationship details without querying table structure
          from_node, to_node = self._infer_relationship_nodes(table_name)

          relationships.append(
            {
              "label": table_name,
              "type": "relationship",
              "comment": table_comment,
              "from_node": from_node,
              "to_node": to_node,
              "description": self._get_relationship_description(table_name),
            }
          )

      # Nodes first, then relationships, each alphabetical: the model reads
      # the labels before the edges that join them, and the payload is
      # byte-stable across calls (a prerequisite for prompt caching) rather
      # than following catalog insertion order.
      nodes.sort(key=lambda entry: entry["label"])
      relationships.sort(key=lambda entry: entry["label"])
      schema_info = nodes + relationships

      logger.info(f"Retrieved schema for {len(schema_info)} tables")
      return schema_info

    except Exception as e:
      logger.error(f"Failed to get schema: {e}")
      sanitized = self._sanitize_error_message(e, "schema retrieval")
      raise GraphAPIError(sanitized)

  def _get_common_properties(self, node_name: str) -> list[str] | None:
    """Curated query-hint properties for well-known node types.

    Returns ``None`` for node types not in the map so the caller introspects
    real columns from the catalog rather than reporting a generic guess.
    """
    common_props = {
      "Entity": [
        "name",
        "cik",
        "ticker",
        "identifier",
        "sic",
        "sic_description",
        "legal_name",
        "fiscal_year_end",
      ],
      "Fact": ["identifier", "value", "numeric_value", "fact_type", "decimals", "uri"],
      "Report": [
        "form",
        "filing_date",
        "report_date",
        "accession_number",
      ],
      "Period": [
        "start_date",
        "end_date",
        "calendar_year",
        "calendar_quarter",
        "period_type",
        "calendar_period_key",
      ],
      "Element": ["name", "qname", "balance", "item_type", "is_numeric", "is_abstract"],
      # The ledger spine (Transaction / Entry / LineItem / Event) is deliberately
      # absent: those tables carry the live-row filter (`is_live`, `status`)
      # a curated list would omit, so they are introspected from the catalog.
      "Account": ["name", "number", "type", "balance", "parent_account_id"],
      "User": ["id", "name", "email", "is_active", "created_at"],
      "Connection": ["provider", "status", "last_sync", "realm_id", "connection_id"],
      "Unit": ["measure", "numerator_uri", "denominator_uri"],
    }
    return common_props.get(node_name)

  async def _introspect_node_properties(self, node_name: str) -> list[str]:
    """Fetch real column names for a node table from the catalog.

    Uses ``CALL TABLE_INFO`` — a catalog lookup, not a data scan, so it is
    O(1) even on 100M+ node databases. Drops high-dimensional vector
    columns (embeddings) since they are noise as query hints. Falls back to
    the legacy generic list only if the catalog call fails.
    """
    try:
      rows = await self.execute_query(
        f"CALL TABLE_INFO('{node_name}') RETURN name, type"
      )
    except Exception as e:  # pragma: no cover - defensive catalog fallback
      logger.warning(f"TABLE_INFO introspection failed for {node_name}: {e}")
      return ["identifier", "name", "value", "created_at", "updated_at"]

    properties: list[str] = []
    for row in rows:
      col_name = row.get("name", "")
      col_type = (row.get("type", "") or "").upper()
      if not col_name:
        continue
      # Skip fixed-size array columns (e.g. embedding FLOAT[384]) — not
      # useful as a Cypher query hint and noisy in the schema output.
      if "[" in col_type:
        continue
      properties.append(col_name)
    return properties or ["identifier"]

  def _get_node_description(self, node_name: str) -> str:
    """Get description for common node types (base + roboledger extension only)."""
    descriptions = {
      # Base nodes
      "Entity": "Business entities (companies, subsidiaries) with financial data and SEC filings",
      "Period": "Time periods for financial reporting (instant/duration)",
      "Unit": "Measurement units (USD, shares, percentages)",
      "Element": "XBRL taxonomy elements defining financial concepts",
      "Label": "Human-readable labels for XBRL elements",
      "Reference": "Authoritative references for XBRL elements",
      "Taxonomy": "XBRL taxonomy definitions and structures",
      # RoboLedger extension nodes - Reporting section
      "Report": "SEC filings (10-K annual, 10-Q quarterly reports)",
      "Fact": "XBRL data points. Use has_dimensions=false for consolidated totals (excludes segment breakdowns)",
      "Structure": "XBRL presentation and calculation structures",
      "Dimension": "Dimensional qualifiers for financial data (segments, departments, geography, projects)",
      "Association": "XBRL calculation relationships between elements",
      "FactSet": "Logical groupings of related facts",
      # RoboLedger extension nodes - Transaction section (entity graphs only)
      "Transaction": "Financial transactions from accounting systems",
      "LineItem": "Individual accounting entries with debits/credits",
    }
    return descriptions.get(node_name, f"{node_name} entities in the graph")

  def _get_relationship_description(self, rel_name: str) -> str:
    """Get description for common relationship types (base + roboledger extension only)."""
    descriptions = {
      # Base relationships
      "ELEMENT_HAS_LABEL": "Links XBRL elements to their human-readable labels",
      "ELEMENT_HAS_REFERENCE": "Links elements to authoritative references",
      "TAXONOMY_HAS_LABEL": "Links taxonomies to their labels",
      "TAXONOMY_HAS_REFERENCE": "Links taxonomies to references",
      # RoboLedger extension relationships - Reporting section
      "ENTITY_HAS_REPORT": "Links companies to their SEC filings (10-K, 10-Q, etc.)",
      "REPORT_HAS_FACT": "Links reports to their financial data points",
      "FACT_HAS_ELEMENT": "Links facts to their XBRL taxonomy elements (concept definitions)",
      "FACT_HAS_ENTITY": "Links facts to the reporting entity",
      "FACT_HAS_PERIOD": "Links facts to their time periods (instant or duration)",
      "FACT_HAS_UNIT": "Links facts to their measurement units (USD, shares, etc.)",
      "STRUCTURE_HAS_TAXONOMY": "Links presentation structure to taxonomy",
      "FACT_HAS_DIMENSION": "Links facts to dimensional qualifiers",
      "DIMENSION_HAS_AXIS_ELEMENT": "Links dimensions to their axis element definitions",
      "DIMENSION_HAS_MEMBER_ELEMENT": "Links dimensions to their member element values",
      "FACT_SET_CONTAINS_FACT": "Groups related facts together",
      "REPORT_HAS_FACT_SET": "Groups related facts within a report",
      "REPORT_USES_TAXONOMY": "Links reports to their XBRL taxonomy definitions",
      "STRUCTURE_HAS_ASSOCIATION": "Links structure to calculation relationships",
      "ASSOCIATION_HAS_FROM_ELEMENT": "Source element in calculation relationship",
      "ASSOCIATION_HAS_TO_ELEMENT": "Target element in calculation relationship",
      # RoboLedger extension relationships - Transaction section (entity graphs only)
      "LINE_ITEM_HAS_DIMENSION": "Links line items to dimensional qualifiers (department, class, etc.)",
      # RoboLedger extension relationships - Transaction section (entity graphs only)
      "ENTITY_HAS_TRANSACTION": "Links entities to their financial transactions",
      "TRANSACTION_HAS_ENTRY": "Links transactions to their ledger entries",
      "ENTRY_HAS_LINE_ITEM": "Links ledger entries to their line items (debits/credits)",
      "TRANSACTION_HAS_DIMENSION": "Links transactions to dimensional qualifiers (source system, provenance)",
      "ENTRY_HAS_DIMENSION": "Links entries to dimensional qualifiers (fund, trust account, product channel)",
      "LINE_ITEM_RELATES_TO_ELEMENT": "Maps line items to XBRL elements for reporting",
    }
    return descriptions.get(rel_name, f"{rel_name} relationship in the graph")

  def _infer_relationship_nodes(self, rel_name: str) -> tuple[str, str]:
    """Resolve a relationship's source and target node labels.

    The declared schema (base + every extension) is authoritative for the
    platform's own relationships — several don't follow the naming patterns
    below (``ENTRY_FROM_SCHEDULE`` targets ``Structure``; the ``EVENT_*``
    verbs are neither HAS nor OWNS nor RELATES_TO). Name-pattern inference
    remains the fallback for custom graphs that add their own tables.
    """
    try:
      from robosystems.schemas.loader import get_schema_loader

      declared = get_schema_loader().get_relationship_schema(rel_name)
    except (
      Exception
    ) as e:  # pragma: no cover - never let a loader fault break schema retrieval
      logger.warning(f"Declared-schema lookup failed for {rel_name}: {e}")
      declared = None
    if declared is not None:
      return (declared.from_node, declared.to_node)

    # Legacy hand map, kept for graphs whose extensions were not discoverable.
    known_relationships = {
      # Base relationships
      "ELEMENT_HAS_LABEL": ("Element", "Label"),
      "ELEMENT_HAS_REFERENCE": ("Element", "Reference"),
      "TAXONOMY_HAS_LABEL": ("Taxonomy", "Label"),
      "TAXONOMY_HAS_REFERENCE": ("Taxonomy", "Reference"),
      # RoboLedger extension - Reporting section
      "ENTITY_HAS_REPORT": ("Entity", "Report"),
      "REPORT_HAS_FACT": ("Report", "Fact"),
      "FACT_HAS_ELEMENT": ("Fact", "Element"),
      "FACT_HAS_ENTITY": ("Fact", "Entity"),
      "FACT_HAS_PERIOD": ("Fact", "Period"),
      "FACT_HAS_UNIT": ("Fact", "Unit"),
      "STRUCTURE_HAS_TAXONOMY": ("Structure", "Taxonomy"),
      "FACT_HAS_DIMENSION": ("Fact", "Dimension"),
      "DIMENSION_HAS_AXIS_ELEMENT": ("Dimension", "Element"),
      "DIMENSION_HAS_MEMBER_ELEMENT": ("Dimension", "Element"),
      "FACT_SET_CONTAINS_FACT": ("FactSet", "Fact"),
      "REPORT_HAS_FACT_SET": ("Report", "FactSet"),
      "REPORT_USES_TAXONOMY": ("Report", "Taxonomy"),
      "STRUCTURE_HAS_ASSOCIATION": ("Structure", "Association"),
      "ASSOCIATION_HAS_FROM_ELEMENT": ("Association", "Element"),
      "ASSOCIATION_HAS_TO_ELEMENT": ("Association", "Element"),
      # RoboLedger extension - Transaction section
      "ENTITY_HAS_TRANSACTION": ("Entity", "Transaction"),
      "TRANSACTION_HAS_ENTRY": ("Transaction", "Entry"),
      "ENTRY_HAS_LINE_ITEM": ("Entry", "LineItem"),
      "TRANSACTION_HAS_DIMENSION": ("Transaction", "Dimension"),
      "ENTRY_HAS_DIMENSION": ("Entry", "Dimension"),
      "LINE_ITEM_RELATES_TO_ELEMENT": ("LineItem", "Element"),
      "LINE_ITEM_HAS_DIMENSION": ("LineItem", "Dimension"),
    }

    # Check if we have a known mapping
    if rel_name in known_relationships:
      return known_relationships[rel_name]

    # Try pattern-based inference for unknown relationships
    if "_HAS_" in rel_name:
      parts = rel_name.split("_HAS_")
      if len(parts) == 2:
        from_node = parts[0].replace("_", " ").title().replace(" ", "")
        to_node = parts[1].replace("_", " ").title().replace(" ", "")
        return (from_node, to_node)
    elif "_OWNS_" in rel_name:
      parts = rel_name.split("_OWNS_")
      if len(parts) == 2:
        from_node = parts[0].replace("_", " ").title().replace(" ", "")
        to_node = parts[1].replace("_", " ").title().replace(" ", "")
        return (from_node, to_node)
    elif "_RELATES_TO_" in rel_name:
      parts = rel_name.split("_RELATES_TO_")
      if len(parts) == 2:
        from_node = parts[0].replace("_", " ").title().replace(" ", "")
        to_node = parts[1].replace("_", " ").title().replace(" ", "")
        return (from_node, to_node)

    # If we can't infer, return Unknown
    return ("Unknown", "Unknown")

  async def get_graph_info(self) -> dict[str, Any]:
    """
    Get basic graph information and statistics (optimized for large databases).

    Uses efficient aggregation query for node counts. Skips relationship counts
    to ensure fast response times on 100M+ node databases.

    Returns:
        Dictionary with graph statistics
    """
    try:
      # Get API info
      response = await self.client.get(f"{self.api_base_url}/info")
      response.raise_for_status()
      api_info = response.json()

      # Get table information from database
      node_labels = []
      rel_labels = []
      total_nodes = 0

      try:
        tables_query = "CALL SHOW_TABLES() RETURN id, name, type, comment"
        tables = await self.execute_query(tables_query)

        # Extract node and relationship labels from tables
        for t in tables:
          table_type = t.get("type", "")
          label = t.get("name", "")
          if label:
            if table_type.upper() == "NODE":
              node_labels.append(label)
            elif table_type.upper() == "REL":
              rel_labels.append(label)

        # Use efficient aggregation query to get node counts in ONE query
        try:
          node_count_query = "MATCH (n) RETURN labels(n) as label, count(*) as cnt"
          node_count_result = await self.execute_query(node_count_query)
          for row in node_count_result:
            total_nodes += row.get("cnt", 0)
        except Exception as e:
          logger.warning(f"Could not get node counts: {e}")

      except Exception as e:
        logger.warning(f"Failed to get table info: {e}")

      return {
        "graph_id": self.graph_id,
        "total_nodes": total_nodes,
        "node_labels": node_labels,
        "relationship_types": rel_labels,
        "database_path": api_info.get("database_path", ""),
        "read_only": api_info.get("read_only", True),
        "uptime_seconds": api_info.get("uptime_seconds", 0),
      }

    except Exception as e:
      logger.error(f"Failed to get graph info: {e}")
      sanitized = self._sanitize_error_message(e, "graph info retrieval")
      raise GraphAPIError(sanitized)

  def _sanitize_error_message(
    self, error: Exception, context: str = "operation"
  ) -> str:
    """
    Sanitize error messages to prevent leaking internal details.

    For MCP context, we preserve query-related errors to help AI agents debug.

    Args:
        error: The exception to sanitize
        context: Context about what operation failed

    Returns:
        User-friendly error message
    """
    error_str = str(error)

    # For query errors, preserve helpful error messages for AI agents
    # These patterns indicate query issues that AI agents need to know about
    query_error_patterns = [
      "Parser exception",
      "Binder exception",
      "does not exist",
      "Cannot find property",
      "Invalid input",
      "Syntax error",
      "Unknown function",
      "Property not found",
      "Label not found",
      "Table .* does not exist",
      "Catalog exception",
      "Runtime exception",
    ]

    # Check if this is a query error that should be passed through
    import re

    from robosystems.config import env

    # Only preserve query errors in development/staging for debugging
    # In production, use generic messages to avoid schema leakage
    if env.ENVIRONMENT in ("dev", "staging"):
      for pattern in query_error_patterns:
        if re.search(pattern, error_str, re.IGNORECASE):
          # This is a query error - preserve it for AI debugging
          # Just remove any file paths but keep the error message
          sanitized = re.sub(r"/[\w/]+\.(py|cpp|h)", "[internal]", error_str)
          sanitized = re.sub(r"\bline \d+", "", sanitized)
          logger.debug(f"Preserving query error for MCP: {sanitized}")
          return sanitized
    else:
      # Production: log internally but return generic message
      for pattern in query_error_patterns:
        if re.search(pattern, error_str, re.IGNORECASE):
          logger.error(f"Query error in production (hidden from user): {error_str}")
          return "Query validation failed. Please check your query syntax."

    # Map common internal errors to user-friendly messages
    error_mappings = {
      # Connection/Network errors
      "connection refused": "Service temporarily unavailable. Please try again later.",
      "connection reset": "Connection interrupted. Please try again.",
      "timed out": f"Request timed out during {context}. Try a simpler query or increase timeout.",
      "name resolution failed": "Service configuration error. Please contact support.",
      # Authentication/Authorization
      "unauthorized": "Authentication required. Please check your credentials.",
      "forbidden": "Access denied. You don't have permission for this operation.",
      "invalid api key": "Invalid API key. Please check your credentials.",
      # Resource errors
      "out of memory": "Query requires too many resources. Try limiting results or simplifying the query.",
      "disk full": "Storage capacity exceeded. Please contact support.",
      "too many open files": "Resource limit reached. Please try again later.",
    }

    # Check if error contains sensitive patterns
    sensitive_patterns = [
      r"\b0x[0-9a-fA-F]+\b",  # Memory addresses (with word boundaries)
      r"/[\w/]+\.(py|cpp|h)",  # File paths
      r"\bline \d+",  # Line numbers
      r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b",  # IP addresses
      r"\bport \d+",  # Port numbers
      r"[\w_]+\.(db|lbug|kuzu|sqlite)",  # Database file names (lbug, legacy kuzu, sqlite)
    ]

    # Find matching user-friendly message
    error_lower = error_str.lower()
    for pattern, friendly_msg in error_mappings.items():
      if pattern in error_lower:
        logger.debug(f"Sanitized error containing '{pattern}': {error_str}")
        return friendly_msg

    # Remove sensitive information using regex
    sanitized = error_str
    for pattern in sensitive_patterns:
      sanitized = re.sub(pattern, "[REDACTED]", sanitized, flags=re.IGNORECASE)

    # If message was sanitized, log the original for debugging
    if sanitized != error_str:
      logger.debug(f"Sanitized error message: {error_str} -> {sanitized}")

    # Generic fallback messages based on exception type
    if isinstance(error, TimeoutError):
      return f"Operation timed out during {context}. Please try again."
    elif isinstance(error, ConnectionError):
      return "Connection error. Please check your network and try again."
    elif isinstance(error, ValueError):
      return f"Invalid input for {context}. Please check your parameters."

    # If no specific match, return generic message
    if sanitized == error_str:
      # No sensitive info found, but still be cautious
      return f"Error during {context}. Please try again or contact support if the issue persists."

    return sanitized

  def _has_aggregation_function(self, query_upper: str) -> bool:
    """
    Check if query contains aggregation functions that naturally limit results.

    Aggregation functions like COUNT, SUM, AVG, MIN, MAX typically return
    a single row or small number of rows, so LIMIT injection is unnecessary
    and can actually break the query semantics.

    Args:
        query_upper: Uppercase query string

    Returns:
        True if query contains aggregation functions
    """
    aggregation_functions = [
      "COUNT(",
      "SUM(",
      "AVG(",
      "MIN(",
      "MAX(",
      "COLLECT(",
      "GROUP BY",
      "DISTINCT",
      # Common patterns that indicate aggregation
      "COUNT{",  # Graph database COUNT subquery syntax
    ]

    return any(func in query_upper for func in aggregation_functions)

  def _inject_limit_intelligently(self, query: str, limit: int) -> str:
    """Append a LIMIT clause without changing the query's semantics.

    A LIMIT already present is left alone. UNION queries get a LIMIT on each
    branch so every branch is sampled rather than only the first. Otherwise
    the LIMIT lands at the very end, after any trailing ORDER BY, so the sort
    still governs which rows survive.

    Args:
        query: The Cypher query to modify
        limit: The limit value to inject

    Returns:
        Query with LIMIT appended.

    Examples:
        >>> _inject_limit_intelligently("MATCH (n) RETURN n", 100)
        "MATCH (n) RETURN n LIMIT 100"

        >>> _inject_limit_intelligently("MATCH (n) RETURN n ORDER BY n.name", 100)
        "MATCH (n) RETURN n ORDER BY n.name LIMIT 100"
    """
    import re

    # Normalize for analysis; the original is what gets returned unchanged.
    query_normalized = query.strip()
    query_upper = query_normalized.upper()

    if "LIMIT" in query_upper:
      return query

    if "UNION" in query_upper:
      parts = re.split(r"\bUNION\b", query_normalized, flags=re.IGNORECASE)
      limited_parts = []

      for part in parts:
        part_trimmed = part.strip()
        if part_trimmed and "RETURN" in part_trimmed.upper():
          limited_parts.append(self._inject_limit_to_simple_query(part_trimmed, limit))
        else:
          limited_parts.append(part)

      return " UNION ".join(limited_parts)

    return self._inject_limit_to_simple_query(query_normalized, limit)

  def _inject_limit_to_simple_query(self, query: str, limit: int) -> str:
    """
    Inject LIMIT to a simple (non-UNION) query.

    Handles ORDER BY and ensures LIMIT goes at the very end.
    """
    import re

    query_trimmed = query.rstrip()

    if query_trimmed.endswith(";"):
      query_trimmed = query_trimmed[:-1].rstrip()

    # Inject LIMIT after a trailing ORDER BY clause. Find the LAST "ORDER BY"
    # via a linear keyword scan — the previous `(.*)(\bORDER\s+BY\s+[^;]+?)$`
    # regex backtracks polynomially (O(n^2)) on queries WITHOUT a trailing
    # ORDER BY (py/polynomial-redos, verified catastrophic).
    order_by_matches = list(
      re.finditer(r"\bORDER\s+BY\s+", query_trimmed, re.IGNORECASE)
    )
    if order_by_matches:
      last = order_by_matches[-1]
      order_clause = query_trimmed[last.start() :]
      # Preserve the original contract: the clause runs to end with no ';'.
      if ";" not in order_clause:
        before_order = query_trimmed[: last.start()].rstrip()
        return f"{before_order} {order_clause} LIMIT {limit}"

    return f"{query_trimmed} LIMIT {limit}"

  def _is_read_only_query(self, query: str) -> bool:
    """Return True when the query contains no write operation.

    Keyword matching at word boundaries, so identifiers like `CREATED_AT`
    don't read as a `CREATE`.
    """
    query_normalized = query.strip().upper()

    # List of write operation keywords that should be blocked
    write_operations = [
      # Data modification
      "CREATE",
      "SET",
      "DELETE",
      "REMOVE",
      "MERGE",
      # Schema modification
      "DROP",
      "ALTER",
      "ADD",
      "DETACH",
      # Index/constraint operations
      "INDEX",
      "CONSTRAINT",
      # Database operations
      "START",
      "COMMIT",
      "ROLLBACK",
      # Procedure calls that might modify data
      "CALL DB.",
      "CALL APOC.",
    ]

    # Check for write operations at word boundaries
    import re

    for operation in write_operations:
      # Use word boundary to avoid false positives (e.g., "CREATED_AT" field)
      pattern = r"\b" + operation + r"\b"
      if re.search(pattern, query_normalized):
        logger.warning(f"Blocked write operation '{operation}' in query")
        return False

    # Additional checks for sneaky patterns
    # Check for property setting with =
    if re.search(r"\s+SET\s+\w+\s*=", query_normalized):
      logger.warning("Blocked SET property operation")
      return False

    # Check for relationship creation patterns
    if re.search(r"-\[(\w+)?\]->", query_normalized) and "CREATE" in query_normalized:
      logger.warning("Blocked relationship creation")
      return False

    # If we get here, query appears to be read-only
    return True
