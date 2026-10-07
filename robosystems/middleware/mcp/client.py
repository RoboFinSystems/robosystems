"""MCP client for graph databases, backed by the Graph API."""

import asyncio
import json
import os
import re
import time
from typing import Any

import httpx
from httpx import HTTPError, TimeoutException

from robosystems.config import env
from robosystems.config.tuning import TuningConfig
from robosystems.graph_api.client import GraphClient
from robosystems.graph_api.client.cypher_text import mask_literals
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

  _config_cache = None
  _config_cache_time = 0

  @classmethod
  def _get_config_cache_ttl(cls) -> int:
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
    """Timeouts are in seconds; `max_query_length` is in characters."""
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
    self.graph_client.graph_id = graph_id

    timeout_config = httpx.Timeout(
      connect=10.0,
      read=max(self.timeout, query_timeout + 10),
      write=10.0,
      pool=5.0,
    )
    self.client = httpx.AsyncClient(timeout=timeout_config)

    logger.info(
      f"Initialized Graph MCP client for graph '{graph_id}' at {api_base_url} "
      f"(query_timeout={query_timeout}s, max_length={max_query_length})"
    )

  def _load_cached_config(self):
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
      GraphMCPClient._config_cache = {
        "max_result_rows": TuningConfig.get_mcp_max_result_rows(),
        "auto_limit_enabled": env.MCP_AUTO_LIMIT_ENABLED,
      }
      GraphMCPClient._config_cache_time = current_time
      logger.debug("Refreshed MCP configuration cache")

    self.max_result_rows = GraphMCPClient._config_cache["max_result_rows"]
    self.auto_limit_enabled = GraphMCPClient._config_cache["auto_limit_enabled"]

  async def close(self):
    """Close the Graph client before httpx; its cleanup may still use httpx."""
    try:
      await self.graph_client.close()
    except Exception as e:
      logger.error(f"Error closing Graph client: {e}")

    try:
      await self.client.aclose()
    except Exception as e:
      logger.warning(f"Error closing httpx client: {e}")

  def _validate_query_complexity(self, cypher: str) -> None:
    """Raise GraphQueryComplexityError past the length or subquery limit.

    Unfiltered scan patterns are only logged: a legitimate query can look
    the same.
    """
    if len(cypher) > self.max_query_length:
      raise GraphQueryComplexityError(
        f"Query length {len(cypher)} exceeds maximum {self.max_query_length} characters"
      )

    cypher_upper = cypher.upper()

    risky_patterns = [
      ("MATCH ()", "Queries matching all nodes without filters"),
      ("MATCH ()-[]->()", "Queries matching all relationships without filters"),
      ("CARTESIAN", "Cartesian products can be very expensive"),
    ]

    for pattern, reason in risky_patterns:
      if pattern in cypher_upper:
        logger.warning(f"Potentially expensive query detected: {reason}")

    subquery_count = cypher_upper.count("CALL {") + cypher_upper.count("WITH ")
    if subquery_count > 10:
      raise GraphQueryComplexityError(
        f"Query has {subquery_count} subqueries/WITH clauses, which may be too complex"
      )

    logger.debug(
      f"Query complexity validation passed for {len(cypher)} character query"
    )

  def prepare_read_query(self, cypher: str) -> str:
    """Apply the complexity check and the MCP row cap to a read statement.

    The final RETURN of each UNION branch is capped at ``max_result_rows``:
    a LIMIT is added where there is none, and a larger literal one is
    lowered. DISTINCT and aggregations are capped too, since a LIMIT after
    the final RETURN only caps output rows.
    """
    self._validate_query_complexity(cypher)
    if not self.auto_limit_enabled:
      return cypher

    capped = self._cap_rows(cypher, self.max_result_rows)
    if capped != cypher:
      logger.info(f"MCP safety: capped result rows at {self.max_result_rows}")
    return capped

  async def execute_query(
    self, cypher: str, parameters: dict[str, Any] | None = None
  ) -> list[dict[str, Any]]:
    """Run a read query with the auto-LIMIT and size caps applied.

    Raises GraphAPIError, GraphQueryTimeoutError or GraphQueryComplexityError.
    """
    logger.info(f"MCP execute_query called with: {cypher[:100]}...")
    original_query = cypher
    cypher = self.prepare_read_query(cypher)
    max_rows = self.max_result_rows
    auto_limit_enabled = self.auto_limit_enabled

    try:
      logger.info(
        f"MCP: Executing graph query (timeout={self.query_timeout}s): {cypher[:200]}..."
      )

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

      if not isinstance(result, dict):
        raise GraphAPIError(
          "Expected dictionary result from query, got async generator"
        )

      data = result.get("data", [])
      execution_time = result.get("execution_time_ms", 0)

      logger.info(f"MCP: Query returned {len(data)} rows in {execution_time:.2f}ms")
      logger.info(f"MCP: Full result object keys: {list(result.keys())}")
      logger.info(f"MCP: Result data preview: {data[:2] if data else 'No data'}")

      # Backstop for a LIMIT the text cap cannot read ($param, an expression).
      over_cap = auto_limit_enabled and len(data) > max_rows
      if over_cap:
        del data[max_rows:]
      if (
        auto_limit_enabled
        and len(data) == max_rows
        and (over_cap or cypher != original_query)
      ):
        logger.warning(
          f"MCP query results truncated at {max_rows} rows for context safety"
        )
        data.append(
          {
            "_mcp_note": "RESULTS_TRUNCATED",
            "_mcp_message": f"Results capped at {max_rows} rows for LLM context safety. Narrow the query with filters or aggregation to see the rest.",
            "_mcp_total_rows": f">={max_rows}",
          }
        )

      max_size_mb = TuningConfig.get_mcp_max_result_size_mb()
      result_size_mb = len(json.dumps(data)) / (1024 * 1024)
      if result_size_mb > max_size_mb:
        logger.warning(
          f"MCP query results too large: {result_size_mb:.1f}MB exceeds {max_size_mb}MB limit"
        )
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
      logger.error(f"HTTP error executing query: {e}")

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
            try:
              error_detail = e.response.json()
              detail = error_detail.get("detail", "")
              if detail:
                user_msg = self._sanitize_error_message(
                  Exception(detail), "query execution"
                )
            except Exception:
              user_msg = f"Request failed with status {status_code}"
      except Exception:
        user_msg = self._sanitize_error_message(e, "query execution")

      raise GraphAPIError(user_msg)

    except Exception as e:
      logger.error(f"Unexpected error executing query: {e}")
      user_msg = self._sanitize_error_message(e, "query execution")
      raise GraphAPIError(user_msg)

  async def get_schema(self) -> list[dict[str, Any]]:
    """Node and relationship tables, without counts so it stays fast at 100M+ nodes."""
    try:
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
          # Curated map for well-known nodes, real catalog columns otherwise.
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

      # Nodes before edges, each alphabetical: byte-stable output keeps
      # prompt caching effective.
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
    """Curated query-hint properties; ``None`` means introspect the catalog."""
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
      # The ledger spine (Transaction / Entry / LineItem / Event) is absent on
      # purpose: introspection surfaces its live-row filter (`is_live`, `status`).
      "Account": ["name", "number", "type", "balance", "parent_account_id"],
      "User": ["id", "name", "email", "is_active", "created_at"],
      "Connection": ["provider", "status", "last_sync", "realm_id", "connection_id"],
      "Unit": ["measure", "numerator_uri", "denominator_uri"],
    }
    return common_props.get(node_name)

  async def _introspect_node_properties(self, node_name: str) -> list[str]:
    """Column names via ``CALL TABLE_INFO`` (a catalog lookup, not a scan)."""
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
      # Fixed-size arrays (embeddings) are noise as query hints.
      if "[" in col_type:
        continue
      properties.append(col_name)
    return properties or ["identifier"]

  def _get_node_description(self, node_name: str) -> str:
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
      "ENTITY_OWNS_ENTITY": "Links a group parent to each subsidiary that keeps its books in the graph",
      "ENTITY_HAS_TRANSACTION": "Links entities to their financial transactions",
      "TRANSACTION_HAS_ENTRY": "Links transactions to their ledger entries",
      "ENTRY_HAS_LINE_ITEM": "Links ledger entries to their line items (debits/credits)",
      "TRANSACTION_HAS_DIMENSION": "Links transactions to dimensional qualifiers (source system, provenance)",
      "ENTRY_HAS_DIMENSION": "Links entries to dimensional qualifiers (fund, trust account, product channel)",
      "LINE_ITEM_RELATES_TO_ELEMENT": "Maps line items to XBRL elements for reporting",
    }
    return descriptions.get(rel_name, f"{rel_name} relationship in the graph")

  def _infer_relationship_nodes(self, rel_name: str) -> tuple[str, str]:
    """Source and target labels: the declared schema first (several platform
    relationships break the naming patterns), then name-pattern inference for
    custom tables."""
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

    # For graphs whose extensions were not discoverable.
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
      "ENTITY_OWNS_ENTITY": ("Entity", "Entity"),
      "ENTITY_HAS_TRANSACTION": ("Entity", "Transaction"),
      "TRANSACTION_HAS_ENTRY": ("Transaction", "Entry"),
      "ENTRY_HAS_LINE_ITEM": ("Entry", "LineItem"),
      "TRANSACTION_HAS_DIMENSION": ("Transaction", "Dimension"),
      "ENTRY_HAS_DIMENSION": ("Entry", "Dimension"),
      "LINE_ITEM_RELATES_TO_ELEMENT": ("LineItem", "Element"),
      "LINE_ITEM_HAS_DIMENSION": ("LineItem", "Dimension"),
    }

    if rel_name in known_relationships:
      return known_relationships[rel_name]

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

    return ("Unknown", "Unknown")

  async def get_graph_info(self) -> dict[str, Any]:
    """Graph statistics; relationship counts are skipped to stay fast at scale."""
    try:
      # Through GraphClient so it carries X-Graph-API-Key like every other call.
      api_info = await self.graph_client.get_info()

      node_labels = []
      rel_labels = []
      total_nodes = 0

      try:
        tables_query = "CALL SHOW_TABLES() RETURN id, name, type, comment"
        tables = await self.execute_query(tables_query)

        for t in tables:
          table_type = t.get("type", "")
          label = t.get("name", "")
          if label:
            if table_type.upper() == "NODE":
              node_labels.append(label)
            elif table_type.upper() == "REL":
              rel_labels.append(label)

        try:
          node_count_query = "MATCH (n) RETURN labels(n) as label, count(*) as cnt"
          node_count_result = await self.execute_query(node_count_query)
          for row in node_count_result:
            total_nodes += row.get("cnt", 0)
        except Exception as e:
          logger.warning(f"Could not get node counts: {e}")

      except Exception as e:
        logger.warning(f"Failed to get table info: {e}")

      info: dict[str, Any] = {
        "graph_id": self.graph_id,
        "total_nodes": total_nodes,
        "node_labels": node_labels,
        "relationship_types": rel_labels,
        "database_path": api_info.get("database_path", ""),
        "read_only": api_info.get("read_only", True),
        "uptime_seconds": api_info.get("uptime_seconds", 0),
      }
      entities = await self._ledger_entities()
      if entities is not None:
        info["entities"] = entities
      return info

    except Exception as e:
      logger.error(f"Failed to get graph info: {e}")
      sanitized = self._sanitize_error_message(e, "graph info retrieval")
      raise GraphAPIError(sanitized)

  async def _ledger_entities(self) -> list[dict[str, Any]] | None:
    """The reporting group of a ledger graph; None on a shared repository or
    a graph with no ledger."""
    from robosystems.config.shared_repositories import (
      is_shared_repository_or_subgraph,
    )
    from robosystems.middleware.mcp.entities import ledger_entities
    from robosystems.middleware.operations import run_off_loop

    if is_shared_repository_or_subgraph(self.graph_id):
      return None
    return await run_off_loop(ledger_entities, self.graph_id)

  def _sanitize_error_message(
    self, error: Exception, context: str = "operation"
  ) -> str:
    """Strip internal details; query errors pass through outside production."""
    error_str = str(error)

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

    import re

    from robosystems.config import env

    # Production hides query errors to avoid leaking schema.
    if env.ENVIRONMENT in ("dev", "staging"):
      for pattern in query_error_patterns:
        if re.search(pattern, error_str, re.IGNORECASE):
          sanitized = re.sub(r"/[\w/]+\.(py|cpp|h)", "[internal]", error_str)
          sanitized = re.sub(r"\bline \d+", "", sanitized)
          logger.debug(f"Preserving query error for MCP: {sanitized}")
          return sanitized
    else:
      for pattern in query_error_patterns:
        if re.search(pattern, error_str, re.IGNORECASE):
          logger.error(f"Query error in production (hidden from user): {error_str}")
          return "Query validation failed. Please check your query syntax."

    error_mappings = {
      "connection refused": "Service temporarily unavailable. Please try again later.",
      "connection reset": "Connection interrupted. Please try again.",
      "timed out": f"Request timed out during {context}. Try a simpler query or increase timeout.",
      "name resolution failed": "Service configuration error. Please contact support.",
      "unauthorized": "Authentication required. Please check your credentials.",
      "forbidden": "Access denied. You don't have permission for this operation.",
      "invalid api key": "Invalid API key. Please check your credentials.",
      "out of memory": "Query requires too many resources. Try limiting results or simplifying the query.",
      "disk full": "Storage capacity exceeded. Please contact support.",
      "too many open files": "Resource limit reached. Please try again later.",
    }

    sensitive_patterns = [
      r"\b0x[0-9a-fA-F]+\b",  # Memory addresses (with word boundaries)
      r"/[\w/]+\.(py|cpp|h)",  # File paths
      r"\bline \d+",  # Line numbers
      r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b",  # IP addresses
      r"\bport \d+",  # Port numbers
      r"[\w_]+\.(db|lbug|kuzu|sqlite)",  # Database file names (lbug, legacy kuzu, sqlite)
    ]

    error_lower = error_str.lower()
    for pattern, friendly_msg in error_mappings.items():
      if pattern in error_lower:
        logger.debug(f"Sanitized error containing '{pattern}': {error_str}")
        return friendly_msg

    sanitized = error_str
    for pattern in sensitive_patterns:
      sanitized = re.sub(pattern, "[REDACTED]", sanitized, flags=re.IGNORECASE)

    if sanitized != error_str:
      logger.debug(f"Sanitized error message: {error_str} -> {sanitized}")

    if isinstance(error, TimeoutError):
      return f"Operation timed out during {context}. Please try again."
    elif isinstance(error, ConnectionError):
      return "Connection error. Please check your network and try again."
    elif isinstance(error, ValueError):
      return f"Invalid input for {context}. Please check your parameters."

    if sanitized == error_str:
      return f"Error during {context}. Please try again or contact support if the issue persists."

    return sanitized

  def _cap_rows(self, query: str, cap: int) -> str:
    """Cap the final RETURN of each UNION branch at ``cap`` rows.

    Clauses are found in a copy with literals and comments blanked, so a
    keyword inside either is never taken for one. A LIMIT given as a
    parameter or expression is left alone.
    """
    masked = mask_literals(query)
    parts: list[str] = []
    start = 0
    for union in _UNION_RE.finditer(masked):
      parts.append(
        _cap_branch(query[start : union.start()], masked[start : union.start()], cap)
      )
      parts.append(query[union.start() : union.end()])
      start = union.end()
    parts.append(_cap_branch(query[start:], masked[start:], cap))
    capped = "".join(parts)
    return query if capped == query else capped.rstrip()


_UNION_RE = re.compile(r"(?<![\w.$])UNION(?:\s+ALL)?\b", re.IGNORECASE)
_RETURN_RE = re.compile(r"(?<![\w.$])RETURN\b", re.IGNORECASE)
_LIMIT_RE = re.compile(r"(?<![\w.$])LIMIT\s+(\d+(?!\w)|\S+)", re.IGNORECASE)


def _cap_branch(query: str, masked: str, cap: int) -> str:
  returns = list(_RETURN_RE.finditer(masked))
  if not returns:
    return query

  limits = list(_LIMIT_RE.finditer(masked, returns[-1].end()))
  if limits:
    value = limits[-1]
    if value.group(1).isdigit() and int(value.group(1)) > cap:
      return f"{query[: value.start(1)]}{cap}{query[value.end(1) :]}"
    return query

  end = len(masked.rstrip())
  if masked[:end].endswith(";"):
    return f"{query[: end - 1].rstrip()} LIMIT {cap}{query[end:]}"
  return f"{query[:end]} LIMIT {cap}{query[end:]}"
