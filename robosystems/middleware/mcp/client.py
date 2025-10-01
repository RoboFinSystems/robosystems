"""
Kuzu MCP Client - Main client class for Model Context Protocol adapter.

This module contains the KuzuMCPClient class which provides MCP functionality
using the Kuzu API instead of direct database connections.
"""

import json
import asyncio
import os
import time
from typing import Any, Dict, List, Optional

import httpx
from httpx import HTTPError, TimeoutException

from robosystems.logger import logger
from robosystems.config import env
from robosystems.kuzu_api.client import KuzuClient

from .exceptions import KuzuAPIError, KuzuQueryTimeoutError, KuzuQueryComplexityError


class KuzuMCPClient:
  """
  MCP client that communicates with Kuzu databases via REST API.

  This replaces direct graph database connections with HTTP-based Kuzu API calls,
  enabling deployment in read-only cluster environments.
  """

  # Class-level cached configuration
  _config_cache = None
  _config_cache_time = 0
  _config_cache_ttl = 300  # 5 minutes

  def __init__(
    self,
    api_base_url: str,
    timeout: int = None,
    query_timeout: int = 120,
    max_query_length: int = 50000,
    graph_id: str = "sec",
    **kwargs,
  ):
    """
    Initialize Kuzu MCP client.

    Args:
        api_base_url: Base URL for Kuzu API (e.g., http://kuzu-api:8001)
        timeout: HTTP request timeout in seconds
        query_timeout: Maximum query execution time in seconds
        max_query_length: Maximum allowed query length in characters
        graph_id: Graph/database identifier
    """
    self.api_base_url = api_base_url.rstrip("/")
    self.timeout = timeout if timeout is not None else env.KUZU_HTTP_TIMEOUT
    self.query_timeout = query_timeout
    self.max_query_length = max_query_length
    self.graph_id = graph_id

    # Load and cache configuration
    self._load_cached_config()

    # Create KuzuClient with unified API key from centralized config
    # This ensures we get the key from Secrets Manager in production
    api_key = env.KUZU_API_KEY

    self.kuzu_client = KuzuClient(base_url=api_base_url, api_key=api_key)
    self.kuzu_client.graph_id = graph_id  # Set the graph_id for queries

    # Keep the httpx client for any non-Kuzu HTTP operations if needed
    timeout_config = httpx.Timeout(
      connect=10.0,  # Connection timeout
      read=max(self.timeout, query_timeout + 10),  # Read timeout (query + buffer)
      write=10.0,  # Write timeout
      pool=5.0,  # Pool timeout
    )
    self.client = httpx.AsyncClient(timeout=timeout_config)

    logger.info(
      f"Initialized Kuzu MCP client for graph '{graph_id}' at {api_base_url} "
      f"(query_timeout={query_timeout}s, max_length={max_query_length})"
    )

  def _load_cached_config(self):
    """Load configuration with caching to avoid repeated env var reads."""
    # For testing, don't use cache - always read fresh values
    if os.getenv("PYTEST_CURRENT_TEST"):
      self.max_result_rows = env.MCP_MAX_RESULT_ROWS
      self.auto_limit_enabled = env.MCP_AUTO_LIMIT_ENABLED
      return

    current_time = time.time()

    # Check if we need to refresh the cache
    if (
      KuzuMCPClient._config_cache is None
      or current_time - KuzuMCPClient._config_cache_time
      > KuzuMCPClient._config_cache_ttl
    ):
      # Load configuration from environment
      KuzuMCPClient._config_cache = {
        "max_result_rows": env.MCP_MAX_RESULT_ROWS,
        "auto_limit_enabled": env.MCP_AUTO_LIMIT_ENABLED,
      }
      KuzuMCPClient._config_cache_time = current_time
      logger.debug("Refreshed MCP configuration cache")

    # Apply cached configuration to instance
    self.max_result_rows = KuzuMCPClient._config_cache["max_result_rows"]
    self.auto_limit_enabled = KuzuMCPClient._config_cache["auto_limit_enabled"]

  async def close(self):
    """Close HTTP clients in proper order."""
    # Close the Kuzu client first (handles database connections, may need httpx for cleanup)
    try:
      await self.kuzu_client.close()
    except Exception as e:
      logger.error(f"Error closing Kuzu client: {e}")

    # Then close the httpx client (after Kuzu client is done with any cleanup requests)
    try:
      await self.client.aclose()
    except Exception as e:
      logger.warning(f"Error closing httpx client: {e}")

  def _validate_query_complexity(self, cypher: str) -> None:
    """
    Validate query complexity to prevent resource exhaustion and protect MCP clients.

    This method implements a multi-layered defense against queries that could:
    - Exhaust memory on the server or client
    - Cause timeouts due to computational complexity
    - Return result sets too large for AI agent context windows

    Validation Rules:
    1. **Length Check**: Queries over 50,000 chars likely indicate generated/malicious code
    2. **Pattern Detection**: Identifies risky patterns like:
       - `MATCH ()` - Matches all nodes (potential millions)
       - `MATCH ()-[]-()` - Cartesian products without filters
       - Multiple `UNWIND` - Exponential expansion risk
    3. **Warning Generation**: Logs warnings for concerning patterns that don't block execution

    Args:
        cypher: Cypher query string to validate

    Raises:
        KuzuQueryComplexityError: If query violates hard limits or contains
                                  patterns likely to cause resource exhaustion
    """
    # Check query length
    if len(cypher) > self.max_query_length:
      raise KuzuQueryComplexityError(
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
      raise KuzuQueryComplexityError(
        f"Query has {subquery_count} subqueries/WITH clauses, which may be too complex"
      )

    logger.debug(
      f"Query complexity validation passed for {len(cypher)} character query"
    )

  async def execute_query(
    self, cypher: str, parameters: Optional[Dict[str, Any]] = None
  ) -> List[Dict[str, Any]]:
    """
    Execute a Cypher query via Kuzu API with timeout and complexity controls.

    Args:
        cypher: Cypher query string
        parameters: Optional query parameters

    Returns:
        List of result dictionaries

    Raises:
        KuzuAPIError: If query execution fails
        KuzuQueryTimeoutError: If query times out
        KuzuQueryComplexityError: If query is too complex
    """
    logger.info(f"MCP execute_query called with: {cypher[:100]}...")
    # Validate query complexity before execution
    self._validate_query_complexity(cypher)

    # Auto-append LIMIT for MCP context safety
    original_query = cypher
    cypher_upper = cypher.strip().upper()

    # Use cached configuration
    max_rows = self.max_result_rows
    auto_limit_enabled = self.auto_limit_enabled

    # Check if we should auto-append LIMIT
    has_limit = "LIMIT" in cypher_upper
    has_return = "RETURN" in cypher_upper
    has_aggregation = self._has_aggregation_function(cypher_upper)

    if auto_limit_enabled and has_return and not has_limit and not has_aggregation:
      # Use intelligent LIMIT injection to handle complex queries
      # Skip injection for aggregation queries since they naturally return limited results
      cypher = self._inject_limit_intelligently(cypher, max_rows)
      logger.info(
        f"MCP safety: Auto-injected LIMIT {max_rows} to prevent context exhaustion"
      )
    elif has_aggregation:
      logger.debug("MCP: Skipping auto-LIMIT for aggregation query")

    try:
      logger.info(
        f"MCP: Executing Kuzu query (timeout={self.query_timeout}s): {cypher[:200]}..."
      )

      # Use KuzuClient instead of direct HTTP calls
      try:
        result = await asyncio.wait_for(
          self.kuzu_client.query(
            cypher=cypher, graph_id=self.graph_id, parameters=parameters
          ),
          timeout=self.query_timeout,
        )
      except asyncio.TimeoutError:
        error_msg = f"Query execution timed out after {self.query_timeout} seconds"
        logger.error(error_msg)
        raise KuzuQueryTimeoutError(error_msg)

      # Ensure result is a dictionary (non-streaming mode)
      if not isinstance(result, dict):
        raise KuzuAPIError("Expected dictionary result from query, got async generator")

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
      max_size_mb = env.MCP_MAX_RESULT_SIZE_MB
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

    except KuzuQueryTimeoutError:
      # Re-raise timeout errors
      raise
    except KuzuQueryComplexityError:
      # Re-raise complexity errors
      raise
    except TimeoutException as e:
      error_msg = f"HTTP timeout executing query after {self.timeout}s: {e}"
      logger.error(error_msg)
      raise KuzuQueryTimeoutError(error_msg)
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

      raise KuzuAPIError(user_msg)

    except Exception as e:
      # Log full error for debugging
      logger.error(f"Unexpected error executing query: {e}")
      # Return sanitized error to user
      user_msg = self._sanitize_error_message(e, "query execution")
      raise KuzuAPIError(user_msg)

  async def get_schema(self) -> List[Dict[str, Any]]:
    """
    Get database schema information (simplified for performance).

    Returns:
        List of schema information dictionaries with basic details
    """
    try:
      # Get table information with explicit column names
      tables_query = "CALL SHOW_TABLES() RETURN id, name, type, comment"
      tables_result = await self.execute_query(tables_query)

      schema_info = []

      for table in tables_result:
        # Now we have named columns from our explicit query
        table_name = table.get("name", "")
        table_type = table.get("type", "")
        table_comment = table.get("comment", "")

        if not table_name:
          continue

        # Create basic schema entry without detailed property inspection
        if table_type.upper() == "NODE":
          # Always try to get count for all nodes - it's a fast operation
          count = 0
          try:
            count_query = f"MATCH (n:{table_name}) RETURN count(n) as count"
            count_result = await self.execute_query(count_query)
            if count_result and len(count_result) > 0:
              count = count_result[0].get("count", 0)
          except Exception as e:
            logger.debug(f"Could not count {table_name}: {e}")
            pass  # Skip count if query fails

          # Try to get sample properties for important node types
          sample_properties = []
          if table_name in [
            "Entity",
            "Fact",
            "Report",
            "Transaction",
            "User",
            "Period",
            "Element",
          ]:
            try:
              prop_query = f"MATCH (n:{table_name}) RETURN keys(n) as props LIMIT 1"
              prop_result = await self.execute_query(prop_query)
              if prop_result and prop_result[0].get("props"):
                sample_properties = prop_result[0]["props"][
                  :15
                ]  # Limit to first 15 properties
            except Exception:
              pass

          schema_info.append(
            {
              "label": table_name,
              "type": "node",
              "comment": table_comment,
              "count": count,
              "description": self._get_node_description(table_name),
              "sample_properties": sample_properties,
              "common_properties": self._get_common_properties(table_name),
            }
          )

        elif table_type.upper() == "REL":
          # Infer relationship details without querying table structure
          from_node, to_node = self._infer_relationship_nodes(table_name)

          schema_info.append(
            {
              "label": table_name,
              "type": "relationship",
              "comment": table_comment,
              "from_node": from_node,
              "to_node": to_node,
              "description": self._get_relationship_description(table_name),
            }
          )

      logger.info(f"Retrieved simplified schema for {len(schema_info)} tables")
      return schema_info

    except Exception as e:
      logger.error(f"Failed to get schema: {e}")
      raise KuzuAPIError(f"Schema retrieval failed: {e}")

  def _get_common_properties(self, node_name: str) -> List[str]:
    """Get commonly used properties for node types."""
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
        "accession_number",
        "period_end_date",
        "entity_identifier",
      ],
      "Period": [
        "start_date",
        "end_date",
        "instant_date",
        "fiscal_year",
        "fiscal_quarter",
        "period_type",
      ],
      "Element": ["name", "qname", "balance", "item_type", "is_numeric", "is_abstract"],
      "Transaction": ["date", "amount", "description", "type", "transaction_id"],
      "Account": ["name", "number", "type", "balance", "parent_account_id"],
      "User": ["id", "name", "email", "is_active", "created_at"],
      "Connection": ["provider", "status", "last_sync", "realm_id", "connection_id"],
      "Unit": ["measure", "numerator_uri", "denominator_uri"],
    }
    return common_props.get(
      node_name, ["identifier", "name", "value", "created_at", "updated_at"]
    )

  def _get_node_description(self, node_name: str) -> str:
    """Get description for common node types."""
    descriptions = {
      "Entity": "Business entities (companies, subsidiaries) with financial data and SEC filings",
      "User": "System users with authentication and authorization",
      "Report": "SEC filings (10-K annual, 10-Q quarterly reports)",
      "Fact": "XBRL data points with financial values and contexts",
      "Element": "XBRL taxonomy elements defining financial concepts",
      "Transaction": "Financial transactions from accounting systems",
      "LineItem": "Individual accounting entries with debits/credits",
      "Account": "Chart of accounts for financial categorization",
      "Period": "Time periods for financial reporting (instant/duration)",
      "Unit": "Measurement units (USD, shares, percentages)",
      "Connection": "External system integrations (QuickBooks, etc.)",
      "GraphMetadata": "Metadata about the graph database itself",
      "Address": "Physical or mailing addresses",
      "Contact": "Contact information for people or organizations",
      "Document": "Files and documents associated with entities",
      "Event": "Business events and activities",
      "Security": "Financial securities (stocks, bonds, etc.)",
    }
    return descriptions.get(node_name, f"{node_name} entities in the graph")

  def _get_relationship_description(self, rel_name: str) -> str:
    """Get description for common relationship types."""
    descriptions = {
      # Entity relationships
      "ENTITY_HAS_REPORT": "Links companies to their SEC filings (10-K, 10-Q, etc.)",
      "ENTITY_HAS_CONNECTION": "Links entities to external system integrations",
      "ENTITY_OWNS_ENTITY": "Parent-subsidiary ownership relationships",
      "ENTITY_EVOLVED_FROM": "Tracks entity changes over time (mergers, acquisitions)",
      "ENTITY_HAS_ADDRESS": "Links entities to their addresses",
      # Report relationships
      "REPORT_HAS_FACT": "Links reports to their financial data points",
      "REPORT_HAS_FACT_SET": "Groups related facts within a report",
      "REPORT_USES_TAXONOMY": "Links reports to their XBRL taxonomy definitions",
      "REPORTED_IN": "Links facts to the reports they appear in",
      # Fact relationships
      "FACT_HAS_ELEMENT": "Links facts to their XBRL taxonomy elements (concept definitions)",
      "FACT_HAS_ENTITY": "Links facts to the reporting entity",
      "FACT_HAS_PERIOD": "Links facts to their time periods (instant or duration)",
      "FACT_HAS_UNIT": "Links facts to their measurement units (USD, shares, etc.)",
      "FACT_HAS_DIMENSION": "Links facts to dimensional qualifiers",
      "FACT_SET_CONTAINS_FACT": "Groups related facts together",
      "FACT_DIMENSION_AXIS_ELEMENT": "Defines dimensional axes for facts",
      "FACT_DIMENSION_MEMBER_ELEMENT": "Defines dimensional members for facts",
      # Element relationships
      "ELEMENT_HAS_LABEL": "Links XBRL elements to their human-readable labels",
      "ELEMENT_HAS_REFERENCE": "Links elements to authoritative references",
      "ELEMENT_IN_TAXONOMY": "Places elements within taxonomy structure",
      # Taxonomy relationships
      "TAXONOMY_HAS_LABEL": "Links taxonomies to their labels",
      "TAXONOMY_HAS_REFERENCE": "Links taxonomies to references",
      # Structure relationships
      "STRUCTURE_HAS_PARENT": "Parent relationship in presentation hierarchy",
      "STRUCTURE_HAS_CHILD": "Child relationship in presentation hierarchy",
      "STRUCTURE_HAS_TAXONOMY": "Links presentation structure to taxonomy",
      "STRUCTURE_HAS_ASSOCIATION": "Links structure to calculation relationships",
      # Association relationships
      "ASSOCIATION_HAS_FROM_ELEMENT": "Source element in calculation relationship",
      "ASSOCIATION_HAS_TO_ELEMENT": "Target element in calculation relationship",
      # User relationships
      "USER_HAS_ACCESS": "Links users to entities they can access",
      # Transaction relationships (for accounting data)
      "HAS_LINE_ITEM": "Links transactions to their line items",
      "HAS_ACCOUNT": "Links line items to chart of accounts",
      "HAS_CONNECTION": "Links entities to external integrations",
      # Contact relationships
      "CONTACT_HAS_ADDRESS": "Links contacts to their addresses",
    }
    return descriptions.get(rel_name, f"{rel_name} relationship in the graph")

  def _infer_relationship_nodes(self, rel_name: str) -> tuple[str, str]:
    """Infer source and target nodes from relationship name."""
    # SEC-specific relationship mappings
    sec_relationships = {
      "ENTITY_HAS_REPORT": ("Entity", "Report"),
      "ENTITY_HAS_CONNECTION": ("Entity", "Connection"),
      "ENTITY_OWNS_ENTITY": ("Entity", "Entity"),
      "ENTITY_EVOLVED_FROM": ("Entity", "Entity"),
      "REPORT_HAS_FACT": ("Report", "Fact"),
      "REPORT_HAS_FACT_SET": ("Report", "FactSet"),
      "REPORT_USES_TAXONOMY": ("Report", "Taxonomy"),
      "FACT_HAS_ELEMENT": ("Fact", "Element"),
      "FACT_HAS_ENTITY": ("Fact", "Entity"),
      "FACT_HAS_PERIOD": ("Fact", "Period"),
      "FACT_HAS_UNIT": ("Fact", "Unit"),
      "FACT_HAS_DIMENSION": ("Fact", "FactDimension"),
      "FACT_SET_CONTAINS_FACT": ("FactSet", "Fact"),
      "FACT_DIMENSION_AXIS_ELEMENT": ("FactDimension", "Element"),
      "FACT_DIMENSION_MEMBER_ELEMENT": ("FactDimension", "Element"),
      "ELEMENT_HAS_LABEL": ("Element", "Label"),
      "ELEMENT_HAS_REFERENCE": ("Element", "Reference"),
      "ELEMENT_IN_TAXONOMY": ("Element", "Taxonomy"),
      "TAXONOMY_HAS_LABEL": ("Taxonomy", "Label"),
      "TAXONOMY_HAS_REFERENCE": ("Taxonomy", "Reference"),
      "STRUCTURE_HAS_PARENT": ("Structure", "Structure"),
      "STRUCTURE_HAS_CHILD": ("Structure", "Structure"),
      "STRUCTURE_HAS_TAXONOMY": ("Structure", "Taxonomy"),
      "STRUCTURE_HAS_ASSOCIATION": ("Structure", "Association"),
      "ASSOCIATION_HAS_FROM_ELEMENT": ("Association", "Element"),
      "ASSOCIATION_HAS_TO_ELEMENT": ("Association", "Element"),
      "USER_HAS_ACCESS": ("User", "Entity"),
    }

    # Check if we have a known mapping
    if rel_name in sec_relationships:
      return sec_relationships[rel_name]

    # Try pattern-based inference for unknown relationships
    if "_HAS_" in rel_name:
      parts = rel_name.split("_HAS_")
      if len(parts) == 2:
        # Clean up the parts - capitalize properly
        from_node = parts[0].replace("_", " ").title().replace(" ", "")
        to_node = parts[1].replace("_", " ").title().replace(" ", "")
        return (from_node, to_node)
    elif "_OWNS_" in rel_name:
      parts = rel_name.split("_OWNS_")
      if len(parts) == 2:
        from_node = parts[0].replace("_", " ").title().replace(" ", "")
        to_node = parts[1].replace("_", " ").title().replace(" ", "")
        return (from_node, to_node)
    elif "REPORTED_IN" in rel_name:
      return ("Fact", "Report")

    # If we can't infer, return Unknown
    return ("Unknown", "Unknown")

  async def describe_graph_structure(self) -> str:
    """
    Generate a natural language description of the graph structure.

    Returns:
        Human-readable description of the graph
    """
    # Import here to avoid circular import
    from .tools import KuzuMCPTools

    # Delegate to MCP tools
    mcp_tools = KuzuMCPTools(self)
    return await mcp_tools.structure_tool._describe_graph_structure()

  async def get_graph_info(self) -> Dict[str, Any]:
    """
    Get basic graph information and statistics.

    Returns:
        Dictionary with graph statistics
    """
    try:
      # Get API info
      response = await self.client.get(f"{self.api_base_url}/info")
      response.raise_for_status()
      api_info = response.json()

      # Get table information from database
      try:
        tables_query = "CALL SHOW_TABLES() RETURN id, name, type, comment"
        tables = await self.execute_query(tables_query)

        # Now we have named columns from our explicit query
        node_tables = []
        for t in tables:
          table_type = t.get("type", "")
          if table_type.upper() == "NODE":
            node_tables.append(t)

        # Try to get actual node and relationship counts
        total_nodes = 0
        total_relationships = 0

        # Count all nodes (more efficient than counting per table)
        try:
          node_count_query = "MATCH (n) RETURN count(n) as count"
          node_count_result = await self.execute_query(node_count_query)
          if node_count_result:
            total_nodes = node_count_result[0].get("count", 0)
        except Exception:
          # Fallback to per-table counting
          for table in node_tables[:5]:  # Limit to first 5 node tables
            try:
              table_name = table.get("name", "")
              if table_name:
                count_query = f"MATCH (n:{table_name}) RETURN count(n) as count"
                count_result = await self.execute_query(count_query)
                if count_result:
                  total_nodes += count_result[0].get("count", 0)
            except Exception:
              pass

        # Count all relationships
        try:
          rel_count_query = "MATCH ()-[r]->() RETURN count(r) as count"
          rel_count_result = await self.execute_query(rel_count_query)
          if rel_count_result:
            total_relationships = rel_count_result[0].get("count", 0)
        except Exception:
          pass

        # Extract node labels from tables
        node_labels = []
        for t in node_tables:
          label = t.get("name", "")
          if label:
            node_labels.append(label)
      except Exception as e:
        logger.warning(f"Failed to get table counts: {e}")
        # Fallback to basic info
        node_labels = []
        total_nodes = 0
        total_relationships = 0

      return {
        "graph_id": self.graph_id,
        "total_nodes": total_nodes,
        "total_relationships": total_relationships,
        "node_labels": node_labels,
        "database_path": api_info.get("database_path", ""),
        "read_only": api_info.get("read_only", True),
        "uptime_seconds": api_info.get("uptime_seconds", 0),
      }

    except Exception as e:
      logger.error(f"Failed to get graph info: {e}")
      raise KuzuAPIError(f"Graph info retrieval failed: {e}")

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
      r"[\w_]+\.(db|kuzu|sqlite)",  # Database file names
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
      "COUNT{",  # Kuzu COUNT subquery syntax
    ]

    for func in aggregation_functions:
      if func in query_upper:
        return True

    return False

  def _inject_limit_intelligently(self, query: str, limit: int) -> str:
    """
    Intelligently inject LIMIT clause into Cypher query to prevent context exhaustion.

    This method uses a sophisticated algorithm to safely inject LIMIT clauses into
    complex Cypher queries without breaking their semantics. It handles:

    - **UNION queries**: Adds LIMIT to each UNION branch to ensure fair sampling
    - **Subqueries**: Preserves subquery structure while limiting final results
    - **ORDER BY clauses**: Places LIMIT after ORDER BY to maintain sort order
    - **WITH clauses**: Adds LIMIT after the final RETURN to avoid intermediate truncation

    Algorithm:
    1. Check if LIMIT already exists (no-op if present)
    2. For UNION queries: Split by UNION, recursively process each part
    3. For standard queries: Find the last RETURN statement
    4. Place LIMIT after ORDER BY if present, otherwise at query end
    5. Handle edge cases like comments and string literals

    Args:
        query: The Cypher query to modify
        limit: The limit value to inject (typically 1000 for MCP safety)

    Returns:
        Query with LIMIT intelligently injected without breaking semantics

    Examples:
        >>> _inject_limit_intelligently("MATCH (n) RETURN n", 100)
        "MATCH (n) RETURN n LIMIT 100"

        >>> _inject_limit_intelligently("MATCH (n) RETURN n ORDER BY n.name", 100)
        "MATCH (n) RETURN n ORDER BY n.name LIMIT 100"
    """
    import re

    # Normalize query for analysis (preserve original for output)
    query_normalized = query.strip()
    query_upper = query_normalized.upper()

    # If query already has LIMIT, return as-is
    if "LIMIT" in query_upper:
      return query

    # Handle UNION queries - need to add LIMIT to each part
    if "UNION" in query_upper:
      # Split by UNION and add LIMIT to each part
      parts = re.split(r"\bUNION\b", query_normalized, flags=re.IGNORECASE)
      limited_parts = []

      for part in parts:
        part_trimmed = part.strip()
        if part_trimmed and "RETURN" in part_trimmed.upper():
          # Add LIMIT to this part
          limited_parts.append(self._inject_limit_to_simple_query(part_trimmed, limit))
        else:
          limited_parts.append(part)

      return " UNION ".join(limited_parts)

    # For non-UNION queries, inject at the end
    return self._inject_limit_to_simple_query(query_normalized, limit)

  def _inject_limit_to_simple_query(self, query: str, limit: int) -> str:
    """
    Inject LIMIT to a simple (non-UNION) query.

    Handles ORDER BY and ensures LIMIT goes at the very end.
    """
    import re

    query_trimmed = query.rstrip()

    # Remove trailing semicolon if present
    if query_trimmed.endswith(";"):
      query_trimmed = query_trimmed[:-1].rstrip()

    # Check if query ends with ORDER BY clause
    # Pattern: ORDER BY <expression> [ASC|DESC]
    order_by_pattern = r"(.*)(\bORDER\s+BY\s+[^;]+?)$"
    match = re.match(order_by_pattern, query_trimmed, re.IGNORECASE | re.DOTALL)

    if match:
      # Query has ORDER BY at the end
      before_order = match.group(1).rstrip()
      order_clause = match.group(2)
      return f"{before_order} {order_clause} LIMIT {limit}"

    # No ORDER BY, just append LIMIT
    return f"{query_trimmed} LIMIT {limit}"

  def _is_read_only_query(self, query: str) -> bool:
    """
    Enhanced validation to ensure query is read-only.

    Uses a whitelist approach with pattern matching to detect write operations.
    Returns True if query is safe (read-only), False otherwise.
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
