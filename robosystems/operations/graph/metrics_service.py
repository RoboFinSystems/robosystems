"""Graph metrics collection service for usage monitoring."""

import logging
from typing import Dict, Any, Optional
from datetime import datetime, timezone

from ...middleware.graph.router import get_universal_repository
from ...middleware.graph.multitenant_utils import MultiTenantUtils
from ...models.iam import UserGraph
from ...middleware.otel.metrics import get_endpoint_metrics
from ...database import session

logger = logging.getLogger(__name__)


class GraphMetricsService:
  """Service for collecting and aggregating graph database metrics."""

  # Configuration constants
  DEFAULT_GRAPH_LIMIT = 5
  DEFAULT_NODE_SIZE_ESTIMATE = 150  # bytes per node for size estimation

  def __init__(self):
    self.metrics_instance = get_endpoint_metrics()

  async def collect_metrics_for_graph(self, graph_id: str) -> Dict[str, Any]:
    """
    Collect comprehensive metrics for a specific graph database.

    Args:
        graph_id: The graph database identifier

    Returns:
        Dict containing node counts, relationship counts, and database size estimates
    """
    try:
      # Get database name from graph_id
      database_name = MultiTenantUtils.get_database_name(graph_id)
      # Get repository asynchronously
      repository = await get_universal_repository(database_name, operation_type="write")

      # Collect basic metrics
      metrics = {
        "graph_id": graph_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "node_counts": await self._get_node_counts_by_label(repository),
        "relationship_counts": await self._get_relationship_counts_by_type(repository),
        "total_nodes": 0,
        "total_relationships": 0,
        "estimated_size": await self._estimate_database_size(repository),
        "health_status": await self._check_graph_health(repository),
      }

      # Calculate totals (filter out non-numeric values)
      metrics["total_nodes"] = sum(
        v for v in metrics["node_counts"].values() if isinstance(v, (int, float))
      )
      metrics["total_relationships"] = sum(
        v
        for v in metrics["relationship_counts"].values()
        if isinstance(v, (int, float))
      )

      # Record OpenTelemetry metrics
      self._record_otel_metrics(graph_id, metrics)

      return metrics

    except Exception as e:
      logger.error(f"Error collecting metrics for graph {graph_id}: {str(e)}")
      return {
        "graph_id": graph_id,
        "error": str(e),
        "timestamp": datetime.now(timezone.utc).isoformat(),
      }

  async def collect_metrics_for_user_graphs(
    self, user_id: str
  ) -> Dict[str, Dict[str, Any]]:
    """
    Collect metrics for all graphs accessible to a user.

    Args:
        user_id: The user identifier

    Returns:
        Dict mapping graph_id to metrics
    """
    try:
      # Get user's accessible graphs
      user_graphs = UserGraph.get_by_user_id(user_id, session)
      logger.info(
        f"collect_metrics_for_user: Found {len(user_graphs)} graphs for user {user_id}"
      )

      metrics_by_graph = {}
      total_nodes = 0
      total_relationships = 0

      for user_graph in user_graphs:
        graph_metrics = await self.collect_metrics_for_graph(user_graph.graph_id)
        metrics_by_graph[user_graph.graph_id] = graph_metrics

        # Add graph name and role info
        if "error" not in graph_metrics:
          graph_metrics["graph_name"] = user_graph.graph.graph_name
          graph_metrics["user_role"] = user_graph.role
          total_nodes += graph_metrics.get("total_nodes", 0)
          total_relationships += graph_metrics.get("total_relationships", 0)

      # Add summary statistics
      metrics_by_graph["_summary"] = {
        "user_id": user_id,
        "total_graphs": len(user_graphs),
        "total_nodes_across_graphs": total_nodes,
        "total_relationships_across_graphs": total_relationships,
        "timestamp": datetime.now(timezone.utc).isoformat(),
      }

      return metrics_by_graph

    except Exception as e:
      logger.error(f"Error collecting metrics for user {user_id}: {str(e)}")
      return {
        "_error": {
          "user_id": user_id,
          "error": str(e),
          "timestamp": datetime.now(timezone.utc).isoformat(),
        }
      }

  async def get_usage_summary(self, user_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Get usage summary for monitoring dashboards.

    Args:
        user_id: Optional user ID to filter metrics

    Returns:
        Summary statistics suitable for dashboard display
    """
    try:
      if user_id:
        # User-specific summary
        user_graphs = UserGraph.get_by_user_id(user_id, session)
        graph_count = len(user_graphs)
        logger.info(
          f"Found {graph_count} graphs for user {user_id}: {[g.graph_id for g in user_graphs]}"
        )

        # Calculate total nodes/relationships across user's graphs
        total_nodes = 0
        total_relationships = 0

        for user_graph in user_graphs[
          : self.DEFAULT_GRAPH_LIMIT
        ]:  # Configurable limit to avoid timeout
          try:
            metrics = await self.collect_metrics_for_graph(user_graph.graph_id)
            if "error" not in metrics:
              total_nodes += metrics.get("total_nodes", 0)
              total_relationships += metrics.get("total_relationships", 0)
          except Exception as e:
            logger.warning(
              f"Failed to collect metrics for graph {user_graph.graph_id}: {e}"
            )
            continue

        return {
          "user_id": user_id,
          "graph_count": graph_count,
          "total_nodes": total_nodes,
          "total_relationships": total_relationships,
          "timestamp": datetime.now(timezone.utc).isoformat(),
        }
      else:
        # System-wide summary (admin only)
        total_graphs = session.query(UserGraph).count()
        return {
          "system_wide": True,
          "total_graphs": total_graphs,
          "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    except Exception as e:
      logger.error(f"Error generating usage summary: {str(e)}")
      return {
        "error": str(e),
        "timestamp": datetime.now(timezone.utc).isoformat(),
      }

  async def collect_metrics_for_graph_async(self, graph_id: str) -> Dict[str, Any]:
    """
    Async version of collect_metrics_for_graph for use in async contexts.
    This is now just an alias since the main method is async.

    Args:
        graph_id: The graph database identifier

    Returns:
        Dict containing node counts, relationship counts, and database size estimates
    """
    return await self.collect_metrics_for_graph(graph_id)

  async def get_usage_summary_async(
    self, user_id: Optional[str] = None
  ) -> Dict[str, Any]:
    """
    Async version of get_usage_summary for use in async contexts.
    This is now just an alias since the main method is async.

    Args:
        user_id: Optional user ID to filter metrics

    Returns:
        Summary statistics suitable for dashboard display
    """
    return await self.get_usage_summary(user_id)

  async def collect_metrics_for_user_graphs_async(
    self, user_id: str
  ) -> Dict[str, Dict[str, Any]]:
    """
    Async version of collect_metrics_for_user_graphs for use in async contexts.
    This is now just an alias since the main method is async.

    Args:
        user_id: The user identifier

    Returns:
        Dict mapping graph_id to metrics
    """
    return await self.collect_metrics_for_user_graphs(user_id)

  async def _get_node_counts_by_label(self, repository) -> Dict[str, int]:
    """Get node counts grouped by label."""
    try:
      # Get all unique node labels using Kuzu-compatible query
      # Avoid selecting balance property to prevent type conflicts between Element and Account nodes
      labels_query = "MATCH (n) RETURN DISTINCT LABEL(n) AS label"
      # Use async method for repository
      labels_result = await repository.execute_query(labels_query)

      node_counts = {}

      # Count nodes for each label individually to avoid dynamic query construction
      for record in labels_result:
        label = record["label"]
        try:
          # Use parameterized query construction - escape label name properly
          count_query = f"MATCH (n:`{label}`) RETURN count(n) as count"
          count_result = await repository.execute_query(count_query)
          count = count_result[0]["count"] if count_result else 0
          node_counts[label] = count
        except Exception as label_e:
          logger.warning(f"Failed to count nodes for label {label}: {label_e}")
          node_counts[label] = 0

      return node_counts

    except Exception as e:
      logger.warning(f"Failed to get node counts by label: {e}")
      # Fallback to simple total count
      try:
        result = await repository.execute_query("MATCH (n) RETURN count(n) as total")
        total = result[0]["total"] if result else 0
        return {"_total": total}
      except Exception as fallback_e:
        logger.warning(f"Fallback node count also failed: {fallback_e}")
        return {"_total": 0}  # Return 0 instead of string to avoid type issues

  async def _get_relationship_counts_by_type(self, repository) -> Dict[str, int]:
    """Get relationship counts grouped by type."""
    try:
      # First get all relationship types
      # Get all unique relationship types using Kuzu-compatible query
      types_query = "MATCH ()-[r]->() RETURN DISTINCT LABEL(r) AS relationshipType"
      types_result = await repository.execute_query(types_query)

      rel_counts = {}

      # Count relationships for each type individually to avoid dynamic query construction
      for record in types_result:
        rel_type = record["relationshipType"]
        try:
          # Use parameterized query construction - escape relationship type name properly
          count_query = f"MATCH ()-[r:`{rel_type}`]->() RETURN count(r) as count"
          count_result = await repository.execute_query(count_query)
          count = count_result[0]["count"] if count_result else 0
          rel_counts[rel_type] = count
        except Exception as type_e:
          logger.warning(f"Failed to count relationships for type {rel_type}: {type_e}")
          rel_counts[rel_type] = 0

      return rel_counts

    except Exception as e:
      logger.warning(f"Failed to get relationship counts by type: {e}")
      # Fallback to simple query
      try:
        query = "MATCH ()-[r]->() RETURN count(r) as total"
        result = await repository.execute_query(query)
        total = list(result)[0]["total"] if result else 0
        return {"_total": total}
      except Exception as fallback_e:
        logger.warning(f"Fallback relationship count also failed: {fallback_e}")
        return {"_total": 0}  # Return 0 instead of string to avoid type issues

  async def _estimate_database_size(self, repository) -> Dict[str, Any]:
    """Estimate database storage usage."""
    try:
      # Try to get database size information with fallback strategies
      size_queries = [
        # Try to get database size information (Kuzu doesn't have JMX, skip this)
        # "CALL dbms.queryJmx('...') YIELD attributes RETURN attributes",
        # Simple fallback with estimation
        "MATCH (n) RETURN count(n) as nodeCount, count(n) * 100 as estimatedBytes",
      ]

      for query in size_queries:
        try:
          result = await repository.execute_query(query)
          records = list(result)
          if records:
            return {"size_info": dict(records[0]), "method": "database_query"}
        except Exception as query_error:
          logger.debug(f"Size query failed: {query_error}")
          continue

      # Final fallback - basic estimation
      try:
        result = await repository.execute_query("MATCH (n) RETURN count(n) as total")
        node_count = result[0]["total"] if result else 0
      except Exception as count_e:
        logger.debug(f"Node count estimation failed: {count_e}")
        node_count = 0
      estimated_bytes = (
        node_count * self.DEFAULT_NODE_SIZE_ESTIMATE
      )  # Configurable estimate per node

      return {
        "estimated_bytes": estimated_bytes,
        "estimated_kb": estimated_bytes / 1024,
        "estimated_mb": estimated_bytes / (1024 * 1024),
        "method": "estimation",
        "note": "Based on node count estimation",
      }

    except Exception as e:
      logger.warning(f"Failed to estimate database size: {e}")
      return {"error": "Unable to estimate size", "method": "failed"}

  async def _check_graph_health(self, repository) -> Dict[str, Any]:
    """Check graph database health status."""
    try:
      health_info = await repository.health_check()
      return {
        "status": "healthy" if health_info.get("status") == "healthy" else "unhealthy",
        "details": health_info,
      }
    except Exception as e:
      return {"status": "unhealthy", "error": str(e)}

  def _record_otel_metrics(self, graph_id: str, metrics: Dict[str, Any]):
    """Record metrics to OpenTelemetry."""
    try:
      # Record node and relationship counts as gauges
      if "error" not in metrics:
        # Record graph metrics
        estimated_size = metrics.get("estimated_size", {}).get("estimated_bytes", 0)
        if not isinstance(estimated_size, int):
          estimated_size = 0

        self.metrics_instance.record_graph_metrics(
          graph_id=graph_id,
          node_count=metrics.get("total_nodes", 0),
          relationship_count=metrics.get("total_relationships", 0),
          estimated_size_bytes=estimated_size,
          additional_attributes={
            "node_label_count": len(metrics.get("node_counts", {})),
            "relationship_type_count": len(metrics.get("relationship_counts", {})),
            "health_status": metrics.get("health_status", {}).get("status", "unknown"),
          },
        )

        # Also record as business event for additional tracking
        self.metrics_instance.record_business_event(
          endpoint="graph_metrics_collection",
          method="INTERNAL",
          event_type="graph_metrics_collected",
          event_data={
            "graph_id": graph_id,
            "total_nodes": metrics.get("total_nodes", 0),
            "total_relationships": metrics.get("total_relationships", 0),
            "node_label_count": len(metrics.get("node_counts", {})),
            "relationship_type_count": len(metrics.get("relationship_counts", {})),
            "health_status": metrics.get("health_status", {}).get("status", "unknown"),
          },
        )
    except Exception as e:
      logger.warning(f"Failed to record OpenTelemetry metrics: {e}")
