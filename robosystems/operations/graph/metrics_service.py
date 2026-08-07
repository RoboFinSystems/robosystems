"""Graph metrics collection service for usage monitoring."""

import logging
from datetime import UTC, datetime
from typing import Any

from ...database import session
from ...middleware.graph.router import get_universal_repository
from ...middleware.graph.utils import MultiTenantUtils
from ...middleware.otel.metrics import get_endpoint_metrics
from ...models.core import GraphUser

logger = logging.getLogger(__name__)


class GraphMetricsService:
  """Collect node/relationship counts, storage, and health per graph.

  Every method degrades to an ``error`` key rather than raising, so a metrics
  failure never breaks the caller. Counts are gathered with one query per
  label, so cost scales with the number of distinct labels.
  """

  DEFAULT_GRAPH_LIMIT = 5

  def __init__(self):
    self.metrics_instance = get_endpoint_metrics()

  async def collect_metrics_for_graph(self, graph_id: str) -> dict[str, Any]:
    """Counts, storage, and health for one graph, also emitted to OpenTelemetry.

    Returns a dict with an ``error`` key instead of raising on failure.
    """
    try:
      database_name = MultiTenantUtils.get_database_name(graph_id)
      repository = await get_universal_repository(database_name, operation_type="write")

      metrics = {
        "graph_id": graph_id,
        "timestamp": datetime.now(UTC).isoformat(),
        "node_counts": await self._get_node_counts_by_label(repository),
        "relationship_counts": await self._get_relationship_counts_by_type(repository),
        "total_nodes": 0,
        "total_relationships": 0,
        "storage": await self._get_storage(graph_id),
        "health_status": await self._check_graph_health(repository),
      }

      metrics["total_nodes"] = sum(
        v for v in metrics["node_counts"].values() if isinstance(v, (int, float))
      )
      metrics["total_relationships"] = sum(
        v
        for v in metrics["relationship_counts"].values()
        if isinstance(v, (int, float))
      )

      self._record_otel_metrics(graph_id, metrics)

      return metrics

    except Exception as e:
      logger.error(f"Error collecting metrics for graph {graph_id}: {e!s}")
      return {
        "graph_id": graph_id,
        "error": str(e),
        "timestamp": datetime.now(UTC).isoformat(),
      }

  async def collect_metrics_for_user_graphs(
    self, user_id: str
  ) -> dict[str, dict[str, Any]]:
    """Metrics for every graph the user can reach, keyed by graph id.

    Carries an extra ``_summary`` entry with the cross-graph totals; on failure
    the whole result is a single ``_error`` entry.
    """
    try:
      user_graphs = GraphUser.get_by_user_id(user_id, session)
      logger.info(
        f"collect_metrics_for_user: Found {len(user_graphs)} graphs for user {user_id}"
      )

      metrics_by_graph = {}
      total_nodes = 0
      total_relationships = 0

      for user_graph in user_graphs:
        graph_metrics = await self.collect_metrics_for_graph(user_graph.graph_id)
        metrics_by_graph[user_graph.graph_id] = graph_metrics

        if "error" not in graph_metrics:
          graph_metrics["graph_name"] = user_graph.graph.graph_name
          graph_metrics["user_role"] = user_graph.role
          total_nodes += graph_metrics.get("total_nodes", 0)
          total_relationships += graph_metrics.get("total_relationships", 0)

      metrics_by_graph["_summary"] = {
        "user_id": user_id,
        "total_graphs": len(user_graphs),
        "total_nodes_across_graphs": total_nodes,
        "total_relationships_across_graphs": total_relationships,
        "timestamp": datetime.now(UTC).isoformat(),
      }

      return metrics_by_graph

    except Exception as e:
      logger.error(f"Error collecting metrics for user {user_id}: {e!s}")
      return {
        "_error": {
          "user_id": user_id,
          "error": str(e),
          "timestamp": datetime.now(UTC).isoformat(),
        }
      }

  async def get_usage_summary(self, user_id: str | None = None) -> dict[str, Any]:
    """Dashboard-shaped totals for one user, or a system-wide graph count.

    The per-user totals cover only the first ``DEFAULT_GRAPH_LIMIT`` graphs, so
    a user with more graphs sees an undercount rather than a slow response.
    """
    try:
      if user_id:
        user_graphs = GraphUser.get_by_user_id(user_id, session)
        graph_count = len(user_graphs)
        logger.info(
          f"Found {graph_count} graphs for user {user_id}: {[g.graph_id for g in user_graphs]}"
        )

        total_nodes = 0
        total_relationships = 0

        for user_graph in user_graphs[: self.DEFAULT_GRAPH_LIMIT]:
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
          "timestamp": datetime.now(UTC).isoformat(),
        }
      else:
        total_graphs = session.query(GraphUser).count()
        return {
          "system_wide": True,
          "total_graphs": total_graphs,
          "timestamp": datetime.now(UTC).isoformat(),
        }

    except Exception as e:
      logger.error(f"Error generating usage summary: {e!s}")
      return {
        "error": str(e),
        "timestamp": datetime.now(UTC).isoformat(),
      }

  async def collect_metrics_for_graph_async(self, graph_id: str) -> dict[str, Any]:
    """Alias for :meth:`collect_metrics_for_graph`."""
    return await self.collect_metrics_for_graph(graph_id)

  async def get_usage_summary_async(self, user_id: str | None = None) -> dict[str, Any]:
    """Alias for :meth:`get_usage_summary`."""
    return await self.get_usage_summary(user_id)

  async def collect_metrics_for_user_graphs_async(
    self, user_id: str
  ) -> dict[str, dict[str, Any]]:
    """Alias for :meth:`collect_metrics_for_user_graphs`."""
    return await self.collect_metrics_for_user_graphs(user_id)

  async def _get_node_counts_by_label(self, repository) -> dict[str, int]:
    """Node count per label; falls back to a single ``_total`` on failure.

    One COUNT per label rather than a grouped aggregate — LadybugDB has no
    label-keyed grouping, and the labels come from the graph itself, so each is
    backtick-quoted before interpolation.
    """
    try:
      labels_query = "MATCH (n) RETURN DISTINCT LABEL(n) AS label"
      labels_result = await repository.execute_query(labels_query)

      node_counts = {}

      for record in labels_result:
        label = record["label"]
        try:
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
      try:
        result = await repository.execute_query("MATCH (n) RETURN count(n) as total")
        total = result[0]["total"] if result else 0
        return {"_total": total}
      except Exception as fallback_e:
        logger.warning(f"Fallback node count also failed: {fallback_e}")
        return {"_total": 0}

  async def _get_relationship_counts_by_type(self, repository) -> dict[str, int]:
    """Relationship count per type; falls back to a single ``_total``.

    One COUNT per type, for the same reason as
    :meth:`_get_node_counts_by_label`.
    """
    try:
      types_query = "MATCH ()-[r]->() RETURN DISTINCT LABEL(r) AS relationshipType"
      types_result = await repository.execute_query(types_query)

      rel_counts = {}

      for record in types_result:
        rel_type = record["relationshipType"]
        try:
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
      try:
        query = "MATCH ()-[r]->() RETURN count(r) as total"
        result = await repository.execute_query(query)
        total = next(iter(result))["total"] if result else 0
        return {"_total": total}
      except Exception as fallback_e:
        logger.warning(f"Fallback relationship count also failed: {fallback_e}")
        return {"_total": 0}

  async def _get_storage(self, graph_id: str) -> dict[str, Any]:
    """Measured on-disk storage for a graph and everything it owns.

    Spans the LadybugDB databases, vector indexes, and staging file, so it is
    occupied disk rather than anything derived from node count.
    """
    from robosystems.graph_api.client.factory import GraphClientFactory

    try:
      client = await GraphClientFactory.create_client(
        graph_id=graph_id, operation_type="read"
      )
      try:
        breakdown = await client.get_storage_breakdown(graph_id)
      finally:
        await client.close()

      total_bytes = breakdown.get("total_bytes", 0)
      return {
        "total_bytes": total_bytes,
        "total_kb": total_bytes / 1024,
        "total_mb": total_bytes / (1024 * 1024),
        "total_gb": total_bytes / (1024**3),
        "items": breakdown.get("items", []),
      }

    except Exception as e:
      # No estimate on failure: an absent measure is honest, an invented one
      # silently understates real usage on every dashboard that reads it.
      logger.warning(f"Failed to measure storage for {graph_id}: {e}")
      return {"error": "Unable to measure storage"}

  async def _check_graph_health(self, repository) -> dict[str, Any]:
    """Probe the graph; any failure reports ``unhealthy`` rather than raising."""
    try:
      health_info = await repository.health_check()
      return {
        "status": "healthy" if health_info.get("status") == "healthy" else "unhealthy",
        "details": health_info,
      }
    except Exception as e:
      return {"status": "unhealthy", "error": str(e)}

  def _record_otel_metrics(self, graph_id: str, metrics: dict[str, Any]):
    """Emit the collected metrics to OpenTelemetry. Best-effort."""
    try:
      if "error" not in metrics:
        storage_bytes = metrics.get("storage", {}).get("total_bytes", 0)
        if not isinstance(storage_bytes, int):
          storage_bytes = 0

        self.metrics_instance.record_graph_metrics(
          graph_id=graph_id,
          node_count=metrics.get("total_nodes", 0),
          relationship_count=metrics.get("total_relationships", 0),
          estimated_size_bytes=storage_bytes,
          additional_attributes={
            "node_label_count": len(metrics.get("node_counts", {})),
            "relationship_type_count": len(metrics.get("relationship_counts", {})),
            "health_status": metrics.get("health_status", {}).get("status", "unknown"),
          },
        )

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
