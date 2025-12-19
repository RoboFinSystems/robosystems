"""
Metrics endpoints for Graph API monitoring.

This module provides endpoints for retrieving comprehensive metrics
about the cluster node, including system resources, database statistics,
query performance, and ingestion queue status.
"""

from typing import Any

from fastapi import APIRouter, Depends

from robosystems.graph_api.core.admission_control import get_admission_controller
from robosystems.graph_api.core.ladybug import get_ladybug_service

router = APIRouter(tags=["Cluster Metrics"])


@router.get("/metrics")
async def get_metrics(
  ladybug_service=Depends(get_ladybug_service),
) -> dict[str, Any]:
  """
  Get comprehensive metrics for the cluster node.

  Returns a complete snapshot of the node's operational metrics including:

  - **System Metrics**: CPU usage, memory consumption, disk space
  - **Database Metrics**: Size, table counts, connection pools for each database
  - **Query Metrics**: Query counts, average execution times, slow queries
  - **Ingestion Metrics**: Queue depth, processing rates, active tasks
  - **Cluster Info**: Node identification, type, and uptime

  This endpoint is designed for monitoring systems like Prometheus
  or custom dashboards to track the health and performance of the
  LadybugDB cluster.
  """
  metrics_collector = ladybug_service.metrics_collector

  # Collect all metrics
  system_metrics = metrics_collector.collect_system_metrics()
  database_metrics = metrics_collector.collect_database_metrics()
  query_metrics = metrics_collector.get_query_metrics()
  ingestion_metrics = await metrics_collector.collect_ingestion_metrics()

  # Get admission control metrics
  admission_controller = get_admission_controller()
  admission_metrics = admission_controller.get_metrics()

  return {
    "timestamp": system_metrics.get("timestamp"),
    "system": system_metrics,
    "databases": database_metrics,
    "queries": query_metrics,
    "ingestion": ingestion_metrics,
    "admission_control": admission_metrics,
    "cluster": {
      "node_id": ladybug_service.node_id,
      "node_type": ladybug_service.node_type.value,
      "uptime_seconds": ladybug_service.get_uptime(),
    },
  }
