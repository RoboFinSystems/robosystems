"""
Metrics endpoints for Kuzu API monitoring.

This module provides endpoints for retrieving comprehensive metrics
about the cluster node, including system resources, database statistics,
query performance, and ingestion queue status.
"""

from typing import Dict, Any
from fastapi import APIRouter, Depends

from robosystems.graph_api.core.cluster_manager import get_cluster_service
from robosystems.graph_api.core.admission_control import get_admission_controller

router = APIRouter(tags=["Cluster Metrics"])


@router.get("/metrics")
async def get_metrics(
  cluster_service=Depends(get_cluster_service),
) -> Dict[str, Any]:
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
  Kuzu cluster.
  """
  metrics_collector = cluster_service.metrics_collector

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
      "node_id": cluster_service.node_id,
      "node_type": cluster_service.node_type.value,
      "uptime_seconds": cluster_service.get_uptime(),
    },
  }
