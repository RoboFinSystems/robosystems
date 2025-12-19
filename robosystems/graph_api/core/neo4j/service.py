import time
from datetime import datetime

import psutil
from fastapi import HTTPException, status

from robosystems.graph_api.backends import get_backend
from robosystems.graph_api.models.cluster import (
  ClusterHealthResponse,
  ClusterInfoResponse,
)
from robosystems.graph_api.models.database import QueryRequest, QueryResponse
from robosystems.logger import logger


class Neo4jService:
  def __init__(self):
    self.backend = get_backend()
    self.start_time = time.time()
    self.last_activity: datetime | None = None
    logger.info(
      f"Neo4jService initialized with backend type: {type(self.backend).__name__}"
    )

  def get_uptime(self) -> float:
    return time.time() - self.start_time

  async def execute_query(self, request: QueryRequest) -> QueryResponse:
    start_time = time.time()

    try:
      logger.debug(f"Executing query on {request.database}: {request.cypher[:100]}...")

      result = await self.backend.execute_query(
        graph_id=request.database,
        cypher=request.cypher,
        parameters=request.parameters,
      )

      execution_time = (time.time() - start_time) * 1000
      self.last_activity = datetime.now()

      logger.info(
        f"Query executed successfully on {request.database}: "
        f"{len(result)} rows in {execution_time:.2f}ms"
      )

      columns = list(result[0].keys()) if result else []

      return QueryResponse(
        data=result,
        columns=columns,
        execution_time_ms=execution_time,
        row_count=len(result),
        database=request.database,
      )

    except HTTPException:
      raise
    except Exception as e:
      execution_time = (time.time() - start_time) * 1000
      logger.error(
        f"Query execution failed on {request.database}: {e} "
        f"(after {execution_time:.2f}ms)"
      )
      raise HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail=f"Query execution failed: {e!s}",
      )

  async def get_cluster_health(self) -> ClusterHealthResponse:
    try:
      is_healthy = await self.backend.health_check()
      uptime = self.get_uptime()

      cpu_usage = psutil.cpu_percent(interval=0.1)
      memory_usage = psutil.virtual_memory().percent

      if not is_healthy:
        status_str = "unhealthy"
      elif cpu_usage > 90 or memory_usage > 90:
        status_str = "critical"
      elif cpu_usage > 75 or memory_usage > 75:
        status_str = "warning"
      else:
        status_str = "healthy"

      return ClusterHealthResponse(
        status=status_str,
        uptime_seconds=uptime,
        node_type="backend",
        base_path="",
        max_databases=0,
        current_databases=0,
        capacity_remaining=0,
        read_only=False,
        last_activity=(self.last_activity.isoformat() if self.last_activity else None),
      )
    except Exception as e:
      logger.error(f"Health check failed: {e}")
      raise HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail=f"Health check failed: {e!s}",
      )

  async def get_cluster_info(self) -> ClusterInfoResponse:
    try:
      await self.backend.get_cluster_topology()
      databases = await self.backend.list_databases()
      uptime = self.get_uptime()

      return ClusterInfoResponse(
        node_id=f"backend-{type(self.backend).__name__}",
        node_type="backend",
        cluster_version="1.0.0",
        base_path="",
        max_databases=0,
        databases=databases,
        uptime_seconds=uptime,
        read_only=False,
        configuration=None,
      )
    except Exception as e:
      logger.error(f"Failed to get cluster info: {e}")
      raise HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail=f"Failed to get cluster info: {e!s}",
      )
