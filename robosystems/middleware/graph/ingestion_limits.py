"""Graph content limit checking for materialization operations.

Enforces per-operation safety limits (rows per copy, rows per table) to prevent
OOM during materialization. Instance storage is tracked as a soft limit for
reporting and alerting — it does not block operations.

Data sources:
- Row counts: GraphFile.duckdb_row_count (tracked at upload time)
- Storage: Graph API get_database_info() -> size_bytes (file stat, instant)
- Tier limits: GraphTierConfig.get_graph_limits(tier)
"""

import asyncio
import logging
from typing import Any

from sqlalchemy import func
from sqlalchemy.orm import Session

from robosystems.config.graph_tier import GraphTierConfig
from robosystems.models.core import GraphFile, GraphTable

logger = logging.getLogger(__name__)


class IngestionLimitChecker:
  """Check graph content limits before materialization operations.

  Uses tier configuration from graph.yml and existing data from
  graph_files table (duckdb_row_count) for enforcement.
  """

  @classmethod
  async def check_materialization_limits(
    cls,
    db: Session,
    graph_id: str,
    tier: str,
    table_name: str | None = None,
  ) -> dict[str, Any]:
    """Check if materialization would exceed per-operation safety limits.

    Only enforces max_rows_per_copy and max_single_table_rows — these are
    hard limits to prevent OOM during a single materialization operation.
    Instance storage capacity is tracked separately as a soft limit.

    Args:
        db: Database session
        graph_id: Graph database identifier
        tier: Graph tier (ladybug-standard, ladybug-large, ladybug-xlarge)
        table_name: Optional specific table being materialized

    Returns:
        Dict with: allowed, errors, warnings, current_usage, limits
    """
    limits = GraphTierConfig.get_graph_limits(tier)
    errors: list[str] = []
    warnings: list[str] = []

    # Get pending row counts from graph_files
    pending_rows = cls._get_pending_row_counts(db, graph_id)
    total_pending_rows = sum(pending_rows.values())

    # Check max_rows_per_copy (hard limit — prevents OOM during materialization)
    max_rows_per_copy = limits.get("max_rows_per_copy", 2_000_000)
    if total_pending_rows > max_rows_per_copy:
      errors.append(
        f"Total rows ({total_pending_rows:,}) exceeds max_rows_per_copy limit ({max_rows_per_copy:,})"
      )

    # Check individual table row limits (hard limit)
    max_single_table = limits.get("max_single_table_rows", 5_000_000)
    for tbl_name, row_count in pending_rows.items():
      if row_count > max_single_table:
        errors.append(
          f"Table '{tbl_name}' has {row_count:,} rows, exceeding max_single_table_rows limit ({max_single_table:,})"
        )

    return {
      "allowed": len(errors) == 0,
      "errors": errors,
      "warnings": warnings,
      "current_usage": {
        "total_pending_rows": total_pending_rows,
      },
      "limits": {
        "max_rows_per_copy": max_rows_per_copy,
        "max_single_table_rows": max_single_table,
        "chunk_size_rows": limits.get("chunk_size_rows", 1_000_000),
      },
      "tier": tier,
    }

  @classmethod
  async def check_instance_storage(
    cls,
    db: Session,
    graph_id: str,
    tier: str,
  ) -> dict[str, Any]:
    """Check aggregate storage usage across the entire dedicated instance.

    Sums storage for the parent graph and all subgraphs. This is a soft
    limit — used for reporting and email alerts, not enforcement.

    Args:
        db: Database session
        graph_id: Parent graph database identifier
        tier: Graph tier

    Returns:
        Dict with: total_storage_gb, limit_gb, usage_percentage, status, databases
    """
    from robosystems.models.core.graph import Graph

    limit_gb = GraphTierConfig.get_instance_storage_limit_gb(tier)
    warn_pct = (
      GraphTierConfig.get_graph_limits(tier).get("warn_at_percentage", 80) / 100
    )

    # Collect all database IDs on this instance (parent + subgraphs)
    database_ids = [(graph_id, True)]
    subgraphs = Graph.get_subgraphs(graph_id, db)
    for sg in subgraphs:
      database_ids.append((sg.graph_id, False))

    # Fetch storage for all databases in parallel
    size_tasks = [cls._get_database_size_bytes(gid) for gid, _ in database_ids]
    sizes = await asyncio.gather(*size_tasks)

    # Build breakdown and total
    databases: list[dict[str, Any]] = []
    total_bytes = 0
    for (gid, is_parent), size_bytes in zip(database_ids, sizes, strict=False):
      size_mb = round(size_bytes / (1024**2), 2) if size_bytes is not None else None
      databases.append(
        {
          "graph_id": gid,
          "is_parent": is_parent,
          "size_mb": size_mb,
        }
      )
      if size_bytes is not None:
        total_bytes += size_bytes

    total_storage_gb = round(total_bytes / (1024**3), 2)
    usage_percentage = (
      round((total_storage_gb / limit_gb) * 100, 1) if limit_gb > 0 else 0
    )

    # Determine status
    if usage_percentage > 100:
      instance_status = "over_limit"
    elif usage_percentage >= warn_pct * 100:
      instance_status = "approaching"
    else:
      instance_status = "healthy"

    return {
      "total_storage_gb": total_storage_gb,
      "limit_gb": limit_gb,
      "usage_percentage": usage_percentage,
      "status": instance_status,
      "databases": databases,
    }

  @classmethod
  async def check_graph_usage(
    cls,
    db: Session,
    graph_id: str,
    tier: str,
  ) -> dict[str, Any]:
    """Check current graph storage usage against tier limits (for /limits endpoint).

    Args:
        db: Database session
        graph_id: Graph database identifier
        tier: Graph tier

    Returns:
        Dict with: within_limits, warnings, instance_usage
    """
    instance_usage = await cls.check_instance_storage(db, graph_id, tier)
    warnings: list[str] = []

    within_limits = instance_usage["status"] != "over_limit"
    if instance_usage["status"] == "approaching":
      warnings.append(
        f"storage ({instance_usage['total_storage_gb']} GB / {instance_usage['limit_gb']} GB)"
      )
    elif instance_usage["status"] == "over_limit":
      warnings.append(
        f"storage over limit ({instance_usage['total_storage_gb']} GB / {instance_usage['limit_gb']} GB, "
        f"{instance_usage['usage_percentage']}%)"
      )

    return {
      "within_limits": within_limits,
      "warnings": warnings,
      "instance_usage": instance_usage,
    }

  @classmethod
  def _get_pending_row_counts(cls, db: Session, graph_id: str) -> dict[str, int]:
    """Get row counts for all active files in a graph, grouped by table name.

    Uses GraphFile.duckdb_row_count which is populated at upload/staging time.
    """
    results = (
      db.query(
        GraphTable.table_name,
        func.sum(GraphFile.duckdb_row_count).label("total_rows"),
      )
      .join(GraphTable, GraphFile.table_id == GraphTable.id)
      .filter(
        GraphFile.graph_id == graph_id,
        GraphFile.upload_status != "failed",
        GraphFile.duckdb_row_count.isnot(None),
      )
      .group_by(GraphTable.table_name)
      .all()
    )

    return {r.table_name: int(r.total_rows) for r in results if r.table_name}

  @classmethod
  async def _get_database_size_bytes(cls, graph_id: str) -> int | None:
    """Get database size in bytes from Graph API (file stat, instant).

    Returns:
        Size in bytes, or None if unavailable
    """
    from robosystems.graph_api.client.factory import GraphClientFactory

    try:
      client = await GraphClientFactory.create_client(
        graph_id=graph_id, operation_type="read"
      )
      db_info = await asyncio.wait_for(client.get_database_info(graph_id), timeout=10)
      await client.close()
      return db_info.get("size_bytes")
    except Exception as e:
      logger.debug(f"Could not get database size for {graph_id}: {e}")
      return None
