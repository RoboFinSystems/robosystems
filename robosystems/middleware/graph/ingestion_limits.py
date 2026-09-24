"""Limits that block materialization.

Aggregate instance storage is the tier's product cap; the per-operation row
caps (`max_rows_per_copy`, `max_single_table_rows`) are internal OOM
guardrails, not part of the published tier.
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
  """Check graph content limits before materialization."""

  @classmethod
  async def check_materialization_limits(
    cls,
    db: Session,
    graph_id: str,
    tier: str,
    table_name: str | None = None,
  ) -> dict[str, Any]:
    """Check pending rows (total and per table) and aggregate instance storage."""
    limits = GraphTierConfig.get_graph_limits(tier)
    errors: list[str] = []
    warnings: list[str] = []

    pending_rows = cls._get_pending_row_counts(db, graph_id)
    total_pending_rows = sum(pending_rows.values())

    # Fallbacks track ladybug-standard, the smallest tier: a fallback larger
    # than the actual box defeats the guardrail.
    max_rows_per_copy = limits.get("max_rows_per_copy", 1_000_000)
    if total_pending_rows > max_rows_per_copy:
      errors.append(
        f"Total rows ({total_pending_rows:,}) exceeds max_rows_per_copy limit ({max_rows_per_copy:,})"
      )

    max_single_table = limits.get("max_single_table_rows", 2_500_000)
    for tbl_name, row_count in pending_rows.items():
      if row_count > max_single_table:
        errors.append(
          f"Table '{tbl_name}' has {row_count:,} rows, exceeding max_single_table_rows limit ({max_single_table:,})"
        )

    # Storage is measured for the whole instance (a subgraph shares its
    # parent's box); the row checks stay scoped to graph_id's own tables.
    storage_check = await cls.check_instance_storage(
      db, cls._resolve_instance_scope(db, graph_id), tier
    )
    if not storage_check["allowed"]:
      errors.extend(storage_check["errors"])

    return {
      "allowed": len(errors) == 0,
      # Unverifiable storage: callers answer 503, not 413.
      "retryable": storage_check.get("retryable", False),
      "errors": errors,
      "warnings": warnings,
      "current_usage": {
        "total_pending_rows": total_pending_rows,
        "total_storage_gb": storage_check["total_storage_gb"],
        "storage_usage_percentage": storage_check["usage_percentage"],
      },
      "limits": {
        "max_rows_per_copy": max_rows_per_copy,
        "max_single_table_rows": max_single_table,
        "chunk_size_rows": limits.get("chunk_size_rows", 250_000),
        "instance_storage_limit_gb": storage_check["limit_gb"],
      },
      "tier": tier,
    }

  @classmethod
  def _resolve_instance_scope(cls, db: Session, graph_id: str) -> str:
    """A subgraph widens to its parent: the storage scan covers ``{id}_*`` only."""
    from robosystems.models.core import Graph

    graph = Graph.get_by_id(graph_id, db)
    if graph is not None and graph.parent_graph_id:
      return str(graph.parent_graph_id)
    return graph_id

  @classmethod
  async def check_instance_storage(
    cls,
    db: Session,
    graph_id: str,
    tier: str,
  ) -> dict[str, Any]:
    """Aggregate storage for the graph and its subgraphs against the tier cap."""
    limit_gb = GraphTierConfig.get_instance_storage_limit_gb(tier)
    warn_pct = (
      GraphTierConfig.get_graph_limits(tier).get("warn_at_percentage", 80) / 100
    )

    # One call covers the instance: subgraphs, memory database, vector
    # indexes, staging, and on-disk leftovers the registry lost track of.
    breakdown = await cls._get_storage_breakdown(graph_id)
    if breakdown is None:
      # Fail closed: "cannot verify" is not "empty".
      return {
        "allowed": False,
        "retryable": True,
        "errors": [
          f"Instance storage usage for {graph_id} could not be verified "
          "(Graph API unavailable). Retry shortly."
        ],
        "total_storage_gb": None,
        "enforced_storage_gb": None,
        "limit_gb": limit_gb,
        "usage_percentage": None,
        "status": "unknown",
        "databases": [],
        "items": [],
      }

    items = cls.label_orphans(db, graph_id, breakdown.get("items", []))
    total_bytes = breakdown.get("total_bytes", 0)

    bytes_by_database: dict[str, int] = {}
    for item in items:
      item_id = item.get("id") or graph_id
      bytes_by_database[item_id] = bytes_by_database.get(item_id, 0) + item.get(
        "bytes", 0
      )

    databases: list[dict[str, Any]] = [
      {
        "graph_id": gid,
        "is_parent": gid == graph_id,
        # 2 decimals of MB would round a fresh subgraph to zero.
        "size_mb": round(size / (1024**2), 6),
      }
      for gid, size in sorted(bytes_by_database.items())
    ]

    # Byte-level precision: a 0.01 GB quantum (~10.7 MB) can erase a whole
    # tenant footprint.
    total_storage_gb = round((total_bytes or 0) / (1024**3), 9)

    # The cap counts durable bytes only: a blue-green `-wip` copy would fail a
    # tenant near the cap mid-materialize, and a crashed build's leftover would
    # block the materialize that reclaims it. Orphans stay counted.
    # `total_storage_gb` remains the full disk figure that metering records.
    from robosystems.graph_api.core.storage_breakdown import TYPE_TRANSIENT

    transient_bytes = sum(
      item.get("bytes", 0) for item in items if item.get("type") == TYPE_TRANSIENT
    )
    enforced_storage_gb = round(
      max((total_bytes or 0) - transient_bytes, 0) / (1024**3), 9
    )
    usage_percentage = (
      round((enforced_storage_gb / limit_gb) * 100, 1) if limit_gb > 0 else 0
    )

    if usage_percentage > 100:
      instance_status = "over_limit"
    elif usage_percentage >= warn_pct * 100:
      instance_status = "approaching"
    else:
      instance_status = "healthy"

    errors: list[str] = []
    if instance_status == "over_limit":
      errors.append(
        f"Aggregate instance storage {enforced_storage_gb:.2f} GB exceeds "
        f"{tier} limit of {limit_gb:.0f} GB ({usage_percentage:.1f}%). "
        f"Upgrade tier or reduce data before materializing."
      )

    return {
      "allowed": len(errors) == 0,
      "retryable": False,
      "errors": errors,
      "total_storage_gb": total_storage_gb,
      "enforced_storage_gb": enforced_storage_gb,
      "limit_gb": limit_gb,
      "usage_percentage": usage_percentage,
      "status": instance_status,
      "databases": databases,
      "items": items,
    }

  @classmethod
  def label_orphans(
    cls, db: Session, graph_id: str, items: list[dict[str, Any]]
  ) -> list[dict[str, Any]]:
    """Re-label items of subgraphs the graph registry has no row for as orphans.

    The instance can't tell a live subgraph from a deleted one's remains;
    only the registry can. Covers the whole estate (database, vector index,
    staging file). Bytes are untouched, so orphans still count against the cap.
    """
    from robosystems.graph_api.core.storage_breakdown import (
      TYPE_ORPHAN,
      TYPE_STAGING,
      TYPE_SUBGRAPH,
      TYPE_VECTORS,
    )

    subgraph_shaped_types = (TYPE_SUBGRAPH, TYPE_VECTORS, TYPE_STAGING)

    def _subgraph_estate(item: dict[str, Any]) -> bool:
      """``{parent}_memory`` is subgraph-shaped but never registered."""
      item_id = item.get("id") or ""
      return (
        item.get("type") in subgraph_shaped_types
        and item_id.startswith(f"{graph_id}_")
        and item_id != f"{graph_id}_memory"
      )

    if not any(_subgraph_estate(item) for item in items):
      return items

    try:
      from robosystems.models.core import Graph

      registered = {
        str(row[0])
        for row in db.query(Graph.graph_id)
        .filter(Graph.parent_graph_id == graph_id, Graph.deleted_at.is_(None))
        .all()
      }
    except Exception as e:
      # Labelling only; not worth failing a storage check over.
      logger.debug(f"Could not resolve registered subgraphs for {graph_id}: {e}")
      return items

    return [
      {**item, "type": TYPE_ORPHAN}
      if _subgraph_estate(item) and item.get("id") not in registered
      else item
      for item in items
    ]

  @classmethod
  def _get_pending_row_counts(cls, db: Session, graph_id: str) -> dict[str, int]:
    """Row counts per table from GraphFile.duckdb_row_count, set at upload time."""
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
  async def _get_storage_breakdown(cls, graph_id: str) -> dict[str, Any] | None:
    """Itemized disk usage from the Graph API, or None when unavailable."""
    from robosystems.graph_api.client.factory import GraphClientFactory

    try:
      client = await GraphClientFactory.create_client(
        graph_id=graph_id, operation_type="read"
      )
      breakdown = await asyncio.wait_for(
        client.get_storage_breakdown(graph_id), timeout=10
      )
      await client.close()
      return breakdown
    except Exception as e:
      logger.debug(f"Could not get database size for {graph_id}: {e}")
      return None
