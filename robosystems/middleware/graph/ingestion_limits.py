"""Graph content limit checking for materialization operations.

Two limit categories, both hard-blocking at the write path:

1. **Aggregate storage GB** — the tier-scoped product cap. Bounds
   instance disk COGS, drives upgrades. Hit at the write path so
   customers can't pile up data they then can't promote.
2. **Per-operation row caps** (`max_rows_per_copy`,
   `max_single_table_rows`) — engineering OOM guardrails. Set per
   instance class, framed internally; not surfaced as the marketing
   tier limit.

Both block materialization when exceeded.

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
    """Check if materialization would exceed any tier limit.

    Hard-blocks on three checks:

    - ``max_rows_per_copy`` — OOM guardrail across all pending tables
    - ``max_single_table_rows`` — OOM guardrail per table
    - aggregate instance storage GB — product cap

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

    # Check max_rows_per_copy (hard limit — prevents OOM during materialization).
    # Fallbacks here and below track ladybug-standard, the smallest tier, for the
    # reason given in GraphTierConfig.get_graph_limits: a fallback larger than the
    # actual box defeats the guardrail.
    max_rows_per_copy = limits.get("max_rows_per_copy", 1_000_000)
    if total_pending_rows > max_rows_per_copy:
      errors.append(
        f"Total rows ({total_pending_rows:,}) exceeds max_rows_per_copy limit ({max_rows_per_copy:,})"
      )

    # Check individual table row limits (hard limit)
    max_single_table = limits.get("max_single_table_rows", 2_500_000)
    for tbl_name, row_count in pending_rows.items():
      if row_count > max_single_table:
        errors.append(
          f"Table '{tbl_name}' has {row_count:,} rows, exceeding max_single_table_rows limit ({max_single_table:,})"
        )

    # Check aggregate instance storage cap (hard limit — product cap, blocks COGS overruns)
    storage_check = await cls.check_instance_storage(db, graph_id, tier)
    if not storage_check["allowed"]:
      errors.extend(storage_check["errors"])

    return {
      "allowed": len(errors) == 0,
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
  async def check_instance_storage(
    cls,
    db: Session,
    graph_id: str,
    tier: str,
  ) -> dict[str, Any]:
    """Check aggregate storage usage against the tier's instance cap.

    Sums storage for the parent graph and all subgraphs. Returns
    ``allowed=False`` with a populated ``errors`` list when the
    aggregate exceeds the tier's ``instance_storage_limit_gb``. This is
    a hard product cap, blocking at the write path (folded into
    :meth:`check_materialization_limits`).

    Args:
        db: Database session
        graph_id: Parent graph database identifier
        tier: Graph tier

    Returns:
        Dict with: allowed, errors, total_storage_gb, limit_gb,
        usage_percentage, status, databases
    """
    limit_gb = GraphTierConfig.get_instance_storage_limit_gb(tier)
    warn_pct = (
      GraphTierConfig.get_graph_limits(tier).get("warn_at_percentage", 80) / 100
    )

    # One call covers the whole instance. Subgraphs always live on their
    # parent's instance, so the breakdown's `{id}_*` scan already includes
    # them — plus the memory database, vector indexes and staging file. This
    # also replaces the previous N+1 (one Graph API call per subgraph), and
    # catches on-disk leftovers the graph registry has lost track of.
    breakdown = await cls._get_storage_breakdown(graph_id)
    items: list[dict[str, Any]] = breakdown.get("items", []) if breakdown else []
    total_bytes = breakdown.get("total_bytes", 0) if breakdown else None

    # Roll the itemized view up per database for the summary shape, so a
    # database's graph/vector/staging bytes appear as one line.
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
        "size_mb": round(size / (1024**2), 2),
      }
      for gid, size in sorted(bytes_by_database.items())
    ]

    total_storage_gb = round((total_bytes or 0) / (1024**3), 2)
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

    errors: list[str] = []
    if instance_status == "over_limit":
      errors.append(
        f"Aggregate instance storage {total_storage_gb:.2f} GB exceeds "
        f"{tier} limit of {limit_gb:.0f} GB ({usage_percentage:.1f}%). "
        f"Upgrade tier or reduce data before materializing."
      )

    return {
      "allowed": len(errors) == 0,
      "errors": errors,
      "total_storage_gb": total_storage_gb,
      "limit_gb": limit_gb,
      "usage_percentage": usage_percentage,
      "status": instance_status,
      "databases": databases,
      "items": items,
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
  async def _get_storage_breakdown(cls, graph_id: str) -> dict[str, Any] | None:
    """Get itemized disk usage for a graph, from the Graph API.

    Covers the graph's own database plus its memory database, subgraph
    databases, vector indexes and staging file — all real disk on the same
    instance. The pieces outside the primary ``.lbug`` frequently outweigh
    it, so measuring only that file undercounts the cap denominator and
    therefore real COGS.

    Returns:
        ``{graph_id, total_bytes, items}``, or None if unavailable
    """
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
