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

    # Check aggregate instance storage cap (hard limit — product cap, blocks
    # COGS overruns). Measured at instance scope: a subgraph shares its
    # parent's box, so the denominator must be the whole instance — measuring
    # the subgraph's own prefix excludes the parent and sibling databases.
    # The row-count checks above stay scoped to the requested graph_id, whose
    # own staging tables are what materialize.
    storage_check = await cls.check_instance_storage(
      db, cls._resolve_instance_scope(db, graph_id), tier
    )
    if not storage_check["allowed"]:
      errors.extend(storage_check["errors"])

    return {
      "allowed": len(errors) == 0,
      # Unverifiable storage is a transient condition, not a limit violation;
      # callers surface it as retryable (503) rather than over-limit (413).
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
    """Map a graph id to the id owning its instance.

    The storage breakdown's ``{id}_*`` prefix scan only covers the given id
    and its children, so a subgraph id must be widened to its parent before
    the instance cap is measured. Mirrors the resolution the ``/limits``
    reporting path performs.
    """
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
    if breakdown is None:
      # Usage could not be measured. This used to fall through as 0.0 GB /
      # healthy / allowed, which silently disabled the cap exactly when the
      # instance was struggling. "Cannot verify" is not "empty": the write
      # path fails closed, and `retryable` tells callers to surface it as a
      # transient condition rather than a limit violation.
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

    items = cls._label_orphans(db, graph_id, breakdown.get("items", []))
    total_bytes = breakdown.get("total_bytes", 0)

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
        # Same reason as total_storage_gb below: 2 decimals of MB bottoms out
        # at ~10.24 KB, which is larger than a freshly created subgraph.
        "size_mb": round(size / (1024**2), 6),
      }
      for gid, size in sorted(bytes_by_database.items())
    ]

    # Not rounded to 2 decimals. A tenant's whole footprint is routinely tens
    # of megabytes against a 20 GB cap, and 0.01 GB is a ~10.7 MB quantum —
    # enough to erase the entire number and to disagree visibly with the
    # itemized bytes below it. 9 decimals is byte-level.
    total_storage_gb = round((total_bytes or 0) / (1024**3), 9)

    # The cap is enforced on durable bytes only. A blue-green build briefly
    # holds a `-wip` copy the size of the database it is rebuilding — counting
    # it would fail a tenant near the cap in the middle of every materialize,
    # and a `-wip` left by a crashed build would block the very operation
    # (materialization) that rebuilds and reclaims it. Orphans stay counted:
    # they are durable, and blocking on them surfaces registry drift instead
    # of hiding it. `total_storage_gb` above stays the full real-disk figure —
    # it is what metering records and what the itemized list sums to.
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
  def _label_orphans(
    cls, db: Session, graph_id: str, items: list[dict[str, Any]]
  ) -> list[dict[str, Any]]:
    """Re-label items belonging to subgraphs the graph registry has no row for.

    ``compute_storage_breakdown`` runs on the instance and classifies every
    ``{parent}_*`` database as a subgraph, because from there a live subgraph
    and the remains of a deleted one look identical. Only here, with a session
    in hand, can the two be told apart.

    That mattered beyond tidiness: ``/usage`` sums these items by type while
    the subgraphs page sums by registered id, so leftovers inflated one number
    and not the other, and the same graph reported two different subgraph
    footprints. The pass covers a deleted subgraph's whole estate — its
    database, its vector index, and any staging file it accumulated before
    staging was closed to subgraphs — not just the ``.lbug`` file. Bytes are
    untouched — this is a labelling pass, and orphans still occupy disk and
    still count against the cap.
    """
    from robosystems.graph_api.core.storage_breakdown import (
      TYPE_ORPHAN,
      TYPE_STAGING,
      TYPE_SUBGRAPH,
      TYPE_VECTORS,
    )

    subgraph_shaped_types = (TYPE_SUBGRAPH, TYPE_VECTORS, TYPE_STAGING)

    def _subgraph_estate(item: dict[str, Any]) -> bool:
      """Whether this item's id names a subgraph (live or deleted).

      The ``_memory`` exclusion matters for the vectors arm: the memory
      database's index is ``{parent}_memory``, which is subgraph-shaped but
      never registered, and must not read as an orphan.
      """
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
      # A labelling refinement is not worth failing a storage check over —
      # the cap reads bytes, which this pass does not touch.
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
