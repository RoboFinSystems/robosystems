"""Graph content limit checking for materialization operations.

Enforces tier-based limits on nodes, relationships, and rows before
materialization to prevent graphs from exceeding their tier capacity.

Data sources:
- Row counts: GraphFile.duckdb_row_count (tracked at upload time)
- Current graph counts: Graph API get_database_info() -> node_count, relationship_count
- Tier limits: GraphTierConfig.get_graph_limits(tier)
"""

import logging
from typing import Any

from sqlalchemy import func
from sqlalchemy.orm import Session

from robosystems.config.graph_tier import GraphTierConfig
from robosystems.models.iam import GraphFile

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
    """Check if materialization would exceed tier limits.

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

    # Check max_rows_per_copy
    max_rows_per_copy = limits.get("max_rows_per_copy", 2_000_000)
    if total_pending_rows > max_rows_per_copy:
      errors.append(
        f"Total rows ({total_pending_rows:,}) exceeds max_rows_per_copy limit ({max_rows_per_copy:,})"
      )

    # Check individual table row limits
    max_single_table = limits.get("max_single_table_rows", 5_000_000)
    for tbl_name, row_count in pending_rows.items():
      if row_count > max_single_table:
        errors.append(
          f"Table '{tbl_name}' has {row_count:,} rows, exceeding max_single_table_rows limit ({max_single_table:,})"
        )

    # Try to get current graph counts for node/relationship limit checks
    current_nodes = None
    current_rels = None
    try:
      current_nodes, current_rels = await cls._get_current_graph_counts(graph_id)
    except Exception as e:
      logger.warning(f"Could not get current graph counts for {graph_id}: {e}")

    # Estimate how many new nodes/rels this materialization would add
    node_rows = sum(
      count
      for name, count in pending_rows.items()
      if not cls._is_relationship_table(name)
    )
    rel_rows = sum(
      count for name, count in pending_rows.items() if cls._is_relationship_table(name)
    )

    max_nodes = limits.get("max_nodes", 5_000_000)
    max_rels = limits.get("max_relationships", 10_000_000)
    warn_pct = limits.get("warn_at_percentage", 80) / 100

    # Check node limits (current + pending vs max)
    if current_nodes is not None:
      projected_nodes = current_nodes + node_rows
      if projected_nodes > max_nodes:
        errors.append(
          f"Projected nodes ({projected_nodes:,}) would exceed limit ({max_nodes:,}). "
          f"Current: {current_nodes:,}, pending: {node_rows:,}"
        )
      elif projected_nodes > max_nodes * warn_pct:
        warnings.append(
          f"Approaching node limit: {projected_nodes:,} / {max_nodes:,} ({projected_nodes / max_nodes * 100:.0f}%)"
        )

    # Check relationship limits
    if current_rels is not None:
      projected_rels = current_rels + rel_rows
      if projected_rels > max_rels:
        errors.append(
          f"Projected relationships ({projected_rels:,}) would exceed limit ({max_rels:,}). "
          f"Current: {current_rels:,}, pending: {rel_rows:,}"
        )
      elif projected_rels > max_rels * warn_pct:
        warnings.append(
          f"Approaching relationship limit: {projected_rels:,} / {max_rels:,} ({projected_rels / max_rels * 100:.0f}%)"
        )

    return {
      "allowed": len(errors) == 0,
      "errors": errors,
      "warnings": warnings,
      "current_usage": {
        "current_nodes": current_nodes,
        "current_relationships": current_rels,
        "pending_node_rows": node_rows,
        "pending_relationship_rows": rel_rows,
        "total_pending_rows": total_pending_rows,
      },
      "limits": {
        "max_nodes": max_nodes,
        "max_relationships": max_rels,
        "max_rows_per_copy": max_rows_per_copy,
        "max_single_table_rows": max_single_table,
        "chunk_size_rows": limits.get("chunk_size_rows", 1_000_000),
      },
      "tier": tier,
    }

  @classmethod
  async def check_graph_usage(
    cls,
    graph_id: str,
    tier: str,
  ) -> dict[str, Any]:
    """Check current graph against tier limits (for /limits endpoint).

    Args:
        graph_id: Graph database identifier
        tier: Graph tier

    Returns:
        Dict with: within_limits, warnings, current_usage, limits
    """
    limits = GraphTierConfig.get_graph_limits(tier)
    warnings: list[str] = []

    current_nodes = None
    current_rels = None
    try:
      current_nodes, current_rels = await cls._get_current_graph_counts(graph_id)
    except Exception as e:
      logger.warning(f"Could not get graph counts for {graph_id}: {e}")

    max_nodes = limits.get("max_nodes", 5_000_000)
    max_rels = limits.get("max_relationships", 10_000_000)
    warn_pct = limits.get("warn_at_percentage", 80) / 100

    within_limits = True
    if current_nodes is not None:
      if current_nodes > max_nodes:
        within_limits = False
      elif current_nodes > max_nodes * warn_pct:
        warnings.append(f"nodes ({current_nodes:,} / {max_nodes:,})")

    if current_rels is not None:
      if current_rels > max_rels:
        within_limits = False
      elif current_rels > max_rels * warn_pct:
        warnings.append(f"relationships ({current_rels:,} / {max_rels:,})")

    return {
      "within_limits": within_limits,
      "warnings": warnings,
      "current_usage": {
        "nodes": current_nodes,
        "relationships": current_rels,
      },
      "limits": limits,
    }

  @classmethod
  def _get_pending_row_counts(cls, db: Session, graph_id: str) -> dict[str, int]:
    """Get row counts for all active files in a graph, grouped by table name.

    Uses GraphFile.duckdb_row_count which is populated at upload/staging time.
    """
    results = (
      db.query(
        GraphFile.table_name,
        func.sum(GraphFile.duckdb_row_count).label("total_rows"),
      )
      .filter(
        GraphFile.graph_id == graph_id,
        GraphFile.deleted_at.is_(None),
        GraphFile.duckdb_row_count.isnot(None),
      )
      .group_by(GraphFile.table_name)
      .all()
    )

    return {r.table_name: int(r.total_rows) for r in results if r.table_name}

  @classmethod
  async def _get_current_graph_counts(
    cls, graph_id: str
  ) -> tuple[int | None, int | None]:
    """Get current node and relationship counts from Graph API.

    Returns:
        Tuple of (node_count, relationship_count), either may be None if unavailable
    """
    import asyncio

    from robosystems.graph_api.client.factory import GraphClientFactory

    try:
      client = await GraphClientFactory.create_client(
        graph_id=graph_id, operation_type="read"
      )
      db_info = await asyncio.wait_for(client.get_database_info(graph_id), timeout=10)
      await client.close()

      return db_info.get("node_count"), db_info.get("relationship_count")
    except Exception as e:
      logger.debug(f"Could not get graph counts for {graph_id}: {e}")
      return None, None

  @classmethod
  def _is_relationship_table(cls, table_name: str) -> bool:
    """Detect relationship tables by naming convention.

    Relationship tables use UPPERCASE names or contain _HAS_, _IN_, _OF_ patterns.
    Node tables use PascalCase (e.g., Entity, Fact, Person).
    """
    if not table_name:
      return False
    # All uppercase = relationship table (e.g., ENTITY_HAS_FACT, PERSON_WORKS_AT)
    if table_name == table_name.upper() and "_" in table_name:
      return True
    # Contains common relationship patterns
    rel_patterns = [
      "_HAS_",
      "_IN_",
      "_OF_",
      "_TO_",
      "_FROM_",
      "_BELONGS_",
      "_CONTAINS_",
    ]
    return any(pattern in table_name for pattern in rel_patterns)
