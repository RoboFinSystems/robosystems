"""Staleness tracking for graph materialization.

Marks a graph as stale after OLTP writes so the AI (and UI) can see
whether the graph database is current with the extensions PostgreSQL data.

Usage:
    from robosystems.operations.extensions.staleness import mark_graph_stale

    # After any OLTP write that changes data:
    mark_graph_stale(graph_id, "schedule_created")
"""

from robosystems.database import SessionFactory
from robosystems.logger import logger
from robosystems.models.core import Graph


def mark_graph_stale(graph_id: str, reason: str) -> None:
  """Mark a graph as stale after an OLTP write.

  This is a synchronous utility — the write is a single UPDATE on the
  platform database and doesn't benefit from async overhead.

  Args:
      graph_id: The graph whose LadybugDB projection is now out of date.
      reason: Short tag describing what caused staleness:
          - schedule_created
          - closing_entry_created
          - connector_sync
          - report_generated
          - transaction_created
          - entity_updated
          - period_closed / period_reopened
          - event_block_created / event_block_updated / event_published
          - journal_entry_updated / journal_entry_deleted
          - obligations_promoted
  """
  try:
    session = SessionFactory()
    try:
      graph = Graph.get_by_id(graph_id, session)
      if graph is None:
        logger.warning(f"Cannot mark stale: graph {graph_id} not found")
        return
      graph.mark_stale(session, reason)
      logger.info(f"Marked graph {graph_id} stale: {reason}")
    finally:
      session.close()
  except Exception as e:
    # Non-fatal — staleness is advisory, not critical path
    logger.warning(f"Failed to mark graph {graph_id} stale: {e}")
