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
  """Record that a graph's LadybugDB projection is behind its OLTP data.

  ``reason`` is a short tag naming what changed — ``schedule_created``,
  ``connector_sync``, ``period_closed``, ``journal_entry_updated``, and so on.
  It is surfaced to operators and the AI, so keep it specific.

  Synchronous by design: this is a single UPDATE on the platform database.
  Never raises — staleness is advisory, and losing the flag must not fail the
  write that triggered it.
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
    logger.warning(f"Failed to mark graph {graph_id} stale: {e}")
