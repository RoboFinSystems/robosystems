"""Mark a graph stale after an OLTP write, so readers can tell whether the
graph is current with the extensions database."""

import time
from datetime import UTC, datetime

from robosystems.config.shared_repositories import is_shared_repository_or_subgraph
from robosystems.database import SessionFactory
from robosystems.logger import logger
from robosystems.middleware.graph.utils.subgraph import is_subgraph
from robosystems.models.api.views import GraphFreshness
from robosystems.models.core import Graph

_MARK_ATTEMPTS = 3
_MARK_BACKOFF_SECONDS = 0.2


def mark_graph_stale(graph_id: str, reason: str) -> None:
  """Record that a graph's LadybugDB projection is behind its OLTP data.

  ``reason`` is a short tag naming what changed — ``schedule_created``,
  ``connector_sync``, ``period_closed``, ``journal_entry_updated``, and so on.
  It is surfaced to operators and the AI, so keep it specific.

  Synchronous by design: this is a single UPDATE on the platform database.
  Never raises: the OLTP write has already committed in another database, so
  failing the request would report a write that happened as one that did not.
  The mark is how the write reaches the graph, though, so a failure is retried
  and a final one is logged as ``graph_stale_mark_failed`` for alerting.
  """
  for attempt in range(1, _MARK_ATTEMPTS + 1):
    try:
      session = SessionFactory()
      try:
        graph = Graph.get_by_id(graph_id, session)
        if graph is None:
          logger.warning(f"Cannot mark stale: graph {graph_id} not found")
          return
        graph.mark_stale(session, reason)
        logger.info(f"Marked graph {graph_id} stale: {reason}")
        return
      finally:
        session.close()
    except Exception as e:
      if attempt < _MARK_ATTEMPTS:
        time.sleep(_MARK_BACKOFF_SECONDS * attempt)
        continue
      logger.error(
        "graph_stale_mark_failed",
        extra={
          "graph_id": graph_id,
          "reason": reason,
          "attempts": attempt,
          "error": str(e),
        },
      )


def _iso(value: datetime | None) -> str | None:
  if value is None:
    return None
  if value.tzinfo is None:
    value = value.replace(tzinfo=UTC)
  return value.isoformat()


def graph_freshness(graph_id: str) -> GraphFreshness | None:
  """Whether a ledger's graph is current with its books.

  ``None`` where the question does not apply — shared repositories and
  subgraphs are not rebuilt from a ledger — or the graph has no row.
  ``stale_since`` is the onset (``graph_stale_since``), not the latest write.
  """
  if is_shared_repository_or_subgraph(graph_id) or is_subgraph(graph_id):
    return None
  with SessionFactory() as session:
    graph = Graph.get_by_id(graph_id, session)
    if graph is None:
      return None
    stale = bool(graph.graph_stale)
    metadata = graph.graph_metadata or {}
    return GraphFreshness(
      stale=stale,
      stale_since=_iso(graph.graph_stale_since or graph.graph_stale_at)
      if stale
      else None,
      stale_reason=graph.graph_stale_reason if stale else None,
      last_materialized_at=metadata.get("last_materialized_at"),
    )
