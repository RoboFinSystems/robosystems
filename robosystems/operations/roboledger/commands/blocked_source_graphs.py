"""Write operations for the cross-graph share block list.

These commands run against the *recipient's* tenant schema — the graph doing the
blocking. See ``models/extensions/roboledger/blocked_source_graph.py`` for why
the deny list lives there rather than in the platform DB.
"""

from __future__ import annotations

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from robosystems.models.api.extensions.blocked_source_graphs import (
  BlockedSourceGraphResponse,
  BlockSourceGraphRequest,
  BlockSourceGraphResult,
  UnblockSourceGraphRequest,
)
from robosystems.models.extensions import BlockedSourceGraph, Report
from robosystems.operations.roboledger.reads.blocked_source_graphs import (
  _to_response,
  enrich_blocks,
)


class SelfBlockError(Exception):
  """Raised when a graph tries to block itself."""


class SourceGraphNotBlockedError(LookupError):
  """Raised when unblocking a source that isn't on the block list."""

  def __init__(self, source_graph_id: str) -> None:
    super().__init__(f"Graph '{source_graph_id}' is not blocked.")
    self.source_graph_id = source_graph_id


def block_source_graph(
  session: Session, body: BlockSourceGraphRequest, created_by: str, *, graph_id: str
) -> BlockSourceGraphResult:
  """Block ``source_graph_id`` from sharing reports into this graph.

  Idempotent: re-blocking an already-blocked source returns the existing record
  with ``already_blocked=True`` and leaves ``blocked_at``/``blocked_by`` alone,
  so a retry cannot rewrite who blocked whom and when.

  With ``purge`` set, every report already shared in from that source is deleted
  along with its fact sets and facts. Only shared copies are eligible — the
  ``source_graph_id`` predicate cannot match a report this graph authored.

  Raises `SelfBlockError` if the caller passes their own graph ID.
  """
  if body.source_graph_id == graph_id:
    raise SelfBlockError("A graph cannot block itself.")

  existing = session.execute(
    select(BlockedSourceGraph).where(
      BlockedSourceGraph.source_graph_id == body.source_graph_id
    )
  ).scalar_one_or_none()

  already_blocked = existing is not None
  if existing is None:
    existing = BlockedSourceGraph(
      source_graph_id=body.source_graph_id,
      blocked_by=created_by,
      reason=body.reason,
    )
    session.add(existing)
    session.flush()

  purged = 0
  if body.purge:
    purged = _purge_shared_reports(session, body.source_graph_id)

  return BlockSourceGraphResult(
    block=enrich_blocks([existing])[0],
    already_blocked=already_blocked,
    purged_report_count=purged,
  )


def unblock_source_graph(
  session: Session, body: UnblockSourceGraphRequest
) -> BlockedSourceGraphResponse:
  """Lift a block, allowing that source to share in again.

  Returns the removed record. Purged reports are not restored — unblocking
  reopens the channel, it does not undo.

  Raises `SourceGraphNotBlockedError` when the source was not blocked.
  """
  row = session.execute(
    select(BlockedSourceGraph).where(
      BlockedSourceGraph.source_graph_id == body.source_graph_id
    )
  ).scalar_one_or_none()
  if row is None:
    raise SourceGraphNotBlockedError(body.source_graph_id)

  response = _to_response(row)
  session.delete(row)
  session.flush()
  return response


def _purge_shared_reports(session: Session, source_graph_id: str) -> int:
  """Delete every report shared in from ``source_graph_id``. Returns the count.

  Facts cascade from their parent fact_sets, so the fact_sets go first — the
  same two-step ``delete_report`` uses.
  """
  report_ids = list(
    session.execute(select(Report.id).where(Report.source_graph_id == source_graph_id))
    .scalars()
    .all()
  )
  if not report_ids:
    return 0

  session.execute(
    text("DELETE FROM fact_sets WHERE report_id = ANY(:report_ids)"),
    {"report_ids": report_ids},
  )
  session.execute(
    text("DELETE FROM reports WHERE id = ANY(:report_ids)"),
    {"report_ids": report_ids},
  )
  session.flush()
  return len(report_ids)
