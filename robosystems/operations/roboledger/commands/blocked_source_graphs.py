"""Write operations for the cross-graph share block list.

These commands run against the *recipient's* tenant schema — the graph doing the
blocking. See ``models/extensions/roboledger/blocked_source_graph.py`` for why
the deny list lives there rather than in the platform DB.
"""

from __future__ import annotations

from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from robosystems.db.integrity import violates
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
  """A graph tried to block itself."""


class AdminRoleRequiredError(Exception):
  """A non-admin asked to purge shared reports or lift a block."""


class SourceGraphNotBlockedError(LookupError):
  def __init__(self, source_graph_id: str) -> None:
    super().__init__(f"Graph '{source_graph_id}' is not blocked.")
    self.source_graph_id = source_graph_id


def block_source_graph(
  session: Session,
  body: BlockSourceGraphRequest,
  created_by: str,
  *,
  graph_id: str,
  acting_user_is_graph_admin: bool = False,
) -> BlockSourceGraphResult:
  """Block ``source_graph_id`` from sharing reports into this graph.

  Idempotent: a re-block returns the existing record (``already_blocked=True``)
  without rewriting ``blocked_at``/``blocked_by``.

  ``purge`` deletes every report shared in from that source (never one this
  graph authored). Blocking is open to any writer; purging is admin-only, as
  deleting a single shared report already is.

  Raises `SelfBlockError` or `AdminRoleRequiredError`.
  """
  if body.source_graph_id == graph_id:
    raise SelfBlockError("A graph cannot block itself.")
  if body.purge and not acting_user_is_graph_admin:
    raise AdminRoleRequiredError(
      "Purging reports already shared in requires the graph admin role. "
      "Block without `purge` to stop further shares."
    )

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
    try:
      # Savepoint: a concurrent block of the same source is the idempotent
      # case; fall back to the row that won.
      with session.begin_nested():
        session.add(existing)
        session.flush()
    except IntegrityError as exc:
      if not violates(exc, "uq_blocked_source_graphs_source"):
        raise
      existing = session.execute(
        select(BlockedSourceGraph).where(
          BlockedSourceGraph.source_graph_id == body.source_graph_id
        )
      ).scalar_one()
      already_blocked = True

  purged_ids: list[str] = []
  if body.purge:
    purged_ids = _purge_shared_reports(session, body.source_graph_id)

  return BlockSourceGraphResult(
    block=enrich_blocks([existing])[0],
    already_blocked=already_blocked,
    purged_report_count=len(purged_ids),
    purged_report_ids=purged_ids,
  )


def unblock_source_graph(
  session: Session,
  body: UnblockSourceGraphRequest,
  *,
  acting_user_is_graph_admin: bool = False,
) -> BlockedSourceGraphResponse:
  """Lift a block, allowing that source to share in again.

  Returns the removed record; purged reports are not restored.

  Admin only, and hand-mounted in the router rather than registered, because
  every registrar spec becomes an MCP tool: an AI operator reading content from
  a blocked sender must never be able to lift the block.

  Raises `SourceGraphNotBlockedError` or `AdminRoleRequiredError`.
  """
  if not acting_user_is_graph_admin:
    raise AdminRoleRequiredError("Lifting a block requires the graph admin role.")

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


def _purge_shared_reports(session: Session, source_graph_id: str) -> list[str]:
  """Delete every report shared in from ``source_graph_id`` and return their ids.

  The router's after-success hook uses the ids to delete each copy's stored
  publication; doing it here, inside an uncommitted transaction, could destroy
  a live report's artifact on rollback.
  """
  report_ids = list(
    session.execute(select(Report.id).where(Report.source_graph_id == source_graph_id))
    .scalars()
    .all()
  )
  if not report_ids:
    return []

  from robosystems.operations.roboledger.commands.reports import (
    delete_report_fact_sets,
  )

  delete_report_fact_sets(session, report_ids)
  session.execute(
    text("DELETE FROM reports WHERE id = ANY(:report_ids)"),
    {"report_ids": report_ids},
  )
  session.flush()
  return report_ids
