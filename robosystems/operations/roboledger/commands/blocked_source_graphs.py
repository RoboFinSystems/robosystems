"""Write operations for the cross-graph share block list.

These commands run against the *recipient's* tenant schema — the graph doing the
blocking. See ``models/extensions/roboledger/blocked_source_graph.py`` for why
the deny list lives there rather than in the platform DB.
"""

from __future__ import annotations

from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError
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


class AdminRoleRequiredError(Exception):
  """Raised when a writer attempts an admin-only half of the block controls.

  Stopping a sender is open to any writer; purging what already landed and
  re-opening a closed channel are not. See `block_source_graph`.
  """


class SourceGraphNotBlockedError(LookupError):
  """Raised when unblocking a source that isn't on the block list."""

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

  Idempotent: re-blocking an already-blocked source returns the existing record
  with ``already_blocked=True`` and leaves ``blocked_at``/``blocked_by`` alone,
  so a retry cannot rewrite who blocked whom and when.

  With ``purge`` set, every report already shared in from that source is deleted
  along with its fact sets and facts. Only shared copies are eligible — the
  ``source_graph_id`` predicate cannot match a report this graph authored.

  **Blocking is open to any writer; purging is not.** Stopping an unwanted
  sender should be the easy half — a member who notices a bad share should not
  have to find an admin to make it stop. Deleting what already landed is bulk
  and irreversible, and deleting one shared report already requires an admin
  (`delete_report`); letting a member remove all of them through this door
  would make that gate decorative.

  Raises `SelfBlockError` if the caller passes their own graph ID, or
  `AdminRoleRequiredError` if a non-admin requests a purge.
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
      # Savepoint: a concurrent block of the same source between the read
      # above and this insert trips the unique key; that is the idempotent
      # case, not a fault, so fall back to the row that won.
      with session.begin_nested():
        session.add(existing)
        session.flush()
    except IntegrityError:
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

  Returns the removed record. Purged reports are not restored — unblocking
  reopens the channel, it does not undo.

  **Admin only.** A block is a standing decision about who may write into this
  graph, so reversing it is not something a member should be able to do over an
  admin's head. This is also why the operation is hand-mounted in the router
  rather than declared through the registrar: every registrar spec becomes an
  MCP tool, and shared reports are readable by the recipient's AI operators —
  an operator that could lift a block, over content a blocked sender supplied,
  would hand the sender the undo button for their own exclusion.

  Raises `SourceGraphNotBlockedError` when the source was not blocked, or
  `AdminRoleRequiredError` when the caller is not a graph admin.
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
  """Delete every report shared in from ``source_graph_id``. Returns their ids.

  Facts cascade from their parent fact_sets, so the fact_sets go first — the
  same two-step ``delete_report`` uses.

  The ids are returned rather than the count because a purged copy's stored
  publication has to go too — otherwise the sender's report survives the purge
  in the recipient's bundle prefix. That deletion runs from the router's
  after-success hook, not here: this function does not own the transaction, and
  removing an artifact for a purge that then rolls back would destroy a live
  report's publication. An orphan object after a crash is the recoverable
  direction; a missing one is not.
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
