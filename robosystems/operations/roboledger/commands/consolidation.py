"""Draft the consolidating entries a group's month needs (spec
``consolidation`` C2): the engine's proposals written as drafts in the
consolidation entity, which its close posts; and the allocation overrides a
person sets in place of a member's pro-rata share."""

from __future__ import annotations

from datetime import UTC, date, datetime

from sqlalchemy import text
from sqlalchemy.orm import Session

from robosystems.models.api.extensions.consolidation import (
  AllocationOverrideResponse,
  ConsolidationProposal,
  ProposeConsolidationEntriesRequest,
  ProposeConsolidationEntriesResponse,
  SetAllocationOverrideRequest,
)
from robosystems.models.api.extensions.journal_entries import (
  CreateJournalEntryRequest,
  JournalEntryLineItemInput,
  JournalEntryResponse,
)
from robosystems.models.extensions import Entity
from robosystems.models.extensions.roboledger import Entry, LineItem, Transaction
from robosystems.operations.locking import lock_by_id
from robosystems.operations.roboledger.commands._guards import (
  assert_period_not_closed,
)
from robosystems.operations.roboledger.commands.journal_entries import (
  _entry_to_response,
  _flow_qnames,
  _load_line_items,
  create_journal_entry,
)
from robosystems.operations.roboledger.commands.opening_balances import OPENING
from robosystems.operations.roboledger.entity_scope import (
  is_group_parent,
  resolve_entity,
)
from robosystems.operations.roboledger.reads.consolidation import (
  ALLOCATION_OVERRIDES,
  ENGINE_PROVENANCE,
  ENGINE_SOURCE,
  engine_drafts,
  engine_opening,
  first_month_eve,
  month_bounds,
  opening_is_recorded,
  preview_consolidation_entries,
)


def propose_consolidation_entries(
  session: Session, body: ProposeConsolidationEntriesRequest, created_by: str
) -> ProposeConsolidationEntriesResponse:
  """Write the month's proposals as drafts in the consolidation entity,
  replacing the engine's earlier drafts of the month. A closed month is left
  as posted. Drafts are not held until they post, so the proposals are the
  same whether or not earlier drafts exist.

  Raises ``NoConsolidationEntityError`` when the group has none."""
  # One proposer per month at a time: two would each replace the same drafts
  # and each write a full set.
  session.execute(
    text(
      "SELECT pg_advisory_xact_lock(hashtext('consolidation_propose'), "
      "hashtext(current_schema() || :period))"
    ),
    {"period": body.period},
  )
  preview = preview_consolidation_entries(session, period=body.period)
  consolidation_id = preview.consolidation_entity_id
  response = ProposeConsolidationEntriesResponse(
    period=preview.period,
    consolidation_entity_id=consolidation_id,
    closed=preview.closed,
    residuals=preview.residuals,
    reconciling_items=preview.reconciling_items,
    notes=preview.notes,
  )
  if preview.closed:
    return response

  start, end = month_bounds(body.period)
  # Fences the month against a close that began after the preview read it.
  assert_period_not_closed(session, end, entity_id=consolidation_id)
  stale = engine_drafts(session, consolidation_id, start, end)
  for entry in stale:
    session.delete(entry)
  session.flush()
  response.replaced = len(stale)

  eve = first_month_eve(session, consolidation_id, start)
  if eve is not None and not opening_is_recorded(
    session, consolidation_id, preview.proposals
  ):
    opening = _record_opening(
      session,
      consolidation_id,
      eve,
      [p for p in preview.proposals if p.opening],
      created_by,
    )
    if opening is not None:
      response.entries.append(opening)

  for proposal in preview.proposals:
    if proposal.opening:
      continue
    response.entries.append(
      create_journal_entry(
        session,
        CreateJournalEntryRequest(
          posting_date=proposal.posting_date,
          memo=proposal.memo,
          type="adjusting",
          status="draft",
          source=ENGINE_SOURCE,
          adjustment_kind=proposal.adjustment_kind,
          adjusts_entity_id=proposal.adjusts_entity_id,
          line_items=[
            JournalEntryLineItemInput(
              element_id=line.element_id,
              debit_amount=line.debit_amount,
              credit_amount=line.credit_amount,
              flow_qname=line.flow_qname,
            )
            for line in proposal.lines
          ],
        ),
        created_by,
        entity_id=consolidation_id,
      )
    )
  session.commit()
  return response


def _record_opening(
  session: Session,
  consolidation_id: str,
  eve: date,
  proposals: list[ConsolidationProposal],
  created_by: str,
) -> JournalEntryResponse | None:
  """Replace the consolidation entity's opening with ``proposals``: one
  posted ``opening`` entry on the eve of its first month, each line carrying
  the kind and member it holds. None when the opening needs nothing."""
  replaced = engine_opening(session, consolidation_id)
  if replaced is not None:
    session.execute(
      text("DELETE FROM line_items WHERE entry_id = :e"), {"e": replaced.id}
    )
    transaction_id = replaced.transaction_id
    session.delete(replaced)
    session.flush()
    if transaction_id:
      txn = session.get(Transaction, transaction_id)
      if txn is not None:
        session.delete(txn)
        session.flush()
  if not proposals:
    return None

  memo = "Opening consolidation balances"
  now = datetime.now(UTC)
  txn = Transaction(
    entity_id=consolidation_id,
    type="opening_balance",
    amount=sum(line.debit_amount for p in proposals for line in p.lines),
    date=eve,
    description=memo,
    source="native",
    status="posted",
    posted_at=now,
    created_by=created_by,
  )
  session.add(txn)
  session.flush()
  entry = Entry(
    entity_id=consolidation_id,
    transaction_id=txn.id,
    type=OPENING,
    status="posted",
    posting_date=eve,
    memo=memo,
    provenance=ENGINE_PROVENANCE,
    posted_at=now,
    created_by=created_by,
  )
  session.add(entry)
  session.flush()
  order = 0
  for proposal in proposals:
    for line in proposal.lines:
      order += 1
      session.add(
        LineItem(
          entry_id=entry.id,
          element_id=line.element_id,
          debit_amount=line.debit_amount,
          credit_amount=line.credit_amount,
          description=proposal.memo,
          metadata_={
            "adjustment_kind": proposal.adjustment_kind,
            "adjusts_entity_id": proposal.adjusts_entity_id,
          },
          line_order=order,
        )
      )
  session.flush()
  lines = _load_line_items(session, entry.id)
  return _entry_to_response(entry, lines, _flow_qnames(session, lines))


class AllocationOverrideError(ValueError):
  """An allocation override that cannot be set as given."""


def set_allocation_override(
  session: Session, body: SetAllocationOverrideRequest, created_by: str
) -> AllocationOverrideResponse:
  """Set, or clear, the outside owners' share of a member's result for one
  month, in place of the pro-rata share the engine would propose. Refused in
  a month the consolidation entity has closed.

  Raises ``AllocationOverrideError`` for a member not consolidated in full
  or an override without one share and a basis."""
  entity = resolve_entity(session, body.entity_id)
  if (
    is_group_parent(session, str(entity.id))
    or entity.entity_role != "operating"
    or entity.consolidation_method != "full"
  ):
    raise AllocationOverrideError(
      f"{entity.name} is not a member consolidated in full; only such a "
      "member's outside owners have a share of its result to override."
    )
  if body.clear:
    if body.amount is not None or body.percentage is not None:
      raise AllocationOverrideError("Clearing an override takes no share.")
  else:
    if (body.amount is None) == (body.percentage is None):
      raise AllocationOverrideError("Give the share as an amount or a percentage.")
    if not (body.basis or "").strip():
      raise AllocationOverrideError(
        "Give the basis: why the share differs from the ownership percentage."
      )
  _start, end = month_bounds(body.period)
  consolidation_id = session.execute(
    text(
      "SELECT id FROM entities WHERE entity_role = 'consolidation' "
      "AND source <> 'linked' ORDER BY created_at, id LIMIT 1"
    )
  ).scalar()
  # A group with no consolidation entity yet has no consolidated month closed.
  if consolidation_id is not None:
    assert_period_not_closed(session, end, entity_id=str(consolidation_id))

  locked = lock_by_id(
    session,
    Entity,
    entity.id,
    f"{entity.name} is being updated by another process. Retry in a moment.",
  )
  if locked is None:
    raise AllocationOverrideError(f"{entity.name} is no longer in the group.")
  metadata = dict(locked.metadata_ or {})
  overrides = dict(metadata.get(ALLOCATION_OVERRIDES) or {})
  if body.clear:
    overrides.pop(body.period, None)
    record: dict = {}
  else:
    record = {
      "amount": body.amount,
      "percentage": body.percentage,
      "basis": (body.basis or "").strip(),
      "set_by": created_by,
    }
    overrides[body.period] = record
  metadata[ALLOCATION_OVERRIDES] = overrides
  locked.metadata_ = metadata
  session.commit()
  return AllocationOverrideResponse(
    entity_id=str(entity.id),
    period=body.period,
    amount=record.get("amount"),
    percentage=record.get("percentage"),
    basis=record.get("basis"),
    set_by=record.get("set_by"),
    cleared=body.clear,
  )
