"""Draft the consolidating entries a group's month needs (spec
``consolidation`` C2): the engine's proposals written as drafts in the
consolidation entity, which its close posts."""

from __future__ import annotations

from sqlalchemy.orm import Session

from robosystems.models.api.extensions.consolidation import (
  ProposeConsolidationEntriesRequest,
  ProposeConsolidationEntriesResponse,
)
from robosystems.models.api.extensions.journal_entries import (
  CreateJournalEntryRequest,
  JournalEntryLineItemInput,
)
from robosystems.operations.roboledger.commands._guards import (
  assert_period_not_closed,
)
from robosystems.operations.roboledger.commands.journal_entries import (
  create_journal_entry,
)
from robosystems.operations.roboledger.reads.consolidation import (
  ENGINE_SOURCE,
  engine_drafts,
  month_bounds,
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
  assert_period_not_closed(session, end, entity_id=consolidation_id)
  stale = engine_drafts(session, consolidation_id, start, end)
  for entry in stale:
    session.delete(entry)
  session.flush()
  response.replaced = len(stale)

  for proposal in preview.proposals:
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
