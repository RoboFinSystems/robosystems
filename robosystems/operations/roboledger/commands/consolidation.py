"""Draft the consolidating entries a group's month needs (spec
``consolidation`` C2): the engine's proposals written as drafts in the
consolidation entity, which its close posts; the allocation overrides a
person sets in place of a member's pro-rata share; and a member's
acquisition, whose consideration and fair values only a person can give."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from robosystems.models.api.extensions.consolidation import (
  AcquisitionResponse,
  AllocationOverrideResponse,
  ConsolidationProposal,
  OwnershipChangeResponse,
  ProposeConsolidationEntriesRequest,
  ProposeConsolidationEntriesResponse,
  RecordAcquisitionRequest,
  RecordOwnershipChangeRequest,
  SetAllocationOverrideRequest,
)
from robosystems.models.api.extensions.journal_entries import (
  CreateJournalEntryRequest,
  JournalEntryLineItemInput,
  JournalEntryResponse,
)
from robosystems.models.extensions import Entity
from robosystems.models.extensions.roboledger import Entry, LineItem, Transaction
from robosystems.operations.locking import RowLockedError, lock_by_id
from robosystems.operations.roboledger.commands._guards import (
  assert_period_not_closed,
)
from robosystems.operations.roboledger.commands.journal_entries import (
  _entry_to_response,
  _flow_qnames,
  _load_line_items,
  create_journal_entry,
)
from robosystems.operations.roboledger.commands.opening_balances import (
  OPENING,
  opening_entry,
)
from robosystems.operations.roboledger.entity_scope import (
  find_consolidation_entity,
  is_group_parent,
  resolve_entity,
  resolve_entity_id,
)
from robosystems.operations.roboledger.reads.consolidation import (
  ACQUISITION,
  ALLOCATION_OVERRIDES,
  ENGINE_PROVENANCE,
  ENGINE_SOURCE,
  EQUITY_CLASSIFICATIONS,
  GOODWILL,
  INVESTMENT_IN_SUBSIDIARIES,
  NONCONTROLLING_INTEREST,
  OWNERSHIP_CHANGE,
  capital_and_earnings,
  elimination_flow,
  engine_drafts,
  engine_opening,
  first_month_eve,
  month_bounds,
  opening_is_recorded,
  ownership_changes,
  pct_as_of,
  posting_accounts,
  preview_consolidation_entries,
  primary_concepts,
  share_of,
)
from robosystems.operations.roboledger.reports.network_picker import (
  load_close_target_concept,
  load_entity_reporting_style,
  taggable_flow_qnames,
)
from robosystems.operations.roboledger.supersede import (
  record_replacements,
  supersede_entries,
)
from robosystems.operations.taxonomy_block.coa_mappings import (
  BOOK_FRAMEWORK,
  find_entity_mapping,
)

BARGAIN_PURCHASE_GAIN = "rs-gaap:BusinessCombinationBargainPurchaseGainRecognizedAmount"


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
  superseded = (
    supersede_entries(
      session,
      [replaced.id],
      op="opening_replaced",
      actor=created_by,
      reason="The engine proposed the consolidation entity's opening again.",
    )
    if replaced is not None
    else []
  )
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
  try:
    session.flush()
  except IntegrityError as exc:
    raise RowLockedError(
      "Another opening balance was recorded for the consolidation entity at "
      "the same time. Propose the month again."
    ) from exc
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
  record_replacements(session, superseded, [entry.id])
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


class AcquisitionError(ValueError):
  """An acquisition that cannot be recorded as given."""


def _equity_at_acquisition(session: Session, member: Entity, opening: Entry) -> int:
  """The member's equity in its opening balance, credit-positive, read by the
  concept each account maps to as the engine reads its capital."""
  mapping = find_entity_mapping(session, str(member.id), BOOK_FRAMEWORK)
  if mapping is None:
    raise AcquisitionError(
      f"{member.name} has no chart mapping; its equity at acquisition cannot be "
      "read. Map its chart first."
    )
  concepts = primary_concepts(session, [str(mapping.id)])
  equity = 0
  for line in session.execute(
    select(LineItem).where(LineItem.entry_id == opening.id)
  ).scalars():
    concept = concepts.get(str(line.element_id))
    if (
      concept is not None
      and concept.classification in EQUITY_CLASSIFICATIONS
      and concept.qname != NONCONTROLLING_INTEREST
    ):
      equity += int(line.credit_amount) - int(line.debit_amount)
  return equity


def record_acquisition(
  session: Session, body: RecordAcquisitionRequest, created_by: str
) -> AcquisitionResponse:
  """Record a member bought from its outside owners as a draft in the
  consolidation entity, dated the day it joined, which the consolidation
  entity's close posts: goodwill (or a bargain purchase gain) and the
  fair-value step-ups against the excess of the consideration over the
  parent's share of the member's equity at acquisition, and the outside
  owners' share of what they add. The engine then reads the consideration as
  the parent's investment in the member. Replaces an earlier draft of it.

  Raises ``AcquisitionError`` for a member that did not join mid-life with an
  opening balance, a consolidation entity lacking an account the entry needs,
  a posted acquisition already held, or a bargain purchase not confirmed."""
  member = resolve_entity(session, body.entity_id)
  name = member.name
  if (
    is_group_parent(session, str(member.id))
    or member.entity_role != "operating"
    or member.consolidation_method != "full"
  ):
    raise AcquisitionError(
      f"{name} is not a member consolidated in full; only such a member's "
      "acquisition is eliminated against its equity."
    )
  if member.joined_on is None:
    raise AcquisitionError(
      f"{name} has no joined_on date; set the day it joined the group first."
    )
  opening = opening_entry(session, str(member.id))
  if opening is None:
    raise AcquisitionError(
      f"{name}'s books have no opening balance. Record its balance sheet at "
      "acquisition with record-opening-balance first: the acquisition is "
      "measured against it."
    )
  consolidation = find_consolidation_entity(session)
  if consolidation is None:
    raise AcquisitionError(
      "This group has no consolidation entity; create one with entity_role "
      "'consolidation' for the acquisition entry."
    )
  consolidation_id = str(consolidation.id)
  assert_period_not_closed(session, member.joined_on, entity_id=consolidation_id)

  computed = {
    GOODWILL,
    BARGAIN_PURCHASE_GAIN,
    INVESTMENT_IN_SUBSIDIARIES,
    NONCONTROLLING_INTEREST,
  }
  for allocation in body.allocations:
    if allocation.concept in computed:
      raise AcquisitionError(
        f"{allocation.concept} is computed from the consideration, not "
        "allocated; a step-up names an identifiable asset or liability."
      )
    if not allocation.amount:
      raise AcquisitionError(
        f"The step-up to {allocation.concept} is zero; leave it out."
      )

  share = (
    float(member.ownership_pct) / 100.0 if member.ownership_pct is not None else 1.0
  )
  equity = _equity_at_acquisition(session, member, opening)
  parent_part = share_of(equity, share)
  outside_equity = equity - parent_part
  stepped = sum(a.amount for a in body.allocations)
  excess = body.consideration - parent_part
  if body.nci_fair_value is not None:
    if share >= 1.0:
      raise AcquisitionError(
        f"{name} has no outside owners; nci_fair_value is for a member the "
        "parent owns less than all of."
      )
    nci = body.nci_fair_value - outside_equity
  else:
    nci = stepped - share_of(stepped, share)
  goodwill = excess + nci - stepped
  if goodwill < 0 and not body.confirm_bargain_purchase:
    raise AcquisitionError(
      f"The parent paid {_cents(body.consideration)} for a share worth "
      f"{_cents(body.consideration - goodwill)} at fair value: a bargain "
      f"purchase of {_cents(-goodwill)}. ASC 805 has the acquirer reassess the "
      "assets, liabilities and consideration before recognizing the gain; "
      "after that review, record it again with confirm_bargain_purchase."
    )

  wanted: dict[str, int] = {}
  for allocation in body.allocations:
    wanted[allocation.concept] = wanted.get(allocation.concept, 0) + allocation.amount
  if goodwill:
    concept = GOODWILL if goodwill > 0 else BARGAIN_PURCHASE_GAIN
    wanted[concept] = wanted.get(concept, 0) + goodwill
  if excess:
    wanted[INVESTMENT_IN_SUBSIDIARIES] = (
      wanted.get(INVESTMENT_IN_SUBSIDIARIES, 0) - excess
    )
  if nci:
    wanted[NONCONTROLLING_INTEREST] = wanted.get(NONCONTROLLING_INTEREST, 0) - nci
  wanted = {q: a for q, a in wanted.items() if a}

  if not wanted:
    raise AcquisitionError(
      f"The consideration equals the parent's share of {name}'s equity at "
      "acquisition and nothing is stepped up: there is no excess to record."
    )
  consolidation_mapping = find_entity_mapping(session, consolidation_id, BOOK_FRAMEWORK)
  accounts = (
    posting_accounts(
      session,
      consolidation_id,
      primary_concepts(session, [str(consolidation_mapping.id)]),
    )
    if consolidation_mapping is not None
    else {}
  )
  missing = sorted(q for q in wanted if q not in accounts)
  if missing:
    raise AcquisitionError(
      f"{consolidation.name} has no account mapped to "
      + ", ".join(missing)
      + "; add one to its chart for the acquisition entry."
    )

  held = list(
    session.execute(
      select(Entry).where(
        Entry.entity_id == consolidation_id,
        Entry.adjustment_kind == ACQUISITION,
        Entry.adjusts_entity_id == str(member.id),
        Entry.status.in_(("posted", "draft")),
      )
    ).scalars()
  )
  if any(e.status == "posted" for e in held):
    raise AcquisitionError(
      f"{name}'s acquisition is posted. Reverse that entry first to record it again."
    )
  # A draft never landed, so replacing it leaves nothing to supersede, as
  # propose-consolidation-entries replaces its own.
  for entry in held:
    session.delete(entry)
  session.flush()

  response = create_journal_entry(
    session,
    CreateJournalEntryRequest(
      posting_date=member.joined_on,
      memo=f"Acquisition of {name}",
      type="adjusting",
      status="draft",
      adjustment_kind=ACQUISITION,
      adjusts_entity_id=str(member.id),
      line_items=[
        JournalEntryLineItemInput(
          element_id=accounts[qname][0],
          debit_amount=max(amount, 0),
          credit_amount=max(-amount, 0),
          description=body.basis,
        )
        for qname, amount in sorted(wanted.items(), key=lambda kv: -kv[1])
      ],
    ),
    created_by,
    entity_id=consolidation_id,
  )
  entry = session.get(Entry, response.id)
  assert entry is not None
  entry.metadata_ = {
    **(entry.metadata_ or {}),
    "acquisition": {
      "consideration": body.consideration,
      "nci_fair_value": body.nci_fair_value,
      "equity_at_acquisition": equity,
      "allocations": [a.model_dump() for a in body.allocations],
      "basis": body.basis,
      "bargain_purchase_confirmed": body.confirm_bargain_purchase and goodwill < 0,
      "recorded_by": created_by,
    },
  }
  session.commit()
  return AcquisitionResponse(
    entity_id=str(member.id),
    entity_name=name,
    joined_on=member.joined_on,
    equity_at_acquisition=equity,
    parent_share=parent_part,
    consideration=body.consideration,
    excess=excess,
    goodwill=goodwill,
    noncontrolling_interest=nci,
    entry=response,
    replaced=bool(held),
  )


def _cents(amount: int) -> str:
  return f"{amount / 100:,.2f}"


# The parent's equity an ownership change adjusts, by the Style's close
# target: a corporation's paid-in capital, else the capital account itself.
_PARENT_EQUITY = {
  "rs-gaap:RetainedEarningsAccumulatedDeficit": "rs-gaap:AdditionalPaidInCapital",
}
_NCI_FLOWS = (
  "rs-gaap:ProceedsFromMinorityShareholders",
  "rs-gaap:PaymentsToMinorityShareholders",
)


class OwnershipChangeError(ValueError):
  """An ownership change that cannot be recorded as given."""


def record_ownership_change(
  session: Session, body: RecordOwnershipChangeRequest, created_by: str
) -> OwnershipChangeResponse:
  """Record the parent buying more of a member from its outside owners, from
  ``period``, as a draft in the consolidation entity that its close posts,
  and set the member's share. The investment elimination moves the share
  bought of the member's capital at its next month; this entry carries what
  that cannot: the price above that book value against the investment, the
  share bought of the member's results since it joined out of the outside
  owners' interest, and the difference in the parent's equity (ASC 810: no
  gain or loss). Its lines carry the cash flow rows that move the price from
  investing to financing. Replaces an earlier draft for the same month.

  Raises ``OwnershipChangeError`` for anything but a member in full buying
  more within the method, a month out of order, or an account the entry
  needs that the consolidation entity lacks."""
  member = resolve_entity(session, body.entity_id)
  name = member.name
  if (
    is_group_parent(session, str(member.id))
    or member.entity_role != "operating"
    or member.consolidation_method != "full"
  ):
    raise OwnershipChangeError(
      f"{name} is not a member consolidated in full; its outside owners hold no "
      "interest the parent buys within the method."
    )
  if body.ownership_pct < 50:
    raise OwnershipChangeError(
      f"Below 50% the parent no longer controls {name}: that is a change of "
      "method, not a change within it."
    )
  consolidation = find_consolidation_entity(session)
  if consolidation is None:
    raise OwnershipChangeError(
      "This group has no consolidation entity; create one with entity_role "
      "'consolidation' for the ownership change entry."
    )
  consolidation_id = str(consolidation.id)
  start, _end = month_bounds(body.period)
  if member.joined_on is not None and body.period < f"{member.joined_on:%Y-%m}":
    raise OwnershipChangeError(
      f"{name} joined the group in {member.joined_on:%Y-%m}; record the share "
      "it joined with on the entity, or its acquisition."
    )
  assert_period_not_closed(session, start, entity_id=consolidation_id)

  held = list(
    session.execute(
      select(Entry).where(
        Entry.entity_id == consolidation_id,
        Entry.adjustment_kind == OWNERSHIP_CHANGE,
        Entry.adjusts_entity_id == str(member.id),
        Entry.status.in_(("posted", "draft")),
      )
    ).scalars()
  )
  later = [
    e
    for e in held
    if (e.metadata_ or {}).get("ownership_change", {}).get("period", "") > body.period
  ]
  if later:
    raise OwnershipChangeError(
      f"{name} has an ownership change recorded after {body.period}; record "
      "changes in the order they happened."
    )
  same = [
    e
    for e in held
    if (e.metadata_ or {}).get("ownership_change", {}).get("period") == body.period
  ]
  if any(e.status == "posted" for e in same):
    raise OwnershipChangeError(
      f"{name}'s ownership change for {body.period} is posted. Reverse that "
      "entry first to record it again."
    )

  changes = [
    c
    for c in ownership_changes(session).get(str(member.id), [])
    if c.period != body.period
  ]
  current = float(member.ownership_pct) if member.ownership_pct is not None else 100.0
  if same:
    from_pct = float(same[0].metadata_["ownership_change"]["from_pct"])
  else:
    from_pct = pct_as_of(current, changes, body.period) or 100.0
  if body.ownership_pct <= from_pct:
    raise OwnershipChangeError(
      f"The parent held {from_pct:g}% of {name} before {body.period}; a share "
      f"of {body.ownership_pct:g}% is not more. Selling shares within the "
      "method is not supported yet."
    )

  bought = (body.ownership_pct - from_pct) / 100.0
  capital, earnings = capital_and_earnings(
    session, str(member.id), start - timedelta(days=1)
  )
  book_value = share_of(capital, bought)
  earnings_transferred = share_of(earnings, bought)
  excess = body.consideration - book_value
  wanted = {
    INVESTMENT_IN_SUBSIDIARIES: -excess,
    NONCONTROLLING_INTEREST: earnings_transferred,
  }
  style = load_entity_reporting_style(session, resolve_entity_id(session, None))
  close_target = load_close_target_concept(session, style)
  equity_concept = _PARENT_EQUITY.get(close_target, close_target)
  wanted[equity_concept] = wanted.get(equity_concept, 0) + excess - earnings_transferred
  wanted = {q: a for q, a in wanted.items() if a}

  mapping = find_entity_mapping(session, consolidation_id, BOOK_FRAMEWORK)
  accounts = (
    posting_accounts(
      session, consolidation_id, primary_concepts(session, [str(mapping.id)])
    )
    if mapping is not None
    else {}
  )
  missing = sorted(q for q in wanted if q not in accounts)
  if missing:
    raise OwnershipChangeError(
      f"{consolidation.name} has no account mapped to "
      + ", ".join(missing)
      + "; add one to its chart for the ownership change entry."
    )
  # The price is the group's payment to its outside owners: its investing
  # outflow on the parent's books, offset here, and a financing one.
  flows = {
    q: elimination_flow(q, "elimination_investment", a)
    if q == INVESTMENT_IN_SUBSIDIARIES
    else (_NCI_FLOWS[0] if a < 0 else _NCI_FLOWS[1])
    for q, a in wanted.items()
  }
  taggable = taggable_flow_qnames(
    session, load_entity_reporting_style(session, consolidation_id)
  )
  # As the engine tags: a Style with no rows read restricts nothing.
  if taggable and not all(f in taggable for f in flows.values()):
    flows = {}

  # A draft never landed, so replacing it leaves nothing to supersede.
  for entry in same:
    session.delete(entry)
  session.flush()

  response = create_journal_entry(
    session,
    CreateJournalEntryRequest(
      posting_date=start,
      memo=f"{name}: parent's share to {body.ownership_pct:g}%",
      type="adjusting",
      status="draft",
      adjustment_kind=OWNERSHIP_CHANGE,
      adjusts_entity_id=str(member.id),
      line_items=[
        JournalEntryLineItemInput(
          element_id=accounts[qname][0],
          debit_amount=max(amount, 0),
          credit_amount=max(-amount, 0),
          description=body.basis,
          flow_qname=flows.get(qname),
        )
        for qname, amount in sorted(wanted.items(), key=lambda kv: -kv[1])
      ],
    ),
    created_by,
    entity_id=consolidation_id,
  )
  entry = session.get(Entry, response.id)
  assert entry is not None
  entry.metadata_ = {
    **(entry.metadata_ or {}),
    "ownership_change": {
      "period": body.period,
      "from_pct": from_pct,
      "to_pct": body.ownership_pct,
      "consideration": body.consideration,
      "book_value": book_value,
      "earnings_transferred": earnings_transferred,
      "basis": body.basis,
      "recorded_by": created_by,
    },
  }
  locked = lock_by_id(
    session,
    Entity,
    member.id,
    f"{name} is being updated by another process. Retry in a moment.",
  )
  if locked is not None:
    locked.ownership_pct = body.ownership_pct
  session.commit()
  return OwnershipChangeResponse(
    entity_id=str(member.id),
    entity_name=name,
    period=body.period,
    from_pct=from_pct,
    to_pct=body.ownership_pct,
    consideration=body.consideration,
    book_value=book_value,
    earnings_transferred=earnings_transferred,
    equity_adjustment=excess - earnings_transferred,
    entry=response,
    replaced=bool(same),
  )
