"""Journal entry write commands: create, update and delete drafts, reverse posted.

Posted entries are immutable and corrected only by reversal, which posts a new
entry with flipped lines and marks the original ``reversed``. An entry created
without a ``transaction_id`` gets a synthetic ``journal_entry`` Transaction so
the graph keeps its Entity → Transaction → Entry path. Every write takes the
closed-period fence (``_guards.assert_period_not_closed``), and no row lands
dated before the entity's calendar (``_guards.assert_within_calendar``).
"""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import func, select, text
from sqlalchemy.orm import Session

from robosystems.logger import logger
from robosystems.models.api.extensions.journal_entries import (
  CreateJournalEntryRequest,
  DeleteJournalEntryRequest,
  JournalEntryLineDimensions,
  JournalEntryLineFlow,
  JournalEntryLineItemInput,
  JournalEntryLineItemResponse,
  JournalEntryResponse,
  ReverseJournalEntryRequest,
  UpdateJournalEntryRequest,
)
from robosystems.models.extensions.roboledger.entry import Entry
from robosystems.models.extensions.roboledger.event import Event
from robosystems.models.extensions.roboledger.line_item import LineItem
from robosystems.models.extensions.roboledger.transaction import Transaction
from robosystems.operations.locking import RowLockedError, lock_by_id
from robosystems.operations.roboledger.commands._guards import (
  assert_accounts_postable,
  assert_period_not_closed,
  assert_within_calendar,
)
from robosystems.operations.roboledger.commands.connections import SEVERABLE_SOURCES
from robosystems.operations.roboledger.entity_scope import ensure_entity_id
from robosystems.operations.roboledger.entry_status import SUPERSEDED_ENTRY_STATUS
from robosystems.operations.roboledger.fiscal_calendar.qb_writeback import (
  assert_local_posting_allowed,
)
from robosystems.operations.roboledger.line_dimensions import (
  line_dimension_ids,
  validate_line_dimensions,
  write_line_dimensions,
)


class JournalEntryNotFoundError(LookupError):
  def __init__(self, entry_id: str) -> None:
    super().__init__(f"Journal entry not found: {entry_id}")
    self.entry_id = entry_id


class JournalEntryNotDraftError(ValueError):
  def __init__(self, entry_id: str, status: str) -> None:
    super().__init__(
      f"Journal entry {entry_id} is {status!r}; only draft entries can be "
      f"updated or deleted. Fire `create-event-block(event_type="
      f"'journal_entry_reversed', metadata={{entry_id: ...}})` for "
      f"posted entries."
    )
    self.entry_id = entry_id
    self.status = status


class JournalEntryAlreadyReversedError(ValueError):
  """The entry already has a reversing entry.

  Reachable without a race: a schedule with ``auto_reverse`` creates the
  reversal up front and leaves the original ``posted``.
  """

  def __init__(self, entry_id: str, reversing_entry_id: str) -> None:
    super().__init__(
      f"Journal entry {entry_id} already has a reversing entry "
      f"({reversing_entry_id}); an entry is reversed at most once."
    )
    self.entry_id = entry_id
    self.reversing_entry_id = reversing_entry_id


class JournalEntryOwnedByEventError(ValueError):
  """A delete would leave a live (not voided or superseded) event with no
  ledger rows, which `execute` would still publish. Deleting one of several
  drafts is allowed."""

  def __init__(self, entry_id: str, event_id: str, event_status: str) -> None:
    super().__init__(
      f"Journal entry {entry_id} is the only ledger entry of event {event_id} "
      f"(status {event_status!r}). Void or supersede the event instead of "
      "deleting its draft; a retracted event's drafts can then be deleted."
    )
    self.entry_id = entry_id
    self.event_id = event_id
    self.event_status = event_status


_RETRACTED_EVENT_STATUSES = frozenset({"voided", "superseded"})


def _lock_owning_event(session: Session, entry: Entry) -> Event | None:
  """Lock the draft's event before the draft itself.

  Lock order is event, then entry, everywhere (`execute_event_block` and close
  use it), so a correction serializes with a publish instead of deadlocking or
  letting QuickBooks receive the pre-correction rows.
  """
  if entry.triggered_by_event_id is None:
    return None
  return lock_by_id(
    session,
    Event,
    entry.triggered_by_event_id,
    f"Event {entry.triggered_by_event_id} is being written by another "
    "process. Retry in a moment.",
  )


class JournalEntrySettlesItemsError(ValueError):
  """One draft of an event that settled reconciling items: the settlement
  counts the whole event, so the draft cannot go alone."""

  def __init__(self, entry_id: str, event_id: str, item_ids: list[str]) -> None:
    super().__init__(
      f"Journal entry {entry_id} is part of event {event_id}, which settled "
      f"reconciling item(s) {', '.join(item_ids)}. Void the event instead, "
      "which flags them again."
    )
    self.entry_id = entry_id
    self.event_id = event_id
    self.item_ids = item_ids


class JournalEntryNotPostedError(ValueError):
  def __init__(self, entry_id: str, status: str) -> None:
    super().__init__(
      f"Journal entry {entry_id} is {status!r}; only posted entries can be "
      f"reversed. Draft entries should be deleted instead."
    )
    self.entry_id = entry_id
    self.status = status


class UnbalancedJournalEntryError(ValueError):
  def __init__(self, total_debit: int, total_credit: int) -> None:
    super().__init__(
      f"Journal entry does not balance: "
      f"total_debit={total_debit} total_credit={total_credit} "
      f"(difference={total_debit - total_credit})"
    )
    self.total_debit = total_debit
    self.total_credit = total_credit


# ── Helpers ──────────────────────────────────────────────────────────────


_FLOW_TAG_KEY = "transaction_description_code"


def resolve_flow_element_id(session: Session, metadata: dict | None) -> str | None:
  """Resolve a line's flow tag (``transaction_description_code`` qname) to the
  Element id it names, or ``None`` when absent or unresolved. Warns, without
  failing, when the element has no ``activityType`` trait.
  """
  if not metadata:
    return None
  qname = metadata.get(_FLOW_TAG_KEY)
  if not qname:
    return None
  row = session.execute(
    text("SELECT id FROM elements WHERE qname = :qname LIMIT 1"),
    {"qname": qname},
  ).fetchone()
  if row is None:
    logger.warning(
      "flow tag %s did not resolve to an element; flow_element_id left NULL", qname
    )
    return None
  element_id = row[0]
  has_activity_type = session.execute(
    text(
      """
      SELECT 1 FROM element_traits et
      JOIN traits t ON t.id = et.trait_id
      WHERE et.element_id = :eid AND t.category = 'activityType'
      LIMIT 1
      """
    ),
    {"eid": element_id},
  ).fetchone()
  if has_activity_type is None:
    logger.warning(
      "flow element %s (%s) has no activityType trait — not a recognized flow "
      "concept (advisory; trait coverage backfilled in the rs-gaap content phase)",
      qname,
      element_id,
    )
  return element_id


def _split_flow_tag(metadata: dict | None) -> dict:
  """``metadata`` without the flow tag, which lives in ``flow_element_id``."""
  meta = dict(metadata or {})
  meta.pop(_FLOW_TAG_KEY, None)
  return meta


def propagate_flows(lines: list[tuple[str, str | None]]) -> list[str | None]:
  """The both-lines rule: when every line on one side of the entry (its
  debits, or its credits) carries the same flow, the untagged lines on the
  other side take it. The cash flow statement reads the cash line's flow and
  the roll-forwards read the account line's, so one tag has to reach both.
  A side with two flows, or a tagged and an untagged line, says nothing
  about the other, and the author's tags stand as written."""
  flows = [flow for _side, flow in lines]
  for side, other in (("debit", "credit"), ("credit", "debit")):
    own = {flow for s, flow in lines if s == side}
    if len(own) != 1 or None in own:
      continue
    (flow,) = own
    for index, (s, existing) in enumerate(lines):
      if s == other and existing is None:
        flows[index] = flow
  return flows


def resolve_line_flows(
  session: Session, normalized: list[dict], *, entity_id: str
) -> list[str | None]:
  """Each line's ``flow_element_id``. A first-class ``flow_qname`` must be a
  row of the entity's cash flow statement (``resolve_flows`` refuses one that
  is not, listing the rows); the legacy ``transaction_description_code``
  metadata key resolves by name only, as before. Then the both-lines rule,
  except on the consolidation entity."""
  strict = {li["flow_qname"] for li in normalized if li.get("flow_qname")}
  resolved: dict[str, str] = {}
  if strict:
    from robosystems.operations.event_block.python_handlers.bank_feed import (
      resolve_flows,
    )

    resolved = resolve_flows(session, strict, entity_id=entity_id)
  tagged = [
    (
      "debit" if li["debit_amount"] > 0 else "credit",
      resolved[li["flow_qname"]]
      if li.get("flow_qname")
      else resolve_flow_element_id(session, li.get("metadata")),
    )
    for li in normalized
  ]
  # The consolidation entity moves no cash: each tagged line counts on its
  # own, so a tag stays on the line it was written on.
  role = session.execute(
    text("SELECT entity_role FROM entities WHERE id = :id"), {"id": entity_id}
  ).scalar()
  if role == "consolidation":
    return [flow for _side, flow in tagged]
  return propagate_flows(tagged)


def _element_qnames(session: Session, element_ids: set[str]) -> dict[str, str]:
  """``{element_id: qname}`` for the given elements."""
  ids = sorted(element_ids)
  if not ids:
    return {}
  rows = session.execute(
    text("SELECT id, qname FROM elements WHERE id = ANY(:ids)"), {"ids": ids}
  ).all()
  return {str(element_id): str(qname) for element_id, qname in rows}


def _flow_qnames(session: Session, line_items: list[LineItem]) -> dict[str, str]:
  """``{element_id: qname}`` for the flows the lines carry."""
  return _element_qnames(
    session, {str(li.flow_element_id) for li in line_items if li.flow_element_id}
  )


_RETAGGABLE_STATUSES = ("draft", "posted", "shadowed")


def _apply_line_flows(
  session: Session, entry: Entry, line_flows: list[JournalEntryLineFlow]
) -> None:
  """Set the named lines' flows in place and let the lines in step with them
  follow.

  A flow moves no balance, so a posted entry's lines are re-tagged, closed
  month or not, and the change is recorded on the entry. After the named
  lines are set, when every line on a named line's side carries one flow (or
  none, after a clear), the lines on the other side the request did not name
  take it too when they were in step with the named lines: untagged, or
  carrying the flow those lines carried before. A line the author tagged
  differently keeps its tag. Both sides are decided from the state as named,
  so the result does not depend on which side is read first. A line named
  twice, a line that is not this entry's, or a flow that is not a row of the
  entity's cash flow statement refuses the request whole.
  """
  from robosystems.operations.event_block.python_handlers.bank_feed import (
    resolve_flows,
  )

  lines = _load_line_items(session, entry.id)
  by_id = {str(li.id): li for li in lines}
  ids = [f.line_id for f in line_flows]
  twice = sorted({line_id for line_id in ids if ids.count(line_id) > 1})
  if twice:
    raise ValueError(f"line_flows names {', '.join(twice)} more than once.")
  unknown = sorted(line_id for line_id in ids if line_id not in by_id)
  if unknown:
    raise ValueError(f"Journal entry {entry.id} has no line {', '.join(unknown)}.")
  resolved = resolve_flows(
    session,
    {f.flow_qname for f in line_flows if f.flow_qname},
    entity_id=str(entry.entity_id),
  )
  named = {
    f.line_id: (resolved[f.flow_qname] if f.flow_qname else None) for f in line_flows
  }

  def _side(li: LineItem) -> str:
    # A line has exactly one non-zero side (validate_and_normalize_lines).
    return "debit" if int(li.debit_amount) > 0 else "credit"

  before = {str(li.id): li.flow_element_id for li in lines}
  as_named = {**before, **named}
  after = dict(as_named)
  for side, other in (("debit", "credit"), ("credit", "debit")):
    named_here = [line_id for line_id in named if _side(by_id[line_id]) == side]
    if not named_here:
      continue
    own = {as_named[str(li.id)] for li in lines if _side(li) == side}
    if len(own) != 1:
      continue
    (flow_id,) = own
    previous = {before[line_id] for line_id in named_here}
    followed = next(iter(previous)) if len(previous) == 1 else None
    for li in lines:
      line_id = str(li.id)
      if _side(li) != other or line_id in named:
        continue
      if before[line_id] is None or (
        followed is not None and before[line_id] == followed
      ):
        after[line_id] = flow_id
  for li in lines:
    li.flow_element_id = after[str(li.id)]

  changed = sorted(line_id for line_id in after if after[line_id] != before[line_id])
  qnames = _element_qnames(
    session, {flow for flow in (*before.values(), *after.values()) if flow}
  )
  history = list((entry.metadata_ or {}).get("flow_retags") or [])
  history.append(
    {
      "at": datetime.now(UTC).isoformat(),
      "line_ids": sorted(named),
      "flows": sorted({f.flow_qname for f in line_flows if f.flow_qname}),
      "from": {line_id: qnames.get(str(before[line_id])) for line_id in changed},
    }
  )
  entry.metadata_ = {**(entry.metadata_ or {}), "flow_retags": history}
  session.flush()


def _apply_line_dimensions(
  session: Session, entry: Entry, line_dimensions: list[JournalEntryLineDimensions]
) -> None:
  """Set the named lines' management dimensions in place, recording on the
  entry what each changed line carried before. A dimension moves no balance,
  so a posted entry's lines are re-tagged, closed month or not. A line named
  twice or not this entry's refuses the request whole; a retired member may
  stay on a line already carrying it."""
  by_id = {str(li.id) for li in _load_line_items(session, entry.id)}
  ids = [d.line_id for d in line_dimensions]
  twice = sorted({line_id for line_id in ids if ids.count(line_id) > 1})
  if twice:
    raise ValueError(f"line_dimensions names {', '.join(twice)} more than once.")
  unknown = sorted(line_id for line_id in ids if line_id not in by_id)
  if unknown:
    raise ValueError(f"Journal entry {entry.id} has no line {', '.join(unknown)}.")
  before = line_dimension_ids(session, ids)
  checked = validate_line_dimensions(
    session,
    [d.dimension_ids for d in line_dimensions],
    kept=[before.get(line_id, []) for line_id in ids],
  )
  after = dict(zip(ids, checked, strict=True))
  changed = sorted(
    line_id
    for line_id in ids
    if sorted(after[line_id]) != sorted(before.get(line_id, []))
  )
  if not changed:
    return
  write_line_dimensions(session, {line_id: after[line_id] for line_id in changed})
  history = list((entry.metadata_ or {}).get("dimension_retags") or [])
  history.append(
    {
      "at": datetime.now(UTC).isoformat(),
      "line_ids": changed,
      "from": {line_id: before.get(line_id, []) for line_id in changed},
    }
  )
  entry.metadata_ = {**(entry.metadata_ or {}), "dimension_retags": history}
  session.flush()


def _retag_lines(
  session: Session,
  peek: Entry,
  line_flows: list[JournalEntryLineFlow] | None,
  line_dimensions: list[JournalEntryLineDimensions] | None = None,
) -> JournalEntryResponse:
  """The tag-only update: no draft rule and no reopen, since neither a flow
  nor a dimension moves a balance; the entry is still locked while its lines
  change. A posted entry's month is fenced and, where closed, its statements
  re-stamped: the tag changes what they present (`retag_restamp`)."""
  from robosystems.operations.roboledger.commands.retag_restamp import (
    fence_retag_months,
    restamp_retagged_months,
  )

  landed = peek.status != "draft"
  if landed:
    fence_retag_months(session, [peek.posting_date])
  event = _lock_owning_event(session, peek)
  entry = lock_by_id(
    session,
    Entry,
    peek.id,
    f"Journal entry {peek.id} is being written by another process. Retry in a moment.",
  )
  if entry is None:
    raise JournalEntryNotFoundError(peek.id)
  if entry.status not in _RETAGGABLE_STATUSES:
    raise JournalEntryNotDraftError(entry.id, entry.status)
  if (entry.status != "draft") != landed:
    # A close posted it between the peek and the lock; its month went
    # unfenced.
    raise RowLockedError(
      f"Journal entry {entry.id} was posted by another process. Retry."
    )
  if line_flows is not None:
    _apply_line_flows(session, entry, line_flows)
  if line_dimensions is not None:
    _apply_line_dimensions(session, entry, line_dimensions)
  line_items = _load_line_items(session, entry.id)
  if line_flows is not None:
    _mirror_flows_onto_bank_event(session, event, line_items)
  if line_dimensions is not None:
    _mirror_dimensions_onto_bank_event(session, event, line_items)
  if landed:
    session.flush()
    restamp_retagged_months(
      session,
      [entry.posting_date],
      entity_id=str(entry.entity_id),
      actor_id=str(entry.created_by or "retag"),
    )
  return _respond(session, entry, line_items)


def _mirror_flows_onto_bank_event(
  session: Session, event: Event | None, line_items: list[LineItem]
) -> None:
  """A bank line's flows are held on its event too: the inbox shows them, and
  a restatement re-posts from them. A re-tag of its entry is written back
  there, so neither puts the old flow back."""
  from robosystems.operations.event_block.python_handlers.bank_feed import (
    BANK_FEED_HANDLERS,
  )

  if event is None or event.event_type not in BANK_FEED_HANDLERS:
    return
  metadata = dict(event.metadata_ or {})
  if not any(
    metadata.get(key)
    for key in ("classified_element_id", "accept_suggestion", "classified_allocations")
  ):
    return
  contra = [li for li in line_items if li.element_id != event.resource_element_id]
  qnames = _flow_qnames(session, contra)
  flow_of = {str(li.element_id): qnames.get(str(li.flow_element_id)) for li in contra}
  if metadata.get("classified_allocations"):
    metadata["classified_allocations"] = [
      {**allocation, "flow_qname": flow_of.get(str(allocation.get("element_id")))}
      for allocation in metadata["classified_allocations"]
    ]
  else:
    flow = flow_of.get(str(contra[0].element_id)) if contra else None
    if flow:
      metadata["classified_flow_qname"] = flow
    else:
      metadata.pop("classified_flow_qname", None)
  event.metadata_ = metadata
  session.flush()


def _mirror_dimensions_onto_bank_event(
  session: Session, event: Event | None, line_items: list[LineItem]
) -> None:
  """A bank line's account-side members are held on its event too, so a
  restatement re-posting from the event keeps a re-tag made here."""
  from robosystems.operations.event_block.python_handlers.bank_feed import (
    BANK_FEED_HANDLERS,
  )
  from robosystems.operations.roboledger.reads.event_block import (
    _load_dimension_ids,
  )

  if event is None or event.event_type not in BANK_FEED_HANDLERS:
    return
  metadata = dict(event.metadata_ or {})
  if not any(
    metadata.get(key)
    for key in ("classified_element_id", "accept_suggestion", "classified_allocations")
  ):
    return
  contra = [li for li in line_items if li.element_id != event.resource_element_id]
  # The event's own members reach the lines anyway; holding them on the
  # classification too would pin them there past a change to the event's.
  inherited = set(_load_dimension_ids(session, str(event.id)))
  tags = line_dimension_ids(session, (li.id for li in contra))
  of_account = {
    str(li.element_id): [i for i in tags.get(str(li.id), []) if i not in inherited]
    for li in contra
  }
  if metadata.get("classified_allocations"):
    metadata["classified_allocations"] = [
      {**allocation, "dimension_ids": of_account.get(str(allocation.get("element_id")))}
      for allocation in metadata["classified_allocations"]
    ]
  else:
    metadata["classified_dimension_ids"] = (
      of_account.get(str(contra[0].element_id)) if contra else None
    ) or None
  event.metadata_ = metadata
  session.flush()


def validate_and_normalize_lines(
  lines: list[JournalEntryLineItemInput],
) -> tuple[list[dict], int, int]:
  """Validate lines (element required, exactly one positive side, balanced)
  and return ``(normalized, total_debit, total_credit)``."""
  if not lines:
    raise ValueError("Journal entry requires at least one line item")

  normalized: list[dict] = []
  total_debit = 0
  total_credit = 0
  for i, li in enumerate(lines):
    if not li.element_id:
      raise ValueError(f"Line item {i}: missing element_id")
    debit = int(li.debit_amount or 0)
    credit = int(li.credit_amount or 0)
    if debit < 0 or credit < 0:
      raise ValueError(f"Line item {i}: amounts must be non-negative")
    if debit == 0 and credit == 0:
      raise ValueError(f"Line item {i}: must have a non-zero debit or credit amount")
    if debit > 0 and credit > 0:
      raise ValueError(f"Line item {i}: cannot have both debit and credit amounts")
    total_debit += debit
    total_credit += credit
    normalized.append(
      {
        "element_id": li.element_id,
        "debit_amount": debit,
        "credit_amount": credit,
        "description": li.description,
        "flow_qname": li.flow_qname,
        "dimension_ids": list(li.dimension_ids or []),
        "metadata": li.metadata or {},
      }
    )

  if total_debit != total_credit:
    raise UnbalancedJournalEntryError(total_debit, total_credit)

  return normalized, total_debit, total_credit


def _entry_to_response(
  entry: Entry,
  line_items: list[LineItem],
  flow_qnames: dict[str, str] | None = None,
  dimension_ids: dict[str, list[str]] | None = None,
) -> JournalEntryResponse:
  flows = flow_qnames or {}
  dimensions = dimension_ids or {}
  line_item_responses = [
    JournalEntryLineItemResponse(
      id=li.id,
      element_id=li.element_id,
      debit_amount=int(li.debit_amount),
      credit_amount=int(li.credit_amount),
      description=li.description,
      line_order=int(li.line_order),
      flow_qname=flows.get(str(li.flow_element_id)) if li.flow_element_id else None,
      dimension_ids=dimensions.get(str(li.id), []),
    )
    for li in sorted(line_items, key=lambda li: li.line_order)
  ]
  total_debit = sum(int(li.debit_amount) for li in line_items)
  total_credit = sum(int(li.credit_amount) for li in line_items)
  return JournalEntryResponse(
    id=entry.id,
    transaction_id=entry.transaction_id,
    type=entry.type,
    status=entry.status,
    posting_date=entry.posting_date,
    memo=entry.memo,
    provenance=entry.provenance,
    reversal_of=entry.reversal_of,
    posted_at=entry.posted_at,
    line_items=line_item_responses,
    total_debit=total_debit,
    total_credit=total_credit,
    adjustment_kind=entry.adjustment_kind,
    adjusts_entity_id=entry.adjusts_entity_id,
  )


def _respond(
  session: Session, entry: Entry, line_items: list[LineItem]
) -> JournalEntryResponse:
  return _entry_to_response(
    entry,
    line_items,
    _flow_qnames(session, line_items),
    line_dimension_ids(session, (li.id for li in line_items)),
  )


def _insert_lines(
  session: Session, entry_id: str, normalized: list[dict], flow_ids: list[str | None]
) -> None:
  """Write an entry's lines in order, with the members each carries."""
  lines = [
    LineItem(
      entry_id=entry_id,
      element_id=li["element_id"],
      flow_element_id=flow_id,
      debit_amount=li["debit_amount"],
      credit_amount=li["credit_amount"],
      description=li["description"],
      line_order=order,
      metadata_=_split_flow_tag(li.get("metadata")),
    )
    for order, (li, flow_id) in enumerate(zip(normalized, flow_ids, strict=True), 1)
  ]
  for line in lines:
    session.add(line)
  session.flush()
  write_line_dimensions(
    session,
    {
      str(line.id): li["dimension_ids"]
      for line, li in zip(lines, normalized, strict=True)
      if li.get("dimension_ids")
    },
  )


def _load_line_items(session: Session, entry_id: str) -> list[LineItem]:
  return list(
    session.execute(select(LineItem).where(LineItem.entry_id == entry_id))
    .scalars()
    .all()
  )


def _load_entry_or_404(session: Session, entry_id: str) -> Entry:
  entry = session.execute(
    select(Entry).where(Entry.id == entry_id)
  ).scalar_one_or_none()
  if entry is None:
    raise JournalEntryNotFoundError(entry_id)
  return entry


def _raise_if_posting_date_moved(entry: Entry, peeked_date) -> None:
  """Refuse instead of re-fencing: lock order is fence, then row, and fencing
  the new period while holding the row could deadlock with close. The caller
  retries."""
  if entry.posting_date != peeked_date:
    raise RowLockedError(
      f"Journal entry {entry.id} was moved to another period by another "
      "process. Retry in a moment."
    )


# ── Create ───────────────────────────────────────────────────────────────


# Sources the platform itself emits. `source` is otherwise an open vocabulary
# of connection providers, so anything not listed here is a sync.
_PLATFORM_SOURCE_PROVENANCE = {
  "manual": "manual_entry",
  "native": "manual_entry",
  "system": "system_computed",
  "schedule": "schedule_derived",
}


class ConsolidationAdjustmentError(ValueError):
  """An adjustment kind on an entry that is not a consolidating one."""


def assert_adjustment_allowed(
  session: Session, entity_id: str, body: CreateJournalEntryRequest
) -> None:
  """An ``adjustment_kind`` belongs only on the consolidation entity's
  books, and names the entity it adjusts (any of the group's own entities
  but the consolidation entity) unless the kind is ``manual``."""
  if body.adjustment_kind is None:
    if body.adjusts_entity_id is not None:
      raise ConsolidationAdjustmentError(
        "adjusts_entity_id is set without an adjustment_kind."
      )
    return
  role = session.execute(
    text("SELECT entity_role FROM entities WHERE id = :id"), {"id": entity_id}
  ).scalar()
  if role != "consolidation":
    raise ConsolidationAdjustmentError(
      "An adjustment_kind is only for the group's consolidation entity; "
      "this entry is in an operating entity's books."
    )
  if body.adjusts_entity_id is None:
    if body.adjustment_kind != "manual":
      raise ConsolidationAdjustmentError(
        f"A {body.adjustment_kind} entry names the entity it adjusts "
        "(adjusts_entity_id)."
      )
    return
  adjusted = session.execute(
    text("SELECT entity_role FROM entities WHERE id = :id AND source <> 'linked'"),
    {"id": body.adjusts_entity_id},
  ).scalar()
  if adjusted is None or adjusted == "consolidation":
    raise ConsolidationAdjustmentError(
      f"adjusts_entity_id {body.adjusts_entity_id!r} is not one of the "
      "group's own entities."
    )


def provenance_for_source(source: str | None) -> str:
  """Map a Transaction/Event source onto `Entry.provenance`."""
  if not source:
    return "manual_entry"
  return _PLATFORM_SOURCE_PROVENANCE.get(source, "source_sync")


def create_journal_entry(
  session: Session,
  body: CreateJournalEntryRequest,
  created_by: str,
  *,
  entity_id: str | None = None,
  mirrors_source: bool = False,
) -> JournalEntryResponse:
  """Create a journal entry with balanced line items in one entity's books.

  ``entity_id`` defaults to the group parent. ``status='posted'`` (historical
  import) posts immediately, bypassing the draft-review-close workflow. Either
  status is refused in a closed period.

  On books a shadow close keeps, a posted entry is refused unless it keeps
  the ledger equal to QuickBooks: synced history (a synced ``source``), or
  ``mirrors_source``, a catch-up that follows an upstream edit.

  Raises:
    `ClosedPeriodError`, `UnbalancedJournalEntryError`, `ValueError` for a
      malformed line.
    `InactiveAccountError` if a line names a retired account, except for a
      synced ledger's replayed history (a synced `source` with `status='posted'`).
    `EntityNotInGraphError` if the named entity is not this graph's.
    `ShadowLedgerPostingError` for a posted entry authored on shadow books.
  """
  entity_id = ensure_entity_id(session, entity_id)
  assert_period_not_closed(session, body.posting_date, entity_id=entity_id)
  assert_within_calendar(session, body.posting_date, entity_id=entity_id)
  assert_adjustment_allowed(session, entity_id, body)
  synced = (body.source or "").lower() in SEVERABLE_SOURCES
  if body.status == "posted" and not synced and not mirrors_source:
    assert_local_posting_allowed(session, entity_id, "Posting this journal entry")

  normalized, total_debit, _total_credit = validate_and_normalize_lines(body.line_items)
  wanted = [li["dimension_ids"] for li in normalized]
  for li, ids in zip(
    normalized,
    # Replayed history keeps what the source tagged, retired here or not,
    # as it keeps a retired account.
    validate_line_dimensions(
      session, wanted, kept=wanted if synced and body.status == "posted" else None
    ),
    strict=True,
  ):
    li["dimension_ids"] = ids
  # Only posted (replayed) history carries the source into the exemption; a
  # draft is authored whatever source it names.
  assert_accounts_postable(
    session,
    (li["element_id"] for li in normalized),
    source=body.source if body.status == "posted" else None,
    entity_id=entity_id,
  )

  status = body.status
  now = datetime.now(UTC) if status == "posted" else None

  transaction_id = body.transaction_id
  if not transaction_id:
    txn = Transaction(
      entity_id=entity_id,
      type=body.transaction_type,
      amount=total_debit,
      date=body.posting_date,
      description=body.memo,
      source=body.source or "native",
      connection_id=body.connection_id,
      status="posted" if status == "posted" else "pending",
      posted_at=now,
      created_by=created_by,
    )
    session.add(txn)
    session.flush()
    transaction_id = txn.id

  entry = Entry(
    entity_id=entity_id,
    transaction_id=transaction_id,
    type=body.type,
    status=status,
    posting_date=body.posting_date,
    memo=body.memo,
    provenance=provenance_for_source(body.source),
    posted_at=now,
    created_by=created_by,
    adjustment_kind=body.adjustment_kind,
    adjusts_entity_id=body.adjusts_entity_id,
  )
  session.add(entry)
  session.flush()

  flow_ids = resolve_line_flows(session, normalized, entity_id=entity_id)
  _insert_lines(session, entry.id, normalized, flow_ids)

  return _respond(session, entry, _load_line_items(session, entry.id))


# ── Update ───────────────────────────────────────────────────────────────


def update_journal_entry(
  session: Session, body: UpdateJournalEntryRequest
) -> JournalEntryResponse:
  """Update a draft journal entry; omitted fields are unchanged, and
  ``line_items`` replaces the whole set. ``line_flows`` and
  ``line_dimensions`` re-tag lines in place; alone, either is allowed on a
  posted entry in a closed month too, since neither moves a balance.

  Raises:
    `JournalEntryNotFoundError`, `JournalEntryNotDraftError`,
    `UnbalancedJournalEntryError`.
    `ClosedPeriodError` if the existing or new `posting_date` is closed.
    `RowLockedError` if another writer holds the entry or its date moved.
    `ValueError` for `line_flows` or `line_dimensions` beside `line_items`,
      naming a line that is not the entry's, or a dimension member that is
      unknown, retired or a second on its axis; `HandlerMetadataValidationError` for a flow
      that is not a row of the entity's cash flow statement.
  """
  # Peek, fence, then lock and re-check status. Keep the peeked date as a
  # value: after `lock_by_id` refreshes the same identity, `peek.posting_date`
  # would report the new date.
  peek = _load_entry_or_404(session, body.entry_id)
  updates = body.model_dump(exclude_unset=True)
  updates.pop("entry_id", None)
  line_flows = body.line_flows if updates.pop("line_flows", None) is not None else None
  line_dimensions = (
    body.line_dimensions if updates.pop("line_dimensions", None) is not None else None
  )
  if line_flows is not None and "line_items" in updates:
    raise ValueError(
      "line_flows names lines by id, and line_items replaces the lines; "
      "set flow_qname on the new lines instead."
    )
  if line_dimensions is not None and "line_items" in updates:
    raise ValueError(
      "line_dimensions names lines by id, and line_items replaces the lines; "
      "set dimension_ids on the new lines instead."
    )
  if (line_flows is not None or line_dimensions is not None) and not updates:
    return _retag_lines(session, peek, line_flows, line_dimensions)
  peeked_date = peek.posting_date
  dates = [peeked_date]
  if body.posting_date is not None:
    dates.append(body.posting_date)
  assert_period_not_closed(session, *dates, entity_id=peek.entity_id)
  # The date the draft ends on, not the one it leaves: a draft stranded
  # before the calendar can be moved into it.
  assert_within_calendar(
    session, body.posting_date or peeked_date, entity_id=peek.entity_id
  )

  _lock_owning_event(session, peek)
  entry = lock_by_id(
    session,
    Entry,
    body.entry_id,
    f"Journal entry {body.entry_id} is being written by another process. "
    "Retry in a moment.",
  )
  if entry is None:
    raise JournalEntryNotFoundError(body.entry_id)
  _raise_if_posting_date_moved(entry, peeked_date)
  if entry.status != "draft":
    raise JournalEntryNotDraftError(entry.id, entry.status)

  replacement_lines = updates.pop("line_items", None)

  for key, value in updates.items():
    setattr(entry, key, value)

  if replacement_lines is not None:
    # Validate before deleting, so a bad batch never clobbers the old lines.
    new_line_inputs = [
      JournalEntryLineItemInput(**li) if isinstance(li, dict) else li
      for li in replacement_lines
    ]
    normalized, _dr, _cr = validate_and_normalize_lines(new_line_inputs)
    # A draft edit is authored, whatever the entry's provenance: retired
    # accounts are closed to it.
    assert_accounts_postable(
      session, (li["element_id"] for li in normalized), entity_id=entry.entity_id
    )
    # A retired member an old line carried may stay on the replacement at
    # its position.
    old_lines = sorted(
      _load_line_items(session, entry.id), key=lambda li: li.line_order
    )
    held = line_dimension_ids(session, (li.id for li in old_lines))
    for li, ids in zip(
      normalized,
      validate_line_dimensions(
        session,
        [li["dimension_ids"] for li in normalized],
        kept=[held.get(str(old.id), []) for old in old_lines],
      ),
      strict=True,
    ):
      li["dimension_ids"] = ids

    session.query(LineItem).filter(LineItem.entry_id == entry.id).delete(
      synchronize_session=False
    )
    session.flush()

    flow_ids = resolve_line_flows(session, normalized, entity_id=str(entry.entity_id))
    _insert_lines(session, entry.id, normalized, flow_ids)

  if line_flows is not None:
    _apply_line_flows(session, entry, line_flows)
  if line_dimensions is not None:
    _apply_line_dimensions(session, entry, line_dimensions)

  session.flush()
  return _respond(session, entry, _load_line_items(session, entry.id))


# ── Delete ───────────────────────────────────────────────────────────────


def delete_journal_entry(session: Session, body: DeleteJournalEntryRequest) -> dict:
  """Hard delete a draft journal entry (line items cascade).

  Raises:
    `JournalEntryNotFoundError`, `JournalEntryNotDraftError`,
    `ClosedPeriodError`, `JournalEntryOwnedByEventError`.
    `JournalEntrySettlesItemsError` if its event settled reconciling items.
    `RowLockedError` if another writer holds the entry or its date moved.
  """
  peek = _load_entry_or_404(session, body.entry_id)
  peeked_date = peek.posting_date
  assert_period_not_closed(session, peeked_date, entity_id=peek.entity_id)

  # Under the event lock the sibling count below cannot change.
  owner = _lock_owning_event(session, peek)
  entry = lock_by_id(
    session,
    Entry,
    body.entry_id,
    f"Journal entry {body.entry_id} is being written by another process. "
    "Retry in a moment.",
  )
  if entry is None:
    raise JournalEntryNotFoundError(body.entry_id)
  _raise_if_posting_date_moved(entry, peeked_date)
  if entry.status != "draft":
    raise JournalEntryNotDraftError(entry.id, entry.status)

  if owner is not None and owner.status not in _RETRACTED_EVENT_STATUSES:
    siblings = session.execute(
      select(func.count())
      .select_from(Entry)
      .where(
        Entry.triggered_by_event_id == owner.id, Entry.status != SUPERSEDED_ENTRY_STATUS
      )
    ).scalar_one()
    if siblings <= 1:
      raise JournalEntryOwnedByEventError(
        str(entry.id), str(owner.id), str(owner.status)
      )
    from robosystems.operations.roboledger.commands.reconciling_items import (
      items_settled_by,
    )

    settled = items_settled_by(session, owner)
    if settled:
      raise JournalEntrySettlesItemsError(str(entry.id), str(owner.id), settled)

  session.delete(entry)
  session.flush()
  return {"deleted": True}


# ── Reverse ───────────────────────────────────────────────────────────────


def reverse_journal_entry(
  session: Session,
  body: ReverseJournalEntryRequest,
  created_by: str,
) -> JournalEntryResponse:
  """Post a reversing entry with flipped lines and mark the original ``reversed``.

  ``posting_date`` defaults to today.

  Raises:
    `JournalEntryNotFoundError`, `JournalEntryNotPostedError`,
    `JournalEntryAlreadyReversedError`.
    `ClosedPeriodError` if the original's or the reversal's date is closed.
    `ShadowLedgerPostingError` on books a shadow close keeps.
    `SettlementNotReopenableError` if it settled a QuickBooks round trip that
    has moved on since.
  """
  # Fence first, then the row: close takes the fence and then updates entries.
  peek = session.get(Entry, body.entry_id)
  if peek is None:
    raise JournalEntryNotFoundError(body.entry_id)
  assert_local_posting_allowed(
    session, str(peek.entity_id), "Reversing this journal entry"
  )
  posting_date = body.posting_date or datetime.now(UTC).date()
  assert_period_not_closed(
    session, peek.posting_date, posting_date, entity_id=peek.entity_id
  )
  assert_within_calendar(session, posting_date, entity_id=peek.entity_id)

  # Locked so two concurrent reversals cannot both see 'posted' and reverse
  # twice (balanced, so the trial balance would not catch it).
  # `uq_entries_one_reversal_per_original` backs this at the database.
  original = lock_by_id(
    session,
    Entry,
    body.entry_id,
    f"Journal entry {body.entry_id} is being written by another process. "
    "Retry in a moment.",
  )
  if original is None:
    raise JournalEntryNotFoundError(body.entry_id)
  if original.status != "posted":
    raise JournalEntryNotPostedError(original.id, original.status)
  if original.type == "opening":
    # Reversed, the books' starting balances would post as a month's activity.
    raise ValueError(
      f"Journal entry {original.id} is the opening balance; record it again "
      "with record-opening-balance before the first close, or correct it with "
      "an adjusting entry after."
    )

  existing_reversal = session.execute(
    select(Entry.id).where(Entry.reversal_of == original.id)
  ).scalar_one_or_none()
  if existing_reversal is not None:
    raise JournalEntryAlreadyReversedError(str(original.id), str(existing_reversal))

  original_lines = _load_line_items(session, original.id)
  if not original_lines:
    raise ValueError(f"Journal entry {original.id} has no line items to reverse")
  memo = body.memo or f"Reversal of journal entry {original.id}"
  now = datetime.now(UTC)

  reversing_entry = Entry(
    entity_id=original.entity_id,
    transaction_id=original.transaction_id,
    type="reversing",
    status="posted",
    posting_date=posting_date,
    memo=memo,
    # Not inherited: reversing even a synced entry is a decision made here.
    provenance="manual_entry",
    reversal_of=original.id,
    # A consolidating entry's reversal undoes what the consolidation entity
    # holds of that kind and member.
    adjustment_kind=original.adjustment_kind,
    adjusts_entity_id=original.adjusts_entity_id,
    posted_at=now,
    created_by=created_by,
  )
  session.add(reversing_entry)
  session.flush()

  reversing_lines = [
    LineItem(
      entry_id=reversing_entry.id,
      element_id=li.element_id,
      # Rollforwards need the offsetting flow on the reversal period.
      flow_element_id=li.flow_element_id,
      debit_amount=int(li.credit_amount),
      credit_amount=int(li.debit_amount),
      metadata_=dict(li.metadata_ or {}),
      description=(
        f"Reversal of line {li.line_order}"
        + (f": {li.description}" if li.description else "")
      ),
      line_order=order,
    )
    for order, li in enumerate(sorted(original_lines, key=lambda x: x.line_order), 1)
  ]
  for line in reversing_lines:
    session.add(line)
  session.flush()
  # The reversal carries the original's members, so each slice nets to zero.
  held = line_dimension_ids(session, (li.id for li in original_lines))
  write_line_dimensions(
    session,
    {
      str(new.id): held[str(old.id)]
      for new, old in zip(
        reversing_lines,
        sorted(original_lines, key=lambda x: x.line_order),
        strict=True,
      )
      if str(old.id) in held
    },
  )

  original.status = "reversed"
  session.flush()

  if original.triggered_by_event_id is not None:
    # An alignment that settled reconciling items no longer books them.
    from robosystems.operations.roboledger.commands.reconciling_items import (
      reopen_items_settled_by,
    )

    reopen_items_settled_by(
      session, str(original.triggered_by_event_id), created_by, reason="reversed"
    )

  return _respond(
    session, reversing_entry, _load_line_items(session, reversing_entry.id)
  )
