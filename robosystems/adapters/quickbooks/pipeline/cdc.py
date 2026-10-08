"""QuickBooks CDC as a change detector: deletions and old edits reach the mirror.

The JournalReport stays the data source. CDC only says what changed since the
last sync, and two of its answers the window pull cannot give: a transaction
QuickBooks deleted, and an edit dated before the lookback window. The extract
turns the answer into a plan (the deletions, and the dates to pull again); the
load applies the deletions after the UPSERT, so a row edited and then deleted
within one window ends deleted.

A deletion never voids a posted entry by itself. An unposted event is voided;
a posted one becomes a reconciling item whose accepted payload carries no
entry, so restate is refused and catch-up reverses it — the same convention
every feed uses for a retracted line.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from robosystems.logger import logger
from robosystems.models.extensions.roboledger.event import Event
from robosystems.operations.extensions.loader import comparable_payload
from robosystems.operations.roboledger.fiscal_calendar.qb_writeback import (
  WRITEBACK_EVENT_SOURCES,
)

# The transaction types the JournalReport covers. Parties and accounts stay
# full snapshots, so they are not asked for.
TXN_ENTITIES = (
  "JournalEntry",
  "Invoice",
  "Bill",
  "Payment",
  "BillPayment",
  "SalesReceipt",
  "Purchase",
  "Deposit",
  "Transfer",
  "CreditMemo",
  "VendorCredit",
  "RefundReceipt",
)

# Past this many distinct back-dated days, one span from the earliest is
# cheaper than a pull per day: each day costs one JournalReport call and six
# header calls, so the cap bounds a sync at about seventy extra calls.
DISTINCT_DATE_CAP = 10

PLAN_FILE = "cdc_plan.json"

# QuickBooks keeps 30 days of changes. A watermark past this is refused here
# rather than sent, so the fallback never depends on how the rejection is worded.
CDC_MAX_AGE = timedelta(days=29)

# The next watermark is taken when CDC is asked, less this margin for clock
# skew: a change made while the sync runs falls inside the next one's window,
# and the SyncToken gate dedups the overlap.
WATERMARK_SKEW = timedelta(minutes=5)

# An event's external_id is ``{tx_type}_{Id}`` with the JournalReport's own
# label, normalised, while CDC names the entity. Observed on the sandbox
# sample company on 2026-10-08, one id per label resolved against every
# entity type: a credit-card credit is a Purchase the report calls "Credit
# Card Credit", a refund receipt is "Refund", and a credit-card bill payment
# keeps the un-normalised "Bill Payment (Credit Card)". Transfer and
# VendorCredit had no sample and keep the entity name. A label the map does
# not list is learned at apply time, so a miss is loud, not silent.
CDC_ENTITY_LABELS: dict[str, tuple[str, ...]] = {
  "JournalEntry": ("JournalEntry",),
  "Invoice": ("Invoice",),
  "Bill": ("Bill",),
  "Payment": ("Payment",),
  "BillPayment": ("BillPayment", "Bill Payment (Credit Card)"),
  "SalesReceipt": ("SalesReceipt",),
  "Purchase": (
    "Cash Expense",
    "Expense",
    "Check",
    "Credit Card Expense",
    "Credit Card Credit",
  ),
  "Deposit": ("Deposit",),
  "Transfer": ("Transfer",),
  "CreditMemo": ("CreditMemo",),
  "VendorCredit": ("VendorCredit",),
  "RefundReceipt": ("RefundReceipt", "Refund"),
}

UNPOSTED_STATUSES = ("captured", "classified")
POSTED_STATUSES = ("committed", "fulfilled")
VOID_REASON = "deleted_in_quickbooks"

# Every label the map knows, and the entity it belongs to.
_ENTITY_OF_LABEL: dict[str, str] = {
  label: entity for entity, labels in CDC_ENTITY_LABELS.items() for label in labels
}


@dataclass
class CdcPlan:
  """What the extract learned from CDC, for the load to finish."""

  checked: bool
  reason: str | None = None
  watermark: str | None = None
  # Where the next sync asks from; None leaves the watermark where it is.
  next_watermark: str | None = None
  changed: int = 0
  deletions: list[dict[str, Any]] = field(default_factory=list)
  # ``[start, end]`` ISO dates the JournalReport is pulled again for.
  extra_windows: list[list[str]] = field(default_factory=list)

  def to_dict(self) -> dict[str, Any]:
    return asdict(self)

  @classmethod
  def from_dict(cls, data: dict[str, Any]) -> CdcPlan:
    return cls(
      checked=bool(data.get("checked")),
      reason=data.get("reason"),
      watermark=data.get("watermark"),
      next_watermark=data.get("next_watermark"),
      changed=int(data.get("changed") or 0),
      deletions=list(data.get("deletions") or []),
      extra_windows=[list(w) for w in data.get("extra_windows") or []],
    )


def plan_cdc(
  client: Any,
  watermark: datetime | None,
  window_start: str,
  *,
  log: Any = None,
  now: datetime | None = None,
) -> CdcPlan:
  """Ask CDC what changed since ``watermark`` and plan the follow-up.

  No watermark (a first sync, or one reset) is today's path. A watermark past
  QuickBooks' 30 days falls through to today's path too, and says so:
  deletions in that gap can no longer be detected. Otherwise the deletions
  are listed, and every changed transaction dated before ``window_start``
  names a day to pull again. Every plan carries the watermark the next sync
  asks from, taken before CDC is asked.
  """
  now = now or datetime.now(UTC)
  next_watermark = (now - WATERMARK_SKEW).isoformat()
  if watermark is None:
    return CdcPlan(checked=False, reason="no_watermark", next_watermark=next_watermark)
  if watermark.tzinfo is None:
    watermark = watermark.replace(tzinfo=UTC)
  stamp = watermark.isoformat()
  too_old = now - watermark > CDC_MAX_AGE
  changed: dict[str, list[Any]] = {}
  if not too_old:
    changed, too_old = client.cdc(watermark, list(TXN_ENTITIES))
  if too_old:
    if log is not None:
      log.warning(
        f"The CDC watermark {stamp} is past QuickBooks' 30 days: deletions "
        "since then can no longer be detected"
      )
    return CdcPlan(
      checked=False,
      reason="watermark_too_old",
      watermark=stamp,
      next_watermark=next_watermark,
    )

  deletions: list[dict[str, Any]] = []
  old_dates: set[str] = set()
  count = 0
  for entity, rows in changed.items():
    for row in rows or []:
      count += 1
      if str(row.get("status") or "").lower() == "deleted":
        deletions.append(
          {
            "entity": entity,
            "id": str(row.get("Id") or ""),
            "last_updated": (row.get("MetaData") or {}).get("LastUpdatedTime"),
          }
        )
        continue
      txn_date = str(row.get("TxnDate") or "")[:10]
      if txn_date and txn_date < window_start:
        old_dates.add(txn_date)

  if len(old_dates) > DISTINCT_DATE_CAP:
    before_window = date.fromisoformat(window_start) - timedelta(days=1)
    windows = [[min(old_dates), before_window.isoformat()]]
  else:
    windows = [[day, day] for day in sorted(old_dates)]
  if log is not None:
    log.info(
      f"CDC since {stamp}: {count} changed, {len(deletions)} deleted, "
      f"{len(old_dates)} back-dated day(s) → {len(windows)} extra window(s)"
    )
  return CdcPlan(
    checked=True,
    watermark=stamp,
    next_watermark=next_watermark,
    changed=count,
    deletions=deletions,
    extra_windows=windows,
  )


def write_plan(extract_dir: Path, plan: CdcPlan) -> Path:
  extract_dir.mkdir(parents=True, exist_ok=True)
  path = extract_dir / PLAN_FILE
  path.write_text(json.dumps(plan.to_dict()))
  return path


def read_plan(extract_dir: Path) -> CdcPlan | None:
  path = extract_dir / PLAN_FILE
  if not path.exists():
    return None
  return CdcPlan.from_dict(json.loads(path.read_text()))


@dataclass
class CdcApplyResult:
  voided: int = 0
  flagged: int = 0
  skipped: int = 0
  already_applied: int = 0
  unmatched: int = 0
  # Entity → the report labels found for it that the map did not list.
  observed_labels: dict[str, list[str]] = field(default_factory=dict)


def apply_deletions(
  session: Session, deletions: list[dict[str, Any]], *, now: datetime | None = None
) -> CdcApplyResult:
  """Apply QuickBooks' deletions to the mirror, by the event's status.

  Unposted: voided, with the reason. Posted: a reconciling item whose
  accepted payload carries no entry, so restate is refused and catch-up
  reverses it. Already retracted, or already carrying this deletion:
  nothing, so a plan applied twice applies once. An id with no event was
  deleted before it ever synced, and is named in the log. An entry
  RoboLedger published to QuickBooks is a JournalEntry there, so only a
  JournalEntry deletion can be one of its recorded ids.

  QuickBooks ids are unique per entity type only, so a deletion is matched
  as ``(entity, id)``: through the label map first, then by the id's suffix
  for a label the map does not list, which is learned into
  ``observed_labels`` rather than dropped.
  """
  result = CdcApplyResult()
  if not deletions:
    return result
  stamp = (now or datetime.now(UTC)).isoformat()

  wanted: dict[tuple[str, str], str] = {}
  for deletion in deletions:
    entity, qb_id = str(deletion.get("entity") or ""), str(deletion.get("id") or "")
    if entity and qb_id:
      wanted[(entity, qb_id)] = qb_id
  matched: set[tuple[str, str]] = set()

  by_external_id: dict[str, tuple[str, str]] = {}
  for entity, qb_id in wanted:
    for label in CDC_ENTITY_LABELS.get(entity, (entity,)):
      by_external_id[f"{label}_{qb_id}"] = (entity, qb_id)
  for event in _mirrored(session, sorted(by_external_id)):
    key = by_external_id[str(event.external_id)]
    matched.add(key)
    _retract(event, [key[1]], result, stamp=stamp, published=False)

  # A label the map does not list: the id's suffix finds the event, and the
  # label it carries is learned, unless it belongs to another entity. Ids are
  # unique per type only, so a suffix can name another type's transaction:
  # it is flagged for review if posted, and never voided.
  for entity, qb_id in sorted(set(wanted) - matched):
    for event in _mirrored_by_suffix(session, qb_id):
      label = str(event.external_id)[: -(len(qb_id) + 1)]
      owner = _ENTITY_OF_LABEL.get(label)
      if owner is not None and owner != entity:
        continue
      matched.add((entity, qb_id))
      result.observed_labels.setdefault(entity, [])
      if label not in result.observed_labels[entity]:
        result.observed_labels[entity].append(label)
        logger.warning(
          "QuickBooks CDC label map miss: entity %s reports as %r in the "
          "JournalReport; add it to CDC_ENTITY_LABELS",
          entity,
          label,
        )
      _retract(event, [qb_id], result, stamp=stamp, published=False, may_void=False)

  deleted_journal_entries = {
    qb_id for (entity, qb_id) in wanted if entity == "JournalEntry"
  }
  if deleted_journal_entries:
    for event in _published(session):
      # Write-back records each entry as the external id its synced copy
      # carries (``JournalEntry_153``), so the round-trip matcher can find
      # it; a bare id is tolerated for older records.
      recorded = {
        part.rsplit("_", 1)[-1]
        for part in str((event.metadata_ or {}).get("qb_external_id") or "").split(",")
        if part
      }
      hit = sorted(recorded & deleted_journal_entries)
      if not hit:
        continue
      matched.update(("JournalEntry", qb_id) for qb_id in hit)
      _retract(event, hit, result, stamp=stamp, published=True)

  for entity, qb_id in sorted(set(wanted) - matched):
    result.unmatched += 1
    logger.warning(
      "QuickBooks deleted %s %s, which the mirror never held (deleted before "
      "it synced, or a label the map does not know)",
      entity,
      qb_id,
    )
  session.flush()
  logger.info(
    "QuickBooks deletions applied: %d voided, %d flagged, %d already applied, "
    "%d already retracted, %d never synced",
    result.voided,
    result.flagged,
    result.already_applied,
    result.skipped,
    result.unmatched,
  )
  return result


def _mirrored(session: Session, external_ids: list[str]) -> list[Event]:
  if not external_ids:
    return []
  return list(
    session.execute(
      select(Event)
      .where(Event.source == "quickbooks", Event.external_id.in_(external_ids))
      .order_by(Event.id)
      .with_for_update()
    )
    .scalars()
    .all()
  )


def _mirrored_by_suffix(session: Session, qb_id: str) -> list[Event]:
  escaped = qb_id.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
  return list(
    session.execute(
      select(Event)
      .where(
        Event.source == "quickbooks",
        Event.external_id.like(f"%\\_{escaped}", escape="\\"),
      )
      .order_by(Event.id)
      .with_for_update()
    )
    .scalars()
    .all()
  )


def _published(session: Session) -> list[Event]:
  """Entries RoboLedger wrote to QuickBooks, which record the ids it gave them."""
  return list(
    session.execute(
      select(Event)
      .where(
        Event.source.in_(WRITEBACK_EVENT_SOURCES),
        Event.metadata_["qb_external_id"].astext.isnot(None),
      )
      .order_by(Event.id)
      .with_for_update()
    )
    .scalars()
    .all()
  )


def _already_carries(event: Event, qb_ids: list[str]) -> bool:
  """Whether this deletion is already on the event: adopted into its live
  payload by a resolution, pending in its flagged payload, or the reason it
  was voided."""
  live = dict(event.metadata_ or {})
  if live.get("void_reason") == VOID_REASON:
    return True
  for store in (live, live.get("drift_payload") or {}):
    if store.get("source_removed") and set(qb_ids) <= {
      str(i) for i in store.get("source_removed_transaction_ids") or []
    }:
      return True
  return False


def _retract(
  event: Event,
  qb_ids: list[str],
  result: CdcApplyResult,
  *,
  stamp: str,
  published: bool,
  may_void: bool = True,
) -> None:
  if _already_carries(event, qb_ids):
    result.already_applied += 1
    return
  status = str(event.status)
  if status in UNPOSTED_STATUSES and not may_void:
    result.skipped += 1
    logger.warning(
      "QuickBooks deletion of id %s matched unposted event %s by suffix only; "
      "left in place",
      qb_ids[0],
      event.id,
    )
    return
  if status in UNPOSTED_STATUSES:
    event.status = "voided"
    event.metadata_ = {
      **(event.metadata_ or {}),
      "void_reason": VOID_REASON,
      "voided_at": stamp,
      "source_removed_transaction_ids": qb_ids,
    }
    result.voided += 1
    return
  if status not in POSTED_STATUSES:
    result.skipped += 1
    return
  accepted = comparable_payload(event.metadata_)
  accepted["entries"] = []
  accepted["source_removed"] = True
  accepted["source_removed_transaction_ids"] = qb_ids
  accepted["deleted_upstream"] = True
  if published:
    accepted["published_entry_deleted"] = True
  metadata = dict(event.metadata_ or {})
  metadata["drift_payload"] = accepted
  metadata["drift_detected_at"] = stamp
  event.metadata_ = metadata
  event.payload_drift = True
  result.flagged += 1
