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
# cheaper than a pull per day.
DISTINCT_DATE_CAP = 10

PLAN_FILE = "cdc_plan.json"

# An event's external_id is ``{tx_type}_{Id}`` with the JournalReport's own
# label, normalised, while CDC names the entity. Seeded from the labels the
# pipeline normalises and the Purchase payment-type map; the long tail
# (Deposit, Transfer, CreditMemo, VendorCredit, RefundReceipt) is the report's
# label as observed so far and is confirmed on a sandbox pull that holds
# every type.
CDC_ENTITY_LABELS: dict[str, tuple[str, ...]] = {
  "JournalEntry": ("JournalEntry",),
  "Invoice": ("Invoice",),
  "Bill": ("Bill",),
  "Payment": ("Payment",),
  "BillPayment": ("BillPayment",),
  "SalesReceipt": ("SalesReceipt",),
  "Purchase": ("Cash Expense", "Expense", "Check", "Credit Card Expense"),
  "Deposit": ("Deposit",),
  "Transfer": ("Transfer",),
  "CreditMemo": ("CreditMemo",),
  "VendorCredit": ("VendorCredit",),
  "RefundReceipt": ("RefundReceipt",),
}

UNPOSTED_STATUSES = ("captured", "classified")
POSTED_STATUSES = ("committed", "fulfilled")


@dataclass
class CdcPlan:
  """What the extract learned from CDC, for the load to finish."""

  checked: bool
  reason: str | None = None
  watermark: str | None = None
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
      changed=int(data.get("changed") or 0),
      deletions=list(data.get("deletions") or []),
      extra_windows=[list(w) for w in data.get("extra_windows") or []],
    )


def plan_cdc(
  client: Any, watermark: datetime | None, window_start: str, *, log: Any = None
) -> CdcPlan:
  """Ask CDC what changed since ``watermark`` and plan the follow-up.

  No watermark (a first sync, or one reset) is today's path. A watermark
  QuickBooks rejects (older than ~30 days) falls through to today's path
  too, and says so: deletions in the gap are not checked until a full
  rebuild. Otherwise the deletions are listed, and every changed
  transaction dated before ``window_start`` names a day to pull again.
  """
  if watermark is None:
    return CdcPlan(checked=False, reason="no_watermark")
  changed, too_old = client.cdc(watermark, list(TXN_ENTITIES))
  stamp = watermark.isoformat()
  if too_old:
    if log is not None:
      log.warning(
        f"QuickBooks rejected the CDC watermark {stamp}: deletions since then "
        "were not checked; a full rebuild catches them"
      )
    return CdcPlan(checked=False, reason="watermark_too_old", watermark=stamp)

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
  unmatched: int = 0


def apply_deletions(
  session: Session, deletions: list[dict[str, Any]], *, now: datetime | None = None
) -> CdcApplyResult:
  """Apply QuickBooks' deletions to the mirror, by the event's status.

  Unposted: voided, with the reason. Posted: a reconciling item whose
  accepted payload carries no entry, so restate is refused and catch-up
  reverses it. Already retracted: nothing. An id with no event was deleted
  before it ever synced. An entry RoboLedger published to QuickBooks is
  matched by the QuickBooks ids it recorded, and flagged the same way.
  """
  result = CdcApplyResult()
  if not deletions:
    return result
  stamp = (now or datetime.now(UTC)).isoformat()

  by_external_id: dict[str, str] = {}
  for deletion in deletions:
    qb_id = str(deletion.get("id") or "")
    if not qb_id:
      continue
    labels = CDC_ENTITY_LABELS.get(str(deletion.get("entity")), ())
    for label in labels or (str(deletion.get("entity")),):
      by_external_id[f"{label}_{qb_id}"] = qb_id
  deleted_ids = set(by_external_id.values())
  matched: set[str] = set()

  mirrored = (
    session.execute(
      select(Event)
      .where(
        Event.source == "quickbooks",
        Event.external_id.in_(sorted(by_external_id)),
      )
      .order_by(Event.id)
      .with_for_update()
    )
    .scalars()
    .all()
  )
  for event in mirrored:
    qb_id = by_external_id[str(event.external_id)]
    matched.add(qb_id)
    _retract(event, [qb_id], result, stamp=stamp, published=False)

  # Entries RoboLedger wrote to QuickBooks record the ids QuickBooks gave them.
  published = (
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
  for event in published:
    recorded = {
      part
      for part in str((event.metadata_ or {}).get("qb_external_id") or "").split(",")
      if part
    }
    hit = sorted(recorded & deleted_ids)
    if not hit:
      continue
    matched.update(hit)
    _retract(event, hit, result, stamp=stamp, published=True)

  result.unmatched = len(deleted_ids - matched)
  session.flush()
  logger.info(
    "QuickBooks deletions applied: %d voided, %d flagged, %d already retracted, "
    "%d never synced",
    result.voided,
    result.flagged,
    result.skipped,
    result.unmatched,
  )
  return result


def _retract(
  event: Event,
  qb_ids: list[str],
  result: CdcApplyResult,
  *,
  stamp: str,
  published: bool,
) -> None:
  status = str(event.status)
  if status in UNPOSTED_STATUSES:
    event.status = "voided"
    event.metadata_ = {
      **(event.metadata_ or {}),
      "void_reason": "deleted_in_quickbooks",
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
