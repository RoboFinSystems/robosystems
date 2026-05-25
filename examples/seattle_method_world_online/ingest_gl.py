#!/usr/bin/env python3
"""Ingest Charlie Hoffman's "The World Online" general ledger.

Reads ``GeneralLedger.csv`` (22,288 lines) + ``ChartOfAccounts.csv``
(239 GL accounts) pulled into
``local/datasets/seattle_method_world_online/`` by
``pull_world_online.sh``, groups lines by ``journalId`` (~3,389 balanced
entries), and posts one event per journal id via ``create-event-block``
with ``apply_handlers=True``.

This is the scaled-up sibling of
``examples.seattle_method_demo.ingest_transactions`` (Charlie's 14-JE
lemonade stand). The methodology is identical — each LineItem carries
its business-event concept on ``metadata['transaction_description_code']``,
which the ingest handler resolves to ``LineItem.flow_element_id`` (the
first-class FK the rollforward filter engine matches). The differences:

- **GL flat format** (not pre-journalized JE CSV). Columns:
  ``glAccountNumber`` (GL account), ``userDefined2`` (the mini line-item
  concept — used as the LineItem element), ``tags`` (the mini business
  event — stamped as the flow tag), ``amount`` + ``amountCreditDebitIndicator``.
- **CoA layer** — ``ChartOfAccounts.csv`` maps the 239 real GL accounts
  to mini concepts. We collapse to the mini concept at ingest (matching
  Test Case 1, and Charlie's note #3: use the lowest-level line item as
  the code and let software roll up; subclassifications deferred). The
  GL account number + name are preserved on ``LineItem.metadata`` for
  audit/traceability.
- **Opening balances** — 98 ``BBF`` (beginning-balance-forward) lines
  dated 12/31/2023, tagged ``mini:OpeningBalance``. They ingest as real
  transactions exactly like every other line; for them to attribute in
  the rollforward (and reconcile against Charlie's OpeningBalance pivot
  row), ``mini:OpeningBalance`` must be a loaded Element — see the load
  step, which adds it as an extension concept (it is NOT in MINI 2026).
- **Provenance** — ``transactionId``, ``enteredBy``, ``enteredDateTime``
  preserved on metadata (Charlie's note #5).

**Sign convention**: the ``amount`` column is already signed in
**debit-positive** form (verified against ``SummaryOfTransactions.csv``:
asset openings positive, liability/equity openings negative; the D/C
indicator agrees with the sign on every row). So
``debit_amount - credit_amount == round(amount * 100)`` and no indicator
gymnastics are needed.

Usage:
    uv run python -m examples.seattle_method_world_online.ingest_gl <graph_id>
    uv run python -m examples.seattle_method_world_online.ingest_gl <graph_id> --dry-run
    uv run python -m examples.seattle_method_world_online.ingest_gl <graph_id> --limit 50
    uv run python -m examples.seattle_method_world_online.ingest_gl <graph_id> --start 1000
"""

from __future__ import annotations

import argparse
import csv
import json
import time
from collections import OrderedDict
from datetime import date, datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = REPO_ROOT / "local" / "datasets" / "seattle_method_world_online"
GL_PATH = DATA_DIR / "GeneralLedger.csv"
COA_PATH = DATA_DIR / "ChartOfAccounts.csv"

OPENING_BALANCE_QNAME = "mini:OpeningBalance"

# journalEntryType → best-effort REA event_category. The GL impact is
# what drives the reconciliation; the REA classification is advisory.
_JE_TYPE_CATEGORY: dict[str, str] = {
  "BBF": "adjustment",  # beginning balance forward (opening)
  "SJ": "sales",  # sales journal
  "INVCE": "sales",
  "SLSTE": "sales",
  "SLSDP": "sales",
  "CRJ": "treasury",  # cash receipts journal
  "RECVG": "treasury",
  "PMTRX": "purchase",  # payments
  "PMCHK": "purchase",
  "PMPAY": "purchase",
  "PMVPY": "purchase",
  "PMAPY": "purchase",
  "CMTRX": "purchase",
  "CMXFR": "treasury",
  "POIVC": "purchase",  # purchase order invoice
  "UPRCC": "purchase",  # the bulk AP/inventory accrual stream
  "IVADJ": "adjustment",  # inventory adjustment
  "GJ": "adjustment",  # general journal
}


def _get_ledger_client():
  from robosystems_client.clients.ledger_client import LedgerClient

  config_path = REPO_ROOT / ".local" / "config.json"
  if not config_path.exists():
    raise SystemExit("Missing .local/config.json — run `just demo-user` first.")
  with config_path.open() as f:
    cfg = json.load(f)
  return LedgerClient(
    {
      "base_url": cfg.get("base_url", "http://localhost:8000"),
      "token": cfg["api_key"],
    }
  )


def _parse_date(raw: str) -> date:
  """Parse a GL ``period`` value → date.

  The World Online GL uses ``M/D/YYYY`` (e.g. ``1/1/2024``). ISO
  ``YYYY-MM-DD`` is also accepted defensively. Raises ``ValueError`` on
  any other shape so the demo fails loudly rather than mis-dating events.
  """
  raw = raw.strip()
  if "-" in raw:
    return date.fromisoformat(raw)
  m, d, y = raw.split("/")
  year = int(y)
  if year < 100:
    year += 2000
  return date(year, int(m), int(d))


def load_coa_map(coa_path: Path = COA_PATH) -> dict[str, tuple[str, str]]:
  """Read ChartOfAccounts.csv → {glAccountNumber: (mini_qname, name)}.

  ``userDefined2`` carries the mini concept the GL account rolls up to;
  ``userDefined1`` is the human-readable account name. Used to enrich
  LineItem metadata (the GL rows also carry these inline, but the CoA
  file is the canonical mapping and lets us flag inline/CoA drift).
  """
  out: dict[str, tuple[str, str]] = {}
  if not coa_path.exists():
    return out
  with coa_path.open(encoding="utf-8-sig") as f:
    for row in csv.DictReader(f):
      gl = (row.get("glAccountNumber") or "").strip()
      if gl:
        out[gl] = (
          (row.get("userDefined2") or "").strip(),
          (row.get("userDefined1") or "").strip(),
        )
  return out


def read_gl_grouped(gl_path: Path = GL_PATH) -> OrderedDict[str, list[dict]]:
  """Read GeneralLedger.csv → ordered {journalId: [row, ...]}.

  Insertion order (by first appearance of each journalId) is preserved,
  and the journalId=1 opening entry sorts first because it appears
  first in the file.
  """
  by_journal: OrderedDict[str, list[dict]] = OrderedDict()
  with gl_path.open(encoding="utf-8-sig") as f:
    for row in csv.DictReader(f):
      jid = row["journalId"]
      by_journal.setdefault(jid, []).append(row)
  return by_journal


def _build_event_payload(
  journal_id: str,
  rows: list[dict],
  qname_to_id: dict[str, str],
  coa_map: dict[str, tuple[str, str]],
  warnings: list[str],
) -> dict:
  """Build one ``create-event-block`` request from a journalId's rows."""
  posting = _parse_date(rows[0]["period"])
  je_type = (rows[0].get("journalEntryType") or "").strip()
  memo = (rows[0].get("jeLineDescription") or je_type or "").strip()

  line_items: list[dict] = []
  for row in rows:
    mini_qname = (row.get("userDefined2") or "").strip()
    element_id = qname_to_id.get(mini_qname)
    if not element_id:
      raise ValueError(
        f"journalId {journal_id} line references {mini_qname!r}, which has "
        f"no matching Element on this graph. Check the load step output."
      )

    # amount is already debit-positive signed (verified against
    # SummaryOfTransactions.csv). Split into the non-negative
    # debit/credit columns the LineItem model expects.
    cents = round(float(row["amount"]) * 100)

    # Skip zero-amount lines. Charlie's GL carries the occasional $0
    # memo line (e.g. a placeholder InterestExpense line on a card
    # transaction); our double-entry handler rejects any line with no
    # debit or credit, and rejecting the whole entry would drop its
    # non-zero siblings too. A $0 line doesn't affect the balance, so
    # dropping just it keeps the entry valid. This mirrors Test Case 1's
    # JE-225 nil-entry finding — the $0 lines have no economic substance,
    # so the only consequence is a count-only delta on cells where
    # Charlie's pivot counts the $0 line (classified Their data quality).
    if cents == 0:
      warnings.append(
        f"journalId {journal_id}: skipped $0 line on {mini_qname} "
        f"(no economic substance; not postable in double-entry)"
      )
      continue
    debit = cents if cents >= 0 else 0
    credit = -cents if cents < 0 else 0

    flow_qname = (row.get("tags") or "").strip()
    gl_account = (row.get("glAccountNumber") or "").strip()
    account_name = (row.get("userDefined1") or "").strip()

    # CoA drift check — the GL row's inline mini concept should agree
    # with the canonical ChartOfAccounts.csv mapping for that account.
    coa_entry = coa_map.get(gl_account)
    if coa_entry and coa_entry[0] and coa_entry[0] != mini_qname:
      warnings.append(
        f"journalId {journal_id}: GL account {gl_account} inline concept "
        f"{mini_qname!r} != CoA mapping {coa_entry[0]!r}"
      )

    line_items.append(
      {
        "element_id": element_id,
        "debit_amount": debit,
        "credit_amount": credit,
        "description": (row.get("jeLineDescription") or "").strip(),
        "metadata": {
          "transaction_description_code": flow_qname,
          "source_account_code": gl_account,
          "source_account_name": account_name,
          "source_transaction_id": (row.get("transactionId") or "").strip(),
          "entered_by": (row.get("enteredBy") or "").strip(),
          "entered_datetime": (row.get("enteredDateTime") or "").strip(),
          "journal_entry_type": je_type,
          "mini_qname": mini_qname,
        },
      }
    )

  occurred_at = datetime.combine(posting, datetime.min.time()).isoformat() + "Z"
  event_category = _JE_TYPE_CATEGORY.get(je_type, "adjustment")
  # JournalEntryRecordedMetadata.type enum is {standard, adjusting, closing,
  # reversing}; opening balances are ordinary posted entries (type=standard).
  # The BBF distinction is preserved on metadata.journal_entry_type.
  entry_type = "standard"

  return {
    "event_type": "journal_entry_recorded",
    "event_category": event_category,
    "event_class": "economic",
    "source": "manual",
    "external_id": f"world_online:{journal_id}",
    "occurred_at": occurred_at,
    "description": f"World Online journalId {journal_id} ({je_type}): {memo}",
    "apply_handlers": True,
    "metadata": {
      "posting_date": posting.isoformat(),
      "memo": memo,
      "status": "posted",
      "type": entry_type,
      "line_items": line_items,
      "world_online_journal_id": journal_id,
      "journal_entry_type": je_type,
    },
  }


def _find_mini_taxonomy_id(client, graph_id: str) -> str | None:
  taxonomies = client.list_taxonomies(graph_id) or []
  for tax in taxonomies:
    if "mini" in (tax.get("name") or "").lower():
      return tax.get("id")
  return None


def _build_qname_to_element_id_map(client, graph_id: str) -> dict[str, str]:
  """Resolve every mini Element qname → element_id (paged by taxonomy_id)."""
  taxonomy_id = _find_mini_taxonomy_id(client, graph_id)
  if not taxonomy_id:
    raise SystemExit("No mini CoA taxonomy found on this graph. Did the load step run?")
  out: dict[str, str] = {}
  offset = 0
  page_size = 1000
  while True:
    page = client.list_elements(
      graph_id, taxonomy_id=taxonomy_id, limit=page_size, offset=offset
    )
    items = (page or {}).get("elements", [])
    if not items:
      break
    for e in items:
      if e.get("qname"):
        out[e["qname"]] = e["id"]
    if len(items) < page_size:
      break
    offset += page_size
  return out


def ingest(
  graph_id: str,
  gl_path: Path = GL_PATH,
  coa_path: Path = COA_PATH,
  dry_run: bool = False,
  limit: int | None = None,
  start: int = 0,
) -> tuple[int, list[str]]:
  """Walk the GL's journal ids and post each as one event.

  Returns ``(events_created, warnings)``. ``limit`` caps the number of
  entries (handy for smoke tests); ``start`` skips the first N entries
  (resume after a partial run).
  """
  if not gl_path.exists():
    raise SystemExit(f"GeneralLedger.csv not found at {gl_path} — run the pull step.")

  coa_map = load_coa_map(coa_path)
  grouped = read_gl_grouped(gl_path)
  journal_ids = list(grouped.keys())
  if start:
    journal_ids = journal_ids[start:]
  if limit is not None:
    journal_ids = journal_ids[:limit]

  total_lines = sum(len(grouped[j]) for j in journal_ids)
  print(
    f"GeneralLedger.csv → {len(grouped)} journal entries "
    f"({sum(len(v) for v in grouped.values())} lines). "
    f"Ingesting {len(journal_ids)} entries ({total_lines} lines)"
    + (f" starting at index {start}" if start else "")
    + "."
  )

  warnings: list[str] = []

  if dry_run:
    fake_map = {
      (row.get("userDefined2") or "").strip(): "elem_dry"
      for j in journal_ids
      for row in grouped[j]
    }
    sample = journal_ids[:5]
    for jid in sample:
      payload = _build_event_payload(jid, grouped[jid], fake_map, coa_map, warnings)
      li = payload["metadata"]["line_items"]
      print(
        f"  journalId {jid}: {len(li)} line(s), "
        f"posting_date={payload['metadata']['posting_date']}, "
        f"category={payload['event_category']}, type={payload['metadata']['type']}"
      )
    print(f"  … (dry-run — showed {len(sample)} of {len(journal_ids)} entries)")
    return len(journal_ids), warnings

  client = _get_ledger_client()
  print("Resolving mini qnames to element_ids on the graph…")
  qname_to_id = _build_qname_to_element_id_map(client, graph_id)
  print(f"  {len(qname_to_id)} mini element(s) found")
  if OPENING_BALANCE_QNAME not in qname_to_id:
    warnings.append(
      f"{OPENING_BALANCE_QNAME} is not a loaded Element — opening-balance "
      f"lines will not resolve flow_element_id and won't attribute in the "
      f"rollforward. Re-run the load step (it adds the extension concept)."
    )
    print(f"  ⚠️  {warnings[-1]}")

  created = 0
  t0 = time.time()
  for idx, jid in enumerate(journal_ids, 1):
    payload = _build_event_payload(jid, grouped[jid], qname_to_id, coa_map, warnings)
    try:
      response = client.create_event_block(graph_id, payload)
      created += 1
      _ = response  # response.id available; suppressed in the bulk loop
    except Exception as exc:
      warnings.append(f"journalId {jid}: {exc}")
      print(f"  ✗ journalId {jid}: {exc}")

    if idx % 250 == 0 or idx == len(journal_ids):
      rate = idx / max(time.time() - t0, 1e-6)
      print(
        f"  … {idx}/{len(journal_ids)} entries "
        f"({created} ok, {len(warnings)} warning(s), {rate:.1f}/s)"
      )

  return created, warnings


def main() -> None:
  parser = argparse.ArgumentParser(
    description="Ingest The World Online general ledger as Events.",
  )
  parser.add_argument("graph_id", help="Target graph id.")
  parser.add_argument(
    "--gl", type=Path, default=GL_PATH, help="GeneralLedger.csv path."
  )
  parser.add_argument(
    "--coa", type=Path, default=COA_PATH, help="ChartOfAccounts.csv path."
  )
  parser.add_argument(
    "--limit", type=int, default=None, help="Only ingest the first N entries."
  )
  parser.add_argument(
    "--start", type=int, default=0, help="Skip the first N entries (resume)."
  )
  parser.add_argument(
    "--dry-run",
    action="store_true",
    help="Build payloads + report shape; do not call the API.",
  )
  args = parser.parse_args()

  created, warnings = ingest(
    args.graph_id,
    args.gl,
    args.coa,
    dry_run=args.dry_run,
    limit=args.limit,
    start=args.start,
  )

  if warnings:
    shown = warnings[:25]
    print(f"\n{len(warnings)} warning(s){' (first 25)' if len(warnings) > 25 else ''}:")
    for w in shown:
      print(f"  ⚠️  {w}")

  action = "Would create" if args.dry_run else "Created"
  print(f"\n{action} {created} event(s).")


if __name__ == "__main__":
  main()
