#!/usr/bin/env python3
"""Ingest Charlie Hoffman's 14-JE lemonade-stand dataset.

Reads ``GeneralJournal.csv``, groups its lines by ``JournalEntryID``, and
posts one event per journal entry via ``create-event-block`` with
``apply_handlers=True`` — so the ledger is built by replaying business events,
not by writing balances. Each line carries its ``TransactionDescriptionCode``
on ``metadata['transaction_description_code']``: that is the flow concept the
rollforward filter engine later matches on to decompose a balance-sheet
movement into the events that caused it.

Prerequisites: ``load_taxonomy.py`` and ``seed_mappings.py`` have run against
this graph. JE lines name mini concepts by qname, and this script resolves each
one to an element id against the graph's Element table before posting — so a
missing CoA fails loudly here rather than producing unresolvable line items.

CSV columns (read by header name via ``csv.DictReader``, so column order does
not matter):

- ``JournalEntryID``: e.g. "JE-201"
- ``EconomicEntityIdentifier``: ignored (this dataset is a single entity)
- ``TransactionPeriod``: posting date — accepts both ``YYYY-MM-DD`` and
  ``M/D/YY`` shapes
- ``Account``: the source system's chart-of-accounts code (e.g.
  "000-1100-00"); preserved on ``LineItem.metadata['source_account_code']``
  for traceability back to the source, never used to resolve the element
- ``GeneralLedgerAccountCode``: the mini qname (e.g.
  "mini:CashAndCashEquivalents") — this is the LineItem's element reference
- ``TransactionDescriptionCode``: the flow concept (e.g.
  "mini:ProceedsFromInvestmentsByOwner"), stamped on LineItem metadata for
  rollforward attribution and normalized through ``_KNOWN_TDC_ALIASES``
  against the canonical mini.xsd vocabulary
- ``Amount``: whole dollars (stored as cents)
- ``Balance``: "D" for debit, "C" for credit
- ``Sequence``: line order within the JE
- ``TransactionDescription``: per-line memo

Run it (the orchestrator runs this as step 5):
    uv run python -m examples.seattle_method_demo.ingest_transactions <graph_id>
    uv run python -m examples.seattle_method_demo.ingest_transactions <graph_id> --dry-run
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path

# Resolved relative to this file so the script works from any working
# directory. ``local/datasets/seattle_method/`` is gitignored and populated by
# ``pull_general_journal.sh``, which fetches the CSV from Charlie Hoffman's
# ``seattlemethod/prototypes`` repository — the canonical source.
CSV_PATH = (
  Path(__file__).resolve().parents[2]
  / "local"
  / "datasets"
  / "seattle_method"
  / "GeneralJournal.csv"
)


# Flow-concept names that appear in the upstream CSV but not in mini.xsd.
# Normalizing them at ingest keeps the rollforward filters matching against
# canonical concepts. Every substitution is logged as a warning so it stays
# visible, and surfaces in the reconciliation report under "Their data
# quality".
_KNOWN_TDC_ALIASES: dict[str, str] = {
  # JE-209: Cash-line TDC. mini.xsd's canonical name has no "Of", while the
  # sibling AccruedExpenses line uses ``mini:DecreaseFromPaymentOfInterest``
  # which does keep it — the source naming is internally inconsistent.
  "mini:PaymentOfInterest": "mini:PaymentInterest",
}


def _get_ledger_client():
  from robosystems_client.clients.ledger_client import LedgerClient

  config_path = Path(".local/config.json")
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
  """Parse a TransactionPeriod value → date.

  Accepts two shapes:
  - ISO ``YYYY-MM-DD``
  - ``M/D/YY`` or ``M/D/YYYY``, where a two-digit year is read as 20YY

  Raises ``ValueError`` on anything else, so the demo fails loudly rather
  than silently mis-dating events.
  """
  raw = raw.strip()
  if "-" in raw:
    return date.fromisoformat(raw)
  m, d, y = raw.split("/")
  year = int(y)
  if year < 100:
    year += 2000
  return date(year, int(m), int(d))


def _normalize_tdc(tdc: str, je_id: str, warnings: list[str]) -> str:
  """Apply ``_KNOWN_TDC_ALIASES`` and log when a substitution happens."""
  canonical = _KNOWN_TDC_ALIASES.get(tdc)
  if canonical is None:
    return tdc
  warnings.append(
    f"{je_id}: normalized TDC {tdc!r} → {canonical!r} (mini.xsd canonical name)"
  )
  return canonical


def _read_csv_grouped(csv_path: Path) -> dict[str, list[dict]]:
  """Read the CSV → {JournalEntryID: [line_row_dict, ...]}.

  Rows within each entry come back sorted by ``Sequence``, so a multi-line
  entry keeps its intended line order.
  """
  by_je: dict[str, list[dict]] = defaultdict(list)
  # The CSV is UTF-8 BOM-prefixed; ``utf-8-sig`` strips the BOM so
  # ``JournalEntryID`` parses cleanly as the first column header.
  with csv_path.open(encoding="utf-8-sig") as f:
    reader = csv.DictReader(f)
    for row in reader:
      je_id = row["JournalEntryID"]
      by_je[je_id].append(row)

  # Sort each JE's lines by Sequence (string-numeric in the CSV).
  for je_id, rows in by_je.items():
    rows.sort(key=lambda r: int(r["Sequence"]))
  return dict(by_je)


def _find_mini_taxonomy_id(client, graph_id: str) -> str | None:
  """Find the mini CoA taxonomy_id on the graph by name."""
  taxonomies = client.list_taxonomies(graph_id) or []
  for tax in taxonomies:
    if "mini" in (tax.get("name") or "").lower():
      return tax.get("id")
  return None


def _build_qname_to_element_id_map(client, graph_id: str) -> dict[str, str]:
  """Resolve every ``mini:*`` qname to its element_id on the graph.

  Mini elements persist with ``source='native'`` — the Element source CHECK
  constraint admits a fixed enum that has no 'mini' member — so the lookup
  scopes by taxonomy_id rather than by source.
  """
  taxonomy_id = _find_mini_taxonomy_id(client, graph_id)
  if not taxonomy_id:
    raise SystemExit(
      "No mini CoA taxonomy found on this graph. Did load_taxonomy.py run?"
    )
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


def _build_event_payload(
  je_id: str,
  rows: list[dict],
  qname_to_id: dict[str, str],
  warnings: list[str] | None = None,
) -> dict:
  """Build a single ``create-event-block`` request from a JE's rows.

  Emits flat single-entry ``journal_entry_recorded`` metadata: one
  ``line_items`` list plus ``posting_date`` and ``memo``, with each line
  carrying its mini element reference and its
  ``transaction_description_code``.

  ``source`` is ``manual`` because the dataset comes from none of the
  integrated sources, and the events CHECK constraint admits only
  {manual, system, schedule, quickbooks, xero, plaid}.
  """
  posting = _parse_date(rows[0]["TransactionPeriod"])

  # Entry-level memo. Each line also keeps its own description.
  memo = rows[0]["TransactionDescription"]

  line_items: list[dict] = []
  for row in rows:
    amount_dollars = int(row["Amount"])
    amount_cents = amount_dollars * 100
    balance = row["Balance"]
    debit = amount_cents if balance == "D" else 0
    credit = amount_cents if balance == "C" else 0

    mini_qname = row["GeneralLedgerAccountCode"]
    tdc = _normalize_tdc(
      row["TransactionDescriptionCode"],
      je_id,
      warnings if warnings is not None else [],
    )
    legacy_code = row["Account"]

    element_id = qname_to_id.get(mini_qname)
    if not element_id:
      raise ValueError(
        f"JE {je_id} line references {mini_qname!r}, which has no "
        f"matching Element on this graph. Check load_taxonomy.py output."
      )
    line_items.append(
      {
        "element_id": element_id,
        "debit_amount": debit,
        "credit_amount": credit,
        "description": row["TransactionDescription"],
        "metadata": {
          "transaction_description_code": tdc,
          "source_account_code": legacy_code,
          "mini_qname": mini_qname,
        },
      }
    )

  # REA event category, inferred from the description. Advisory only — the
  # reconciliation turns on the GL impact, not on this classification.
  event_category = _pick_event_category(rows[0]["TransactionDescription"])

  occurred_at = datetime.combine(posting, datetime.min.time()).isoformat() + "Z"

  return {
    "event_type": "journal_entry_recorded",
    "event_category": event_category,
    "event_class": "economic",
    "source": "manual",
    "external_id": f"seattle_method:{je_id}",
    "occurred_at": occurred_at,
    "description": f"Charlie Hoffman test case {je_id}: {memo}",
    "apply_handlers": True,
    "metadata": {
      "posting_date": posting.isoformat(),
      "memo": memo,
      "status": "posted",
      "type": _pick_entry_type(rows[0]["TransactionDescription"]),
      "line_items": line_items,
      "seattle_method_je_id": je_id,
    },
  }


def _pick_event_category(description: str) -> str:
  """Best-effort REA category from the JE description string."""
  d = description.lower()
  if "investment" in d or "borrowing" in d or "long term debt" in d:
    return "financing"
  if "sales" in d:
    return "sales"
  if "purchase" in d or "payment for" in d or "payment of" in d:
    return "purchase"
  if "collection" in d:
    return "treasury"
  if "depreciation" in d or "amortization" in d or "write off" in d or "taxes" in d:
    return "adjustment"
  if "interest" in d:
    return "treasury"
  return "adjustment"


def _pick_entry_type(description: str) -> str:
  """Classify an entry as closing, adjusting, or standard from its description."""
  if "closing entry" in description.lower():
    return "closing"
  if "accrual" in description.lower():
    return "adjusting"
  return "standard"


def ingest(
  graph_id: str, csv_path: Path = CSV_PATH, dry_run: bool = False
) -> tuple[int, list[str]]:
  """Walk the CSV's JEs and post each one via create-event-block.

  Returns ``(events_created, warnings)``.
  """
  if not csv_path.exists():
    raise SystemExit(f"Transactions CSV not found at {csv_path}")

  grouped = _read_csv_grouped(csv_path)
  print(f"Found {len(grouped)} JournalEntry(ies) in {csv_path.name}")

  warnings: list[str] = []
  if dry_run:
    # Stand-in element ids so the payload shape can be validated offline.
    fake_map = {
      row["GeneralLedgerAccountCode"]: f"elem_dry_{row['GeneralLedgerAccountCode']}"
      for rows in grouped.values()
      for row in rows
    }
    for je_id, rows in sorted(grouped.items()):
      payload = _build_event_payload(je_id, rows, fake_map, warnings)
      print(
        f"  {je_id}: {len(payload['metadata']['line_items'])} line(s), "
        f"posting_date={payload['metadata']['posting_date']}, "
        f"category={payload['event_category']}, "
        f"type={payload['metadata']['type']}"
      )
    return len(grouped), warnings

  client = _get_ledger_client()
  print("Resolving mini qnames to element_ids on the graph…")
  qname_to_id = _build_qname_to_element_id_map(client, graph_id)
  print(f"  {len(qname_to_id)} mini element(s) found")

  created = 0
  for je_id, rows in sorted(grouped.items()):
    payload = _build_event_payload(je_id, rows, qname_to_id, warnings)
    try:
      response = client.create_event_block(graph_id, payload)
      created += 1
      print(f"  ✓ {je_id} → event {response.id} status={response.status}")
    except Exception as exc:  # noqa: BLE001 — surface every failure for diagnosis
      warnings.append(f"{je_id}: {exc}")
      print(f"  ✗ {je_id}: {exc}")

  return created, warnings


def main() -> None:
  parser = argparse.ArgumentParser(
    description="Ingest Charlie's 14-JE lemonade-stand dataset as Events.",
  )
  parser.add_argument("graph_id", help="Target graph id.")
  parser.add_argument(
    "--csv",
    type=Path,
    default=CSV_PATH,
    help="Path to the transactions CSV.",
  )
  parser.add_argument(
    "--dry-run",
    action="store_true",
    help="Build payloads + report shape; do not call the API.",
  )
  args = parser.parse_args()

  created, warnings = ingest(args.graph_id, args.csv, dry_run=args.dry_run)

  if warnings:
    print(f"\n{len(warnings)} JE(s) failed:")
    for w in warnings:
      print(f"  ⚠️  {w}")

  action = "Would create" if args.dry_run else "Created"
  print(f"\n{action} {created} event(s).")


if __name__ == "__main__":
  main()
