#!/usr/bin/env python3
"""Ingest Charlie Hoffman's 14-JE lemonade-stand dataset.

Reads the GeneralJournal.csv pulled from Charlie's
``seattlemethod/prototypes`` GitHub repo into
``local/datasets/seattle_method/`` (see ``pull_general_journal.sh``),
groups by ``JournalEntryID``, and posts one event per JE via
``create-event-block`` with ``apply_handlers=True``. Each line carries
its ``TransactionDescriptionCode`` on
``metadata['transaction_description_code']`` — the field the rollforward
filter engine matches against.

Pre-condition: ``load_taxonomy.py`` + ``seed_mappings.py`` have run
against this graph (the JE lines reference mini concepts by
element_external_id, which the journal_entry_recorded handler
resolves against the Element table).

CSV column convention (Charlie's format — read by header name via
``csv.DictReader``, so column order is tolerant):

- ``JournalEntryID``: e.g. "JE-201"
- ``EconomicEntityIdentifier``: ignored (single entity in this dataset)
- ``TransactionPeriod``: posting date — handles ISO ``YYYY-MM-DD`` (Charlie's
  GitHub format) and the legacy ``M/D/YY`` shape from earlier exports
- ``Account``: legacy CoA code (e.g. "000-1100-00"); preserved on
  ``LineItem.metadata['source_account_code']`` for audit but not used
  for element resolution
- ``GeneralLedgerAccountCode``: the mini qname (e.g.
  "mini:CashAndCashEquivalents") — used as the LineItem element
  reference
- ``TransactionDescriptionCode``: the flow concept (e.g.
  "mini:ProceedsFromInvestmentsByOwner") — stamped on
  LineItem.metadata for rollforward attribution; passed through a small
  alias map (``_KNOWN_TDC_ALIASES``) to repair known typos against the
  canonical mini.xsd vocabulary (e.g. ``mini:PaymentOfInterest`` →
  ``mini:PaymentInterest``)
- ``Amount``: whole dollars (multiply by 100 for cents)
- ``Balance``: "D" for debit, "C" for credit
- ``Sequence``: line order within the JE
- ``TransactionDescription``: per-line memo

Usage:
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

# Resolve relative to this file so the script works regardless of the
# caller's CWD — matches the pattern ``main.py`` uses. Points at the
# gitignored ``local/datasets/seattle_method/`` location populated by
# ``pull_general_journal.sh``; Charlie's GitHub repo is the canonical
# source.
CSV_PATH = (
  Path(__file__).resolve().parents[2]
  / "local"
  / "datasets"
  / "seattle_method"
  / "GeneralJournal.csv"
)


# Known TDC typos / vocabulary mismatches in Charlie's upstream CSV.
# We normalize at ingest time so the rollforward filter engine matches
# against the canonical mini.xsd concepts. Logged as warnings when
# applied so the substitution is transparent — also surfaces in the
# reconciliation report as a "Their data quality" classification.
_KNOWN_TDC_ALIASES: dict[str, str] = {
  # JE-209: Cash-line TDC. mini.xsd's canonical name has no "Of"; the
  # sibling AccruedExpenses line uses ``mini:DecreaseFromPaymentOfInterest``
  # which DOES keep the "Of" — Charlie's naming is internally inconsistent.
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
  - ISO ``YYYY-MM-DD`` — Charlie's current GitHub CSV format
  - ``M/D/YY`` or ``M/D/YYYY`` — legacy short-date shape from earlier
    exports; two-digit years are assumed to be 20YY (the dataset is
    Q1 2024)

  Raises ``ValueError`` on any other shape so the demo fails loudly
  rather than silently mis-dating events.
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

  Preserves CSV row order; rows within a JE are sorted by Sequence on
  the way out so JE-209's 4 lines stay in their intended order.
  """
  by_je: dict[str, list[dict]] = defaultdict(list)
  # Charlie's CSV is UTF-8 BOM-prefixed; ``utf-8-sig`` strips the BOM
  # so ``JournalEntryID`` parses cleanly as the first column header.
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

  Mini elements get persisted with ``source='native'`` (the source
  CHECK constraint doesn't admit 'mini'), so we look them up by
  taxonomy_id rather than source.
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

  Flat-shape (single-entry) journal_entry_recorded metadata: one
  ``line_items`` list, ``posting_date``, ``memo``. Each line carries
  ``element_external_id`` (the mini qname) and per-line metadata
  with ``transaction_description_code``.

  ``source`` is ``manual`` — Charlie's data isn't from any of our
  integrated sources (QB, Plaid, etc.), and the events DB CHECK
  constraint only admits {manual, system, schedule, quickbooks, xero,
  plaid}. ``manual`` is the closest semantic fit.
  """
  posting = _parse_date(rows[0]["TransactionPeriod"])

  # Use the first line's description as the entry-level memo. Each line
  # also carries its own description.
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

  # Pick an event_category from REA's vocabulary. Best-fit per JE
  # would require parsing the description; for this fixture we use
  # 'adjustment' as a catch-all neutral category — the GL impact is
  # what matters for the reconciliation, not the REA classification.
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
  """Detect closing entries — JE-223..226 are explicitly closing."""
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
    # Build payloads with a fake mapping so dry-run validates shape.
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
