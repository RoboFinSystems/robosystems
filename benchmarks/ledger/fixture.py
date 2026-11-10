"""The company under test: a fresh Cascade load, and what gets planted in it."""

from __future__ import annotations

import calendar
import json
import subprocess
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from examples.roboledger_demo.data import (
  SCHEDULES,
  add_months,
  get_all_transactions,
  get_demo_start_date,
)

from .mcp import McpClient
from .score import ScheduleEntry
from .tasks import CASH, PAYABLES, BankLine, Task

_ROOT = Path(__file__).resolve().parents[2]
_CONFIG = _ROOT / ".local" / "config.json"
_SLOT = "cascade_demo"


@dataclass
class Company:
  mcp: McpClient
  period: str
  period_end: str
  code_by_id: dict[str, str]
  id_by_code: dict[str, str]
  trait_by_code: dict[str, str]
  agent_id_by_name: dict[str, str]

  def day(self, day: int) -> str:
    return f"{self.period}-{day:02d}"

  def refresh_chart(self) -> None:
    accounts = self.mcp.graphql(
      "{ accounts(limit: 500) { accounts { id code trait } } }"
    )
    rows = accounts["accounts"]["accounts"]
    self.code_by_id = {a["id"]: a["code"] for a in rows}
    self.id_by_code = {a["code"]: a["id"] for a in rows}
    self.trait_by_code = {a["code"]: a["trait"] for a in rows}


def load_company(reset: bool) -> Company:
  """Reload Cascade into the demo slot (a full reset) and open it over MCP."""
  if reset or not _CONFIG.exists():
    done = subprocess.run(
      ["just", "demo-roboledger"], cwd=_ROOT, capture_output=True, text=True
    )
    if done.returncode != 0:
      raise SystemExit(
        f"company load failed:\n{done.stdout[-1500:]}{done.stderr[-500:]}"
      )
  config = json.loads(_CONFIG.read_text())
  mcp = McpClient(
    config["base_url"], config["graphs"][_SLOT]["graph_id"], config["api_key"]
  )
  calendar_state = mcp.read("get-fiscal-calendar", {})
  period = calendar_state["close_target"]
  year, month = (int(part) for part in period.split("-"))
  last_day = calendar.monthrange(year, month)[1]
  agents = mcp.graphql("{ agents(limit: 500) { id name } }")["agents"]
  company = Company(
    mcp=mcp,
    period=period,
    period_end=f"{period}-{last_day:02d}",
    code_by_id={},
    id_by_code={},
    trait_by_code={},
    agent_id_by_name={a["name"]: a["id"] for a in agents},
  )
  company.refresh_chart()
  return company


def plant(company: Company, task: Task) -> tuple[dict[str, str], dict[str, str]]:
  """Enter the task's bills and land its bank lines; returns event ids by key."""
  mcp = company.mcp
  bill_ids: dict[str, str] = {}
  for bill in task.bills:
    created = mcp.read(
      "create-event-block",
      {
        "event_type": "bill_received",
        "event_category": "purchase",
        "occurred_at": f"{company.day(bill.day)}T00:00:00Z",
        "source": "manual",
        "agent_id": company.agent_id_by_name[bill.vendor],
        "amount": bill.cents,
        "description": bill.description,
        "terms_days": 30,
        "apply_handlers": True,
        "metadata": {
          # Posted, as the demo's own bills are, so it is an open payable.
          "type": "standard",
          "status": "posted",
          "posting_date": company.day(bill.day),
          "memo": bill.description,
          "line_items": [
            {
              "element_id": company.id_by_code[bill.expense_code],
              "debit_amount": bill.cents,
              "credit_amount": 0,
            },
            {
              "element_id": company.id_by_code[PAYABLES],
              "debit_amount": 0,
              "credit_amount": bill.cents,
            },
          ],
        },
      },
    )
    bill_ids[bill.key] = created["id"]
  line_ids: dict[str, str] = {}
  for line in task.lines:
    # A bank line posts only if its cash account is bound when it is created.
    created = mcp.read(
      "create-event-block",
      {
        "event_type": "bank_transaction",
        "event_category": "treasury",
        "occurred_at": f"{company.day(line.day)}T00:00:00Z",
        "source": "manual",
        "amount": line.cents,
        "description": line.description,
        "resource_type": "money",
        "resource_element_id": company.id_by_code[CASH],
        "metadata": {"merchant_name": line.merchant},
      },
    )
    line_ids[line.key] = created["id"]
  return bill_ids, line_ids


def _period_offset(company: Company) -> int:
  start = get_demo_start_date()
  year, month = (int(part) for part in company.period.split("-"))
  return (year - start.year) * 12 + month - start.month


def schedule_entries(company: Company) -> tuple[ScheduleEntry, ...]:
  """The recurring entries the period's close should draft, from the demo's
  own schedule list."""
  offset = _period_offset(company)
  return tuple(
    (debit, credit, monthly)
    for _name, debit, credit, monthly, _original, life, first in SCHEDULES
    if first <= offset < first + life
  )


def inbox_lines(company: Company) -> tuple[tuple[BankLine, ...], dict[str, str]]:
  """The demo's own open bank lines for the period, each with the answer the
  demo's data gives it, and their event ids. Read before anything is planted.

  Mirrors how the demo lands them: a customer payment settles its invoice
  through receivables, a known vendor's bill payment settles the bill through
  payables, and a card purchase goes straight to its accounts.
  """
  start = add_months(get_demo_start_date(), _period_offset(company))
  today = date.today()
  lines: list[BankLine] = []
  for txn_date, txn_type, description, agent, postings in get_all_transactions():
    if (txn_date.year, txn_date.month) != (start.year, start.month) or txn_date > today:
      continue
    cash_in = sum(debit for code, debit, _ in postings if code == CASH)
    cash_out = sum(credit for code, _, credit in postings if code == CASH)
    if txn_type == "payment":
      accounts, linked, cents = {"1100": cash_in}, True, cash_in
    elif txn_type == "card_purchase":
      accounts = {code: debit for code, debit, _ in postings if code != CASH}
      linked, cents = False, -cash_out
    elif (
      txn_type == "bill_payment"
      and agent in company.agent_id_by_name
      and not any(code.startswith("2") and debit for code, debit, _ in postings)
    ):
      accounts, linked, cents = {PAYABLES: cash_out}, True, -cash_out
    else:
      continue
    lines.append(
      BankLine(
        key=f"inbox-{txn_date.isoformat()}-{abs(cents)}",
        day=txn_date.day,
        cents=cents,
        description=description,
        merchant=agent or "",
        expect="classify",
        accounts=accounts,
        linked=linked,
      )
    )
  captured = company.mcp.read(
    "list-event-blocks",
    {"status": "captured", "event_type": "bank_transaction", "limit": 500},
  )["events"]
  by_day_amount = {(e["occurred_at"][:10], e["amount"]): e["id"] for e in captured}
  ids = {line.key: by_day_amount[(company.day(line.day), line.cents)] for line in lines}
  return tuple(lines), ids
