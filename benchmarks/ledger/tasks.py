"""Tasks: what is planted in the open month and what the books should say after.

Every expectation is authored here, before any run. Accounts are named by chart
code and counterparties by name, since ids differ on every load.
"""

from __future__ import annotations

from dataclasses import dataclass, field

CASH = "1000"
PAYABLES = "2000"

FINANCING_FLOWS = frozenset(
  {"rs-gaap:ProceedsFromIssuanceOfLongTermDebt", "rs-gaap:ProceedsFromNotesPayable"}
)

# Cascade's five recurring schedule entries: (debit code, credit code, cents).
SCHEDULE_ENTRIES = (
  ("7000", "1350", 13333),
  ("7000", "1350", 2500),
  ("6400", "1200", 10000),
  ("6100", "1210", 2500),
  ("6200", "1220", 5000),
)


@dataclass(frozen=True)
class Bill:
  """An open vendor bill entered before the model starts."""

  key: str
  day: int
  vendor: str
  cents: int
  expense_code: str
  description: str


@dataclass(frozen=True)
class BankLine:
  """A bank line landed unclassified. `cents` is signed: money out is negative.

  `expect` is `classify`, `void` or `open`. For `classify`, `accounts` maps a
  chart code to the positive cents the line puts there; `new_account_trait`
  stands in for an account the model has to create.
  """

  key: str
  day: int
  cents: int
  description: str
  merchant: str
  expect: str
  accounts: dict[str, int] = field(default_factory=dict)
  new_account_trait: str | None = None
  flows: frozenset[str] = frozenset()
  settles: str | None = None


@dataclass(frozen=True)
class Task:
  name: str
  on_script: bool
  lines: tuple[BankLine, ...]
  bills: tuple[Bill, ...] = ()
  close: bool = False


KNOWN_VENDOR = BankLine(
  "known-vendor",
  27,
  -18437,
  "AMZN Mktp US*2K4RT81X0",
  "Amazon",
  "classify",
  {"6500": 18437},
)
NEW_VENDOR = BankLine(
  "new-vendor",
  28,
  -61240,
  "DELTA AIR 0062341178823 ATLANTA",
  "Delta Air Lines",
  "classify",
  {"6600": 61240},
)
SPLIT = BankLine(
  "split",
  30,
  -100000,
  "AMAZON.COM*BUSINESS ORDER 114-88 (printer paper + toner $700; Adobe license $300)",
  "Amazon",
  "classify",
  {"6500": 70000, "6100": 30000},
)
CHAIR_BILL = Bill(
  "chair-bill",
  22,
  "Herman Miller",
  90000,
  "6500",
  "Herman Miller - task chairs (2), invoice HM-88213",
)
SETTLE_BILL = BankLine(
  "settle-bill",
  29,
  -90000,
  "HERMANMILLER.COM ACH PMT HM-88213",
  "Herman Miller",
  "classify",
  {PAYABLES: 90000},
  settles="chair-bill",
)
LOAN = BankLine(
  "loan",
  30,
  2500000,
  "WIRE IN FIRST CASCADE BANK LOAN PROCEEDS TERM NOTE 4471",
  "First Cascade Bank",
  "classify",
  new_account_trait="liability",
  flows=FINANCING_FLOWS,
)
DUPLICATE = BankLine(
  "duplicate",
  20,
  -40000,
  "UNITED AIRLINES 0167745521",
  "United Airlines",
  "void",
)
AMBIGUOUS = BankLine(
  "ambiguous",
  30,
  -500000,
  "ONLINE TRANSFER TO M REYES",
  "Marcus Reyes",
  "open",
)

TASKS = {
  t.name: t
  for t in (
    Task("known-vendor", True, (KNOWN_VENDOR,)),
    Task("new-vendor", True, (NEW_VENDOR,)),
    Task("split", True, (SPLIT,)),
    Task("settle-bill", True, (SETTLE_BILL,), (CHAIR_BILL,)),
    Task("loan", False, (LOAN,)),
    Task("duplicate", False, (DUPLICATE,)),
    Task("ambiguous", False, (AMBIGUOUS,)),
    Task(
      "month",
      True,
      (KNOWN_VENDOR, NEW_VENDOR, SPLIT, SETTLE_BILL, LOAN, DUPLICATE),
      (CHAIR_BILL,),
      close=True,
    ),
  )
}

SYSTEM_PREAMBLE = """\
You are the accountant for this company, working its books through the tools \
provided. Nobody is available to answer questions during this session. You have \
authority to classify, commit and void bank lines, to add accounts and schedules, \
and to close a period, without asking first. Base every decision on the company's \
own history and documents. If a line cannot be decided without a person, leave it \
open and say why; never force a posting you cannot support. When you are done, \
reply with a short summary of what you did and what you left open."""


def prompt(task: Task, period: str) -> str:
  inbox = (
    f"New bank lines have arrived for {period}. They are `bank_transaction` events "
    "with status `captured`. Work each one: classify and commit what the company's "
    "history and policies support, void a line only if it should not post, and "
    "leave open any line that needs a person."
  )
  if task.close:
    return (
      f"{inbox} Then do the month-end close for {period}: draft the recurring "
      "entries, review the drafts, and close the period."
    )
  return f"{inbox} Do not close the period."
