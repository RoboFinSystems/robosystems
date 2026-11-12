# Reporting acceptance

Checks the rendered financial statements against the ledger beneath them, for
every entity and every closed month of a graph, and for the consolidated group.
Read-only: it posts nothing, so it can be pointed at any graph.

A ledger line lands on an account; the statements are a pivot of those accounts
through the chart-of-accounts mapping, the statement networks, the flow
elements and, for a group, the consolidation. This harness tests that pivot. It
never asks the reporting engine what a number should be: the expected side is
the trial balance summed through the mapping, in `expected.py`, and for the
cash flow rows the journal lines themselves, in `cashflow.py`.

It is not part of the Docker image.

## Running it

Needs a loaded graph. Locally, any demo will do:

```bash
just demo-coffee-roaster                         # Driftline, a group of six entities
just bench-reporting coffee_roaster --months 16  # a demo slot in .local/config.json
just bench-reporting cascade_demo
just bench-reporting kg1a… --months 6 --scope consolidated --json out.json
```

`--base-url` reads another API, with the key in `ROBOSYSTEMS_API_KEY`. It exits
non-zero when any check fails.

## What is checked

For each entity whose books cover the month, and for the consolidated group:

| Statement | Checks |
|---|---|
| Ledger | every account with a balance is mapped to a concept |
| Balance sheet | assets equal liabilities plus equity; total assets, liabilities and equity equal the accounts of each trait (equity with the earnings to date); every asset and liability row equals the accounts mapped to it and to the rows beneath it; every concept with a balance has a row |
| Income statement | net income equals revenue less expense in the ledger; every row equals its accounts |
| Cash flow | opening cash, closing cash and the change equal the cash accounts; the sections sum to the change; the operating section needs no reconciling item; it starts from the income statement's net income; every investing and financing row equals the journal lines that carry its flow; every operating row equals the income statement figure it repeats or the change in its working-capital balances |
| Equity statement | opening and closing equity equal the ledger at each date; the movements carry one to the other; it carries the income statement's net income |

Each statement's own `validation` is recorded beside these as one more check.

The consolidated scope sums the parent, the consolidation entity and every
fully consolidated member that was in the group on the date, read from the
entity records (`consolidationMethod`, `joinedOn`, `leftOn`). An equity-method
member is not summed.

A window the engine declines (`inconclusive`: a month before the books begin)
is left unchecked and shows as `-` in the grid. A statement that fails to
render is a failed check, and the run goes on. A run that checks nothing exits
non-zero.

## What it assumes

A month's activity is the difference between two cumulative trial balances, so
the ledger must not post year-end closing entries or reset its income accounts.
The demo ledgers do neither. On a ledger that does, the income statement of the
month holding the closing entry would compare wrongly.

A statement lists the rows beneath a row before the row itself. If the order
changed, the parent rows would fail the row check rather than pass it.

## How the cash flow rows are worked out

`cashflow.py` reads the month's posted journal entries and the library's cash
flow arcs (which section a row sits in, and each balance concept's default
row). It follows the rules ASC 230 and the product's own documentation state,
written a second time from the ledger's side:

- An entry that moves cash: a tagged cash line is its own row; an untagged one
  leaves each other line to its tag or to its concept's default row.
- An entry with no cash line moves none. An investing or financing line in it
  is backed out of the working-capital line it was set against. Against
  payables it is a bill, and reaches its row when the bill is paid.
- A group's consolidation entity moves no cash; its tagged lines adjust the
  rows the members' entries produced.
- A member bought or sold is stated net of the cash it held.
- An operating row the income statement also carries is that figure at the
  row's weight. A working-capital row is the cash effect of its balances'
  change, less what was backed out.

Bills are followed from the first month checked, so a bill from before the
window that is paid inside it shows as an unexplained row. Check from the
first month of the books to avoid that.

## What is not checked yet

- Cash-flow-only rows: the supplemental disclosures (non-cash acquisitions,
  capital expenditures not yet paid, interest and taxes paid) and
  `OtherNoncashIncomeExpense`.
- Working-capital rows of the consolidated statement in a month a member
  joins or leaves. The two dates hold different entities, so the change in a
  balance is not a flow.
- Which equity row carries the period's earnings. Total equity is checked;
  the split between rows is not.
- Reports of record (a filed report's stamped statements). Only the live
  statements are read.

## Layout

| File | Holds |
|---|---|
| `expected.py` | the trial balance folded by concept and by trait |
| `cashflow.py` | the cash flow rows from the journal lines and the taxonomy's arcs |
| `checks.py` | the comparisons |
| `__main__.py` | the API reads, the entity and month loop, the grid |
