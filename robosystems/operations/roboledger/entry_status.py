"""Which entry statuses are *in the books* — the one definition, for every reader.

**The invariant**: an entry's effect belongs in a balance when it has landed, and
it has landed at `posted` *or* `reversed`. A reversed original did not leave the
books; `reverse_journal_entry` writes a full offsetting entry against it, and the
pair nets to zero only if **both** halves are summed.

This module exists because that was not true. Every balance read filtered
`status = 'posted'`, which drops the original while its offset — an ordinary
`posted` row — stays in. Reversing a $5,000 rent accrual left rent expense at
**-$5,000** and cash at **+$5,000**. Grand debits still equalled grand credits, so
the trial balance footed and nothing raised a flag; the defect reached the
materialized graph through `is_live`, and from there every statement, fact grid
and published report.

The two halves were each locally defensible — `account_rollups` documented
excluding `reversed` on purpose, and it *would* be right if the reversal were the
only offsetting row. It is not. Ten SQL reads and two materializer projections
each restated the predicate, so the one that needed to change could not be
found by changing any of the others.

So the predicate is defined once, here, and interpolated. A reader that wants a
different question — "show me entries not yet corrected" — asks it explicitly
against `status`, rather than by re-deriving this one and drifting from it.

`draft` is excluded and stays excluded: a draft has not landed, and entries of
voided events remain `draft`.
"""

from __future__ import annotations

from sqlalchemy import BindParameter, bindparam

# The statuses whose line items belong in a balance. Kept as a frozenset for
# Python-side membership tests (`entry.status in LANDED_ENTRY_STATUSES`) —
# `event_block.commands` imports this for its retraction fence, which asks the
# same question in the other direction: retracting an event that produced one of
# these would strand a row the books depend on.
LANDED_ENTRY_STATUSES: frozenset[str] = frozenset({"posted", "reversed"})

# The same predicate as a SQL fragment, for string-built SQL that has no bind
# parameters — the materializer's DuckDB projections. Rendered from the set above
# so the two can never disagree; sorted so the emitted SQL is stable across runs
# (frozenset iteration order is not).
#
# It carries its own parentheses so a call site cannot half-apply it, and
# contains only literals from this module — never caller input.
LANDED_ENTRY_SQL: str = "({})".format(
  ", ".join(f"'{status}'" for status in sorted(LANDED_ENTRY_STATUSES))
)

# The bind-parameter form, for the `text()` reads. Preferred over interpolating
# `LANDED_ENTRY_SQL` into those: several of them are plain triple-quoted strings
# whose SQL contains literal braces, so making them f-strings to interpolate a
# constant would mean escaping unrelated SQL and inviting a different class of
# mistake. `expanding=True` renders the IN list at execution.
#
# A factory rather than a shared instance: a `BindParameter` is bound into each
# `text()` construct it is attached to, and one object threaded through a dozen
# statements is a coupling nobody asked for.
#
# The value is baked in, so call sites keep executing with their own parameter
# dicts and never pass this one.
LANDED_ENTRY_PARAM = "landed_entry_statuses"


def landed_entry_bindparam() -> BindParameter:
  """The `:landed_entry_statuses` IN-list, ready to `.bindparams()` onto a read."""
  return bindparam(
    LANDED_ENTRY_PARAM, value=sorted(LANDED_ENTRY_STATUSES), expanding=True
  )


# The DuckDB/LadybugDB boolean projection of the same rule, for the materializer.
# `is_live` on `Entry` and `LineItem` means exactly "in the books", which is what
# the MCP tool guidance and the AI Operator prompt tell callers it means — so it
# has to be this predicate and not a narrower one.
#
# Takes the column reference because the two projections qualify it differently
# (`status` on the Entry table, `e.status` across the LineItem join).
def landed_is_live_sql(status_column: str) -> str:
  """`is_live` for a column holding an entry status."""
  return f"({status_column} IN {LANDED_ENTRY_SQL})"


# A reversing entry is an entry like any other and is summed like any other — it
# is the half that offsets its original. This constant exists for the *reconcile*
# question instead: "does this schedule already have its closing entry for this
# period?" An auto-reversal carries the same `source_structure_id` as the accrual
# it reverses and is posted on the first day of the *next* period, so it lands
# inside the next period's window and answers that question wrongly. Readers
# asking it must exclude the type; readers summing money must not.
REVERSING_ENTRY_TYPE = "reversing"
