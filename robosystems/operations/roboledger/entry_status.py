"""Which entry statuses are *in the books*: the one definition, for every reader.

An entry has landed at `posted` *or* `reversed`. A reversed original stays in
the books because `reverse_journal_entry` writes a full offsetting entry
against it, and the pair nets to zero only if both halves are summed;
filtering on `posted` alone leaves the offset without its original and
misstates both accounts while the trial balance still foots. `draft` has not
landed (entries of voided events stay `draft`).

Readers interpolate these forms rather than restating the predicate; a
different question ("entries not yet corrected") asks `status` explicitly.
"""

from __future__ import annotations

from sqlalchemy import BindParameter, bindparam

LANDED_ENTRY_STATUSES: frozenset[str] = frozenset({"posted", "reversed"})

# SQL fragment for string-built SQL without bind parameters (the materializer).
# Sorted for stable output; literals from this module only, never caller input.
LANDED_ENTRY_SQL: str = "({})".format(
  ", ".join(f"'{status}'" for status in sorted(LANDED_ENTRY_STATUSES))
)

# Bind-parameter form for `text()` reads; the value is baked in, so callers
# never pass it.
LANDED_ENTRY_PARAM = "landed_entry_statuses"


def landed_entry_bindparam() -> BindParameter:
  """The `:landed_entry_statuses` IN-list, ready to `.bindparams()` onto a read."""
  return bindparam(
    LANDED_ENTRY_PARAM, value=sorted(LANDED_ENTRY_STATUSES), expanding=True
  )


# `is_live` on materialized Entry and LineItem means exactly "in the books".
def landed_is_live_sql(status_column: str) -> str:
  """`is_live` for a column holding an entry status."""
  return f"({status_column} IN {LANDED_ENTRY_SQL})"


# A schedule's own entry vs. the auto-reversal generated against it. Key on the
# reversal link, never the entry type: a schedule may legitimately post primary
# entries typed "reversing", and filtering those out drafts duplicates.
PRIMARY_ENTRY_SQL = "reversal_of IS NULL"
GENERATED_REVERSAL_SQL = "reversal_of IS NOT NULL"
