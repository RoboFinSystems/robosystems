"""Structural guard: every code path that deletes ledger rows is on the record.

A landed entry, line or transaction leaves the books by reversal or
supersession, never by ``DELETE`` (spec ``posted-rows-permanent``); the tenant
triggers refuse the delete at the database. This catches a new delete path in
CI, before a tenant's trigger does in production: every file that deletes from
``entries``, ``line_items`` or ``transactions`` is listed below with what
confines it to rows that never landed.

Adding a file here is a decision on the record, not a way to quiet the test:
name the predicate that keeps the delete off landed rows.
"""

from __future__ import annotations

import re
from pathlib import Path

import robosystems

_ROOT = Path(robosystems.__file__).parent

_LEDGER_DELETE = re.compile(
  r"DELETE FROM (entries|line_items|transactions)\b"
  r"|delete\((Entry|LineItem|Transaction)\)"
  r"|query\((Entry|LineItem|Transaction)\)(?:(?!\n\s*\n).){0,400}?\.delete\(",
  re.S,
)

_CONFINED: dict[str, str] = {
  "operations/extensions/loader.py": (
    "Full-rebuild wipe: entries `status == 'draft'`, transactions `status != "
    "'posted'`, and the sync stops if an uncommitted event owns a landed entry."
  ),
  "operations/roboledger/commands/connections.py": (
    "Disconnect: draft entries, and transactions not posted and referenced by no entry."
  ),
  "operations/roboledger/commands/journal_entries.py": (
    "Replacing a draft's lines; the edit refuses an entry that is not a draft."
  ),
  "operations/roboledger/commands/schedules.py": (
    "Drafts of voided obligations, and a schedule's drafts on delete, which "
    "refuses while landed entries exist."
  ),
  "operations/roboledger/schedules/service.py": (
    "Truncate deletes drafts after the cutoff and refuses over a terminal entry; "
    "the single-draft delete refuses a non-draft."
  ),
}


def _deleting_files() -> set[str]:
  found = set()
  for path in _ROOT.rglob("*.py"):
    relative = path.relative_to(_ROOT).as_posix()
    if relative.startswith("models/"):
      continue
    if _LEDGER_DELETE.search(path.read_text()):
      found.add(relative)
  return found


def test_every_ledger_delete_is_confined_to_rows_that_never_landed():
  unlisted = _deleting_files() - set(_CONFINED)
  assert not unlisted, (
    f"{sorted(unlisted)} delete ledger rows. A landed row is reversed or "
    "superseded, never deleted; confine the delete to drafts and add the file "
    "to _CONFINED with the predicate that does it."
  )


def test_the_record_lists_no_file_that_no_longer_deletes():
  stale = set(_CONFINED) - _deleting_files()
  assert not stale, f"{sorted(stale)} no longer delete ledger rows; drop them."
