"""Structural: no balance read may restate the landed-entry predicate itself.

The reversal defect was not one wrong line. Ten SQL reads and two materializer
projections each spelled out `status = 'posted'`, so the invariant lived in
twelve places and was owned by none — changing the one that mattered could not be
found by changing any of the others, and the graph drifted from the OLTP reads
without a single test noticing.

`roboledger.entry_status` now owns it. This test is what keeps it owned: it fails
on a *new* hardcoded predicate rather than on the ones already fixed, which is
the only way a rule like this survives contact with the next contributor.

It is deliberately a sweep over files rather than a list of known call sites.
A list would need updating by whoever adds the thirteenth reader, which is
exactly the person who does not know the rule exists.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[3]

# Modules that read ledger money. A hardcoded status predicate here is the defect;
# elsewhere (a status transition, a guard on a specific state) it is legitimate.
SCANNED = [
  "robosystems/operations/roboledger/reads",
  "robosystems/operations/roboledger/reports",
  "robosystems/operations/extensions/materialize.py",
]

# `status = 'posted'` / `status == "posted"` / `status IN ('posted')`, in SQL or
# Python, qualified by a table alias or not.
HARDCODED = re.compile(
  r"""(?:\w+\.)?status\s*(?:=|==)\s*['"]posted['"]"""
  r"""|(?:\w+\.)?status\s+IN\s*\(\s*'posted'\s*\)""",
  re.IGNORECASE,
)

# The one file allowed to name the statuses: the module that defines them.
ALLOWED = {"robosystems/operations/roboledger/entry_status.py"}


def _python_files():
  for target in SCANNED:
    path = REPO_ROOT / target
    if path.is_file():
      yield path
    else:
      yield from sorted(path.rglob("*.py"))


def test_no_balance_read_hardcodes_the_landed_predicate():
  offenders = []
  for path in _python_files():
    rel = path.relative_to(REPO_ROOT).as_posix()
    if rel in ALLOWED:
      continue
    for lineno, line in enumerate(path.read_text().splitlines(), 1):
      if HARDCODED.search(line):
        offenders.append(f"{rel}:{lineno}: {line.strip()}")

  assert not offenders, (
    "These read ledger money but restate the landed-entry predicate instead of "
    "asking `roboledger.entry_status`. A reversed original is in the books — its "
    "reversing entry only nets it to zero if both halves are summed, so "
    "`status = 'posted'` alone reports the balance with the sign flipped.\n  "
    + "\n  ".join(offenders)
  )


def test_landed_predicate_covers_reversed_and_excludes_draft():
  """The definition itself, so a well-meaning narrowing fails here first."""
  from robosystems.operations.roboledger.entry_status import (
    LANDED_ENTRY_SQL,
    LANDED_ENTRY_STATUSES,
    landed_is_live_sql,
  )

  assert frozenset({"posted", "reversed"}) == LANDED_ENTRY_STATUSES
  assert "draft" not in LANDED_ENTRY_STATUSES
  # The SQL and Python forms are rendered from one set and cannot disagree.
  assert LANDED_ENTRY_SQL == "('posted', 'reversed')"
  assert landed_is_live_sql("e.status") == "(e.status IN ('posted', 'reversed'))"
