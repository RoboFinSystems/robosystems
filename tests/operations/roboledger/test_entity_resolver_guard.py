"""Only ``entity_scope`` decides which entity an operation acts on. Eleven
local copies of "the earliest entity" drifted apart before the single
resolver; this keeps a twelfth from appearing."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

_PACKAGE = Path(__file__).resolve().parents[3] / "robosystems"
_RESOLVER = _PACKAGE / "operations" / "roboledger" / "entity_scope.py"

_ENTITY_PICKS = [
  re.compile(r"FROM\s+entities\s+ORDER\s+BY", re.IGNORECASE),
  re.compile(r"FROM\s+entities\s+WHERE\s+is_parent", re.IGNORECASE),
  re.compile(r"order_by\(\s*Entity\.created_at"),
  re.compile(r"Entity\.is_parent\.is_\(\s*True\s*\)"),
]


def test_no_entity_pick_outside_the_resolver():
  offenders = []
  for path in _PACKAGE.rglob("*.py"):
    if path == _RESOLVER:
      continue
    source = path.read_text()
    for pattern in _ENTITY_PICKS:
      for match in pattern.finditer(source):
        line = source.count("\n", 0, match.start()) + 1
        offenders.append(f"{path.relative_to(_PACKAGE.parent)}:{line}")
  assert not offenders, (
    "Resolve the entity through operations.roboledger.entity_scope instead: "
    + ", ".join(offenders)
  )
