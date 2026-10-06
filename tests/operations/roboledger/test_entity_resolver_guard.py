"""Only ``entity_scope`` decides which entity an operation acts on. Eleven
local copies of "the earliest entity" drifted apart before the single
resolver; this keeps a twelfth from appearing."""

from __future__ import annotations

import ast
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


_LEDGER_ROWS = {"Entry", "Event", "Transaction"}


def _ledger_row_names(tree: ast.Module) -> set[str]:
  """The ledger models a module imports, by the name it binds them to."""
  names: set[str] = set()
  for node in ast.walk(tree):
    if isinstance(node, ast.ImportFrom) and (node.module or "").startswith(
      "robosystems.models.extensions"
    ):
      names.update(
        alias.asname or alias.name for alias in node.names if alias.name in _LEDGER_ROWS
      )
  return names


def test_every_ledger_row_is_built_with_its_entity():
  """An entry, event or transaction built without ``entity_id`` is a row no
  entity-scoped read returns."""
  offenders = []
  for path in _PACKAGE.rglob("*.py"):
    tree = ast.parse(path.read_text())
    rows = _ledger_row_names(tree)
    if not rows:
      continue
    for node in ast.walk(tree):
      if (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id in rows
        and not any(keyword.arg == "entity_id" for keyword in node.keywords)
      ):
        offenders.append(f"{path.relative_to(_PACKAGE.parent)}:{node.lineno}")
  assert not offenders, "Ledger rows built without entity_id: " + ", ".join(offenders)


_GUARDS = _PACKAGE / "operations" / "roboledger" / "commands" / "_guards.py"
_CALENDAR_CHECKS = {"assert_period_not_closed", "closed_periods"}


def test_every_closed_period_check_names_its_entity():
  """A closed-month check with no entity reads the group parent's calendar,
  which is the wrong one for a subsidiary's write."""
  offenders = []
  for path in _PACKAGE.rglob("*.py"):
    if path == _GUARDS:
      continue
    for node in ast.walk(ast.parse(path.read_text())):
      if (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id in _CALENDAR_CHECKS
        and not any(keyword.arg == "entity_id" for keyword in node.keywords)
      ):
        offenders.append(f"{path.relative_to(_PACKAGE.parent)}:{node.lineno}")
  assert not offenders, "Closed-period checks with no entity_id: " + ", ".join(
    offenders
  )
