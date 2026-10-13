"""Every door that settles a reconciling item is pinned to its preview.

A reconciling item is settled against the payload the operator previewed: a
door carries the preview's `expected_drift_detected_at`, and
`resolve_reconciling_item` refuses when the item was flagged again since. The
rule has been broken twice, both times by a door added after it (an
alignment entry, then the commit of a captured one), and nothing failed
because each door's own tests settled successfully.

This test guards the shape. It finds every site that can settle an item, by
reference to the settling functions and by clearing `payload_drift`, and
fails on any site not listed here. A new door is listed with the test that
shows it refuses a stale stamp, and that test must exist.
"""

from __future__ import annotations

import ast
import re
from collections.abc import Iterator
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[4]
PACKAGE = REPO_ROOT / "robosystems"
ITEMS_DB_TESTS = "tests/operations/roboledger/commands/test_reconciling_items_db.py"

SETTLING = {"resolve_reconciling_item", "_acknowledge_settled_items"}

# (module, enclosing function) → the stale-stamp tests that pin the door, or
# an empty list for the relay every entry door passes through.
DOORS: dict[tuple[str, str], list[str]] = {
  # resolve-reconciling-item: the REST operation and the MCP tool.
  ("robosystems/routers/extensions/roboledger/operations/ledger.py", "<module>"): [
    f"{ITEMS_DB_TESTS}::test_a_reflag_between_preview_and_resolve_is_refused",
    f"{ITEMS_DB_TESTS}::test_a_reflag_after_the_preview_is_refused",
  ],
  # A journal entry created naming the items it settles.
  ("robosystems/operations/event_block/commands.py", "create_event_block_in_session"): [
    f"{ITEMS_DB_TESTS}::test_an_alignment_from_a_preview_the_item_has_moved_past_is_refused",
  ],
  # A captured journal entry committed naming the items it settles.
  ("robosystems/operations/event_block/commands.py", "update_event_block"): [
    f"{ITEMS_DB_TESTS}::test_an_alignment_committed_from_a_preview_the_item_has_moved_past_is_refused",
  ],
  # The relay both entry doors share; it forwards each item's stamp.
  ("robosystems/operations/event_block/commands.py", "_acknowledge_settled_items"): [],
}

# Sites that clear the flag. Only the settlement is a decision on a preview;
# the sync's own clears follow the source back to the posted payload.
CLEARS = {
  (
    "robosystems/operations/roboledger/commands/reconciling_items.py",
    "resolve_reconciling_item",
  ),
  # QuickBooks reverted the change before anyone settled it.
  ("robosystems/operations/extensions/loader.py", "_capture_transactions_as_events"),
  # The change was this ledger's own entry coming back round.
  ("robosystems/operations/extensions/loader.py", "_match_round_trips"),
}

_CLEARING_SQL = re.compile(r"payload_drift\s*=\s*false", re.IGNORECASE)


def _scoped(node: ast.AST, scope: str = "<module>") -> Iterator[tuple[ast.AST, str]]:
  """Every node with the name of the function it sits in."""
  for child in ast.iter_child_nodes(node):
    yield child, scope
    inner = (
      child.name if isinstance(child, ast.FunctionDef | ast.AsyncFunctionDef) else scope
    )
    yield from _scoped(child, inner)


def _is_false(node: ast.AST | None) -> bool:
  return isinstance(node, ast.Constant) and node.value is False


def _settles(node: ast.AST) -> bool:
  if isinstance(node, ast.ImportFrom):
    return any(alias.name in SETTLING for alias in node.names)
  if isinstance(node, ast.Name):
    return node.id in SETTLING
  return isinstance(node, ast.Attribute) and node.attr in SETTLING


def _clears(node: ast.AST) -> bool:
  if isinstance(node, ast.Assign):
    return _is_false(node.value) and any(
      isinstance(t, ast.Attribute) and t.attr == "payload_drift" for t in node.targets
    )
  if isinstance(node, ast.keyword):
    return node.arg == "payload_drift" and _is_false(node.value)
  if isinstance(node, ast.Dict):
    return any(
      isinstance(k, ast.Constant) and k.value == "payload_drift" and _is_false(v)
      for k, v in zip(node.keys, node.values, strict=True)
    )
  return (
    isinstance(node, ast.Constant)
    and isinstance(node.value, str)
    and bool(_CLEARING_SQL.search(node.value))
  )


def _sites() -> tuple[set[tuple[str, str]], set[tuple[str, str]]]:
  doors: set[tuple[str, str]] = set()
  clears: set[tuple[str, str]] = set()
  for path in sorted(PACKAGE.rglob("*.py")):
    module = str(path.relative_to(REPO_ROOT))
    for node, scope in _scoped(ast.parse(path.read_text())):
      if _settles(node):
        doors.add((module, scope))
      if _clears(node):
        clears.add((module, scope))
  return doors, clears


@pytest.mark.unit
def test_every_door_that_settles_an_item_is_listed():
  doors, _clears = _sites()
  assert not doors - DOORS.keys(), (
    "A new site can settle a reconciling item. Pass the preview's "
    "expected_drift_detected_at through it, add a test that it refuses a "
    "stale stamp, and list it in DOORS with that test."
  )
  assert not DOORS.keys() - doors, "A listed door is gone; update DOORS."


@pytest.mark.unit
def test_only_the_settlement_and_the_sync_clear_the_flag():
  _doors, clears = _sites()
  assert clears == CLEARS, (
    "payload_drift is cleared somewhere new. Settle the item through "
    "resolve_reconciling_item, which checks the preview's stamp, instead."
  )


@pytest.mark.unit
@pytest.mark.parametrize(
  "test_id", sorted({test for tests in DOORS.values() for test in tests})
)
def test_each_doors_stale_stamp_test_exists(test_id):
  path, name = test_id.split("::")
  tree = ast.parse((REPO_ROOT / path).read_text())
  names = {
    node.name
    for node in ast.walk(tree)
    if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)
  }
  assert name in names
