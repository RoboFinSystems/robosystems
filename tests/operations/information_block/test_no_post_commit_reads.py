"""Writes must not read their ORM instance back after committing it.

`session.commit()` expires every instance in the session — the extensions
`sessionmaker` does not set `expire_on_commit=False` — so an attribute access
after the commit issues a refresh SELECT. The commit has already returned the
connection to the pool, so that SELECT runs on whichever connection comes back
out, and `extensions_session` resets `search_path` to `public` in its `finally`.
A refresh that lands on a reset connection resolves `structures` to the public
schema, finds no row, and raises `ObjectDeletedError`.

The caller sees a 500 for a write that committed successfully — the same
disease as reporting a committed event-block write as a 500, in a different
path. It is intermittent by construction, because it depends on which pooled
connection serves the refresh, which is why it survived until a 22,288-line
demo run happened to hit it.

The fix is to read what the response needs off the flush, before committing.
This test guards the shape rather than the symptom: a timing-dependent bug
cannot be reproduced reliably enough for an integration test to be the guard.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

# Modules whose writes build a response from an ORM instance.
GUARDED = [
  "robosystems/operations/information_block/rollforward.py",
  "robosystems/operations/information_block/forecast.py",
  "robosystems/operations/roboledger/commands/schedules.py",
]

REPO_ROOT = Path(__file__).resolve().parents[3]


def _is_session_commit(node: ast.stmt) -> bool:
  return (
    isinstance(node, ast.Expr)
    and isinstance(node.value, ast.Call)
    and isinstance(node.value.func, ast.Attribute)
    and node.value.func.attr == "commit"
    and isinstance(node.value.func.value, ast.Name)
    and node.value.func.value.id == "session"
  )


def _orm_bound_names(func: ast.AST) -> set[str]:
  """Locals bound to a session-tracked ORM instance.

  Only these expire on commit. A `Row` from `session.execute(text(...))` and a
  plain dict do not, so flagging every attribute read after a commit would bury
  the real defect in false positives — `period_row.cnt` is safe and appears
  right next to `structure.name`, which is not.
  """
  loaders = {"get", "first", "one", "one_or_none", "scalar_one", "scalar_one_or_none"}
  names: set[str] = set()

  for node in ast.walk(func):
    if not isinstance(node, ast.Assign) or not node.targets:
      continue
    target = node.targets[0]
    if not isinstance(target, ast.Name):
      continue

    value = node.value
    # `Structure(...)` — a model constructor, by naming convention.
    if (
      isinstance(value, ast.Call)
      and isinstance(value.func, ast.Name)
      and value.func.id[:1].isupper()
    ) or (
      isinstance(value, ast.Call)
      and isinstance(value.func, ast.Attribute)
      and value.func.attr in loaders
    ):
      names.add(target.id)

  return names


def _orm_attribute_loads(node: ast.AST, orm_names: set[str]) -> list[str]:
  """`x.y` reads in `node` where `x` is a session-tracked ORM instance."""
  found = []
  for child in ast.walk(node):
    if not isinstance(child, ast.Attribute) or not isinstance(child.ctx, ast.Load):
      continue
    if not isinstance(child.value, ast.Name):
      continue
    if child.value.id not in orm_names or child.attr.startswith("__"):
      continue
    found.append(f"{child.value.id}.{child.attr}")
  return found


def _violations(path: Path) -> list[tuple[str, int, str]]:
  """ORM attribute loads that appear after a `session.commit()` in the same block."""
  tree = ast.parse(path.read_text())
  out = []

  for func in ast.walk(tree):
    if not isinstance(func, ast.FunctionDef | ast.AsyncFunctionDef):
      continue

    orm_names = _orm_bound_names(func)
    if not orm_names:
      continue

    for node in ast.walk(func):
      for field in ("body", "orelse", "finalbody"):
        block = getattr(node, field, None)
        if not isinstance(block, list):
          continue

        committed = False
        for stmt in block:
          if _is_session_commit(stmt):
            committed = True
            continue
          if not committed:
            continue
          for name in _orm_attribute_loads(stmt, orm_names):
            out.append((str(path), getattr(stmt, "lineno", 0), name))

  return out


@pytest.mark.unit
@pytest.mark.parametrize("relative_path", GUARDED)
def test_no_orm_attribute_read_after_commit(relative_path: str) -> None:
  path = REPO_ROOT / relative_path
  assert path.exists(), f"guarded module moved: {relative_path}"

  violations = _violations(path)

  assert not violations, (
    "ORM attribute read after session.commit() in "
    + f"{relative_path}:\n"
    + "\n".join(f"  line {line}: {name}" for _, line, name in violations)
    + "\n\nThe commit expires the instance, so this issues a refresh SELECT on a "
    "pooled connection whose search_path may be `public` — the row resolves to "
    "the wrong schema and a committed write returns a 500. Read the value off "
    "the flush, before the commit:\n"
    "    session.flush()\n"
    "    structure_id = structure.id\n"
    "    session.commit()\n"
    "    return structure_id"
  )
