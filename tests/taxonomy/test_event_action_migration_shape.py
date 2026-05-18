"""Shape tests for migration 0012 event_action column + CHECK constraint.

Static checks — no DB round-trip. Locks down the pieces that, if they
drift, cause spurious alembic autogenerate diffs or silent CHECK
mismatches between the model and DB.

Critical invariant: the migration's hardcoded `_EVENT_ACTION_CHECK`
string must be byte-identical to what the SQLAlchemy model emits via
`sorted(EVENT_ACTIONS)`. Alembic compares constraint expressions
textually, so any ordering or whitespace mismatch produces a noisy
"alter check_event_action" diff on every subsequent migration.
"""

from __future__ import annotations

import importlib.util as _util
from pathlib import Path

from robosystems.models.extensions.roboledger.event import (
  _EVENT_ACTION_CHECK as MODEL_CHECK,
)
from robosystems.models.extensions.roboledger.event import (
  EVENT_ACTIONS,
)

_MIGRATION_PATH = (
  Path(__file__).resolve().parents[2]
  / "migrations"
  / "extensions"
  / "versions"
  / "0012_event_action.py"
)

_spec = _util.spec_from_file_location("mig_0012", _MIGRATION_PATH)
assert _spec is not None and _spec.loader is not None
mig_0012 = _util.module_from_spec(_spec)
_spec.loader.exec_module(mig_0012)


class TestMigrationChain:
  def test_revision_and_down_revision(self) -> None:
    assert mig_0012.revision == "0012"
    assert mig_0012.down_revision == "0011"


class TestEventActionCheck:
  def test_all_19_verbs_present(self) -> None:
    check = mig_0012._EVENT_ACTION_CHECK
    for verb in EVENT_ACTIONS:
      assert f"'{verb}'" in check, f"CHECK missing canonical verb {verb!r}"

  def test_no_extra_verbs(self) -> None:
    import re

    # Extract every single-quoted token from the migration string and
    # confirm none is outside the canonical set.
    verbs_in_check = set(re.findall(r"'([a-zA-Z]+)'", mig_0012._EVENT_ACTION_CHECK))
    extras = verbs_in_check - EVENT_ACTIONS
    assert not extras, f"CHECK contains non-canonical verbs: {sorted(extras)}"

  def test_migration_string_matches_model_string_exactly(self) -> None:
    """Byte-equality guard: the migration's hardcoded IN clause must
    match what the SQLAlchemy model emits via sorted(EVENT_ACTIONS).
    Any drift here means alembic autogenerate produces spurious
    'alter check_event_action' diffs on every subsequent migration."""
    assert mig_0012._EVENT_ACTION_CHECK == MODEL_CHECK, (
      "Migration 0012 CHECK string drift from SQLAlchemy model.\n"
      f"  migration: {mig_0012._EVENT_ACTION_CHECK!r}\n"
      f"  model:     {MODEL_CHECK!r}\n"
      "  Fix: both must list the 19 verbs in alphabetical order "
      "(sorted(EVENT_ACTIONS)). The migration is hardcoded; the model "
      "builds via sorted(). Keep them byte-identical."
    )


class TestTenantHelpers:
  def test_add_helper_exists(self) -> None:
    assert callable(mig_0012._add_in_tenant)

  def test_drop_helper_exists(self) -> None:
    assert callable(mig_0012._drop_in_tenant)
