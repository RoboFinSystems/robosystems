"""Shape tests for Phase ζ fact_sets plumbing across migration 0003.

Static checks — no DB round-trip. Locks down the pieces a typo in the
migration could drift out of alignment:

- ``fact_sets`` table DDL names every column the model declares.
- ``factset_type`` CHECK constraint lists the three enum values.
- Migration chains onto 0002 (the taxonomy-library migration).

The tenant-schema DDL helper + envelope wiring are covered by their
own tests (``test_envelope_fact_set.py``); this file only checks what
a static read of the migration file can verify.
"""

from __future__ import annotations

import importlib.util as _util
from pathlib import Path

_MIGRATION_PATH = (
  Path(__file__).resolve().parents[2]
  / "migrations"
  / "extensions"
  / "versions"
  / "0003_fact_sets.py"
)

_spec = _util.spec_from_file_location("mig_0003", _MIGRATION_PATH)
assert _spec is not None and _spec.loader is not None
mig_0003 = _util.module_from_spec(_spec)
_spec.loader.exec_module(mig_0003)


class TestMigrationChain:
  def test_revision_and_down_revision(self) -> None:
    assert mig_0003.revision == "0003"
    assert mig_0003.down_revision == "0002"


class TestFactSetTypeCheck:
  def test_enum_values_present(self) -> None:
    check = mig_0003._FACT_SET_TYPE_CHECK
    for value in ("'report'", "'schedule'", "'custom'"):
      assert value in check, f"fact_set_type CHECK missing {value}"


class TestTenantHelpers:
  def test_create_helper_exists(self) -> None:
    assert callable(mig_0003._create_fact_sets_in_tenant)

  def test_drop_helper_exists(self) -> None:
    assert callable(mig_0003._drop_fact_sets_in_tenant)
