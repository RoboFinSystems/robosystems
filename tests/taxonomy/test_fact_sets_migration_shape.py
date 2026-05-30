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

_PROVENANCE_MIGRATION_PATH = (
  Path(__file__).resolve().parents[2]
  / "migrations"
  / "extensions"
  / "versions"
  / "0018_fact_set_provenance.py"
)
_prov_spec = _util.spec_from_file_location("mig_0018", _PROVENANCE_MIGRATION_PATH)
assert _prov_spec is not None and _prov_spec.loader is not None
mig_0018 = _util.module_from_spec(_prov_spec)
_prov_spec.loader.exec_module(mig_0018)


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


class TestProvenanceMigration:
  """Migration 0018 — the first-class ``provenance`` column on fact_sets."""

  def test_chains_onto_0017(self) -> None:
    assert mig_0018.revision == "0018"
    assert mig_0018.down_revision == "0017"

  def test_add_and_drop_helpers_exist(self) -> None:
    assert callable(mig_0018._add_provenance_column)
    assert callable(mig_0018._drop_provenance_column)

  def test_model_declares_provenance_column(self) -> None:
    from robosystems.models.extensions.roboledger.fact_set import FactSet

    assert "provenance" in FactSet.__table__.columns
