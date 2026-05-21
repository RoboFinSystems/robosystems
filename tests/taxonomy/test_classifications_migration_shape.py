"""Shape tests for Phase epsilon association-classifications plumbing.

Static checks — no DB round-trip. The extensions-DB migration harness
lives in the dev loop (``just migrate-up extensions`` + CI dry runs);
these tests lock down the pieces that a typo in the migration file
could otherwise drift out of alignment:

- Immutability trigger covers ``association_classifications``.
- Tenant writer's ``_ASSOC_CLASSIFICATION_COLS`` names every column the
  tenant-schema table expects.
- :class:`CopyStats` exposes the ``association_classifications`` field
  alongside the other tenant-copy counters.
"""

from __future__ import annotations

import importlib.util as _util
from pathlib import Path

from robosystems.db import extensions as extensions_db
from robosystems.taxonomy.writer import (
  _ASSOC_CLASSIFICATION_COLS,
  CopyStats,
)

_MIGRATION_PATH = (
  Path(__file__).resolve().parents[2]
  / "migrations"
  / "extensions"
  / "versions"
  / "0002_taxonomy_library.py"
)

_spec = _util.spec_from_file_location("mig_0002", _MIGRATION_PATH)
assert _spec is not None and _spec.loader is not None
mig_0002 = _util.module_from_spec(_spec)
_spec.loader.exec_module(mig_0002)


class TestImmutableTables:
  def test_association_classifications_is_immutable_in_tenant_schemas(self) -> None:
    """Library-seeded association classifications must be trigger-protected
    against UPDATE/DELETE — same contract as element_traits /
    rules / structures."""
    assert "association_classifications" in mig_0002._IMMUTABLE_TABLES

  def test_fresh_tenant_provisioning_installs_same_immutability_trigger(self) -> None:
    """Fresh schemas use ``provision_tenant_schema`` instead of the migration
    backfill helper, so its trigger table list must stay in sync."""
    assert "association_classifications" in extensions_db._LIBRARY_IMMUTABLE_TABLES


class TestTenantWriterCols:
  def test_cols_include_composite_pk_and_provenance(self) -> None:
    for col in (
      "association_id",
      "classification_id",
      "is_primary",
      "confidence",
      "source",
      "created_at",
      "updated_at",
      "created_by",
    ):
      assert col in _ASSOC_CLASSIFICATION_COLS, (
        f"_ASSOC_CLASSIFICATION_COLS missing {col!r}"
      )


class TestCopyStats:
  def test_has_association_classifications_field(self) -> None:
    """Without this counter, the migration's backfill log loses visibility
    into whether association classifications actually copied into tenant
    schemas."""
    stats = CopyStats(
      taxonomies=1,
      elements=1,
      element_labels=1,
      element_references=1,
      structures=1,
      associations=1,
      traits=1,
      element_traits=1,
      classifications=1,
      association_classifications=7,
      rules=1,
      reporting_style_networks=1,
    )
    assert stats.association_classifications == 7
    assert stats.total == 18
