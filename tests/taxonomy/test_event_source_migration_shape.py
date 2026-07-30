"""Shape tests for migration 0025 — dropping the Event.source CHECK.

Static checks — no DB round-trip. Locks down the pieces that, if they
drift, break the event-source registry cutover: the model must no
longer carry the CHECK (new tenant schemas provision from model
metadata), the migration must drop BOTH historical constraint names
(0005 raw DDL used ``check_{schema}_event_source``; the metadata path
used ``check_event_source``), and the downgrade's re-add string must be
byte-identical to 0005's original so a rollback recreates exactly what
shipped.
"""

from __future__ import annotations

import importlib.util as _util
import inspect
from pathlib import Path

from robosystems.models.extensions.roboledger.event import Event

_VERSIONS = (
  Path(__file__).resolve().parents[2] / "migrations" / "extensions" / "versions"
)


def _load(name: str, filename: str):
  spec = _util.spec_from_file_location(name, _VERSIONS / filename)
  assert spec is not None and spec.loader is not None
  module = _util.module_from_spec(spec)
  spec.loader.exec_module(module)
  return module


mig_0025 = _load("mig_0025", "0025_drop_event_source_check.py")
mig_0005 = _load("mig_0005", "0005_event_driven_ledger.py")


class TestMigrationChain:
  def test_revision_and_down_revision(self) -> None:
    assert mig_0025.revision == "0025"
    assert mig_0025.down_revision == "0024"


class TestModelNoLongerCarriesCheck:
  def test_check_event_source_removed_from_model(self) -> None:
    names = {c.name for c in Event.__table__.constraints}
    assert "check_event_source" not in names

  def test_idempotency_index_intact(self) -> None:
    names = {i.name for i in Event.__table__.indexes}
    assert "idx_events_source_external" in names


class TestDropCoversBothHistoricalNames:
  def test_drop_helper_removes_both_names(self) -> None:
    source = inspect.getsource(mig_0025._drop)
    assert '"check_event_source"' in source
    assert 'f"check_{schema}_event_source"' in source

  def test_upgrade_fans_out_to_public_and_tenants(self) -> None:
    source = inspect.getsource(mig_0025.upgrade)
    assert '"public"' in source
    assert "for_each_tenant_schema" in source


class TestDowngradeReAddsOriginalCheck:
  def test_check_string_byte_equals_0005(self) -> None:
    assert mig_0025._SOURCE_CHECK == mig_0005._SOURCE_CHECK, (
      "Migration 0025's downgrade re-add string drifted from 0005's "
      "original CHECK — a rollback would recreate a different constraint."
    )
