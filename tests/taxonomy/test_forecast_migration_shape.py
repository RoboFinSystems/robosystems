"""Shape tests for the forecast/scenario plumbing across the migration surface.

Static checks — no DB round-trip (the extensions-DB migration harness
lives in the dev loop). Locks down the six-way CHECK/vocabulary sync
that a typo in any one file would silently drift:

- Migration 0024's widened constants admit ``'forecast'`` (block_type)
  and ``'rs-driver'`` (element source), and its narrow constants don't.
- Migration 0002's fresh-path source CHECK admits ``'rs-driver'`` (its
  seed reads the current frameworks/ manifest, which now lists
  rs-driver — without the widen a fresh DB CheckViolations at 0002).
- ``_widen_library_checks`` (fresh tenant provisioning) carries both.
- The SQLAlchemy models carry both.
- The rs-gaap@v1 manifest lists rs-driver at ordinal 15.
"""

from __future__ import annotations

import importlib.util as _util
import json
from pathlib import Path

_VERSIONS = (
  Path(__file__).resolve().parents[2] / "migrations" / "extensions" / "versions"
)


def _load_migration(filename: str, name: str):
  spec = _util.spec_from_file_location(name, _VERSIONS / filename)
  assert spec is not None and spec.loader is not None
  module = _util.module_from_spec(spec)
  spec.loader.exec_module(module)
  return module


mig_0002 = _load_migration("0002_taxonomy_library.py", "mig_0002_forecast_shape")
mig_0024 = _load_migration("0024_forecast.py", "mig_0024")


class TestMigration0024Constants:
  def test_widened_block_type_admits_forecast(self) -> None:
    assert "'forecast'" in mig_0024._BLOCK_TYPE_WIDENED
    assert "'forecast'" not in mig_0024._BLOCK_TYPE_ORIGINAL

  def test_widened_source_admits_rs_driver(self) -> None:
    assert "'rs-driver'" in mig_0024._SOURCE_WIDENED
    # The downgrade target ALSO admits 'rs-driver' — fresh chains seed
    # the catalog at 0002 (before this migration exists), so narrowing
    # it out would break a fresh-chain downgrade. Deployed downgrades
    # delete the package content first, so the value is unused there.
    assert "'rs-driver'" in mig_0024._SOURCE_ORIGINAL

  def test_package_identity(self) -> None:
    assert mig_0024._PACKAGE_STANDARD == "rs-driver"
    assert mig_0024._PACKAGE_VERSION == "v1"
    assert mig_0024._PACKAGE_ORDINAL == 15

  def test_original_source_matches_0022_widened(self) -> None:
    """0024's narrow target IS 0022's widened state — the downgrade
    restores exactly the pre-0024 vocabulary."""
    mig_0022 = _load_migration("0022_metrics.py", "mig_0022_forecast_shape")
    assert mig_0024._SOURCE_ORIGINAL.replace(" ", "").replace(
      "\n", ""
    ) == mig_0022._SOURCE_WIDENED.replace(" ", "").replace("\n", "")


class TestFreshPathParity:
  def test_0002_source_check_admits_rs_driver(self) -> None:
    """0002's seed is dynamic (reads the current manifest) — a fresh DB
    inserts rs-driver elements AT 0002, before 0024's widen ever runs."""
    assert "'rs-driver'" in mig_0002._WIDENED_ELEMENT_SOURCE_CHECK

  def test_0002_adds_item_type_before_the_seed(self) -> None:
    """The seed runs the CURRENT library_creator, whose element upsert
    writes item_type (M-2) — the column historically arrived at 0021,
    so 0002 must pre-create it or every fresh chain fails mid-seed
    (the 2026-07-23 dagster-daemon rebuild failure)."""
    import inspect

    source = inspect.getsource(mig_0002.upgrade)
    add = source.find("ADD COLUMN IF NOT EXISTS item_type")
    seed = source.find("for seed_path in SEED_FILES")
    assert add != -1, "0002.upgrade no longer pre-creates elements.item_type"
    assert seed != -1, "0002.upgrade seed loop moved — update this anchor"
    assert add < seed, "item_type add must precede the seed passes"

  def test_every_source_check_reinstall_admits_rs_driver(self) -> None:
    """Migrations between the 0002 seed and the 0024 widen that DROP+ADD
    check_element_source must admit 'rs-driver' — fresh chains carry the
    rows from 0002, so a narrower re-install CheckViolations mid-chain
    (0007 and 0022 both bit during the fresh-chain verification)."""
    mig_0007 = _load_migration("0007_frameworks_bridges.py", "mig_0007_forecast_shape")
    mig_0022 = _load_migration("0022_metrics.py", "mig_0022_source_shape")
    assert "'rs-driver'" in mig_0007._WIDENED_ELEMENT_SOURCE_CHECK
    assert "'rs-driver'" in mig_0022._SOURCE_WIDENED

  def test_provisioning_widens_match(self) -> None:
    """Fresh tenant schemas get their CHECKs from _widen_library_checks,
    not from the migration chain — it must carry both new values."""
    import inspect

    from robosystems.db import extensions as extensions_db

    source = inspect.getsource(extensions_db._widen_library_checks)
    assert "'forecast'" in source
    assert "'rs-driver'" in source

  def test_models_match(self) -> None:
    import inspect

    from robosystems.models.extensions import element as element_module
    from robosystems.models.extensions import structure as structure_module

    assert "'forecast'" in inspect.getsource(structure_module)
    assert "'rs-driver'" in inspect.getsource(element_module)

  def test_manifest_lists_rs_driver_at_ordinal_15(self) -> None:
    manifest_path = (
      Path(__file__).resolve().parents[2] / "frameworks" / "rs-gaap" / "v1.json"
    )
    manifest = json.loads(manifest_path.read_text())
    by_standard = {p["standard"]: p for p in manifest["packages"]}
    assert "rs-driver" in by_standard
    assert by_standard["rs-driver"]["ordinal"] == 15
    assert by_standard["rs-driver"]["version"] == "v1"


class TestScenarioAxisDDL:
  def test_scenario_axis_helpers_cover_column_fk_index(self) -> None:
    """The 0024 scenario-axis DDL adds the column, the CASCADE FK, and
    the partial index — and the downgrade drops all three."""
    import inspect

    add = inspect.getsource(mig_0024._add_scenario_axis)
    assert "scenario_id" in add
    assert "fk_fact_sets_scenario" in add
    assert "idx_fact_sets_scenario" in add
    assert "CASCADE" in add
    drop = inspect.getsource(mig_0024._drop_scenario_axis)
    assert "scenario_id" in drop
    assert "fk_fact_sets_scenario" in drop
    assert "idx_fact_sets_scenario" in drop

  def test_model_carries_scenario_column_and_cascade(self) -> None:
    from robosystems.models.extensions.roboledger.fact_set import FactSet

    column = FactSet.__table__.columns["scenario_id"]
    assert column.nullable is True
    fks = list(column.foreign_keys)
    assert len(fks) == 1
    assert fks[0].ondelete == "CASCADE"
    index_names = {idx.name for idx in FactSet.__table__.indexes}
    assert "idx_fact_sets_scenario" in index_names
