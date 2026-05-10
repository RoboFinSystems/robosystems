"""Shape tests for the Framework + Bridge tables seeded by migration 0007.

These tests ensure the framework manifest at
``robosystems/taxonomy/frameworks/rs-gaap-base/v1.json`` round-trips
through the migration into the public-schema rows that the rest of the
system queries — and that the row counts match the manifest's
declarations.

The disk manifest stays the source of truth for ``resolve_pin``; these
tests verify the migration successfully mirrors it into the DB so
admin tooling and API surfaces can query framework composition via
SQL/ORM.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from robosystems.db.extensions import extensions_session
from robosystems.models.extensions import (
  Bridge,
  Framework,
  FrameworkBridge,
  FrameworkPackage,
)

_MANIFEST_PATH = (
  Path(__file__).resolve().parents[2]
  / "robosystems"
  / "taxonomy"
  / "frameworks"
  / "rs-gaap-base"
  / "v1.json"
)


@pytest.fixture(scope="module")
def manifest() -> dict:
  return json.loads(_MANIFEST_PATH.read_text())


@pytest.mark.integration
class TestFrameworkSeeded:
  def test_default_framework_row_exists(self, manifest: dict) -> None:
    with extensions_session("library") as s:
      fw = s.query(Framework).filter_by(framework="rs-gaap-base", version="v1").one()
      assert fw.title == manifest["title"]
      assert fw.framework_type == manifest["framework_type"]
      assert fw.is_active is True

  def test_package_pin_count_matches_manifest(self, manifest: dict) -> None:
    with extensions_session("library") as s:
      fw = s.query(Framework).filter_by(framework="rs-gaap-base", version="v1").one()
      count = s.query(FrameworkPackage).filter_by(framework_id=fw.id).count()
      assert count == len(manifest["packages"])

  def test_required_vs_optional_package_split(self, manifest: dict) -> None:
    expected_required = sum(
      1 for p in manifest["packages"] if p.get("is_required", True)
    )
    expected_optional = len(manifest["packages"]) - expected_required

    with extensions_session("library") as s:
      fw = s.query(Framework).filter_by(framework="rs-gaap-base", version="v1").one()
      required = (
        s.query(FrameworkPackage)
        .filter_by(framework_id=fw.id, is_required=True)
        .count()
      )
      optional = (
        s.query(FrameworkPackage)
        .filter_by(framework_id=fw.id, is_required=False)
        .count()
      )
      assert required == expected_required
      assert optional == expected_optional

  def test_package_ordinal_preserved(self, manifest: dict) -> None:
    """Ordinal drives load order; must round-trip from manifest."""
    expected_by_std = {p["standard"]: p.get("ordinal", 0) for p in manifest["packages"]}
    with extensions_session("library") as s:
      fw = s.query(Framework).filter_by(framework="rs-gaap-base", version="v1").one()
      for fp in s.query(FrameworkPackage).filter_by(framework_id=fw.id).all():
        assert fp.ordinal == expected_by_std[fp.package_standard]


@pytest.mark.integration
class TestBridgeSeeded:
  def test_bridge_count_matches_manifest(self, manifest: dict) -> None:
    expected = len(manifest["bridges"])
    with extensions_session("library") as s:
      fw = s.query(Framework).filter_by(framework="rs-gaap-base", version="v1").one()
      count = s.query(FrameworkBridge).filter_by(framework_id=fw.id).count()
      assert count == expected

  def test_each_manifest_bridge_has_db_row(self, manifest: dict) -> None:
    with extensions_session("library") as s:
      for entry in manifest["bridges"]:
        bridge = (
          s.query(Bridge)
          .filter_by(bridge=entry["bridge"], version=entry["version"])
          .one_or_none()
        )
        assert bridge is not None, (
          f"Bridge {entry['bridge']}@{entry['version']} declared in manifest "
          f"but not seeded into public.bridges"
        )


@pytest.mark.integration
class TestResolvePinDispatch:
  """resolve_pin must continue to work for all four polymorphic shapes
  after the Framework/Bridge tables land."""

  def test_default_resolves_to_default_framework(self) -> None:
    from robosystems.taxonomy.pins import (
      DEFAULT_FRAMEWORK,
      DEFAULT_TAXONOMY_PIN,
      resolve_pin,
    )

    assert DEFAULT_FRAMEWORK == "rs-gaap-base@v1"
    assert resolve_pin(None) == DEFAULT_TAXONOMY_PIN

  def test_framework_ref_dispatches(self) -> None:
    from robosystems.taxonomy.pins import DEFAULT_TAXONOMY_PIN, resolve_pin

    class _G:
      taxonomy_pin = {"framework": "rs-gaap-base@v1"}

    assert resolve_pin(_G()) == DEFAULT_TAXONOMY_PIN  # type: ignore[arg-type]

  def test_overrides_layer_on_top_of_framework(self) -> None:
    from robosystems.taxonomy.pins import resolve_pin

    class _G:
      taxonomy_pin = {
        "framework": "rs-gaap-base@v1",
        "overrides": {"rs-gaap": "v999"},
      }

    pin = resolve_pin(_G())  # type: ignore[arg-type]
    assert pin["rs-gaap"] == "v999"
    assert pin["fac"] == "v1"  # untouched by override

  def test_legacy_flat_dict_returned_asis(self) -> None:
    from robosystems.taxonomy.pins import resolve_pin

    class _G:
      taxonomy_pin = {"fac": "v1", "rs-gaap": "v1"}

    assert resolve_pin(_G()) == {"fac": "v1", "rs-gaap": "v1"}  # type: ignore[arg-type]
