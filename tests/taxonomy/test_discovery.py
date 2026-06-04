"""Unit tests for the taxonomy discovery layer.

These exercise the framework-resolution algorithms directly against
synthetic framework trees built in ``tmp_path`` — no DB, no real
taxonomy content. They cover the logic that would break silently when a
new framework is added with an unexpected shape: manifest validation,
``depends_on`` expansion (chains, diamonds, cycles, overrides), seed-path
ordering, and manifest enumeration.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from robosystems.taxonomy.discovery import (
  expand_framework_to_pin,
  framework_root,
  list_framework_manifests,
  list_framework_seed_paths,
  load_framework_manifest,
)


def _write_manifest(
  root: Path,
  framework: str,
  version: str = "v1",
  *,
  depends_on: list[dict] | None = None,
  packages: list[dict] | None = None,
  bridges: list[dict] | None = None,
) -> Path:
  """Write a ``frameworks/{framework}/{version}.json`` manifest under root."""
  fw_dir = root / framework
  fw_dir.mkdir(parents=True, exist_ok=True)
  manifest = {
    "framework": framework,
    "version": version,
    "title": f"{framework} {version}",
    "framework_type": "reporting",
    "depends_on": depends_on or [],
    "packages": packages or [],
    "bridges": bridges or [],
  }
  path = fw_dir / f"{version}.json"
  path.write_text(json.dumps(manifest))
  return path


def _write_package(
  root: Path, framework: str, standard: str, version: str = "v1"
) -> None:
  """Create an (empty) ``packages/{standard}/{version}/taxonomy.jsonld``."""
  pkg_dir = root / framework / "packages" / standard / version
  pkg_dir.mkdir(parents=True, exist_ok=True)
  (pkg_dir / "taxonomy.jsonld").write_text("{}")


def _write_bridge(root: Path, framework: str, bridge: str, version: str = "v1") -> None:
  """Create an (empty) ``bridges/{bridge}/{version}/taxonomy.jsonld``."""
  brg_dir = root / framework / "bridges" / bridge / version
  brg_dir.mkdir(parents=True, exist_ok=True)
  (brg_dir / "taxonomy.jsonld").write_text("{}")


class TestLoadFrameworkManifest:
  @pytest.mark.unit
  def test_happy_path(self, tmp_path: Path) -> None:
    _write_manifest(tmp_path, "fac", packages=[{"standard": "fac", "version": "v1"}])
    m = load_framework_manifest("fac", "v1", root=tmp_path)
    assert m["framework"] == "fac"
    assert m["packages"] == [{"standard": "fac", "version": "v1"}]

  @pytest.mark.unit
  def test_missing_file_raises(self, tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="not found"):
      load_framework_manifest("nope", "v1", root=tmp_path)

  @pytest.mark.unit
  def test_missing_top_level_key_raises(self, tmp_path: Path) -> None:
    fw_dir = tmp_path / "fac"
    fw_dir.mkdir()
    (fw_dir / "v1.json").write_text(json.dumps({"framework": "fac", "version": "v1"}))
    with pytest.raises(ValueError, match="missing required key"):
      load_framework_manifest("fac", "v1", root=tmp_path)

  @pytest.mark.unit
  def test_name_mismatch_raises(self, tmp_path: Path) -> None:
    fw_dir = tmp_path / "fac"
    fw_dir.mkdir()
    (fw_dir / "v1.json").write_text(
      json.dumps({"framework": "wrong", "version": "v1", "packages": [], "bridges": []})
    )
    with pytest.raises(ValueError, match="declares framework"):
      load_framework_manifest("fac", "v1", root=tmp_path)

  @pytest.mark.unit
  def test_version_mismatch_raises(self, tmp_path: Path) -> None:
    fw_dir = tmp_path / "fac"
    fw_dir.mkdir()
    (fw_dir / "v1.json").write_text(
      json.dumps({"framework": "fac", "version": "v9", "packages": [], "bridges": []})
    )
    with pytest.raises(ValueError, match="declares version"):
      load_framework_manifest("fac", "v1", root=tmp_path)

  @pytest.mark.unit
  def test_malformed_package_entry_raises(self, tmp_path: Path) -> None:
    """A package entry missing 'version' fails loudly at load, not later."""
    _write_manifest(tmp_path, "fac", packages=[{"standard": "fac"}])
    with pytest.raises(ValueError, match="packages\\[\\] entry missing"):
      load_framework_manifest("fac", "v1", root=tmp_path)

  @pytest.mark.unit
  def test_malformed_bridge_entry_raises(self, tmp_path: Path) -> None:
    _write_manifest(tmp_path, "rs-gaap", bridges=[{"version": "v1"}])
    with pytest.raises(ValueError, match="bridges\\[\\] entry missing"):
      load_framework_manifest("rs-gaap", "v1", root=tmp_path)


class TestExpandFrameworkToPin:
  @pytest.mark.unit
  def test_single_framework_no_deps(self, tmp_path: Path) -> None:
    _write_manifest(
      tmp_path,
      "fac",
      packages=[
        {"standard": "fac", "version": "v1"},
        {"standard": "fac-traits", "version": "v1"},
      ],
    )
    m = load_framework_manifest("fac", "v1", root=tmp_path)
    pin = expand_framework_to_pin(m, root=tmp_path)
    assert pin == {"fac": "v1", "fac-traits": "v1"}

  @pytest.mark.unit
  def test_depends_on_chain_merges(self, tmp_path: Path) -> None:
    _write_manifest(tmp_path, "fac", packages=[{"standard": "fac", "version": "v1"}])
    _write_manifest(
      tmp_path,
      "rs-gaap",
      depends_on=[{"framework": "fac", "version": "v1"}],
      packages=[{"standard": "rs-gaap", "version": "v1"}],
      bridges=[{"bridge": "fac-to-rs-gaap", "version": "v1"}],
    )
    m = load_framework_manifest("rs-gaap", "v1", root=tmp_path)
    pin = expand_framework_to_pin(m, root=tmp_path)
    assert pin == {"fac": "v1", "rs-gaap": "v1", "fac-to-rs-gaap": "v1"}

  @pytest.mark.unit
  def test_diamond_dependency_resolves_once(self, tmp_path: Path) -> None:
    """A → [B, C]; B → D; C → D. D's package appears once, no false cycle."""
    _write_manifest(tmp_path, "D", packages=[{"standard": "d", "version": "v1"}])
    _write_manifest(
      tmp_path,
      "B",
      depends_on=[{"framework": "D", "version": "v1"}],
      packages=[{"standard": "b", "version": "v1"}],
    )
    _write_manifest(
      tmp_path,
      "C",
      depends_on=[{"framework": "D", "version": "v1"}],
      packages=[{"standard": "c", "version": "v1"}],
    )
    _write_manifest(
      tmp_path,
      "A",
      depends_on=[
        {"framework": "B", "version": "v1"},
        {"framework": "C", "version": "v1"},
      ],
      packages=[{"standard": "a", "version": "v1"}],
    )
    m = load_framework_manifest("A", "v1", root=tmp_path)
    pin = expand_framework_to_pin(m, root=tmp_path)
    assert pin == {"a": "v1", "b": "v1", "c": "v1", "d": "v1"}

  @pytest.mark.unit
  def test_cycle_raises(self, tmp_path: Path) -> None:
    _write_manifest(tmp_path, "A", depends_on=[{"framework": "B", "version": "v1"}])
    _write_manifest(tmp_path, "B", depends_on=[{"framework": "A", "version": "v1"}])
    m = load_framework_manifest("A", "v1", root=tmp_path)
    with pytest.raises(ValueError, match="Cyclic depends_on"):
      expand_framework_to_pin(m, root=tmp_path)

  @pytest.mark.unit
  def test_downstream_overrides_dependency_version(self, tmp_path: Path) -> None:
    """A package pinned by both a dependency and the downstream framework
    resolves to the downstream version (it overlays last)."""
    _write_manifest(tmp_path, "fac", packages=[{"standard": "shared", "version": "v1"}])
    _write_manifest(
      tmp_path,
      "rs-gaap",
      depends_on=[{"framework": "fac", "version": "v1"}],
      packages=[{"standard": "shared", "version": "v2"}],
    )
    m = load_framework_manifest("rs-gaap", "v1", root=tmp_path)
    pin = expand_framework_to_pin(m, root=tmp_path)
    assert pin["shared"] == "v2"

  @pytest.mark.unit
  def test_tenant_copy_false_excluded_from_pin(self, tmp_path: Path) -> None:
    """A package/bridge marked ``tenant_copy: false`` is omitted from the
    pin — it seeds into the public library but is not copied per-tenant.
    ``tenant_copy`` defaults to true (absent == included)."""
    _write_manifest(
      tmp_path,
      "rs-gaap",
      packages=[
        {"standard": "rs-gaap", "version": "v1"},
        {"standard": "rs-gaap-default-true", "version": "v1", "tenant_copy": True},
        {"standard": "rs-gaap-parked", "version": "v1", "tenant_copy": False},
      ],
      bridges=[
        {"bridge": "kept-bridge", "version": "v1"},
        {"bridge": "parked-bridge", "version": "v1", "tenant_copy": False},
      ],
    )
    m = load_framework_manifest("rs-gaap", "v1", root=tmp_path)
    pin = expand_framework_to_pin(m, root=tmp_path)
    assert pin == {
      "rs-gaap": "v1",
      "rs-gaap-default-true": "v1",
      "kept-bridge": "v1",
    }
    assert "rs-gaap-parked" not in pin
    assert "parked-bridge" not in pin


class TestListFrameworkSeedPaths:
  @pytest.mark.unit
  def test_ordinal_ordering(self, tmp_path: Path) -> None:
    _write_manifest(
      tmp_path,
      "fac",
      packages=[
        {"standard": "second", "version": "v1", "ordinal": 1},
        {"standard": "first", "version": "v1", "ordinal": 0},
      ],
    )
    _write_package(tmp_path, "fac", "first")
    _write_package(tmp_path, "fac", "second")
    m = load_framework_manifest("fac", "v1", root=tmp_path)
    paths = list_framework_seed_paths(m, root=tmp_path)
    assert [p.parent.parent.name for p in paths] == ["first", "second"]

  @pytest.mark.unit
  def test_dependencies_load_before_owner(self, tmp_path: Path) -> None:
    _write_manifest(tmp_path, "fac", packages=[{"standard": "fac", "version": "v1"}])
    _write_package(tmp_path, "fac", "fac")
    _write_manifest(
      tmp_path,
      "rs-gaap",
      depends_on=[{"framework": "fac", "version": "v1"}],
      packages=[{"standard": "rs-gaap", "version": "v1"}],
    )
    _write_package(tmp_path, "rs-gaap", "rs-gaap")
    m = load_framework_manifest("rs-gaap", "v1", root=tmp_path)
    paths = list_framework_seed_paths(m, root=tmp_path)
    names = [p.parent.parent.name for p in paths]
    assert names == ["fac", "rs-gaap"]

  @pytest.mark.unit
  def test_skip_missing_optional(self, tmp_path: Path) -> None:
    _write_manifest(
      tmp_path,
      "fac",
      packages=[
        {"standard": "present", "version": "v1"},
        {"standard": "absent", "version": "v1", "is_required": False},
      ],
    )
    _write_package(tmp_path, "fac", "present")  # "absent" intentionally not written
    m = load_framework_manifest("fac", "v1", root=tmp_path)
    paths = list_framework_seed_paths(m, root=tmp_path, skip_missing_optional=True)
    assert [p.parent.parent.name for p in paths] == ["present"]

  @pytest.mark.unit
  def test_tenant_copy_orthogonal_to_seed_paths(self, tmp_path: Path) -> None:
    """``tenant_copy`` governs the per-tenant copy, NOT the public seed: a
    present file for a ``tenant_copy: false`` package is still returned as a
    seed path (it lands in the public library; only ``expand_framework_to_pin``
    drops it so the per-tenant copy skips it)."""
    _write_manifest(
      tmp_path,
      "fac",
      packages=[
        {"standard": "kept", "version": "v1"},
        {"standard": "public-only", "version": "v1", "tenant_copy": False},
      ],
    )
    _write_package(tmp_path, "fac", "kept")
    _write_package(tmp_path, "fac", "public-only")
    m = load_framework_manifest("fac", "v1", root=tmp_path)
    paths = list_framework_seed_paths(m, root=tmp_path)
    assert {p.parent.parent.name for p in paths} == {"kept", "public-only"}

  @pytest.mark.unit
  def test_missing_required_raises(self, tmp_path: Path) -> None:
    _write_manifest(tmp_path, "fac", packages=[{"standard": "gone", "version": "v1"}])
    # taxonomy.jsonld never written
    m = load_framework_manifest("fac", "v1", root=tmp_path)
    with pytest.raises(FileNotFoundError, match="Required package not found"):
      list_framework_seed_paths(m, root=tmp_path)

  @pytest.mark.unit
  def test_diamond_dedups_shared_dependency(self, tmp_path: Path) -> None:
    """rs-call-report → [fac, rs-gaap]; rs-gaap → fac. fac's seed appears once."""
    _write_manifest(tmp_path, "fac", packages=[{"standard": "fac", "version": "v1"}])
    _write_package(tmp_path, "fac", "fac")
    _write_manifest(
      tmp_path,
      "rs-gaap",
      depends_on=[{"framework": "fac", "version": "v1"}],
      packages=[{"standard": "rs-gaap", "version": "v1"}],
    )
    _write_package(tmp_path, "rs-gaap", "rs-gaap")
    _write_manifest(
      tmp_path,
      "rs-call-report",
      depends_on=[
        {"framework": "fac", "version": "v1"},
        {"framework": "rs-gaap", "version": "v1"},
      ],
      packages=[{"standard": "rs-call-report", "version": "v1"}],
    )
    _write_package(tmp_path, "rs-call-report", "rs-call-report")
    m = load_framework_manifest("rs-call-report", "v1", root=tmp_path)
    paths = list_framework_seed_paths(m, root=tmp_path)
    # fac appears exactly once despite being reachable via two branches.
    fac_paths = [p for p in paths if p.parent.parent.name == "fac"]
    assert len(fac_paths) == 1
    assert len(paths) == len(set(paths))


class TestListFrameworkManifests:
  @pytest.mark.unit
  def test_finds_all_frameworks(self, tmp_path: Path) -> None:
    _write_manifest(tmp_path, "fac")
    _write_manifest(tmp_path, "rs-gaap")
    found = {p.parent.name for p in list_framework_manifests(root=tmp_path)}
    assert found == {"fac", "rs-gaap"}

  @pytest.mark.unit
  def test_multiple_versions_same_framework(self, tmp_path: Path) -> None:
    _write_manifest(tmp_path, "fac", version="v1")
    _write_manifest(tmp_path, "fac", version="v2")
    names = sorted(p.name for p in list_framework_manifests(root=tmp_path))
    assert names == ["v1.json", "v2.json"]

  @pytest.mark.unit
  def test_skips_hidden_and_non_json(self, tmp_path: Path) -> None:
    _write_manifest(tmp_path, "fac")
    (tmp_path / "fac" / ".DS_Store").write_text("junk")
    (tmp_path / "fac" / "README.md").write_text("# docs")
    (tmp_path / ".hidden").mkdir()
    manifests = list_framework_manifests(root=tmp_path)
    assert [p.name for p in manifests] == ["v1.json"]

  @pytest.mark.unit
  def test_empty_root_returns_empty(self, tmp_path: Path) -> None:
    assert list_framework_manifests(root=tmp_path / "does-not-exist") == []


class TestFrameworkRoot:
  @pytest.mark.unit
  def test_appends_name(self, tmp_path: Path) -> None:
    assert framework_root("rs-gaap", root=tmp_path) == tmp_path / "rs-gaap"
