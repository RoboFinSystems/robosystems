"""Filesystem discovery for the three-tier taxonomy library layout.

The library lives at ``robosystems/taxonomy/`` with three sibling
directories:

- ``packages/{name}/{version}/taxonomy.jsonld`` — atomic taxonomy units
- ``bridges/{name}/{version}/taxonomy.jsonld`` — cross-namespace
  equivalence taxonomies
- ``frameworks/{name}/{version}.json`` — composition manifests that pin
  specific package + bridge versions

This module is the seam between the filesystem layout and the rest of
the system. The migration uses it to walk seed files in framework-pinned
order; ``pins.py`` uses ``expand_framework_to_pin`` to flatten a
manifest into the ``{standard: version}`` shape ``tenant_writer``
expects.

A package and a bridge are both addressable as ``(name, version)``
pairs; they differ only in directory placement (separating the two at
the filesystem level lets us reason about composition without cracking
open every ``taxonomy.jsonld``).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
  from collections.abc import Iterable


TAXONOMY_ROOT = Path(__file__).parent.parent
"""Absolute path to ``robosystems/taxonomy/`` regardless of how this
module is imported."""

PACKAGES_DIR = TAXONOMY_ROOT / "packages"
BRIDGES_DIR = TAXONOMY_ROOT / "bridges"
FRAMEWORKS_DIR = TAXONOMY_ROOT / "frameworks"


def list_packages(root: Path | None = None) -> list[Path]:
  """Walk ``packages/`` for every ``v*/taxonomy.jsonld`` file.

  Returns a flat list of absolute paths sorted by ``(package_name,
  version)``. Used by the migration when walking the manifest in
  pinned order; it relies on this enumeration to fail loudly on a
  manifest entry that points at a path that doesn't exist.
  """
  base = root or PACKAGES_DIR
  return _list_taxonomy_jsonld(base)


def list_bridges(root: Path | None = None) -> list[Path]:
  """Walk ``bridges/`` for every ``v*/taxonomy.jsonld`` file."""
  base = root or BRIDGES_DIR
  return _list_taxonomy_jsonld(base)


def package_path(standard: str, version: str, root: Path | None = None) -> Path:
  """Resolve a ``(standard, version)`` pair to its on-disk path."""
  base = root or PACKAGES_DIR
  return base / standard / version / "taxonomy.jsonld"


def bridge_path(bridge: str, version: str, root: Path | None = None) -> Path:
  """Resolve a ``(bridge, version)`` pair to its on-disk path."""
  base = root or BRIDGES_DIR
  return base / bridge / version / "taxonomy.jsonld"


def load_framework_manifest(
  name: str,
  version: str,
  root: Path | None = None,
) -> dict:
  """Read ``frameworks/{name}/{version}.json`` and validate its shape.

  Raises:
      FileNotFoundError: manifest file missing
      ValueError: manifest is missing required keys
  """
  base = root or FRAMEWORKS_DIR
  path = base / name / f"{version}.json"
  if not path.exists():
    raise FileNotFoundError(f"Framework manifest not found: {path}")

  manifest = json.loads(path.read_text())

  for required_key in ("framework", "version", "packages", "bridges"):
    if required_key not in manifest:
      raise ValueError(
        f"Framework manifest {path} missing required key: {required_key!r}"
      )
  if manifest["framework"] != name:
    raise ValueError(
      f"Framework manifest {path} declares framework={manifest['framework']!r}; "
      f"expected {name!r}"
    )
  if manifest["version"] != version:
    raise ValueError(
      f"Framework manifest {path} declares version={manifest['version']!r}; "
      f"expected {version!r}"
    )

  return manifest


def expand_framework_to_pin(manifest: dict) -> dict[str, str]:
  """Flatten a framework manifest into a ``{standard: version}`` dict.

  The flat dict is the shape that ``tenant_writer.copy_library_into_tenant``
  consumes. Bridges and packages share a flat namespace because the loader
  treats them identically once parsed — every ``taxonomy.jsonld`` declares
  a ``standard`` field that becomes the dict key, regardless of whether
  it lived under ``packages/`` or ``bridges/`` on disk.
  """
  pin: dict[str, str] = {}
  for pkg in manifest.get("packages", []):
    pin[pkg["standard"]] = pkg["version"]
  for brg in manifest.get("bridges", []):
    pin[brg["bridge"]] = brg["version"]
  return pin


def list_framework_seed_paths(
  manifest: dict,
  *,
  packages_root: Path | None = None,
  bridges_root: Path | None = None,
  skip_missing_optional: bool = True,
) -> list[Path]:
  """Resolve a framework manifest to the on-disk paths of every seed
  it pins, ordered by the ``ordinal`` field within each section.

  Packages come first, bridges second. Within each section, entries
  are sorted by ``ordinal`` so the load order encoded in the manifest
  is preserved.

  When ``skip_missing_optional`` is True (default), entries marked
  ``is_required: false`` whose taxonomy.jsonld doesn't exist on disk
  are silently skipped — useful while authoring a framework whose
  optional packages haven't been written yet. Missing required entries
  always raise.
  """
  paths: list[Path] = []
  for pkg in sorted(manifest.get("packages", []), key=lambda p: p.get("ordinal", 0)):
    path = package_path(pkg["standard"], pkg["version"], packages_root)
    if not path.exists():
      if pkg.get("is_required", True) or not skip_missing_optional:
        raise FileNotFoundError(
          f"Required package not found: {path} "
          f"(framework={manifest['framework']!r} pins it)"
        )
      continue
    paths.append(path)
  for brg in sorted(manifest.get("bridges", []), key=lambda b: b.get("ordinal", 0)):
    path = bridge_path(brg["bridge"], brg["version"], bridges_root)
    if not path.exists():
      if brg.get("is_required", True) or not skip_missing_optional:
        raise FileNotFoundError(
          f"Required bridge not found: {path} "
          f"(framework={manifest['framework']!r} pins it)"
        )
      continue
    paths.append(path)
  return paths


def _list_taxonomy_jsonld(root: Path) -> list[Path]:
  """Walk ``root/{name}/{version}/taxonomy.jsonld`` in sorted order."""
  if not root.exists():
    return []
  paths: list[Path] = []
  for name_dir in sorted(_iter_dirs(root)):
    for version_dir in sorted(_iter_dirs(name_dir)):
      candidate = version_dir / "taxonomy.jsonld"
      if candidate.exists():
        paths.append(candidate)
  return paths


def _iter_dirs(root: Path) -> Iterable[Path]:
  """Yield child directories of ``root``, skipping hidden entries
  (``.DS_Store``, dot-prefixed files committed accidentally)."""
  for entry in root.iterdir():
    if entry.is_dir() and not entry.name.startswith("."):
      yield entry


__all__ = [
  "BRIDGES_DIR",
  "FRAMEWORKS_DIR",
  "PACKAGES_DIR",
  "TAXONOMY_ROOT",
  "bridge_path",
  "expand_framework_to_pin",
  "list_bridges",
  "list_framework_seed_paths",
  "list_packages",
  "load_framework_manifest",
  "package_path",
]
