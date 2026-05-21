"""Filesystem discovery for the reporting-framework library.

The library lives at the repo-root ``frameworks/`` directory (peer to
``robosystems/``), with content nested under per-framework directories:

- ``frameworks/{name}/{version}.json`` — composition manifest that pins
  specific package + bridge versions. Multiple manifests (v1.json,
  v2.json, …) can coexist side-by-side at the framework root.
- ``frameworks/{name}/packages/{std}/{ver}/taxonomy.jsonld`` — atomic
  taxonomy units owned by this framework. Each package has its own
  version directory so multiple package versions can coexist and be
  referenced by different framework manifests.
- ``frameworks/{name}/bridges/{name}/{ver}/taxonomy.jsonld`` — cross-
  namespace equivalence taxonomies owned by this framework.

Packages and bridges live at the framework root (not under a per-
framework-version directory) because the common case is that successive
framework versions share most package versions; pulling them out of
a per-version directory avoids duplication when only the composition
(manifest) changes.

This module is the seam between the filesystem layout and the rest of
the system. The migration uses it to walk seed files in framework-pinned
order; ``pins.py`` uses ``expand_framework_to_pin`` to flatten a manifest
into the ``{standard: version}`` shape ``writer`` expects.

A package and a bridge are both addressable as ``(name, version)``
pairs; they differ only in directory placement within a framework
(separating the two at the filesystem level lets us reason about
composition without cracking open every ``taxonomy.jsonld``).

Every future framework (rs-call-report, rs-irs, rs-ferc, rs-statutory,
rs-ifrs) is a sibling under ``frameworks/`` with the same internal
shape. Packages with byte-identical content (e.g. ``fac/v1``) may be
duplicated across frameworks — atoms are self-contained inside their
framework, and the framework is the authority boundary.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
  from collections.abc import Iterable


FRAMEWORKS_DIR = Path(__file__).resolve().parent.parent.parent / "frameworks"
"""Absolute path to the repo-root ``frameworks/`` directory regardless of
how this module is imported. Resolves from
``robosystems/taxonomy/discovery.py`` → ``robosystems/taxonomy/`` →
``robosystems/`` → repo root → ``frameworks/``."""


def framework_root(
  name: str,
  root: Path | None = None,
) -> Path:
  """Return the per-framework directory: ``frameworks/{name}/``.

  This is the anchor every other discovery operation derives from —
  manifest files (``v1.json``, ``v2.json``, …), packages, and bridges
  all live inside this directory. Framework versions are encoded in
  the manifest filename, not in a subdirectory.
  """
  base = root or FRAMEWORKS_DIR
  return base / name


def list_packages(
  framework: str,
  root: Path | None = None,
) -> list[Path]:
  """Walk a framework's ``packages/`` for every ``v*/taxonomy.jsonld``.

  Returns a flat list of absolute paths sorted by ``(package_name,
  version)``. Packages live at the framework root, not under a per-
  framework-version subdirectory — multiple manifest versions share
  the same package pool.
  """
  base = framework_root(framework, root) / "packages"
  return _list_taxonomy_jsonld(base)


def list_bridges(
  framework: str,
  root: Path | None = None,
) -> list[Path]:
  """Walk a framework's ``bridges/`` for every ``v*/taxonomy.jsonld``."""
  base = framework_root(framework, root) / "bridges"
  return _list_taxonomy_jsonld(base)


def package_path(
  standard: str,
  version: str,
  packages_root: Path,
) -> Path:
  """Resolve a ``(standard, version)`` pair to its on-disk path.

  ``packages_root`` is the per-framework packages directory; callers
  typically derive it as ``framework_root(fw) / "packages"``.
  """
  return packages_root / standard / version / "taxonomy.jsonld"


def bridge_path(
  bridge: str,
  version: str,
  bridges_root: Path,
) -> Path:
  """Resolve a ``(bridge, version)`` pair to its on-disk path.

  ``bridges_root`` is the per-framework bridges directory; callers
  typically derive it as ``framework_root(fw) / "bridges"``.
  """
  return bridges_root / bridge / version / "taxonomy.jsonld"


def load_framework_manifest(
  name: str,
  version: str,
  root: Path | None = None,
) -> dict:
  """Read ``frameworks/{name}/{version}.json`` and validate.

  Raises:
      FileNotFoundError: manifest file missing
      ValueError: manifest is missing required keys
  """
  path = framework_root(name, root) / f"{version}.json"
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


def expand_framework_to_pin(
  manifest: dict,
  *,
  root: Path | None = None,
  _seen: set[tuple[str, str]] | None = None,
) -> dict[str, str]:
  """Flatten a framework manifest (and its ``depends_on`` chain) into
  a ``{standard: version}`` dict.

  The flat dict is the shape that ``writer.copy_library_into_tenant``
  consumes. Bridges and packages share a flat namespace because the loader
  treats them identically once parsed — every ``taxonomy.jsonld`` declares
  a ``standard`` field that becomes the dict key, regardless of whether
  it lived under ``packages/`` or ``bridges/`` on disk.

  Dependency frameworks are expanded first (depth-first); the current
  framework's packages overlay on top, letting a downstream framework
  pin a different version of a dependency-owned package via override.
  Cycles raise ``ValueError``.
  """
  seen = _seen if _seen is not None else set()
  key = (manifest["framework"], manifest["version"])
  if key in seen:
    raise ValueError(
      f"Cyclic depends_on detected at {key[0]}@{key[1]} — already in resolution path"
    )
  seen = seen | {key}

  pin: dict[str, str] = {}
  for dep in manifest.get("depends_on", []):
    dep_manifest = load_framework_manifest(dep["framework"], dep["version"], root)
    pin.update(expand_framework_to_pin(dep_manifest, root=root, _seen=seen))
  for pkg in manifest.get("packages", []):
    pin[pkg["standard"]] = pkg["version"]
  for brg in manifest.get("bridges", []):
    pin[brg["bridge"]] = brg["version"]
  return pin


def list_framework_seed_paths(
  manifest: dict,
  *,
  root: Path | None = None,
  skip_missing_optional: bool = True,
  _seen: set[tuple[str, str]] | None = None,
) -> list[Path]:
  """Resolve a framework manifest (and its ``depends_on`` chain) to the
  on-disk paths of every seed it pins, ordered by load dependency then
  by the ``ordinal`` field within each section.

  Dependencies load first (depth-first walk of ``depends_on``), then
  the current framework's packages, then its bridges. Within each
  section, entries are sorted by ``ordinal`` so the per-manifest load
  order is preserved. Cycles raise ``ValueError``.

  ``root`` overrides the default ``FRAMEWORKS_DIR`` for tests. Per-
  framework directories are derived from ``manifest["framework"]``, so
  a dependency framework's packages resolve against its own framework
  root, not the caller's.

  When ``skip_missing_optional`` is True (default), entries marked
  ``is_required: false`` whose taxonomy.jsonld doesn't exist on disk
  are silently skipped — useful while authoring a framework whose
  optional packages haven't been written yet. Missing required entries
  always raise.
  """
  seen = _seen if _seen is not None else set()
  key = (manifest["framework"], manifest["version"])
  if key in seen:
    raise ValueError(
      f"Cyclic depends_on detected at {key[0]}@{key[1]} — already in resolution path"
    )
  seen = seen | {key}

  paths: list[Path] = []

  # 1) Recurse into depends_on first so dependencies load before us.
  for dep in manifest.get("depends_on", []):
    dep_manifest = load_framework_manifest(dep["framework"], dep["version"], root)
    paths.extend(
      list_framework_seed_paths(
        dep_manifest,
        root=root,
        skip_missing_optional=skip_missing_optional,
        _seen=seen,
      )
    )

  # 2) This framework's own packages + bridges.
  fw_dir = framework_root(manifest["framework"], root)
  packages_root = fw_dir / "packages"
  bridges_root = fw_dir / "bridges"

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


def list_framework_manifests(root: Path | None = None) -> list[Path]:
  """Walk ``frameworks/{name}/{version}.json`` for every framework manifest.

  Returns absolute paths sorted by ``(framework_name, version)``. Used by
  the framework-seeding migration to enumerate every manifest on disk.
  Hidden files (dot-prefixed) and non-.json files are skipped, so the
  sibling ``packages/`` and ``bridges/`` directories don't pollute the
  result.
  """
  base = root or FRAMEWORKS_DIR
  if not base.exists():
    return []
  paths: list[Path] = []
  for name_dir in sorted(_iter_dirs(base)):
    for entry in sorted(name_dir.iterdir()):
      if entry.is_file() and entry.suffix == ".json" and not entry.name.startswith("."):
        paths.append(entry)
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
  "FRAMEWORKS_DIR",
  "bridge_path",
  "expand_framework_to_pin",
  "framework_root",
  "list_bridges",
  "list_framework_manifests",
  "list_framework_seed_paths",
  "list_packages",
  "load_framework_manifest",
  "package_path",
]
