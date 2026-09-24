"""Filesystem discovery for the repo-root ``frameworks/`` library.

- ``frameworks/{name}/{version}.json``: manifest pinning package and bridge
  versions (several manifests can coexist).
- ``frameworks/{name}/packages/{std}/{ver}/taxonomy.jsonld``: taxonomy units.
- ``frameworks/{name}/bridges/{name}/{ver}/taxonomy.jsonld``: cross-namespace
  equivalence taxonomies.

Packages and bridges sit at the framework root so successive manifests share
them. A framework is its own authority boundary: identical packages may be
duplicated across frameworks.
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
  """``frameworks/{name}/``; versions are manifest filenames, not directories."""
  base = root or FRAMEWORKS_DIR
  return base / name


def list_packages(
  framework: str,
  root: Path | None = None,
) -> list[Path]:
  """Every ``packages/*/v*/taxonomy.jsonld``, sorted by (package, version)."""
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
  """On-disk path of a ``(standard, version)`` package under ``packages_root``."""
  return packages_root / standard / version / "taxonomy.jsonld"


def bridge_path(
  bridge: str,
  version: str,
  bridges_root: Path,
) -> Path:
  """On-disk path of a ``(bridge, version)`` under ``bridges_root``."""
  return bridges_root / bridge / version / "taxonomy.jsonld"


def load_framework_manifest(
  name: str,
  version: str,
  root: Path | None = None,
) -> dict:
  """Read and validate ``frameworks/{name}/{version}.json``.

  Validates entry shapes here so a malformed manifest fails clearly rather
  than as a KeyError downstream. Raises FileNotFoundError or ValueError.
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

  for pkg in manifest["packages"]:
    if "standard" not in pkg or "version" not in pkg:
      raise ValueError(
        f"Framework manifest {path} has a packages[] entry missing "
        f"'standard' or 'version': {pkg!r}"
      )
  for brg in manifest["bridges"]:
    if "bridge" not in brg or "version" not in brg:
      raise ValueError(
        f"Framework manifest {path} has a bridges[] entry missing "
        f"'bridge' or 'version': {brg!r}"
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

  Packages and bridges share one namespace. Dependencies expand first
  (depth-first) and this framework's entries overlay them; cycles raise
  ValueError.

  ``tenant_copy: false`` entries are omitted: still seeded into ``public``,
  but not copied into tenant schemas. Set it back to true and re-sync to
  promote one.
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
    if not pkg.get("tenant_copy", True):
      continue
    pin[pkg["standard"]] = pkg["version"]
  for brg in manifest.get("bridges", []):
    if not brg.get("tenant_copy", True):
      continue
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

  Dependencies first, then packages, then bridges, each section sorted by
  ``ordinal``. Cycles raise ValueError. With ``skip_missing_optional``,
  missing ``is_required: false`` entries are skipped; missing required ones
  always raise. A diamond ``depends_on`` is deduplicated in the top-level call.
  """
  is_root_call = _seen is None
  seen = _seen if _seen is not None else set()
  key = (manifest["framework"], manifest["version"])
  if key in seen:
    raise ValueError(
      f"Cyclic depends_on detected at {key[0]}@{key[1]} — already in resolution path"
    )
  seen = seen | {key}

  paths: list[Path] = []

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

  if is_root_call:
    return list(dict.fromkeys(paths))
  return paths


def list_framework_manifests(root: Path | None = None) -> list[Path]:
  """Walk ``frameworks/{name}/{version}.json`` for every framework manifest.

  Sorted by (framework, version); hidden and non-.json entries are skipped.
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
  """Non-hidden child directories of ``root``."""
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
