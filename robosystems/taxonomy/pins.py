"""Per-graph taxonomy library pinning.

``Graph.taxonomy_pin`` takes three shapes: ``null`` (the default framework),
``{"framework": "rs-gaap@v1", "overrides": {...}}``, or a legacy direct
``{standard: version}`` dict. ``resolve_pin`` flattens any of them to
``{standard: version}`` for ``writer.copy_library_into_tenant``. Manifests
are read from disk, so this works without the extensions database.
"""

from __future__ import annotations

from functools import lru_cache
from typing import TYPE_CHECKING, Any

from robosystems.taxonomy.discovery import (
  expand_framework_to_pin,
  load_framework_manifest,
)

if TYPE_CHECKING:
  from robosystems.models.core.graph.graph import Graph


DEFAULT_FRAMEWORK = "rs-gaap@v1"
"""The framework every graph falls back to when ``taxonomy_pin`` is
unset. Encoded as ``name@version`` for ergonomic admin overrides."""


def _parse_framework_ref(ref: str) -> tuple[str, str]:
  """Split ``"rs-gaap@v1"`` into ``("rs-gaap", "v1")``."""
  if "@" not in ref:
    raise ValueError(
      f"Framework reference {ref!r} must be in 'name@version' format "
      f"(e.g. 'rs-gaap@v1')."
    )
  name, _, version = ref.partition("@")
  if not name or not version:
    raise ValueError(f"Framework reference {ref!r} has empty name or version.")
  return name, version


def _expand_framework(name: str, version: str) -> dict[str, str]:
  """Read a framework manifest from disk and return a flat pin."""
  manifest = load_framework_manifest(name, version)
  return expand_framework_to_pin(manifest)


def resolve_pin(graph: Graph | None) -> dict[str, str]:
  """Flatten ``graph.taxonomy_pin`` to ``{standard: version}``.

  A missing manifest raises FileNotFoundError, an unrecognized shape
  ValueError; both are configuration errors.
  """
  pin = getattr(graph, "taxonomy_pin", None) if graph is not None else None

  if not pin:
    name, version = _parse_framework_ref(DEFAULT_FRAMEWORK)
    return _expand_framework(name, version)

  if isinstance(pin, dict) and "framework" in pin:
    name, version = _parse_framework_ref(str(pin["framework"]))
    base = _expand_framework(name, version)
    overrides = pin.get("overrides", {})
    if not isinstance(overrides, dict):
      raise ValueError(
        f"Graph.taxonomy_pin overrides must be a dict, got {type(overrides).__name__}"
      )
    return {**base, **{str(k): str(v) for k, v in overrides.items()}}

  if isinstance(pin, dict):
    # Legacy direct pin
    return {str(k): str(v) for k, v in pin.items()}

  raise ValueError(
    f"Graph.taxonomy_pin must be null, a framework reference dict, or a "
    f"flat {{standard: version}} dict; got {type(pin).__name__}"
  )


@lru_cache(maxsize=1)
def _default_taxonomy_pin() -> dict[str, str]:
  """Lazy, so importing this module never touches the filesystem."""
  return resolve_pin(None)


def __getattr__(name: str) -> Any:
  """Lazy ``DEFAULT_TAXONOMY_PIN``; a copy, so callers cannot corrupt the cache."""
  if name == "DEFAULT_TAXONOMY_PIN":
    return dict(_default_taxonomy_pin())
  raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
