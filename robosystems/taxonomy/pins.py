"""Per-graph taxonomy library pinning.

A pin describes which library taxonomies a tenant schema should copy
from ``public.*`` at provision time. ``Graph.taxonomy_pin`` (JSONB)
holds the pin and accepts three polymorphic shapes:

1. ``null`` — fall back to the default framework
   (:data:`DEFAULT_FRAMEWORK`).
2. ``{"framework": "rs-gaap@v1"}`` — pin a named framework. The
   resolver looks up the framework manifest at
   ``frameworks/{name}/{version}.json`` and expands it to a flat
   ``{standard: version}`` dict. An optional ``"overrides"`` sub-dict
   lets a tenant pin different versions for individual packages while
   keeping the rest of the framework intact.
3. ``{"fac": "v1", "rs-gaap": "v1", ...}`` — legacy direct pin. Treated
   as the literal pin to copy. Pre-framework graphs use this shape;
   they continue to provision identically.

``resolve_pin`` returns a flat ``{standard: version}`` dict regardless
of input shape, which is what
``robosystems.taxonomy.writers.tenant_writer.copy_library_into_tenant``
consumes.

This module reads the framework manifest from disk so it stays usable
in environments where the extensions database is unreachable (tests,
CLI tools, the loader itself). The Phase B follow-up may add a
DB-backed expansion path that prefers the ``frameworks`` table when
present and falls back to disk on failure; the call site signature
does not change.
"""

from __future__ import annotations

from functools import lru_cache
from typing import TYPE_CHECKING, Any

from robosystems.taxonomy.loaders.discovery import (
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
  """Return the ``{standard: version}`` pin for a graph.

  Dispatches by the shape of ``graph.taxonomy_pin``:

  - ``None`` / not set → expand :data:`DEFAULT_FRAMEWORK`.
  - ``{"framework": ref}`` → expand that framework, layering any
    ``overrides`` on top.
  - any other dict → return as-is (legacy direct pin).

  The function never raises for an unknown framework — if the manifest
  is missing, the underlying ``load_framework_manifest`` raises and
  the caller should treat that as a configuration error.
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
    # Legacy direct pin — return as-is.
    return {str(k): str(v) for k, v in pin.items()}

  raise ValueError(
    f"Graph.taxonomy_pin must be null, a framework reference dict, or a "
    f"flat {{standard: version}} dict; got {type(pin).__name__}"
  )


@lru_cache(maxsize=1)
def _default_taxonomy_pin() -> dict[str, str]:
  """Cache-once expansion of the default framework manifest.

  Lazy so importing :mod:`robosystems.taxonomy.pins` never touches the
  filesystem — environments without the taxonomy tree on disk can still
  import the module, and the manifest read only happens the first time
  a caller actually needs the pin.
  """
  return resolve_pin(None)


def __getattr__(name: str) -> Any:
  """Lazy module-level attribute: ``DEFAULT_TAXONOMY_PIN``.

  Resolves the default pin on first access and caches it. Returning a
  shallow copy keeps mutation from callers from corrupting the cache.
  """
  if name == "DEFAULT_TAXONOMY_PIN":
    return dict(_default_taxonomy_pin())
  raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
