"""Per-graph taxonomy library pinning.

A pin is a ``{standard: version}`` map naming which library taxonomies a
tenant schema should copy from ``public.*`` at provision time. The default
pin covers the full canonical library shipped with this codebase — all
nine JSON-LD seeds. Graphs store their pin on ``Graph.taxonomy_pin``
(nullable JSONB); when null, ``resolve_pin`` returns the default.

This decouples "what to copy" (the pin) from "what's stored on the Graph
row" (the persisted JSONB column), so pre-``taxonomy_pin`` tenants — any
graph rows created before the column existed — still provision correctly.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
  from robosystems.models.core.graph.graph import Graph


DEFAULT_TAXONOMY_PIN: dict[str, str] = {
  "fac": "v1",
  "rs-gaap": "v1",
  "rs-gaap-hierarchy": "v1",
  "rs-gaap-presentation": "v1",
  "rs-gaap-calculations": "v1",
  "type-subtype": "v1",
  "fac-to-rs-gaap": "v1",
  "fac-calculations": "v1",
  "fac-presentation": "v1",
  "fac-rules": "v1",
}


def resolve_pin(graph: Graph | None) -> dict[str, str]:
  """Return the taxonomy pin for a graph, falling back to the default.

  Args:
      graph: A Graph row, or None for callers that don't have one (e.g.,
          the backfill migration copying the library into every existing
          tenant schema).

  Returns:
      A ``{standard: version}`` dict naming every library taxonomy to
      copy into the tenant schema.
  """
  if graph is not None:
    pin = getattr(graph, "taxonomy_pin", None)
    if pin:
      return dict(pin)
  return dict(DEFAULT_TAXONOMY_PIN)
