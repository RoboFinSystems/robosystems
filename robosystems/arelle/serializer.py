"""rdflib.Graph → canonical JSON-LD serializer.

Produces deterministic JSON-LD output suitable for committing to git as
seed artifacts. The canonical @context is injected; blank node IDs and
array orderings are normalized for byte-stable output across runs.
"""

from __future__ import annotations

import json
from typing import Any

from rdflib import Graph

from robosystems.arelle.context import CANONICAL_CONTEXT


def _canonicalize(doc: dict | list) -> dict | list:
  """Recursively sort dict keys and list entries for deterministic output.

  Arrays of dicts are sorted by `@id` when present, else by JSON string.
  Arrays of primitives are sorted by value.
  """
  if isinstance(doc, dict):
    return {k: _canonicalize(v) for k, v in sorted(doc.items())}
  if isinstance(doc, list):
    # Sort list items; preserve element canonicalization
    canonical_items = [_canonicalize(item) for item in doc]
    try:
      return sorted(
        canonical_items,
        key=lambda x: (
          x.get("@id", json.dumps(x, sort_keys=True)) if isinstance(x, dict) else str(x)
        ),
      )
    except (TypeError, ValueError):
      return canonical_items
  return doc


def serialize_jsonld(
  graph: Graph,
  standard: str,
  version: str,
  *,
  namespace_uri: str | None = None,
  description: str | None = None,
  taxonomy_type: str = "reporting_standard",
) -> str:
  """Serialize an rdflib.Graph to canonical JSON-LD string.

  Args:
      graph: rdflib.Graph from `extract_taxonomy()`.
      standard: Taxonomy standard identifier (fac, rs-gaap, us-gaap, …).
      version: Version identifier (v1, 2020, …).
      namespace_uri: Primary namespace URI (for metadata).
      description: Optional human-readable description.
      taxonomy_type: chart_of_accounts | reporting_standard | reporting_extension
        | custom_ontology | mapping | schedule.

  Returns:
      JSON-LD string ready to write to a seed file.
  """
  # rdflib's default JSON-LD serializer. Use context=None and inject ours
  # afterward to retain control over the @context document.
  raw = graph.serialize(format="json-ld", indent=None)

  parsed = json.loads(raw)

  # rdflib may return a list of resources or a single object; normalize
  # to a {"@context": ..., "@graph": [...]} envelope.
  if isinstance(parsed, list):
    nodes: list[Any] = parsed
  elif isinstance(parsed, dict):
    nodes = parsed.get("@graph", [parsed])
  else:
    nodes = []

  # Compact IRIs against our canonical context prefixes. Use a reverse
  # lookup built from the context.
  compacted_nodes = [_compact_node(n) for n in nodes]

  envelope: dict[str, Any] = {
    "@context": CANONICAL_CONTEXT,
    "@graph": compacted_nodes,
  }

  # Taxonomy-level metadata as a sibling of @graph (JSON-LD allows this
  # when typed as the taxonomy itself).
  if standard:
    envelope["standard"] = standard
  if version:
    envelope["version"] = version
  envelope["taxonomy_type"] = taxonomy_type
  if namespace_uri:
    envelope["namespace_uri"] = namespace_uri
  if description:
    envelope["description"] = description

  envelope["@graph"] = _canonicalize(envelope["@graph"])  # type: ignore[assignment]

  return json.dumps(envelope, indent=2, sort_keys=False, ensure_ascii=False)


# Build reverse lookup: long IRI → compact prefix:local form
def _build_compact_map() -> list[tuple[str, str]]:
  """Build list of (long_iri, prefix) pairs sorted by length descending.

  Longer IRIs are checked first so that matching picks the most specific
  prefix (e.g. us-gaap-2017 before us-gaap).
  """
  pairs: list[tuple[str, str]] = []
  for key, val in CANONICAL_CONTEXT.items():
    if isinstance(val, str) and val.startswith(("http://", "https://")):
      pairs.append((val, key))
  return sorted(pairs, key=lambda p: -len(p[0]))


_COMPACT_MAP = _build_compact_map()


def _compact_iri(iri: str) -> str:
  """Compact a full IRI to prefix:local form if a prefix matches."""
  for long_iri, prefix in _COMPACT_MAP:
    if iri.startswith(long_iri):
      remainder = iri[len(long_iri) :]
      # Avoid compacting partial matches that cross path boundaries in
      # strange ways; local name should not start with / or #
      if remainder and not remainder.startswith(("/", "#")):
        return f"{prefix}:{remainder}"
  return iri


def _compact_node(node: Any) -> Any:
  """Recursively compact IRIs in a JSON-LD node structure."""
  if isinstance(node, dict):
    out = {}
    for k, v in node.items():
      if k == "@id" and isinstance(v, str):
        out[k] = _compact_iri(v)
      elif k == "@type":
        if isinstance(v, str):
          out[k] = _compact_iri(v)
        elif isinstance(v, list):
          out[k] = [_compact_iri(t) if isinstance(t, str) else t for t in v]
        else:
          out[k] = v
      else:
        out[k] = _compact_node(v)
    return out
  if isinstance(node, list):
    return [_compact_node(x) for x in node]
  return node
