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
  # Serialize WITH the canonical context so rdflib compacts predicate IRIs
  # to their readable terms (`balance`, `summationOf`, `from`, …) and @id/@type
  # to prefixed qnames. Keeping keys compact keeps committed seeds readable and
  # keeps diffs surgical (a concept's `balance` key is stable even though the
  # context now maps it to xbrli:balance).
  raw = graph.serialize(format="json-ld", auto_compact=True, context=CANONICAL_CONTEXT)

  parsed = json.loads(raw)

  # rdflib returns a list of resources or a {"@context", "@graph"|node} object;
  # normalize to a flat node list (we re-attach the full context below).
  if isinstance(parsed, list):
    nodes: list[Any] = parsed
  elif isinstance(parsed, dict):
    if "@graph" in parsed:
      nodes = parsed["@graph"]
    else:
      nodes = [{k: v for k, v in parsed.items() if k != "@context"}]
  else:
    nodes = []

  envelope: dict[str, Any] = {
    "@context": CANONICAL_CONTEXT,
    "@graph": nodes,
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
