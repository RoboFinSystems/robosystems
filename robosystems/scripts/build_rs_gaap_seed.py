"""Mint the rs-gaap + fac-to-rs-gaap seeds from Charlie's FAC → us-gaap
2017 mapping.

rs-gaap is our year-independent canonical under our namespace. This
script bootstraps it by:

1. Reading `fac/v1/taxonomy.jsonld` (Charlie's ingested ConceptMap via
   Arelle — raw, with fac concepts AND their equivalence arcs into
   us-gaap-2017 in one file).
2. Partitioning the graph into us-gaap nodes vs everything else.
3. Renaming us-gaap-YYYY IRIs to rs-gaap and writing rs-gaap/v1.
4. Splitting the remaining fac graph: concept predicates stay in
   fac/v1 (taxonomy_type=reporting, browseable), arc predicates +
   structure nodes move to fac-to-rs-gaap/v1 (taxonomy_type=mapping).

After this runs the library has:
  Concepts:  sfac6, fac, rs-gaap
  Mappings:  fac-to-rs-gaap, type-subtype

FAC concepts are classified on the FASB elementsOfFinancialStatements
trait axis directly via rs:classifiedAs arcs inside fac/v1 — no separate
sfac6-to-fac anchoring taxonomy is emitted.

Future us-gaap versions (2020, 2024) become additional mapping
taxonomies alongside fac-to-rs-gaap — our canonical (rs-gaap) stays
stable as FASB evolves.

Usage:
    uv run python -m robosystems.scripts.build_rs_gaap_seed
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

from robosystems.arelle.context import CANONICAL_CONTEXT
from robosystems.logger import logger

SEEDS_DIR = Path(__file__).parent.parent / "taxonomy" / "seeds"
FAC_PATH = SEEDS_DIR / "fac" / "v1" / "taxonomy.jsonld"
FAC_TO_RS_GAAP_PATH = SEEDS_DIR / "fac-to-rs-gaap" / "v1" / "taxonomy.jsonld"
TYPE_SUBTYPE_PATH = SEEDS_DIR / "type-subtype" / "v1" / "taxonomy.jsonld"
RS_GAAP_PATH = SEEDS_DIR / "rs-gaap" / "v1" / "taxonomy.jsonld"

# Arc predicate IRIs — nodes' triples with these predicates are moved out
# of the concept taxonomy (fac/v1) into the mapping taxonomy
# (fac-to-rs-gaap/v1) so each taxonomy carries a single concern.
ARC_PREDICATES = {
  "http://www.w3.org/2002/07/owl#equivalentClass",
  "https://robosystems.ai/vocab/parent",
  "https://robosystems.ai/vocab/summationOf",
  "https://robosystems.ai/vocab/generalOf",
  "https://robosystems.ai/vocab/dimensionOf",
  "https://robosystems.ai/vocab/hypercubeOf",
  # Compact forms (in case the source JSON-LD uses them)
  "equivalent",
  "summationOf",
  "generalOf",
  "dimensionOf",
  "hypercubeOf",
}

# Predicates that identify a node as a concept (stays in fac/v1).
_CONCEPT_MARKER_PREDS = {
  "https://robosystems.ai/vocab/classification",
  "https://robosystems.ai/vocab/balance",
  "https://robosystems.ai/vocab/elementType",
  "classification",
  "balance",
  "elementType",
}

# Predicates that identify a node as a structure (ELR) — moves with arcs.
_STRUCTURE_MARKER_PREDS = {
  "https://robosystems.ai/vocab/roleUri",
  "roleUri",
}

TARGET_PREFIX = "rs-gaap:"

# us-gaap versions that are now subsumed under rs-gaap (our namespace).
# Any `us-gaap-YYYY:X` IRI in a seed gets rewritten to `rs-gaap:X`. Plain
# `us-gaap:` references (from dei / cross-references) are also folded in.
SOURCE_PREFIXES = (
  "us-gaap-2017:",
  "us-gaap-2020:",
  "us-gaap-2022:",
  "us-gaap-2024:",
  "us-gaap:",
)

# Rewrite source in element metadata too.
SOURCE_FIELD_REWRITE = {
  "us-gaap": "rs-gaap",
  "us-gaap-2017": "rs-gaap",
  "us-gaap-2020": "rs-gaap",
  "us-gaap-2022": "rs-gaap",
  "us-gaap-2024": "rs-gaap",
}


def _rewrite_iri(iri: str) -> str:
  """If IRI uses one of the source prefixes, swap for target prefix."""
  if not isinstance(iri, str):
    return iri
  for src in SOURCE_PREFIXES:
    if iri.startswith(src):
      return TARGET_PREFIX + iri[len(src) :]
  return iri


def _rewrite_node(node: Any) -> Any:
  """Walk a JSON-LD node tree and swap us-gaap-2017 → rs-gaap in @id values."""
  if isinstance(node, dict):
    out: dict[str, Any] = {}
    for k, v in node.items():
      if k == "@id" and isinstance(v, str):
        out[k] = _rewrite_iri(v)
      elif k == "@type":
        if isinstance(v, str):
          out[k] = _rewrite_iri(v)
        elif isinstance(v, list):
          out[k] = [_rewrite_iri(t) if isinstance(t, str) else t for t in v]
        else:
          out[k] = v
      elif k == "https://robosystems.ai/vocab/source" or k == "source":
        # Rewrite source field: "us-gaap" → "rs-gaap" (we own these now)
        out[k] = _rewrite_source(v)
      else:
        out[k] = _rewrite_node(v)
    return out
  if isinstance(node, list):
    return [_rewrite_node(x) for x in node]
  return node


def _rewrite_source(value: Any) -> Any:
  """Rewrite string/list/dict source values from us-gaap → rs-gaap."""
  if isinstance(value, str):
    return SOURCE_FIELD_REWRITE.get(value, value)
  if isinstance(value, list):
    return [_rewrite_source(v) for v in value]
  if isinstance(value, dict):
    # {"@value": "us-gaap"} style
    if "@value" in value and isinstance(value["@value"], str):
      return {
        **value,
        "@value": SOURCE_FIELD_REWRITE.get(value["@value"], value["@value"]),
      }
    return value
  return value


def _is_us_gaap_2017_node(node: Any) -> bool:
  """True if this @graph entry is a us-gaap (any version) concept node.

  Retained name for backward-compat — the check now covers all us-gaap
  versions that get rewritten to rs-gaap.
  """
  if not (isinstance(node, dict) and isinstance(node.get("@id"), str)):
    return False
  iri = node["@id"]
  return any(iri.startswith(src) for src in SOURCE_PREFIXES)


def build_rs_gaap_seed() -> int:
  if not FAC_PATH.exists():
    logger.error(f"Missing source seed: {FAC_PATH}")
    return 1

  doc = json.loads(FAC_PATH.read_text(encoding="utf-8"))
  graph: list[Any] = doc.get("@graph", [])

  # Split the @graph into: us-gaap-2017 nodes (→ rs-gaap seed) and
  # everything else (stays in fac with rewritten references).
  us_gaap_nodes: list[Any] = []
  other_nodes: list[Any] = []
  for node in graph:
    if _is_us_gaap_2017_node(node):
      us_gaap_nodes.append(node)
    else:
      other_nodes.append(node)

  logger.info(
    f"Partitioned @graph: {len(us_gaap_nodes)} us-gaap-2017 concepts → rs-gaap, "
    f"{len(other_nodes)} other (fac/infrastructure) → stay in fac"
  )

  # 1. Build rs-gaap seed — take us-gaap-2017 nodes, rename their @ids to
  # rs-gaap, and rewrite any substitutionGroup / type references they
  # carry.
  rs_gaap_graph = [_rewrite_node(n) for n in us_gaap_nodes]

  rs_gaap_doc: dict[str, Any] = {
    # Re-inject the up-to-date CANONICAL_CONTEXT so any prefixes added
    # after the source seed was generated (e.g. rs-gaap itself) resolve
    # correctly when the loader walks triples.
    "@context": CANONICAL_CONTEXT,
    "@graph": rs_gaap_graph,
    "standard": "rs-gaap",
    "version": "v1",
    "taxonomy_type": "reporting",
    "namespace_uri": "https://robosystems.ai/taxonomy/rs-gaap/v1/",
    "description": (
      "rs-gaap v1 — RoboSystems's year-independent canonical reporting "
      "taxonomy. Bootstrapped from Charlie Hoffman's FAC→us-gaap 2017 "
      "mapping: every us-gaap-2017 concept was renamed to rs-gaap under "
      "our namespace. Future us-gaap versions (2020, 2024, …) will be "
      "seeded as separate external taxonomies with equivalence arcs from "
      "rs-gaap, keeping our canonical stable as FASB evolves."
    ),
  }

  RS_GAAP_PATH.parent.mkdir(parents=True, exist_ok=True)
  RS_GAAP_PATH.write_text(
    json.dumps(rs_gaap_doc, indent=2, ensure_ascii=False),
    encoding="utf-8",
  )
  size_kb = RS_GAAP_PATH.stat().st_size / 1024
  logger.info(f"Wrote {RS_GAAP_PATH} ({size_kb:.1f} KB, {len(rs_gaap_graph)} elements)")

  # 2. Rewrite + split fac: concepts go to fac/v1 (reporting), arcs +
  # structures go to fac-to-rs-gaap/v1 (mapping).
  rewritten_other = [_rewrite_node(n) for n in other_nodes]
  fac_concept_nodes, fac_arc_nodes = _split_fac_nodes(rewritten_other)

  fac_doc: dict[str, Any] = {
    "@context": CANONICAL_CONTEXT,
    "@graph": fac_concept_nodes,
    "standard": "fac",
    "version": doc.get("version", "v1"),
    "taxonomy_type": "reporting",
    "namespace_uri": doc.get("namespace_uri", ""),
    "description": (
      "FAC (Fundamental Accounting Concepts) — Charlie Hoffman's 199 "
      "fundamental concepts under the fac: namespace. A browseable "
      "concept taxonomy in its own right. Equivalence arcs into rs-gaap "
      "live in the sibling fac-to-rs-gaap/v1 mapping taxonomy."
    ),
  }
  FAC_PATH.write_text(
    json.dumps(fac_doc, indent=2, ensure_ascii=False),
    encoding="utf-8",
  )
  logger.info(
    f"Wrote {FAC_PATH} "
    f"({FAC_PATH.stat().st_size / 1024:.1f} KB, "
    f"{len(fac_concept_nodes)} concept nodes)"
  )

  fac_to_rs_gaap_doc: dict[str, Any] = {
    "@context": CANONICAL_CONTEXT,
    "@graph": fac_arc_nodes,
    "standard": "fac-to-rs-gaap",
    "version": "v1",
    "taxonomy_type": "mapping",
    "namespace_uri": "https://robosystems.ai/taxonomy/fac-to-rs-gaap/v1/",
    "description": (
      "fac → rs-gaap mapping — Charlie Hoffman's ConceptMap equivalence "
      "arcs. Each fac: concept resolves to one or more rs-gaap: concepts "
      "via owl:equivalentClass. Kept as its own taxonomy so peer mappings "
      "(fac → ifrs, fac → us-gaap-2024, …) can be added without touching "
      "fac's concept layer."
    ),
  }
  FAC_TO_RS_GAAP_PATH.parent.mkdir(parents=True, exist_ok=True)
  FAC_TO_RS_GAAP_PATH.write_text(
    json.dumps(fac_to_rs_gaap_doc, indent=2, ensure_ascii=False),
    encoding="utf-8",
  )
  logger.info(
    f"Wrote {FAC_TO_RS_GAAP_PATH} "
    f"({FAC_TO_RS_GAAP_PATH.stat().st_size / 1024:.1f} KB, "
    f"{len(fac_arc_nodes)} arc/structure nodes)"
  )

  # 3. Same treatment for type-subtype seed — strip its us-gaap-2017
  # duplicate element entries (they live in rs-gaap now) and rewrite its
  # general-special arcs to target rs-gaap instead of us-gaap-2017.
  _rewrite_additional_seed(TYPE_SUBTYPE_PATH, "type-subtype")

  return 0


def _split_fac_nodes(
  nodes: list[Any],
) -> tuple[list[Any], list[Any]]:
  """Partition fac nodes into (concept-only, arc+structure).

  For each concept node we produce two views: the concept view with its
  identity + attributes but NO arc predicates, and the arc view with its
  identity + ONLY the arc predicates. Concept-only → fac/v1; arc-only +
  structure nodes → fac-to-rs-gaap/v1.
  """
  concepts: list[Any] = []
  arcs: list[Any] = []
  for node in nodes:
    if not isinstance(node, dict):
      # Non-dict entries (shouldn't happen but stay defensive) go with
      # concepts — they're usually already-normalized literals or lists.
      concepts.append(node)
      continue

    if _is_structure_node(node):
      arcs.append(node)
      continue

    if _is_concept_node(node):
      concept_view = {k: v for k, v in node.items() if k not in ARC_PREDICATES}
      arc_keys = [k for k in node if k in ARC_PREDICATES]
      if _has_meaningful_keys(concept_view):
        concepts.append(concept_view)
      if arc_keys:
        arc_view = {"@id": node["@id"]}
        for k in arc_keys:
          arc_view[k] = node[k]
        arcs.append(arc_view)
      continue

    # Blank nodes, reference citations, etc. — stay with concepts
    # (they're referenced from concept rows via dcterms:references).
    concepts.append(node)
  return concepts, arcs


def _is_concept_node(node: dict[str, Any]) -> bool:
  return any(k in node for k in _CONCEPT_MARKER_PREDS)


def _is_structure_node(node: dict[str, Any]) -> bool:
  return any(k in node for k in _STRUCTURE_MARKER_PREDS)


def _has_meaningful_keys(node: dict[str, Any]) -> bool:
  """True if node has more than just @id/@type/@context."""
  return any(k not in {"@id", "@type", "@context"} for k in node)


def _rewrite_additional_seed(path: Path, standard_name: str) -> None:
  """Rewrite references + keep renamed us-gaap nodes in another seed.

  Used for taxonomies like `type-subtype` whose Arelle ingest pulls in
  us-gaap concepts AND attaches arcs to them (general-special, etc).
  We rewrite every node's @id + predicate values from us-gaap-YYYY to
  rs-gaap — keeping the us-gaap nodes in the seed under our namespace
  so their arcs survive. Library writer de-dupes against rs-gaap via UUID5
  so the duplicate element rows are idempotent inserts.

  This is what lets type-subtype's 5,499+ general-special classification
  arcs land in the DB alongside rs-gaap's element rows.
  """
  if not path.exists():
    logger.warning(f"Seed not found, skipping rewrite: {path}")
    return

  doc = json.loads(path.read_text(encoding="utf-8"))
  graph: list[Any] = doc.get("@graph", [])

  # Rewrite ALL nodes (us-gaap-YYYY → rs-gaap in @id + nested @ids). No
  # stripping — we need the us-gaap nodes' arcs.
  rewritten = [_rewrite_node(n) for n in graph]

  # Count for diagnostics
  us_gaap_count = sum(1 for n in graph if _is_us_gaap_2017_node(n))
  logger.info(
    f"{standard_name}: {us_gaap_count} us-gaap nodes rewritten to rs-gaap "
    f"(arcs preserved); {len(graph) - us_gaap_count} other nodes kept"
  )

  new_doc: dict[str, Any] = {
    "@context": CANONICAL_CONTEXT,
    "@graph": rewritten,
    "standard": doc.get("standard", standard_name),
    "version": doc.get("version", "v1"),
    # Linkbase enrichments (arcs onto rs-gaap) get taxonomy_type='mapping'
    # to match fac. Preserves whatever was set in the source doc if present.
    "taxonomy_type": doc.get("taxonomy_type", "mapping"),
    "namespace_uri": doc.get("namespace_uri", ""),
    "description": doc.get("description"),
  }
  path.write_text(json.dumps(new_doc, indent=2, ensure_ascii=False), encoding="utf-8")
  size_kb = path.stat().st_size / 1024
  logger.info(f"Rewrote {path} ({size_kb:.1f} KB, {len(rewritten)} entries)")


def main() -> int:
  return build_rs_gaap_seed()


if __name__ == "__main__":
  sys.exit(main())
