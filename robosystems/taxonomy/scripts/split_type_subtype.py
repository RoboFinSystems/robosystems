"""Split ``rs-gaap-type-subtype`` into three linkbase packages (one-shot).

``rs-gaap-type-subtype`` was authored as a single file carrying three distinct
XBRL linkbases plus a redundant re-definition of every concept it touches:

  * **definition linkbase** — the general-special (``type/subtype``) arcs,
  * **reference linkbase** — FASB ASC citations (``dcterms:references``),
  * **label linkbase** — supplementary labels (``rdfs:label`` / ``rdfs:comment``
    + ``rs:labelRole`` total-label nodes),
  * concept stubs re-stating ``balance`` / ``periodType`` / ``elementType`` /
    ``abstract`` / … that the **rs-gaap base already owns** (verified: 0
    base-vs-type-subtype disagreements on balance/periodType; the only
    divergences are 159 ``*Member`` / ``*Domain`` ``elementType`` values type-
    subtype flips to ``abstract`` — base's ``member`` is the correct value and
    wins after the split).

This pass rewrites the file into three packages, each a clean single-purpose
linkbase, and **drops the redundant concept attribute re-definitions** (base is
the sole authority for concept attributes):

  * ``rs-gaap-type-subtype/v1`` — arcs + the classification ELR structures only,
  * ``rs-gaap-references/v1``   — ``@id`` → ``dcterms:references`` nodes (+ their
    citation blank nodes),
  * ``rs-gaap-labels/v1``       — ``@id`` → ``rdfs:label`` / ``rdfs:comment`` /
    ``rs:labelRole`` nodes (+ their label-role blank nodes).

The references/labels nodes carry NO concept attributes, so the loader treats
them as label-/reference-linkbase entries and attaches them to the base concept
**by qname** (see ``taxonomy/loader.py`` + ``library_creator.create_library_arcs``).
The deterministic label/reference ids key on ``(element_id, role, language)`` /
``(element_id, citation)``, so every moved row reseeds byte-identically.

Idempotent: if the source file no longer carries concept stubs (already split),
the script reports that and writes nothing.

    uv run python -m robosystems.taxonomy.scripts.split_type_subtype
"""

from __future__ import annotations

import json
from typing import Any

from robosystems.taxonomy.discovery import FRAMEWORKS_DIR

_PKGS = FRAMEWORKS_DIR / "rs-gaap" / "packages"
_SRC = _PKGS / "rs-gaap-type-subtype" / "v1" / "taxonomy.jsonld"
_REF_OUT = _PKGS / "rs-gaap-references" / "v1" / "taxonomy.jsonld"
_LBL_OUT = _PKGS / "rs-gaap-labels" / "v1" / "taxonomy.jsonld"

# Raw predicate keys, exactly as they appear in the on-disk JSON-LD.
_ARC_KEY = "from"  # reified rs:Association node marker (xlink:from, compacted)
_STRUCT_KEY = "https://robosystems.ai/vocab/roleUri"  # classification ELR node
_REF_KEY = "http://purl.org/dc/terms/references"
_LABEL_KEYS = (
  "http://www.w3.org/2000/01/rdf-schema#label",
  "http://www.w3.org/2000/01/rdf-schema#comment",
  "https://robosystems.ai/vocab/labelRole",
)


def _blank_refs(value: Any) -> list[str]:
  """Blank-node @ids ('_:...') referenced anywhere inside a JSON value."""
  out: list[str] = []

  def walk(v: Any) -> None:
    if isinstance(v, dict):
      i = v.get("@id")
      if isinstance(i, str) and i.startswith("_:"):
        out.append(i)
      for vv in v.values():
        walk(vv)
    elif isinstance(v, list):
      for vv in v:
        walk(vv)

  walk(value)
  return out


def _collect_blanks(seed_ids: set[str], by_id: dict[str, dict]) -> list[dict]:
  """Transitively pull every blank node reachable from ``seed_ids``."""
  out: dict[str, dict] = {}
  frontier = set(seed_ids)
  while frontier:
    nxt: set[str] = set()
    for bid in frontier:
      if bid in out:
        continue
      node = by_id.get(bid)
      if node is None:
        continue
      out[bid] = node
      for v in node.values():
        for b in _blank_refs(v):
          if b not in out:
            nxt.add(b)
    frontier = nxt
  return list(out.values())


def main() -> None:
  doc = json.loads(_SRC.read_text(encoding="utf-8"))
  context = doc["@context"]
  graph = doc["@graph"]
  by_id = {n["@id"]: n for n in graph if isinstance(n, dict) and "@id" in n}

  arc_nodes: list[dict] = []
  struct_nodes: list[dict] = []
  ref_nodes: list[dict] = []
  label_nodes: list[dict] = []
  ref_blank_seed: set[str] = set()
  label_blank_seed: set[str] = set()

  for n in graph:
    if not isinstance(n, dict):
      continue
    aid = n.get("@id", "")
    if aid.startswith("_:"):
      continue  # blank nodes are pulled by reachability from kept nodes
    keys = set(n.keys())
    if _ARC_KEY in keys:
      arc_nodes.append(n)
      continue
    if _STRUCT_KEY in keys:
      struct_nodes.append(n)
      continue
    # Concept stub node: split its reference + label predicates into the two
    # new linkbases and drop the (redundant) concept attributes entirely.
    # Only rs-gaap concepts belong in the rs-gaap-* linkbases — srt: / cm: /
    # fasb codification stubs never resolve to an rs-gaap element (their refs/
    # labels were never loaded; the loader source-skips them), so they're
    # dropped as residue rather than carried as inert nodes. Their general-
    # special arcs stay in the arcs file (resolution is unchanged).
    if not aid.startswith("rs-gaap:"):
      continue
    if _REF_KEY in keys:
      ref_nodes.append({"@id": aid, _REF_KEY: n[_REF_KEY]})
      ref_blank_seed.update(_blank_refs(n[_REF_KEY]))
    label_part = {k: n[k] for k in _LABEL_KEYS if k in keys}
    if label_part:
      label_nodes.append({"@id": aid, **label_part})
      for v in label_part.values():
        label_blank_seed.update(_blank_refs(v))

  if not ref_nodes and not label_nodes:
    print(f"{_SRC} carries no concept stubs — already split. Nothing to do.")
    return

  ref_blanks = _collect_blanks(ref_blank_seed, by_id)
  label_blanks = _collect_blanks(label_blank_seed, by_id)

  # 1) Rewrite type-subtype as arcs + classification structures only.
  arc_doc = {k: v for k, v in doc.items() if k not in ("@context", "@graph")}
  arc_doc["description"] = (
    "rs-gaap type-subtype classification linkbase — general-special "
    "(type/subtype) arcs that classify rs-gaap concepts into reporting "
    "networks (CurrentAssets, Revenues, OperatingExpenses, …). Arcs only: the "
    "ASC citations split out to rs-gaap-references and the supplementary "
    "labels to rs-gaap-labels; concept attributes live in the rs-gaap base. "
    "RoboSystems canonical fork (frozen 2026-04-19) of Charlie Hoffman's "
    "type-subtype-usgaap linkbase."
  )
  out_arc = {"@context": context, **arc_doc, "@graph": arc_nodes + struct_nodes}

  # 2) rs-gaap-references — ASC citation reference linkbase.
  ns_base = "https://robosystems.ai/taxonomy"
  out_ref = {
    "@context": context,
    "standard": "rs-gaap-references",
    "version": "v1",
    "taxonomy_type": "mapping",
    "namespace_uri": f"{ns_base}/rs-gaap-references/v1/",
    "forked_from": doc.get("forked_from"),
    "forked_at": doc.get("forked_at"),
    "upstream_tracking": doc.get("upstream_tracking"),
    "description": (
      "rs-gaap reference linkbase — FASB ASC citations attached to rs-gaap "
      "concepts by qname (split out of rs-gaap-type-subtype). Attaches to base "
      "concept definitions; defines no concepts itself."
    ),
    "@graph": ref_nodes + ref_blanks,
  }

  # 3) rs-gaap-labels — supplementary + total-role label linkbase.
  out_lbl = {
    "@context": context,
    "standard": "rs-gaap-labels",
    "version": "v1",
    "taxonomy_type": "mapping",
    "namespace_uri": f"{ns_base}/rs-gaap-labels/v1/",
    "forked_from": doc.get("forked_from"),
    "forked_at": doc.get("forked_at"),
    "upstream_tracking": doc.get("upstream_tracking"),
    "description": (
      "rs-gaap label linkbase — supplementary and total-role labels attached "
      "to rs-gaap concepts by qname (split out of rs-gaap-type-subtype). "
      "Attaches to base concept definitions; defines no concepts itself."
    ),
    "@graph": label_nodes + label_blanks,
  }

  _REF_OUT.parent.mkdir(parents=True, exist_ok=True)
  _LBL_OUT.parent.mkdir(parents=True, exist_ok=True)
  for path, payload in ((_SRC, out_arc), (_REF_OUT, out_ref), (_LBL_OUT, out_lbl)):
    path.write_text(
      json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )

  print(f"Wrote {_SRC}")
  print(f"  arcs={len(arc_nodes)} structures={len(struct_nodes)}")
  print(f"Wrote {_REF_OUT}")
  print(f"  reference nodes={len(ref_nodes)} citation blanks={len(ref_blanks)}")
  print(f"Wrote {_LBL_OUT}")
  print(f"  label nodes={len(label_nodes)} label-role blanks={len(label_blanks)}")


if __name__ == "__main__":
  main()
