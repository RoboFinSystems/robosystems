"""Consolidate all rs-gaap labels to a single ``en`` language.

After the Part-A split + the base label backfill, rs-gaap labels exist in two
languages: ``en`` (the newer us-gaap label vintage, on the base concept nodes)
and ``en-US`` (an older vintage, carried by the ``rs-gaap-labels`` linkbase).
They never deduped because the text differs by capitalization / plural / Oxford
comma ("Long-**T**erm" vs "Long-**t**erm"). Reports are unaffected (they render
``Element.name``, which is the ``en`` label), but the CoA-mapping browse shows
two near-identical labels per concept.

This one-shot transform makes every label ``en`` and removes the redundancy:

- **base** (``rs-gaap``): relabel any ``en-US`` ``rdfs:label`` / ``rdfs:comment``
  to ``en``; if both languages are present on a node, keep ``en`` (drop the
  older ``en-US``).
- **rs-gaap-labels**: a concept-node ``rdfs:label`` / ``rdfs:comment`` is
  DROPPED when the base already provides one (base's newer ``en`` wins — the
  seeder collides them on the deterministic ``_label_id`` and keeps base
  anyway, so this just makes the source match the seeded result); otherwise the
  sole label is relabeled ``en-US`` → ``en``. The supplementary total /
  periodStart / periodEnd blank-node labels (``labelLanguage``) are always
  relabeled ``en`` and kept (no base twin). Concept nodes left with only an
  ``@id`` are removed.

End state: one label per (concept, role), all ``en``, base's newer text wins.
Round-trips byte-identically (indent=2) on a no-op re-run.

    uv run python -m robosystems.taxonomy.scripts.consolidate_labels_to_en [--dry-run]
"""

from __future__ import annotations

import argparse
import json

from robosystems.taxonomy.discovery import framework_root

_BASE = framework_root("rs-gaap") / "packages" / "rs-gaap" / "v1" / "taxonomy.jsonld"
_LABELS = (
  framework_root("rs-gaap") / "packages" / "rs-gaap-labels" / "v1" / "taxonomy.jsonld"
)

_LABEL_PREDS = ("rdfs:label", "http://www.w3.org/2000/01/rdf-schema#label")
_COMMENT_PREDS = ("rdfs:comment", "http://www.w3.org/2000/01/rdf-schema#comment")
_LABEL_LANG = "https://robosystems.ai/vocab/labelLanguage"
_LABEL_ROLE = "https://robosystems.ai/vocab/labelRole"
_ROLE = "https://robosystems.ai/vocab/role"


def _normalize_base_node(node: dict) -> int:
  """Relabel en-US -> en on a base node's rdfs:label / rdfs:comment, dropping
  the en-US value when an en value already exists. Returns # of values changed
  or dropped."""
  touched = 0
  for preds in (_LABEL_PREDS, _COMMENT_PREDS):
    for pred in preds:
      vals = node.get(pred)
      if not isinstance(vals, list):
        continue
      has_en = any(isinstance(v, dict) and v.get("@language") == "en" for v in vals)
      kept: list = []
      for v in vals:
        if isinstance(v, dict) and v.get("@language") == "en-US":
          if has_en:
            touched += 1
            continue  # drop older en-US, keep en
          v["@language"] = "en"
          touched += 1
        kept.append(v)
      node[pred] = kept
  return touched


def _base_provides(base: dict) -> tuple[set[str], set[str]]:
  """(qnames base defines a standard label for, qnames base defines a comment
  for) — any language, evaluated AFTER base normalization."""
  std: set[str] = set()
  doc: set[str] = set()
  for n in base.get("@graph", []):
    if not isinstance(n, dict) or not isinstance(n.get("@id"), str):
      continue
    if any(isinstance(n.get(p), list) and n[p] for p in _LABEL_PREDS):
      std.add(n["@id"])
    if any(isinstance(n.get(p), list) and n[p] for p in _COMMENT_PREDS):
      doc.add(n["@id"])
  return std, doc


def consolidate(dry_run: bool = False) -> dict:
  base = json.loads(_BASE.read_text())
  labels = json.loads(_LABELS.read_text())
  counts = {
    "base_relabeled_or_dropped": 0,
    "labels_standard_dropped": 0,
    "labels_comment_dropped": 0,
    "labels_relabeled_en": 0,
    "labels_blank_relabeled": 0,
    "labels_empty_nodes_removed": 0,
  }

  # 1) base: relabel/dedup to en
  for n in base.get("@graph", []):
    if isinstance(n, dict):
      counts["base_relabeled_or_dropped"] += _normalize_base_node(n)

  base_std, base_doc = _base_provides(base)

  # 2) rs-gaap-labels: drop where base provides, else relabel to en
  new_graph: list = []
  for n in labels.get("@graph", []):
    if not isinstance(n, dict):
      new_graph.append(n)
      continue
    qid = n.get("@id", "")

    # blank-node supplementary label (total / periodStart / periodEnd)
    if _LABEL_LANG in n:
      for v in n.get(_LABEL_LANG, []):
        if isinstance(v, dict) and v.get("@value") == "en-US":
          v["@value"] = "en"
          counts["labels_blank_relabeled"] += 1
      new_graph.append(n)
      continue

    # concept node — standard (rdfs:label) + documentation (rdfs:comment)
    is_concept = qid.startswith("rs-gaap:")
    if is_concept:
      for pred in _LABEL_PREDS:
        if pred in n:
          if qid in base_std:
            del n[pred]
            counts["labels_standard_dropped"] += 1
          else:
            for v in n[pred]:
              if isinstance(v, dict) and v.get("@language") == "en-US":
                v["@language"] = "en"
                counts["labels_relabeled_en"] += 1
      for pred in _COMMENT_PREDS:
        if pred in n:
          if qid in base_doc:
            del n[pred]
            counts["labels_comment_dropped"] += 1
          else:
            for v in n[pred]:
              if isinstance(v, dict) and v.get("@language") == "en-US":
                v["@language"] = "en"
                counts["labels_relabeled_en"] += 1

    # drop a now-empty concept node (only @id, no label/comment/labelRole left)
    content_keys = [k for k in n if k not in ("@id", "@type")]
    if is_concept and not content_keys:
      counts["labels_empty_nodes_removed"] += 1
      continue
    new_graph.append(n)
  labels["@graph"] = new_graph

  if not dry_run:
    _BASE.write_text(json.dumps(base, indent=2, ensure_ascii=False) + "\n")
    _LABELS.write_text(json.dumps(labels, indent=2, ensure_ascii=False) + "\n")
  return counts


if __name__ == "__main__":
  ap = argparse.ArgumentParser()
  ap.add_argument("--dry-run", action="store_true")
  args = ap.parse_args()
  print(json.dumps(consolidate(dry_run=args.dry_run), indent=2))
