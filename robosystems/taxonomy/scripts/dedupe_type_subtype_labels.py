"""Strip duplicate concept labels/comments from rs-gaap-type-subtype.

``rs-gaap-type-subtype`` is a general-special **arc** package, but it was
generated carrying a full second definition of every concept — including an
``rdfs:label`` (standard) and ``rdfs:comment`` (documentation) in ``en-US`` that
duplicate the canonical ``en`` labels the ``rs-gaap`` base already defines. The
loader faithfully ingests both, so every such concept ends up with twin
``en`` / ``en-US`` labels in the library (and in every tenant copy).

This one-shot, **idempotent** maintenance pass removes a concept node's
``rdfs:label`` / ``rdfs:comment`` from type-subtype **only when the rs-gaap base
defines the same ``@id`` with the same text** — so a label is only ever dropped
where the base already provides it, never orphaned. Left untouched: the
``dcterms:references`` (ASC citations — unique to type-subtype), the
``rs:labelRole`` blank nodes (``total`` labels — no base twin), the
general-special arcs, and the element attributes.

Run against the on-disk seed, then re-seed (``just reset-local``) to refresh the
public library + every fresh tenant copy:

    uv run python -m robosystems.taxonomy.scripts.dedupe_type_subtype_labels
"""

from __future__ import annotations

import json

from robosystems.taxonomy.discovery import FRAMEWORKS_DIR

_BASE = FRAMEWORKS_DIR / "rs-gaap" / "packages" / "rs-gaap" / "v1" / "taxonomy.jsonld"
_TYPE_SUBTYPE = (
  FRAMEWORKS_DIR
  / "rs-gaap"
  / "packages"
  / "rs-gaap-type-subtype"
  / "v1"
  / "taxonomy.jsonld"
)

# The two seed files express the same predicate differently — the rs-gaap base
# uses the compact ``rdfs:`` form, type-subtype uses the expanded IRI — so match
# both when reading and strip whichever is present.
_LABEL_KEYS = ("rdfs:label", "http://www.w3.org/2000/01/rdf-schema#label")
_COMMENT_KEYS = ("rdfs:comment", "http://www.w3.org/2000/01/rdf-schema#comment")


def _texts(node: dict, keys: tuple[str, ...]) -> set[str]:
  out: set[str] = set()
  for key in keys:
    for v in node.get(key, []):
      if isinstance(v, dict) and "@value" in v:
        out.add(v["@value"])
  return out


def _strip_keys(node: dict, keys: tuple[str, ...]) -> bool:
  removed = False
  for key in keys:
    if key in node:
      del node[key]
      removed = True
  return removed


def _is_concept_node(node: object) -> bool:
  return (
    isinstance(node, dict)
    and isinstance(node.get("@id"), str)
    and not node["@id"].startswith("_:")
  )


def main() -> None:
  base = json.loads(_BASE.read_text(encoding="utf-8"))
  base_label: dict[str, set[str]] = {}
  base_comment: dict[str, set[str]] = {}
  for node in base.get("@graph", []):
    if _is_concept_node(node):
      base_label[node["@id"]] = _texts(node, _LABEL_KEYS)
      base_comment[node["@id"]] = _texts(node, _COMMENT_KEYS)

  raw = _TYPE_SUBTYPE.read_text(encoding="utf-8")
  doc = json.loads(raw)
  removed_labels = removed_comments = 0
  for node in doc.get("@graph", []):
    if not _is_concept_node(node):
      continue
    aid = node["@id"]
    # Subset test: drop only when *every* text type-subtype carries for this
    # concept is also in the base — i.e. pure redundancy, nothing unique lost.
    ts_label = _texts(node, _LABEL_KEYS)
    if ts_label and ts_label <= base_label.get(aid, set()):
      if _strip_keys(node, _LABEL_KEYS):
        removed_labels += 1
    ts_comment = _texts(node, _COMMENT_KEYS)
    if ts_comment and ts_comment <= base_comment.get(aid, set()):
      if _strip_keys(node, _COMMENT_KEYS):
        removed_comments += 1

  out = json.dumps(doc, indent=2, ensure_ascii=False)
  if raw.endswith("\n"):
    out += "\n"
  _TYPE_SUBTYPE.write_text(out, encoding="utf-8")
  print(f"Rewrote {_TYPE_SUBTYPE}")
  print(
    f"  removed {removed_labels} redundant rdfs:label + "
    f"{removed_comments} redundant rdfs:comment (base-confirmed duplicates)"
  )


if __name__ == "__main__":
  main()
