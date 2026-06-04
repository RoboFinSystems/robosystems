"""Backfill missing ``en`` standard labels onto base rs-gaap concept nodes.

A base rs-gaap concept whose standard label lives ONLY in the ``rs-gaap-labels``
linkbase (under ``@language="en-US"``) has no ``rdfs:label`` on its own node, so
``loader.py`` falls back to the bare QName local-part for ``Element.name`` — the
concept then renders as a raw CamelCase QName (``CostOfGoodsSold``,
``PropertyPlantAndEquipmentNet``, …) in every name-driven surface, even though a
clean human label is one join away in ``element_labels``.

This one-shot transform copies that ``en-US`` standard label verbatim onto the
base concept node as an ``en`` ``rdfs:label`` (the canonical pattern used by
concepts like ``GrossProfit`` that already carry one), so the loader promotes it
into ``Element.name``. It does NOT touch ``rs-gaap-labels`` — the ``en-US`` entry
stays as the regional variant (identical text), so the change is additive and
the Part-A by-qname attach stays byte-identical.

Idempotent: a concept that already has an ``en`` ``rdfs:label`` is skipped.
Round-trips byte-identically (indent=2, ensure_ascii=False) on a no-op run.

    uv run python -m robosystems.taxonomy.scripts.backfill_base_en_labels [--dry-run]
"""

from __future__ import annotations

import argparse
import json

from robosystems.taxonomy.discovery import framework_root

_BASE = framework_root("rs-gaap") / "packages" / "rs-gaap" / "v1" / "taxonomy.jsonld"
_LABELS = (
  framework_root("rs-gaap") / "packages" / "rs-gaap-labels" / "v1" / "taxonomy.jsonld"
)

_RDFS_LABEL_COMPACT = "rdfs:label"
_RDFS_LABEL_FULL = "http://www.w3.org/2000/01/rdf-schema#label"


def _label_values(node: dict) -> list[dict]:
  """All rdfs:label value objects on a node (compact or expanded predicate)."""
  out: list[dict] = []
  for pred in (_RDFS_LABEL_COMPACT, _RDFS_LABEL_FULL):
    vals = node.get(pred)
    if isinstance(vals, list):
      out.extend(v for v in vals if isinstance(v, dict))
  return out


def _has_en_label(node: dict) -> bool:
  return any(v.get("@language") == "en" for v in _label_values(node))


def _standard_label_text(node: dict) -> str | None:
  """The standard label text from a labels-package node — prefer en, then en-US,
  then any. The labels package carries the standard role as a plain rdfs:label."""
  vals = _label_values(node)
  for lang in ("en", "en-US"):
    for v in vals:
      if v.get("@language") == lang and v.get("@value"):
        return v["@value"]
  for v in vals:
    if v.get("@value"):
      return v["@value"]
  return None


def backfill(dry_run: bool = False) -> int:
  base = json.loads(_BASE.read_text())
  labels = json.loads(_LABELS.read_text())

  label_text_by_id: dict[str, str] = {}
  for n in labels.get("@graph", []):
    if isinstance(n, dict) and isinstance(n.get("@id"), str):
      text = _standard_label_text(n)
      if text is not None:
        label_text_by_id[n["@id"]] = text

  added: list[tuple[str, str]] = []
  for n in base.get("@graph", []):
    if not isinstance(n, dict):
      continue
    qid = n.get("@id")
    if not isinstance(qid, str) or _has_en_label(n):
      continue
    text = label_text_by_id.get(qid)
    if text is None:
      continue
    n[_RDFS_LABEL_COMPACT] = [{"@value": text, "@language": "en"}]
    added.append((qid, text))

  print(f"base concepts missing en label that gained one: {len(added)}")
  for qid, text in sorted(added)[:12]:
    print(f"  {qid:55} → {text}")
  if len(added) > 12:
    print(f"  … and {len(added) - 12} more")

  if not dry_run and added:
    _BASE.write_text(json.dumps(base, indent=2, ensure_ascii=False) + "\n")
  return len(added)


if __name__ == "__main__":
  ap = argparse.ArgumentParser()
  ap.add_argument("--dry-run", action="store_true")
  args = ap.parse_args()
  backfill(dry_run=args.dry_run)
