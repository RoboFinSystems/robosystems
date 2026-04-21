"""Load Charlie Hoffman's FAC (Financial Accounting Concepts) taxonomy via Arelle.

This is a POC script for the taxonomy library initiative. Goals:

1. Validate that the existing SEC-scoped ArelleClient can ingest a non-SEC
   taxonomy without modification. This tells us what SEC-specific plumbing
   really is vs. what's generic platform infrastructure.
2. Inventory what's in Charlie's FAC (concepts, labels, relationships,
   arcroles, roles) so we know what a JSON-LD serializer needs to cover.
3. Surface any load errors / missing dependencies so we know what Arelle
   cache state is required for non-SEC ingest.

Usage:
    uv run python -m robosystems.scripts.load_fac_taxonomy
    uv run python -m robosystems.scripts.load_fac_taxonomy --url <taxonomy-url>
    uv run python -m robosystems.scripts.load_fac_taxonomy --verbose
"""

from __future__ import annotations

import argparse
import sys
from collections import Counter, defaultdict
from typing import Any

from robosystems.adapters.sec.client.arelle import ArelleClient
from robosystems.logger import logger

FAC_REPORT_XSD = "https://xbrlsite.azurewebsites.net/seattlemethod/fac/report.xsd"


def _qname_ns_prefix(qname: Any) -> str:
  """Best-effort namespace prefix for a concept qname."""
  if qname is None:
    return "(none)"
  prefix = getattr(qname, "prefix", None)
  if prefix:
    return prefix
  ns = getattr(qname, "namespaceURI", None)
  return ns or "(unknown)"


def _summarize_concepts(model_xbrl) -> dict[str, Any]:
  """Count concepts by namespace prefix; sample a few from each."""
  by_prefix: dict[str, list[Any]] = defaultdict(list)
  for concept in model_xbrl.qnameConcepts.values():
    prefix = _qname_ns_prefix(concept.qname)
    by_prefix[prefix].append(concept)

  return {
    "total": len(model_xbrl.qnameConcepts),
    "by_prefix": {
      prefix: len(concepts) for prefix, concepts in sorted(by_prefix.items())
    },
    "samples_by_prefix": {
      prefix: [str(c.qname) for c in concepts[:3]]
      for prefix, concepts in sorted(by_prefix.items())
    },
  }


def _summarize_relationships(model_xbrl) -> dict[str, Any]:
  """Walk all relationship sets; count by arcrole."""
  arcrole_counts: Counter[str] = Counter()
  role_uris: set[str] = set()
  equivalence_samples: list[tuple[str, str]] = []

  for baseset_key in model_xbrl.baseSets:
    arcrole, role, link_qname, arc_qname = baseset_key
    if not arcrole or not role:
      continue
    try:
      rel_set = model_xbrl.relationshipSet(arcrole, role, link_qname, arc_qname)
    except Exception:
      continue
    if not rel_set or not rel_set.modelRelationships:
      continue

    count = len(rel_set.modelRelationships)
    arcrole_counts[arcrole] += count
    role_uris.add(role)

    # Collect sample equivalence arcs (FAC's class-equivalentClass)
    if "equivalentClass" in arcrole and len(equivalence_samples) < 15:
      for rel in rel_set.modelRelationships[: 15 - len(equivalence_samples)]:
        from_q = (
          str(rel.fromModelObject.qname) if rel.fromModelObject is not None else "?"
        )
        to_q = str(rel.toModelObject.qname) if rel.toModelObject is not None else "?"
        equivalence_samples.append((from_q, to_q))

  return {
    "arcroles": dict(arcrole_counts.most_common()),
    "role_count": len(role_uris),
    "sample_roles": sorted(role_uris)[:5],
    "equivalence_samples": equivalence_samples,
  }


def _summarize_labels(model_xbrl, sample_count: int = 5) -> dict[str, Any]:
  """Inventory label linkbase roles + samples."""
  from arelle import XbrlConst

  label_rel_set = model_xbrl.relationshipSet(XbrlConst.conceptLabel)
  if not label_rel_set or not label_rel_set.modelRelationships:
    return {"total": 0, "roles": {}, "samples": []}

  role_counts: Counter[str] = Counter()
  lang_counts: Counter[str] = Counter()
  samples: list[dict[str, str]] = []

  for rel in label_rel_set.modelRelationships:
    label = rel.toModelObject
    if label is None:
      continue
    role = getattr(label, "role", "(no role)") or "(no role)"
    lang = getattr(label, "xmlLang", "(no lang)") or "(no lang)"
    role_counts[role] += 1
    lang_counts[lang] += 1
    if len(samples) < sample_count:
      concept = rel.fromModelObject
      samples.append(
        {
          "concept": str(concept.qname) if concept is not None else "?",
          "role": role.rsplit("/", 1)[-1] if role else "?",
          "lang": lang,
          "text": (label.textValue or "")[:80],
        }
      )

  return {
    "total": len(label_rel_set.modelRelationships),
    "roles": dict(role_counts.most_common()),
    "langs": dict(lang_counts.most_common()),
    "samples": samples,
  }


def _summarize_concept(concept: Any) -> dict[str, Any]:
  """Flatten a single ModelConcept to key fields."""
  return {
    "qname": str(concept.qname),
    "name": concept.name,
    "type": str(concept.typeQname) if concept.typeQname else None,
    "substitution_group": str(concept.substitutionGroupQname)
    if concept.substitutionGroupQname
    else None,
    "balance": getattr(concept, "balance", None),
    "period_type": getattr(concept, "periodType", None),
    "abstract": getattr(concept, "isAbstract", False),
    "nillable": getattr(concept, "isNillable", False),
  }


def load_and_summarize(url: str, verbose: bool = False) -> int:
  """Load a taxonomy and print a summary. Returns exit code."""
  print(f"\n{'=' * 72}")
  print(f"Loading taxonomy: {url}")
  print(f"{'=' * 72}\n")

  client = ArelleClient()
  try:
    model_xbrl = client.controller(url)
  except Exception as e:
    logger.error(f"Failed to load taxonomy: {e}")
    return 1

  if model_xbrl is None or model_xbrl.modelDocument is None:
    print("ERROR: Arelle returned an empty ModelXbrl.")
    return 1

  # --- Errors / warnings ---------------------------------------------------
  errors = list(model_xbrl.errors or [])
  print(f"Arelle errors during load: {len(errors)}")
  if errors and verbose:
    for err in errors[:10]:
      print(f"  - {err}")
    if len(errors) > 10:
      print(f"  ... ({len(errors) - 10} more)")
  print()

  # --- Document graph ------------------------------------------------------
  docs = model_xbrl.urlDocs
  print(f"Loaded documents: {len(docs)}")
  if verbose:
    for doc_url in list(docs.keys())[:20]:
      print(f"  - {doc_url}")
    if len(docs) > 20:
      print(f"  ... ({len(docs) - 20} more)")
  print()

  # --- Concepts ------------------------------------------------------------
  concepts = _summarize_concepts(model_xbrl)
  print(f"Total concepts: {concepts['total']}")
  print("Concepts by namespace prefix:")
  for prefix, count in concepts["by_prefix"].items():
    print(f"  {prefix:<20} {count:>6}")
  print()

  if verbose:
    print("Sample concepts by prefix:")
    for prefix, samples in concepts["samples_by_prefix"].items():
      print(f"  {prefix}:")
      for qn in samples:
        concept = model_xbrl.qnameConcepts.get(
          next(q for q in model_xbrl.qnameConcepts if str(q) == qn)
        )
        if concept:
          summary = _summarize_concept(concept)
          print(
            f"    {summary['qname']:<60} "
            f"bal={summary['balance']} "
            f"per={summary['period_type']} "
            f"abstract={summary['abstract']}"
          )
    print()

  # --- Relationships / linkbases -------------------------------------------
  rels = _summarize_relationships(model_xbrl)
  print(f"Extended link roles in use: {rels['role_count']}")
  if rels["sample_roles"]:
    print("Sample role URIs:")
    for r in rels["sample_roles"]:
      print(f"  {r}")
    print()

  print("Relationships by arcrole:")
  for arcrole, count in rels["arcroles"].items():
    short = arcrole.rsplit("/", 1)[-1] if arcrole else "(none)"
    print(f"  {short:<35} {count:>6}   [{arcrole}]")
  print()

  if rels["equivalence_samples"]:
    print(
      f"Sample class-equivalentClass arcs (FAC → us-gaap, first {len(rels['equivalence_samples'])}):"
    )
    for from_q, to_q in rels["equivalence_samples"]:
      print(f"  {from_q}  ≡  {to_q}")
    print()
  else:
    print(
      "No class-equivalentClass arcs found — check whether FAC mapping linkbase loaded."
    )
    print()

  # --- Labels --------------------------------------------------------------
  labels = _summarize_labels(model_xbrl, sample_count=5 if not verbose else 15)
  print(f"Total label arcs: {labels['total']}")
  if labels["roles"]:
    print("Labels by role:")
    for role, count in list(labels["roles"].items())[:10]:
      short = role.rsplit("/", 1)[-1] if role else "(none)"
      print(f"  {short:<20} {count:>6}")
    print()
  if labels["langs"]:
    print(f"Label languages: {dict(labels['langs'])}")
    print()
  if labels["samples"]:
    print("Sample labels:")
    for s in labels["samples"]:
      print(f"  {s['concept']:<50} [{s['role']:<16}] [{s['lang']}] {s['text']}")
    print()

  print(f"{'=' * 72}")
  print("Load complete. This taxonomy is ingestable via the existing ArelleClient.")
  print(f"{'=' * 72}\n")
  return 0


def main() -> int:
  parser = argparse.ArgumentParser(description=__doc__)
  parser.add_argument(
    "--url",
    default=FAC_REPORT_XSD,
    help=f"Taxonomy URL to load (default: {FAC_REPORT_XSD})",
  )
  parser.add_argument(
    "--verbose",
    "-v",
    action="store_true",
    help="Print document list, full concept samples, more label samples",
  )
  args = parser.parse_args()
  return load_and_summarize(args.url, verbose=args.verbose)


if __name__ == "__main__":
  sys.exit(main())
