#!/usr/bin/env python3
"""Load the Seattle Method ``mini`` reporting framework as a CoA.

Parses Charlie Hoffman's mini XBRL taxonomy (already pulled into
``local/taxonomies/mini/`` by ``pull_mini.sh``) and ingests it as a
``chart_of_accounts`` Taxonomy Block — identical shape to how a
QuickBooks CoA import or a hand-authored CoA (see
``examples.roboledger_demo.create_chart_of_accounts``) lands in the
system. Mini concepts become Element rows; mini's presentation
linkbases supply the parent_ref hierarchy.

**Sources parsed** (all under ``local/taxonomies/mini/``):

- ``mini.xsd`` — concept declarations: name, type, ``xbrli:balance``,
  ``xbrli:periodType``, ``substitutionGroup`` (for abstract/hypercube
  detection).
- ``mini-lab.xml`` — human-readable labels (the Element ``name`` field).
- ``mini-pre-*.xml`` — presentation linkbases. Each file is one
  XBRL Network covering one statement or disclosure. A concept can
  appear in multiple networks; we use a canonical-parent priority
  (BalanceSheet > IncomeStatement > CashFlowStatement > ChangesInEquity
  > roll-forwards > everything else) so each Element ends up with a
  single deterministic parent_ref.

**Why this is a CoA shape, not a "load the full taxonomy" shape**:
Charlie's mini collapses the chart-of-accounts and the reporting
framework into one structure (his "fix the process" thesis — every GL
account IS its own reporting concept). We treat it the same way our
QB sync treats QB accounts — as a tenant CoA. Future cross-taxonomy
test cases (us-gaap, IFRS) would each be loaded the same way into
their own tenant graphs.

Usage:
    uv run python -m examples.seattle_method_demo.load_taxonomy <graph_id>
    uv run python -m examples.seattle_method_demo.load_taxonomy <graph_id> --taxonomy-dir <path>
"""

from __future__ import annotations

import argparse
import sys
import xml.etree.ElementTree as ET
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

# ── XBRL namespaces ──────────────────────────────────────────────────────

NS_XSD = "{http://www.w3.org/2001/XMLSchema}"
NS_XLINK = "{http://www.w3.org/1999/xlink}"
NS_LINK = "{http://www.xbrl.org/2003/linkbase}"
NS_XBRLI = "{http://www.xbrl.org/2003/instance}"

# Canonical-parent priority. When a concept appears in multiple
# presentation networks, the first match wins. The order reflects
# accounting practice: BS positions outrank IS / CF / SE outranks
# detail roll-forwards outranks subclassification disclosures.
PRESENTATION_PRIORITY = (
  "mini-pre-BalanceSheet.xml",
  "mini-pre-IncomeStatement.xml",
  "mini-pre-CashFlowStatement.xml",
  "mini-pre-ChangesInEquity.xml",
  "mini-pre-CashAndCashEquivalents.xml",
  "mini-pre-Receivables.xml",
  "mini-pre-Inventories.xml",
  "mini-pre-PropertyPlantAndEquipment.xml",
  "mini-pre-AccountsPayable.xml",
  "mini-pre-AccruedExpenses.xml",
  "mini-pre-LongTermDebt.xml",
  "mini-pre-PaidInCapital.xml",
  "mini-pre-RetainedEarnings.xml",
)


# ── Domain types ─────────────────────────────────────────────────────────


@dataclass
class Concept:
  """One mini concept parsed from mini.xsd."""

  name: str  # local name, e.g. "CashAndCashEquivalents"
  type: str  # e.g. "xbrli:monetaryItemType"
  substitution_group: str
  period_type: str | None
  balance_type: str | None
  label: str = ""  # filled from mini-lab.xml

  @property
  def qname(self) -> str:
    return f"mini:{self.name}"

  @property
  def is_monetary(self) -> bool:
    return self.type.endswith("monetaryItemType")

  @property
  def is_hypercube(self) -> bool:
    return self.substitution_group.endswith("hypercubeItem")

  @property
  def is_abstract(self) -> bool:
    # A concept is structural (abstract/roll-up/line-items container)
    # when it's not monetary and not a hypercube. Mini uses
    # ``stringItemType`` for these.
    return not self.is_monetary and not self.is_hypercube

  @property
  def element_type(self) -> str:
    """Map to RoboSystems Element.element_type enum."""
    if self.is_hypercube:
      return "hypercube"
    if self.is_abstract:
      return "abstract"
    return "concept"


@dataclass
class PresentationArc:
  """One parent-child arc from a mini-pre-*.xml linkbase."""

  parent_concept: str  # local name
  child_concept: str  # local name
  source_file: str
  order: float


@dataclass
class ParsedTaxonomy:
  concepts: dict[str, Concept] = field(default_factory=dict)
  arcs_by_child: dict[str, list[PresentationArc]] = field(
    default_factory=lambda: defaultdict(list)
  )


# ── Parsing ──────────────────────────────────────────────────────────────


def parse_concepts(xsd_path: Path) -> dict[str, Concept]:
  """Parse mini.xsd → {name: Concept}."""
  tree = ET.parse(xsd_path)
  root = tree.getroot()
  out: dict[str, Concept] = {}
  for el in root.iter(f"{NS_XSD}element"):
    name = el.attrib.get("name")
    if not name:
      continue
    out[name] = Concept(
      name=name,
      type=el.attrib.get("type", ""),
      substitution_group=el.attrib.get("substitutionGroup", ""),
      period_type=el.attrib.get(f"{NS_XBRLI}periodType"),
      balance_type=el.attrib.get(f"{NS_XBRLI}balance"),
    )
  return out


def parse_labels(lab_path: Path, concepts: dict[str, Concept]) -> None:
  """Parse mini-lab.xml → fill ``Concept.label`` in place.

  Mini labels live as ``<label xml:lang="en">…</label>`` resources
  bound via ``<labelArc xlink:from=concept xlink:to=label>``. The
  locator's ``xlink:href="mini.xsd#mini_ConceptName"`` is how we
  recover the concept name. We use the standard
  ``http://www.xbrl.org/2003/role/label`` role and ignore the
  negated/total/period-start variants.
  """
  tree = ET.parse(lab_path)
  root = tree.getroot()

  # Build locator label → concept name from <loc> elements.
  loc_to_concept: dict[str, str] = {}
  for loc in root.iter(f"{NS_LINK}loc"):
    href = loc.attrib.get(f"{NS_XLINK}href", "")
    if "#mini_" not in href:
      continue
    concept_name = href.split("#mini_", 1)[1]
    locator_label = loc.attrib.get(f"{NS_XLINK}label", "")
    if locator_label:
      loc_to_concept[locator_label] = concept_name

  # Build labelArc resource label → locator label.
  resource_to_concept: dict[str, str] = {}
  for arc in root.iter(f"{NS_LINK}labelArc"):
    from_ = arc.attrib.get(f"{NS_XLINK}from", "")
    to_ = arc.attrib.get(f"{NS_XLINK}to", "")
    if from_ in loc_to_concept:
      resource_to_concept[to_] = loc_to_concept[from_]

  # Pull label text for the standard label role.
  for label in root.iter(f"{NS_LINK}label"):
    role = label.attrib.get(f"{NS_XLINK}role", "")
    if role != "http://www.xbrl.org/2003/role/label":
      continue
    resource_label = label.attrib.get(f"{NS_XLINK}label", "")
    concept_name = resource_to_concept.get(resource_label)
    if concept_name and concept_name in concepts:
      concepts[concept_name].label = (label.text or "").strip()


def parse_presentation_arcs(
  pre_path: Path,
) -> list[PresentationArc]:
  """Parse one mini-pre-*.xml → list of parent-child arcs."""
  tree = ET.parse(pre_path)
  root = tree.getroot()

  # Locator label → concept name within this file.
  loc_to_concept: dict[str, str] = {}
  for loc in root.iter(f"{NS_LINK}loc"):
    href = loc.attrib.get(f"{NS_XLINK}href", "")
    if "#mini_" not in href:
      continue
    concept_name = href.split("#mini_", 1)[1]
    locator_label = loc.attrib.get(f"{NS_XLINK}label", "")
    if locator_label:
      loc_to_concept[locator_label] = concept_name

  arcs: list[PresentationArc] = []
  for arc in root.iter(f"{NS_LINK}presentationArc"):
    parent_label = arc.attrib.get(f"{NS_XLINK}from", "")
    child_label = arc.attrib.get(f"{NS_XLINK}to", "")
    parent_name = loc_to_concept.get(parent_label)
    child_name = loc_to_concept.get(child_label)
    if not parent_name or not child_name:
      continue
    try:
      order = float(arc.attrib.get("order", "0"))
    except ValueError:
      order = 0.0
    arcs.append(
      PresentationArc(
        parent_concept=parent_name,
        child_concept=child_name,
        source_file=pre_path.name,
        order=order,
      )
    )
  return arcs


def parse_all(taxonomy_dir: Path) -> ParsedTaxonomy:
  """Parse the full mini taxonomy from ``taxonomy_dir``."""
  parsed = ParsedTaxonomy()
  parsed.concepts = parse_concepts(taxonomy_dir / "mini.xsd")
  parse_labels(taxonomy_dir / "mini-lab.xml", parsed.concepts)

  # Pull arcs from every mini-pre-*.xml; index by child for fast
  # canonical-parent selection.
  for pre_path in sorted(taxonomy_dir.glob("mini-pre-*.xml")):
    arcs = parse_presentation_arcs(pre_path)
    for a in arcs:
      parsed.arcs_by_child[a.child_concept].append(a)
  return parsed


# ── Hierarchy resolution ─────────────────────────────────────────────────


def pick_canonical_parent(
  arcs: list[PresentationArc],
) -> PresentationArc | None:
  """Pick the canonical parent arc for a child concept.

  Strategy: prefer arcs from higher-priority presentation files
  (BS > IS > CF > SE > roll-forwards > others). Ties broken by
  insertion order. Returns None if the concept has no presentation
  arcs (a true root).
  """
  if not arcs:
    return None
  priority_index = {name: i for i, name in enumerate(PRESENTATION_PRIORITY)}
  default_rank = len(PRESENTATION_PRIORITY)
  return min(
    arcs,
    key=lambda a: (priority_index.get(a.source_file, default_rank), a.order),
  )


# ── Element payload construction ─────────────────────────────────────────


def build_element_payloads(
  parsed: ParsedTaxonomy,
) -> list[dict]:
  """Build the elements list for ``create_taxonomy_block``.

  **Filter rule**: only **monetary concepts** (the ~92 leaves with
  ``xbrli:balance``) become CoA elements. The other ~98 mini concepts
  (hypercubes, line-items containers, abstract roll-ups, text-block
  policies) are XBRL structural plumbing — they're load-bearing for
  rendering presentation networks but they aren't GL-postable
  accounts. The CoA validator requires every element to declare a
  ``trait`` from a fixed enum (asset/liability/equity/contraAsset/
  temporaryEquity/revenue/expense/income/gain/loss); only monetary
  leaves cleanly satisfy that — matches how QB CoAs land (flat list
  of balance-bearing accounts).

  **Hierarchy**: ``parent_ref=None`` for every leaf — the mini
  hierarchy lives in the presentation linkbases, not in CoA
  parent-child relationships among the monetary leaves themselves.
  This mirrors how QB sync produces a flat CoA when the source
  doesn't carry sub-account nesting between balance-bearing accounts.

  Excluded structural concepts can be loaded as a separate
  ``custom_ontology`` Taxonomy Block later if their presentation
  hierarchy is needed for rendering.
  """
  payloads: list[dict] = []

  for name, concept in sorted(parsed.concepts.items()):
    if not concept.is_monetary:
      # Skip XBRL plumbing — not a CoA leaf.
      continue
    trait = _derive_trait(concept)
    if trait is None:
      # Shouldn't happen for monetary concepts, but be defensive.
      continue
    payloads.append(
      {
        "qname": concept.qname,
        "name": concept.label or concept.name,
        "trait": trait,
        "balance_type": concept.balance_type,
        "element_type": "concept",
        "period_type": concept.period_type,
        "is_monetary": True,
        "description": None,
        "code": concept.qname,  # mini's qname doubles as the CoA code
        "sub_classification": None,
        # Flat CoA — mini's parent_ref hierarchy lives in the
        # presentation linkbases (loadable separately as a
        # custom_ontology block if rendering needs it).
        "parent_ref": None,
        "metadata": {
          "source_taxonomy": "seattle-method-mini",
          "concept_type": concept.type,
          "substitution_group": concept.substitution_group,
        },
      }
    )

  return payloads


def _derive_trait(concept: Concept) -> str | None:
  """Best-effort FASB trait classification for a monetary concept.

  Mini doesn't carry an explicit trait per concept, so we infer from
  ``balance_type`` + ``period_type``:

  - instant + debit → ``asset``
  - instant + credit → ``liability`` (the most common case; equity
    concepts like ``PaidInCapital`` get re-classified by name below)
  - duration + debit → ``expense``
  - duration + credit → ``revenue``

  Equity-side concepts (PaidInCapital, RetainedEarnings, etc.) are
  detected by name suffix and re-classified to ``equity``. Mapping
  refinement against rs-gaap happens separately via the
  MappingOperator path; the validator just needs *a* valid trait
  from {asset, liability, equity, contraAsset, contraLiability,
  contraEquity, temporaryEquity, revenue, expense, income, gain, loss}.
  """
  if concept.is_hypercube or not concept.is_monetary:
    return None

  if concept.period_type == "instant":
    # BS concepts. Equity-side concepts get re-classified by name —
    # PaidInCapital, RetainedEarnings, CommonStockValue, etc. are
    # credit-balance like liabilities but the canonical CoA trait is
    # ``equity``. Name-detection runs only inside the instant branch
    # so duration-flow concepts (e.g. ``ChangesInRetainedEarnings``)
    # don't trip the equity check — they're flow concepts that hit
    # the revenue/expense classification below.
    equity_markers = (
      "paidincapital",
      "retainedearnings",
      "commonstock",
      "preferredstock",
      "treasurystock",
    )
    lower = concept.name.lower()
    if any(m in lower for m in equity_markers):
      return "equity"
    return "asset" if concept.balance_type == "debit" else "liability"

  # period_type == 'duration' → income statement / flow concept
  return "revenue" if concept.balance_type == "credit" else "expense"


# ── HTTP client + upload ─────────────────────────────────────────────────


def _get_ledger_client():
  """Construct a LedgerClient from saved credentials.

  Mirrors the helper in ``examples.roboledger_demo.main``."""
  import json

  from robosystems_client.clients.ledger_client import LedgerClient

  config_path = Path(".local/config.json")
  if not config_path.exists():
    raise SystemExit(
      "Missing .local/config.json — run `just demo-user` first to create credentials."
    )
  with config_path.open() as f:
    cfg = json.load(f)
  return LedgerClient(
    {
      "base_url": cfg.get("base_url", "http://localhost:8000"),
      "token": cfg["api_key"],
    }
  )


def upload_taxonomy(graph_id: str, elements: list[dict]) -> str:
  """Create the mini CoA via ``create_taxonomy_block``.

  Returns the new taxonomy_id. Idempotent: if a taxonomy with
  ``name="Seattle Method mini CoA"`` already exists on the graph,
  returns its existing id instead of recreating (which would fail
  on the unique-qname constraint).
  """
  client = _get_ledger_client()

  # Idempotency check — re-runs against the same graph reuse the
  # existing taxonomy rather than colliding on unique-qname.
  existing_taxonomies = client.list_taxonomies(graph_id) or []
  for tax in existing_taxonomies:
    if (tax.get("name") or "").lower().startswith("seattle method"):
      print(f"  Reusing existing mini CoA taxonomy: {tax.get('id')}")
      return tax.get("id")

  envelope = client.create_taxonomy_block(
    graph_id,
    {
      "name": "Seattle Method mini CoA",
      "taxonomy_type": "chart_of_accounts",
      "description": (
        "Chart of accounts loaded from Charlie Hoffman's mini "
        "reporting framework (https://github.com/seattlemethod/mini). "
        "Each mini concept lands as one Element with the presentation "
        "linkbase supplying parent_ref hierarchy. Per Charlie's "
        "'fix the process' thesis, mini concepts ARE the chart of "
        "accounts — there's no separate CoA→reporting mapping step "
        "to wire."
      ),
      "elements": elements,
      "structures": [
        {
          "name": "mini to rs-gaap Mapping",
          "description": (
            "Maps mini CoA concepts to rs-gaap reporting concepts. "
            "Populated by ``seed_mappings.py``."
          ),
          "block_type": "coa_mapping",
        },
      ],
      "associations": [],
      "rules": [],
    },
  )
  return envelope.id


# ── Main ─────────────────────────────────────────────────────────────────


def main() -> None:
  parser = argparse.ArgumentParser(
    description="Load the Seattle Method mini taxonomy as a CoA on a graph.",
  )
  parser.add_argument(
    "graph_id",
    help="Target graph id (e.g. kg_seattle_method_test)",
  )
  parser.add_argument(
    "--taxonomy-dir",
    type=Path,
    default=Path("local/taxonomies/mini"),
    help="Directory containing the pulled mini artifacts.",
  )
  parser.add_argument(
    "--dry-run",
    action="store_true",
    help="Parse + build payloads; print summary; do not upload.",
  )
  args = parser.parse_args()

  if not args.taxonomy_dir.exists():
    raise SystemExit(
      f"Taxonomy dir {args.taxonomy_dir} does not exist. "
      "Run `examples/seattle_method_demo/pull_mini.sh` first."
    )

  print(f"Parsing mini taxonomy from {args.taxonomy_dir} …")
  parsed = parse_all(args.taxonomy_dir)
  print(f"  Concepts:                  {len(parsed.concepts)}")
  print(
    f"  Concepts with labels:      "
    f"{sum(1 for c in parsed.concepts.values() if c.label)}"
  )
  print(
    f"  Concepts with parent arc:  "
    f"{sum(1 for c in parsed.concepts if c in parsed.arcs_by_child)}"
  )
  print(
    f"  Monetary concepts:         "
    f"{sum(1 for c in parsed.concepts.values() if c.is_monetary)}"
  )

  payloads = build_element_payloads(parsed)
  print(f"  Built {len(payloads)} element payload(s)")

  if args.dry_run:
    sample = payloads[:8]
    print("\nFirst 8 element payloads (sample):")
    for p in sample:
      parent = p["parent_ref"] or "(top-level)"
      balance = p["balance_type"] or "—"
      trait = p["trait"] or "—"
      print(f"  {p['qname']:<55} parent={parent:<55} trait={trait} balance={balance}")
    print("\nDry run — no upload performed.")
    return

  print(f"\nUploading to graph {args.graph_id} …")
  taxonomy_id = upload_taxonomy(args.graph_id, payloads)
  print(f"✓ Created taxonomy {taxonomy_id} with {len(payloads)} elements")


if __name__ == "__main__":
  main()
