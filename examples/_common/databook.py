#!/usr/bin/env python3
"""Render a published Report's JSON-LD bundle as a **DataBook** markdown file.

A DataBook (Kurt Cagle / Charlie Hoffman, ``https://w3id.org/databook/ns#``)
is a single self-describing markdown file that carries human-readable prose,
machine-processable RDF, and a provenance "process stamp" together: YAML
frontmatter + typed fenced code blocks + prose. This is the publication/
transport skin for a report — *not* a storage substrate.

A RoboSystems Report maps onto the pattern almost 1:1: the report **is** a
collection of Information Blocks, and each IB renders two ways —

* a **markdown table** (the human view of the block's facts), and
* an addressable ``turtle`` fenced block ``{#ib-...}`` (the machine view —
  the same facts as RDF, which reads far better than the JSON-LD).

Everything is derived from the on-disk ``.jsonld`` artifact (same design as
``validate.py``): no API, no database, no container. The DataBook therefore
*regenerates from the artifact it describes* — which is exactly the
portability claim the format is built on.

Usage:
    uv run python -m examples._common.databook \
        --jsonld examples/roboledger_demo/output/roboledger-demo.jsonld \
        --label  "RoboLedger Demo"
"""

from __future__ import annotations

import argparse
import json
import re
from collections import defaultdict
from pathlib import Path

import rdflib
from rdflib import RDF, Graph, URIRef
from rdflib.namespace import Namespace

REPO_ROOT = Path(__file__).resolve().parents[2]

RS = Namespace("https://robosystems.ai/vocab/")
XBRLI = Namespace("http://www.xbrl.org/2003/instance#")
XLINK = Namespace("http://www.w3.org/1999/xlink#")
LINK = Namespace("http://www.xbrl.org/2003/linkbase#")
SKOS = Namespace("http://www.w3.org/2004/02/skos/core#")

# Prefixes bound on every emitted turtle slice so concept qnames compact
# (rs-gaap:Assets rather than the full IRI) — mirrors the encoder's bindings.
_TURTLE_PREFIXES: dict[str, str] = {
  "rs": str(RS),
  "rs-gaap": "https://robosystems.ai/taxonomy/rs-gaap/v1/",
  "disclosures": "https://robosystems.ai/taxonomy/rs-gaap/disclosures/v1/",
  "fac": "http://www.xbrlsite.com/fac#",
  "us-gaap": "http://fasb.org/us-gaap/",
  "dei": "http://xbrl.sec.gov/dei/",
  "skos": str(SKOS),
  "xbrli": str(XBRLI),
  "xlink": str(XLINK),
  "link": str(LINK),
  "iso4217": "http://www.xbrl.org/2003/iso4217#",
  "xsd": "http://www.w3.org/2001/XMLSchema#",
}

# Stable display order for the canonical statement IBs; anything else falls
# after these, in graph order.
_BLOCK_ORDER: dict[str, int] = {
  "balance_sheet": 0,
  "income_statement": 1,
  "cash_flow_statement": 2,
  "equity_statement": 3,
}


def _rel(p: Path) -> str:
  try:
    return str(p.resolve().relative_to(REPO_ROOT))
  except ValueError:
    return str(p)


def _fmt_money(v: float | None) -> str:
  if v is None:
    return "—"
  if v < 0:
    return f"$({abs(v):,.2f})"
  return f"${v:,.2f}"


def _qname(uri: URIRef) -> str:
  """Compact a concept IRI to ``prefix:Local`` using the known prefixes."""
  s = str(uri)
  for prefix, ns in _TURTLE_PREFIXES.items():
    if s.startswith(ns):
      return f"{prefix}:{s[len(ns) :]}"
  return s.rsplit("/", 1)[-1]


def _humanize(uri: URIRef) -> str:
  """Readable concept name when the bundle carries no ``skos:prefLabel`` —
  split the CamelCase local name into words (``AssetsCurrent`` → ``Assets
  Current``). A graceful fallback for older bundles that predate label emit."""
  local = _qname(uri).split(":", 1)[-1]
  return re.sub(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])", " ", local)


# ── Report-level extraction ──────────────────────────────────────────────────


def _root(g: Graph) -> URIRef:
  root = next(g.subjects(RDF.type, RS.Report), None)
  if root is None:
    raise SystemExit("No rs:Report node in bundle — is this a report bundle?")
  return root  # type: ignore[return-value]


def _period_columns(g: Graph, root: URIRef) -> list[dict]:
  cols = []
  for col in g.objects(root, RS.periods):
    cols.append(
      {
        "node": col,
        "start": str(g.value(col, RS.start) or ""),
        "end": str(g.value(col, RS.end) or ""),
        "label": str(g.value(col, SKOS.prefLabel) or ""),
      }
    )
  return sorted(cols, key=lambda c: (c["start"], c["end"]))


def _fact_end_date(g: Graph, fact: URIRef) -> str:
  """The period end a fact lands in — instant for BS, endDate for flows."""
  period = g.value(fact, RS.period)
  if period is None:
    return ""
  inst = g.value(period, XBRLI.instant)
  return str(inst or g.value(period, XBRLI.endDate) or "")


def _presentation_order(g: Graph, structure: URIRef) -> dict[URIRef, tuple[int, int]]:
  """Post-order the structure's presentation arcs → {concept: (order, depth)}.

  Traversal is **post-order** so a total renders *after* its components — the
  financial-statement convention (cash + receivables … then *Assets, Current*;
  the section subtotals last). ``order`` is the flattened post-order position
  (drives row order); ``depth`` is the tree depth (drives indentation). Roots
  are visited by their subtree's leading arc order, so the balance sheet's two
  roots (Assets, then Liabilities and Equity) come out in the right sequence.
  """
  children: dict[URIRef, list[tuple[float, URIRef]]] = defaultdict(list)
  froms: set[URIRef] = set()
  tos: set[URIRef] = set()
  for assoc in g.objects(structure, RS.hasAssociation):
    if str(g.value(assoc, RS.associationType)) != "presentation":
      continue
    frm = g.value(assoc, XLINK["from"])
    to = g.value(assoc, XLINK.to)
    if frm is None or to is None:
      continue
    order = g.value(assoc, LINK.order)
    children[frm].append((float(order) if order is not None else 0.0, to))  # type: ignore[arg-type]
    froms.add(frm)  # type: ignore[arg-type]
    tos.add(to)  # type: ignore[arg-type]
  for kids in children.values():
    kids.sort(key=lambda t: t[0])

  out: dict[URIRef, tuple[int, int]] = {}
  seen: set[URIRef] = set()
  counter = 0

  def walk(node: URIRef, depth: int) -> None:
    nonlocal counter
    if node in seen:
      return
    seen.add(node)
    for _, child in children.get(node, []):
      walk(child, depth + 1)
    out[node] = (counter, depth)  # assigned after children → total comes last
    counter += 1

  def root_key(root: URIRef) -> float:
    kids = children.get(root, [])
    return kids[0][0] if kids else 0.0

  for root in sorted(froms - tos, key=root_key):
    walk(root, 0)
  return out


def _calc_subtotals(g: Graph) -> set[URIRef]:
  """Concepts that are calculation parents — the authoritative subtotal set.

  A presentation leaf can still be a subtotal: Gross Profit / Operating Income
  / Net Income are *siblings* in the presentation tree but *parents* in the
  calculation tree (they sum other concepts). Bolding off the calc tree marks
  exactly those, matching the live ``statement`` renderer's ``isSubtotal``.
  """
  out: set[URIRef] = set()
  for assoc in g.subjects(RS.associationType, rdflib.Literal("calculation")):
    frm = g.value(assoc, XLINK["from"])
    if isinstance(frm, URIRef):
      out.add(frm)
  return out


def _structure_for_block(g: Graph, block_type: str) -> URIRef | None:
  for struct in g.subjects(RDF.type, RS.Structure):
    if str(g.value(struct, RS.blockType)) == block_type:
      return struct  # type: ignore[return-value]
  return None


# ── Per-IB markdown table ────────────────────────────────────────────────────


def _render_ib_table(g: Graph, ib: URIRef, columns: list[dict]) -> str:
  fact_set = g.value(ib, RS.factSet)
  block_type = str(g.value(ib, RS.blockType) or "")
  structure = _structure_for_block(g, block_type)
  order = _presentation_order(g, structure) if structure is not None else {}
  subtotals = _calc_subtotals(g)

  # Collect this IB's facts, keyed by element, with a value per period column.
  col_end = {c["end"]: i for i, c in enumerate(columns)}
  by_element: dict[URIRef, list[float | None]] = {}
  for fact in g.subjects(RDF.type, RS.Fact):
    if g.value(fact, RS.factSet) != fact_set:
      continue
    element = g.value(fact, RS.element)
    if element is None:
      continue
    value = g.value(fact, RS.numericValue)
    idx = col_end.get(_fact_end_date(g, fact))
    row = by_element.setdefault(element, [None] * len(columns))  # type: ignore[arg-type]
    if idx is not None and value is not None:
      row[idx] = float(value)

  def sort_key(el: URIRef) -> tuple[int, str]:
    o = order.get(el)
    return (o[0] if o else 10_000, str(el))

  header_cols = " | ".join(f"{c['label']}" for c in columns)
  sep = " | ".join(["---:"] * len(columns))
  lines = [f"| QName | Concept | {header_cols} |", f"|---|---|{sep}|"]
  for element in sorted(by_element, key=sort_key):
    _, depth = order.get(element, (0, 0))
    is_subtotal = element in subtotals
    label = str(g.value(element, SKOS.prefLabel) or _humanize(element))
    indent = "  " * depth
    bold = "**" if is_subtotal else ""
    name = f"{indent}{bold}{label}{bold}"
    values = " | ".join(_fmt_money(v) for v in by_element[element])
    lines.append(f"| `{_qname(element)}` | {name} | {values} |")
  return "\n".join(lines)


# ── Per-IB turtle slice ──────────────────────────────────────────────────────


def _bind_prefixes(g: Graph) -> None:
  for prefix, ns in _TURTLE_PREFIXES.items():
    g.bind(prefix, ns, replace=True)


def _render_ib_turtle(src: Graph, ib: URIRef) -> str:
  """A focused turtle graph for one IB: the block node + its facts + the
  elements / periods / units those facts reference. The bulky ``envelopeJson``
  literal is dropped for readability (it lives in the JSON-LD bundle)."""
  out = Graph()
  _bind_prefixes(out)
  fact_set = src.value(ib, RS.factSet)

  for p, o in src.predicate_objects(ib):
    if p == RS.envelopeJson:
      continue
    out.add((ib, p, o))

  referenced: set[URIRef] = set()
  for fact in src.subjects(RDF.type, RS.Fact):
    if src.value(fact, RS.factSet) != fact_set:
      continue
    for p, o in src.predicate_objects(fact):
      out.add((fact, p, o))
    for ref_pred in (RS.element, RS.period, RS.unit, RS.entity):
      ref = src.value(fact, ref_pred)
      if isinstance(ref, URIRef):
        referenced.add(ref)

  for node in referenced:
    for p, o in src.predicate_objects(node):
      out.add((node, p, o))

  return out.serialize(format="turtle").strip()


# ── Frontmatter ──────────────────────────────────────────────────────────────


def _frontmatter(g: Graph, root: URIRef, ibs: list[URIRef], columns: list[dict]) -> str:
  meta = g.value(root, RS.reportMeta)
  entity = g.value(root, RS.entity)
  entity_name = str(g.value(entity, SKOS.prefLabel) or "") if entity else ""

  pins: list[tuple[str, str]] = []
  for pin in g.objects(root, RS.frameworkPins):
    fw = str(g.value(pin, RS.framework) or "")
    ver = str(g.value(pin, RS.version) or "")
    if (fw, ver) not in pins:
      pins.append((fw, ver))

  lines: list[str] = ["---"]
  lines.append(f"id: {root}")
  lines.append("type: databook")
  lines.append("vocab: https://w3id.org/databook/ns#")
  lines.append(
    f'serialization_version: "{g.value(root, RS.serializationVersion) or ""}"'
  )
  lines.append("report:")
  lines.append(f"  reporting_style: {g.value(root, RS.reportingStyle) or ''}")
  if entity_name:
    lines.append(f"  entity: {json.dumps(entity_name)}")
  lines.append("  periods:")
  for c in columns:
    lines.append(
      f"    - {{ label: {json.dumps(c['label'])}, start: {c['start']}, end: {c['end']} }}"
    )
  lines.append("  framework_pins:")
  for fw, ver in pins:
    lines.append(f"    - {{ framework: {fw}, version: {ver} }}")

  # The process stamp — RoboSystems report-engine provenance, DataBook-shaped.
  lines.append("process:")
  lines.append("  transformer: robosystems-report-engine")
  if meta is not None:
    for key, pred in (
      ("report_id", RS.reportId),
      ("generation_count", RS.generationCount),
      ("filing_status", RS.filingStatus),
      ("filed_at", RS.filedAt),
      ("supersedes_id", RS.supersedesId),
      ("source_graph_id", RS.sourceGraphId),
      ("source_report_id", RS.sourceReportId),
      ("shared_at", RS.sharedAt),
    ):
      val = g.value(meta, pred)
      if val is not None:
        lines.append(f"  {key}: {val}")

  lines.append("information_blocks:")
  for ib in ibs:
    bt = str(g.value(ib, RS.blockType) or "")
    lines.append(
      f"  - {{ id: {g.value(ib, RS.internalId)}, block_type: {bt}, anchor: ib-{bt} }}"
    )
  lines.append("license: CC-BY-4.0")
  lines.append("---")
  return "\n".join(lines)


# ── Validation evidence (self-contained proof) ───────────────────────────────


def _evidence_section(shacl_md: Path | None, xbrl_md: Path | None) -> str:
  """Inline the (short) SHACL + Arelle verdict reports so the DataBook carries
  its own proof. Each report's ``# H1`` becomes a ``###`` subsection and its
  inner ``##`` headings demote to ``####`` to nest under the block sections."""
  blocks: list[str] = []
  for path, fallback in ((shacl_md, "SHACL conformance"), (xbrl_md, "XBRL 2.1")):
    if path is None or not path.exists():
      continue
    body = path.read_text().strip()
    title = fallback
    if body.startswith("# "):
      first, _, rest = body.partition("\n")
      title = first[2:].strip()
      body = rest.strip()
    blocks.append(f"### {title}\n\n{re.sub(r'(?m)^## ', '#### ', body)}")
  if not blocks:
    return ""
  return (
    "\n## Validation evidence\n\n"
    "Independent, standards-grade checks of the same bundle this DataBook "
    "renders — embedded so the artifact travels with its own proof.\n\n"
    + "\n\n".join(blocks)
  )


# ── Top-level render ─────────────────────────────────────────────────────────


def write_databook(
  jsonld_path: Path,
  out_md: Path,
  label: str,
  *,
  shacl_md: Path | None = None,
  xbrl_md: Path | None = None,
) -> Path:
  """Render the JSON-LD report bundle at ``jsonld_path`` to a DataBook ``.md``.

  When ``shacl_md`` / ``xbrl_md`` are given, their verdict reports are inlined
  as a trailing evidence section so the DataBook is fully self-contained.
  """
  g = rdflib.Graph().parse(str(jsonld_path), format="json-ld")
  root = _root(g)
  columns = _period_columns(g, root)

  ibs = sorted(
    g.subjects(RDF.type, RS.InformationBlock),
    key=lambda ib: (
      _BLOCK_ORDER.get(str(g.value(ib, RS.blockType)), 99),
      str(g.value(ib, RS.blockType)),
    ),
  )

  parts: list[str] = [_frontmatter(g, root, ibs, columns)]  # type: ignore[arg-type]
  parts.append(
    f"\n# {label} — Report DataBook\n\n"
    f"A report **is** a collection of Information Blocks. Each block below is "
    f"shown twice: a markdown table (human view) and an addressable `turtle` "
    f"block (machine view — the same facts as RDF). Frontmatter carries the "
    f"report's provenance *process stamp*. Everything here is derived from "
    f"`{jsonld_path.name}`; the bundle and this DataBook are two skins of one "
    f"graph.\n"
  )

  for ib in ibs:
    bt = str(g.value(ib, RS.blockType) or "block")
    name = str(g.value(ib, SKOS.prefLabel) or bt)
    parts.append(f"\n## {name}  {{#ib-{bt}}}\n")
    parts.append(f"- **Block type**: `{bt}`")
    parts.append(f"- **Information Block**: `{g.value(ib, RS.internalId)}`")
    parts.append(
      f"- **FactSet**: `{str(g.value(ib, RS.factSet)).rsplit('/', 1)[-1]}`\n"
    )
    parts.append(_render_ib_table(g, ib, columns))  # type: ignore[arg-type]
    parts.append(f"\n```turtle {{#ib-{bt}-rdf}}")
    parts.append(_render_ib_turtle(g, ib))  # type: ignore[arg-type]
    parts.append("```\n")

  evidence = _evidence_section(shacl_md, xbrl_md)
  if evidence:
    parts.append(evidence)

  out_md.write_text("\n".join(parts) + "\n")
  return out_md


# ── CLI ──────────────────────────────────────────────────────────────────────


def main() -> None:
  parser = argparse.ArgumentParser(description="Render a report bundle as a DataBook.")
  parser.add_argument("--jsonld", type=Path, required=True, help="JSON-LD bundle in")
  parser.add_argument(
    "--out", type=Path, help="DataBook .md out (default: *.databook.md)"
  )
  parser.add_argument("--shacl-md", type=Path, help="SHACL validation .md to inline")
  parser.add_argument("--xbrl-md", type=Path, help="XBRL validation .md to inline")
  parser.add_argument("--label", required=True)
  args = parser.parse_args()

  jsonld = args.jsonld.resolve()
  if not jsonld.exists():
    raise SystemExit(f"{jsonld} missing — run the demo's download-bundles step.")
  out = args.out or jsonld.with_suffix("").with_suffix(".databook.md")
  written = write_databook(
    jsonld, out, args.label, shacl_md=args.shacl_md, xbrl_md=args.xbrl_md
  )
  print(f"DataBook: {jsonld.name} → {_rel(written)} ({written.stat().st_size:,} bytes)")


if __name__ == "__main__":
  main()
