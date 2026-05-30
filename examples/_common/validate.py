#!/usr/bin/env python3
"""Container-free validation of demo bundle artifacts.

Validates the artifacts a demo already wrote to ``output/`` — **without the
running stack**: no API, no database, no Docker. Both projections of the same
bundle are checked against independent, standards-grade tooling on the host:

* **JSON-LD → SHACL** against the published ontology
  (``frameworks/ontology/v1/shapes.ttl``) — semantic conformance.
* **XBRL 2.1 → Arelle** against the XBRL 2.1 spec — structural conformance
  (Arelle is the de-facto processor SEC EDGAR uses).

Each writes a markdown evidence report next to the artifact. Because it reads
the on-disk ``.jsonld`` / ``.zip`` (rather than re-fetching from the API), it
runs anywhere the host venv is installed, with the container down.

Usage:
    uv run python -m examples._common.validate \
        --jsonld examples/seattle_method_demo/output/seattle-method-case-1.jsonld \
        --zip    examples/seattle_method_demo/output/seattle-method-case-1.zip \
        --label  "Seattle Method (Test Case 1)"
"""

from __future__ import annotations

import argparse
import io
import zipfile
from pathlib import Path

import rdflib
from pyshacl import validate as _shacl_validate

REPO_ROOT = Path(__file__).resolve().parents[2]
SHAPES_PATH = REPO_ROOT / "frameworks" / "ontology" / "v1" / "shapes.ttl"
RS = rdflib.Namespace("https://robosystems.ai/vocab/")


def _rel(p: Path) -> str:
  try:
    return str(p.resolve().relative_to(REPO_ROOT))
  except ValueError:
    return str(p)


def _count(graph: rdflib.Graph, cls: rdflib.URIRef) -> int:
  return len(list(graph.subjects(rdflib.RDF.type, cls)))


# ── JSON-LD → SHACL ─────────────────────────────────────────────────────────


def validate_shacl(jsonld_path: Path, out_md: Path, label: str) -> bool:
  """SHACL-validate a JSON-LD bundle against the ontology; write the report."""
  graph = rdflib.Graph().parse(str(jsonld_path), format="json-ld")
  shapes = rdflib.Graph().parse(str(SHAPES_PATH), format="turtle")
  conforms, _, report = _shacl_validate(graph, shacl_graph=shapes, inference="none")

  n_shapes = len(list(shapes.subjects(rdflib.RDF.type, rdflib.SH.NodeShape)))
  verdict = (
    "✅ **Conforms to RoboSystems RDF Ontology v1**"
    if conforms
    else "❌ **Does NOT conform** — see violations below"
  )
  lines = [
    f"# {label} — SHACL Ontology Conformance",
    "",
    f"## Result: {verdict}",
    "",
    f"- **Bundle**: `{jsonld_path.name}`",
    f"- **Graph triples**: {len(graph):,}",
    f"- **rs:Fact nodes**: {_count(graph, RS.Fact)}",
    f"- **rs:Association nodes**: {_count(graph, RS.Association)}",
    f"- **rs:Element nodes**: {_count(graph, RS.Element)}",
    (
      f"- **SHACL shapes checked**: {n_shapes} (positive instance shapes + "
      + "negative shapes banning the retired dialects)"
    ),
    "",
    (
      "Validated on the host with **pyshacl** against "
      + "`frameworks/ontology/v1/shapes.ttl` — the *same* shapes that gate the "
      + "framework seeds and the publish-time bundle validation, run here directly "
      + "on the on-disk artifact (no API, no database, no container). Conformance "
      + "means every `rs:Fact` references its aspects directly "
      + "(`rs:element`/`rs:entity`/`rs:period`/`rs:unit` — no XBRL `context`), every "
      + "`rs:Association` carries `xlink:from`/`to` + `xlink:arcrole`, and none of the "
      + "retired dialects (`xbrli:contextRef`, `arcFrom`, direct `summationOf`) appear."
    ),
    "",
    "## Violations",
    "",
    "_None._ Zero violations." if conforms else "```\n" + report.strip() + "\n```",
    "",
  ]
  out_md.write_text("\n".join(lines))
  return bool(conforms)


# ── XBRL → Arelle ───────────────────────────────────────────────────────────


def validate_arelle(zip_path: Path, out_md: Path, label: str) -> bool:
  """Validate an on-disk XBRL 2.1 report-package zip with Arelle; write report.

  Reads the zip from disk and runs Arelle on the host (the ``arelle`` package
  is a host dependency) — no API download, no DB lookup. This is the container-
  free counterpart to the demos' earlier API-fetch validation path.
  """
  import tempfile

  from robosystems.adapters.sec.client.arelle import ArelleClient

  with zipfile.ZipFile(io.BytesIO(zip_path.read_bytes())) as zf:
    files = sorted(zf.namelist())
    # The report package's entry point is `instance.xml` by contract; fall back
    # to the first non-schema .xml so a producer-side rename fails loudly with a
    # bad model rather than a silent FileNotFoundError on a hard-coded path.
    entry = next(
      (f for f in files if Path(f).name == "instance.xml"),
      next((f for f in files if f.endswith(".xml")), None),
    )
    if entry is None:
      raise SystemExit(f"No XBRL instance (.xml) found in {zip_path.name}: {files}")
    with tempfile.TemporaryDirectory(prefix="xbrl-validate-") as tmp:
      zf.extractall(tmp)
      client = ArelleClient()
      model = client.controller(str(Path(tmp) / entry))
      load_errors = [str(e) for e in (model.errors or [])]
      fact_count = len(model.facts) if hasattr(model, "facts") else 0
      result = client.validate(model)
      val_errors = [e for e in result.get("errors", []) if e not in load_errors]

  valid = not (load_errors or val_errors)
  verdict = "✅ **Valid XBRL 2.1**" if valid else "❌ **Validation failed**"
  lines = [
    f"# {label} — XBRL 2.1 Validation (Arelle)",
    "",
    f"## Result: {verdict}",
    "",
    f"- **Package**: `{zip_path.name}` ({zip_path.stat().st_size:,} bytes)",
    f"- **Files in zip**: {len(files)} (`{', '.join(files)}`)",
    f"- **Facts loaded by Arelle**: {fact_count}",
    f"- **Load errors**: {len(load_errors)}",
    f"- **Validation errors**: {len(val_errors)}",
    "",
    (
      "Validated on the host with **Arelle** (the de-facto XBRL processor, also "
      + "used by SEC EDGAR) directly against the on-disk report package — no API, no "
      + "container. Zero load + validation errors is the structural-correctness "
      + "claim: the output is valid XBRL 2.1, consumable by any standards-compliant "
      + "processor. This is **base XBRL 2.1** validation; SEC/EFM disclosure-system "
      + "checks are not enabled (the instance isn't an SEC filing)."
    ),
    "",
  ]
  if load_errors:
    lines += ["## Load errors", "", *[f"- `{e}`" for e in load_errors], ""]
  if val_errors:
    lines += ["## Validation errors", "", *[f"- `{e}`" for e in val_errors], ""]
  if valid:
    lines += [
      "## Errors",
      "",
      (
        "_None._ Arelle reported no load errors and no XBRL 2.1 validation errors "
        + "against the emitted instance + schema + linkbases."
      ),
      "",
    ]
  out_md.write_text("\n".join(lines))
  return valid


# ── CLI ─────────────────────────────────────────────────────────────────────


def main() -> None:
  parser = argparse.ArgumentParser(description="Container-free bundle validation.")
  parser.add_argument("--jsonld", type=Path, help="JSON-LD bundle → SHACL")
  parser.add_argument("--zip", type=Path, help="XBRL 2.1 zip → Arelle")
  parser.add_argument("--out-dir", type=Path, help="Where to write the reports")
  parser.add_argument("--label", required=True)
  args = parser.parse_args()

  if not args.jsonld and not args.zip:
    raise SystemExit("Pass at least one of --jsonld / --zip.")
  ok = True

  if args.jsonld:
    jsonld = args.jsonld.resolve()
    if not jsonld.exists():
      raise SystemExit(f"{jsonld} missing — run the demo's download-bundles step.")
    out = (args.out_dir or jsonld.parent) / f"{jsonld.stem}-shacl-validation.md"
    conforms = validate_shacl(jsonld, out, args.label)
    ok = ok and conforms
    print(
      f"SHACL : {jsonld.name} {'conforms' if conforms else 'VIOLATIONS'} → {_rel(out)}"
    )

  if args.zip:
    zpath = args.zip.resolve()
    if not zpath.exists():
      raise SystemExit(f"{zpath} missing — run the demo's download-bundles step.")
    out = (args.out_dir or zpath.parent) / f"{zpath.stem}-xbrl-validation.md"
    valid = validate_arelle(zpath, out, args.label)
    ok = ok and valid
    print(f"Arelle: {zpath.name} {'valid' if valid else 'INVALID'} → {_rel(out)}")

  if not ok:
    raise SystemExit(1)


if __name__ == "__main__":
  main()
