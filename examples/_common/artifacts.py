#!/usr/bin/env python3
"""The aligned report-artifact set every RoboLedger demo emits.

One function — :func:`render_report_artifacts` — defines *what a report demo
produces*, so every demo (the bespoke ``download_bundles.py`` scripts and the
``_scenario`` showcase runner) emits the identical set through one code path.

For a report at ``(graph_id, report_id)`` it writes, under ``out_dir``:

* ``{stem}.jsonld``            — the flat canonical bundle (stamped at publish)
* ``{stem}.holon.jsonld``      — the **native holon**, downloaded from the report
  endpoint (scene / boundary / projection named graphs) — the artifact the
  holon viewer consumes
* ``{stem}.tavi.json``         — the Project Tavi compiled model (compact JSON;
  the same standards form the SEC surface publishes per filing — what
  report-components renders with no RDF step)
* ``{stem}.zip``               — the XBRL 2.1 report package
* ``{stem}-shacl-validation.md`` — SHACL conformance verdict (over the JSON-LD)
* ``{stem}-xbrl-validation.md``  — Arelle verdict (over the XBRL zip)
* ``{stem}.databook.md``       — the DataBook (references the native holon;
  ``derive_holon=False`` since it's downloaded, not re-derived)

All four bundle flavors come from the product endpoint via the published SDK —
the demos exercise the real download path, they don't reconstruct artifacts
client-side. ``.holon.trig`` is intentionally not emitted: the holon travels as
dataset-form JSON-LD, from which any consumer can derive TriG or N-Quads.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
  from robosystems_client.clients import LedgerClient


def render_report_artifacts(
  client: LedgerClient,
  graph_id: str,
  report_id: str,
  out_dir: Path,
  stem: str,
  label: str,
) -> None:
  """Download + validate + render the aligned artifact set for one report.

  ``stem`` is the shared filename prefix (e.g. ``roboledger-demo``); ``label``
  is the human name used in the validation reports and DataBook title. Errors
  propagate — a failed download or validation is loud, not swallowed.
  """
  out_dir.mkdir(parents=True, exist_ok=True)
  jsonld_path = out_dir / f"{stem}.jsonld"
  holon_path = out_dir / f"{stem}.holon.jsonld"
  tavi_path = out_dir / f"{stem}.tavi.json"
  xbrl_path = out_dir / f"{stem}.zip"

  # All four flavors pulled natively from the report endpoint.
  jsonld = client.download_report_bundle(
    graph_id, report_id, format="jsonld", to=jsonld_path
  )
  holon = client.download_report_bundle(
    graph_id, report_id, format="holon-jsonld", to=holon_path
  )
  tavi = client.download_report_bundle(
    graph_id, report_id, format="tavi", to=tavi_path
  )
  xbrl = client.download_report_bundle(
    graph_id, report_id, format="xbrl-2.1", to=xbrl_path
  )
  print(f"  JSON-LD:  {jsonld.path} ({len(jsonld.content):,} bytes)")
  print(f"  Holon:    {holon.path} ({len(holon.content):,} bytes)")
  print(f"  Tavi:     {tavi.path} ({len(tavi.content):,} bytes)")
  print(f"  XBRL 2.1: {xbrl.path} ({len(xbrl.content):,} bytes)")

  # Container-free validation of the received artifacts: JSON-LD → SHACL
  # (ontology conformance), XBRL zip → Arelle (XBRL 2.1). Deferred imports keep
  # this module light for callers that only need the download.
  from examples._common.databook import write_databook
  from examples._common.validate import validate_arelle, validate_shacl

  shacl_md = out_dir / f"{stem}-shacl-validation.md"
  xbrl_md = out_dir / f"{stem}-xbrl-validation.md"
  conforms = validate_shacl(jsonld_path, shacl_md, label)
  valid = validate_arelle(xbrl_path, xbrl_md, label)
  print(f"  SHACL:    {shacl_md} ({'conforms' if conforms else 'VIOLATIONS'})")
  print(f"  Arelle:   {xbrl_md} ({'valid' if valid else 'INVALID'})")

  # The DataBook — the report as a collection of Information Blocks, with the
  # SHACL/Arelle verdicts inlined. Points at the natively-downloaded holon.
  databook_md = out_dir / f"{stem}.databook.md"
  write_databook(
    jsonld_path,
    databook_md,
    f"{label} Demo",
    shacl_md=shacl_md,
    xbrl_md=xbrl_md,
    derive_holon=False,
  )
  print(f"  DataBook: {databook_md}")
