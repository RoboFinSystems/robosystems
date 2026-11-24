#!/usr/bin/env python3
"""The aligned report-artifact set every RoboLedger demo emits.

One function — :func:`render_report_artifacts` — defines *what a report demo
produces*, so every demo (the bespoke ``download_bundles.py`` scripts and the
``_scenario`` showcase runner) emits the identical set through one code path.

For a report at ``(graph_id, report_id)`` it writes, under ``out_dir``:

* ``{stem}.tavi.json``         — the Project Tavi compiled model (the same
  standards form the SEC surface publishes per filing — what
  report-components renders with no RDF step)
* ``{stem}.holon.jsonld``      — the **native holon** (scene / boundary /
  projection named graphs), stamped at publish — the artifact the holon
  viewer consumes
* ``{stem}.lineage.holon.jsonld`` — the same holon with its fourth named
  graph, ``#lineage``: the ledger lines, entries and events behind every
  frozen figure (this graph's own members only)
* ``{stem}.zip``               — the XBRL 2.1 report package
* ``{stem}-xbrl-validation.md``  — Arelle verdict (over the XBRL zip)
* ``{stem}-holon-validation.md`` — xbrlkit verdict (over both holons, one per
  named graph, plus the check that the report holds together)
* ``{stem}.databook.md``       — the report as a document to read: a
  projection of the holon (tables, the same facts as RDF, the verdict) that
  names the holon as its source

Every flavor comes from the product endpoint via the published SDK — the
demos exercise the real download path, they don't reconstruct artifacts
client-side.
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
  """Download + validate the aligned artifact set for one report.

  ``stem`` is the shared filename prefix (e.g. ``roboledger-demo``); ``label``
  is the human name used in the validation report. Errors propagate — a
  failed download or validation is loud, not swallowed.
  """
  out_dir.mkdir(parents=True, exist_ok=True)
  tavi_path = out_dir / f"{stem}.tavi.json"
  holon_path = out_dir / f"{stem}.holon.jsonld"
  lineage_path = out_dir / f"{stem}.lineage.holon.jsonld"
  xbrl_path = out_dir / f"{stem}.zip"

  tavi = client.download_report_bundle(graph_id, report_id, format="tavi", to=tavi_path)
  holon = client.download_report_bundle(
    graph_id, report_id, format="holon-jsonld", to=holon_path
  )
  # The same holon with its fourth named graph, ``#lineage``: the ledger
  # lines, entries and events behind every frozen figure.
  lineage = client.download_report_bundle(
    graph_id, report_id, format="holon-jsonld-lineage", to=lineage_path
  )
  xbrl = client.download_report_bundle(
    graph_id, report_id, format="xbrl-2.1", to=xbrl_path
  )
  print(f"  Tavi:     {tavi.path} ({len(tavi.content):,} bytes)")
  print(f"  Holon:    {holon.path} ({len(holon.content):,} bytes)")
  print(f"  Lineage:  {lineage.path} ({len(lineage.content):,} bytes)")
  print(f"  XBRL 2.1: {xbrl.path} ({len(xbrl.content):,} bytes)")

  # Container-free validation of the XBRL zip (Arelle) and of both holons
  # (xbrlkit). Deferred import keeps this module light for callers that only
  # need the download.
  from examples._common.validate import validate_arelle, validate_holons

  xbrl_md = out_dir / f"{stem}-xbrl-validation.md"
  valid = validate_arelle(xbrl_path, xbrl_md, label)
  print(f"  Arelle:   {xbrl_md} ({'valid' if valid else 'INVALID'})")
  holon_md = out_dir / f"{stem}-holon-validation.md"
  conforms = validate_holons([holon_path, lineage_path], holon_md, label)
  print(f"  xbrlkit:  {holon_md} ({'conforms' if conforms else 'DOES NOT CONFORM'})")

  # The holon as a document a person reads. Written from the downloaded file
  # alone, so it regenerates from the artifact it describes.
  from xbrlkit.holon.databook import databook_from_holon

  databook_path = out_dir / f"{stem}.databook.md"
  databook = databook_from_holon(
    holon_path.read_text(encoding="utf-8"), href=holon_path.name
  )
  databook_path.write_text(databook, encoding="utf-8")
  print(f"  DataBook: {databook_path} ({len(databook):,} bytes)")
