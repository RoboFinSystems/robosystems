#!/usr/bin/env python3
"""The aligned report-artifact set every RoboLedger demo emits.

One function — :func:`render_report_artifacts` — defines *what a report demo
produces*, so every demo (the bespoke ``download_bundles.py`` scripts and the
``_scenario`` showcase runner) emits the identical set through one code path.

For a report at ``(graph_id, report_id)`` it writes, under ``out_dir``:

* ``{stem}.tavi.json``         — the Project Tavi compiled model, stamped at
  publish (the same standards form the SEC surface publishes per filing —
  what report-components renders with no RDF step)
* ``{stem}.holon.jsonld``      — the **native holon** (scene / boundary /
  projection named graphs) — the artifact the holon viewer consumes
* ``{stem}.zip``               — the XBRL 2.1 report package
* ``{stem}-xbrl-validation.md``  — Arelle verdict (over the XBRL zip)

All three flavors come from the product endpoint via the published SDK — the
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
  xbrl_path = out_dir / f"{stem}.zip"

  tavi = client.download_report_bundle(
    graph_id, report_id, format="tavi", to=tavi_path
  )
  holon = client.download_report_bundle(
    graph_id, report_id, format="holon-jsonld", to=holon_path
  )
  xbrl = client.download_report_bundle(
    graph_id, report_id, format="xbrl-2.1", to=xbrl_path
  )
  print(f"  Tavi:     {tavi.path} ({len(tavi.content):,} bytes)")
  print(f"  Holon:    {holon.path} ({len(holon.content):,} bytes)")
  print(f"  XBRL 2.1: {xbrl.path} ({len(xbrl.content):,} bytes)")

  # Container-free validation of the XBRL zip (Arelle). Deferred import keeps
  # this module light for callers that only need the download.
  from examples._common.validate import validate_arelle

  xbrl_md = out_dir / f"{stem}-xbrl-validation.md"
  valid = validate_arelle(xbrl_path, xbrl_md, label)
  print(f"  Arelle:   {xbrl_md} ({'valid' if valid else 'INVALID'})")
