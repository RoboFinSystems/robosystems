#!/usr/bin/env python3
"""Container-free validation of demo bundle artifacts.

Validates the XBRL 2.1 package a demo already wrote to ``output/`` —
**without the running stack**: no API, no database, no Docker — against the
XBRL 2.1 spec with Arelle (the de-facto processor SEC EDGAR uses).

It writes a markdown evidence report next to the artifact. Because it reads
the on-disk ``.zip`` (rather than re-fetching from the API), it runs anywhere
the host venv is installed, with the container down.

Usage:
    uv run python -m examples._common.validate \
        --zip    examples/roboledger_demo/output/roboledger-demo.zip \
        --label  "RoboLedger Demo"
"""

from __future__ import annotations

import argparse
import io
import zipfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


def _rel(p: Path) -> str:
  try:
    return str(p.resolve().relative_to(REPO_ROOT))
  except ValueError:
    return str(p)


# ── XBRL → Arelle ───────────────────────────────────────────────────────────


def validate_arelle(zip_path: Path, out_md: Path, label: str) -> bool:
  """Validate an on-disk XBRL 2.1 report-package zip with Arelle; write report.

  Reads the zip from disk and runs Arelle on the host (the ``arelle`` package
  is a host dependency) — no API download, no DB lookup. This is the container-
  free counterpart to the demos' earlier API-fetch validation path.
  """
  import tempfile

  from arelle import ValidateXbrl
  from xbrlkit.parse import DtsResolutionError

  from robosystems.adapters.sec.client.arelle import close_filing, load_filing

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
      # The platform's load (xbrlkit's cache-first Arelle loader on the repo's
      # schema cache); a DTS the loader cannot resolve is a load error here,
      # not a crash — the report should say so.
      fact_count = 0
      val_errors: list[str] = []
      try:
        model = load_filing(str(Path(tmp) / entry))
      except DtsResolutionError as exc:
        load_errors = [f"unresolved DTS document: {url}" for url in exc.unresolved]
      else:
        try:
          load_errors = [str(e) for e in (model.errors or [])]
          fact_count = len(model.facts) if hasattr(model, "facts") else 0
          ValidateXbrl.ValidateXbrl(model).validate(model)
          val_errors = [
            str(e) for e in (model.errors or []) if str(e) not in load_errors
          ]
        finally:
          close_filing(model)

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
      + "checks are not enabled (the instance isn't an SEC filing). Scope: "
      + "tenant-authored disclosure notes are excluded from this package — the "
      + "emitter's fixed framework prefixes cannot declare an extension concept, "
      + "so notes ride the Tavi and holon flavors. A report with notes "
      + "therefore has fewer facts here than in those files."
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
  parser.add_argument("--zip", type=Path, required=True, help="XBRL 2.1 zip → Arelle")
  parser.add_argument("--out-dir", type=Path, help="Where to write the reports")
  parser.add_argument("--label", required=True)
  args = parser.parse_args()

  zpath = args.zip.resolve()
  if not zpath.exists():
    raise SystemExit(f"{zpath} missing — run the demo's download-bundles step.")
  out = (args.out_dir or zpath.parent) / f"{zpath.stem}-xbrl-validation.md"
  valid = validate_arelle(zpath, out, args.label)
  print(f"Arelle: {zpath.name} {'valid' if valid else 'INVALID'} → {_rel(out)}")
  if not valid:
    raise SystemExit(1)


if __name__ == "__main__":
  main()
