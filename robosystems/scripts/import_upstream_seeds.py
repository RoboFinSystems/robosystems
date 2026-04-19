"""Archaeological importer for bootstrapping taxonomy seeds from upstream XBRL.

ONE-SHOT BOOTSTRAP TOOL — NOT part of routine maintenance. The JSON-LD
seeds under `robosystems/taxonomy/seeds/` are RoboSystems canonical forks
(see seeds/README.md). Edits happen directly in those JSON-LD files;
upstream XBRL is not tracked.

Use this script only to:
  1. Bootstrap a brand-new taxonomy from upstream XBRL for the first time, or
  2. Re-derive a frozen seed from upstream to compare against our fork
     (e.g. to audit drift or migrate to a new upstream version intentionally).

Running this over existing seeds WILL overwrite curated edits. If you want
that, delete the target seed file first so git diff shows the full churn.

    uv run python -m robosystems.scripts.import_upstream_seeds --only sfac6
    uv run python -m robosystems.scripts.import_upstream_seeds --url https://… --standard custom --version v1
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path

from robosystems.adapters.sec.client.arelle import ArelleClient
from robosystems.arelle import extract_taxonomy, serialize_jsonld
from robosystems.logger import logger

SEEDS_DIR = Path(__file__).parent.parent / "taxonomy" / "seeds"


@dataclass
class SeedSpec:
  standard: str
  version: str
  url: str
  namespace_uri: str = ""
  description: str = ""
  taxonomy_type: str = "reporting"


SEEDS: list[SeedSpec] = [
  SeedSpec(
    standard="sfac6",
    version="v1",
    url="http://xbrlsite.com/seattlemethod/golden/sfac6/sfac6-entryPoint.xsd",
    namespace_uri="http://xbrlsite.com/seattlemethod/sfac6/",
    taxonomy_type="reporting",
    description=(
      "FASB Statement of Financial Accounting Concepts No. 6 — "
      "Elements of Financial Statements. Adapted from Charlie Hoffman's "
      "Seattle Method SFAC 6 entry point."
    ),
  ),
  # Charlie's FAC ConceptMap is a MAPPING — 199 fac: concepts wired into
  # rs-gaap via class-equivalentClass arcs. The luca.auditchain variant
  # (fka fac-conceptual) was retired because it had concepts without
  # equivalence arcs. Structural hypercubes (BS/IS/Equity) will be
  # re-curated as part of rs-gaap's structure layer.
  SeedSpec(
    standard="fac",
    version="v1",
    url="http://www.xbrlsite.com/2021/kg/us-gaap/fac/Rules_Mapping/ConceptMap_General_mapping-definition.xml",
    namespace_uri="http://xbrlsite.com/fac/",
    taxonomy_type="mapping",
    description=(
      "FAC (Fundamental Accounting Concepts) → rs-gaap mapping. 199 fac: "
      "concepts with 221 class-equivalentClass arcs into rs-gaap — "
      "collapses the us-gaap combinatorial explosion into a compact "
      "canonical layer. Primary semantic target for CoA mapping."
    ),
  ),
  # type-subtype has 0 concepts of its own — it's a pure classification
  # linkbase (general-special arcs) that enriches rs-gaap elements with
  # "which reporting bucket does this belong to?" metadata.
  SeedSpec(
    standard="type-subtype",
    version="v1",
    url="http://xbrlsite.com/site1/seattlemethod/fac/type-subtype-usgaap/type-subtype.xsd",
    namespace_uri="http://xbrlsite.com/type-subtype/",
    taxonomy_type="mapping",
    description=(
      "Charlie Hoffman's type-subtype classification linkbase — 2,161+ "
      "general-special arcs that classify rs-gaap concepts into reporting "
      "networks (CurrentAssets, Revenues, OperatingExpenses, etc.). Pure "
      "arc enrichment on rs-gaap; contributes no new concepts of its own. "
      "Primary classification source — replaces the balance+period heuristic."
    ),
  ),
]


def build_seed(seed: SeedSpec, client: ArelleClient, output_dir: Path) -> Path:
  """Load, extract, serialize, and write one seed."""
  logger.info(f"=== Building seed: {seed.standard}/{seed.version} ===")
  logger.info(f"Source: {seed.url}")

  model_xbrl = client.controller(seed.url)
  if model_xbrl is None or model_xbrl.modelDocument is None:
    raise RuntimeError(f"Failed to load {seed.url}")

  graph = extract_taxonomy(model_xbrl)

  jsonld = serialize_jsonld(
    graph,
    standard=seed.standard,
    version=seed.version,
    namespace_uri=seed.namespace_uri,
    description=seed.description,
    taxonomy_type=seed.taxonomy_type,
  )

  out_dir = output_dir / seed.standard / seed.version
  out_dir.mkdir(parents=True, exist_ok=True)
  out_path = out_dir / "taxonomy.jsonld"
  out_path.write_text(jsonld, encoding="utf-8")

  size_kb = len(jsonld) / 1024
  logger.info(f"Wrote {out_path} ({size_kb:.1f} KB)")
  return out_path


def main() -> int:
  parser = argparse.ArgumentParser(description=__doc__)
  parser.add_argument(
    "--only",
    type=str,
    default=None,
    help="Build only this standard (e.g. 'sfac6', 'fac', 'type-subtype')",
  )
  parser.add_argument(
    "--url",
    type=str,
    default=None,
    help="Custom URL to ingest (requires --standard and --version)",
  )
  parser.add_argument("--standard", type=str, default=None)
  parser.add_argument("--version", type=str, default=None)
  parser.add_argument(
    "--output-dir",
    type=str,
    default=str(SEEDS_DIR),
    help=f"Output directory (default: {SEEDS_DIR})",
  )
  args = parser.parse_args()

  output_dir = Path(args.output_dir)
  output_dir.mkdir(parents=True, exist_ok=True)

  # Determine which seeds to build
  if args.url:
    if not args.standard or not args.version:
      print("--url requires --standard and --version", file=sys.stderr)
      return 2
    seeds = [SeedSpec(standard=args.standard, version=args.version, url=args.url)]
  elif args.only:
    seeds = [s for s in SEEDS if s.standard == args.only]
    if not seeds:
      print(
        f"No seed named {args.only!r}. Known: {[s.standard for s in SEEDS]}",
        file=sys.stderr,
      )
      return 2
  else:
    seeds = SEEDS

  client = ArelleClient()
  try:
    for seed in seeds:
      build_seed(seed, client, output_dir)
  finally:
    client.close()

  print(f"\nAll seeds written to {output_dir}")
  return 0


if __name__ == "__main__":
  sys.exit(main())
