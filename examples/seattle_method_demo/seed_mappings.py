#!/usr/bin/env python3
"""Seed mini → rs-gaap mapping associations on a graph.

Walks the curated table in ``mappings.py`` and posts one
``create_mapping_association`` per pair, wiring each mini concept to its
rs-gaap counterpart. This is what makes the cross-taxonomy projection
possible: once the associations exist, the same postings can be rendered
through either vocabulary.

Prerequisites: ``load_taxonomy.py`` has run against this graph — it creates
the mini CoA and the empty "mini to rs-gaap Mapping" structure these
associations attach to — and rs-gaap is available as a library taxonomy.

Pairs whose concepts do not resolve on the graph are reported as warnings and
skipped, so a partially-loaded taxonomy is visible rather than silent.

Run it (the orchestrator runs this as step 4):
    uv run python -m examples.seattle_method_demo.seed_mappings <graph_id>
    uv run python -m examples.seattle_method_demo.seed_mappings <graph_id> --dry-run
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .mappings import ALL_MAPPINGS, BS_IS_MAPPINGS, FLOW_MAPPINGS


def _get_ledger_client():
  """Construct a LedgerClient from the credentials ``just demo-user`` saved."""
  from robosystems_client.clients.ledger_client import LedgerClient

  config_path = Path(".local/config.json")
  if not config_path.exists():
    raise SystemExit(
      "Missing .local/config.json — run `just demo-user` first."
    )
  with config_path.open() as f:
    cfg = json.load(f)
  return LedgerClient(
    {
      "base_url": cfg.get("base_url", "http://localhost:8000"),
      "token": cfg["api_key"],
    }
  )


def _find_mini_taxonomy_id(client, graph_id: str) -> str | None:
  """Find the mini CoA taxonomy_id on the graph by name.

  Mini Elements load with ``source='native'`` — the Element source CHECK
  constraint admits a fixed enum with no 'mini' member — so element lookups
  scope by ``taxonomy_id``, resolved once here.
  """
  taxonomies = client.list_taxonomies(graph_id) or []
  for tax in taxonomies:
    if "mini" in (tax.get("name") or "").lower():
      return tax.get("id")
  return None


def _build_qname_lookup_by_taxonomy(
  client, graph_id: str, taxonomy_id: str
) -> dict[str, str]:
  """Page through ``list_elements(taxonomy_id=…)`` → {qname: element_id}."""
  out: dict[str, str] = {}
  offset = 0
  page_size = 1000
  while True:
    page = client.list_elements(
      graph_id, taxonomy_id=taxonomy_id, limit=page_size, offset=offset
    )
    items = page.elements if page else []
    if not items:
      break
    for e in items:
      if e.qname:
        out[e.qname] = e.id
    if len(items) < page_size:
      break
    offset += page_size
  return out


def _build_qname_lookup(
  client, graph_id: str, source: str
) -> dict[str, str]:
  """Page through ``list_elements(source=…)`` → {qname: element_id}.

  Used for rs-gaap, which is filterable by its canonical source.
  """
  out: dict[str, str] = {}
  offset = 0
  page_size = 1000
  while True:
    page = client.list_elements(
      graph_id, source=source, limit=page_size, offset=offset
    )
    items = page.elements if page else []
    if not items:
      break
    for e in items:
      if e.qname:
        out[e.qname] = e.id
    if len(items) < page_size:
      break
    offset += page_size
  return out


def _find_mapping_structure(client, graph_id: str) -> str:
  """Return the structure_id of the mini→rs-gaap mapping structure.

  ``load_taxonomy.py`` creates it as ``block_type='coa_mapping'`` alongside
  the CoA elements.
  """
  structures = client.list_structures(graph_id, block_type="coa_mapping")
  if not structures:
    raise SystemExit(
      "No coa_mapping structure found on this graph. Did you run "
      "load_taxonomy.py first?"
    )
  # Prefer the mini mapping structure when the graph carries several.
  for s in structures:
    if "mini" in (s.name or "").lower():
      return s.id
  return structures[0].id


def seed_mappings(graph_id: str, dry_run: bool = False) -> tuple[int, list[str]]:
  """Seed every (mini, rs-gaap) pair in ``ALL_MAPPINGS`` as a derivation
  association on the graph.

  Returns ``(created_count, warnings)``. A warning means one side of a pair
  did not resolve to an Element — usually a taxonomy that loaded only
  partially, or a qname typo in ``mappings.py``.
  """
  client = _get_ledger_client()

  print(f"Resolving mini concept ids on graph {graph_id} …")
  mini_taxonomy_id = _find_mini_taxonomy_id(client, graph_id)
  if not mini_taxonomy_id:
    raise SystemExit(
      "No mini CoA taxonomy found on this graph. Did load_taxonomy.py run?"
    )
  mini_lookup = _build_qname_lookup_by_taxonomy(
    client, graph_id, mini_taxonomy_id
  )
  print(f"  {len(mini_lookup)} mini concept(s) found (taxonomy {mini_taxonomy_id})")

  print(f"Resolving rs-gaap concept ids on graph {graph_id} …")
  rsgaap_lookup = _build_qname_lookup(client, graph_id, source="rs-gaap")
  print(f"  {len(rsgaap_lookup)} rs-gaap concept(s) found")

  print(f"Mapping table: {len(BS_IS_MAPPINGS)} BS/IS + {len(FLOW_MAPPINGS)} flow")
  print(f"  Total to seed: {len(ALL_MAPPINGS)}")

  mapping_id = _find_mapping_structure(client, graph_id)
  print(f"  Mapping structure: {mapping_id}")

  warnings: list[str] = []
  created = 0
  for mini_qname, rsgaap_qname in ALL_MAPPINGS:
    mini_id = mini_lookup.get(mini_qname)
    rsgaap_id = rsgaap_lookup.get(rsgaap_qname)
    if not mini_id:
      warnings.append(f"mini qname not found: {mini_qname}")
      continue
    if not rsgaap_id:
      warnings.append(f"rs-gaap qname not found: {rsgaap_qname}")
      continue
    if dry_run:
      created += 1
      continue
    client.create_mapping_association(
      graph_id,
      mapping_id=mapping_id,
      from_element_id=mini_id,
      to_element_id=rsgaap_id,
    )
    created += 1

  return created, warnings


def main() -> None:
  parser = argparse.ArgumentParser(
    description="Seed mini→rs-gaap mapping associations.",
  )
  parser.add_argument("graph_id", help="Target graph id.")
  parser.add_argument(
    "--dry-run",
    action="store_true",
    help="Resolve lookups + report counts; do not write associations.",
  )
  args = parser.parse_args()

  created, warnings = seed_mappings(args.graph_id, dry_run=args.dry_run)

  if warnings:
    print("\nWarnings:")
    for w in warnings:
      print(f"  ⚠️  {w}")

  action = "Would create" if args.dry_run else "Created"
  print(f"\n{action} {created} mapping association(s).")
  if warnings:
    print(f"({len(warnings)} pair(s) skipped — see warnings above.)")
    sys.exit(1 if not args.dry_run else 0)


if __name__ == "__main__":
  main()
