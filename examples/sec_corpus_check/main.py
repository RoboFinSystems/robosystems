"""SEC corpus check: load a known set of filings through the pipeline, then audit
what the text index holds against what the parsers produced.

The corpus is a directory in the Filing Ladder's layout — one folder per
accession holding ``meta.json`` (``cik``, ``form``, ``filing_date``) — which
``filing-ladder materialize`` writes and its ``bin/check_text_layer.py`` reads.
The ladder checks the parsers against each filing's own text-block facts; this
checks the platform's index against the parsers, so between them a parse
defect and an index defect are told apart.

    just sec-corpus-check load    --corpus ../filing-ladder/data
    just sec-corpus-check audit   --corpus ../filing-ladder/data [--check text-layer.json]
    just sec-corpus-check probe   --corpus ../filing-ladder/data
    just sec-corpus-check reindex --corpus ../filing-ladder/data

``load`` runs the pipeline's stages as ``just sec-load`` does, selecting filers
by CIK per quarter (a delisted filer no longer resolves by ticker; an empty
filter would download the whole quarter) and resetting the local SEC graph
first, as the pipeline does. ``audit`` reads OpenSearch per accession: the
section sets and part counts against the ladder check's JSON when given, the
part chains, the metadata the index assets add, the embeddings, and one CDN
copy per source. ``probe`` searches each filing through the API the way an
agent would and reads the top hit. ``reindex`` force re-indexes both source
types for the corpus quarters, for re-running ``audit`` after a parser change.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

import requests
import yaml

from examples._common.config import require_api_key, require_config
from robosystems.scripts.sec_pipeline import (
  DEFAULT_MATERIALIZE_TIMEOUT,
  SECPipeline,
  StageResult,
)

GRAPH_ID = "sec"
FORM_TYPES = ["10-K", "10-Q"]
INDEX_JOBS = (
  ("sec_narratives_index", "sec_narratives_indexed"),
  ("sec_ixbrl_index", "sec_ixbrl_disclosures_indexed"),
)
SOURCE = {"narrative_section": "narrative", "ixbrl_disclosure": "ixbrl"}
WEBSERVER = "robosystems-dagster-webserver"


# ---------------------------------------------------------------------------
# The corpus


def quarter_of(date: str) -> str:
  year, month, _ = date.split("-")
  return f"{year}-Q{(int(month) - 1) // 3 + 1}"


def corpus_filings(corpus: Path) -> list[dict[str, Any]]:
  filings = []
  for meta_path in sorted(corpus.glob("*/meta.json")):
    meta = json.loads(meta_path.read_text())
    filings.append(
      {
        "accession": meta["accession"],
        "cik": str(int(meta["cik"])),
        "form": meta["form"],
        "quarter": quarter_of(meta["filing_date"]),
        "ticker": meta.get("ticker") or "",
        "name": meta.get("entity_name") or "",
      }
    )
  if not filings:
    raise SystemExit(f"No */meta.json under {corpus}")
  return filings


def filers_by_quarter(filings: list[dict[str, Any]]) -> dict[str, set[str]]:
  out: dict[str, set[str]] = defaultdict(set)
  for f in filings:
    out[f["quarter"]].add(f["cik"])
  return out


def job_config(op: str, config: dict[str, Any]) -> str:
  """Write a run config for ``op`` and copy it into the webserver container."""
  body = {"ops": {op: {"config": config}}}
  with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
    yaml.dump(body, f, default_flow_style=False)
    host_path = f.name
  os.chmod(host_path, 0o644)
  container_path = f"/tmp/sec_corpus_{op}_{int(time.time() * 1000)}.yaml"
  subprocess.run(
    ["docker", "cp", host_path, f"{WEBSERVER}:{container_path}"],
    check=True,
    capture_output=True,
  )
  Path(host_path).unlink()
  return container_path


# ---------------------------------------------------------------------------
# load


class CorpusPipeline(SECPipeline):
  """The SEC pipeline with the download stage selecting filers by CIK per quarter."""

  def __init__(self, by_quarter: dict[str, set[str]]):
    years = sorted({q.split("-Q")[0] for q in by_quarter})
    super().__init__(tickers=[], years=years)
    self.by_quarter = by_quarter

  def run_stage(
    self, job_name: str, config_path: str, year: str | None = None, timeout: int = 600
  ) -> StageResult:
    if job_name == "sec_download":
      ciks = sorted(self.by_quarter.get(year or "", ()))
      if not ciks:
        print(f"  {year}: no corpus filer, download skipped")
        return StageResult(
          stage=job_name, year=year or "all", success=True, duration_seconds=0.0
        )
      print(f"  {year}: {len(ciks)} filers by CIK")
      config_path = job_config(
        "sec_raw_filings",
        {"skip_existing": True, "form_types": FORM_TYPES, "tickers": [], "ciks": ciks},
      )
    return super().run_stage(job_name, config_path, year=year, timeout=timeout)

  def _exec_docker(self, cmd: list[str], timeout: int = 600) -> tuple[bool, str, str]:
    # A quarter of large 10-Ks can outrun the driver's fixed processing budget.
    if "sec_process" in cmd:
      timeout = max(timeout, DEFAULT_MATERIALIZE_TIMEOUT)
    return super()._exec_docker(cmd, timeout)


def cmd_load(args: argparse.Namespace) -> int:
  filings = corpus_filings(args.corpus)
  by_quarter = filers_by_quarter(filings)
  print(f"{len(filings)} filings over {', '.join(sorted(by_quarter))}")
  results = CorpusPipeline(by_quarter).run()
  print(json.dumps({k: v for k, v in results.items() if k != "companies"}, indent=2))
  return 0 if results.get("status") == "success" else 1


# ---------------------------------------------------------------------------
# reindex


def cmd_reindex(args: argparse.Namespace) -> int:
  quarters = sorted(filers_by_quarter(corpus_filings(args.corpus)))
  pipeline = SECPipeline(tickers=[], years=[])
  failed = 0
  for quarter in quarters:
    for job, op in INDEX_JOBS:
      path = job_config(op, {"graph_id": GRAPH_ID, "force_reindex": True})
      result = pipeline.run_stage(job, path, year=quarter, timeout=3600)
      failed += 0 if result.success else 1
      print(
        f"{quarter} {job} {'ok' if result.success else 'FAILED'} "
        f"{result.duration_seconds:.0f}s {result.error or ''}"
      )
  return 1 if failed else 0


# ---------------------------------------------------------------------------
# audit


def opensearch_client() -> tuple[Any, str]:
  from opensearchpy import OpenSearch

  from robosystems.config import env

  return OpenSearch(env.OPENSEARCH_URL, timeout=60), env.OPENSEARCH_INDEX


def fetch_documents(client: Any, index: str, accession: str) -> list[dict[str, Any]]:
  body = {
    "size": 2000,
    "query": {
      "bool": {
        "filter": [
          {"term": {"graph_id": GRAPH_ID}},
          {"term": {"accession_number": accession}},
        ]
      }
    },
    "_source": {"excludes": ["embedding", "content"]},
    "sort": ["_doc"],
  }
  hits = client.search(index=index, body=body)["hits"]["hits"]
  return [h["_source"] for h in hits]


def embedded_count(client: Any, index: str, accession: str) -> int:
  body = {
    "query": {
      "bool": {
        "filter": [
          {"term": {"graph_id": GRAPH_ID}},
          {"term": {"accession_number": accession}},
          {"exists": {"field": "embedding"}},
        ]
      }
    }
  }
  return int(client.count(index=index, body=body)["count"])


def expected_from_check(
  check: list[dict[str, Any]],
) -> dict[str, dict[str, dict[str, int]]]:
  """{accession: {source: {section_id: part count}}} from the ladder check's JSON."""
  out: dict[str, dict[str, dict[str, int]]] = {}
  for entry in check:
    sections: dict[str, dict[str, int]] = {"narrative": {}, "ixbrl": {}}
    for row in entry.get("items", {}).get("rows", []):
      if "parts" in row:
        sections["narrative"][row["item"]] = row["parts"]
    for row in entry.get("disclosures", {}).get("rows", []):
      if row.get("parts"):
        sections["ixbrl"][row["concept"]] = row["parts"]
    out[entry["accession"]] = sections
  return out


def audit_accession(
  client: Any,
  index: str,
  filing: dict[str, Any],
  expected: dict[str, dict[str, int]] | None,
) -> tuple[dict[str, Any], list[str]]:
  accession = filing["accession"]
  docs = fetch_documents(client, index, accession)
  problems: list[str] = []
  if not docs:
    return {"docs": 0}, ["no documents indexed"]

  by_source: dict[str, list[dict[str, Any]]] = defaultdict(list)
  for d in docs:
    by_source[SOURCE.get(d.get("source_type", ""), d.get("source_type", ""))].append(d)

  summary: dict[str, Any] = {"docs": len(docs)}
  unexpected = {
    k: len(v) for k, v in by_source.items() if k not in ("narrative", "ixbrl")
  }
  if unexpected:
    problems.append(
      f"documents of a source type this audit does not check: {unexpected}"
    )
  for source in ("narrative", "ixbrl"):
    sections: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for d in by_source.get(source, []):
      sections[d["section_id"]].append(d)
    summary[source] = {
      "sections": len(sections),
      "docs": len(by_source.get(source, [])),
    }

    if expected is not None:
      want = expected[source]
      missing = sorted(set(want) - set(sections))
      extra = sorted(set(sections) - set(want))
      if missing:
        problems.append(
          f"{source}: {len(missing)} parsed sections not indexed: {missing[:4]}"
        )
      if extra:
        problems.append(
          f"{source}: {len(extra)} indexed sections the parser did not produce: {extra[:4]}"
        )
      for sid, ds in sections.items():
        if sid in want and want[sid] != len(ds):
          problems.append(
            f"{source} {sid}: {len(ds)} parts indexed, parser made {want[sid]}"
          )

    for sid, ds in sections.items():
      ds.sort(key=lambda d: d.get("part") or 1)
      count = ds[0].get("part_count") or 1
      if [d.get("part") or 1 for d in ds] != list(range(1, count + 1)):
        problems.append(
          f"{source} {sid}: parts {[d.get('part') for d in ds]} of {count}"
        )
        continue
      if count > 1:
        ids = [d["document_id"] for d in ds]
        nexts = [d.get("next_document_id") for d in ds]
        parents = {d.get("parent_document_id") for d in ds}
        if len(parents) != 1 or None in parents or nexts[:-1] != ids[1:] or nexts[-1]:
          problems.append(f"{source} {sid}: part chain broken")
      elif ds[0].get("parent_document_id") or ds[0].get("next_document_id"):
        problems.append(f"{source} {sid}: an unsplit section carries chain fields")

  fields = (
    "entity_ticker",
    "entity_name",
    "entity_cik",
    "form_type",
    "fiscal_year",
    "filing_date",
  )
  values: dict[str, set[Any]] = defaultdict(set)
  for d in docs:
    for f in fields:
      values[f].add(d.get(f))
  for f, seen in values.items():
    if len(seen) != 1:
      problems.append(
        f"{f} varies across the filing's documents: {sorted(map(str, seen))[:4]}"
      )
  meta = {f: next(iter(v)) for f, v in values.items()}
  summary["meta"] = meta
  if meta.get("entity_ticker") in (None, "", "<NA>", "nan", "None"):
    problems.append(f"entity_ticker is {meta.get('entity_ticker')!r}")
  if not meta.get("entity_name"):
    problems.append("entity_name is empty")
  if (meta.get("form_type") or "").upper() != filing["form"].upper():
    problems.append(
      f"form_type {meta.get('form_type')!r}, corpus says {filing['form']!r}"
    )
  if meta.get("fiscal_year") is None:
    problems.append("fiscal_year is missing")

  embedded = embedded_count(client, index, accession)
  summary["embedded"] = embedded
  if embedded != len(docs):
    problems.append(f"embeddings on {embedded} of {len(docs)} documents")

  # One CDN copy per source, fetched. Disclosure documents whose fact was stored
  # inline (short, tag-free) have no copy by design; only a present URL is checked.
  for source in ("narrative", "ixbrl"):
    with_url = [d for d in by_source.get(source, []) if d.get("content_url")]
    if not with_url:
      continue
    url = max(with_url, key=lambda d: d.get("content_length") or 0)["content_url"]
    try:
      status = requests.get(url, timeout=30).status_code
    except requests.RequestException as e:
      problems.append(f"{source} content_url unreachable: {e}")
      continue
    if status != 200:
      problems.append(f"{source} content_url {url} -> HTTP {status}")
  return summary, problems


def cmd_audit(args: argparse.Namespace) -> int:
  filings = corpus_filings(args.corpus)
  client, index = opensearch_client()
  expected = (
    expected_from_check(json.loads(Path(args.check).read_text()))
    if args.check
    else None
  )
  bad = 0
  for filing in filings:
    want = expected.get(filing["accession"]) if expected else None
    summary, problems = audit_accession(client, index, filing, want)
    label = filing["ticker"] or filing["cik"]
    narr = summary.get("narrative", {})
    ix = summary.get("ixbrl", {})
    print(
      f"{label:10} {filing['form']:5} {filing['accession']} docs={summary.get('docs', 0):4} "
      f"narrative={narr.get('sections', 0)}s/{narr.get('docs', 0)}d "
      f"disclosures={ix.get('sections', 0)}s/{ix.get('docs', 0)}d "
      f"ticker={summary.get('meta', {}).get('entity_ticker')} "
      + ("OK" if not problems else f"{len(problems)} PROBLEMS")
    )
    for p in problems:
      print(f"    - {p}")
    bad += 1 if problems else 0
  print(f"{len(filings)} filings, {bad} with problems")
  return 1 if bad else 0


# ---------------------------------------------------------------------------
# probe


def cmd_probe(args: argparse.Namespace) -> int:
  cfg = require_config()
  headers = {"X-API-Key": require_api_key(cfg), "Content-Type": "application/json"}
  base = cfg.get("base_url", "http://localhost:8000")

  def search(body: dict[str, Any]) -> dict[str, Any]:
    r = requests.post(
      f"{base}/v1/graphs/{GRAPH_ID}/search", headers=headers, json=body, timeout=60
    )
    r.raise_for_status()
    return r.json()

  def section(document_id: str) -> dict[str, Any]:
    r = requests.get(
      f"{base}/v1/graphs/{GRAPH_ID}/search/{document_id}", headers=headers, timeout=60
    )
    r.raise_for_status()
    return r.json()

  bad = 0
  for filing in corpus_filings(args.corpus):
    cik = filing["cik"].zfill(10)
    # The entity filter matches a ticker, a CIK or a name. A hit reports the
    # ticker (the CIK for a ticker-less filer) and the name, not the CIK, so a
    # hit is the filer's when either matches — the name also covers an index
    # written before #1357, where a ticker-less filer's ticker reads "<NA>".
    entity = filing["ticker"] or cik
    name = filing["name"].upper()
    is_10k = filing["form"].upper().startswith("10-K")
    probes = [
      (
        "MD&A",
        {
          "query": "results of operations liquidity",
          "entity": entity,
          "section": "item_7" if is_10k else "item_2",
          "form_type": filing["form"],
        },
      ),
      (
        "policies",
        {
          "query": "significant accounting policies revenue recognition",
          "entity": entity,
          "source_type": "ixbrl_disclosure",
        },
      ),
    ]
    for name, body in probes:
      try:
        hits = search(body).get("hits", [])
      except requests.RequestException as e:
        print(f"{entity:10} {name}: search failed: {e}")
        bad += 1
        continue
      mine = [
        h
        for h in hits
        if h.get("entity_ticker") == entity
        or (name and (h.get("entity_name") or "").upper() == name)
      ]
      if not mine:
        print(f"{entity:10} {name}: no hit for the filer ({len(hits)} hits)")
        bad += 1
        continue
      top = mine[0]
      doc = section(top["document_id"])
      content = doc.get("content") or ""
      head = " ".join(content[:90].split())
      print(
        f"{entity:10} {name:8} {top.get('section_id')} part {top.get('part')}/{top.get('part_count')} "
        f"chars={len(content)} | {head}"
      )
  return 1 if bad else 0


# ---------------------------------------------------------------------------


def main() -> int:
  parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n\n")[0])
  sub = parser.add_subparsers(dest="command", required=True)
  for name, fn in (
    ("load", cmd_load),
    ("reindex", cmd_reindex),
    ("audit", cmd_audit),
    ("probe", cmd_probe),
  ):
    p = sub.add_parser(name)
    p.add_argument(
      "--corpus",
      type=Path,
      required=True,
      help="directory of <accession>/meta.json folders",
    )
    if name == "audit":
      p.add_argument(
        "--check", help="JSON written by the ladder's bin/check_text_layer.py --json"
      )
    p.set_defaults(fn=fn)
  args = parser.parse_args()
  return args.fn(args)


if __name__ == "__main__":
  sys.exit(main())
