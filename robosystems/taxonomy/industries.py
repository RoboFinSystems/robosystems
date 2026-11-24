"""The industry classification: the vocabulary and the public filers placed in it.

Two files in ``frameworks/industries/``:

  industries.csv  one row per node: id, parent, level, label, carried, replaced_by.
                  The id is the slug path (``health-care/biotechnology/clinical-stage``),
                  so a node's ancestors are its id's prefixes.
  companies.csv   one row per filer-industry pair: key, cik, name, industry, primary,
                  method, confidence, review. A filer has one primary row and any
                  number of others. The cik is the filer; key (its ticker) and name
                  only record who the row was written about, and are never a source
                  for either. Tickers and SEC names change.

The levels are sector > subsector > industry > sub-industry. Every subsector has at
least one industry; where nobody has divided one, its single industry repeats its name
(``carried``). A node that is removed keeps its row with ``replaced_by`` naming its
successor, so an id stored on an entity never dangles.

The vocabulary is seeded into ``public.industries`` (``seed_industries``), one copy
that every tenant reads. The filer placements stay a file: only the SEC pipeline
reads them.
"""

from __future__ import annotations

import csv
from collections import Counter, defaultdict
from functools import lru_cache
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.engine import Connection

from robosystems.taxonomy.discovery import FRAMEWORKS_DIR

INDUSTRIES_DIR = FRAMEWORKS_DIR / "industries"
INDUSTRIES_CSV = INDUSTRIES_DIR / "industries.csv"
COMPANIES_CSV = INDUSTRIES_DIR / "companies.csv"

LEVELS = ("sector", "subsector", "industry", "sub-industry")
# How a filer was placed: the SIC crosswalk alone, recall of the company, or its filing.
METHODS = frozenset({"crosswalk", "recall", "filing"})
CONFIDENCE = frozenset({"high", "medium", "low"})


def _rows(path: Path) -> list[dict[str, str]]:
  with path.open(encoding="utf-8", newline="") as f:
    return list(csv.DictReader(f))


@lru_cache(maxsize=1)
def load_industries() -> dict[str, dict[str, str]]:
  """id -> row, in file order."""
  return {r["id"]: r for r in _rows(INDUSTRIES_CSV)}


@lru_cache(maxsize=1)
def load_companies() -> tuple[dict[str, str], ...]:
  return tuple(_rows(COMPANIES_CSV))


def lineage(node_id: str) -> list[str]:
  """A node's ancestors and itself, root first: a/b/c -> [a, a/b, a/b/c]."""
  parts = node_id.split("/")
  return ["/".join(parts[: i + 1]) for i in range(len(parts))]


def labels(node_id: str) -> list[str]:
  """Display path, root first. A carried industry repeats its subsector, so it is
  left out."""
  industries = load_industries()
  return [
    industries[n]["label"]
    for n in lineage(node_id)
    if industries[n]["carried"] != "yes"
  ]


def resolve(node_id: str) -> str | None:
  """The active id a stored id stands for: itself, or its successor when it has been
  retired. None when it is not in the vocabulary."""
  industries = load_industries()
  seen: set[str] = set()
  while node_id in industries and node_id not in seen:
    seen.add(node_id)
    successor = industries[node_id]["replaced_by"]
    if not successor:
      return node_id
    node_id = successor
  return None


def check() -> list[str]:
  """Every rule the two files must hold. Empty when clean."""
  problems: list[str] = []
  rows = _rows(INDUSTRIES_CSV)
  industries = {r["id"]: r for r in rows}
  if len(rows) != len(industries):
    dupes = [i for i, n in Counter(r["id"] for r in rows).items() if n > 1]
    problems.append(f"industries.csv: duplicate ids {dupes}")

  active_children: dict[str, list[str]] = defaultdict(list)
  for r in rows:
    if r["parent"] and not r["replaced_by"]:
      active_children[r["parent"]].append(r["id"])

  for r in rows:
    i, depth = r["id"], r["id"].count("/")
    if r["parent"] != "/".join(i.split("/")[:-1]):
      problems.append(f"{i}: parent '{r['parent']}' is not the id's own prefix")
    if r["parent"] and r["parent"] not in industries:
      problems.append(f"{i}: parent '{r['parent']}' is not a node")
    if depth >= len(LEVELS) or r["level"] != LEVELS[depth]:
      problems.append(f"{i}: level '{r['level']}' does not match its depth")
    if r["replaced_by"]:
      successor = industries.get(r["replaced_by"])
      if successor is None or successor["replaced_by"]:
        problems.append(f"{i}: replaced_by must name an active node")
      continue
    if r["level"] == "subsector" and not active_children[i]:
      problems.append(f"{i}: a subsector needs at least one industry")
    if r["carried"] == "yes":
      parent = industries.get(r["parent"], {})
      if r["level"] != "industry" or r["label"] != parent.get("label"):
        problems.append(f"{i}: only an industry named for its subsector is carried")
      if len(active_children[r["parent"]]) != 1:
        problems.append(
          f"{i}: a carried industry must be its subsector's only one; "
          "a split replaces it"
        )
      if active_children[i]:
        problems.append(f"{i}: a carried industry has no sub-industries; split it")

  # The cik is the filer. key and name say who we meant, so a filer's rows agree on them.
  primaries: Counter[str] = Counter()
  seen: set[tuple[str, str]] = set()
  said: dict[str, tuple[str, str]] = {}
  keys: dict[str, str] = {}
  for r in _rows(COMPANIES_CSV):
    key, cik, node = r["key"], r["cik"], r["industry"]
    if len(cik) != 10 or not cik.isdigit():
      problems.append(f"{key}: cik '{cik}' is not ten digits")
    if (cik, node) in seen:
      problems.append(f"{key}: listed twice in {node}")
    seen.add((cik, node))
    if said.setdefault(cik, (key, r["name"])) != (key, r["name"]):
      problems.append(f"{key}: cik {cik} is written under two keys or names")
    if keys.setdefault(key, cik) != cik:
      problems.append(f"{key}: used for two ciks")
    row = industries.get(node)
    if row is None:
      problems.append(f"{key}: industry '{node}' is not a node")
      continue
    if row["replaced_by"]:
      problems.append(f"{key}: industry '{node}' is retired; use its successor")
    # A crosswalk row is a SIC baseline nobody has judged, so it may sit at a subsector.
    level = row["level"]
    if level == "sector" or (level == "subsector" and r["method"] != "crosswalk"):
      problems.append(f"{key}: attach at an industry or deeper, not {node}")
    if r["method"] not in METHODS:
      problems.append(f"{key}: method '{r['method']}' is not one of {sorted(METHODS)}")
    if r["confidence"] not in CONFIDENCE:
      problems.append(
        f"{key}: confidence '{r['confidence']}' is not high, medium or low"
      )
    primaries[cik] += r["primary"] == "yes"
  problems.extend(
    f"{said[cik][0]}: needs exactly one primary industry, has {n}"
    for cik, n in primaries.items()
    if n != 1
  )
  return problems


def seed_industries(conn: Connection) -> int:
  """Make ``public.industries`` match industries.csv. Idempotent. A row the file no
  longer carries is left in place: an id is retired with ``replaced_by``, not deleted."""
  rows = [
    {
      "id": r["id"],
      "parent_id": r["parent"] or None,
      "level": r["level"],
      "label": r["label"],
      "carried": r["carried"] == "yes",
      "replaced_by": r["replaced_by"] or None,
    }
    for r in _rows(INDUSTRIES_CSV)
  ]
  conn.execute(
    text("""
      INSERT INTO public.industries (id, parent_id, level, label, carried, replaced_by)
      VALUES (:id, :parent_id, :level, :label, :carried, :replaced_by)
      ON CONFLICT (id) DO UPDATE SET
        parent_id   = EXCLUDED.parent_id,
        level       = EXCLUDED.level,
        label       = EXCLUDED.label,
        carried     = EXCLUDED.carried,
        replaced_by = EXCLUDED.replaced_by
    """),
    rows,
  )
  return len(rows)
