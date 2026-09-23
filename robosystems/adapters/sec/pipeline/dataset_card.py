"""SEC dataset card: snapshot statistics and the Hugging Face README.

The counts are taken on the shared master just before the R2 snapshot is cut,
so they describe the file that ships. They travel beside it in R2 as a stats
file; the Hugging Face publish reads them back and renders ``dataset_card.md``.
"""

import math
import re
from datetime import datetime
from pathlib import Path
from typing import Any

from robosystems.graph_api.client.client import GraphClient

SEC_GRAPH_ID = "sec"
STATS_QUERY_TIMEOUT_SECONDS = 600.0
# Forms with fewer filings than this are summed into one trailing clause.
MINOR_FORM_THRESHOLD = 100

CARD_TEMPLATE_PATH = Path(__file__).with_name("dataset_card.md")
_PLACEHOLDER = re.compile(r"\{\{(\w+)\}\}")
_TABLE_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


async def _rows(client: GraphClient, cypher: str) -> list[dict[str, Any]]:
  result = await client.query(cypher, SEC_GRAPH_ID, timeout=STATS_QUERY_TIMEOUT_SECONDS)
  return result.get("data", []) if isinstance(result, dict) else []


async def _scalar(client: GraphClient, cypher: str) -> Any:
  rows = await _rows(client, cypher)
  return next(iter(rows[0].values())) if rows else None


async def capture_snapshot_stats(client: GraphClient) -> dict[str, Any]:
  """Count what the SEC graph holds, for the dataset card."""
  tables = await _rows(client, "CALL show_tables() RETURN name, type")
  node_tables = sorted(t["name"] for t in tables if t["type"] == "NODE")
  rel_tables = [t["name"] for t in tables if t["type"] == "REL"]

  node_counts: dict[str, int] = {}
  for name in node_tables:
    if not _TABLE_NAME.match(name):
      raise ValueError(f"Unexpected table name from show_tables: {name!r}")
    node_counts[name] = int(
      await _scalar(client, f"MATCH (n:{name}) RETURN count(n) AS n") or 0
    )

  form_rows = await _rows(
    client, "MATCH (r:Report) RETURN r.form AS form, count(r) AS n"
  )
  forms = {row["form"] or "": int(row["n"]) for row in form_rows}

  coverage = (
    await _rows(
      client,
      "MATCH (r:Report) RETURN min(r.filing_date) AS first, max(r.filing_date) AS last",
    )
  )[0]

  return {
    "filers": node_counts.get("Entity", 0),
    "filers_with_ticker": int(
      await _scalar(
        client,
        "MATCH (e:Entity) WHERE e.ticker IS NOT NULL AND e.ticker <> '' "
        "RETURN count(e) AS n",
      )
      or 0
    ),
    "filings": node_counts.get("Report", 0),
    "forms": forms,
    "facts": node_counts.get("Fact", 0),
    "numeric_facts": int(
      await _scalar(
        client,
        "MATCH (f:Fact) WHERE f.numeric_value IS NOT NULL RETURN count(f) AS n",
      )
      or 0
    ),
    "nodes": sum(node_counts.values()),
    "node_counts": node_counts,
    "node_types": len(node_tables),
    "relationship_types": len(rel_tables),
    "coverage_start": str(coverage["first"])[:10],
    "coverage_end": str(coverage["last"])[:10],
  }


def _gib(size_bytes: int) -> float:
  return size_bytes / 1024**3


def _millions(count: int) -> str:
  return f"{count / 1_000_000:.1f}"


def _forms_line(forms: dict[str, int]) -> str:
  ranked = sorted(forms.items(), key=lambda item: (-item[1], item[0]))
  major = [
    f"{form} {count:,}"
    for form, count in ranked
    if form and count >= MINOR_FORM_THRESHOLD
  ]
  minor = sum(
    count for form, count in ranked if not form or count < MINOR_FORM_THRESHOLD
  )
  line = " · ".join(major)
  if minor:
    line += f" (and {minor:,} other filing{'' if minor == 1 else 's'})"
  return line


def render_dataset_card(snapshot: dict[str, Any]) -> str:
  """Render the Hugging Face README for one snapshot.

  ``snapshot`` carries ``snapshot_at``, ``compressed_size_bytes``,
  ``original_size_bytes``, ``engine_version``, ``storage_version`` and the
  ``stats`` captured at cut time. Raises if any placeholder goes unfilled, so
  a card is never published with a hole in it.
  """
  stats = snapshot["stats"]
  snapshot_at: datetime = snapshot["snapshot_at"]
  compressed = snapshot["compressed_size_bytes"]
  uncompressed = snapshot["original_size_bytes"]
  storage_version = snapshot.get("storage_version")

  values = {
    "snapshot_date": f"{snapshot_at:%Y-%m-%d}",
    "coverage_start": stats["coverage_start"],
    "coverage_end": stats["coverage_end"],
    "filers": f"{stats['filers']:,}",
    "filers_with_ticker": f"{stats['filers_with_ticker']:,}",
    "filings": f"{stats['filings']:,}",
    "forms": _forms_line(stats["forms"]),
    "facts_millions": _millions(stats["facts"]),
    "numeric_facts_millions": _millions(stats["numeric_facts"]),
    "nodes_millions": _millions(stats["nodes"]),
    "node_types": str(stats["node_types"]),
    "relationship_types": str(stats["relationship_types"]),
    "engine_version": snapshot["engine_version"],
    "storage_format": f"v{storage_version}" if storage_version else "unknown",
    "compressed_gib": f"{_gib(compressed):.1f}",
    "compressed_bytes": f"{compressed:,}",
    "uncompressed_gib": f"{_gib(uncompressed):.1f}",
    "uncompressed_gib_whole": f"{_gib(uncompressed):.0f}",
    "uncompressed_bytes": f"{uncompressed:,}",
    "free_gib": str(math.ceil(_gib(compressed + uncompressed) / 10) * 10),
  }

  template = CARD_TEMPLATE_PATH.read_text()
  missing = sorted(set(_PLACEHOLDER.findall(template)) - values.keys())
  if missing:
    raise ValueError(f"Dataset card placeholders without a value: {missing}")
  return _PLACEHOLDER.sub(lambda m: values[m.group(1)], template)
