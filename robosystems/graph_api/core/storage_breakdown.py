"""Itemized, instance-scoped storage measurement for a graph.

The single source of truth for "how much disk does this graph occupy". Every
consumer — live display, cap enforcement, historical metering — reads this
rather than measuring its own way, because measuring it several ways is
exactly how the number came to be wrong several different ways.

A graph's footprint is not just its ``.lbug`` file. It spans three
instance-local roots, and the pieces outside the primary database routinely
outweigh it — staging alone is frequently larger than the graph. Counting only
the primary file undercounts real COGS by a wide margin.

Backups live in S3, off-instance, and are deliberately out of scope.
"""

from pathlib import Path
from typing import Any

from robosystems.config import env
from robosystems.graph_api.core.utils import validate_database_name
from robosystems.logger import logger

# Storage item types, in the order they are reported.
TYPE_GRAPH = "graph"
TYPE_MEMORY = "memory"
TYPE_SUBGRAPH = "subgraph"
TYPE_VECTORS = "vectors"
TYPE_STAGING = "staging"
TYPE_TRANSIENT = "transient"

# Applied by the platform API, not here: a `{parent}_*` database with no row in
# the graph registry. This module is instance-local and cannot tell a live
# subgraph from the remains of a deleted one, so it reports both as
# TYPE_SUBGRAPH and the registry-aware layer re-labels what it knows is gone.
TYPE_ORPHAN = "orphan"

# Blue-green build artifacts (see `swap_database`). Real disk, but transient and
# reclaimable, so they are reported apart from the databases they belong to.
TRANSIENT_SUFFIXES = ("-wip", "-prev")


def path_size_bytes(path: Path) -> int:
  """Size of a file or directory, recursing into directories.

  Both shapes must be handled: a LadybugDB database is a single file in some
  engine versions and a directory in others, and a caller that sizes only
  files reports zero for the directory form.
  """
  total = 0
  try:
    if path.is_file():
      return path.stat().st_size
    if path.is_dir():
      for item in path.rglob("*"):
        try:
          if item.is_file():
            total += item.stat().st_size
        except OSError:
          # A file vanishing mid-walk (compaction, WAL rotation) is normal;
          # skip it rather than failing the whole measurement.
          continue
  except OSError as e:
    logger.debug(f"Could not size {path}: {e}")
  return total


def _base_name(name: str) -> str:
  """Strip a blue-green suffix, leaving the database the artifact belongs to."""
  for suffix in TRANSIENT_SUFFIXES:
    if name.endswith(suffix):
      return name[: -len(suffix)]
  return name


def _owns(name: str, graph_id: str) -> bool:
  """Whether an on-disk name belongs to this graph.

  Exact match is the graph itself; the ``_`` boundary covers its memory
  database and subgraphs. Top-level ids are fixed-length ``kg`` + hex, so the
  prefix cannot collide with a different tenant's graph.

  The comparison is against the *base* name, which is what brings the graph's
  own build artifacts into scope: ``{graph_id}-wip`` is hyphen-separated and
  matches neither arm on its raw name, while a subgraph's
  ``{graph_id}_{name}-wip`` matches the ``_`` prefix regardless. Stripping the
  suffix first is what makes both count the same way.
  """
  base = _base_name(name)
  return base == graph_id or base.startswith(f"{graph_id}_")


def _classify_lbug(stem: str, graph_id: str) -> str:
  """Classify a `.lbug` database by its name.

  Order matters: the transient check must come first, because a subgraph's
  build artifact also matches the subgraph fallthrough, and classifying it as
  a subgraph makes this report disagree with the subgraph list.
  """
  if stem.endswith(TRANSIENT_SUFFIXES):
    return TYPE_TRANSIENT
  if stem == graph_id:
    return TYPE_GRAPH
  if stem == f"{graph_id}_memory":
    return TYPE_MEMORY
  return TYPE_SUBGRAPH


def _collect_lbug(root: Path, graph_id: str) -> list[dict[str, Any]]:
  """Databases and their write-ahead logs under the LadybugDB root.

  WAL bytes are folded into the database they belong to rather than itemized
  separately — they are the same logical object to anyone reading a quota.
  """
  sizes: dict[str, int] = {}
  if not root.is_dir():
    return []

  for item in root.iterdir():
    name = item.name
    if name.endswith(".lbug"):
      stem = name[: -len(".lbug")]
    elif name.endswith(".lbug.wal"):
      stem = name[: -len(".lbug.wal")]
    else:
      continue

    if not _owns(stem, graph_id):
      continue
    sizes[stem] = sizes.get(stem, 0) + path_size_bytes(item)

  return [
    {"type": _classify_lbug(stem, graph_id), "id": stem, "bytes": size}
    for stem, size in sorted(sizes.items())
  ]


def _collect_vectors(root: Path, graph_id: str) -> list[dict[str, Any]]:
  """LanceDB index directories, one per graph / memory / subgraph."""
  if not root.is_dir():
    return []
  return [
    {"type": TYPE_VECTORS, "id": item.name, "bytes": path_size_bytes(item)}
    for item in sorted(root.iterdir(), key=lambda p: p.name)
    if _owns(item.name, graph_id)
  ]


def _collect_staging(root: Path, graph_id: str) -> list[dict[str, Any]]:
  """DuckDB staging file — the Data Lake's physical footprint.

  Surfaced elsewhere as logical table bytes, which is a much smaller number
  than the file on disk. This counts the file, because that is what occupies
  the volume.
  """
  if not root.is_dir():
    return []
  return [
    {
      "type": TYPE_STAGING,
      "id": item.name[: -len(".duckdb")],
      "bytes": path_size_bytes(item),
    }
    for item in sorted(root.iterdir(), key=lambda p: p.name)
    if item.name.endswith(".duckdb") and _owns(item.name[: -len(".duckdb")], graph_id)
  ]


def compute_storage_breakdown(graph_id: str) -> dict[str, Any]:
  """Total and per-item disk usage for a graph and everything it owns.

  A graph's memory database, subgraphs, vector indexes and staging file are
  all attributed to it. Returns
  ``{graph_id, total_bytes, items: [{type, id, bytes}]}`` where ``type`` is
  one of graph, memory, subgraph, vectors, staging, transient. Raises
  ``HTTPException`` if ``graph_id`` fails name validation.
  """
  validated = validate_database_name(graph_id)

  items: list[dict[str, Any]] = []
  items.extend(_collect_lbug(Path(env.LBUG_DATABASE_PATH), validated))
  items.extend(_collect_vectors(Path(env.LANCE_INDEX_PATH), validated))
  items.extend(_collect_staging(Path(env.DUCKDB_STAGING_PATH), validated))

  return {
    "graph_id": validated,
    "total_bytes": sum(item["bytes"] for item in items),
    "items": items,
  }
