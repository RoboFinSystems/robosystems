"""Reclaim the disk the storage breakdown calls reclaimable.

Two classes of leftover accumulate on an instance and nothing else collects
them:

- **Transient build artifacts** (`{base}-wip` / `{base}-prev`): a crashed
  blue-green build strands its WIP copy, and only a later materialization of
  the *same* database would clean it up (builds start by deleting any leftover
  WIP — there are no resume semantics, so deleting one early costs nothing).
  The Graph API refuses the delete while a build holds the base's
  materialization lock, so an in-flight rebuild cannot lose its target.
- **Orphan estates**: `{parent}_*` databases (plus their vector indexes and
  staging files) whose registry row is gone — the remains of a deleted
  subgraph. ``delete-subgraph`` cannot reach them because it resolves through
  the registry, which is precisely what no longer knows they exist.

Labelling reuses the same registry pass the storage breakdown uses
(`IngestionLimitChecker.label_orphans`), so this deletes exactly what
``/usage`` reports as `orphan`/`transient` — never anything the breakdown
still attributes to a live database. A registry read failure leaves items
unlabelled, so orphans are then *not* deleted: the sweep fails safe.

Driven by the daily ``storage_reclaim`` Dagster job, not by any user-facing
surface.

Known limitation: an orphan whose ``.lbug`` file is already gone but whose
vector index or staging file lingers is skipped (the Graph API's delete is
anchored on the database file). Those fragments are rare and small; they are
reported in ``skipped`` rather than silently ignored.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy.orm import Session

from robosystems.logger import logger

__all__ = ["reclaim_instance_storage"]


async def reclaim_instance_storage(
  graph_id: str,
  db: Session,
  dry_run: bool = False,
) -> dict[str, Any]:
  """Delete reclaimable storage items on a parent graph's instance.

  Returns ``{reclaimed, skipped, bytes_freed, ...}`` where ``reclaimed`` and
  ``skipped`` carry one entry per database id (an estate's lbug/vectors/staging
  bytes are folded together, mirroring how the breakdown attributes them).
  """
  from robosystems.graph_api.client.factory import get_graph_client
  from robosystems.graph_api.core.storage_breakdown import (
    TYPE_ORPHAN,
    TYPE_TRANSIENT,
  )
  from robosystems.middleware.graph.ingestion_limits import IngestionLimitChecker

  client = await get_graph_client(graph_id=graph_id, operation_type="write")
  try:
    breakdown = await client.get_storage_breakdown(graph_id)
    items = IngestionLimitChecker.label_orphans(
      db, graph_id, breakdown.get("items", [])
    )

    # One entry per database id: an orphan's lbug + vectors + staging items
    # are a single estate and a single delete call.
    targets: dict[str, dict[str, Any]] = {}
    for item in items:
      if item.get("type") not in (TYPE_TRANSIENT, TYPE_ORPHAN):
        continue
      item_id = item.get("id") or ""
      entry = targets.setdefault(
        item_id, {"id": item_id, "type": item.get("type"), "bytes": 0}
      )
      entry["bytes"] += item.get("bytes", 0)

    plan = sorted(targets.values(), key=lambda e: str(e["id"]))

    if dry_run:
      return {
        "graph_id": graph_id,
        "dry_run": True,
        "reclaimed": [],
        "would_reclaim": plan,
        "skipped": [],
        "bytes_freed": 0,
        "bytes_reclaimable": sum(e["bytes"] for e in plan),
      }

    reclaimed: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    for entry in plan:
      target_id = str(entry["id"])
      try:
        # A transient artifact shares its base database's name — preserve the
        # base's DuckDB staging and vector index (deleting only lbug + WAL).
        # An orphan is a whole estate: take its staging and vectors with it.
        await client.delete_database(
          target_id, preserve_duckdb=entry["type"] == TYPE_TRANSIENT
        )
        reclaimed.append(entry)
        logger.info(
          f"Reclaimed {entry['type']} storage {target_id} "
          f"({entry['bytes']} bytes) on {graph_id}'s instance"
        )
      except Exception as e:
        # 409 = a materialization holds the base lock (transient artifacts
        # only); 404 = no lbug file to anchor the delete (vectors/staging
        # fragment). Both are per-item outcomes, not reasons to abort the
        # rest of the sweep.
        skipped.append({**entry, "reason": str(e)})
        logger.warning(f"Could not reclaim {target_id} on {graph_id}: {e}")

    return {
      "graph_id": graph_id,
      "dry_run": False,
      "reclaimed": reclaimed,
      "skipped": skipped,
      "bytes_freed": sum(e["bytes"] for e in reclaimed),
    }
  finally:
    await client.close()
