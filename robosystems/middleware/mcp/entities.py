"""The reporting group an MCP session is connected to, for the handshake
instructions and `get-graph-info`."""

from __future__ import annotations

from typing import Any

from robosystems.db.extensions import extensions_session
from robosystems.operations.roboledger.reads.entity import list_entities


def ledger_entities(graph_id: str) -> list[dict[str, Any]] | None:
  """The graph's own entities, the group parent first, then by name. None
  when the graph has no ledger schema or the read fails — the caller says
  nothing rather than something wrong."""
  try:
    with extensions_session(graph_id) as session:
      rows = list_entities(session)
  except Exception:
    return None
  own = [row for row in rows if row.source != "linked"]
  parent_id = _group_parent_id(own)
  own.sort(key=lambda row: (row.id != parent_id, row.name.lower()))
  return [
    {
      "id": row.id,
      "name": row.name,
      "ticker": row.ticker,
      "entity_type": row.entity_type,
      "is_group_parent": row.id == parent_id,
      "parent_entity_id": row.parent_entity_id,
    }
    for row in own
  ]


def _group_parent_id(own: list[Any]) -> str | None:
  """The resolver's rule (`entity_scope`): the earliest ``is_parent`` row
  that is not a linked counterparty."""
  parents = sorted(
    (row for row in own if row.is_parent), key=lambda r: r.created_at or ""
  )
  return parents[0].id if parents else None
