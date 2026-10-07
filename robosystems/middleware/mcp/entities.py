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
  own.sort(key=lambda row: (not _is_group_parent(row), row.name.lower()))
  return [
    {
      "id": row.id,
      "name": row.name,
      "ticker": row.ticker,
      "entity_type": row.entity_type,
      "is_group_parent": _is_group_parent(row),
      "parent_entity_id": row.parent_entity_id,
    }
    for row in own
  ]


def _is_group_parent(row: Any) -> bool:
  return bool(row.is_parent) and row.parent_entity_id is None
