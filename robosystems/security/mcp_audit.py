"""The mutation audit: one `mcp_mutation_audit` row per mutating MCP call.

Fail open: an audit row that cannot be written never undoes or blocks the
call it describes. The row is logged at ERROR as
`mcp_mutation_audit.unrecorded`, with every field, for backfill and alarms.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from typing import Any

from ..logger import logger
from .request_context import audit_context

_ID_KEYS = ("id", "structure_id", "block_id", "agent_id", "memory_id", "subgraph_id")


@dataclass
class McpCaller:
  """Who is calling, when the tool manager is driven by an in-app operator
  rather than an external MCP client."""

  operator_type: str | None = None
  operation_id: str | None = None


@dataclass
class McpMutationRecord:
  graph_id: str
  tool_name: str
  status: str
  duration_ms: float
  arguments_sha256: str
  caller_kind: str
  user_id: str | None = None
  error_code: str | None = None
  auth_method: str | None = None
  api_key_prefix: str | None = None
  request_id: str | None = None
  operator_type: str | None = None
  operation_id: str | None = None
  object_ids: list[str] = field(default_factory=list)


def fingerprint(arguments: dict[str, Any] | None) -> str:
  """A stable hash of the arguments: proves what was sent without storing it."""
  canonical = json.dumps(arguments or {}, sort_keys=True, default=str)
  return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def object_ids(arguments: dict[str, Any] | None, result: Any) -> list[str]:
  """Ids of the objects a call touched: the result's (one level down too,
  for wrapped objects), then the arguments' (an update names its target)."""
  sources: list[dict[str, Any]] = []
  if isinstance(result, dict):
    sources.append(result)
    sources.extend(v for v in result.values() if isinstance(v, dict))
  if isinstance(arguments, dict):
    sources.append(arguments)
  found: list[str] = []
  for src in sources:
    for key in _ID_KEYS:
      value = src.get(key)
      if isinstance(value, str) and value and value not in found:
        found.append(value)
  return found


def build_record(
  *,
  graph_id: str,
  tool_name: str,
  arguments: dict[str, Any] | None,
  result: Any,
  error: BaseException | None,
  duration_ms: float,
  user_id: str | None,
  caller: McpCaller | None,
) -> McpMutationRecord:
  if error is not None:
    status, error_code = "failed", type(error).__name__
  elif isinstance(result, dict) and "error" in result:
    status, error_code = "failed", str(result.get("error"))
  else:
    status, error_code = "completed", None

  context = audit_context()
  operator = caller if caller and caller.operator_type else None
  return McpMutationRecord(
    graph_id=graph_id,
    tool_name=tool_name,
    status=status,
    error_code=error_code,
    duration_ms=round(duration_ms, 2),
    arguments_sha256=fingerprint(arguments),
    caller_kind="operator" if operator else "client",
    user_id=user_id,
    auth_method=context.get("auth_method"),
    api_key_prefix=context.get("api_key_prefix"),
    request_id=context.get("request_id"),
    operator_type=operator.operator_type if operator else None,
    operation_id=operator.operation_id if operator else None,
    object_ids=object_ids(arguments, result) if status == "completed" else [],
  )


def write_record(record: McpMutationRecord) -> None:
  """Insert the row in its own short session; never raises."""
  from robosystems.db.platform import SessionFactory
  from robosystems.models.core import Graph, McpMutationAudit

  try:
    session = SessionFactory()
    try:
      graph = session.query(Graph.org_id).filter(Graph.graph_id == record.graph_id)
      org_id = graph.scalar()
      session.add(McpMutationAudit(org_id=org_id, **asdict(record)))
      session.commit()
    finally:
      session.close()
  except Exception as exc:
    logger.error(
      "mcp_mutation_audit.unrecorded",
      extra={"audit": {**asdict(record), "reason": type(exc).__name__}},
      exc_info=True,
    )
