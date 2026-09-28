"""The mutation audit: one `operation_mutation_audit` row per mutating call on
a graph, from the REST API, an external MCP client, or an in-app operator.

Fail open: a row that cannot be built or written never undoes or blocks the
call it describes. It is logged at ERROR as
`operation_mutation_audit.unrecorded`, with every field, for backfill and
alarms.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass, field
from typing import Any

from ..logger import logger
from .request_context import audit_context

# The row carries these in its own columns.
_CONTEXT_ID_KEYS = frozenset({"graph_id", "user_id", "org_id"})
# Enough to name what a call touched; a bulk write can't bloat a row.
_MAX_OBJECT_IDS = 20

# REST rows are recorded from synchronous code on the event loop; the insert
# runs here instead of blocking it.
_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="operation-audit")


@dataclass
class AuditCaller:
  """The in-app operator run driving an MCP tool manager."""

  operator_type: str | None = None
  operation_id: str | None = None


@dataclass
class MutationRecord:
  graph_id: str
  surface: str
  operation_name: str
  status: str
  duration_ms: float
  user_id: str | None = None
  error_code: str | None = None
  auth_method: str | None = None
  api_key_prefix: str | None = None
  request_id: str | None = None
  operation_id: str | None = None
  operator_type: str | None = None
  arguments_fingerprint: str | None = None
  object_ids: list[str] = field(default_factory=list)


def _is_id_key(key: str) -> bool:
  return (key == "id" or key.endswith("_id")) and key not in _CONTEXT_ID_KEYS


def object_ids(arguments: Any, result: Any) -> list[str]:
  """Ids of the objects a call touched: the result's (one level down too,
  for wrapped objects), then the arguments' (an update names its target).
  Any `id` or `*_id` key counts, so a new operation's naming is covered."""
  sources: list[dict[str, Any]] = []
  if isinstance(result, dict):
    sources.append(result)
    sources.extend(v for v in result.values() if isinstance(v, dict))
  if isinstance(arguments, dict):
    sources.append(arguments)
  found: list[str] = []
  for src in sources:
    for key, value in src.items():
      if not (isinstance(key, str) and _is_id_key(key)):
        continue
      if isinstance(value, str) and value and value not in found:
        found.append(value)
        if len(found) == _MAX_OBJECT_IDS:
          return found
  return found


def _with_request_context(record: MutationRecord) -> MutationRecord:
  context = audit_context()
  record.auth_method = context.get("auth_method")
  record.api_key_prefix = context.get("api_key_prefix")
  record.request_id = context.get("request_id")
  return record


def build_tool_record(
  *,
  graph_id: str,
  tool_name: str,
  arguments: dict[str, Any] | None,
  result: Any,
  error: BaseException | None,
  duration_ms: float,
  user_id: str | None,
  caller: AuditCaller | None,
) -> MutationRecord:
  """The row for an MCP tool call: `operator` surface when an in-app
  operator drives the tool manager, `mcp` for an external client."""
  from robosystems.middleware.operations import fingerprint_body

  if error is not None:
    status, error_code = "failed", type(error).__name__
  elif isinstance(result, dict) and "error" in result:
    status, error_code = "failed", str(result.get("error"))
  else:
    status, error_code = "completed", None

  operator = caller if caller and caller.operator_type else None
  return _with_request_context(
    MutationRecord(
      graph_id=graph_id,
      surface="operator" if operator else "mcp",
      operation_name=tool_name,
      status=status,
      error_code=error_code,
      duration_ms=round(duration_ms, 2),
      user_id=user_id,
      operator_type=operator.operator_type if operator else None,
      operation_id=operator.operation_id if operator else None,
      arguments_fingerprint=fingerprint_body(arguments or {}),
      object_ids=object_ids(arguments, result) if status == "completed" else [],
    )
  )


def record_api_operation(
  *,
  operation_name: str,
  operation_id: str,
  user_id: str,
  graph_id: str,
  duration_ms: float,
  status: str,
  error: str | None,
  arguments_fingerprint: str | None,
  result: Any,
) -> None:
  """Queue the row for a REST operation call. Never raises."""
  try:
    record = _with_request_context(
      MutationRecord(
        graph_id=graph_id,
        surface="api",
        operation_name=operation_name,
        status=str(status),
        error_code=error[:200] if error else None,
        duration_ms=round(duration_ms, 2),
        user_id=user_id,
        operation_id=operation_id,
        arguments_fingerprint=arguments_fingerprint,
        object_ids=object_ids(None, result) if status != "failed" else [],
      )
    )
    _executor.submit(write_record, record)
  except Exception:
    logger.error(
      "operation_mutation_audit.unrecorded",
      extra={"audit": {"operation_name": operation_name, "reason": "queue_failed"}},
      exc_info=True,
    )


def write_record(record: MutationRecord) -> None:
  """Insert the row in its own short session; never raises."""
  from robosystems.db.platform import SessionFactory
  from robosystems.models.core import Graph, OperationMutationAudit

  try:
    session = SessionFactory()
    try:
      org_id = (
        session.query(Graph.org_id).filter(Graph.graph_id == record.graph_id).scalar()
      )
      session.add(OperationMutationAudit(org_id=org_id, **asdict(record)))
      session.commit()
    finally:
      session.close()
  except Exception as exc:
    logger.error(
      "operation_mutation_audit.unrecorded",
      extra={"audit": {**asdict(record), "reason": type(exc).__name__}},
      exc_info=True,
    )
