"""The durable record of an operator run, written at the end of every run
whatever its outcome — in its own session, off the event loop, never
raising into the run. `operation_mutation_audit` names a run by its
`operation_id`; this is where that id resolves to a question, a transcript
and an answer once the operation store has let the run expire."""

from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any

from robosystems.logger import logger

_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="operator-run")

# Result rows stay out of the record: the Cypher that produced them is kept
# and re-runs, and a row set can be thousands of rows.
_RESULT_KEYS_DROPPED = frozenset({"rows"})
_MAX_ERROR_CHARS = 2000


@dataclass
class OperatorRunRecord:
  operation_id: str
  graph_id: str
  user_id: str
  operator_type: str
  mode: str
  status: str  # completed | failed | cancelled
  question: str
  started_at: datetime
  finished_at: datetime
  duration_ms: float
  thread_id: str | None = None
  history: list[dict[str, Any]] = field(default_factory=list)
  transcript: list[dict[str, Any]] = field(default_factory=list)
  answer: str | None = None
  error: str | None = None
  result: dict[str, Any] = field(default_factory=dict)


def result_summary(metadata: dict[str, Any]) -> dict[str, Any]:
  """The envelope's metadata without its result rows."""
  return {k: v for k, v in metadata.items() if k not in _RESULT_KEYS_DROPPED}


def _strip_nul(value: Any) -> Any:
  """Postgres text and JSONB both reject NUL; a tool result carrying one must
  not cost the whole record."""
  if isinstance(value, str):
    return value.replace("\x00", "")
  if isinstance(value, list):
    return [_strip_nul(item) for item in value]
  if isinstance(value, dict):
    return {_strip_nul(key): _strip_nul(item) for key, item in value.items()}
  return value


def _json_safe(value: Any) -> Any:
  return _strip_nul(json.loads(json.dumps(value, default=str)))


def record_operator_run(record: OperatorRunRecord) -> None:
  """Queue the row. Never raises: a run that cannot be recorded is logged,
  not failed."""
  try:
    _executor.submit(write_run_record, record)
  except Exception:
    logger.error(
      "operator_run.unrecorded",
      extra={"run": {"operation_id": record.operation_id, "reason": "queue_failed"}},
      exc_info=True,
    )


def write_run_record(record: OperatorRunRecord) -> None:
  """Upsert the row in its own short session; never raises. An upsert
  because a task re-run or resumed under its operation id comes back under
  the same id, and the attempt that finished is the record."""
  from robosystems.db.platform import SessionFactory
  from robosystems.models.core import Graph, OperatorRun

  try:
    session = SessionFactory()
    try:
      org_id = (
        session.query(Graph.org_id).filter(Graph.graph_id == record.graph_id).scalar()
      )
      values = asdict(record)
      for key in ("history", "transcript", "result"):
        values[key] = _json_safe(values[key])
      for key in ("question", "answer", "error"):
        values[key] = _strip_nul(values[key])
      if values["error"]:
        values["error"] = values["error"][:_MAX_ERROR_CHARS]
      session.merge(OperatorRun(org_id=org_id, **values))
      session.commit()
    finally:
      session.close()
  except Exception as exc:
    logger.error(
      "operator_run.unrecorded",
      extra={
        "run": {
          "operation_id": record.operation_id,
          "graph_id": record.graph_id,
          "reason": type(exc).__name__,
        }
      },
      exc_info=True,
    )
