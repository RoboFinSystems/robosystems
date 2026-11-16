"""The durable record of an operator run, written at the end of every run
whatever its outcome — in its own session, off the event loop, never
raising into the run. `operation_mutation_audit` names a run by its
`operation_id`; this is where that id resolves to a question, a transcript
and an answer once the operation store has let the run expire. The system
prompts and tool sets the transcript names by hash land beside it in
`operator_run_contexts`, so the run can be rebuilt as the model saw it."""

from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from functools import cache
from importlib.metadata import PackageNotFoundError, version
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
  # sha256 -> {"kind": "system" | "tools", "content": ...}; see TrackedAIClient.
  contexts: dict[str, dict[str, Any]] = field(default_factory=dict)
  code_version: str | None = None


@cache
def code_version() -> str | None:
  """The installed release, which built the run's prompts and tools."""
  try:
    return version("robosystems")
  except PackageNotFoundError:
    return None


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


def _store_contexts(
  session: Any, graph_id: str, contexts: dict[str, dict[str, Any]]
) -> None:
  """Upsert the run's contexts under a savepoint. They support the record but
  are not the record: one that fails is logged, and the run row is kept with
  hashes that do not resolve, as every run before contexts were kept."""
  from sqlalchemy.dialects.postgresql import insert as pg_insert

  from robosystems.models.core import OperatorRunContext

  now = datetime.now(UTC)
  insert = pg_insert(OperatorRunContext.__table__).values(
    [
      {
        "graph_id": graph_id,
        "sha256": sha,
        "kind": context["kind"],
        "content": _json_safe(context["content"]),
        "first_seen_at": now,
        "last_seen_at": now,
      }
      for sha, context in sorted(contexts.items())
    ]
  )
  try:
    with session.begin_nested():
      session.execute(
        insert.on_conflict_do_update(
          index_elements=["graph_id", "sha256"],
          set_={"last_seen_at": insert.excluded.last_seen_at},
        )
      )
  except Exception as exc:
    logger.warning(
      "operator_run.contexts_unrecorded",
      extra={"run": {"graph_id": graph_id, "reason": type(exc).__name__}},
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
      contexts = values.pop("contexts")
      for key in ("history", "transcript", "result"):
        values[key] = _json_safe(values[key])
      for key in ("question", "answer", "error"):
        values[key] = _strip_nul(values[key])
      if values["error"]:
        values["error"] = values["error"][:_MAX_ERROR_CHARS]
      session.merge(OperatorRun(org_id=org_id, **values))
      if contexts:
        _store_contexts(session, record.graph_id, contexts)
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
