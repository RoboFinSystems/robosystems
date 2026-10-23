"""Reads over operator_runs for one graph: the runs, the threads they form,
and the history a threaded run is seeded with."""

from __future__ import annotations

import base64
from datetime import datetime

from sqlalchemy import and_, func, or_
from sqlalchemy.orm import Session

from robosystems.models.api.graphs.audit import (
  OperatorRunDetail,
  OperatorRunListResponse,
  OperatorRunSummary,
  OperatorThreadListResponse,
  OperatorThreadSummary,
)
from robosystems.models.core import OperatorRun
from robosystems.operations.graph.mutation_audit import InvalidCursorError

# What a threaded run is seeded with: its thread's last exchanges, the
# answers clipped so history stays cheap. The same bound the console used
# while it held the history itself.
THREAD_HISTORY_EXCHANGES = 3
THREAD_HISTORY_ANSWER_CHARS = 2000
_TITLE_CHARS = 120


def _encode_cursor(at: datetime, key: str) -> str:
  raw = f"{at.isoformat()}|{key}"
  return base64.urlsafe_b64encode(raw.encode("utf-8")).decode("ascii")


def _decode_cursor(cursor: str) -> tuple[datetime, str]:
  try:
    raw = base64.urlsafe_b64decode(cursor.encode("ascii")).decode("utf-8")
    at, key = raw.split("|", 1)
    return datetime.fromisoformat(at), key
  except (ValueError, UnicodeDecodeError) as exc:
    raise InvalidCursorError("Invalid cursor") from exc


def thread_history(
  session: Session, graph_id: str, user_id: str, thread_id: str
) -> list[dict[str, str]]:
  """The thread's last completed exchanges, oldest first, as the role/content
  pairs an operator is seeded with. Scoped to the graph and the user, so a
  guessed thread id yields nothing."""
  rows = (
    session.query(OperatorRun)
    .filter(
      OperatorRun.graph_id == graph_id,
      OperatorRun.user_id == user_id,
      OperatorRun.thread_id == thread_id,
      OperatorRun.status == "completed",
      OperatorRun.answer.isnot(None),
    )
    .order_by(OperatorRun.started_at.desc(), OperatorRun.operation_id.desc())
    .limit(THREAD_HISTORY_EXCHANGES)
    .all()
  )
  history: list[dict[str, str]] = []
  for row in reversed(rows):
    history.append({"role": "user", "content": row.question})
    history.append(
      {"role": "assistant", "content": (row.answer or "")[:THREAD_HISTORY_ANSWER_CHARS]}
    )
  return history


def _summary(row: OperatorRun) -> OperatorRunSummary:
  result = row.result or {}
  return OperatorRunSummary(
    operation_id=row.operation_id,
    thread_id=row.thread_id,
    user_id=row.user_id,
    operator_type=row.operator_type,
    mode=row.mode,
    status=row.status,
    question=row.question,
    answer=row.answer,
    error=row.error,
    credits_consumed=float(result.get("credits_consumed") or 0.0),
    call_count=int(result.get("call_count") or 0),
    tools_called=list(result.get("tools_called") or []),
    write_count=len(result.get("writes") or []),
    started_at=row.started_at,
    finished_at=row.finished_at,
    duration_ms=row.duration_ms,
  )


def list_operator_runs(
  session: Session,
  graph_id: str,
  *,
  thread_id: str | None = None,
  user_id: str | None = None,
  operator_type: str | None = None,
  status: str | None = None,
  since: datetime | None = None,
  until: datetime | None = None,
  cursor: str | None = None,
  limit: int = 50,
) -> OperatorRunListResponse:
  """One page of the graph's runs, newest first, keyset-paged on
  (started_at, operation_id)."""
  query = session.query(OperatorRun).filter(OperatorRun.graph_id == graph_id)
  if thread_id:
    query = query.filter(OperatorRun.thread_id == thread_id)
  if user_id:
    query = query.filter(OperatorRun.user_id == user_id)
  if operator_type:
    query = query.filter(OperatorRun.operator_type == operator_type)
  if status:
    query = query.filter(OperatorRun.status == status)
  if since:
    query = query.filter(OperatorRun.started_at >= since)
  if until:
    query = query.filter(OperatorRun.started_at < until)
  if cursor:
    at, key = _decode_cursor(cursor)
    query = query.filter(
      or_(
        OperatorRun.started_at < at,
        and_(OperatorRun.started_at == at, OperatorRun.operation_id < key),
      )
    )

  rows = (
    query.order_by(OperatorRun.started_at.desc(), OperatorRun.operation_id.desc())
    .limit(limit + 1)
    .all()
  )
  page, more = rows[:limit], len(rows) > limit
  return OperatorRunListResponse(
    graph_id=graph_id,
    runs=[_summary(row) for row in page],
    next_cursor=(
      _encode_cursor(page[-1].started_at, page[-1].operation_id)
      if more and page
      else None
    ),
  )


def get_operator_run(
  session: Session, graph_id: str, operation_id: str
) -> OperatorRunDetail | None:
  row = (
    session.query(OperatorRun)
    .filter(OperatorRun.graph_id == graph_id, OperatorRun.operation_id == operation_id)
    .one_or_none()
  )
  if row is None:
    return None
  return OperatorRunDetail(
    **_summary(row).model_dump(),
    history=list(row.history or []),
    transcript=list(row.transcript or []),
    result=dict(row.result or {}),
  )


def list_operator_threads(
  session: Session,
  graph_id: str,
  *,
  user_id: str | None = None,
  cursor: str | None = None,
  limit: int = 50,
) -> OperatorThreadListResponse:
  """One page of the graph's threads, most recently active first, keyset-paged
  on (last_run_at, thread_id). Runs made without a thread id are not threads."""
  last_run_at = func.max(OperatorRun.started_at).label("last_run_at")
  grouped = (
    session.query(
      OperatorRun.thread_id.label("thread_id"),
      OperatorRun.user_id.label("user_id"),
      func.count(OperatorRun.operation_id).label("run_count"),
      func.min(OperatorRun.started_at).label("first_run_at"),
      last_run_at,
    )
    .filter(OperatorRun.graph_id == graph_id, OperatorRun.thread_id.isnot(None))
    .group_by(OperatorRun.thread_id, OperatorRun.user_id)
  )
  if user_id:
    grouped = grouped.filter(OperatorRun.user_id == user_id)
  if cursor:
    at, key = _decode_cursor(cursor)
    grouped = grouped.having(
      or_(last_run_at < at, and_(last_run_at == at, OperatorRun.thread_id < key))
    )
  groups = (
    grouped.order_by(last_run_at.desc(), OperatorRun.thread_id.desc())
    .limit(limit + 1)
    .all()
  )
  page, more = groups[:limit], len(groups) > limit
  if not page:
    return OperatorThreadListResponse(graph_id=graph_id, threads=[], next_cursor=None)

  thread_ids = [g.thread_id for g in page]
  # The first run names the thread; the latest says where it stands.
  first_rank = (
    func.row_number()
    .over(
      partition_by=OperatorRun.thread_id,
      order_by=(OperatorRun.started_at.asc(), OperatorRun.operation_id.asc()),
    )
    .label("first_rank")
  )
  last_rank = (
    func.row_number()
    .over(
      partition_by=OperatorRun.thread_id,
      order_by=(OperatorRun.started_at.desc(), OperatorRun.operation_id.desc()),
    )
    .label("last_rank")
  )
  ranked = (
    session.query(OperatorRun, first_rank, last_rank)
    .filter(OperatorRun.graph_id == graph_id, OperatorRun.thread_id.in_(thread_ids))
    .subquery()
  )
  ends = (
    session.query(ranked)
    .filter(or_(ranked.c.first_rank == 1, ranked.c.last_rank == 1))
    .all()
  )
  firsts = {row.thread_id: row for row in ends if row.first_rank == 1}
  lasts = {row.thread_id: row for row in ends if row.last_rank == 1}

  threads = []
  for group in page:
    first, last = firsts[group.thread_id], lasts[group.thread_id]
    threads.append(
      OperatorThreadSummary(
        thread_id=group.thread_id,
        user_id=group.user_id,
        title=first.question[:_TITLE_CHARS],
        run_count=group.run_count,
        first_run_at=group.first_run_at,
        last_run_at=group.last_run_at,
        last_operator_type=last.operator_type,
        last_status=last.status,
      )
    )
  return OperatorThreadListResponse(
    graph_id=graph_id,
    threads=threads,
    next_cursor=(
      _encode_cursor(page[-1].last_run_at, page[-1].thread_id)
      if more and page
      else None
    ),
  )
