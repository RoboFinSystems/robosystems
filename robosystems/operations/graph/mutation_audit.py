"""Reads over operation_mutation_audit for one graph, newest first."""

from __future__ import annotations

import base64
from datetime import datetime

from sqlalchemy import and_, or_
from sqlalchemy.orm import Session

from robosystems.models.api.graphs.audit import (
  MutationAuditEntry,
  MutationAuditListResponse,
)
from robosystems.models.core import OperationMutationAudit


class InvalidCursorError(ValueError):
  """The cursor was not one this endpoint issued."""


def _encode_cursor(row: OperationMutationAudit) -> str:
  raw = f"{row.occurred_at.isoformat()}|{row.id}"
  return base64.urlsafe_b64encode(raw.encode("utf-8")).decode("ascii")


def _decode_cursor(cursor: str) -> tuple[datetime, str]:
  try:
    raw = base64.urlsafe_b64decode(cursor.encode("ascii")).decode("utf-8")
    occurred_at, row_id = raw.split("|", 1)
    return datetime.fromisoformat(occurred_at), row_id
  except (ValueError, UnicodeDecodeError) as exc:
    raise InvalidCursorError("Invalid cursor") from exc


def list_mutation_audit(
  session: Session,
  graph_id: str,
  *,
  surface: str | None = None,
  operation_name: str | None = None,
  user_id: str | None = None,
  operation_id: str | None = None,
  since: datetime | None = None,
  until: datetime | None = None,
  cursor: str | None = None,
  limit: int = 50,
) -> MutationAuditListResponse:
  """One page of the graph's mutation audit, newest first, keyset-paged on
  (occurred_at, id) so a page never skips or repeats a row as new ones land."""
  query = session.query(OperationMutationAudit).filter(
    OperationMutationAudit.graph_id == graph_id
  )
  if surface:
    query = query.filter(OperationMutationAudit.surface == surface)
  if operation_name:
    query = query.filter(OperationMutationAudit.operation_name == operation_name)
  if user_id:
    query = query.filter(OperationMutationAudit.user_id == user_id)
  if operation_id:
    query = query.filter(OperationMutationAudit.operation_id == operation_id)
  if since:
    query = query.filter(OperationMutationAudit.occurred_at >= since)
  if until:
    query = query.filter(OperationMutationAudit.occurred_at < until)
  if cursor:
    at, row_id = _decode_cursor(cursor)
    query = query.filter(
      or_(
        OperationMutationAudit.occurred_at < at,
        and_(
          OperationMutationAudit.occurred_at == at,
          OperationMutationAudit.id < row_id,
        ),
      )
    )

  rows = (
    query.order_by(
      OperationMutationAudit.occurred_at.desc(), OperationMutationAudit.id.desc()
    )
    .limit(limit + 1)
    .all()
  )
  page, more = rows[:limit], len(rows) > limit
  return MutationAuditListResponse(
    graph_id=graph_id,
    entries=[
      MutationAuditEntry(
        id=row.id,
        occurred_at=row.occurred_at,
        surface=row.surface,
        operation_name=row.operation_name,
        status=row.status,
        error_code=row.error_code,
        duration_ms=row.duration_ms,
        user_id=row.user_id,
        auth_method=row.auth_method,
        api_key_prefix=row.api_key_prefix,
        request_id=row.request_id,
        operation_id=row.operation_id,
        operator_type=row.operator_type,
        arguments_fingerprint=row.arguments_fingerprint,
        object_ids=list(row.object_ids or []),
      )
      for row in page
    ],
    next_cursor=_encode_cursor(page[-1]) if more and page else None,
  )
