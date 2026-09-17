"""Bank-feed obligations every feed provider keeps, whatever the source.

A bank partnership or aggregator agreement asks for the same two records:
who granted access to which institution, over which credential and scope,
when (the consent record, written at connect); and that disconnect deletes
what the feed captured (``purge_bank_feed`` on the tenant schema, logged).
Both land in the security audit log, stamped with the provider.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from ...logger import logger


def record_bank_feed_consent(
  *,
  provider: str,
  environment: str,
  graph_id: str,
  connection_id: str,
  user_id: str,
  auth_mode: str,
  scope: str | None,
  organization: str | None = None,
  institution: str | None = None,
) -> None:
  from ...security.audit_logger import SecurityAuditLogger, SecurityEventType

  SecurityAuditLogger.log_security_event(
    event_type=SecurityEventType.BANK_FEED_CONSENT_GRANTED,
    user_id=user_id,
    endpoint=f"/v1/graphs/{{graph_id}}/connections/oauth/callback/{provider}",
    details={
      "graph_id": graph_id,
      "connection_id": connection_id,
      "provider": provider,
      "auth_mode": auth_mode,
      "scope": scope,
      "organization": organization,
      "institution": institution,
      "environment": environment,
      "granted_at": datetime.now(UTC).isoformat(),
    },
    risk_level="low",
  )


def purge_bank_feed_connection(
  graph_id: str, *, provider: str, connection_id: str
) -> dict[str, int]:
  """Run ``purge_bank_feed`` on the graph's tenant schema and commit.

  A graph never provisioned, or already torn down, has nothing to purge. Any
  other failure surfaces: the purge is the deletion the agreement promises,
  so a disconnect that cannot purge is retried rather than leaving feed data
  behind.
  """
  from sqlalchemy.exc import ProgrammingError

  from ...db.extensions import extensions_session
  from ...middleware.extensions import is_schema_missing
  from ...operations.roboledger.commands.connections import purge_bank_feed

  try:
    with extensions_session(graph_id, statement_timeout_ms=None) as ext:
      purged = purge_bank_feed(ext, source=provider, connection_id=connection_id)
      ext.commit()
  except ProgrammingError as exc:
    if is_schema_missing(exc):
      return {"events_deleted": 0, "events_scrubbed": 0, "agents_deleted": 0}
    raise
  logger.info(
    "Purged %s feed on graph %s for connection %s: %s",
    provider,
    graph_id,
    connection_id,
    purged,
  )
  return purged


def record_bank_feed_purged(
  *,
  provider: str,
  connection: dict[str, Any],
  graph_id: str,
  connection_id: str,
  auth_mode: str,
  purged: dict[str, int],
) -> None:
  from ...security.audit_logger import SecurityAuditLogger, SecurityEventType

  SecurityAuditLogger.log_security_event(
    event_type=SecurityEventType.BANK_FEED_PURGED,
    user_id=str(connection.get("user_id") or ""),
    endpoint="/v1/graphs/{graph_id}/connections/{connection_id}",
    details={
      "graph_id": graph_id,
      "connection_id": connection_id,
      "provider": provider,
      "auth_mode": auth_mode,
      **purged,
    },
    risk_level="low",
  )
