"""Shared-repository query telemetry.

Detection layer for the shared query surface: classifies bad query/tool
outcomes (timeouts, capacity rejections, rate limits, policy denials, engine
disruptions) and records them through the security audit log, which publishes
the CloudWatch metrics the detective-control alarms watch. Also emits
structured per-query cost lines (start/end pairs) so operators can rank users
by total execution time and reconstruct in-flight work around an incident.

Scope: shared repositories ONLY. User graphs run on dedicated instances, so
per-query telemetry of this kind never applies to them; every entry point
gates on the shared-repository check and no-ops elsewhere.

All functions are best-effort — they must never raise into the request path.
"""

import json
import uuid
from typing import Any

from fastapi import Request

from robosystems.logger import logger

# Outcome classes recorded per HTTP status. Statuses not listed are not
# telemetry-relevant on this surface (404s, billing 402s, generic 5xxs).
_SIGNAL_FOR_STATUS: dict[int, str] = {
  400: "statement_rejected",
  403: "access_denied",
  408: "timeout",
  429: "rate_limited",
  503: "capacity_rejected",
}

# Exception shapes that indicate the engine connection was lost mid-query
# (backend restart) rather than a normal error response. Matched by type name
# so no HTTP-client library needs importing here.
_DISRUPTION_TYPE_NAMES = frozenset(
  {
    "ConnectError",
    "ConnectionError",
    "ConnectionResetError",
    "ConnectionAbortedError",
    "ReadError",
    "RemoteProtocolError",
    "ServerDisconnectedError",
    "ClientConnectorError",
    "ClientOSError",
    "IncompleteReadError",
  }
)
_DISRUPTION_MESSAGE_MARKERS = (
  "connection reset",
  "connection refused",
  "connection closed",
  "connection aborted",
  "server disconnected",
  "peer closed connection",
)

# The client-visible envelope produced when an MCP event stream ends without
# any terminal event — see routers/graphs/mcp/streaming.py.
_AGGREGATION_FALLBACK_ERROR = "Unable to aggregate results"

# Greppable prefix for the per-query cost lines (CloudWatch Logs Insights:
# parse @message 'SHARED_QUERY_COST: *' as payload).
_COST_LINE_PREFIX = "SHARED_QUERY_COST"


def _is_shared(graph_id: str) -> bool:
  from robosystems.middleware.graph.utils import MultiTenantUtils

  return MultiTenantUtils.is_shared_repository_or_subgraph(graph_id)


def api_key_prefix_from_request(request: Request | None) -> str | None:
  """Return the identification prefix of the API key that authenticated
  this request, if one did (set by the auth dependencies)."""
  if request is None:
    return None
  return getattr(request.state, "api_key_prefix", None)


def signal_for_status(status_code: int | None) -> str | None:
  if status_code is None:
    return None
  return _SIGNAL_FOR_STATUS.get(status_code)


def is_engine_disruption(error: BaseException) -> bool:
  """True when an exception looks like the engine connection dropped
  mid-query rather than a normal error response."""
  if type(error).__name__ in _DISRUPTION_TYPE_NAMES:
    return True
  message = str(error).lower()
  return any(marker in message for marker in _DISRUPTION_MESSAGE_MARKERS)


def is_disrupted_aggregation(result: Any) -> bool:
  """True when an aggregated MCP stream ended without any terminal event —
  the client-visible shape of an engine connection lost mid-stream."""
  return (
    isinstance(result, dict)
    and result.get("success") is False
    and result.get("error") == _AGGREGATION_FALLBACK_ERROR
  )


def record_shared_query_outcome(
  graph_id: str,
  user_id: Any,
  *,
  signal: str | None = None,
  status_code: int | None = None,
  error: BaseException | None = None,
  api_key_prefix: str | None = None,
  endpoint: str | None = None,
  source: str = "query",
  tool_name: str | None = None,
  disruption: bool = False,
) -> None:
  """Record one bad query outcome on a shared repository (no-op elsewhere).

  Classification precedence: an explicit ``signal`` wins; otherwise
  ``status_code`` maps through the outcome table; otherwise ``error`` is
  checked for the engine-disruption shape. Unclassifiable outcomes are
  silently dropped — this records signals, not every failure.
  """
  try:
    if not _is_shared(graph_id):
      return
    if signal is None:
      signal = signal_for_status(status_code)
    if signal is None and error is not None and is_engine_disruption(error):
      signal = "engine_disruption"
      disruption = True
    if signal is None:
      return

    metadata: dict[str, Any] = {"source": source}
    if status_code is not None:
      metadata["status_code"] = status_code
    if tool_name:
      metadata["tool_name"] = tool_name

    from robosystems.security.audit_logger import SecurityAuditLogger

    SecurityAuditLogger.log_query_abuse_signal(
      user_id=str(user_id) if user_id else None,
      graph_id=graph_id,
      signal=signal,
      api_key_prefix=api_key_prefix,
      endpoint=endpoint,
      metadata=metadata,
      disruption=disruption,
    )
  except Exception as e:
    logger.debug(f"Failed to record shared query outcome: {e}")


def log_shared_query_start(
  graph_id: str,
  user_id: Any,
  *,
  api_key_prefix: str | None = None,
  source: str = "query",
  tool_name: str | None = None,
  query_length: int | None = None,
  strategy: str | None = None,
) -> str | None:
  """Emit the start half of a per-query cost line pair on a shared repository.

  Returns an execution id to pass to ``log_shared_query_end`` so the pair can
  be joined in log queries; returns None (and emits nothing) off the shared
  surface.
  """
  try:
    if not _is_shared(graph_id):
      return None
    exec_id = uuid.uuid4().hex[:12]
    _emit_cost_line(
      {
        "phase": "start",
        "exec_id": exec_id,
        "graph_id": graph_id,
        "user_id": str(user_id) if user_id else None,
        "api_key_prefix": api_key_prefix,
        "source": source,
        "tool_name": tool_name,
        "query_length": query_length,
        "strategy": strategy,
      }
    )
    return exec_id
  except Exception as e:
    logger.debug(f"Failed to log shared query start: {e}")
    return None


def log_shared_query_end(
  exec_id: str | None,
  graph_id: str,
  user_id: Any,
  *,
  outcome: str,
  duration_ms: float | None = None,
  row_count: int | None = None,
  api_key_prefix: str | None = None,
  source: str = "query",
  tool_name: str | None = None,
) -> None:
  """Emit the end half of a cost line pair. Safe to call with a None exec_id
  (emits nothing — the start half was off-surface or failed)."""
  try:
    if exec_id is None or not _is_shared(graph_id):
      return
    _emit_cost_line(
      {
        "phase": "end",
        "exec_id": exec_id,
        "graph_id": graph_id,
        "user_id": str(user_id) if user_id else None,
        "api_key_prefix": api_key_prefix,
        "source": source,
        "tool_name": tool_name,
        "outcome": outcome,
        "duration_ms": round(duration_ms, 1) if duration_ms is not None else None,
        "row_count": row_count,
      }
    )
  except Exception as e:
    logger.debug(f"Failed to log shared query end: {e}")


def _emit_cost_line(payload: dict[str, Any]) -> None:
  compact = {k: v for k, v in payload.items() if v is not None}
  logger.info(f"{_COST_LINE_PREFIX}: {json.dumps(compact, default=str)}")
