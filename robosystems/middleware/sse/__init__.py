"""Server-Sent Events middleware for operation monitoring.

- `event_storage` — Redis event persistence with TTL and replay
- `streaming` — SSE stream generator and connection management
- `operation_manager` — operation lifecycle helpers
- `redis_subscriber` — relays worker-emitted events into the API process

Endpoints queue work and hand back the operation's stream link:

    response = await create_operation_response("graph_creation", user.id, graph_id)

The worker then reports progress; the context manager completes or fails
the operation on exit:

    manager = get_operation_manager()
    async with manager.operation_context("graph_creation", user.id) as op_id:
        await manager.emit_progress(op_id, "Creating nodes...")
"""

from robosystems.dagster.jobs.notifications import build_email_job_config

from .dagster_monitor import (
  DagsterRunMonitor,
  build_graph_job_config,
  run_and_monitor_dagster_job,
  submit_dagster_job_sync,
)
from .event_storage import (
  EventType,
  OperationMetadata,
  OperationStatus,
  SSEEvent,
  SSEEventStorage,
  get_event_storage,
)
from .operation_manager import (
  OperationManager,
  create_operation_response,
  get_operation_manager,
)
from .streaming import (
  SSEConnectionManager,
  create_sse_response_starlette,
  create_sse_stream_starlette,
  emit_event_to_operation,
  get_connection_manager,
)

__all__ = [
  "DagsterRunMonitor",
  "EventType",
  "OperationManager",
  "OperationMetadata",
  "OperationStatus",
  "SSEConnectionManager",
  "SSEEvent",
  "SSEEventStorage",
  "build_email_job_config",
  "build_graph_job_config",
  "create_operation_response",
  "create_sse_response_starlette",
  "create_sse_stream_starlette",
  "emit_event_to_operation",
  "get_connection_manager",
  "get_event_storage",
  "get_operation_manager",
  "run_and_monitor_dagster_job",
  "submit_dagster_job_sync",
]
