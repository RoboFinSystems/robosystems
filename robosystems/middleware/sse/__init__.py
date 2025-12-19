"""
Server-Sent Events (SSE) middleware for unified operation monitoring.

This package provides infrastructure for real-time operation monitoring
using Server-Sent Events with Redis-based event persistence.

Key Components:
- EventStorage: Redis-based event persistence with TTL
- Streaming: SSE endpoint and connection management
- OperationManager: High-level operation lifecycle management

Usage Example:
    # In an endpoint
    from robosystems.middleware.sse import create_operation_response, get_operation_manager

    # Start an operation
    response = await create_operation_response("graph_creation", user.id, graph_id)

    # In a worker/task
    manager = get_operation_manager()
    async with manager.operation_context("graph_creation", user.id) as op_id:
        await manager.emit_progress(op_id, "Creating nodes...")
        # ... do work
        # Operation is automatically completed/failed
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
  # Dagster Integration
  "DagsterRunMonitor",
  "EventType",
  # Operation Management
  "OperationManager",
  "OperationMetadata",
  "OperationStatus",
  # Streaming
  "SSEConnectionManager",
  "SSEEvent",
  # Event Storage
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
