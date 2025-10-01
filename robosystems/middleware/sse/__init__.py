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

from .event_storage import (
  SSEEventStorage,
  SSEEvent,
  OperationMetadata,
  EventType,
  OperationStatus,
  get_event_storage,
)

from .streaming import (
  SSEConnectionManager,
  get_connection_manager,
  create_sse_stream_starlette,
  emit_event_to_operation,
  create_sse_response_starlette,
)

from .operation_manager import (
  OperationManager,
  get_operation_manager,
  create_operation_response,
)

__all__ = [
  # Event Storage
  "SSEEventStorage",
  "SSEEvent",
  "OperationMetadata",
  "EventType",
  "OperationStatus",
  "get_event_storage",
  # Streaming
  "SSEConnectionManager",
  "get_connection_manager",
  "create_sse_stream_starlette",
  "emit_event_to_operation",
  "create_sse_response_starlette",
  # Operation Management
  "OperationManager",
  "get_operation_manager",
  "create_operation_response",
]
