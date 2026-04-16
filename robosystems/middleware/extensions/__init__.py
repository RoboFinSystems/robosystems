"""Extensions middleware — operation dispatch + registrar.

This package holds the plumbing every `/extensions/{domain}/{graph_id}/
operations/{op_name}` endpoint shares:

- **`core`** — the operation dispatch pipeline: envelope, context,
  idempotency cache, audit logging, and `execute_operation`. These
  concerns are tightly coupled (every dispatch flows through all of
  them) and live together in one module.
- **`registry`** — the declarative `OperationSpec` + `OperationRegistrar`
  factory that mounts `core`'s dispatch pipeline as FastAPI routes.
  The same `OperationSpec` is designed to also drive future MCP tool
  and agent tool adapters — one descriptor, three surfaces.

Callers should import from this package's top level — the submodule
layout is an implementation detail that may shift as the surface grows.
"""

from robosystems.middleware.extensions.core import (
  IDEMPOTENCY_TTL_SECONDS,
  IdempotencyCache,
  IdempotencyKeyConflictError,
  OperationContext,
  OperationEnvelope,
  compute_idempotency_cache_key,
  execute_operation,
  fingerprint_body,
  generate_operation_id,
  get_idempotency_cache,
  log_operation_audit,
  wrap_completed,
  wrap_failed,
  wrap_pending,
)
from robosystems.middleware.extensions.registry import (
  ErrorDetailFactory,
  ErrorMap,
  ErrorMapEntry,
  OperationRegistrar,
  OperationSpec,
)

__all__ = [
  # Dispatch pipeline (core)
  "IDEMPOTENCY_TTL_SECONDS",
  # Declarative operation registrar (registry)
  "ErrorDetailFactory",
  "ErrorMap",
  "ErrorMapEntry",
  "IdempotencyCache",
  "IdempotencyKeyConflictError",
  "OperationContext",
  "OperationEnvelope",
  "OperationRegistrar",
  "OperationSpec",
  "compute_idempotency_cache_key",
  "execute_operation",
  "fingerprint_body",
  "generate_operation_id",
  "get_idempotency_cache",
  "log_operation_audit",
  "wrap_completed",
  "wrap_failed",
  "wrap_pending",
]
