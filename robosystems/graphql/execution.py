"""Execution-time guards for the extensions GraphQL surface.

Two Strawberry schema extensions, both concerned with what one tenant's query
can do to everyone else's — not with what it can read (that is `auth.py`).

`OffloadSyncResolvers` — every resolver in `resolvers/` is a plain ``def``
that opens an extensions OLTP session, and Strawberry executes a sync
resolver inline on the event loop. The API runs one uvicorn worker, so an
OLTP statement running inside a resolver stalled every request on the task
for its duration. Resolvers now run in the runner thread pool through
`run_off_loop` — the same limiter the REST operations and MCP tools use, so
GraphQL reads share the pool-sized ceiling instead of sitting outside it.
Only user-written sync resolvers are offloaded; the default attribute
resolvers Strawberry generates for plain fields stay inline (a thread hop
per scalar field would cost more than it saves), as does introspection.

`MaskUnexpectedErrors` — a resolver exception that is not a deliberate
GraphQL error reached the client as ``str(exc)``: for a database fault that
string names tables, constraints, and schemas. Everything that is not a
parse/validation error or a `GraphQLError` the code raised on purpose is
replaced with a fixed message; a cancelled statement (the session's
``statement_timeout``) gets its own code so a client can narrow or retry.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Iterator
from functools import partial
from typing import Any

from graphql import GraphQLError, GraphQLResolveInfo
from strawberry.extensions import SchemaExtension
from strawberry.extensions.utils import is_introspection_field
from strawberry.schema.schema_converter import GraphQLCoreConverter

from robosystems.db.extensions import is_statement_timeout
from robosystems.middleware.extensions import STATEMENT_TIMEOUT_DETAIL

INTERNAL_ERROR_MESSAGE = "Internal error"
INTERNAL_ERROR_CODE = "INTERNAL_ERROR"
STATEMENT_TIMEOUT_CODE = "STATEMENT_TIMEOUT"


def _on_event_loop() -> bool:
  try:
    asyncio.get_running_loop()
  except RuntimeError:
    return False
  return True


def _is_user_sync_resolver(info: GraphQLResolveInfo) -> bool:
  """Whether the field being resolved has a resolver someone wrote (as
  opposed to Strawberry's default attribute getter) and it is synchronous."""
  field = info.parent_type.fields.get(info.field_name)
  if field is None:
    return False
  definition = field.extensions.get(GraphQLCoreConverter.DEFINITION_BACKREF)
  resolver = getattr(definition, "base_resolver", None)
  if resolver is None:
    return False
  return not resolver.is_async


class OffloadSyncResolvers(SchemaExtension):
  """Run user-written sync resolvers in the runner thread pool.

  Skipped when there is no running event loop — `schema.execute_sync` from a
  worker thread (the MCP GraphQL tool's introspection, the unit tests) has
  nothing to protect and cannot await.
  """

  def resolve(
    self,
    _next: Callable[..., Any],
    root: Any,
    info: GraphQLResolveInfo,
    *args: Any,
    **kwargs: Any,
  ) -> Any:
    if (
      not _on_event_loop()
      or is_introspection_field(info)
      or not _is_user_sync_resolver(info)
    ):
      return _next(root, info, *args, **kwargs)
    # Local import: `middleware.operations` pulls in the platform database
    # module; the schema must stay importable without one.
    from robosystems.middleware.operations import run_off_loop

    return run_off_loop(partial(_next, root, info, *args, **kwargs))


def _request_id(context: Any) -> str | None:
  request = None
  if isinstance(context, dict):
    request = context.get("request")
  else:
    request = getattr(context, "request", None)
  state = getattr(request, "state", None)
  return getattr(state, "request_id", None)


def _is_deliberate(error: GraphQLError) -> bool:
  """Parse/validation errors (no original exception) and any `GraphQLError`
  the code raised on purpose keep their message and extensions."""
  original = error.original_error
  return original is None or isinstance(original, GraphQLError)


class MaskUnexpectedErrors(SchemaExtension):
  """Replace unintended resolver exceptions with a fixed message.

  Deliberate errors — `StrawberryGraphQLError` with a ``code`` — pass through
  untouched, so the documented codes (``LEDGER_NOT_INITIALIZED``,
  ``UNAUTHENTICATED``, ``EXTENSION_NOT_PROVISIONED``, ...) still reach the
  client. Everything else is masked; the request id rides in ``extensions``
  so a report can be matched to the server-side log line, which Strawberry's
  own `process_errors` already writes with the traceback.
  """

  def _rewrite(self, error: GraphQLError, request_id: str | None) -> GraphQLError:
    original = error.original_error
    if original is not None and is_statement_timeout(original):
      message = STATEMENT_TIMEOUT_DETAIL
      code = STATEMENT_TIMEOUT_CODE
    else:
      message = INTERNAL_ERROR_MESSAGE
      code = INTERNAL_ERROR_CODE
    extensions: dict[str, Any] = {"code": code}
    if request_id:
      extensions["requestId"] = request_id
    return GraphQLError(
      message=message,
      nodes=error.nodes,
      source=error.source,
      positions=error.positions,
      path=error.path,
      original_error=None,
      extensions=extensions,
    )

  def _process_result(self, result: Any) -> None:
    errors = getattr(result, "errors", None)
    if not errors:
      return
    request_id = _request_id(self.execution_context.context)
    result.errors = [
      error if _is_deliberate(error) else self._rewrite(error, request_id)
      for error in errors
    ]

  def on_operation(self) -> Iterator[None]:
    yield
    result = self.execution_context.result
    if result is None:
      return
    if hasattr(result, "errors"):
      self._process_result(result)
    elif hasattr(result, "initial_result"):
      self._process_result(result.initial_result)


__all__ = [
  "INTERNAL_ERROR_CODE",
  "INTERNAL_ERROR_MESSAGE",
  "STATEMENT_TIMEOUT_CODE",
  "MaskUnexpectedErrors",
  "OffloadSyncResolvers",
]
