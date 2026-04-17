"""Declarative operation registrar for extensions endpoints.

One `OperationSpec` describes a write operation; one `OperationRegistrar`
mounts it as a FastAPI POST handler. The descriptor is adapter-neutral —
the same `OperationSpec` is designed to also drive:

- **REST routes** via `OperationRegistrar.register` (implemented here)
- **MCP tools** via a future `MCPRegistrar` (the spec's `command`,
  `request_model`, and `description` fields give MCP everything it needs)
- **Agent tools** via a future `AgentToolRegistrar` (same reasoning)

This is the scaling surface for capabilities: a new op is **one
`OperationSpec` + one command function + one Pydantic request model**,
and all three adapter surfaces light up from the same declaration.

The factory replaces ~50 lines of per-route boilerplate (route decorator
+ metrics decorator + context builder + runner closure + error
translation + dispatcher) with a single declarative call. It is NOT a
wholesale router replacement — operations with unusual needs (async
Dagster dispatch, platform-DB dependencies, bespoke pre-validation
beyond a simple hook) should still use hand-written `@router.post`
handlers.

Migration policy: new operations should prefer the factory. Existing
hand-written routes can stay hand-written; migrating them is a
cosmetic cleanup, not a correctness requirement.
"""

import sys
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, ClassVar

from fastapi import APIRouter, Depends, Header, HTTPException, Path
from pydantic import BaseModel
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.orm import Session

from robosystems.database import get_db_session
from robosystems.middleware.graph.types import GRAPH_OR_SUBGRAPH_ID_PATTERN
from robosystems.middleware.operations import (
  IdempotencyCache,
  OperationContext,
  OperationEnvelope,
  get_idempotency_cache,
)
from robosystems.middleware.otel.metrics import endpoint_metrics_decorator
from robosystems.models.core import Graph
from robosystems.operations.extensions.staleness import mark_graph_stale

# ── Error-map types ──────────────────────────────────────────────────────

# Detail factory: given an exception, return the HTTP detail string. Used
# when the status code is fixed but the detail needs fields from the
# exception instance (e.g., `f"Element not found: {e.element_id}"`).
ErrorDetailFactory = Callable[[Exception], str]

# One entry in an error map: either a bare int (status code; detail comes
# from `str(exc)`) or a (status_code, detail_factory) tuple.
ErrorMapEntry = int | tuple[int, ErrorDetailFactory]

# Full error map: exception class → translation. Ordering within the map
# matters: the registrar iterates in insertion order and matches with
# `isinstance`, so subclass entries should come before superclass
# entries (Python dicts preserve insertion order).
ErrorMap = dict[type[Exception], ErrorMapEntry]


# ── OperationSpec ────────────────────────────────────────────────────────


@dataclass
class OperationSpec:
  """Declarative description of a single extensions operation.

  Instantiate one of these per operation and hand it to
  `OperationRegistrar.register` to mount the REST route.

  Required fields:
    name: kebab-case operation name (e.g., "create-element"). Used for
      the URL path, OpenAPI operationId, audit log, and metrics label.
    summary: short human-readable title for OpenAPI + MCP tool lists.
    command: the pure-function command to invoke. Signature:
      `(session, body, /, **kwargs) -> Response`. The registrar passes
      `created_by=str(user.id)` by default; set
      `requires_created_by=False` to suppress that kwarg.
    request_model: Pydantic request model type. The registrar hoists
      this into the FastAPI handler's signature so request validation
      happens at the API boundary.
    error_map: mapping of exception classes raised by the command to
      HTTP translations. Each value is either a bare status code (uses
      `str(exc)` as detail) or a `(status, detail_factory)` tuple.

  Optional fields:
    description: longer OpenAPI description (defaults to the command's
      docstring if empty).
    path: URL path override (defaults to `/{name}`).
    business_event_type: metrics event key (defaults to
      `{domain}_{snake_name}`, where `domain` is supplied by the
      `OperationRegistrar` at mount time).
    requires_created_by: pass `created_by=str(user.id)` to the command.
      True by default.
    pre_validate: optional sync validator called before the command.
      Receives the parsed body; may raise `HTTPException` to abort
      with a 4xx response. Use for parse/format checks that don't
      require a DB session (e.g., `parse_period(body.period)`).
    on_fresh_success: optional callback invoked on a non-replayed
      success from `execute_operation`. Common use is
      `lambda _env: mark_graph_stale(graph_id, "<reason>")`. Signature
      takes the envelope. For the common "mark stale after success"
      pattern, prefer the declarative `mark_stale_reason` field — it
      avoids capturing `graph_id` from an outer closure (which a
      module-level spec can't do) and the registrar wires it up against
      the request's graph_id automatically.
    mark_stale_reason: when set, the registrar invokes
      `mark_graph_stale(graph_id, mark_stale_reason)` on fresh success.
      Mutually exclusive with `on_fresh_success` (if both are set,
      `on_fresh_success` wins).
  """

  name: str
  summary: str
  command: Callable
  request_model: type[BaseModel]
  error_map: ErrorMap = field(default_factory=dict)
  description: str | None = None
  path: str | None = None
  business_event_type: str | None = None
  requires_created_by: bool = True
  pre_validate: Callable[[BaseModel], None] | None = None
  on_fresh_success: Callable | None = None
  mark_stale_reason: str | None = None

  @property
  def resolved_path(self) -> str:
    return self.path or f"/{self.name}"

  def resolve_business_event_type(self, domain: str) -> str:
    """Metrics event key for this op.

    If the spec set `business_event_type` explicitly, that wins.
    Otherwise default to `{domain}_{snake_name}` — the registrar
    provides the domain, so adding a new `OperationRegistrar` for a
    different domain (e.g., roboinvestor) gets the right prefix
    automatically instead of silently emitting `ledger_*` events.
    """
    if self.business_event_type:
      return self.business_event_type
    snake = self.name.replace("-", "_")
    return f"{domain}_{snake}"

  @property
  def openapi_operation_id(self) -> str:
    """OpenAPI operationId in camelCase — `op` + CamelCase(name)."""
    parts = self.name.split("-")
    return "op" + "".join(p.capitalize() for p in parts)


# ── Extension feature gate ───────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class GraphExtensionContext:
  """Per-request snapshot of the graph fields the extension gate cares about.

  Returned by `require_graph_extension(...)` so callers that already need
  the graph type or extension list don't reload the row.
  """

  graph_type: str
  schema_extensions: tuple[str, ...]
  is_repository: bool


def load_graph_metadata(graph_id: str, session: Session) -> GraphExtensionContext:
  """Load the Graph row and return only the fields relevant to the gate.

  Raises a 403 (not 404) on miss to match `check_graph_access`'s
  enumeration-safe posture — don't let an unauthenticated probe
  distinguish "graph doesn't exist" from "you lack access."
  """
  graph = Graph.get_by_id(graph_id, session)
  if graph is None:
    raise HTTPException(
      status_code=403,
      detail=f"Access denied to graph: {graph_id}",
    )
  return GraphExtensionContext(
    graph_type=graph.graph_type or "",
    schema_extensions=tuple(graph.schema_extensions or []),
    is_repository=bool(graph.is_repository),
  )


def require_graph_extension(extension: str) -> Callable[..., GraphExtensionContext]:
  """FastAPI dependency factory for extensions **command** endpoints.

  Assumes `get_current_user_with_graph` has already run and validated
  user + graph access. This dependency adds the feature-level check:

  - Repository graphs (shared repos like SEC) are rejected outright.
    Command writes never land in a shared tenant schema; ingestion
    pipelines are the only legitimate write path into those.
  - Graphs whose `schema_extensions` doesn't list `extension` are
    rejected with an explicit 403 instead of falling through to the
    DB layer and surfacing a confusing "schema missing" 404.

  Returns a `GraphExtensionContext` so hand-written handlers that need
  the metadata don't pay for a second DB lookup.
  """

  def _dep(
    graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
    session: Session = Depends(get_db_session),
  ) -> GraphExtensionContext:
    meta = load_graph_metadata(graph_id, session)
    if meta.is_repository or meta.graph_type == "repository":
      raise HTTPException(
        status_code=403,
        detail=f"{extension} commands are not available on repository graphs",
      )
    if extension not in meta.schema_extensions:
      raise HTTPException(
        status_code=403,
        detail=f"{extension} is not provisioned for this graph",
      )
    return meta

  return _dep


# ── Registrar ────────────────────────────────────────────────────────────


class OperationRegistrar:
  """Binds domain-specific plumbing and mounts `OperationSpec`s as
  FastAPI routes.

  Create one per-domain at router setup time, passing the domain's
  context builder, dispatcher, session factory, and schema-missing
  404 helper. Then call `.register(spec)` for each operation. The
  registrar takes care of the decorator chain, the runner closure,
  and the error-translation tree.

  Example:
      _registrar = OperationRegistrar(
          router=router,
          domain="roboledger",
          tag=_OP_TAG,
          rate_limit_dep=_RATE_LIMIT,
          ctx_builder=_ctx,
          dispatcher=_dispatch,
          session_factory=extensions_session,
          schema_missing_404=_ledger_404,
          user_dep=get_current_user_with_graph,
          graph_id_pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN,
      )

      _registrar.register(OperationSpec(
          name="create-element",
          summary="Create Element",
          command=cmd_create_element,
          request_model=CreateElementRequest,
          error_map={
              TaxonomyMissingError: 404,
              ElementMissingError: (
                  400,
                  lambda e: f"Parent element not found: {e.element_id}",
              ),
          },
      ))
  """

  # Class-level registry of every instantiated registrar. MCPRegistrar and
  # future agent-tool generators walk this to find all declared OperationSpecs
  # without having to know where each router module lives. Modules populate
  # this at import time; consumers read it lazily at first request.
  _all_instances: ClassVar[list["OperationRegistrar"]] = []

  def __init__(
    self,
    *,
    router: APIRouter,
    domain: str,
    tag: str,
    rate_limit_dep: Any,
    ctx_builder: Callable[..., OperationContext],
    dispatcher: Callable,
    session_factory: Callable,
    schema_missing_404: Callable[[], HTTPException],
    user_dep: Callable,
    graph_id_pattern: str,
    extension: str,
  ) -> None:
    self.router = router
    self.domain = domain
    self.tag = tag
    self.rate_limit_dep = rate_limit_dep
    self.ctx_builder = ctx_builder
    self.dispatcher = dispatcher
    self.session_factory = session_factory
    self.schema_missing_404 = schema_missing_404
    self.user_dep = user_dep
    self.graph_id_pattern = graph_id_pattern
    self.extension = extension
    # Build the extension-gate dependency once; every handler reuses it
    # so FastAPI's dependency resolution can cache across routes.
    self._extension_dep = require_graph_extension(extension)
    self.full_path_template = f"/extensions/{domain}/{{graph_id}}/operations"
    # Track registered specs for future MCP/agent adapter enumeration.
    self._registered: list[OperationSpec] = []
    OperationRegistrar._all_instances.append(self)

  @classmethod
  def specs_for_extension(
    cls, extension: str
  ) -> list[tuple["OperationRegistrar", OperationSpec]]:
    """Every `(registrar, spec)` pair for the given extension.

    MCPRegistrar and future adapters use this to enumerate declared
    operations. The registrar is included alongside each spec so
    consumers that need the session factory, context builder, or
    schema-missing helper don't have to re-derive them.
    """
    return [
      (reg, spec)
      for reg in cls._all_instances
      if reg.extension == extension
      for spec in reg._registered
    ]

  def register(self, spec: OperationSpec) -> Callable:
    """Mount a FastAPI POST handler for `spec`.

    Returns the metrics-wrapped handler, matching what a hand-written
    `@router.post` + `@endpoint_metrics_decorator` stack would leave
    at module scope. Callers typically bind this to a module-level
    name so tests and other importers can reference the route handler
    directly:

        create_element_op = _registrar.register(OperationSpec(...))
    """
    handler = self._build_handler(spec)
    metrics_wrapped = endpoint_metrics_decorator(
      f"{self.full_path_template}{spec.resolved_path}",
      method="POST",
      business_event_type=spec.resolve_business_event_type(self.domain),
    )(handler)
    self.router.post(
      spec.resolved_path,
      response_model=OperationEnvelope,
      operation_id=spec.openapi_operation_id,
      summary=spec.summary,
      description=spec.description,
      tags=[self.tag],
      dependencies=[self.rate_limit_dep],
    )(metrics_wrapped)
    self._registered.append(spec)
    return metrics_wrapped

  @property
  def registered_specs(self) -> list[OperationSpec]:
    """All `OperationSpec`s registered through this registrar. Useful
    for building MCP tool lists or agent tool manifests from the same
    source of truth."""
    return list(self._registered)

  def _build_handler(self, spec: OperationSpec) -> Callable:
    """Construct the async route handler for a spec.

    Late-binds the command via `getattr(source_module, command_name)`
    at call time so tests can patch the command at its source location
    (e.g., `patch("commands.taxonomies.update_taxonomy", ...)`) and
    the handler sees the patched version. A closure capture of the
    function reference would make patching impossible because the
    closure holds the original object, not a name lookup.

    The `body` parameter's annotation is set via `__annotations__`
    post-creation so FastAPI's signature introspection sees the
    concrete Pydantic class, not the generic `BaseModel`.
    """
    request_model = spec.request_model
    error_map = spec.error_map
    pre_validate = spec.pre_validate
    on_fresh_success = spec.on_fresh_success
    mark_stale_reason = spec.mark_stale_reason
    requires_created_by = spec.requires_created_by
    op_name = spec.name
    ctx_builder = self.ctx_builder
    dispatcher = self.dispatcher
    schema_missing_404 = self.schema_missing_404
    graph_id_pattern = self.graph_id_pattern
    user_dep = self.user_dep
    extension_dep = self._extension_dep
    # Late-bind the command and the session factory via sys.modules so
    # `unittest.mock.patch` on the source location works as expected.
    # A direct closure capture would hold the original function object,
    # making any patch applied after import invisible at call time.
    cmd_module_name = spec.command.__module__
    cmd_func_name = spec.command.__name__
    sf_module_name = self.session_factory.__module__
    sf_func_name = self.session_factory.__qualname__

    def _resolve_command() -> Callable:
      return getattr(sys.modules[cmd_module_name], cmd_func_name)

    def _resolve_session_factory() -> Callable:
      return getattr(sys.modules[sf_module_name], sf_func_name)

    async def handler(
      body: BaseModel,
      graph_id: str = Path(..., pattern=graph_id_pattern),
      user=Depends(user_dep),
      _ext: GraphExtensionContext = Depends(extension_dep),
      idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
      cache: IdempotencyCache = Depends(get_idempotency_cache),
    ) -> OperationEnvelope:
      # Optional pre-validation hook — lets specs do lightweight
      # parse/format checks before we open a DB session.
      if pre_validate is not None:
        pre_validate(body)

      ctx = ctx_builder(
        graph_id=graph_id,
        user_id=str(user.id),
        op=op_name,
        idempotency_key=idempotency_key,
        body=body,
      )

      def _runner():
        command = _resolve_command()
        try:
          with _resolve_session_factory()(graph_id) as session:
            try:
              if requires_created_by:
                return command(session, body, created_by=str(user.id))
              return command(session, body)
            except tuple(error_map.keys()) as exc:
              _raise_mapped(exc, error_map)
              raise AssertionError("unreachable: _raise_mapped always raises")
        except (ValueError, ProgrammingError):
          raise schema_missing_404()

      # Bind graph_id into the stale-reason callback so module-level specs
      # can declare the effect without capturing `graph_id` in a closure.
      effective_on_fresh_success = on_fresh_success
      if effective_on_fresh_success is None and mark_stale_reason is not None:
        _reason = mark_stale_reason

        def _mark_stale(_env, _g=graph_id, _r=_reason):
          mark_graph_stale(_g, _r)

        effective_on_fresh_success = _mark_stale

      return await dispatcher(
        ctx, _runner, cache, on_fresh_success=effective_on_fresh_success
      )

    handler.__name__ = f"{op_name.replace('-', '_')}_op"
    handler.__qualname__ = handler.__name__
    # Hoist the concrete request model into the signature so FastAPI's
    # signature introspection picks it up for request-body validation.
    handler.__annotations__ = {
      **handler.__annotations__,
      "body": request_model,
    }
    return handler


def _raise_mapped(exc: Exception, error_map: ErrorMap) -> None:
  """Look up `exc` in `error_map` and raise the corresponding
  `HTTPException`. Matches by `isinstance` in insertion order so
  subclass entries can precede superclass entries.
  """
  for exc_type, mapping in error_map.items():
    if isinstance(exc, exc_type):
      if isinstance(mapping, int):
        raise HTTPException(status_code=mapping, detail=str(exc))
      status_code, detail_factory = mapping
      raise HTTPException(status_code=status_code, detail=detail_factory(exc))
  # Shouldn't happen — the except clause already filtered on these types.
  raise exc


__all__ = [
  "ErrorDetailFactory",
  "ErrorMap",
  "ErrorMapEntry",
  "GraphExtensionContext",
  "OperationRegistrar",
  "OperationSpec",
  "load_graph_metadata",
  "require_graph_extension",
]
