"""Declarative operation registrar for extensions endpoints.

One `OperationSpec` describes a write operation; one `OperationRegistrar`
mounts it as a FastAPI POST handler, wiring the decorator chain, the
runner closure, and the error-translation tree. A new op is one
`OperationSpec` + one command function + one Pydantic request model.

The descriptor is adapter-neutral: `command`, `request_model`, and
`description` are everything an MCP or Operator tool adapter needs, so
other surfaces can be generated from the same declaration.

Not every operation fits — those needing async Dagster dispatch,
platform-DB dependencies, or pre-validation beyond a simple hook use
hand-written `@router.post` handlers instead.
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
from robosystems.logger import logger
from robosystems.middleware.auth.dependencies import require_graph_write_role
from robosystems.middleware.billing.enforcement import require_graph_access
from robosystems.middleware.graph.types import GRAPH_OR_SUBGRAPH_ID_PATTERN
from robosystems.middleware.graph.utils.subgraph import is_subgraph
from robosystems.middleware.operations import (
  IdempotencyCache,
  OperationContext,
  OperationEnvelope,
  get_idempotency_cache,
)
from robosystems.middleware.otel.metrics import endpoint_metrics_decorator
from robosystems.models.api.common import OPERATION_ERROR_RESPONSES
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


def is_schema_missing(exc: ProgrammingError) -> bool:
  """Whether a ``ProgrammingError`` is the tenant schema (or a table in it)
  not existing — the one case that means "not initialized" rather than a
  bug. Every other programming error (a migration that has not reached this
  tenant, a SQL fault, a privilege error) must surface, not be translated
  into a friendly 404 that hides it from the client and the audit log.
  """
  msg = str(exc)
  return "does not exist" in msg and ("schema" in msg or "relation" in msg)


def guard_command_runner(
  runner: Callable[[], Any],
  *,
  op_name: str,
  graph_id: str,
  schema_missing_404: Callable[[], HTTPException],
) -> Callable[[], Any]:
  """Give a hand-written command runner the registrar runner's error policy.

  Wraps ``runner`` as the outermost layer, so the handler's own typed
  ``except`` clauses (inside the runner) always win. What is left when the
  runner raises:

  - ``HTTPException`` — already translated; passes through.
  - ``ProgrammingError`` — a missing tenant schema/relation is the
    domain's "not initialized" 404. Anything else is a real database fault:
    logged with its traceback and re-raised (500), never hidden behind the
    friendly 404.
  - ``ValueError`` — a domain refusal the handler did not map. Surfaced as
    422 with its message (the shape every mapped ``ValueError`` already
    takes) and logged so the map gets fixed. The extension dependency has
    already refused subgraph and repository ids, so a ``ValueError`` here is
    never the session factory rejecting the graph id.
  """

  def _guarded() -> Any:
    try:
      return runner()
    except HTTPException:
      raise
    except ProgrammingError as exc:
      if is_schema_missing(exc):
        raise schema_missing_404()
      logger.warning(
        f"{op_name} on {graph_id}: database programming error", exc_info=True
      )
      raise
    except ValueError as exc:
      logger.warning(f"{op_name} on {graph_id}: unmapped {type(exc).__name__}: {exc}")
      raise HTTPException(status_code=422, detail=str(exc))

  return _guarded


# ── OperationSpec ────────────────────────────────────────────────────────


@dataclass
class OperationSpec:
  """Declarative description of a single extensions operation.

  Instantiate one per operation and hand it to
  `OperationRegistrar.register` to mount the REST route.

  Required:
    name: kebab-case name (e.g. "create-element"). Drives the URL path,
      OpenAPI operationId, audit log, and metrics label.
    summary: short title for OpenAPI and MCP tool lists.
    command: pure-function command, `(session, body, /, **kwargs) -> Response`.
    request_model: Pydantic request model, hoisted into the handler
      signature so validation happens at the API boundary.
    error_map: exception class → HTTP translation; either a bare status
      code (detail is `str(exc)`) or a `(status, detail_factory)` tuple.

  Optional:
    description: longer OpenAPI description; falls back to the command's
      docstring.
    path: URL path override (default `/{name}`).
    business_event_type: metrics event key (default `{domain}_{snake_name}`).
    requires_created_by: pass `created_by=str(user.id)` to the command
      (default True).
    requires_graph_id: pass `graph_id=<path graph_id>` to the command
      (default False). Needed for cross-DB lookups such as the platform
      Connection registry, since the tenant session alone doesn't carry
      the graph identity.
    pre_validate: sync validator run before any DB session is opened;
      may raise `HTTPException`. For parse/format checks such as
      `parse_period(body.period)`.
    on_fresh_success: callback taking the envelope, invoked on a
      non-replayed success. Prefer `mark_stale_reason` for the common
      mark-stale case — a module-level spec cannot capture `graph_id`
      in a closure.
    mark_stale_reason: when set, the registrar calls
      `mark_graph_stale(graph_id, mark_stale_reason)` on fresh success.
      `on_fresh_success` wins if both are set.
    result_type: Pydantic model the command returns. When set, the
      response model becomes `OperationEnvelope[result_type]` so SDKs
      see `result: <Model>` instead of `result: any`. Leave unset for
      ops returning raw dicts/lists or nothing.
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
  requires_graph_id: bool = False
  pre_validate: Callable[[BaseModel], None] | None = None
  on_fresh_success: Callable | None = None
  mark_stale_reason: str | None = None
  result_type: type[BaseModel] | None = None

  @property
  def resolved_path(self) -> str:
    return self.path or f"/{self.name}"

  def resolve_business_event_type(self, domain: str) -> str:
    """Metrics event key: the spec's explicit `business_event_type`, else
    `{domain}_{snake_name}` so each registrar's domain prefixes its own
    events.
    """
    if self.business_event_type:
      return self.business_event_type
    snake = self.name.replace("-", "_")
    return f"{domain}_{snake}"

  @property
  def openapi_operation_id(self) -> str:
    """OpenAPI operationId in camelCase verbNoun (e.g. create-agent -> createAgent)."""
    parts = self.name.split("-")
    return parts[0] + "".join(p.capitalize() for p in parts[1:])


# ── Extension feature gate ───────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class GraphExtensionContext:
  """Per-request snapshot of the graph fields the extension gate cares about.

  Returned by `require_graph_extension(...)` so callers that already need
  the graph type or extension list don't reload the row.

  Reporting Style is deliberately absent: it lives on the entity in the
  extensions DB, so the render path resolves it from its own extensions
  session (``load_primary_reporting_style`` / ``load_entity_reporting_style``)
  without a platform round-trip.
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
    # A subgraph is a modality container, not an extensions target: it has
    # no tenant schema of its own. The route pattern admits `{parent}_{name}`
    # ids (so the same routers serve graph-scoped reads), so refuse them
    # here — the same answer the GraphQL endpoint gives — instead of letting
    # the session factory reject the id deep inside a handler, where it
    # surfaced as a 404 or a 500 depending on which handler caught it.
    if is_subgraph(graph_id):
      raise HTTPException(
        status_code=403,
        detail=(
          f"Subgraph '{graph_id}' is not addressable via {extension} "
          "operations; target the parent graph."
        ),
      )
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
    # Lifecycle/subscription gate at read strength — every extensions route
    # (reads, views, commands) passes through here, so a suspended or expired
    # graph is closed on the whole surface. Commands additionally run the
    # write-strength check inside `require_graph_write_role`.
    require_graph_access(graph_id, session, require_write=False)
    return meta

  return _dep


# ── Registrar ────────────────────────────────────────────────────────────


class OperationRegistrar:
  """Binds domain-specific plumbing and mounts `OperationSpec`s as
  FastAPI routes.

  Create one per domain at router setup time, then call `.register(spec)`
  for each operation.

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

  # Every instantiated registrar. Tool-adapter generators walk this to find
  # all declared OperationSpecs without knowing where each router module
  # lives. Populated at import time; read lazily at first request.
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

    Tool adapters use this to enumerate declared operations. The registrar
    travels with each spec so consumers can reach the session factory,
    context builder, and schema-missing helper.
    """
    return [
      (reg, spec)
      for reg in cls._all_instances
      if reg.extension == extension
      for spec in reg._registered
    ]

  def register(self, spec: OperationSpec) -> Callable:
    """Mount a FastAPI POST handler for `spec` and return the
    metrics-wrapped handler. Bind it to a module-level name so tests and
    other importers can reference the route handler directly:

        create_element_op = _registrar.register(OperationSpec(...))
    """
    handler = self._build_handler(spec)
    metrics_wrapped = endpoint_metrics_decorator(
      f"{self.full_path_template}{spec.resolved_path}",
      method="POST",
      business_event_type=spec.resolve_business_event_type(self.domain),
    )(handler)
    # When `result_type` is set, parameterize the envelope so the typed
    # result shape lands in OpenAPI; otherwise keep the default
    # `OperationEnvelope` (i.e. `OperationEnvelope[Any]`).
    response_model = (
      OperationEnvelope[spec.result_type]
      if spec.result_type is not None
      else OperationEnvelope
    )
    self.router.post(
      spec.resolved_path,
      response_model=response_model,
      operation_id=spec.openapi_operation_id,
      summary=spec.summary,
      description=spec.description,
      tags=[self.tag],
      dependencies=[self.rate_limit_dep],
      responses={**OPERATION_ERROR_RESPONSES},
    )(metrics_wrapped)
    self._registered.append(spec)
    return metrics_wrapped

  @property
  def registered_specs(self) -> list[OperationSpec]:
    """All `OperationSpec`s registered through this registrar, for building
    MCP tool lists or agent tool manifests from the same source of truth.
    """
    return list(self._registered)

  def _build_handler(self, spec: OperationSpec) -> Callable:
    """Construct the async route handler for a spec."""
    request_model = spec.request_model
    error_map = spec.error_map
    pre_validate = spec.pre_validate
    on_fresh_success = spec.on_fresh_success
    mark_stale_reason = spec.mark_stale_reason
    requires_created_by = spec.requires_created_by
    requires_graph_id = spec.requires_graph_id
    op_name = spec.name
    ctx_builder = self.ctx_builder
    dispatcher = self.dispatcher
    schema_missing_404 = self.schema_missing_404
    graph_id_pattern = self.graph_id_pattern
    user_dep = self.user_dep
    extension_dep = self._extension_dep
    # Late-bind the command and session factory through sys.modules so
    # `unittest.mock.patch` at the source location works. A closure capture
    # would hold the original function object, making a patch applied after
    # import invisible at call time.
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
      # Every registrar op is a command (write). The extension gate proves
      # provisioning and `user_dep` proves graph membership — neither checks
      # the write role, so a read-only `viewer` would otherwise reach the
      # OLTP command surface. Enforce member/admin first, fail-closed, before
      # running any request-shaping logic for an unauthorized caller.
      require_graph_write_role(str(user.id), graph_id)

      # Lightweight parse/format checks, before any DB session is opened.
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
        # Set once the session factory has accepted the graph id, so a
        # ValueError from the factory (not a tenant-schema id — subgraphs
        # share their parent's schema) can be told apart from one the
        # command raised.
        session_bound = False
        try:
          with _resolve_session_factory()(graph_id) as session:
            session_bound = True
            try:
              kwargs = {}
              if requires_created_by:
                kwargs["created_by"] = str(user.id)
              if requires_graph_id:
                kwargs["graph_id"] = graph_id
              return command(session, body, **kwargs)
            except tuple(error_map.keys()) as exc:
              _raise_mapped(exc, error_map)
              raise AssertionError("unreachable: _raise_mapped always raises")
        except ProgrammingError as exc:
          # Only a missing schema/relation means "not initialized". Anything
          # else is a real database fault: log it and let it surface as 500
          # rather than hiding it behind the friendly 404.
          if is_schema_missing(exc):
            raise schema_missing_404()
          logger.warning(
            f"{op_name} on {graph_id}: database programming error", exc_info=True
          )
          raise
        except ValueError as exc:
          if not session_bound:
            raise schema_missing_404()
          # A domain refusal the spec's error_map did not name. Surface it as
          # the validation failure it is (every mapped ValueError is 422),
          # and log it so the map gets fixed.
          logger.warning(
            f"{op_name} on {graph_id}: unmapped {type(exc).__name__}: {exc}"
          )
          raise HTTPException(status_code=422, detail=str(exc))

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
    # Hoist the concrete request model into the signature so FastAPI sees a
    # Pydantic class, not the generic `BaseModel`, for body validation.
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
  "guard_command_runner",
  "is_schema_missing",
  "load_graph_metadata",
  "require_graph_extension",
]
