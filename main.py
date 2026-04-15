"""RoboSystems Service API main application module."""

from datetime import UTC, datetime
from importlib.metadata import version as pkg_version
from pathlib import Path

from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from robosystems.config import env
from robosystems.config.logging import get_logger
from robosystems.config.openapi_tags import MAIN_API_TAGS
from robosystems.config.validation import EnvValidator
from robosystems.middleware.database import DatabaseSessionMiddleware
from robosystems.middleware.logging import (
  SecurityLoggingMiddleware,
  StructuredLoggingMiddleware,
)
from robosystems.middleware.otel import setup_telemetry
from robosystems.middleware.rate_limits import RateLimitHeaderMiddleware
from robosystems.routers import (
  auth_router_v1,
  billing_router_v1,
  graph_router,
  offering_router_v1,
  operations_router_v1,
  orgs_router_v1,
  status_router_v1,
  user_router_v1,
)
from robosystems.routers import (
  router as v1_router,
)
from robosystems.routers.admin import (
  cache_router as admin_cache_router,
)
from robosystems.routers.admin import (
  credits_router as admin_credits_router,
)
from robosystems.routers.admin import (
  graphs_router as admin_graphs_router,
)
from robosystems.routers.admin import (
  invoice_router as admin_invoice_router,
)
from robosystems.routers.admin import (
  orgs_router as admin_orgs_router,
)
from robosystems.routers.admin import (
  subscription_router as admin_subscription_router,
)
from robosystems.routers.admin import (
  users_router as admin_users_router,
)
from robosystems.routers.admin import (
  webhooks_router as admin_webhooks_router,
)

logger = get_logger("robosystems.api")


def is_relaxed_csp_path(path: str) -> bool:
  """Return True for paths that should receive the relaxed CSP.

  The relaxed CSP allows CDN-hosted scripts/styles needed by the
  Swagger UI at `/`, the static asset bundle, and the GraphiQL
  playground. GraphiQL is mounted graph-scoped at
  `/extensions/{graph_id}/graphql`, so a naive
  `path.startswith("/extensions/graphql")` check (its pre-cutover
  shape) no longer matches and would block the React/GraphiQL CDN
  bundle. We accept any `/extensions/.../graphql` path — covering
  both the new graph-scoped URL and the legacy shape defensively.

  Extracted from the security-headers middleware so the matcher
  contract has a unit-testable home; future URL changes that move
  GraphiQL must update this function and its tests.
  """
  if path in ("/", "/docs"):
    return True
  if path.startswith("/static"):
    return True
  if path.startswith("/extensions/") and path.endswith("/graphql"):
    return True
  return False


def create_app() -> FastAPI:
  """
  Create the FastAPI app and include the routers.

  Returns:
      FastAPI: The configured FastAPI application.
  """
  # Load description from markdown file in static folder
  description_file = Path(__file__).parent / "static" / "description.md"
  api_description = (
    description_file.read_text()
    if description_file.exists()
    else "RoboSystems Service API"
  )

  # Create the FastAPI app with custom tag ordering
  app = FastAPI(
    title="RoboSystems API",
    version=pkg_version("robosystems"),
    description=api_description,
    docs_url=None,  # Disable default docs
    redoc_url=None,  # Disable default ReDoc to use custom
    openapi_url="/openapi.json",
    openapi_tags=MAIN_API_TAGS,
  )

  # Setup OpenTelemetry
  setup_telemetry(app)

  # Initialize app state
  app.state.current_time = datetime.now(UTC)

  # Mount static files (always mount since we're serving directly from container)
  if Path("static").exists():
    app.mount("/static", StaticFiles(directory="static"), name="static")

  # Custom docs route with dark theme
  @app.get("/", response_class=HTMLResponse, include_in_schema=False)
  async def custom_docs():
    """Custom Swagger docs with dark theme."""
    try:
      from robosystems.utils.docs_template import generate_robosystems_docs

      return HTMLResponse(content=generate_robosystems_docs())
    except ImportError:
      # Fallback to default FastAPI docs
      from fastapi.openapi.docs import get_swagger_ui_html

      return get_swagger_ui_html(openapi_url="/openapi.json", title="RoboSystems API")

  # Custom ReDoc route with dark theme
  @app.get("/docs", response_class=HTMLResponse, include_in_schema=False)
  async def custom_redoc():
    """Custom ReDoc with dark theme."""
    try:
      from robosystems.utils.docs_template import generate_robosystems_redoc

      return HTMLResponse(content=generate_robosystems_redoc())
    except ImportError:
      # Fallback to default ReDoc
      from fastapi.openapi.docs import get_redoc_html

      return get_redoc_html(openapi_url="/openapi.json", title="RoboSystems API")

  # Startup event handler for validation
  @app.on_event("startup")
  async def startup_event():
    """Validate configuration on startup."""
    logger.info("Starting RoboSystems API...")

    # Validate environment configuration
    try:
      # Use the existing validator
      EnvValidator.validate_required_vars(env)
      config_summary = EnvValidator.get_config_summary(env)
      logger.info(f"Configuration validated successfully: {config_summary}")
    except Exception as e:
      logger.error(f"Configuration validation failed: {e}")
      if env.ENVIRONMENT == "prod":
        # In production, fail fast on invalid configuration
        raise
      else:
        # In development, log warning but continue
        logger.warning("Continuing with invalid configuration (development mode)")

    # Initialize query queue executor
    try:
      from robosystems.routers.graphs.query.setup import setup_query_executor

      setup_query_executor()
    except Exception as e:
      logger.error(f"Failed to initialize query queue: {e}")

    # Start Redis SSE event subscriber for worker-to-API communication
    try:
      from robosystems.middleware.sse.redis_subscriber import start_redis_subscriber

      await start_redis_subscriber()
      logger.info("Redis SSE event subscriber started successfully")
    except Exception as e:
      logger.error(f"Failed to start Redis SSE subscriber: {e}")
      # Don't fail startup, but SSE events from workers won't work
      # Continue without queue - queries will execute directly

    logger.info("RoboSystems API startup complete")

  # Shutdown event handler for cleanup
  @app.on_event("shutdown")
  async def shutdown_event():
    """Clean up resources on shutdown."""
    logger.info("Shutting down RoboSystems API...")

    # Stop Redis SSE event subscriber
    try:
      from robosystems.middleware.sse.redis_subscriber import stop_redis_subscriber

      await stop_redis_subscriber()
      logger.info("Redis SSE event subscriber stopped successfully")
    except Exception as e:
      logger.error(f"Error stopping Redis SSE subscriber: {e}")

    logger.info("RoboSystems API shutdown complete")

  # Configure CORS with specific domains for security
  main_cors_origins = env.get_main_cors_origins()
  logger.info(f"Main API CORS origins: {main_cors_origins}")

  app.add_middleware(
    CORSMiddleware,
    allow_origins=main_cors_origins,
    allow_credentials=True,  # Always enabled for cookie-based auth
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH"],
    allow_headers=[
      "Accept",
      "Accept-Language",
      "Content-Type",
      "Authorization",
      "X-API-Key",
      "X-Requested-With",
      # Operation endpoints under /extensions/{domain}/{graph_id}/operations/*
      # accept Idempotency-Key for safe retries — must be in allow_headers
      # or browser preflight will reject it for cross-origin requests.
      "Idempotency-Key",
    ],
    expose_headers=["X-Request-ID", "X-Rate-Limit-Remaining", "X-Rate-Limit-Reset"],
    max_age=3600,  # Cache preflight requests for 1 hour
  )

  # Add logging middleware (order matters - first added = outermost layer)
  app.add_middleware(StructuredLoggingMiddleware)
  app.add_middleware(SecurityLoggingMiddleware)

  # Add database session cleanup middleware
  app.add_middleware(DatabaseSessionMiddleware)

  # Add rate limit header middleware
  app.add_middleware(RateLimitHeaderMiddleware)

  # Request-level metrics for the extensions GraphQL endpoint.
  #
  # Strawberry's `GraphQLRouter` is a single FastAPI route, so the
  # `endpoint_metrics_decorator` we put on REST operation routes can't
  # reach individual GraphQL requests. Per-resolver spans come from
  # `strawberry.extensions.tracing.opentelemetry.OpenTelemetryExtension`
  # (wired in `graphql/schema.py`); this middleware adds the request-
  # level counter + latency histogram + business event so GraphQL reads
  # show up on the same Prometheus dashboards as the REST surface.
  #
  # Scoped to `/extensions/{graph_id}/graphql` only — every other path
  # falls through instantly to keep overhead at zero.
  @app.middleware("http")
  async def extensions_graphql_metrics_middleware(request: Request, call_next):
    path = request.url.path
    if not (path.startswith("/extensions/") and path.endswith("/graphql")):
      return await call_next(request)

    import time

    from robosystems.middleware.otel.metrics import (
      get_endpoint_metrics,
      record_error_metrics,
      record_request_metrics,
    )

    # `graph_id` is the second segment: /extensions/{graph_id}/graphql
    try:
      graph_id = path.split("/", 3)[2]
    except IndexError:  # pragma: no cover - path matcher already checked shape
      graph_id = None

    # Normalize the endpoint label so Prometheus cardinality stays bounded
    # (one time series for the whole GraphQL endpoint, regardless of
    # tenant). The tenant lives in `event_graph_id` on the business event.
    endpoint_label = "/extensions/{graph_id}/graphql"
    start = time.time()
    error_occurred = False
    status_code = 200
    user_id: str | None = None

    try:
      response = await call_next(request)
      status_code = response.status_code
      user_id = getattr(request.state, "user_id", None)
      # Fire the business event on 2xx GraphQL POSTs so dashboards can
      # differentiate "extensions GraphQL reads" from REST operations.
      if request.method == "POST" and 200 <= status_code < 300:
        get_endpoint_metrics().record_business_event(
          endpoint=endpoint_label,
          method=request.method,
          event_type="extensions_graphql_query",
          event_data={"graph_id": graph_id} if graph_id else {},
          user_id=user_id,
        )
      return response
    except Exception as exc:
      error_occurred = True
      status_code = getattr(exc, "status_code", 500)
      record_error_metrics(
        endpoint=endpoint_label,
        method=request.method,
        error_type=type(exc).__name__,
        error_code=str(getattr(exc, "detail", "Unknown error")),
        user_id=user_id,
      )
      raise
    finally:
      duration = time.time() - start
      record_request_metrics(
        endpoint=endpoint_label,
        method=request.method,
        status_code=status_code,
        duration=duration,
        user_id=user_id,
        error_occurred=error_occurred,
      )

  # Add security headers middleware
  @app.middleware("http")
  async def security_headers_middleware(request: Request, call_next):
    """Add security headers to all responses."""
    response = await call_next(request)

    # Core security headers
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"

    # HSTS for production/staging
    if env.ENVIRONMENT in ["prod", "staging"]:
      response.headers["Strict-Transport-Security"] = (
        "max-age=31536000; includeSubDomains"
      )

    # Path-based CSP - strict for API, relaxed for docs / GraphiQL.
    # See `is_relaxed_csp_path()` for the matched URL contract.
    path = request.url.path
    if is_relaxed_csp_path(path):
      # Relaxed CSP for documentation, static assets, and the GraphiQL
      # playground (which loads React/GraphiQL from unpkg.com, mirroring
      # Swagger UI's pattern).
      csp_directives = [
        "default-src 'self'",
        "script-src 'self' 'unsafe-inline' 'unsafe-eval' https://cdn.jsdelivr.net https://unpkg.com https://cdn.redoc.ly",
        "style-src 'self' 'unsafe-inline' https://unpkg.com https://fonts.googleapis.com",
        "img-src 'self' data: https: blob:",
        "font-src 'self' data: https://fonts.gstatic.com",
        "connect-src 'self' https://unpkg.com https://cdn.redoc.ly webpack:",  # Allow source maps
        "worker-src 'self' blob:",  # Allow web workers from blob URLs
        "frame-ancestors 'none'",
        "base-uri 'self'",
        "form-action 'self'",
      ]

    else:
      # Strict CSP for API endpoints
      csp_directives = [
        "default-src 'self'",
        "script-src 'self'",  # NO unsafe-inline for API
        "style-src 'self'",
        "img-src 'self' data:",
        "connect-src 'self'",
        "frame-ancestors 'none'",
        "base-uri 'self'",
        "form-action 'self'",
      ]

    response.headers["Content-Security-Policy"] = "; ".join(csp_directives)

    # Add cache control for routes that may return sensitive data in the
    # response body. We use a path-prefix allowlist rather than scanning
    # `response.body` — Starlette's `call_next` returns a `StreamingResponse`
    # that exposes no `.body` attribute, so any content-scanning branch
    # would silently never fire. The allowlist below covers every surface
    # that can emit auth tokens, API keys, billing details, or other
    # per-user secrets; everything else falls back to the default cache
    # behavior controlled by the route itself.
    _sensitive_path_prefixes = (
      "/v1/auth",
      "/v1/user",
      "/v1/billing",
      "/v1/orgs",
    )
    if any(path.startswith(p) for p in _sensitive_path_prefixes):
      response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, private"
      response.headers["Pragma"] = "no-cache"

    # Permissions Policy
    response.headers["Permissions-Policy"] = "geolocation=(), camera=(), microphone=()"

    return response

  # Exception handler for application-wide error handling
  @app.exception_handler(Exception)
  async def global_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """Global exception handler returning generic error and request ID.

    Internal exception details are logged server-side; clients receive a generic
    message with a correlation identifier.
    """
    request_id = getattr(request.state, "request_id", None)

    # Log full details with correlation ID
    try:
      logger.error(
        "Unhandled exception", extra={"request_id": request_id}, exc_info=True
      )
    except Exception:
      # Ensure handler never fails
      pass

    return JSONResponse(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      content={"detail": "Internal server error", "request_id": request_id},
    )

  # Include all routers
  app.include_router(auth_router_v1)
  app.include_router(status_router_v1)
  app.include_router(user_router_v1)
  app.include_router(orgs_router_v1)
  app.include_router(
    v1_router
  )  # Now includes sync, agent, and schedule routers as graph-scoped
  app.include_router(graph_router)
  app.include_router(offering_router_v1)
  app.include_router(operations_router_v1)  # Unified SSE operations monitoring
  app.include_router(billing_router_v1)

  # NOTE: /v1/ledger/{graph_id} and /v1/investor/{graph_id} were removed
  # in the extensions cutover. Reads are on /extensions/graphql; writes
  # are on /extensions/{roboledger,roboinvestor}/{graph_id}/operations/*.
  # Mounts appear further down in this function.

  # Extensions GraphQL endpoint (Strawberry). Graph-scoped at
  # `/extensions/{graph_id}/graphql` so:
  #
  # 1. Auth + per-graph access are enforced by the FastAPI dependency
  #    layer (`get_context` reads `graph_id` from the path and runs
  #    `check_graph_access` before Strawberry parses the query).
  # 2. Resolvers don't take `graphId` as an argument — they read it
  #    from `info.context["graph_id"]`. The schema is one-graph-at-a-time.
  # 3. Rate limiting (`subscription_aware_rate_limit_dependency`)
  #    becomes per-tenant naturally because the dependency reads
  #    `graph_id` from `request.path_params`.
  # 4. The wire surface is symmetric with REST writes:
  #    `POST /extensions/{graph_id}/graphql` (reads)
  #    `POST /extensions/{domain}/{graph_id}/operations/{op}` (writes)
  if env.EXTENSIONS_GRAPHQL_ENABLED and (
    env.ROBOLEDGER_ENABLED or env.ROBOINVESTOR_ENABLED
  ):
    from fastapi import Depends as _Depends
    from strawberry.fastapi import GraphQLRouter

    from robosystems.graphql import schema as extensions_graphql_schema
    from robosystems.graphql.context import get_context as graphql_context_getter
    from robosystems.middleware.rate_limits import (
      subscription_aware_rate_limit_dependency,
    )

    graphql_router = GraphQLRouter(
      extensions_graphql_schema,
      context_getter=graphql_context_getter,
      # GraphiQL playground (dev only).
      graphql_ide="graphiql" if env.is_development() else None,
    )
    app.include_router(
      graphql_router,
      prefix="/extensions/{graph_id}/graphql",
      tags=["Extensions: GraphQL"],
      include_in_schema=True,
      dependencies=[_Depends(subscription_aware_rate_limit_dependency)],
    )

  # Extensions REST operation surface — `POST /extensions/{domain}/{graph_id}/operations/{op_name}`.
  # Writes live here (reads go through /extensions/graphql). Each route
  # delegates to `operations/{domain}/commands/*` via the shared
  # `execute_operation` dispatcher in `middleware/extensions.py`.
  if env.ROBOLEDGER_ENABLED:
    from robosystems.routers.extensions.roboledger.operations import (
      router as roboledger_operations_router,
    )

    app.include_router(
      roboledger_operations_router,
      prefix="/extensions/roboledger/{graph_id}/operations",
      include_in_schema=True,
    )

  # `build-fact-grid` lives in its own router so it can be mounted
  # independently of ROBOLEDGER_ENABLED. The fact-grid queries the LadybugDB
  # graph schema (XBRL hypercube), which is shared by roboledger tenant
  # graphs AND the SEC shared repository. SEC-only deployments (no
  # roboledger tenants) still need this endpoint, so gating only on
  # FACT_GRID_ENABLED is correct. Same prefix as the operations router
  # so the wire URL is `POST /extensions/roboledger/{graph_id}/operations/build-fact-grid`.
  if env.FACT_GRID_ENABLED:
    from robosystems.routers.extensions.roboledger.views import (
      router as roboledger_views_router,
    )

    app.include_router(
      roboledger_views_router,
      prefix="/extensions/roboledger/{graph_id}/operations",
      include_in_schema=True,
    )

  if env.ROBOINVESTOR_ENABLED:
    from robosystems.routers.extensions.roboinvestor.operations import (
      router as roboinvestor_operations_router,
    )

    app.include_router(
      roboinvestor_operations_router,
      prefix="/extensions/roboinvestor/{graph_id}/operations",
      include_in_schema=True,
    )

  # Include admin routers (hidden from public docs)
  # The admin routers will not appear in the auto-generated docs
  app.include_router(admin_cache_router, include_in_schema=False)
  app.include_router(admin_subscription_router, include_in_schema=False)
  app.include_router(admin_invoice_router, include_in_schema=False)
  app.include_router(admin_webhooks_router, include_in_schema=False)
  app.include_router(admin_credits_router, include_in_schema=False)
  app.include_router(admin_graphs_router, include_in_schema=False)
  app.include_router(admin_users_router, include_in_schema=False)
  app.include_router(admin_orgs_router, include_in_schema=False)

  # Custom OpenAPI schema
  def custom_openapi():
    """
    Custom OpenAPI schema generator.

    Returns:
        dict: The OpenAPI schema.
    """
    if app.openapi_schema:
      return app.openapi_schema

    openapi_schema = get_openapi(
      title=app.title,
      version=app.version,
      description=app.description,
      routes=app.routes,
    )

    # Set up components structure if it doesn't exist
    if "components" not in openapi_schema:
      openapi_schema["components"] = {}

    # Set up security schemes (API key and Bearer JWT)
    openapi_schema["components"]["securitySchemes"] = {
      "APIKeyHeader": {
        "type": "apiKey",
        "in": "header",
        "name": "X-API-Key",
        "description": "API key for authentication",
      },
      "BearerAuth": {
        "type": "http",
        "scheme": "bearer",
        "bearerFormat": "JWT",
        "description": "JWT bearer token",
      },
    }

    # Ensure schemas section exists
    if "schemas" not in openapi_schema["components"]:
      openapi_schema["components"]["schemas"] = {}

    # Shared error shape for every /extensions/*/operations/* route. SDK
    # codegen tools use this to produce a typed error union so clients
    # can branch on 409 / 422 / 4xx without unstructured `detail` reads.
    openapi_schema["components"]["schemas"]["OperationError"] = {
      "type": "object",
      "description": (
        "Error envelope returned by extensions operation endpoints. "
        "Shape aligns with FastAPI's default error detail plus an optional "
        "`operation_id` for audit correlation."
      ),
      "properties": {
        "detail": {
          "oneOf": [
            {"type": "string"},
            {"type": "object"},
          ],
          "description": "Human-readable error detail or structured payload",
        },
        "operation_id": {
          "type": "string",
          "description": (
            "op_-prefixed ULID if the dispatcher minted one before the "
            "failure (async ops, idempotency conflicts, etc.)"
          ),
        },
      },
    }

    # Standard error responses injected into every extensions operation
    # route (and the GraphQL endpoint). Keeping them in one place here —
    # rather than duplicating `responses={...}` across ~32 @router.post
    # calls — keeps the router files thin and guarantees a single source
    # of truth for the error shape the SDKs compile against.
    _op_error_ref = {
      "application/json": {"schema": {"$ref": "#/components/schemas/OperationError"}}
    }
    _rate_limit_response_headers = {
      "X-Rate-Limit-Remaining": {
        "description": "Requests remaining in the current rate-limit window",
        "schema": {"type": "integer"},
      },
      "X-Rate-Limit-Reset": {
        "description": "Unix epoch seconds at which the current window resets",
        "schema": {"type": "integer"},
      },
    }
    _shared_operation_responses = {
      "400": {"description": "Invalid request payload", "content": _op_error_ref},
      "401": {"description": "Unauthorized — missing or invalid credentials"},
      "403": {"description": "Forbidden — caller cannot access this graph"},
      "404": {
        "description": "Resource not found (graph, ledger, report, etc.)",
        "content": _op_error_ref,
      },
      "409": {
        "description": (
          "Idempotency-Key reused with a different request body, or other "
          "operation-level conflict"
        ),
        "content": _op_error_ref,
      },
      "422": {
        "description": "Semantic validation failure (unbalanced ledger, etc.)",
        "content": _op_error_ref,
      },
      "429": {"description": "Rate limit exceeded"},
      "500": {"description": "Internal error"},
    }
    _idempotency_header_parameter = {
      "name": "Idempotency-Key",
      "in": "header",
      "required": False,
      "description": (
        "Optional client-supplied key for safe retries. Same key + same "
        "body within 24 hours replays the cached envelope; same key + "
        "different body returns HTTP 409 Conflict. Use a fresh key for "
        "distinct payloads (UUID v4 recommended)."
      ),
      "schema": {"type": "string", "maxLength": 255},
    }
    _idempotency_doc_paragraph = (
      "\n\n**Idempotency**: supply an `Idempotency-Key` header to make "
      "safe retries; replays within 24 hours return the same envelope. "
      "Reusing the key with a different body returns HTTP 409 Conflict."
    )

    def _is_operation_path(p: str) -> bool:
      return "/extensions/" in p and "/operations/" in p

    def _is_graphql_path(p: str) -> bool:
      return p.startswith("/extensions/") and p.endswith("/graphql")

    for _path, _methods in openapi_schema.get("paths", {}).items():
      if _is_operation_path(_path):
        for _method_name, _operation in _methods.items():
          if _method_name not in ("post",):
            continue
          # Merge standard error responses without clobbering any the
          # route already declared explicitly (some ops add domain-
          # specific 409/422 messaging).
          existing_responses = _operation.setdefault("responses", {})
          for _code, _resp in _shared_operation_responses.items():
            existing_responses.setdefault(_code, _resp)
          # Attach rate-limit headers to the success response(s) so SDKs
          # pick them up alongside the envelope.
          for _code, _resp in existing_responses.items():
            if _code.startswith("2") and isinstance(_resp, dict):
              _resp.setdefault("headers", {}).update(_rate_limit_response_headers)
          # Add the Idempotency-Key header parameter exactly once.
          params = _operation.setdefault("parameters", [])
          if not any(
            p.get("name") == "Idempotency-Key" and p.get("in") == "header"
            for p in params
          ):
            params.append(_idempotency_header_parameter)
          # Append the idempotency paragraph to the description so it
          # shows up in Swagger/ReDoc without editing each route.
          if _idempotency_doc_paragraph not in _operation.get("description", ""):
            _operation["description"] = (
              _operation.get("description") or ""
            ) + _idempotency_doc_paragraph
      elif _is_graphql_path(_path):
        for _method_name, _operation in _methods.items():
          if _method_name not in ("post", "get"):
            continue
          existing_responses = _operation.setdefault("responses", {})
          existing_responses.setdefault(
            "401",
            {"description": "Unauthorized — credentials presented but invalid"},
          )
          existing_responses.setdefault(
            "403",
            {"description": "Forbidden — caller cannot access this graph"},
          )
          existing_responses.setdefault("429", {"description": "Rate limit exceeded"})
          _graphql_description_note = (
            "\n\n**Auth**: pass `X-API-Key` (or a JWT `Authorization: "
            "Bearer` header). Unauthenticated introspection queries are "
            "deliberately allowed for SDK codegen; data queries require "
            "credentials and raise `UNAUTHENTICATED`."
            "\n\n**Error codes**: `LEDGER_NOT_INITIALIZED`, "
            "`INVESTOR_NOT_INITIALIZED`, and `UNAUTHENTICATED` surface in "
            "the GraphQL `errors[].extensions.code` field — see "
            "`graphql/README.md` for the full vocabulary."
          )
          if _graphql_description_note not in _operation.get("description", ""):
            _operation["description"] = (
              _operation.get("description") or ""
            ) + _graphql_description_note

    # Add security requirement to all endpoints except explicitly public ones
    public_exact_paths = {"/v1/status"}
    public_prefixes = ("/v1/auth", "/v1/offering")

    for path, methods in openapi_schema.get("paths", {}).items():
      is_public = path in public_exact_paths or any(
        path.startswith(p) for p in public_prefixes
      )
      if is_public:
        continue

      for method_name, operation in methods.items():
        # Declare both API key and Bearer as accepted security schemes
        operation["security"] = [
          {"APIKeyHeader": []},
          {"BearerAuth": []},
        ]

    # Custom tag ordering - extract from openapi_tags to avoid duplication
    tag_order = [tag_info["name"] for tag_info in app.openapi_tags or []]

    # Extract existing tags from the schema
    existing_tags = set()
    for path_info in openapi_schema["paths"].values():
      for method_info in path_info.values():
        if "tags" in method_info:
          existing_tags.update(method_info["tags"])

    # Build ordered tags list - only include tags that actually exist
    ordered_tags = []
    for tag in tag_order:
      if tag in existing_tags:
        # Find the tag description from our openapi_tags
        tag_desc = None
        for tag_info in app.openapi_tags or []:
          if tag_info["name"] == tag:
            tag_desc = tag_info["description"]
            break

        ordered_tags.append(
          {"name": tag, "description": tag_desc or f"{tag} operations"}
        )

    # Add any remaining tags not in our predefined order
    for tag in existing_tags:
      if tag not in tag_order:
        ordered_tags.append({"name": tag, "description": f"{tag} operations"})

    # Set the tags in the schema
    openapi_schema["tags"] = ordered_tags

    # Fix any $ref issues in responses if needed
    # Special handling for auth-test endpoint has been removed

    app.openapi_schema = openapi_schema
    return app.openapi_schema

  app.openapi = custom_openapi

  return app


app = create_app()
