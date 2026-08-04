"""RoboSystems Service API main application module."""

import time
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from importlib.metadata import version as pkg_version
from pathlib import Path

from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from starlette.exceptions import HTTPException as StarletteHTTPException

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
from robosystems.middleware.otel.metrics import (
  get_endpoint_metrics,
  record_error_metrics,
  record_request_metrics,
)
from robosystems.middleware.rate_limits import RateLimitHeaderMiddleware
from robosystems.routers import (
  auth_router_v1,
  billing_router_v1,
  graph_router,
  graph_schema_router_v1,
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
from robosystems.utils.docs_template import (
  generate_robosystems_docs,
  generate_robosystems_redoc,
)

logger = get_logger("robosystems.api")

# Path prefixes whose responses may contain per-user secrets (tokens,
# API keys, billing details, org membership). These get `Cache-Control:
# no-store` applied in the security-headers middleware.
_SENSITIVE_PATH_PREFIXES = ("/v1/auth", "/v1/user", "/v1/billing", "/v1/orgs")


def csp_variant_for_path(path: str) -> str:
  """Which CSP variant a path gets.

  - "docs": Swagger UI / ReDoc pages and their assets, self-hosted from
    /static — no third-party script origins and no 'unsafe-inline' script.
  - "graphiql": the GraphiQL playground, which loads React/GraphiQL from
    CDNs and needs the historical relaxed policy.
  - "api": everything else — strict policy.
  """
  if path in ("/", "/docs") or path.startswith("/static"):
    return "docs"
  if path.startswith("/extensions/") and path.endswith("/graphql"):
    return "graphiql"
  return "api"


@asynccontextmanager
async def lifespan(app: FastAPI):
  """Startup + shutdown lifecycle. Replaces deprecated @on_event handlers."""
  logger.info("Starting RoboSystems API...")

  # Validate environment configuration
  try:
    EnvValidator.validate_required_vars(env)
    config_summary = EnvValidator.get_config_summary(env)
    logger.info(f"Configuration validated successfully: {config_summary}")
  except Exception as e:
    logger.error(f"Configuration validation failed: {e}")
    if env.ENVIRONMENT in ("prod", "staging"):
      # Fail fast in prod and staging; continue in dev/test so local iteration
      # isn't blocked.
      raise
    logger.warning("Continuing with invalid configuration (development mode)")

  # Initialize query queue executor
  try:
    from robosystems.routers.graphs.query.setup import setup_query_executor

    setup_query_executor()
  except Exception as e:
    logger.error(f"Failed to initialize query queue: {e}")

  # Start Redis SSE event subscriber for worker → API communication
  try:
    from robosystems.middleware.sse.redis_subscriber import start_redis_subscriber

    await start_redis_subscriber()
    logger.info("Redis SSE event subscriber started successfully")
  except Exception as e:
    logger.error(f"Failed to start Redis SSE subscriber: {e}")

  logger.info("RoboSystems API startup complete")

  yield

  logger.info("Shutting down RoboSystems API...")

  try:
    from robosystems.middleware.sse.redis_subscriber import stop_redis_subscriber

    await stop_redis_subscriber()
    logger.info("Redis SSE event subscriber stopped successfully")
  except Exception as e:
    logger.error(f"Error stopping Redis SSE subscriber: {e}")

  logger.info("RoboSystems API shutdown complete")


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

  app = FastAPI(
    title="RoboSystems API",
    version=pkg_version("robosystems"),
    description=api_description,
    docs_url=None,  # replaced by custom_docs below
    redoc_url=None,  # replaced by custom_redoc below
    openapi_url="/openapi.json",
    openapi_tags=MAIN_API_TAGS,
    lifespan=lifespan,
  )

  setup_telemetry(app)
  app.state.current_time = datetime.now(UTC)

  if Path("static").exists():
    app.mount("/static", StaticFiles(directory="static"), name="static")

  # RFC 9116 vulnerability disclosure pointer (mirrors the frontend apps).
  security_txt_file = Path("static") / "security.txt"
  if security_txt_file.exists():
    security_txt_content = security_txt_file.read_text(encoding="utf-8")

    @app.get("/.well-known/security.txt", include_in_schema=False)
    async def security_txt() -> PlainTextResponse:
      return PlainTextResponse(security_txt_content)

  # Custom dark-themed Swagger + ReDoc (served inline from docs_template).
  @app.get("/", response_class=HTMLResponse, include_in_schema=False)
  async def custom_docs():
    return HTMLResponse(content=generate_robosystems_docs())

  @app.get("/docs", response_class=HTMLResponse, include_in_schema=False)
  async def custom_redoc():
    return HTMLResponse(content=generate_robosystems_redoc())

  # Configure CORS with specific domains for security
  main_cors_origins = env.get_main_cors_origins()
  logger.info(f"Main API CORS origins: {main_cors_origins}")

  app.add_middleware(
    CORSMiddleware,
    allow_origins=main_cors_origins,
    allow_credentials=True,  # Always enabled for cookie-based auth
    # Grant Chrome Private Network Access preflight in dev so a public-origin
    # tunnel (e.g. ngrok) can call back to localhost. Production never needs
    # this — no localhost endpoints are exposed.
    allow_private_network=env.is_development(),
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

  # Request-level metrics for /extensions/{graph_id}/graphql.
  # Per-resolver spans come from Strawberry's OpenTelemetryExtensionSync
  # (wired in graphql/schema.py); this covers the request envelope.
  @app.middleware("http")
  async def extensions_graphql_metrics_middleware(request: Request, call_next):
    path = request.url.path
    if not (path.startswith("/extensions/") and path.endswith("/graphql")):
      return await call_next(request)

    try:
      graph_id = path.split("/", 3)[2]
    except IndexError:  # pragma: no cover - path matcher already checked shape
      graph_id = None

    # Normalized label keeps Prometheus cardinality bounded; tenant goes
    # on the business event instead.
    endpoint_label = "/extensions/{graph_id}/graphql"
    start = time.time()
    error_occurred = False
    status_code = 200
    user_id: str | None = None

    try:
      response = await call_next(request)
      status_code = response.status_code
      user_id = getattr(request.state, "user_id", None)
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

    # Path-based CSP — strict for API, self-hosted policy for docs,
    # relaxed (CDN) policy only for the GraphiQL playground.
    path = request.url.path
    csp_variant = csp_variant_for_path(path)
    if csp_variant == "docs":
      # Swagger UI / ReDoc served entirely from this origin (/static/vendor).
      # Both UIs inject inline <style> at runtime, so style-src keeps
      # 'unsafe-inline'; script-src does not need it (init lives in
      # /static/swagger-init.js) and no third-party origin is allowed.
      csp_directives = [
        "default-src 'self'",
        "script-src 'self'",
        "style-src 'self' 'unsafe-inline'",
        "img-src 'self' data: blob:",
        "font-src 'self' data:",
        "connect-src 'self'",
        "worker-src 'self' blob:",  # ReDoc renders via a blob web worker
        "object-src 'none'",
        "frame-ancestors 'none'",
        "base-uri 'self'",
        "form-action 'self'",
      ]

    elif csp_variant == "graphiql":
      csp_directives = [
        "default-src 'self'",
        "script-src 'self' 'unsafe-inline' 'unsafe-eval' https://cdn.jsdelivr.net https://unpkg.com",
        "style-src 'self' 'unsafe-inline' https://unpkg.com https://fonts.googleapis.com",
        "img-src 'self' data: https: blob:",
        "font-src 'self' data: https://fonts.gstatic.com",
        "connect-src 'self' https://unpkg.com webpack:",  # Allow source maps
        "worker-src 'self' blob:",  # Allow web workers from blob URLs
        "object-src 'none'",
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
        "object-src 'none'",
        "frame-ancestors 'none'",
        "base-uri 'self'",
        "form-action 'self'",
      ]

    response.headers["Content-Security-Policy"] = "; ".join(csp_directives)

    # Cache-Control: no-store for routes that may return per-user secrets.
    # (Path-prefix allowlist — StreamingResponse has no .body to scan.)
    if any(path.startswith(p) for p in _SENSITIVE_PATH_PREFIXES):
      response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, private"
      response.headers["Pragma"] = "no-cache"

    # Permissions Policy
    response.headers["Permissions-Policy"] = "geolocation=(), camera=(), microphone=()"

    return response

  # 422 / RequestValidationError handler — normalize FastAPI's default
  # pydantic-validation error shape (`{"detail": [{"loc": ..., "msg": ..., "type": ...}]}`)
  # into our standard `ErrorResponse` shape (`{"detail": str, "code": str, ...}`).
  # Without this, the OpenAPI spec advertises 422 = HTTPValidationError,
  # but our manual `HTTPException(status_code=422, detail="...")` raises
  # produce a string-detail response. Two shapes for one status code
  # break SDK response parsers (e.g., openapi-python-client). This handler
  # makes 422 consistently use the `ErrorResponse` shape.
  @app.exception_handler(RequestValidationError)
  async def request_validation_handler(
    request: Request, exc: RequestValidationError
  ) -> JSONResponse:
    request_id = getattr(request.state, "request_id", None)
    # Compress the pydantic error list into a readable summary string.
    parts = []
    for err in exc.errors():
      loc = ".".join(str(p) for p in err.get("loc", []) if p != "body")
      msg = err.get("msg", "validation error")
      parts.append(f"{loc}: {msg}" if loc else msg)
    detail = "; ".join(parts) or "Request validation failed"
    return JSONResponse(
      status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
      content={
        "detail": detail,
        "code": "VALIDATION_ERROR",
        "request_id": request_id,
      },
    )

  # HTTPException pass-through (any status code) — match Starlette's default
  # `{"detail": <whatever-was-passed>}` shape and add `request_id` for
  # correlation. Detail may be a string (most common) or a dict (close-period
  # blockers, graph_limit, etc.); callers parse it as `response["detail"]`
  # in both cases, so always wrap — never spread.
  @app.exception_handler(StarletteHTTPException)
  async def http_exception_handler(
    request: Request, exc: StarletteHTTPException
  ) -> JSONResponse:
    request_id = getattr(request.state, "request_id", None)
    return JSONResponse(
      status_code=exc.status_code,
      content={"detail": exc.detail, "request_id": request_id},
      headers=getattr(exc, "headers", None),
    )

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

  # Core platform routers
  app.include_router(auth_router_v1)
  app.include_router(status_router_v1)
  app.include_router(user_router_v1)
  app.include_router(orgs_router_v1)
  app.include_router(v1_router)
  app.include_router(graph_router)
  app.include_router(graph_schema_router_v1)
  app.include_router(offering_router_v1)
  app.include_router(operations_router_v1)
  app.include_router(billing_router_v1)

  # Extensions GraphQL endpoint (Strawberry). Graph-scoped at
  # /extensions/{graph_id}/graphql — see robosystems/graphql/README.md.
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

  # Extensions REST operation surface: POST /extensions/{domain}/{graph_id}/operations/{op}
  if env.ROBOLEDGER_ENABLED:
    from robosystems.routers.extensions.roboledger.operations import (
      router as roboledger_operations_router,
    )
    from robosystems.routers.extensions.roboledger.reads import (
      router as roboledger_reads_router,
    )

    app.include_router(
      roboledger_operations_router,
      prefix="/extensions/roboledger/{graph_id}/operations",
      include_in_schema=True,
    )
    app.include_router(
      roboledger_reads_router,
      prefix="/extensions/roboledger/{graph_id}/operations",
      include_in_schema=True,
    )
    # Serialization-bundle downloads are a READ — they live on the
    # GraphQL surface as `reportDownloadUrl(reportId, format)` on the
    # Report type, not as a REST resource. (Removed the lone REST GET
    # outlier; see issue #751.)

  # build-fact-grid mounts independently of ROBOLEDGER_ENABLED so SEC-only
  # deployments still get it. Rationale in routers/extensions/roboledger/views.py.
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

  # Admin routers — hidden from the public OpenAPI schema.
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

    # Shared error responses / Idempotency-Key header / rate-limit header
    # specs, injected below into every extensions operation route so the
    # router files stay thin and the wire shape stays consistent.
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
      return ("/extensions/" in p and "/operations/" in p) or (
        "/graphs/" in p and "/operations/" in p
      )

    def _is_graphql_path(p: str) -> bool:
      return p.startswith("/extensions/") and p.endswith("/graphql")

    for _path, _methods in openapi_schema.get("paths", {}).items():
      if _is_operation_path(_path):
        for _method_name, _operation in _methods.items():
          if _method_name != "post":
            continue
          existing_responses = _operation.setdefault("responses", {})
          for _code, _resp in _shared_operation_responses.items():
            existing_responses.setdefault(_code, _resp)
          for _code, _resp in existing_responses.items():
            if _code.startswith("2") and isinstance(_resp, dict):
              _resp.setdefault("headers", {}).update(_rate_limit_response_headers)
          params = _operation.setdefault("parameters", [])
          if not any(
            p.get("name") == "Idempotency-Key" and p.get("in") == "header"
            for p in params
          ):
            params.append(_idempotency_header_parameter)
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

    # Declare API key + Bearer as accepted security schemes on every
    # non-public endpoint.
    public_exact_paths = {"/v1/status"}
    public_prefixes = ("/v1/auth", "/v1/offering")

    for path, methods in openapi_schema.get("paths", {}).items():
      if path in public_exact_paths or any(path.startswith(p) for p in public_prefixes):
        continue
      for _method_name, operation in methods.items():
        operation["security"] = [{"APIKeyHeader": []}, {"BearerAuth": []}]

    # Apply the custom tag ordering from openapi_tags, only emitting
    # tags that are actually in use.
    tag_order = [tag_info["name"] for tag_info in app.openapi_tags or []]
    existing_tags = {
      tag
      for path_info in openapi_schema["paths"].values()
      for method_info in path_info.values()
      for tag in method_info.get("tags", [])
    }
    tag_descriptions = {
      tag_info["name"]: tag_info["description"] for tag_info in app.openapi_tags or []
    }
    ordered_tags = [
      {"name": tag, "description": tag_descriptions.get(tag, f"{tag} operations")}
      for tag in tag_order
      if tag in existing_tags
    ]
    for tag in existing_tags - set(tag_order):
      ordered_tags.append({"name": tag, "description": f"{tag} operations"})
    openapi_schema["tags"] = ordered_tags

    app.openapi_schema = openapi_schema
    return app.openapi_schema

  app.openapi = custom_openapi

  return app


app = create_app()
