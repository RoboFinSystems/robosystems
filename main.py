"""Compose and configure the RoboSystems FastAPI application.

`create_app` wires middleware (CORS, logging, security headers, rate-limit
headers), exception handlers, the core `/v1` routers, the flag-gated
`/extensions` GraphQL and operation surfaces, and a custom OpenAPI generator
that injects the shared operation error/idempotency contract.
"""

import json
import time
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from importlib.metadata import version as pkg_version
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi
from fastapi.responses import (
  HTMLResponse,
  JSONResponse,
  PlainTextResponse,
  RedirectResponse,
)
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
  install_uvicorn_log_redaction,
)
from robosystems.middleware.otel import setup_telemetry
from robosystems.middleware.otel.metrics import (
  get_endpoint_metrics,
  record_error_metrics,
  record_request_metrics,
)
from robosystems.middleware.rate_limits import RateLimitHeaderMiddleware
from robosystems.middleware.request_size import RequestSizeLimitMiddleware
from robosystems.routers import (
  auth_router_v1,
  billing_router_v1,
  graph_router,
  graph_schema_router_v1,
  mcp_agnostic_router_v1,
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
  oauth_router as admin_oauth_router,
)
from robosystems.routers.admin import (
  orgs_router as admin_orgs_router,
)
from robosystems.routers.admin import (
  scim_router as admin_scim_router,
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
from robosystems.routers.oauth import router as oauth_router
from robosystems.utils.docs_template import (
  generate_robosystems_docs,
)

logger = get_logger("robosystems.api")

# Responses under these may carry per-user secrets, so they get
# `Cache-Control: no-store` (`/admin` for the SCIM bootstrap bearer token).
_SENSITIVE_PATH_PREFIXES = (
  "/v1/auth",
  "/v1/user",
  "/v1/billing",
  "/v1/orgs",
  "/scim",
  "/admin",
)


def csp_variant_for_path(path: str, *, graphiql_enabled: bool = False) -> str:
  """Which CSP variant a path gets.

  - "docs": the self-hosted Swagger page at ``/`` and ``/static``.
  - "graphiql": the CDN-loaded playground, only while it is served
    (development). Defaults closed, so an omitted flag cannot relax prod.
  - "api": everything else, strict.
  """
  if path == "/" or path.startswith("/static"):
    return "docs"
  if graphiql_enabled and path.startswith("/extensions/") and path.endswith("/graphql"):
    return "graphiql"
  return "api"


@asynccontextmanager
async def lifespan(app: FastAPI):
  """Validate configuration and start/stop background subscribers.

  Configuration failures are fatal in prod and staging, and logged-and-ignored
  elsewhere so local iteration is not blocked.
  """
  logger.info("Starting RoboSystems API...")

  try:
    EnvValidator.validate_required_vars(env)
    config_summary = EnvValidator.get_config_summary(env)
    logger.info(f"Configuration validated successfully: {config_summary}")
  except Exception as e:
    logger.error(f"Configuration validation failed: {e}")
    if env.ENVIRONMENT in ("prod", "staging"):
      raise
    logger.warning("Continuing with invalid configuration (development mode)")

  try:
    from robosystems.routers.graphs.query.setup import setup_query_executor

    setup_query_executor()
  except Exception as e:
    logger.error(f"Failed to initialize query queue: {e}")

  # Worker → API SSE events
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
  """Build the configured FastAPI application."""
  # Before anything serves: Uvicorn's access log writes the raw query string,
  # where MCP connector auth carries a graph-scoped key.
  install_uvicorn_log_redaction()

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
    docs_url=None,  # custom routes below
    redoc_url=None,
    openapi_url="/openapi.json",
    openapi_tags=MAIN_API_TAGS,
    lifespan=lifespan,
  )

  setup_telemetry(app)
  app.state.current_time = datetime.now(UTC)

  # Serves the Swagger page's vendored bundle.
  if Path("static").exists():
    app.mount("/static", StaticFiles(directory="static"), name="static")

  # RFC 9116 vulnerability disclosure pointer.
  security_txt_file = Path("static") / "security.txt"
  if security_txt_file.exists():
    security_txt_content = security_txt_file.read_text(encoding="utf-8")

    @app.get("/.well-known/security.txt", include_in_schema=False)
    async def security_txt() -> PlainTextResponse:
      return PlainTextResponse(security_txt_content)

  # Glama connector-ownership claim.
  glama_file = Path("static") / "glama.json"
  if glama_file.exists():
    glama_content = json.loads(glama_file.read_text(encoding="utf-8"))

    @app.get("/.well-known/glama.json", include_in_schema=False)
    async def glama_json() -> JSONResponse:
      return JSONResponse(glama_content)

  # ChatGPT app-directory domain verification token, served as plain text.
  openai_challenge_file = Path("static") / "openai-apps-challenge"
  if openai_challenge_file.exists():
    openai_challenge_token = openai_challenge_file.read_text(encoding="utf-8").strip()

    @app.get("/.well-known/openai-apps-challenge", include_in_schema=False)
    async def openai_apps_challenge() -> PlainTextResponse:
      return PlainTextResponse(openai_challenge_token)

  # `/` is Swagger UI, kept for its try-it panel (noindex: a tool, not a
  # document). `/docs` redirects to the published per-operation reference so
  # existing links keep working.
  @app.get("/", response_class=HTMLResponse, include_in_schema=False)
  async def custom_docs():
    return HTMLResponse(content=generate_robosystems_docs())

  published_reference = f"{env.ROBOSYSTEMS_URL}/docs/api"

  # HEAD too: FastAPI does not imply it, and crawlers probing with HEAD would
  # get a 405 instead of the redirect.
  @app.api_route("/docs", methods=["GET", "HEAD"], include_in_schema=False)
  async def docs_redirect() -> RedirectResponse:
    return RedirectResponse(published_reference, status_code=301)

  main_cors_origins = env.get_main_cors_origins()
  logger.info(f"Main API CORS origins: {main_cors_origins}")

  app.add_middleware(
    CORSMiddleware,
    allow_origins=main_cors_origins,
    allow_credentials=True,  # cookie-based auth
    # Chrome Private Network Access, so a dev tunnel (ngrok) can reach localhost.
    allow_private_network=env.is_development(),
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH"],
    allow_headers=[
      "Accept",
      "Accept-Language",
      "Content-Type",
      "Authorization",
      "X-API-Key",
      "X-Requested-With",
      "Idempotency-Key",  # operation retries
      "X-App-Source",  # email branding when Referer can't identify the app
      # Browser MCP clients echo it on every request after initialize.
      "MCP-Protocol-Version",
    ],
    expose_headers=["X-Request-ID", "X-Rate-Limit-Remaining", "X-Rate-Limit-Reset"],
    max_age=3600,
  )

  # Body-size cap inside CORS but outside everything else: FastAPI reads the
  # body before dependencies run, so nothing downstream can bound it. The
  # Stripe webhook reads its body before verifying the signature, hence tighter.
  from robosystems.config.constants import (
    PUBLIC_MAX_REQUEST_SIZE,
    WEBHOOK_MAX_REQUEST_SIZE,
  )

  app.add_middleware(
    RequestSizeLimitMiddleware,
    max_body_size=PUBLIC_MAX_REQUEST_SIZE,
    path_limits=[("/admin/v1/webhooks/", WEBHOOK_MAX_REQUEST_SIZE)],
  )

  # Order matters: the last middleware added is the outermost.
  app.add_middleware(StructuredLoggingMiddleware)
  app.add_middleware(SecurityLoggingMiddleware)
  app.add_middleware(DatabaseSessionMiddleware)
  app.add_middleware(RateLimitHeaderMiddleware)

  # Request-level metrics for /extensions/{graph_id}/graphql (resolver spans
  # come from the Strawberry OTel extension).
  @app.middleware("http")
  async def extensions_graphql_metrics_middleware(request: Request, call_next):
    path = request.url.path
    if not (path.startswith("/extensions/") and path.endswith("/graphql")):
      return await call_next(request)

    try:
      graph_id = path.split("/", 3)[2]
    except IndexError:  # pragma: no cover - path matcher already checked shape
      graph_id = None

    # Normalized label bounds Prometheus cardinality; the tenant goes on the event.
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

  @app.middleware("http")
  async def security_headers_middleware(request: Request, call_next):
    """Add security headers to all responses."""
    response = await call_next(request)

    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"

    # The reference is published on the app's domain; the spec (read by SDK
    # generators) and Swagger must not compete with it in search indexes.
    path = request.url.path
    if path in ("/", "/openapi.json") or path.startswith("/static"):
      response.headers["X-Robots-Tag"] = "noindex"

    if env.ENVIRONMENT in ["prod", "staging"]:
      response.headers["Strict-Transport-Security"] = (
        "max-age=31536000; includeSubDomains"
      )

    csp_variant = csp_variant_for_path(path, graphiql_enabled=env.is_development())
    if csp_variant == "docs":
      # Swagger injects inline <style> at runtime; scripts need no inline.
      csp_directives = [
        "default-src 'self'",
        "script-src 'self'",
        "style-src 'self' 'unsafe-inline'",
        "img-src 'self' data: blob:",
        "font-src 'self' data:",
        "connect-src 'self'",
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
      csp_directives = [
        "default-src 'self'",
        "script-src 'self'",
        "style-src 'self'",
        "img-src 'self' data:",
        "connect-src 'self'",
        "object-src 'none'",
        "frame-ancestors 'none'",
        "base-uri 'self'",
        "form-action 'self'",
      ]

    response.headers["Content-Security-Policy"] = "; ".join(csp_directives)

    # By path prefix: a StreamingResponse has no body to scan.
    if any(path.startswith(p) for p in _SENSITIVE_PATH_PREFIXES):
      response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, private"
      response.headers["Pragma"] = "no-cache"

    response.headers["Permissions-Policy"] = "geolocation=(), camera=(), microphone=()"

    return response

  def _scim_envelope(detail: Any, status_code: int) -> dict[str, Any]:
    """RFC 7644 §3.12 error body. A dict that already carries ``schemas``
    passes through verbatim; anything else is wrapped."""
    from robosystems.models.api.scim import ERROR_SCHEMA

    if isinstance(detail, dict) and detail.get("schemas"):
      return detail
    return {
      "schemas": [ERROR_SCHEMA],
      "detail": str(detail),
      "status": str(status_code),
    }

  # 422s use the ErrorResponse shape, not FastAPI's list-detail shape: two
  # shapes for one status code break SDK response parsers.
  @app.exception_handler(RequestValidationError)
  async def request_validation_handler(
    request: Request, exc: RequestValidationError
  ) -> JSONResponse:
    request_id = getattr(request.state, "request_id", None)
    parts = []
    for err in exc.errors():
      loc = ".".join(str(p) for p in err.get("loc", []) if p != "body")
      msg = err.get("msg", "validation error")
      parts.append(f"{loc}: {msg}" if loc else msg)
    detail = "; ".join(parts) or "Request validation failed"
    # SCIM clients parse only the RFC 7644 error envelope, and the RFC maps
    # malformed requests to 400 invalidValue, not FastAPI's 422 shape.
    if request.url.path.startswith("/scim/v2"):
      body = _scim_envelope(detail, status.HTTP_400_BAD_REQUEST)
      body["scimType"] = "invalidValue"
      return JSONResponse(status_code=status.HTTP_400_BAD_REQUEST, content=body)
    return JSONResponse(
      status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
      content={
        "detail": detail,
        "code": "VALIDATION_ERROR",
        "request_id": request_id,
      },
    )

  # Starlette's `{"detail": ...}` plus request_id. A dict detail is wrapped,
  # never spread (callers read response["detail"]). SCIM is the exception:
  # IdPs parse the RFC 7644 envelope at the top level.
  @app.exception_handler(StarletteHTTPException)
  async def http_exception_handler(
    request: Request, exc: StarletteHTTPException
  ) -> JSONResponse:
    request_id = getattr(request.state, "request_id", None)
    if request.url.path.startswith("/scim/v2"):
      return JSONResponse(
        status_code=exc.status_code,
        content=_scim_envelope(exc.detail, exc.status_code),
        headers=getattr(exc, "headers", None),
      )
    return JSONResponse(
      status_code=exc.status_code,
      content={"detail": exc.detail, "request_id": request_id},
      headers=getattr(exc, "headers", None),
    )

  @app.exception_handler(Exception)
  async def global_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """Log the details server-side; the client gets a generic message and the
    request id.
    """
    request_id = getattr(request.state, "request_id", None)

    try:
      logger.error(
        "Unhandled exception", extra={"request_id": request_id}, exc_info=True
      )
    except Exception:
      pass  # the handler must never fail

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
  # Runtime-gated on MCP_OAUTH_ENABLED (404 when off), not import-gated, so the
  # posture is testable.
  app.include_router(mcp_agnostic_router_v1)
  app.include_router(oauth_router)

  # SCIM 2.0 provisioning
  if env.SCIM_ENABLED:
    from robosystems.routers.scim import router as scim_router

    app.include_router(scim_router)

  # Extensions GraphQL endpoint; see robosystems/graphql/README.md.
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
      tags=["GraphQL"],
      include_in_schema=True,
      dependencies=[_Depends(subscription_aware_rate_limit_dependency)],
    )

  # Extensions operations. Mount order is the published reference's order
  # within a tag: commands, then the two routers sharing the Analytical Views tag.
  if env.ROBOLEDGER_ENABLED:
    from robosystems.routers.extensions.roboledger.operations import (
      router as roboledger_operations_router,
    )

    app.include_router(
      roboledger_operations_router,
      prefix="/extensions/roboledger/{graph_id}/operations",
      include_in_schema=True,
    )

  # Independent of ROBOLEDGER_ENABLED so SEC-only deployments get the views.
  if env.FACT_GRID_ENABLED:
    from robosystems.routers.extensions.roboledger.views import (
      router as roboledger_views_router,
    )

    app.include_router(
      roboledger_views_router,
      prefix="/extensions/roboledger/{graph_id}/operations",
      include_in_schema=True,
    )

  # The OLTP-backed analytical read: same tag, mounted after the views, and
  # needs a ledger, so it stays on ROBOLEDGER_ENABLED.
  if env.ROBOLEDGER_ENABLED:
    from robosystems.routers.extensions.roboledger.reads import (
      router as roboledger_reads_router,
    )

    app.include_router(
      roboledger_reads_router,
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
  app.include_router(admin_scim_router, include_in_schema=False)
  app.include_router(admin_oauth_router, include_in_schema=False)

  def custom_openapi():
    """Generate the OpenAPI schema, then layer on this API's own conventions.

    On top of FastAPI's output: security schemes, the shared `OperationError`
    component and error responses applied to every operation route, the
    `Idempotency-Key` header, rate-limit response headers, and tag ordering
    from `openapi_tags`. Cached on `app.openapi_schema` after the first call.
    """
    if app.openapi_schema:
      return app.openapi_schema

    openapi_schema = get_openapi(
      title=app.title,
      version=app.version,
      description=app.description,
      routes=app.routes,
    )

    if "components" not in openapi_schema:
      openapi_schema["components"] = {}

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

    if "schemas" not in openapi_schema["components"]:
      openapi_schema["components"]["schemas"] = {}

    # Shared error shape for operation routes, so SDK codegen emits a typed
    # error union.
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

    # Injected into every operation route below.
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

    _GRAPHQL_SURFACE_NOTE = (
      "Queries are scoped by the URL: `graph_id` is a path parameter and "
      "never a query argument, so a document cannot name a graph that "
      "disagrees with the path it was sent to. Reads hit the operational "
      "(OLTP) extensions database, so they reflect the books as they stand "
      "now; the analytical projection is Cypher at "
      "`POST /v1/graphs/{graph_id}/query/cypher`."
      "\n\nThe schema is composed per deployment: ledger fields require "
      "RoboLedger and investor fields require RoboInvestor, and a disabled "
      "domain is absent from introspection rather than failing at runtime. "
      "Every field carries a description, so introspection is the "
      "authoritative, deployment-specific reference."
      "\n\n**Auth**: pass `X-API-Key` (or a JWT `Authorization: Bearer` "
      "header). Unauthenticated introspection queries are deliberately "
      "allowed for SDK codegen; data queries require credentials and raise "
      "`UNAUTHENTICATED`."
      "\n\n**Error codes**: `LEDGER_NOT_INITIALIZED`, "
      "`INVESTOR_NOT_INITIALIZED`, and `UNAUTHENTICATED` surface in the "
      "GraphQL `errors[].extensions.code` field. GraphQL reports errors with "
      "HTTP 200 and a populated `errors[]`, so check that array rather than "
      "the status code."
    )

    _GRAPHQL_POST_DESCRIPTION = (
      "The typed read surface for a graph's extensions data — RoboLedger and "
      "RoboInvestor records as they stand right now. Writes are not here: "
      "they are the named operations at "
      "`POST /extensions/{domain}/{graph_id}/operations/{name}`."
      "\n\nSend a standard GraphQL POST body: a `query` document, with "
      "optional `variables` and `operationName`."
      "\n\n" + _GRAPHQL_SURFACE_NOTE
    )

    _GRAPHQL_GET_DESCRIPTION = (
      "Serves the in-browser GraphiQL explorer on deployments that enable it, "
      "which is development only — it is not mounted on the hosted API. Run "
      "queries with `POST` to the same URL."
      "\n\n" + _GRAPHQL_SURFACE_NOTE
    )

    def _graphql_request_body() -> dict:
      return {
        "required": True,
        "content": {
          "application/json": {
            "schema": {
              "type": "object",
              "required": ["query"],
              "properties": {
                "query": {
                  "type": "string",
                  "description": "The GraphQL document to execute.",
                },
                "variables": {
                  "type": "object",
                  "additionalProperties": True,
                  "description": "Values for the document's variables.",
                },
                "operationName": {
                  "type": "string",
                  "description": (
                    "Which operation to run, when the document declares more than one."
                  ),
                },
              },
            },
            "examples": {
              "fiscal_calendar": {
                "summary": "What is blocking the close",
                "value": {
                  "query": (
                    "{ fiscalCalendar { closedThrough closeTarget "
                    "closeableNow blockers } }"
                  )
                },
              },
              "paginated_with_variables": {
                "summary": "A filtered, paginated list",
                "value": {
                  "query": (
                    "query Agents($type: String, $limit: Int) { "
                    "agents(agentType: $type, limit: $limit) { id name } }"
                  ),
                  "variables": {"type": "customer", "limit": 10},
                },
              },
              "introspect": {
                "summary": "Discover this deployment's fields",
                "value": {
                  "query": (
                    "{ __schema { queryType { fields { name description } } } }"
                  )
                },
              },
            },
          }
        },
      }

    def _graphql_response_content() -> dict:
      return {
        "application/json": {
          "schema": {
            "type": "object",
            "properties": {
              "data": {
                "type": "object",
                "additionalProperties": True,
                "nullable": True,
                "description": "The query result, shaped like the document.",
              },
              "errors": {
                "type": "array",
                "description": (
                  "Present when the query failed in whole or in part. Each "
                  "entry carries `message`, `path`, and "
                  "`extensions.code`."
                ),
                "items": {"type": "object", "additionalProperties": True},
              },
            },
          }
        }
      }

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

          # Strawberry's handlers are plain routes, so FastAPI would publish them
          # as "Handle Http Post" with no request body.
          if _method_name == "post":
            _operation["summary"] = "Run a GraphQL query"
            _operation["description"] = _GRAPHQL_POST_DESCRIPTION
            _operation.setdefault("requestBody", _graphql_request_body())
            _ok = existing_responses.setdefault(
              "200", {"description": "GraphQL response"}
            )
            if isinstance(_ok, dict):
              _ok.setdefault("content", _graphql_response_content())
          else:
            _operation["summary"] = "GraphQL explorer (development only)"
            _operation["description"] = _GRAPHQL_GET_DESCRIPTION

    public_exact_paths = {"/v1/status"}
    public_prefixes = ("/v1/auth", "/v1/offering")

    for path, methods in openapi_schema.get("paths", {}).items():
      if path in public_exact_paths or any(path.startswith(p) for p in public_prefixes):
        continue
      for _method_name, operation in methods.items():
        operation["security"] = [{"APIKeyHeader": []}, {"BearerAuth": []}]

    # openapi_tags order, emitting only tags in use.
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
