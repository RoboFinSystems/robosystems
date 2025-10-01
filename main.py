"""RoboSystems Service API main application module."""

from datetime import datetime, timezone
from importlib.metadata import version as pkg_version
from pathlib import Path

from fastapi import FastAPI, Request, status
from robosystems.config import env
from robosystems.config.openapi_tags import MAIN_API_TAGS
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles

from robosystems.routers import (
  router as v1_router,
  graph_router,
  user_router_v1,
  auth_router_v1,
  status_router_v1,
  offering_router_v1,
  operations_router_v1,
)
from robosystems.middleware.otel import setup_telemetry
from robosystems.middleware.database import DatabaseSessionMiddleware
from robosystems.middleware.rate_limits import RateLimitHeaderMiddleware
from robosystems.middleware.logging import (
  StructuredLoggingMiddleware,
  SecurityLoggingMiddleware,
)

from robosystems.config.validation import EnvValidator
from robosystems.config.logging import get_logger

logger = get_logger("robosystems.api")


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
    version=pkg_version("robosystems-service"),
    description=api_description,
    docs_url=None,  # Disable default docs
    redoc_url=None,  # Disable default ReDoc to use custom
    openapi_url="/openapi.json",
    openapi_tags=MAIN_API_TAGS,
  )

  # Setup OpenTelemetry
  setup_telemetry(app)

  # Initialize app state
  app.state.current_time = datetime.now(timezone.utc)

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
    allow_credentials=env.CORS_ALLOW_CREDENTIALS,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH"],
    allow_headers=[
      "Accept",
      "Accept-Language",
      "Content-Type",
      "Authorization",
      "X-API-Key",
      "X-Requested-With",
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

    # Path-based CSP - strict for API, relaxed for docs
    path = request.url.path

    if path in ["/", "/docs"] or path.startswith("/static"):
      # Relaxed CSP for documentation and static assets
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

      # Skip trusted types for docs - Swagger UI doesn't support them
      # Trusted types are still enforced for API endpoints for security
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

      # Conditionally add trusted types based on feature flag
      if env.CSP_TRUSTED_TYPES_ENABLED:
        csp_directives.append("require-trusted-types-for 'script'")

    response.headers["Content-Security-Policy"] = "; ".join(csp_directives)

    # Add cache control for sensitive API responses
    if path.startswith("/v1/") and any(
      keyword in str(response.body).lower() if hasattr(response, "body") else False
      for keyword in ["token", "password", "key"]
    ):
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
  app.include_router(
    v1_router
  )  # Now includes sync, agent, and schedule routers as graph-scoped
  app.include_router(graph_router)
  app.include_router(offering_router_v1)
  app.include_router(operations_router_v1)  # Unified SSE operations monitoring

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
