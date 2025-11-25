"""
Graph API FastAPI application setup.

This module creates the FastAPI application with all routers and middleware.
"""

import os
from pathlib import Path
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse

from robosystems.graph_api.routers import (
  databases,
  health,
  info,
  tasks,
  metrics,
)
from robosystems.config import env
from robosystems.config.openapi_tags import GRAPH_API_TAGS
from robosystems.logger import logger

# OpenTelemetry import - conditional based on OTEL_ENABLED
from typing import Optional, Callable

setup_telemetry: Optional[Callable[[FastAPI], None]] = None
try:
  from robosystems.middleware.otel import setup_telemetry
except ImportError:
  pass

try:
  from importlib.metadata import version

  __version__ = version("robosystems-service")
except Exception:
  __version__ = "1.0.0"


def create_app() -> FastAPI:
  """Create FastAPI application with all endpoints."""

  # Set appropriate service name based on node type (only if OTEL is enabled)
  node_type = env.LBUG_NODE_TYPE
  if env.OTEL_ENABLED:
    os.environ["OTEL_SERVICE_NAME"] = f"graph-api-{node_type}"

  @asynccontextmanager
  async def lifespan(app: FastAPI):
    """Manage application lifespan events."""
    # Startup
    logger.info("Graph API starting up")

    # Initialize DuckDB connection pool for staging tables
    from robosystems.graph_api.core.duckdb import initialize_duckdb_pool

    duckdb_base_path = Path(env.DUCKDB_STAGING_PATH)
    duckdb_pool = initialize_duckdb_pool(
      base_path=str(duckdb_base_path),
      max_connections_per_db=3,
      connection_ttl_minutes=30,
    )
    logger.info(
      f"Initialized DuckDB connection pool at {duckdb_base_path} "
      "(databases persist with graph lifecycle)"
    )

    yield  # Application runs here

    # Shutdown
    logger.info("Graph API shutting down")
    duckdb_pool.close_all_connections()
    logger.info("Closed all DuckDB connections")

  # Load description from markdown file
  base_dir = Path(__file__).parent.parent.parent  # Go up to project root
  description_file = base_dir / "static" / "graph-api-description.md"
  api_description = (
    description_file.read_text()
    if description_file.exists()
    else "RoboSystems multi-database graph cluster management API"
  )

  # Generate title based on node type
  api_title = "RoboSystems Graph API"

  app = FastAPI(
    title=api_title,
    description=api_description,
    version=__version__,
    lifespan=lifespan,
    docs_url=None,  # Using custom docs route instead
    redoc_url=None,  # Disable default ReDoc to use custom
    openapi_url="/openapi.json",
    openapi_tags=GRAPH_API_TAGS,
  )

  # Add API key security scheme to OpenAPI documentation
  if app.openapi_schema is None:
    from fastapi.openapi.utils import get_openapi

    def custom_openapi():
      if app.openapi_schema:
        return app.openapi_schema

      openapi_schema = get_openapi(
        title=app.title,
        version=app.version,
        description=app.description,
        routes=app.routes,
        tags=app.openapi_tags,
      )

      # Add security scheme for API key
      openapi_schema["components"]["securitySchemes"] = {
        "ApiKeyHeader": {
          "type": "apiKey",
          "in": "header",
          "name": "X-Graph-API-Key",
          "description": "API key for authentication. Required in production/staging environments.",
        }
      }

      # Apply security to all endpoints except exempt ones
      exempt_paths = {
        "/health",
        "/status",
        "/info",
        "/metrics",
        "/openapi.json",
        "/docs",
        "/redoc",
        "/",
      }
      for path, methods in openapi_schema["paths"].items():
        if path not in exempt_paths:
          for method in methods.values():
            if isinstance(method, dict):
              method["security"] = [{"ApiKeyHeader": []}]

      app.openapi_schema = openapi_schema
      return app.openapi_schema

    app.openapi = custom_openapi

  # Add CORS middleware - restrictive for VPC-internal API
  lbug_cors_origins = env.get_lbug_cors_origins()
  app.add_middleware(
    CORSMiddleware,
    allow_origins=lbug_cors_origins,
    allow_credentials=False,  # No credentials needed for server-to-server API
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization", "X-API-Key", "X-Graph-API-Key"],
  )

  # Add request size limit middleware
  try:
    from .middleware import RequestSizeLimitMiddleware

    app.add_middleware(RequestSizeLimitMiddleware)
    logger.info("Request size limit middleware enabled")
  except Exception as e:
    logger.warning(f"Request size limit middleware not loaded: {e}")

  # Add authentication middleware (only for prod/staging)
  try:
    from .middleware import LadybugAuthMiddleware

    app.add_middleware(LadybugAuthMiddleware)
    logger.info("LadybugDB authentication middleware enabled")
  except Exception as e:
    logger.warning(f"LadybugDB authentication middleware not loaded: {e}")

  # Setup OpenTelemetry instrumentation (only if enabled)
  if env.OTEL_ENABLED and setup_telemetry is not None:
    logger.info("OpenTelemetry is ENABLED for Graph API")
    # Suppress noisy OTEL logs
    import logging

    logging.getLogger("opentelemetry").setLevel(logging.WARNING)
    logging.getLogger("opentelemetry.sdk").setLevel(logging.WARNING)
    logging.getLogger("opentelemetry.exporter").setLevel(logging.ERROR)
    logging.getLogger("opentelemetry.instrumentation").setLevel(logging.WARNING)
    setup_telemetry(app)
  else:
    logger.info("OpenTelemetry is DISABLED for Graph API - no metrics collection")

  # Mount static files from main static directory
  static_dir = base_dir / "static"
  if static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

  # Custom docs route with dark theme at root
  @app.get("/", response_class=HTMLResponse, include_in_schema=False)
  async def custom_docs():
    """Custom Swagger docs with dark theme."""
    try:
      from robosystems.utils.docs_template import generate_lbug_docs

      return HTMLResponse(content=generate_lbug_docs())
    except ImportError:
      # Fallback to default FastAPI docs
      from fastapi.openapi.docs import get_swagger_ui_html

      return get_swagger_ui_html(openapi_url="/openapi.json", title=api_title)

  # Custom ReDoc route with dark theme
  @app.get("/docs", response_class=HTMLResponse, include_in_schema=False)
  async def custom_redoc():
    """Custom ReDoc with dark theme."""
    try:
      from robosystems.utils.docs_template import generate_redoc_docs

      return HTMLResponse(content=generate_redoc_docs(title="RoboSystems Graph API"))
    except ImportError:
      # Fallback to default ReDoc
      from fastapi.openapi.docs import get_redoc_html

      return get_redoc_html(openapi_url="/openapi.json", title=api_title)

  # Include all routers
  app.include_router(health.router)  # /health for load balancer health checks
  app.include_router(info.router)  # /info for comprehensive cluster info
  app.include_router(metrics.router)  # /metrics for monitoring systems

  # Database routers
  app.include_router(databases.management.router)
  app.include_router(databases.query.router)
  app.include_router(databases.schema.router)
  app.include_router(databases.backup.router)
  app.include_router(databases.restore.router)
  app.include_router(
    databases.copy.router
  )  # Direct S3 → LadybugDB copy (legacy/internal for SEC workers)
  app.include_router(databases.metrics.router)

  # Task management (generic for all task types)
  app.include_router(tasks.router)

  # Table routers (DuckDB staging) - now database-scoped under /databases/{graph_id}/tables
  app.include_router(databases.tables.router)

  # Volume management (only on EC2)

  return app
