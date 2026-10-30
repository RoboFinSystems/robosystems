"""Graph API application factory: builds the FastAPI app, routers, middleware."""

import os
from collections.abc import Callable
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

from robosystems.config import env
from robosystems.config.openapi_tags import GRAPH_API_TAGS
from robosystems.graph_api.routers import (
  databases,
  health,
  info,
  metrics,
  migration,
  tasks,
)
from robosystems.logger import logger

setup_telemetry: Callable[[FastAPI], None] | None = None
try:
  from robosystems.middleware.otel import setup_telemetry
except ImportError:
  pass

try:
  from importlib.metadata import version

  __version__ = version("robosystems")
except Exception:
  __version__ = "1.0.0"


def create_app() -> FastAPI:
  """Create FastAPI application with all endpoints."""

  node_type = env.LBUG_NODE_TYPE
  if env.OTEL_ENABLED:
    os.environ["OTEL_SERVICE_NAME"] = f"graph-api-{node_type}"

  @asynccontextmanager
  async def lifespan(app: FastAPI):
    """Manage application lifespan events."""
    import asyncio

    logger.info("Graph API starting up")

    # Replica warmup: read the working set the tools read before /health
    # answers 200 (core/ladybug/warmup.py). The engine opens lazily, so until
    # this runs every structure is paged in from the volume by the first caller.
    if os.getenv("LBUG_ROLE") == "replica":

      async def warmup_local_databases():
        from robosystems.config.tuning import TuningConfig
        from robosystems.graph_api.core.ladybug import get_ladybug_service
        from robosystems.graph_api.core.ladybug.warmup import run_warmup
        from robosystems.graph_api.routers.health import mark_replica_ready

        def _do_warmup():
          service = get_ladybug_service()
          databases = service.db_manager.list_databases()
          budget_seconds = TuningConfig.get_replica_warmup_budget_seconds()
          tickers = TuningConfig.get_replica_warmup_tickers()
          logger.info(
            f"Warming up {len(databases)} local databases "
            f"(budget {budget_seconds}s each, {len(tickers)} filers)..."
          )
          for db_name in databases:
            run_warmup(
              service.db_manager,
              db_name,
              budget_seconds=budget_seconds,
              tickers=tickers,
            )

        try:
          await asyncio.to_thread(_do_warmup)
          mark_replica_ready()
          logger.info("Replica warmup complete - ready for traffic")
        except Exception as e:
          logger.error(f"Replica warmup failed: {e}")

      asyncio.create_task(warmup_local_databases())

    # Extensions cached on the persistent volume survive container image
    # updates and then fail with ABI mismatches (undefined symbols). Clearing
    # the cache at startup forces LadybugDB to load the image's extensions.
    import shutil

    cache_paths_to_clear = [
      # LadybugDB 0.15+ convention: cache at base_path/.lbdb/extension
      Path(env.LBUG_DATABASE_PATH) / ".lbdb" / "extension",
      # Pre-0.15 path: home_dir=base_path, cache at base_path/.lbug/extension
      Path(env.LBUG_DATABASE_PATH) / ".lbug" / "extension",
      # Legacy double-nested path: home_dir=base_path/.lbug, cache at base_path/.lbug/.lbug/extension
      Path(env.LBUG_DATABASE_PATH) / ".lbug" / ".lbug" / "extension",
    ]
    for cache_path in cache_paths_to_clear:
      if cache_path.exists():
        try:
          shutil.rmtree(cache_path)
          logger.info(f"Cleared stale LadybugDB extension cache: {cache_path}")
        except Exception as e:
          logger.warning(f"Failed to clear extension cache at {cache_path}: {e}")

    # Connections are lazy: replicas download staging files after startup.
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

    yield

    logger.info("Graph API shutting down")
    if duckdb_pool is not None:
      duckdb_pool.close_all_connections()
      logger.info("Closed all DuckDB connections")

  base_dir = Path(__file__).parent.parent.parent  # project root
  description_file = base_dir / "static" / "graph-api-description.md"
  api_description = (
    description_file.read_text()
    if description_file.exists()
    else "RoboSystems multi-database graph cluster management API"
  )

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

  # Document the API key security scheme in the published OpenAPI spec.
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

      openapi_schema["components"]["securitySchemes"] = {
        "ApiKeyHeader": {
          "type": "apiKey",
          "in": "header",
          "name": "X-Graph-API-Key",
          "description": "API key for authentication. Required in production/staging environments.",
        }
      }

      exempt_paths = {"/health"}
      for path, methods in openapi_schema["paths"].items():
        if path not in exempt_paths:
          for method in methods.values():
            if isinstance(method, dict):
              method["security"] = [{"ApiKeyHeader": []}]

      app.openapi_schema = openapi_schema
      return app.openapi_schema

    app.openapi = custom_openapi

  # CORS is deliberately restrictive: this API is VPC-internal.
  lbug_cors_origins = env.get_lbug_cors_origins()
  app.add_middleware(
    CORSMiddleware,
    allow_origins=lbug_cors_origins,
    allow_credentials=False,  # No credentials needed for server-to-server API
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization", "X-API-Key", "X-Graph-API-Key"],
  )

  try:
    from .middleware import RequestSizeLimitMiddleware

    app.add_middleware(RequestSizeLimitMiddleware)
    logger.info("Request size limit middleware enabled")
  except Exception as e:
    logger.warning(f"Request size limit middleware not loaded: {e}")

  # Authentication is enforced in prod/staging only.
  try:
    from .middleware import LadybugAuthMiddleware
    from .middleware.auth import read_graph_api_keys

    app.add_middleware(LadybugAuthMiddleware, key_source=read_graph_api_keys)
    logger.info("LadybugDB authentication middleware enabled")
  except Exception as e:
    logger.warning(f"LadybugDB authentication middleware not loaded: {e}")

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

  static_dir = base_dir / "static"
  if static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

  @app.get("/", response_class=HTMLResponse, include_in_schema=False)
  async def custom_docs():
    """Custom Swagger docs with dark theme."""
    try:
      from robosystems.utils.docs_template import generate_lbug_docs

      return HTMLResponse(content=generate_lbug_docs())
    except ImportError:
      from fastapi.openapi.docs import get_swagger_ui_html

      return get_swagger_ui_html(openapi_url="/openapi.json", title=api_title)

  @app.get("/docs", response_class=HTMLResponse, include_in_schema=False)
  async def custom_redoc():
    """Custom ReDoc with dark theme."""
    try:
      from robosystems.utils.docs_template import generate_redoc_docs

      return HTMLResponse(content=generate_redoc_docs(title="RoboSystems Graph API"))
    except ImportError:
      from fastapi.openapi.docs import get_redoc_html

      return get_redoc_html(openapi_url="/openapi.json", title=api_title)

  app.include_router(health.router)
  app.include_router(info.router)
  app.include_router(metrics.router)

  app.include_router(databases.management.router)
  app.include_router(databases.query.router)
  app.include_router(databases.schema.router)
  app.include_router(databases.backup.router)
  app.include_router(databases.restore.router)
  app.include_router(databases.metrics.router)
  app.include_router(databases.memory.router)
  app.include_router(databases.vector_search.router)
  app.include_router(databases.semantic_memory.router)
  app.include_router(databases.swap.router)

  app.include_router(tasks.router)
  app.include_router(migration.router)

  app.include_router(databases.tables.router)

  return app
