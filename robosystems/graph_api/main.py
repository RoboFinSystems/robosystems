"""
Main entry point for the Graph API server.

This module handles command-line arguments and server initialization.
"""

import argparse
from pathlib import Path

import uvicorn

from robosystems.graph_api.app import create_app
from robosystems.graph_api.core import init_cluster_service
from robosystems.middleware.graph.types import NodeType, RepositoryType
from robosystems.logger import logger


def main():
  """Main entry point for the cluster server."""
  parser = argparse.ArgumentParser(description="Graph API Server")

  # Get default base path from environment
  from robosystems.config import env

  default_base_path = env.LBUG_DATABASE_PATH

  parser.add_argument(
    "--base-path",
    default=default_base_path,
    help=f"Base directory for LadybugDB databases (default: {default_base_path})",
  )
  parser.add_argument("--port", type=int, default=8001, help="Port to run server on")
  parser.add_argument("--host", default="0.0.0.0", help="Host to bind server to")
  parser.add_argument(
    "--max-databases",
    type=int,
    default=200,
    help="Maximum number of databases on this node",
  )
  parser.add_argument(
    "--read-only", action="store_true", help="Run in read-only mode (reader node)"
  )
  parser.add_argument(
    "--node-type",
    choices=["writer", "shared_master", "shared_replica"],
    default="writer",
    help="Type of node (writer, shared_master, shared_replica)",
  )
  parser.add_argument(
    "--repository-type",
    choices=["entity", "shared"],
    default="entity",
    help="Repository type (entity, shared)",
  )
  parser.add_argument(
    "--log-level",
    choices=["debug", "info", "warning", "error", "critical"],
    default="info",
    help="Logging level",
  )
  parser.add_argument(
    "--workers", type=int, default=1, help="Number of worker processes"
  )

  args = parser.parse_args()

  # Ensure base path exists
  base_path = Path(args.base_path).resolve()
  base_path.mkdir(parents=True, exist_ok=True)

  # Map string node type to enum
  node_type_map = {
    "writer": NodeType.WRITER,
    "shared_master": NodeType.SHARED_MASTER,
    "shared_replica": NodeType.SHARED_REPLICA,
  }
  node_type = node_type_map[args.node_type]

  # Map string repository type to enum
  repository_type_map = {
    "entity": RepositoryType.ENTITY,
    "shared": RepositoryType.SHARED,
  }
  repository_type = repository_type_map[args.repository_type]

  # Validate node/repository type combinations
  if node_type == NodeType.WRITER and repository_type != RepositoryType.ENTITY:
    parser.error("Writer nodes must use entity repository type")
  elif node_type in [NodeType.SHARED_MASTER, NodeType.SHARED_REPLICA]:
    if repository_type != RepositoryType.SHARED:
      parser.error("Shared nodes must use shared repository type")

  # Get max_databases from tier configuration if available
  max_databases = args.max_databases
  try:
    # Get tier from environment using centralized config
    cluster_tier = env.CLUSTER_TIER
    if cluster_tier and cluster_tier != "unknown":
      tier_config = env.get_lbug_tier_config()
      # Use databases_per_instance from tier config if available
      max_databases = tier_config.get("databases_per_instance", args.max_databases)
      logger.info(
        f"Loaded max_databases={max_databases} from tier config for tier={cluster_tier}"
      )
  except Exception as e:
    logger.warning(f"Could not load tier config, using default: {e}")

  # Initialize the cluster service
  init_cluster_service(
    base_path=str(base_path),
    max_databases=max_databases,
    read_only=args.read_only,
    node_type=node_type,
    repository_type=repository_type,
  )

  # Create the FastAPI app
  app = create_app()

  # Configure logging
  import logging

  logging.basicConfig(level=getattr(logging, args.log_level.upper()))

  # Display startup info
  logger.info(f"Starting Graph API Server v{app.version}")
  logger.info(f"Base path: {base_path}")
  logger.info(
    f"Node type: {node_type.value} ({'read-only' if args.read_only else 'read-write'})"
  )
  logger.info(f"Repository type: {repository_type.value}")
  logger.info(f"Max databases: {max_databases}")
  logger.info(f"Host: {args.host}:{args.port}")
  logger.info(f"Worker processes: {args.workers}")

  # Run the server
  uvicorn.run(
    app,
    host=args.host,
    port=args.port,
    workers=args.workers,
    log_level=args.log_level,
    access_log=True,
  )


if __name__ == "__main__":
  main()
