from __future__ import annotations

from typing import TYPE_CHECKING

from robosystems.config import env
from robosystems.logger import logger

from .base import GraphBackend
from .lbug import LadybugBackend

if TYPE_CHECKING:
  from .neo4j import Neo4jBackend

_backend_instance: LadybugBackend | Neo4jBackend | None = None


def get_backend() -> LadybugBackend | Neo4jBackend:
  global _backend_instance

  if _backend_instance is None:
    backend_type = env.GRAPH_BACKEND_TYPE

    if backend_type == "ladybug":
      _backend_instance = LadybugBackend(data_path=env.LBUG_DATABASE_PATH)
      logger.info(
        f"Initialized LadybugDB backend (Standard tier) at {env.LBUG_DATABASE_PATH}"
      )
    elif backend_type in ("neo4j_community", "neo4j_enterprise"):
      try:
        from .neo4j import Neo4jBackend
      except ImportError:
        raise ImportError(
          "Neo4j backend requires the 'neo4j' package. "
          "Install it with: uv sync --extra neo4j"
        ) from None

      enterprise = backend_type == "neo4j_enterprise"
      _backend_instance = Neo4jBackend(enterprise=enterprise)
      logger.info(f"Initialized Neo4j backend (enterprise={enterprise})")
    else:
      raise ValueError(f"Unknown GRAPH_BACKEND_TYPE: {backend_type}")

  return _backend_instance


__all__ = ["GraphBackend", "LadybugBackend", "get_backend"]
