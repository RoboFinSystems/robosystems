from typing import Optional, Union
from robosystems.config import env
from .base import GraphBackend
from .lbug import LadybugBackend
from .neo4j import Neo4jBackend
from robosystems.logger import logger


_backend_instance: Optional[Union[LadybugBackend, Neo4jBackend]] = None


def get_backend() -> Union[LadybugBackend, Neo4jBackend]:
  global _backend_instance

  if _backend_instance is None:
    backend_type = env.GRAPH_BACKEND_TYPE

    if backend_type == "ladybug":
      _backend_instance = LadybugBackend(data_path=env.LBUG_DATABASE_PATH)
      logger.info(
        f"Initialized LadybugDB backend (Standard tier) at {env.LBUG_DATABASE_PATH}"
      )
    elif backend_type == "neo4j_community":
      _backend_instance = Neo4jBackend(enterprise=False)
      logger.info("Initialized Neo4j Community backend (Professional/Enterprise tiers)")
    elif backend_type == "neo4j_enterprise":
      _backend_instance = Neo4jBackend(enterprise=True)
      logger.info("Initialized Neo4j Enterprise backend (Premium tier)")
    else:
      raise ValueError(f"Unknown GRAPH_BACKEND_TYPE: {backend_type}")

  return _backend_instance


__all__ = ["get_backend", "LadybugBackend", "Neo4jBackend", "GraphBackend"]
