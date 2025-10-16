from typing import Optional, Union
from robosystems.config import env
from .kuzu import KuzuBackend
from .neo4j import Neo4jBackend
from robosystems.logger import logger


_backend_instance: Optional[Union[KuzuBackend, Neo4jBackend]] = None


def get_backend() -> Union[KuzuBackend, Neo4jBackend]:
  global _backend_instance

  if _backend_instance is None:
    backend_type = env.BACKEND_TYPE

    if backend_type == "kuzu":
      _backend_instance = KuzuBackend()
      logger.info("Initialized Kuzu backend (Standard tier)")
    elif backend_type == "neo4j_community":
      _backend_instance = Neo4jBackend(enterprise=False)
      logger.info("Initialized Neo4j Community backend (Professional/Enterprise tiers)")
    elif backend_type == "neo4j_enterprise":
      _backend_instance = Neo4jBackend(enterprise=True)
      logger.info("Initialized Neo4j Enterprise backend (Premium tier)")
    else:
      raise ValueError(f"Unknown BACKEND_TYPE: {backend_type}")

  return _backend_instance


__all__ = ["get_backend", "KuzuBackend", "Neo4jBackend", "GraphBackend"]
