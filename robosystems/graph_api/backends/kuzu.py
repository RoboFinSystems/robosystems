from typing import Dict, Any, List, Optional
from pathlib import Path
import shutil

from robosystems.middleware.graph.engine import Engine
from robosystems.logger import logger
from .base import GraphBackend, DatabaseInfo, ClusterTopology


class KuzuBackend(GraphBackend):
  def __init__(self, data_path: str = "/data/kuzu-dbs"):
    self.data_path = data_path
    self._engines: Dict[str, Engine] = {}
    Path(data_path).mkdir(parents=True, exist_ok=True)

  def _get_engine(self, graph_id: str) -> Engine:
    if graph_id not in self._engines:
      database_path = f"{self.data_path}/{graph_id}"
      self._engines[graph_id] = Engine(database_path)
      logger.debug(f"Created Kuzu engine for {graph_id}: {database_path}")
    return self._engines[graph_id]

  async def execute_query(
    self,
    graph_id: str,
    cypher: str,
    parameters: Optional[Dict[str, Any]] = None,
    database: Optional[str] = None,
  ) -> List[Dict[str, Any]]:
    engine = self._get_engine(graph_id)
    return engine.execute_query(cypher, parameters)

  async def execute_write(
    self,
    graph_id: str,
    cypher: str,
    parameters: Optional[Dict[str, Any]] = None,
    database: Optional[str] = None,
  ) -> List[Dict[str, Any]]:
    engine = self._get_engine(graph_id)
    return engine.execute_query(cypher, parameters)

  async def create_database(self, database_name: str) -> bool:
    self._get_engine(database_name)
    logger.info(f"Created Kuzu database for {database_name}")
    return True

  async def delete_database(self, database_name: str) -> bool:
    if database_name in self._engines:
      self._engines[database_name].close()
      del self._engines[database_name]

    db_path = Path(f"{self.data_path}/{database_name}")
    if db_path.exists():
      shutil.rmtree(db_path)
      logger.info(f"Deleted Kuzu database for {database_name}")
    return True

  async def list_databases(self) -> List[str]:
    db_path = Path(self.data_path)
    if not db_path.exists():
      return []
    return [d.name for d in db_path.iterdir() if d.is_dir()]

  async def get_database_info(self, database_name: str) -> DatabaseInfo:
    engine = self._get_engine(database_name)

    node_count = 0
    relationship_count = 0

    try:
      node_result = engine.execute_single("MATCH (n) RETURN count(n) as count")
      if node_result:
        node_count = node_result.get("count", 0)

      rel_result = engine.execute_single("MATCH ()-[r]->() RETURN count(r) as count")
      if rel_result:
        relationship_count = rel_result.get("count", 0)
    except Exception as e:
      logger.warning(f"Failed to get database stats for {database_name}: {e}")

    db_path = Path(f"{self.data_path}/{database_name}")
    size_bytes = (
      sum(f.stat().st_size for f in db_path.rglob("*") if f.is_file())
      if db_path.exists()
      else 0
    )

    return DatabaseInfo(
      name=database_name,
      node_count=node_count,
      relationship_count=relationship_count,
      size_bytes=size_bytes,
    )

  async def get_cluster_topology(self) -> ClusterTopology:
    return ClusterTopology(mode="embedded", leader={"backend": "kuzu"})

  async def health_check(self) -> bool:
    return True

  async def close(self) -> None:
    for engine in self._engines.values():
      engine.close()
    self._engines.clear()
