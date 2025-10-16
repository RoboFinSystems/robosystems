from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
from dataclasses import dataclass


@dataclass
class DatabaseInfo:
  name: str
  node_count: int
  relationship_count: int
  size_bytes: int


@dataclass
class ClusterTopology:
  mode: str
  leader: Optional[Dict[str, Any]] = None
  followers: Optional[List[Dict[str, Any]]] = None
  members: Optional[List[Dict[str, Any]]] = None


class GraphBackend(ABC):
  @abstractmethod
  async def execute_query(
    self,
    graph_id: str,
    cypher: str,
    parameters: Optional[Dict[str, Any]] = None,
    database: Optional[str] = None,
  ) -> List[Dict[str, Any]]:
    pass

  @abstractmethod
  async def execute_write(
    self,
    graph_id: str,
    cypher: str,
    parameters: Optional[Dict[str, Any]] = None,
    database: Optional[str] = None,
  ) -> List[Dict[str, Any]]:
    pass

  @abstractmethod
  async def create_database(self, database_name: str) -> bool:
    pass

  @abstractmethod
  async def delete_database(self, database_name: str) -> bool:
    pass

  @abstractmethod
  async def list_databases(self) -> List[str]:
    pass

  @abstractmethod
  async def get_database_info(self, database_name: str) -> DatabaseInfo:
    pass

  @abstractmethod
  async def get_cluster_topology(self) -> ClusterTopology:
    pass

  @abstractmethod
  async def health_check(self) -> bool:
    pass

  @abstractmethod
  def close(self) -> None:
    pass
