from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any


class S3IngestionError(Exception):
  pass


@dataclass
class DatabaseInfo:
  name: str
  node_count: int
  relationship_count: int
  size_bytes: int


@dataclass
class ClusterTopology:
  mode: str
  leader: dict[str, Any] | None = None
  followers: list[dict[str, Any]] | None = None
  members: list[dict[str, Any]] | None = None


class GraphBackend(ABC):
  @abstractmethod
  async def execute_query(
    self,
    graph_id: str,
    cypher: str,
    parameters: dict[str, Any] | None = None,
    database: str | None = None,
  ) -> list[dict[str, Any]]:
    pass

  @abstractmethod
  async def execute_write(
    self,
    graph_id: str,
    cypher: str,
    parameters: dict[str, Any] | None = None,
    database: str | None = None,
  ) -> list[dict[str, Any]]:
    pass

  @abstractmethod
  async def create_database(self, database_name: str) -> bool:
    pass

  @abstractmethod
  async def delete_database(self, database_name: str) -> bool:
    pass

  @abstractmethod
  async def list_databases(self) -> list[str]:
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
  async def ingest_from_s3(
    self,
    graph_id: str,
    table_name: str,
    s3_pattern: str,
    s3_credentials: dict[str, Any] | None = None,
    ignore_errors: bool = True,
    database: str | None = None,
  ) -> dict[str, Any]:
    pass

  @abstractmethod
  async def close(self) -> None:
    pass
