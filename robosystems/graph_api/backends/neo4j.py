from typing import Dict, Any, List, Optional
import json
import boto3
from neo4j import AsyncGraphDatabase, AsyncDriver
from robosystems.logger import logger
from robosystems.config import env
from .base import GraphBackend, DatabaseInfo, ClusterTopology


class Neo4jBackend(GraphBackend):
  def __init__(self, enterprise: bool = False):
    self.enterprise = enterprise
    self.bolt_url = env.NEO4J_URI
    self.driver: Optional[AsyncDriver] = None
    self._cluster_topology: Optional[Dict] = None
    self._password: Optional[str] = None
    self._username = env.NEO4J_USERNAME

  async def _ensure_connected(self):
    if self.driver is None:
      await self._connect()

  async def _connect(self):
    if env.NEO4J_PASSWORD is not None and env.NEO4J_PASSWORD != "":
      self._password = env.NEO4J_PASSWORD
    else:
      secrets = boto3.client("secretsmanager", region_name=env.AWS_REGION)
      secret_value = secrets.get_secret_value(
        SecretId=f"robosystems/{env.ENVIRONMENT}/neo4j"
      )
      creds = json.loads(secret_value["SecretString"])
      self._password = creds["password"]

    self.driver = AsyncGraphDatabase.driver(
      self.bolt_url,
      auth=(self._username, self._password),
      max_connection_lifetime=env.NEO4J_MAX_CONNECTION_LIFETIME,
      max_connection_pool_size=env.NEO4J_MAX_CONNECTION_POOL_SIZE,
      connection_acquisition_timeout=env.NEO4J_CONNECTION_ACQUISITION_TIMEOUT,
    )
    logger.info(f"Connected to Neo4j at {self.bolt_url} (enterprise={self.enterprise})")

  def _get_database_name(self, graph_id: str, database: Optional[str] = None) -> str:
    if database:
      return database
    elif self.enterprise:
      return f"kg_{graph_id}_main"
    else:
      return "neo4j"

  async def execute_query(
    self,
    graph_id: str,
    cypher: str,
    parameters: Optional[Dict[str, Any]] = None,
    database: Optional[str] = None,
  ) -> List[Dict[str, Any]]:
    await self._ensure_connected()

    db_name = self._get_database_name(graph_id, database)

    # Use Neo4j driver's automatic routing for cluster mode
    # For non-cluster mode, this has no effect
    async with self.driver.session(database=db_name) as session:
      result = await session.run(cypher, parameters or {})
      records = await result.data()
      return records

  async def execute_write(
    self,
    graph_id: str,
    cypher: str,
    parameters: Optional[Dict[str, Any]] = None,
    database: Optional[str] = None,
  ) -> List[Dict[str, Any]]:
    await self._ensure_connected()

    db_name = self._get_database_name(graph_id, database)

    async with self.driver.session(database=db_name) as session:

      async def _tx_function(tx):
        result = await tx.run(cypher, parameters or {})
        return await result.data()

      records = await session.execute_write(_tx_function)
      return records

  async def create_database(self, database_name: str) -> bool:
    if not self.enterprise:
      raise ValueError("Multi-database requires Neo4j Enterprise")

    await self._ensure_connected()

    async with self.driver.session(database="system") as session:
      await session.run(f"CREATE DATABASE `{database_name}` IF NOT EXISTS")

    logger.info(f"Created Neo4j database: {database_name}")
    return True

  async def delete_database(self, database_name: str) -> bool:
    if not self.enterprise:
      raise ValueError("Cannot delete database in Neo4j Community")

    if database_name == "neo4j":
      raise ValueError("Cannot delete default 'neo4j' database")

    await self._ensure_connected()

    async with self.driver.session(database="system") as session:
      await session.run(f"DROP DATABASE `{database_name}` IF EXISTS")

    logger.info(f"Deleted Neo4j database: {database_name}")
    return True

  async def list_databases(self) -> List[str]:
    await self._ensure_connected()

    if not self.enterprise:
      return ["neo4j"]

    async with self.driver.session(database="system") as session:
      result = await session.run("SHOW DATABASES")
      records = await result.data()
      databases = [record["name"] for record in records]

    return databases

  async def get_database_info(self, database_name: str) -> DatabaseInfo:
    await self._ensure_connected()

    node_count = 0
    relationship_count = 0
    size_bytes = 0

    try:
      async with self.driver.session(database=database_name) as session:
        node_result = await session.run("MATCH (n) RETURN count(n) as count")
        node_data = await node_result.single()
        if node_data:
          node_count = node_data["count"]

        rel_result = await session.run("MATCH ()-[r]->() RETURN count(r) as count")
        rel_data = await rel_result.single()
        if rel_data:
          relationship_count = rel_data["count"]

      if self.enterprise:
        async with self.driver.session(database="system") as session:
          size_result = await session.run(
            f"SHOW DATABASE `{database_name}` YIELD sizeOnDisk"
          )
          size_data = await size_result.single()
          if size_data and "sizeOnDisk" in size_data:
            size_bytes = size_data["sizeOnDisk"]

    except Exception as e:
      logger.warning(f"Failed to get database stats for {database_name}: {e}")

    return DatabaseInfo(
      name=database_name,
      node_count=node_count,
      relationship_count=relationship_count,
      size_bytes=size_bytes,
    )

  async def get_cluster_topology(self) -> ClusterTopology:
    await self._ensure_connected()

    if not self.enterprise:
      return ClusterTopology(mode="single", leader={"url": self.bolt_url})

    try:
      async with self.driver.session(database="system") as session:
        result = await session.run("CALL dbms.cluster.overview()")
        records = await result.data()

        topology = []
        leader = None
        followers = []

        for record in records:
          member = {
            "id": record["id"],
            "address": record["address"],
            "role": record["role"],
            "database": record.get("database", ""),
          }
          topology.append(member)

          if member["role"] == "LEADER":
            leader = member
          elif member["role"] == "FOLLOWER":
            followers.append(member)

        return ClusterTopology(
          mode="cluster", leader=leader, followers=followers, members=topology
        )

    except Exception as e:
      logger.warning(f"Not running in cluster mode: {e}")
      return ClusterTopology(mode="single", leader={"url": self.bolt_url})

  async def health_check(self) -> bool:
    try:
      await self._ensure_connected()

      async with self.driver.session(database="system") as session:
        await session.run("RETURN 1")

      return True
    except Exception as e:
      logger.error(f"Neo4j health check failed: {e}")
      return False

  async def close(self) -> None:
    if self.driver:
      await self.driver.close()
      self.driver = None
