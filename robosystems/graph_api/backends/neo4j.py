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

  async def ingest_from_s3(
    self,
    graph_id: str,
    table_name: str,
    s3_pattern: str,
    s3_credentials: Optional[Dict[str, Any]] = None,
    ignore_errors: bool = True,
    database: Optional[str] = None,
  ) -> Dict[str, Any]:
    import time
    import boto3
    import pyarrow.parquet as pq
    import io
    from botocore.exceptions import ClientError

    await self._ensure_connected()

    db_name = self._get_database_name(graph_id, database)

    s3_client = boto3.client(
      "s3",
      aws_access_key_id=s3_credentials.get("aws_access_key_id")
      if s3_credentials
      else None,
      aws_secret_access_key=s3_credentials.get("aws_secret_access_key")
      if s3_credentials
      else None,
      region_name=s3_credentials.get("region", env.AWS_REGION)
      if s3_credentials
      else env.AWS_REGION,
      endpoint_url=s3_credentials.get("endpoint_url") if s3_credentials else None,
    )

    if s3_pattern.startswith("s3://"):
      s3_pattern = s3_pattern[5:]

    parts = s3_pattern.split("/", 1)
    bucket = parts[0]
    prefix = parts[1] if len(parts) > 1 else ""

    prefix_path = "/".join(prefix.rsplit("/", 1)[:-1]) if "/" in prefix else ""
    file_pattern = prefix.rsplit("/", 1)[-1] if "/" in prefix else prefix

    logger.info(
      f"S3 file discovery - Bucket: {bucket}, Prefix: {prefix_path}, Pattern: {file_pattern}"
    )

    try:
      import fnmatch

      paginator = s3_client.get_paginator("list_objects_v2")
      pages = paginator.paginate(Bucket=bucket, Prefix=prefix_path)

      files = []
      for page in pages:
        if "Contents" in page:
          for obj in page["Contents"]:
            key = obj["Key"]
            if key.endswith(".parquet"):
              filename = key.rsplit("/", 1)[-1] if "/" in key else key
              if fnmatch.fnmatch(filename, file_pattern):
                files.append(key)

      logger.info(f"Found {len(files)} Parquet files matching pattern {s3_pattern}")

      if not files:
        logger.warning(f"No Parquet files found for pattern: {s3_pattern}")
        return {
          "records_loaded": 0,
          "duration_seconds": 0,
          "files_processed": 0,
          "query": "N/A - no files found",
        }

    except ClientError as e:
      logger.error(f"Failed to list S3 files: {e}")
      raise Exception(f"S3 list operation failed: {e}")

    total_records = 0
    start_time = time.time()

    for file_key in files:
      logger.info(f"Loading Parquet file from S3: {file_key}")

      try:
        response = s3_client.get_object(Bucket=bucket, Key=file_key)
        parquet_data = response["Body"].read()

        table = pq.read_table(io.BytesIO(parquet_data))
        df = table.to_pandas()

        df = df.where(df.notnull(), None)

        df = df.fillna({col: "" for col in df.columns if df[col].dtype == "object"})
        for col in df.columns:
          if df[col].dtype in ["float64", "int64"]:
            df[col] = df[col].fillna(0)

        records_in_file = len(df)
        logger.info(f"Read {records_in_file} records from {file_key}")

        batch_size = 1000
        file_records_loaded = 0

        for batch_start in range(0, records_in_file, batch_size):
          batch_end = min(batch_start + batch_size, records_in_file)
          batch_df = df.iloc[batch_start:batch_end]

          batch_records = [
            {k: (v if v is not None else "") for k, v in record.items()}
            for record in batch_df.to_dict("records")
          ]

          async with self.driver.session(database=db_name) as session:

            async def _load_batch(tx):
              id_field = "identifier" if "identifier" in batch_records[0] else "id"
              cypher = f"""
              UNWIND $batch as row
              MERGE (n:{table_name} {{identifier: row.{id_field}}})
              SET n = row
              RETURN count(n) as count
              """
              result = await tx.run(cypher, {"batch": batch_records})
              record = await result.single()
              return record["count"] if record else 0

            try:
              count = await session.execute_write(_load_batch)
              file_records_loaded += count
            except Exception as batch_error:
              if ignore_errors:
                logger.warning(
                  f"Skipped batch {batch_start}-{batch_end} in {file_key} due to error: {batch_error}"
                )
              else:
                raise

          logger.debug(
            f"Loaded batch {batch_start}-{batch_end} ({len(batch_records)} records)"
          )

        total_records += file_records_loaded
        logger.info(f"Completed loading {file_records_loaded} records from {file_key}")

      except Exception as e:
        logger.error(f"Failed to load Parquet file {file_key}: {e}")
        if not ignore_errors:
          raise
        else:
          logger.warning(f"Skipped file {file_key} due to error (ignore_errors=True)")

    duration = time.time() - start_time

    logger.info(
      f"Neo4j ingestion completed: {total_records:,} records from {len(files)} files in {duration:.2f}s"
    )

    return {
      "records_loaded": total_records,
      "duration_seconds": duration,
      "files_processed": len(files),
      "query": f"Python driver + pyarrow for {len(files)} files",
    }

  async def close(self) -> None:
    if self.driver:
      await self.driver.close()
      self.driver = None
