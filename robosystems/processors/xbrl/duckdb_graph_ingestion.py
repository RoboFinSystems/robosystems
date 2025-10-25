"""
XBRL Graph Ingestion Processor (DuckDB-Based).

This module provides an alternative ingestion approach using DuckDB staging tables
instead of the proven consolidation + COPY approach.

Flow:
1. Discover ALL processed Parquet files in S3 (optionally filtered by year)
2. Create DuckDB staging tables via Graph API from discovered files
3. Trigger graph ingestion via Graph API (rebuilds graph from scratch)

Architecture:
- Worker communicates with Graph API via graph API client
- DuckDB connection pool lives on kuzu-api container
- Works directly with processed files (many small files) instead of consolidated files
- Tests performance with high file counts

LIMITATION: This approach currently ALWAYS rebuilds the graph from scratch because it
discovers and loads ALL files from S3, not just new/changed files. This is fundamentally
different from the COPY-based approach which works incrementally with consolidated files.

To add a new company:
1. Process the company's filings (creates processed files in S3)
2. Run DuckDB-based ingestion (discovers ALL files and rebuilds entire graph)

Status: Testing phase - may replace the existing COPY-based pipeline if it proves
more robust and maintainable at scale.
"""

import time
from typing import Dict, Any, List

from robosystems.logger import logger
from robosystems.config import env
from robosystems.adapters.s3 import S3Client
from robosystems.graph_api.client.factory import get_graph_client


class XBRLDuckDBGraphProcessor:
  """
  XBRL graph data processor using DuckDB-based ingestion pattern.

  This processor communicates with the Graph API to:
  1. Create DuckDB staging tables from processed Parquet files
  2. Trigger ingestion to graph database

  Architecture:
  - Uses graph API client to communicate with graph API container
  - DuckDB pool lives on graph API side, not on worker
  - Works directly with processed files (many small files) instead of
    consolidated files to test performance with high file counts
  """

  def __init__(self, graph_id: str = "sec", source_prefix: str = "processed"):
    """
    Initialize XBRL graph ingestion processor.

    Args:
        graph_id: Graph database identifier (default: "sec")
        source_prefix: S3 prefix for source files (default: "processed")
    """
    self.graph_id = graph_id
    self.s3_client = S3Client()
    self.bucket = env.SEC_PROCESSED_BUCKET or "robosystems-sec-processed"
    self.source_prefix = source_prefix

  async def process_files(
    self,
    rebuild: bool = True,
    year: int = None,
  ) -> Dict[str, Any]:
    """
    Process Parquet files into graph database using DuckDB-based pattern.

    IMPORTANT: This approach always rebuilds the graph from scratch because
    DuckDB staging tables contain ALL processed files from S3. Unlike the
    COPY-based approach which uses consolidated files incrementally, this
    method discovers all files and recreates the entire graph.

    Args:
        rebuild: Whether to rebuild graph from scratch (default: True).
                 Setting to False will cause duplicate key errors if graph
                 already contains data.
        year: Optional year filter for processing. If provided, only files
              from that year will be included in the rebuild.

    Returns:
        Processing results with statistics
    """
    start_time = time.time()

    logger.info(
      f"Starting DuckDB-based SEC ingestion for graph {self.graph_id} "
      f"(year={year or 'all'}, rebuild={rebuild})"
    )

    try:
      # Get graph client for API calls
      try:
        client = await get_graph_client(graph_id=self.graph_id, operation_type="write")
      except Exception as client_err:
        logger.error(
          f"Failed to initialize graph client for {self.graph_id}: {client_err}",
          exc_info=True,
        )
        return {
          "status": "error",
          "error": f"Graph client initialization failed: {str(client_err)}",
          "duration_seconds": time.time() - start_time,
        }

      # Step 1: Discover processed files
      logger.info("Step 1: Discovering processed Parquet files...")
      tables_info = await self._discover_processed_files(year)

      if not tables_info:
        logger.warning("No processed files found")
        return {
          "status": "no_data",
          "message": "No processed files found",
          "duration_seconds": time.time() - start_time,
        }

      logger.info(f"Found {len(tables_info)} tables to process")
      total_files = sum(len(files) for files in tables_info.values())
      logger.info(f"Total files: {total_files}")

      # Step 2: Create DuckDB staging tables via Graph API
      logger.info("Step 2: Creating DuckDB staging tables via Graph API...")
      await self._create_duckdb_tables(tables_info, client)

      # Step 3: Trigger ingestion
      logger.info("Step 3: Triggering graph ingestion...")
      ingestion_results = await self._trigger_ingestion(
        list(tables_info.keys()), client, rebuild=rebuild
      )

      duration = time.time() - start_time

      logger.info(
        f"✅ SEC DuckDB-based ingestion complete in {duration:.2f}s: "
        f"{ingestion_results.get('total_rows_ingested', 0)} rows ingested from {total_files} files"
      )

      return {
        "status": "success",
        "tables_processed": len(tables_info),
        "total_files": total_files,
        "ingestion_results": ingestion_results,
        "duration_seconds": duration,
      }

    except Exception as e:
      logger.error(f"SEC DuckDB-based ingestion failed: {e}", exc_info=True)
      return {
        "status": "error",
        "error": str(e),
        "duration_seconds": time.time() - start_time,
      }

  async def _discover_processed_files(self, year: int = None) -> Dict[str, List[str]]:
    """
    Discover processed Parquet files from S3.

    Scans the processed files directory structure:
    processed/year=YYYY/nodes/TableName/file.parquet
    processed/year=YYYY/relationships/TableName/file.parquet

    Args:
        year: Optional year filter

    Returns:
        Dictionary mapping table names to list of S3 keys
    """
    tables_info: Dict[str, List[str]] = {}

    # Scan both nodes and relationships directories
    for entity_type in ["nodes", "relationships"]:
      # Build prefix based on year filter
      if year:
        prefix = f"{self.source_prefix}/year={year}/{entity_type}/"
      else:
        prefix = f"{self.source_prefix}/{entity_type}/"

      logger.info(f"Scanning S3 bucket {self.bucket} with prefix {prefix}")

      # List all files recursively
      paginator = self.s3_client.s3_client.get_paginator("list_objects_v2")
      pages = paginator.paginate(Bucket=self.bucket, Prefix=prefix)

      for page in pages:
        if "Contents" not in page:
          continue

        for obj in page["Contents"]:
          key = obj["Key"]

          # Skip non-Parquet files
          if not key.endswith(".parquet"):
            continue

          # Extract table name from path: processed/year=YYYY/nodes|relationships/TableName/file.parquet
          # Structure: processed/year=YYYY/nodes|relationships/TableName/CIK_ACCESSION.parquet
          path_parts = key.replace(prefix, "").split("/")

          # First part after nodes/ or relationships/ is the table name
          if len(path_parts) >= 2:
            table_name = path_parts[0]
          else:
            logger.debug(f"Skipping file with unexpected path structure: {key}")
            continue

          if table_name not in tables_info:
            tables_info[table_name] = []

          tables_info[table_name].append(key)

    logger.info(f"Discovered {len(tables_info)} tables with files:")
    for table_name, files in tables_info.items():
      logger.info(f"  - {table_name}: {len(files)} files")

    return tables_info

  async def _create_duckdb_tables(
    self,
    tables_info: Dict[str, List[str]],
    graph_client,
  ) -> None:
    """
    Create DuckDB staging tables for each discovered table via Graph API.

    Passes the actual list of S3 file paths instead of wildcards to avoid
    DuckDB prepared parameter issues.

    Args:
        tables_info: Dictionary mapping table names to S3 keys
        graph_client: Graph API client instance
    """
    for table_name, s3_keys in tables_info.items():
      logger.info(f"Creating DuckDB table: {table_name} ({len(s3_keys)} files)")

      # Build list of full S3 URIs
      s3_files = [f"s3://{self.bucket}/{key}" for key in s3_keys]

      try:
        # Use graph client to call Graph API's table creation endpoint
        # Pass the list of files instead of a wildcard pattern
        response = await graph_client.create_table(
          graph_id=self.graph_id,
          table_name=table_name,
          s3_pattern=s3_files,  # Actually a list of files, not a pattern
        )

        logger.info(
          f"✓ Created DuckDB table {table_name}: "
          f"{response.get('row_count', 0)} rows, {response.get('column_count', 0)} columns "
          f"from {len(s3_keys)} files"
        )

      except Exception as e:
        logger.error(f"Failed to create DuckDB table {table_name}: {e}")
        raise

  async def _trigger_ingestion(
    self,
    table_names: List[str],
    graph_client,
    rebuild: bool = True,
  ) -> Dict[str, Any]:
    """
    Trigger ingestion for all tables into Kuzu graph via Graph API.

    Args:
        table_names: List of table names to ingest
        graph_client: Graph API client instance
        rebuild: Whether to rebuild graph database first (default: True).
                 Should be True since DuckDB tables contain all files.

    Returns:
        Ingestion results with statistics
    """
    if not rebuild:
      logger.warning(
        "⚠️  DuckDB-based ingestion with rebuild=False may cause duplicate key errors! "
        "This approach loads ALL files from S3 and should rebuild the graph."
      )

    total_rows = 0
    total_time_ms = 0.0
    results = []

    first_table = True

    for table_name in table_names:
      logger.info(f"Ingesting table: {table_name}")

      try:
        response = await graph_client.ingest_table_to_graph(
          graph_id=self.graph_id,
          table_name=table_name,
          ignore_errors=True,
          rebuild=rebuild and first_table,
        )

        total_rows += response.get("rows_ingested", 0)
        total_time_ms += response.get("execution_time_ms", 0)

        results.append(
          {
            "table_name": table_name,
            "rows_ingested": response.get("rows_ingested", 0),
            "status": response.get("status", "success"),
          }
        )

        logger.info(
          f"✓ Ingested {table_name}: "
          f"{response.get('rows_ingested', 0)} rows in "
          f"{response.get('execution_time_ms', 0):.2f}ms"
        )

        first_table = False

      except Exception as e:
        logger.error(f"Failed to ingest table {table_name}: {e}")
        results.append(
          {
            "table_name": table_name,
            "status": "error",
            "error": str(e),
          }
        )

    return {
      "total_rows_ingested": total_rows,
      "total_time_ms": total_time_ms,
      "tables": results,
    }
