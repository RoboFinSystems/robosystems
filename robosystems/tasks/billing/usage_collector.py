"""
Hourly Storage Usage Collection Task

This task runs hourly to collect detailed storage metrics from all graph databases
and store them as usage snapshots for billing and analytics.

Storage Collection Process:
--------------------------
1. Query all user graphs from database
2. For each graph:
   - Calculate storage using StorageCalculator:
     * Files: S3 user-uploaded files (GraphFile table)
     * Tables: S3 CSV/Parquet imports (GraphTable table)
     * Graphs: EBS Kuzu database files (filesystem walk)
     * Subgraphs: EBS subgraph data
   - Get instance metadata from Graph API (instance_id, region)
   - Record snapshot with full breakdown in GraphUsageTracking
3. Clean up old snapshots (retain 1 year)

Billing Flow:
------------
Hourly: This task → GraphUsageTracking snapshots
Daily: daily_storage_billing → Averages snapshots → Consumes credits for overages
Monthly: monthly_credit_reset → Invoices negative balances → Allocates fresh credits
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import List, Dict

from sqlalchemy.orm import Session

from ...celery import celery_app
from ...models.iam import UserGraph, GraphUsageTracking
from ...graph_api.client.factory import GraphClientFactory
from ...operations.graph.storage_service import StorageCalculator

logger = logging.getLogger(__name__)


@celery_app.task(
  name="robosystems.tasks.billing.usage_collector.graph_usage_collector",
  bind=True,
)
def graph_usage_collector(self):
  """Collect graph database usage metrics per graph_id."""
  logger.info("Starting graph usage collection task")

  try:
    # Get database session directly for Celery task
    from ...database import session as SessionLocal

    db_session = SessionLocal()

    try:
      # Get all user graphs that need billing
      user_graphs = get_user_graphs_with_details(db_session)
      logger.info(f"Found {len(user_graphs)} graphs to collect metrics for")

      # Track collection statistics
      total_records = 0
      collection_timestamp = datetime.now(timezone.utc)
      failed_graphs = []

      # Initialize storage calculator
      calculator = StorageCalculator(db_session)

      # Collect metrics for each graph
      for graph_info in user_graphs:
        graph_id = graph_info["graph_id"]
        user_id = graph_info["user_id"]
        graph_tier = graph_info.get("graph_tier", "kuzu-standard")

        logger.debug(f"Collecting metrics for graph {graph_id} (user: {user_id})")

        try:
          # Calculate detailed storage breakdown
          storage_breakdown = calculator.calculate_graph_storage(graph_id, user_id)

          # Get instance metadata from graph API
          metadata = asyncio.run(collect_graph_metrics(graph_id))

          # Record usage with storage breakdown
          GraphUsageTracking.record_storage_usage(
            user_id=user_id,
            graph_id=graph_id,
            graph_tier=graph_tier,
            storage_bytes=float(storage_breakdown["total_bytes"]),
            files_storage_gb=float(storage_breakdown["files_gb"]),
            tables_storage_gb=float(storage_breakdown["tables_gb"]),
            graphs_storage_gb=float(storage_breakdown["graphs_gb"]),
            subgraphs_storage_gb=float(storage_breakdown["subgraphs_gb"]),
            instance_id=metadata.get("instance_id") if metadata else None,
            region=metadata.get("region", "us-east-1"),
            session=db_session,
            auto_commit=False,
          )
          total_records += 1

        except Exception as e:
          logger.error(f"Failed to collect metrics for graph {graph_id}: {e}")
          failed_graphs.append(graph_id)

      # Commit all records
      db_session.commit()

      logger.info(
        f"Usage collection completed: "
        f"{total_records} records from {len(user_graphs)} graphs"
      )

      # Clean up old records (older than 1 year)
      deleted = GraphUsageTracking.cleanup_old_records(db_session, older_than_days=365)
      if deleted["deleted_records"] > 0:
        logger.info(f"Cleaned up {deleted['deleted_records']} old usage records")

      return {
        "status": "success",
        "timestamp": collection_timestamp.isoformat(),
        "graphs_processed": len(user_graphs),
        "records_created": total_records,
        "failed_graphs": failed_graphs,
        "old_records_deleted": deleted,
      }

    except Exception as e:
      logger.error(f"Graph usage collection failed: {e}")
      db_session.rollback()
      return {
        "status": "error",
        "error": str(e),
        "timestamp": datetime.now(timezone.utc).isoformat(),
      }
    finally:
      db_session.close()

  except Exception as e:
    logger.error(f"Failed to initialize graph usage collection: {e}")
    return {
      "status": "error",
      "error": str(e),
      "timestamp": datetime.now(timezone.utc).isoformat(),
    }


async def collect_graph_metrics(graph_id: str) -> Dict:
  """Collect usage metrics for a specific graph using GraphClientFactory."""

  try:
    # Use factory to create client with proper authentication and routing
    client = await GraphClientFactory.create_client(
      graph_id=graph_id,
      operation_type="read",  # Only reading metrics
    )

    try:
      # Call the new graph-specific metrics endpoint
      metrics = await client.get_database_metrics(graph_id)

      # Extract relevant billing information
      return {
        "size_bytes": metrics.get("size_bytes", 0),
        "node_count": metrics.get("node_count", 0),
        "relationship_count": metrics.get("relationship_count", 0),
        "instance_id": metrics.get("instance_id"),
        "region": "us-east-1",  # Region from environment
      }

    finally:
      await client.close()

  except Exception as e:
    logger.error(f"Error collecting metrics for graph {graph_id}: {e}")
    return {
      "size_bytes": 0,
      "node_count": 0,
      "relationship_count": 0,
      "instance_id": None,
      "region": "us-east-1",
    }


def get_user_graphs_with_details(session: Session) -> List[Dict]:
  """Get all user graphs with their details for billing."""
  from robosystems.models.iam import Graph

  # Join UserGraph with Graph to get tier information
  results = (
    session.query(UserGraph.graph_id, UserGraph.user_id, Graph.graph_tier)
    .join(Graph, UserGraph.graph_id == Graph.graph_id)
    .all()
  )

  graphs = []
  for row in results:
    graphs.append(
      {
        "graph_id": row.graph_id,
        "user_id": row.user_id,
        "graph_tier": row.graph_tier or "standard",
      }
    )

  return graphs
