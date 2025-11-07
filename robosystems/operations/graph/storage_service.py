"""
Storage calculator service for tracking S3 and EBS usage.

Calculates storage breakdown by type:
- Files: S3 user-uploaded files
- Tables: S3 CSV/Parquet imports
- Graphs: EBS Kuzu database files
- Subgraphs: EBS subgraph data (tracked separately for analytics)
"""

import logging
from typing import Dict
from decimal import Decimal

from sqlalchemy.orm import Session
from sqlalchemy import func

from ...models.iam import UserGraph, GraphFile, GraphTable
from ...adapters.s3 import S3Client

logger = logging.getLogger(__name__)


class StorageCalculator:
  """Calculate storage usage across S3 and EBS for billing."""

  def __init__(self, session: Session):
    self.session = session
    self.s3_client = S3Client()

  def calculate_graph_storage(self, graph_id: str, user_id: str) -> Dict[str, Decimal]:
    """
    Calculate total storage for a graph with breakdown by type.

    Args:
        graph_id: Graph database identifier
        user_id: User ID for ownership validation

    Returns:
        Dict with storage breakdown in GB:
        {
          "total_gb": Decimal,
          "files_gb": Decimal,
          "tables_gb": Decimal,
          "graphs_gb": Decimal,
          "subgraphs_gb": Decimal
        }
    """
    files_bytes = self._calculate_files_storage(graph_id)
    tables_bytes = self._calculate_tables_storage(graph_id)
    graphs_bytes = self._calculate_graph_database_storage(graph_id)
    subgraphs_bytes = self._calculate_subgraphs_storage(graph_id)

    total_bytes = files_bytes + tables_bytes + graphs_bytes + subgraphs_bytes

    return {
      "total_gb": Decimal(str(total_bytes / (1024**3))),
      "files_gb": Decimal(str(files_bytes / (1024**3))),
      "tables_gb": Decimal(str(tables_bytes / (1024**3))),
      "graphs_gb": Decimal(str(graphs_bytes / (1024**3))),
      "subgraphs_gb": Decimal(str(subgraphs_bytes / (1024**3))),
      "total_bytes": total_bytes,
    }

  def _calculate_files_storage(self, graph_id: str) -> int:
    """
    Calculate storage for user-uploaded files (S3).

    Returns:
        Total bytes of file storage
    """
    result = (
      self.session.query(func.sum(GraphFile.size_bytes))
      .filter(GraphFile.graph_id == graph_id, GraphFile.deleted_at.is_(None))
      .scalar()
    )

    return int(result) if result else 0

  def _calculate_tables_storage(self, graph_id: str) -> int:
    """
    Calculate storage for table imports (S3 CSV/Parquet files).

    Returns:
        Total bytes of table storage
    """
    result = (
      self.session.query(func.sum(GraphTable.total_size_bytes))
      .filter(GraphTable.graph_id == graph_id, GraphTable.deleted_at.is_(None))
      .scalar()
    )

    return int(result) if result else 0

  def _calculate_graph_database_storage(self, graph_id: str) -> int:
    """
    Calculate storage for main Kuzu graph database (EBS).

    For local development, we can estimate based on file sizes.
    For production, this should query the actual EBS volume or Kuzu database size.

    TODO: Implement production EBS volume size query via AWS API
    TODO: Or query Kuzu database size directly

    Returns:
        Total bytes of graph database storage
    """
    import os

    db_path = f"/app/data/{graph_id}"

    if os.path.exists(db_path):
      total_size = 0
      for dirpath, dirnames, filenames in os.walk(db_path):
        for filename in filenames:
          filepath = os.path.join(dirpath, filename)
          if os.path.exists(filepath):
            total_size += os.path.getsize(filepath)
      return total_size

    return 0

  def _calculate_subgraphs_storage(self, graph_id: str) -> int:
    """
    Calculate storage for subgraphs (part of main database but tracked separately).

    Subgraphs are stored within the main Kuzu database but can be isolated
    for analytics purposes.

    Returns:
        Total bytes of subgraph storage (0 for now, TODO: implement subgraph size tracking)
    """
    return 0

  def calculate_all_user_graphs(self, user_id: str) -> Dict[str, Dict[str, Decimal]]:
    """
    Calculate storage for all graphs owned by a user.

    Args:
        user_id: User ID

    Returns:
        Dict mapping graph_id to storage breakdown
    """
    user_graphs = (
      self.session.query(UserGraph.graph_id).filter(UserGraph.user_id == user_id).all()
    )

    results = {}
    for (graph_id,) in user_graphs:
      try:
        results[graph_id] = self.calculate_graph_storage(graph_id, user_id)
      except Exception as e:
        logger.error(f"Failed to calculate storage for graph {graph_id}: {e}")
        results[graph_id] = {
          "total_gb": Decimal("0"),
          "files_gb": Decimal("0"),
          "tables_gb": Decimal("0"),
          "graphs_gb": Decimal("0"),
          "subgraphs_gb": Decimal("0"),
          "error": str(e),
        }

    return results


def calculate_storage_for_graph(
  graph_id: str, user_id: str, session: Session
) -> Dict[str, Decimal]:
  """
  Convenience function to calculate storage for a single graph.

  Args:
      graph_id: Graph database identifier
      user_id: User ID
      session: Database session

  Returns:
        Storage breakdown in GB
  """
  calculator = StorageCalculator(session)
  return calculator.calculate_graph_storage(graph_id, user_id)
