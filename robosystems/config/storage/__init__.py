"""Storage configuration module.

This module provides centralized S3 path helpers for different data domains:

- shared: Shared/public data sources (SEC, FRED, etc.)
- graph: Customer graph database storage (staging, backups)

Usage:
    from robosystems.config.storage import shared, graph

    # Shared data paths
    key = shared.get_raw_key(shared.DataSourceType.SEC, "year=2024", "filing.zip")

    # Graph data paths
    key = graph.get_staging_key("user123", "kg456", "Entity", "file123", "data.parquet")
"""

from robosystems.config.storage import graph, shared
from robosystems.config.storage.graph import GraphStorageType
from robosystems.config.storage.shared import DataSourceType

__all__ = [
  "DataSourceType",
  "GraphStorageType",
  "graph",
  "shared",
]
