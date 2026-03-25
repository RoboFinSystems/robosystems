"""Dagster assets for data pipelines.

Assets represent data artifacts that are produced and consumed:
- User graph assets (creation, staging, materialization)
- Shared repository assets (replica refresh)

Per-repository publish assets live inside their adapter packages
(e.g., adapters/sec/pipeline/s3_publish.py) and are collected via
get_dagster_components() in definitions.py.

SEC pipeline assets have moved to robosystems.adapters.sec.pipeline
and are collected via get_dagster_components() in definitions.py.
"""

from robosystems.dagster.assets.graphs import (
  user_graph_creation_source,
  user_graph_file_staging_source,
  user_graph_materialized_source,
  user_repository_provisioning_source,
  user_subgraph_creation_source,
)
from robosystems.dagster.assets.shared_repositories import (
  SharedReplicaRefreshConfig,
  build_shared_replicas_refreshed,
)

__all__ = [
  "SharedReplicaRefreshConfig",
  "build_shared_replicas_refreshed",
  "user_graph_creation_source",
  "user_graph_file_staging_source",
  "user_graph_materialized_source",
  "user_repository_provisioning_source",
  "user_subgraph_creation_source",
]
