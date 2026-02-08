"""Dagster assets for data pipelines.

Assets represent data artifacts that are produced and consumed:
- User graph assets (creation, staging, materialization)
- Shared repository assets (S3 publish, replica refresh)

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
  SharedRepositoryPublishConfig,
  shared_replicas_refreshed,
  shared_repository_s3_published,
)

__all__ = [
  "SharedReplicaRefreshConfig",
  "SharedRepositoryPublishConfig",
  "shared_replicas_refreshed",
  "shared_repository_s3_published",
  "user_graph_creation_source",
  "user_graph_file_staging_source",
  "user_graph_materialized_source",
  "user_repository_provisioning_source",
  "user_subgraph_creation_source",
]
