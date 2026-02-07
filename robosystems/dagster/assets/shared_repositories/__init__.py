"""Shared Repository Dagster Assets.

This package contains assets for shared repository infrastructure operations:

1. PUBLISH (replica distribution):
   - shared_repository_s3_published - Publish raw .lbug to S3 for replica cluster S3 ATTACH
   - shared_replicas_refreshed - Rolling refresh of replica fleet to pick up new database

These assets are repository-agnostic. The graph_id parameter in config determines
which repository to operate on. The replica fleet is a single shared ASG that
serves all shared repositories.
"""

from robosystems.dagster.assets.shared_repositories.publish import (
  SharedRepositoryPublishConfig,
  shared_repository_s3_published,
)
from robosystems.dagster.assets.shared_repositories.replicas import (
  SharedReplicaRefreshConfig,
  shared_replicas_refreshed,
)

__all__ = [
  # Config classes
  "SharedReplicaRefreshConfig",
  "SharedRepositoryPublishConfig",
  # Assets
  "shared_replicas_refreshed",
  "shared_repository_s3_published",
]
