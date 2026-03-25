"""Shared Repository Dagster Assets.

This package contains:

1. PUBLISH HELPER (shared logic):
   - publish_to_s3() - Core S3 publish logic called by per-repository assets

2. REPLICA REFRESH (shared infrastructure):
   - build_shared_replicas_refreshed() - Factory that builds the replica refresh
     asset with dynamic upstream deps from enabled adapter pipelines

Per-repository publish assets live inside their adapter packages
(e.g., adapters/sec/pipeline/s3_publish.py) and declare deps on their
own materialization asset. This creates clean lineage:

  sec_graph_materialized -> sec_lbug_s3_published -> shared_replicas_refreshed
  (future) industry_graph_materialized -> industry_s3_published -> shared_replicas_refreshed
"""

from robosystems.dagster.assets.shared_repositories.publish import publish_to_s3
from robosystems.dagster.assets.shared_repositories.replicas import (
  SharedReplicaRefreshConfig,
  build_shared_replicas_refreshed,
)

__all__ = [
  "SharedReplicaRefreshConfig",
  "build_shared_replicas_refreshed",
  "publish_to_s3",
]
