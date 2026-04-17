"""External asset specs for user graph operations.

These asset definitions allow AssetMaterializations reported from the API
(via direct execution) to appear in the Dagster UI's Assets tab.

Direct graph operations bypass Dagster job orchestration for performance
(eliminating 30-60s ECS cold start), but report materializations for
observability.

Asset categories:
- Lifecycle: Graph creation, subgraph creation, repository provisioning
- Data pipeline: File staging (DuckDB), graph materialization (LadybugDB)
"""

from dagster import AssetSpec

# ============================================================================
# Lifecycle Assets (graph/repository provisioning)
# ============================================================================

# External asset for user graph creation (unified)
# Materializations are reported from direct_monitor.py
# Tracks both direct API creation and subscription-based provisioning
# via the 'provisioning_method' metadata field ('direct' or 'subscription')
user_graph_creation_source = AssetSpec(
  key="user_graph_creation",
  description=(
    "User graph databases created via the API. "
    "Check 'provisioning_method' metadata for creation context."
  ),
  group_name="graphs",
  metadata={
    "pipeline": "graphs",
    "stage": "creation",
  },
  kinds={"provision"},
)

# External asset for user repository provisioning
user_repository_provisioning_source = AssetSpec(
  key="user_repository_provisioning",
  description=(
    "User access provisioned to shared repositories. "
    "Includes credit allocation and access grants."
  ),
  group_name="graphs",
  metadata={
    "pipeline": "graphs",
    "stage": "repository_provisioning",
  },
  kinds={"provision"},
)

# External asset for user subgraph creation
user_subgraph_creation_source = AssetSpec(
  key="user_subgraph_creation",
  description=(
    "User subgraphs created from parent graphs. "
    "These operations bypass Dagster orchestration for performance."
  ),
  group_name="graphs",
  metadata={
    "pipeline": "graphs",
    "stage": "subgraph_creation",
  },
  kinds={"provision"},
)

# ============================================================================
# Data Pipeline Assets (staging → materialization)
# ============================================================================

# External asset for user graph file staging
# Materializations reported from direct_staging.py
user_graph_file_staging_source = AssetSpec(
  key="user_graph_file_staging",
  description=(
    "User files staged directly to DuckDB via the API. "
    "These files bypass Dagster orchestration for performance but "
    "report materializations here for observability."
  ),
  group_name="graphs",
  metadata={
    "pipeline": "graphs",
    "stage": "staging",
  },
  kinds={"duckdb"},
)

# External asset for user graph materialization (DuckDB → LadybugDB)
# Materializations reported from materialize_graph_job
user_graph_materialized_source = AssetSpec(
  key="user_graph_materialized",
  description=(
    "User graph data materialized from DuckDB staging to LadybugDB. "
    "Tracks full graph rebuilds and incremental materializations."
  ),
  group_name="graphs",
  deps=["user_graph_file_staging"],
  metadata={
    "pipeline": "graphs",
    "stage": "materialization",
  },
  kinds={"ladybug"},
)

# External asset for extensions materialization (PostgreSQL OLTP → LadybugDB)
# Materializations reported from extensions_materialize_job
user_graph_extensions_materialized_source = AssetSpec(
  key="user_graph_extensions_materialized",
  description=(
    "Extensions OLTP data materialized from PostgreSQL to LadybugDB. "
    "Triggered after connector syncs or on-demand via API."
  ),
  group_name="graphs",
  metadata={
    "pipeline": "extensions",
    "stage": "materialization",
  },
  kinds={"ladybug"},
)

# ============================================================================
# Lifecycle Assets (backup)
# ============================================================================

# External asset for user graph backup
# Materializations reported from backup_graph_job (create_backup op)
user_graph_backup_source = AssetSpec(
  key="user_graph_backup",
  description=(
    "User graph databases backed up to S3. "
    "Triggered on-demand via the create-backup operation."
  ),
  group_name="graphs",
  metadata={
    "pipeline": "graphs",
    "stage": "backup",
  },
  kinds={"s3"},
)
