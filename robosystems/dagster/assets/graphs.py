"""External asset specs so materializations reported outside Dagster jobs (direct API
execution, which skips the ECS cold start) appear in the Assets tab."""

from dagster import AssetSpec

# ============================================================================
# Lifecycle Assets (graph/repository provisioning)
# ============================================================================

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

user_graph_backup_source = AssetSpec(
  key="user_graph_backup",
  description=(
    "User graph databases backed up to S3. "
    "Run nightly by the graph backup schedule and on demand via the "
    "create-backup operation."
  ),
  group_name="graphs",
  metadata={
    "pipeline": "graphs",
    "stage": "backup",
  },
  kinds={"s3"},
)
