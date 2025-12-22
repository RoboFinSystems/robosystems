"""Observable source asset for direct-staged files.

This asset definition allows AssetMaterializations reported from the API
(via direct staging) to appear in the Dagster UI's Assets tab.

Direct staging bypasses Dagster job orchestration for performance, but
reports materializations for observability. This asset definition makes
those materializations visible in the UI.
"""

from dagster import AssetKey, SourceAsset

# Observable source asset for direct-staged files
# This matches the asset key used in direct_staging.py: AssetKey(["staged_files", graph_id])
# Since graph_ids are dynamic, we define a base asset that receives all materializations
staged_files_source = SourceAsset(
  key=AssetKey("staged_files"),
  description=(
    "Files staged directly to DuckDB via the API. "
    "These files bypass Dagster orchestration for performance but "
    "report materializations here for observability."
  ),
  group_name="staging",
)
