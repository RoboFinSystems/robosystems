"""Dagster observability helpers for reporting asset materializations.

Reports AssetMaterialization events to Dagster's observable source assets
from outside Dagster jobs (API workers, provisioning service, etc.).

Usage:
    from robosystems.dagster.reporting import report_asset_materialization

    await report_asset_materialization(
        asset_key="user_graph_creation",
        description="Graph kg_abc123 created",
        metadata={"graph_id": "kg_abc123", "user_id": "usr_123"},
    )
"""

import asyncio
from typing import Any

from robosystems.logger import get_logger

logger = get_logger(__name__)

DAGSTER_REPORT_TIMEOUT = 15.0


async def report_asset_materialization(
  asset_key: str,
  description: str,
  metadata: dict[str, Any],
) -> None:
  """Report an AssetMaterialization to Dagster without blocking."""
  try:
    await asyncio.wait_for(
      asyncio.to_thread(
        report_asset_materialization_sync,
        asset_key,
        description,
        metadata,
      ),
      timeout=DAGSTER_REPORT_TIMEOUT,
    )
    logger.debug(f"Reported {asset_key} materialization to Dagster")
  except TimeoutError:
    logger.warning(
      f"Dagster materialization reporting timed out after {DAGSTER_REPORT_TIMEOUT}s. "
      "Operation succeeded but won't appear in Dagster UI."
    )
  except Exception as e:
    logger.warning(
      f"Failed to report AssetMaterialization to Dagster: {e}. "
      "Operation succeeded but won't appear in Dagster UI."
    )


def report_asset_materialization_sync(
  asset_key: str,
  description: str,
  metadata: dict[str, Any],
) -> None:
  """Synchronous Dagster materialization reporting (runs in thread)."""
  from robosystems.config import env

  if env.ENVIRONMENT == "test":
    logger.debug(f"Skipping Dagster reporting in test environment for {asset_key}")
    return

  from dagster import AssetKey, AssetMaterialization, DagsterInstance, MetadataValue

  instance = DagsterInstance.get()

  dagster_metadata = {}
  for key, value in metadata.items():
    if isinstance(value, int):
      dagster_metadata[key] = MetadataValue.int(value)
    elif isinstance(value, float):
      dagster_metadata[key] = MetadataValue.float(value)
    elif isinstance(value, bool):
      dagster_metadata[key] = MetadataValue.bool(value)
    else:
      dagster_metadata[key] = MetadataValue.text(str(value))

  materialization = AssetMaterialization(
    asset_key=AssetKey(asset_key),
    description=description,
    metadata=dagster_metadata,
  )

  instance.report_runless_asset_event(materialization)
