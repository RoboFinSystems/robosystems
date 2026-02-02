"""Dagster sensors and schedules for event-triggered operations.

Sensors monitor for conditions and trigger jobs when criteria are met.
All sensors start STOPPED by default - enable in Dagster UI when ready.

- Provisioning sensors: Watch for subscriptions needing graph/repository provisioning
- SEC processing sensor: Watch for raw filings and trigger parallel processing
- SEC incremental pipeline: Automated daily download → process → stage → copy → snapshot
"""

from robosystems.dagster.sensors.provisioning import (
  pending_repository_sensor,
  pending_subscription_sensor,
)
from robosystems.dagster.sensors.sec import (
  sec_download_to_process_sensor,
  sec_incremental_download_schedule,
  sec_incremental_post_ingest_snapshot_sensor,
  sec_incremental_staging_sensor,
  sec_post_materialize_snapshot_sensor,
  sec_processing_sensor,
  sec_stage_to_copy_sensor,
)

__all__ = [
  "pending_repository_sensor",
  "pending_subscription_sensor",
  "sec_download_to_process_sensor",
  "sec_incremental_download_schedule",
  "sec_incremental_post_ingest_snapshot_sensor",
  "sec_incremental_staging_sensor",
  "sec_post_materialize_snapshot_sensor",
  "sec_processing_sensor",
  "sec_stage_to_copy_sensor",
]
