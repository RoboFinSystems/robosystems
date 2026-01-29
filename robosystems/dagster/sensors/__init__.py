"""Dagster sensors and schedules for event-triggered operations.

Sensors monitor for conditions and trigger jobs when criteria are met:
- Provisioning sensors: Watch for subscriptions needing graph/repository provisioning
- SEC processing sensor: Watch for raw filings and trigger parallel processing
- SEC post-materialize snapshot: Trigger replica snapshot after SEC materialization

Incremental Pipeline (SEC_INCREMENTAL_PIPELINE_ENABLED=true):
- sec_incremental_download_schedule: Download every 3 hours
- sec_download_to_process_sensor: Chain download → process
- sec_process_to_stage_materialize_sensor: Chain process → stage + materialize
- sec_post_materialize_snapshot_sensor: Chain materialize → snapshot
"""

from robosystems.dagster.sensors.provisioning import (
  pending_repository_sensor,
  pending_subscription_sensor,
)
from robosystems.dagster.sensors.sec import (
  sec_download_to_process_sensor,
  sec_incremental_download_schedule,
  sec_post_materialize_snapshot_sensor,
  sec_process_to_stage_materialize_sensor,
  sec_processing_sensor,
)

__all__ = [
  "pending_repository_sensor",
  "pending_subscription_sensor",
  "sec_download_to_process_sensor",
  "sec_incremental_download_schedule",
  "sec_post_materialize_snapshot_sensor",
  "sec_process_to_stage_materialize_sensor",
  "sec_processing_sensor",
]
