"""Deprovisioning configuration for graph lifecycle management."""

from dataclasses import dataclass, field


@dataclass(frozen=True)
class DeprovisioningConfig:
  """Configuration for graph deprovisioning behavior."""

  # The suspend → auto-deprovision grace window. A canceled subscription
  # suspends the graph; suspended_graph_deprovisioning_sensor
  # (dagster/sensors/graph_lifecycle.py) only tears it down once
  # ends_at < now - retention_days, unless the cancellation was IMMEDIATE.
  # Load-bearing — not the backup hosting window (that is backup_hosting_days).
  retention_days: int = 7
  require_final_backup: bool = True
  backup_delay_hours: int = 24
  # 90 days for every tier, capped by the S3 lifecycle rule
  # (cloudformation/s3.yaml, ExpireGraphBackups: 90), which deletes the object
  # regardless of what is promised here. The final backup is tracked in
  # GraphBackup (DeprovisionService's _register_final_backup), so its expiry
  # runs through the tier-aware cleanup job, which reads the values below.
  # Differentiating by tier therefore takes effect — but only up to that
  # 90-day ceiling. Going beyond it means raising or exempting the rule too.
  backup_hosting_days: dict[str, int] = field(
    default_factory=lambda: {
      "ladybug-standard": 90,
      "ladybug-large": 90,
      "ladybug-xlarge": 90,
    }
  )

  def get_backup_hosting_days(self, tier: str) -> int:
    """Get backup hosting days for a given tier."""
    return self.backup_hosting_days.get(tier, 90)


DEPROVISIONING_CONFIG = DeprovisioningConfig()


def get_deprovisioning_config() -> DeprovisioningConfig:
  """Get the deprovisioning configuration."""
  return DEPROVISIONING_CONFIG
