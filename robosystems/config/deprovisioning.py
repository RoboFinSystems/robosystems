"""Deprovisioning configuration for graph lifecycle management."""

from dataclasses import dataclass, field


@dataclass(frozen=True)
class DeprovisioningConfig:
  """Configuration for graph deprovisioning behavior."""

  retention_days: int = 7
  require_final_backup: bool = True
  backup_delay_hours: int = 24
  backup_hosting_days: dict[str, int] = field(
    default_factory=lambda: {
      "ladybug-standard": 90,
      "ladybug-large": 180,
      "ladybug-xlarge": 365,
    }
  )

  def get_backup_hosting_days(self, tier: str) -> int:
    """Get backup hosting days for a given tier."""
    return self.backup_hosting_days.get(tier, 90)


DEPROVISIONING_CONFIG = DeprovisioningConfig()


def get_deprovisioning_config() -> DeprovisioningConfig:
  """Get the deprovisioning configuration."""
  return DEPROVISIONING_CONFIG
