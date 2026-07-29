"""Tests for the graph volume monitor Lambda's expanding-status reset.

`perform_volume_expansion` stamps a volume 'expanding' during an online resize
but nothing cleared it until the next detach/reattach, which stranded a
still-awake shared master's wake health-gate. `mark_volume_attached` resets
'expanding' -> 'attached' after a filesystem grow, guarded so it never clobbers
a row a detach has already moved to 'available'.
"""

import boto3
import pytest

pytestmark = pytest.mark.unit


def _put_volume(status: str, volume_id: str = "vol-abc") -> None:
  table = boto3.resource("dynamodb", region_name="us-east-1").Table(
    "test-volume-registry"
  )
  table.put_item(
    Item={
      "volume_id": volume_id,
      "status": status,
      "databases": ["sec"],
      "instance_id": "i-123",
    }
  )


def _status(volume_id: str = "vol-abc") -> str:
  table = boto3.resource("dynamodb", region_name="us-east-1").Table(
    "test-volume-registry"
  )
  return table.get_item(Key={"volume_id": volume_id})["Item"]["status"]


class TestMarkVolumeAttached:
  def test_resets_expanding_to_attached(self, gvmon):
    _put_volume("expanding")
    gvmon.mark_volume_attached("vol-abc")
    assert _status() == "attached"

  def test_leaves_available_untouched(self, gvmon):
    # A detach already moved the row to 'available'; the conditional write must
    # not resurrect it to 'attached'.
    _put_volume("available")
    gvmon.mark_volume_attached("vol-abc")
    assert _status() == "available"

  def test_noop_when_already_attached(self, gvmon):
    _put_volume("attached")
    gvmon.mark_volume_attached("vol-abc")
    assert _status() == "attached"

  def test_missing_row_is_non_fatal(self, gvmon):
    # No row for the volume — the conditional write fails and is swallowed.
    gvmon.mark_volume_attached("vol-does-not-exist")


class TestTierVolumeCeiling:
  """Expansion must stop at the product-tier ceiling.

  Without one, this Lambda grew a 20 GB-cap tenant's volume toward the
  16 TB EBS maximum — unbounded EBS spend for data the application-side
  storage cap should have stopped.
  """

  def test_growth_is_capped_at_the_tier_ceiling(self, gvmon):
    # Standard: 20 GB cap x 2.0 multiplier = 40 GB ceiling. 35 GB at 90%
    # wants 60 GB after expansion factor, minimum step, and rounding.
    assert gvmon.calculate_new_volume_size(35, 0.9, tier="ladybug-standard") == 40

  def test_at_ceiling_returns_current_size(self, gvmon):
    """No growth at the ceiling; the caller treats no-growth as no-expansion."""
    assert gvmon.calculate_new_volume_size(40, 0.95, tier="ladybug-standard") == 40

  def test_tiers_without_a_product_cap_grow_freely(self, gvmon):
    # Shared repositories are platform-managed and have no product cap.
    assert gvmon.calculate_new_volume_size(200, 0.9, tier="ladybug-shared") == 300
    assert gvmon.calculate_new_volume_size(200, 0.9, tier="unknown") == 300

  def test_lambda_tier_limits_match_graph_yml(self, gvmon):
    """The Lambda is standalone and cannot import app config, so its copy of
    the per-tier caps is pinned against graph.yml here."""
    from robosystems.config.graph_tier import GraphTierConfig

    GraphTierConfig.clear_cache()
    assert gvmon.TIER_STORAGE_LIMITS_GB, "tier limits table must not be empty"
    for tier, limit in gvmon.TIER_STORAGE_LIMITS_GB.items():
      assert limit == GraphTierConfig.get_instance_storage_limit_gb(
        tier, "production"
      ), f"{tier}: Lambda says {limit}, graph.yml disagrees"
