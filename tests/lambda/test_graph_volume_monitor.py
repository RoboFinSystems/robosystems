"""Tests for the graph volume monitor Lambda's expanding-status reset.

`perform_volume_expansion` stamps a volume 'expanding' during an online resize
but nothing cleared it until the next detach/reattach, which stranded a
still-awake shared master's wake health-gate. `mark_volume_attached` resets
'expanding' -> 'attached' after a filesystem grow, guarded so it never clobbers
a row a detach has already moved to 'available'.
"""

from datetime import UTC, datetime, timedelta

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


# ---------------------------------------------------------------------------
# Instance registry reconciliation
#
# Writers deregister twice over (on-instance lifecycle script + the termination
# hook's detachment Lambda), but both hang off EBS volume management — so
# volume-less nodes get neither. Shared replicas register on boot and nothing
# ever removes them, and they run on spot, so rows accumulate at the reclaim
# rate. Reconciling from EC2 covers every termination path, graceful or not.
# ---------------------------------------------------------------------------

INSTANCE_TABLE = "robosystems-graph-test-instance-registry"


def _put_instance(
  instance_id: str,
  status: str = "healthy",
  node_type: str = "shared_replica",
  created_at: str | None = None,
) -> None:
  ddb = boto3.resource("dynamodb", region_name="us-east-1")
  ddb.Table(INSTANCE_TABLE).put_item(
    Item={
      "instance_id": instance_id,
      "status": status,
      "node_type": node_type,
      "created_at": created_at or (datetime.now(UTC) - timedelta(days=1)).isoformat(),
    }
  )


def _instance_rows() -> set[str]:
  ddb = boto3.resource("dynamodb", region_name="us-east-1")
  return {i["instance_id"] for i in ddb.Table(INSTANCE_TABLE).scan().get("Items", [])}


def _launch_tagged_instance(role: str = "shared_replica") -> str:
  """A running instance tagged the way the launch templates actually tag it.

  Writers carry `LadybugRole` (graph-ladybug.yaml); shared replicas carry only
  `NodeType: shared_replica` (graph-ladybug-replicas.yaml) — no `LadybugRole`.
  Keeping the fake fleet faithful to the real tag scheme is what lets these
  tests catch a discovery filter that can't see one of the node types.
  """
  role_tag = (
    {"Key": "NodeType", "Value": "shared_replica"}
    if role == "shared_replica"
    else {"Key": "LadybugRole", "Value": role}
  )
  ec2 = boto3.client("ec2", region_name="us-east-1")
  ami_id = ec2.describe_images(Owners=["amazon"])["Images"][0]["ImageId"]
  resp = ec2.run_instances(
    ImageId=ami_id,
    MinCount=1,
    MaxCount=1,
    InstanceType="r7g.large",
    TagSpecifications=[
      {
        "ResourceType": "instance",
        "Tags": [
          {"Key": "Service", "Value": "RoboSystems"},
          role_tag,
          {"Key": "Environment", "Value": "test"},
        ],
      }
    ],
  )
  return resp["Instances"][0]["InstanceId"]


def test_sync_removes_row_for_an_instance_that_no_longer_exists(gvmon):
  """The replica case: registered on boot, never deregistered, EC2 gone."""
  _put_instance("i-longgone", status="healthy", node_type="shared_replica")

  result = gvmon.sync_instance_registry()

  assert result["removed"] == 1
  assert "i-longgone" not in _instance_rows()


def test_sync_keeps_a_live_instance(gvmon):
  instance_id = _launch_tagged_instance()
  _put_instance(instance_id, status="healthy")

  result = gvmon.sync_instance_registry()

  assert result["removed"] == 0
  assert instance_id in _instance_rows()


def test_sync_spares_a_recently_registered_instance(gvmon):
  """An instance writes its row before it is running and tagged.

  Without the grace window, reconciliation would delete a row out from under an
  instance that is still booting.
  """
  _put_instance(
    "i-stillbooting",
    status="initializing",
    created_at=datetime.now(UTC).isoformat(),
  )

  result = gvmon.sync_instance_registry()

  assert result["removed"] == 0
  assert result["skipped_recent"] == 1
  assert "i-stillbooting" in _instance_rows()


def test_sync_removes_terminated_writers_and_stale_replicas_together(gvmon):
  """Mixed fleet: only rows without a live EC2 instance are removed.

  The live replica is the regression case: it is visible only via its
  `NodeType` tag, so a reconciler that discovers solely by `LadybugRole`
  would reap it alongside the genuinely dead rows.
  """
  live_writer = _launch_tagged_instance(role="writer")
  live_replica = _launch_tagged_instance(role="shared_replica")
  _put_instance(live_writer, status="healthy", node_type="writer")
  _put_instance(live_replica, status="healthy", node_type="shared_replica")
  _put_instance("i-deadwriter", status="terminated", node_type="writer")
  _put_instance("i-deadreplica", status="healthy", node_type="shared_replica")

  result = gvmon.sync_instance_registry()

  assert result["removed"] == 2
  assert _instance_rows() == {live_writer, live_replica}


def test_sync_is_a_noop_without_a_configured_table(gvmon, monkeypatch):
  monkeypatch.setattr(gvmon, "INSTANCE_REGISTRY_TABLE", None)
  assert gvmon.sync_instance_registry() == {
    "scanned": 0,
    "removed": 0,
    "skipped_recent": 0,
  }


def test_handler_exposes_sync_on_demand(gvmon):
  """Reconciling a known-stale registry shouldn't require waiting for the schedule."""
  _put_instance("i-longgone")

  result = gvmon.handler({"action": "sync_instance_registry"}, None)

  assert result["statusCode"] == 200
  assert result["removed"] == 1


def test_monitor_all_reports_the_instance_sync(gvmon):
  """A failure in one registry sync must not mask or be masked by the other."""
  _put_instance("i-longgone")

  result = gvmon.monitor_all_instances()

  assert result["instance_registry_synced"] is True
  assert result["stale_instances_removed"] == 1
