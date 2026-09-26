"""Unit tests for shared-master parking (wake/sleep).

Boto3 clients, the DynamoDB registries, and the health client are all mocked;
these lock in the two load-bearing invariants — the four-signal wake gate and
the protection-before-scale-down sleep order.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from robosystems.dagster.assets.shared_repositories import master_parking
from robosystems.dagster.assets.shared_repositories.master_parking import (
  MasterWakeTimeout,
  get_shared_master_asg_name,
  sleep_master,
  wait_for_master_healthy,
  wake_master,
)

ASG = "test-shared-asg"


def _asg_client(instance_id: str | None = "i-abc") -> MagicMock:
  client = MagicMock()
  instances = [{"InstanceId": instance_id}] if instance_id else []
  client.describe_auto_scaling_groups.return_value = {
    "AutoScalingGroups": [{"Instances": instances}]
  }
  return client


def _ec2_client(running: bool = True) -> MagicMock:
  client = MagicMock()
  client.describe_instances.return_value = {
    "Reservations": [
      {"Instances": [{"State": {"Name": "running" if running else "pending"}}]}
    ]
  }
  return client


def _dynamodb(
  *,
  vol_status: str = "attached",
  vol_instance: str = "i-abc",
  databases=("sec",),
  node_type: str = "shared_master",
  inst_status: str = "healthy",
  private_ip: str | None = "10.0.0.5",
) -> MagicMock:
  volume_table = MagicMock()
  volume_table.scan.return_value = {
    "Items": [
      {"databases": list(databases), "status": vol_status, "instance_id": vol_instance}
    ]
  }
  instance_item: dict = {"node_type": node_type, "status": inst_status}
  if private_ip is not None:
    instance_item["private_ip"] = private_ip
  instance_table = MagicMock()
  instance_table.get_item.return_value = {"Item": instance_item}

  resource = MagicMock()
  resource.Table.side_effect = lambda name: (
    volume_table if "volume" in name else instance_table
  )
  return resource


def _health_client() -> MagicMock:
  client = MagicMock()
  client.health_check = AsyncMock(return_value={"status": "ok"})
  client.close = AsyncMock()
  return client


@pytest.mark.unit
class TestWakeMaster:
  @patch.object(master_parking, "_autoscaling_client")
  @patch.object(master_parking, "_ec2_client")
  @patch.object(master_parking, "_dynamodb_resource")
  @patch.object(master_parking, "get_graph_client_for_instance", new_callable=AsyncMock)
  def test_happy_path(self, m_health, m_ddb, m_ec2, m_asg):
    asg = _asg_client("i-123")
    m_asg.return_value = asg
    m_ec2.return_value = _ec2_client(running=True)
    m_ddb.return_value = _dynamodb(vol_instance="i-123", private_ip="10.0.0.5")
    m_health.return_value = _health_client()

    result = asyncio.run(wake_master())

    assert result["status"] == "awake"
    assert result["instance_id"] == "i-123"
    assert result["private_ip"] == "10.0.0.5"
    asg.set_desired_capacity.assert_called_once()
    assert asg.set_desired_capacity.call_args.kwargs["DesiredCapacity"] == 1

  @patch.object(master_parking, "_autoscaling_client")
  @patch.object(master_parking, "_ec2_client")
  @patch.object(master_parking, "_dynamodb_resource")
  @patch.object(master_parking, "get_graph_client_for_instance", new_callable=AsyncMock)
  def test_timeout_when_not_registered_healthy(self, m_health, m_ddb, m_ec2, m_asg):
    m_asg.return_value = _asg_client("i-123")
    m_ec2.return_value = _ec2_client(running=True)
    # Instance never reports healthy in the instance registry.
    m_ddb.return_value = _dynamodb(vol_instance="i-123", inst_status="unhealthy")
    m_health.return_value = _health_client()

    with pytest.raises(MasterWakeTimeout):
      asyncio.run(wait_for_master_healthy(ASG, timeout_s=0, poll_interval_s=0))

  @patch.object(master_parking, "_autoscaling_client")
  @patch.object(master_parking, "_ec2_client")
  @patch.object(master_parking, "_dynamodb_resource")
  @patch.object(master_parking, "get_graph_client_for_instance", new_callable=AsyncMock)
  def test_timeout_when_volume_not_attached(self, m_health, m_ddb, m_ec2, m_asg):
    m_asg.return_value = _asg_client("i-123")
    m_ec2.return_value = _ec2_client(running=True)
    # Volume is present but still detaching (not yet reattached to this instance).
    m_ddb.return_value = _dynamodb(vol_instance="i-123", vol_status="available")
    m_health.return_value = _health_client()

    with pytest.raises(MasterWakeTimeout):
      asyncio.run(wait_for_master_healthy(ASG, timeout_s=0, poll_interval_s=0))


@pytest.mark.unit
class TestSleepMaster:
  @patch.object(master_parking, "set_volume_performance", return_value=[])
  @patch.object(master_parking, "_autoscaling_client")
  def test_clears_protection_before_scaling_down(self, m_asg, _m_perf):
    asg = _asg_client("i-xyz")
    m_asg.return_value = asg

    result = sleep_master()

    assert result == {"status": "asleep", "instance_id": "i-xyz", "volumes": []}
    # Load-bearing: protection must be cleared BEFORE desired capacity drops,
    # or the ASG cancels the scale-in and the master strands awake.
    call_names = [c[0] for c in asg.method_calls]
    assert call_names == [
      "describe_auto_scaling_groups",
      "set_instance_protection",
      "set_desired_capacity",
    ]
    assert asg.set_instance_protection.call_args.kwargs["ProtectedFromScaleIn"] is False
    assert asg.set_desired_capacity.call_args.kwargs["DesiredCapacity"] == 0

  @patch.object(master_parking, "set_volume_performance", return_value=[])
  @patch.object(master_parking, "_autoscaling_client")
  def test_no_instance_still_scales_to_zero(self, m_asg, _m_perf):
    asg = _asg_client(instance_id=None)
    m_asg.return_value = asg

    result = sleep_master()

    assert result["instance_id"] is None
    asg.set_instance_protection.assert_not_called()
    asg.set_desired_capacity.assert_called_once()
    assert asg.set_desired_capacity.call_args.kwargs["DesiredCapacity"] == 0

  @patch.object(master_parking, "set_volume_performance")
  @patch.object(master_parking, "_autoscaling_client")
  def test_parks_volume_after_scaling_down(self, m_asg, m_perf):
    order: list[str] = []
    asg = _asg_client("i-xyz")
    asg.set_desired_capacity.side_effect = lambda **_: order.append("scale")
    m_asg.return_value = asg
    m_perf.side_effect = lambda target: (
      order.append("park") or [{"volume_id": "vol-1", "result": "modified"}]
    )

    result = sleep_master()

    assert order == ["scale", "park"]
    m_perf.assert_called_once_with(master_parking.PARKED_VOLUME_PERFORMANCE)
    assert result["volumes"] == [{"volume_id": "vol-1", "result": "modified"}]


def _volume(iops: int, throughput: int, volume_type: str = "gp3") -> dict:
  return {
    "VolumeId": "vol-sec",
    "VolumeType": volume_type,
    "Iops": iops,
    "Throughput": throughput,
  }


@pytest.mark.unit
class TestSetVolumePerformance:
  @patch.object(master_parking, "_ec2_client")
  def test_finds_the_volume_by_tier_database_and_environment(self, m_ec2):
    from robosystems.config import env

    ec2 = MagicMock()
    ec2.describe_volumes.return_value = {"Volumes": []}
    m_ec2.return_value = ec2

    assert master_parking.set_volume_performance({"Iops": 3000}) == []

    filters = {
      f["Name"]: f["Values"] for f in ec2.describe_volumes.call_args.kwargs["Filters"]
    }
    assert filters == {
      "tag:Tier": ["ladybug-shared"],
      "tag:DatabaseId": ["sec"],
      "tag:Environment": [env.ENVIRONMENT],
    }

  @patch.object(master_parking, "_ec2_client")
  def test_parks_a_boosted_volume(self, m_ec2):
    ec2 = MagicMock()
    ec2.describe_volumes.return_value = {"Volumes": [_volume(12000, 500)]}
    m_ec2.return_value = ec2

    outcomes = master_parking.set_volume_performance(
      master_parking.PARKED_VOLUME_PERFORMANCE
    )

    ec2.modify_volume.assert_called_once_with(
      VolumeId="vol-sec", Iops=3000, Throughput=125
    )
    assert outcomes[0]["result"] == "modified"

  @patch.object(master_parking, "_ec2_client")
  def test_changes_only_what_differs(self, m_ec2):
    ec2 = MagicMock()
    ec2.describe_volumes.return_value = {"Volumes": [_volume(3000, 500)]}
    m_ec2.return_value = ec2

    master_parking.set_volume_performance(master_parking.BOOSTED_VOLUME_PERFORMANCE)

    ec2.modify_volume.assert_called_once_with(VolumeId="vol-sec", Iops=12000)

  @patch.object(master_parking, "_ec2_client")
  def test_volume_already_at_target_is_left_alone(self, m_ec2):
    ec2 = MagicMock()
    ec2.describe_volumes.return_value = {"Volumes": [_volume(12000, 500)]}
    m_ec2.return_value = ec2

    outcomes = master_parking.set_volume_performance(
      master_parking.BOOSTED_VOLUME_PERFORMANCE
    )

    ec2.modify_volume.assert_not_called()
    assert outcomes == [{"volume_id": "vol-sec", "result": "unchanged"}]

  @patch.object(master_parking, "_ec2_client")
  def test_non_gp3_volume_is_skipped(self, m_ec2):
    ec2 = MagicMock()
    ec2.describe_volumes.return_value = {"Volumes": [_volume(3000, 125, "io2")]}
    m_ec2.return_value = ec2

    outcomes = master_parking.set_volume_performance(
      master_parking.BOOSTED_VOLUME_PERFORMANCE
    )

    ec2.modify_volume.assert_not_called()
    assert outcomes[0]["result"] == "not_gp3"

  @patch.object(master_parking, "_ec2_client")
  def test_modification_in_flight_is_deferred_not_raised(self, m_ec2):
    from botocore.exceptions import ClientError

    ec2 = MagicMock()
    ec2.describe_volumes.return_value = {"Volumes": [_volume(12000, 500)]}
    ec2.modify_volume.side_effect = ClientError(
      {"Error": {"Code": "IncorrectModificationState", "Message": "in flight"}},
      "ModifyVolume",
    )
    m_ec2.return_value = ec2

    outcomes = master_parking.set_volume_performance(
      master_parking.PARKED_VOLUME_PERFORMANCE
    )

    assert outcomes == [
      {"volume_id": "vol-sec", "result": "deferred: IncorrectModificationState"}
    ]

  @patch.object(master_parking, "_ec2_client")
  def test_lookup_failure_returns_empty(self, m_ec2):
    from botocore.exceptions import ClientError

    ec2 = MagicMock()
    ec2.describe_volumes.side_effect = ClientError(
      {"Error": {"Code": "UnauthorizedOperation", "Message": "no"}},
      "DescribeVolumes",
    )
    m_ec2.return_value = ec2

    assert master_parking.set_volume_performance({"Iops": 3000}) == []


@pytest.mark.unit
class TestVolumeGate:
  @patch.object(master_parking, "_dynamodb_resource")
  def test_ignores_stale_row_and_matches_real_one(self, m_ddb):
    from robosystems.dagster.assets.shared_repositories.master_parking import (
      _volume_attached_to,
    )

    volume_table = MagicMock()
    # Stale row for a "sec" volume appears FIRST; the real attached match is later.
    volume_table.scan.return_value = {
      "Items": [
        {"databases": ["sec"], "status": "available", "instance_id": "i-old"},
        {"databases": ["sec"], "status": "attached", "instance_id": "i-123"},
      ]
    }
    resource = MagicMock()
    resource.Table.return_value = volume_table
    m_ddb.return_value = resource

    assert _volume_attached_to("i-123", "sec") is True

  @patch.object(master_parking, "_dynamodb_resource")
  def test_expanding_status_counts_as_attached(self, m_ddb):
    # A volume mid online-resize is stamped 'expanding' but stays attached and
    # usable; the gate must not hang the wake on it (the monitor doesn't reset
    # the status until the next detach/reattach).
    from robosystems.dagster.assets.shared_repositories.master_parking import (
      _volume_attached_to,
    )

    volume_table = MagicMock()
    volume_table.scan.return_value = {
      "Items": [
        {"databases": ["sec"], "status": "expanding", "instance_id": "i-123"},
      ]
    }
    resource = MagicMock()
    resource.Table.return_value = volume_table
    m_ddb.return_value = resource

    assert _volume_attached_to("i-123", "sec") is True

  @patch.object(master_parking, "_dynamodb_resource")
  def test_expanding_row_for_other_instance_is_ignored(self, m_ddb):
    # 'expanding' still only matches THIS instance — a row bound to a different
    # instance must not green-light the wake.
    from robosystems.dagster.assets.shared_repositories.master_parking import (
      _volume_attached_to,
    )

    volume_table = MagicMock()
    volume_table.scan.return_value = {
      "Items": [
        {"databases": ["sec"], "status": "expanding", "instance_id": "i-other"},
      ]
    }
    resource = MagicMock()
    resource.Table.return_value = volume_table
    m_ddb.return_value = resource

    assert _volume_attached_to("i-123", "sec") is False


@pytest.mark.unit
def test_asg_name_from_config():
  from robosystems.config import env

  assert get_shared_master_asg_name() == env.SHARED_MASTER_ASG_NAME


@pytest.mark.unit
class TestSleepAssetParkingGate:
  """The sleep asset honors SHARED_MASTER_PARKING_ENABLED.

  The pure ``sleep_master`` always scales to 0 (unit-tested above); the gate
  lives in the asset wrapper so both the happy-path and failure-path sleep
  sensors respect it via a single check.
  """

  @patch("robosystems.dagster.assets.shared_repositories.master.sleep_master")
  @patch("robosystems.dagster.assets.shared_repositories.master.env")
  def test_parking_disabled_skips_scale_down(self, mock_env, m_sleep):
    from dagster import build_asset_context

    from robosystems.dagster.assets.shared_repositories.master import (
      shared_master_asleep,
    )

    mock_env.ENVIRONMENT = "prod"
    mock_env.SHARED_MASTER_PARKING_ENABLED = False

    result = shared_master_asleep(build_asset_context())

    m_sleep.assert_not_called()
    assert result.metadata["status"] == "skipped"
    assert result.metadata["reason"] == "parking_disabled"

  @patch("robosystems.dagster.assets.shared_repositories.master.sleep_master")
  @patch("robosystems.dagster.assets.shared_repositories.master.env")
  def test_parking_enabled_scales_down(self, mock_env, m_sleep):
    from dagster import build_asset_context

    from robosystems.dagster.assets.shared_repositories.master import (
      shared_master_asleep,
    )

    mock_env.ENVIRONMENT = "prod"
    mock_env.SHARED_MASTER_PARKING_ENABLED = True
    m_sleep.return_value = {"status": "asleep", "instance_id": "i-xyz"}

    result = shared_master_asleep(build_asset_context())

    m_sleep.assert_called_once()
    assert result.metadata["instance_id"] == "i-xyz"


@pytest.mark.unit
class TestVolumeBoostOp:
  @patch("robosystems.dagster.jobs.shared_repository.set_volume_performance")
  @patch("robosystems.dagster.jobs.shared_repository.env")
  def test_boosts_to_run_performance(self, mock_env, m_perf):
    from dagster import build_op_context

    from robosystems.dagster.jobs.shared_repository import (
      BOOSTED_VOLUME_PERFORMANCE,
      boost_shared_master_volume,
    )

    mock_env.ENVIRONMENT = "prod"
    mock_env.SHARED_MASTER_PARKING_ENABLED = True
    m_perf.return_value = [{"volume_id": "vol-sec", "result": "modified"}]

    result = boost_shared_master_volume(build_op_context())

    m_perf.assert_called_once_with(BOOSTED_VOLUME_PERFORMANCE)
    assert result == [{"volume_id": "vol-sec", "result": "modified"}]

  @pytest.mark.parametrize(
    ("environment", "parking_enabled"), [("dev", True), ("prod", False)]
  )
  @patch("robosystems.dagster.jobs.shared_repository.set_volume_performance")
  @patch("robosystems.dagster.jobs.shared_repository.env")
  def test_skips_in_dev_and_when_parking_disabled(
    self, mock_env, m_perf, environment, parking_enabled
  ):
    from dagster import build_op_context

    from robosystems.dagster.jobs.shared_repository import (
      boost_shared_master_volume,
    )

    mock_env.ENVIRONMENT = environment
    mock_env.SHARED_MASTER_PARKING_ENABLED = parking_enabled

    assert boost_shared_master_volume(build_op_context()) == []
    m_perf.assert_not_called()
