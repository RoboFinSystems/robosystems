"""Tests for the graph volume manager Lambda's race-aware attach path.

Covers the four robustness fixes from the 2026-05-08 incident:
  1. Volume-available waiter runs before attach_volume.
  2. attach_volume retries on VolumeInUse (was previously only IncorrectState).
  3. The registry's `databases` field is preserved (not wiped) when the volume
     already has graphs allocated to it.
  4. graph-registry routing update uses the preserved databases list, so
     replacement instances reroute previously-allocated graphs automatically.
"""

from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import boto3
import pytest
from botocore.exceptions import ClientError

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _create_test_instance() -> str:
  """Spin up a moto EC2 instance and return its id (us-east-1c, m7g.large)."""
  ec2 = boto3.client("ec2", region_name="us-east-1")
  # moto auto-creates an AMI; just pass a known one.
  amis = ec2.describe_images(Owners=["amazon"])["Images"]
  ami_id = amis[0]["ImageId"]
  resp = ec2.run_instances(
    ImageId=ami_id,
    MinCount=1,
    MaxCount=1,
    InstanceType="m7g.large",
    Placement={"AvailabilityZone": "us-east-1c"},
  )
  return resp["Instances"][0]["InstanceId"]


def _create_test_volume(size: int = 50) -> str:
  ec2 = boto3.client("ec2", region_name="us-east-1")
  resp = ec2.create_volume(
    AvailabilityZone="us-east-1c",
    Size=size,
    VolumeType="gp3",
    Iops=3000,
    Throughput=125,
    Encrypted=True,
  )
  return resp["VolumeId"]


def _seed_registry(
  table_name: str,
  volume_id: str,
  databases: list[str],
  status: str = "available",
  node_type: str = "writer",
  tier: str = "ladybug-standard",
  instance_id: str = "unattached",
) -> None:
  ddb = boto3.resource("dynamodb", region_name="us-east-1")
  ddb.Table(table_name).put_item(
    Item={
      "volume_id": volume_id,
      "instance_id": instance_id,
      "availability_zone": "us-east-1c",
      "tier": tier,
      "status": status,
      "databases": databases,
      "node_type": node_type,
    }
  )


def _seed_graph_registry(table_name: str, graph_id: str, instance_id: str) -> None:
  ddb = boto3.resource("dynamodb", region_name="us-east-1")
  ddb.Table(table_name).put_item(
    Item={
      "graph_id": graph_id,
      "instance_id": instance_id,
      "private_ip": "10.0.0.99",
      "status": "active",
    }
  )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_attach_waits_for_volume_available_first(gvm):
  """The volume_available waiter must run before attach_volume.

  Without this, EBS detach being asynchronous lets us race the previous
  instance's detach and hit VolumeInUse — exactly today's failure mode.
  """
  instance_id = _create_test_instance()
  volume_id = _create_test_volume()
  _seed_registry("test-volume-registry", volume_id, [])

  call_order: list[str] = []
  real_get_waiter = gvm.ec2.get_waiter

  def tracking_get_waiter(name):
    call_order.append(f"waiter:{name}")
    return real_get_waiter(name)

  real_attach = gvm.ec2.attach_volume

  def tracking_attach(*args, **kwargs):
    call_order.append("attach_volume")
    return real_attach(*args, **kwargs)

  with (
    patch.object(gvm.ec2, "get_waiter", side_effect=tracking_get_waiter),
    patch.object(gvm.ec2, "attach_volume", side_effect=tracking_attach),
  ):
    gvm.attach_and_register_volume(volume_id, instance_id, [])

  # volume_available waiter must come before attach_volume.
  assert "waiter:volume_available" in call_order
  assert call_order.index("waiter:volume_available") < call_order.index("attach_volume")


def test_attach_retries_on_volume_in_use(gvm):
  """VolumeInUse on first attempt must trigger retry, not raise immediately.

  Today's incident: detachment Lambda ack'd, volume-manager called
  attach_volume ~30s later, EC2 returned VolumeInUse, and the old retry guard
  (only IncorrectState) skipped retries → instance launched without a volume.
  """
  instance_id = _create_test_instance()
  volume_id = _create_test_volume()
  _seed_registry("test-volume-registry", volume_id, [])

  vol_in_use = ClientError(
    {
      "Error": {
        "Code": "VolumeInUse",
        "Message": "Volume is already attached to an instance",
      }
    },
    "AttachVolume",
  )
  real_attach = gvm.ec2.attach_volume
  call_count = {"n": 0}

  def flaky_attach(*args, **kwargs):
    call_count["n"] += 1
    if call_count["n"] == 1:
      raise vol_in_use
    return real_attach(*args, **kwargs)

  # Patch sleep to keep the test fast (real path sleeps 30s on VolumeInUse).
  with (
    patch.object(gvm.ec2, "attach_volume", side_effect=flaky_attach),
    patch.object(gvm.time, "sleep") as mock_sleep,
  ):
    result = gvm.attach_and_register_volume(volume_id, instance_id, [])

  assert call_count["n"] == 2, "attach_volume should be called twice (1 fail + 1 OK)"
  assert result["statusCode"] == 200
  # Sleep should have been called with 30s for the VolumeInUse case.
  mock_sleep.assert_called_once_with(30)


def test_attach_raises_after_three_volume_in_use_attempts(gvm):
  """Persistent VolumeInUse should still fail loudly, not silently return null."""
  instance_id = _create_test_instance()
  volume_id = _create_test_volume()
  _seed_registry("test-volume-registry", volume_id, [])

  vol_in_use = ClientError(
    {"Error": {"Code": "VolumeInUse", "Message": "still detaching"}}, "AttachVolume"
  )

  with (
    patch.object(gvm.ec2, "attach_volume", side_effect=vol_in_use),
    patch.object(gvm.time, "sleep"),
    pytest.raises(ClientError),
  ):
    gvm.attach_and_register_volume(volume_id, instance_id, [])


def test_attach_preserves_existing_databases(gvm):
  """The registry's `databases` field must NOT be wiped when caller passes [].

  allocation_manager.py maintains this list when graphs are created/dropped.
  Userdata for non-shared writers always passes []; the old code overwrote the
  registry, breaking volume↔graph linkage on every replacement. The 2026-05-08
  incident orphaned kg19dcbe757481af06fc9b through this exact path.
  """
  instance_id = _create_test_instance()
  volume_id = _create_test_volume()
  _seed_registry("test-volume-registry", volume_id, ["kgexistinggraph123"])

  gvm.attach_and_register_volume(volume_id, instance_id, [])

  ddb = boto3.resource("dynamodb", region_name="us-east-1")
  item = ddb.Table("test-volume-registry").get_item(Key={"volume_id": volume_id})[
    "Item"
  ]
  assert item["databases"] == ["kgexistinggraph123"], (
    "Registry should preserve pre-existing databases list"
  )


def test_attach_adopts_caller_databases_when_registry_empty(gvm):
  """For a genuinely fresh pool volume, the caller's list should be adopted."""
  instance_id = _create_test_instance()
  volume_id = _create_test_volume()
  _seed_registry("test-volume-registry", volume_id, [])

  gvm.attach_and_register_volume(volume_id, instance_id, ["sec"])

  ddb = boto3.resource("dynamodb", region_name="us-east-1")
  item = ddb.Table("test-volume-registry").get_item(Key={"volume_id": volume_id})[
    "Item"
  ]
  assert item["databases"] == ["sec"]


def test_attach_signals_instance_with_preserved_databases(gvm):
  """The SSM signal to the instance must include the preserved databases list.

  Without this, the OS-side /tmp/databases.json gets the caller's [] on every
  replacement — re-introducing the same orphaning bug at the OS level even
  though the registry write now preserves the list correctly.
  """
  instance_id = _create_test_instance()
  volume_id = _create_test_volume()
  preserved = ["kgexistinggraph123"]
  _seed_registry("test-volume-registry", volume_id, preserved)

  with patch.object(gvm.ssm, "send_command") as mock_send:
    result = gvm.attach_and_register_volume(volume_id, instance_id, [])

  assert mock_send.called, "ssm.send_command should be invoked to signal the instance"
  call_kwargs = mock_send.call_args.kwargs
  commands = call_kwargs["Parameters"]["commands"]
  # commands[1] is the databases.json write
  assert "kgexistinggraph123" in commands[1], (
    "SSM signal must contain preserved databases, not the caller's empty list"
  )
  assert "[]" not in commands[1], (
    "SSM signal must NOT contain the empty caller list when registry has data"
  )

  # Function return value must also reflect the preserved list (callers may
  # parse it to register graphs).
  assert result["databases"] == preserved, (
    "Return value must use preserved databases list"
  )


def test_attach_reroutes_graph_registry_using_preserved_databases(gvm):
  """graph-registry must be updated with the preserved (not caller-provided) list.

  This is what makes the routing layer self-heal on replacement instances.
  """
  instance_id = _create_test_instance()
  volume_id = _create_test_volume()
  graph_id = "kgpreexisting12345"
  _seed_registry("test-volume-registry", volume_id, [graph_id])
  _seed_graph_registry("test-graph-registry", graph_id, "i-old-dead-instance")

  gvm.attach_and_register_volume(volume_id, instance_id, [])

  ddb = boto3.resource("dynamodb", region_name="us-east-1")
  item = ddb.Table("test-graph-registry").get_item(Key={"graph_id": graph_id})["Item"]
  assert item["instance_id"] == instance_id, (
    "graph-registry should now point at the new instance even though caller "
    "passed databases=[]"
  )


# ---------------------------------------------------------------------------
# Pre-detach snapshot behaviour
# ---------------------------------------------------------------------------


def _attach(volume_id: str, instance_id: str) -> None:
  boto3.client("ec2", region_name="us-east-1").attach_volume(
    VolumeId=volume_id, InstanceId=instance_id, Device="/dev/xvdf"
  )


def _moto_safe_detach_volume(gvm):
  """Wrap gvm.ec2.detach_volume to fill in InstanceId from attachment state.

  Real AWS infers InstanceId from the volume's current attachment, but moto
  crashes (`len(None)` on InvalidInstanceIdError) when InstanceId is omitted.
  This wrapper mirrors the real behavior locally.
  """
  real_detach = gvm.ec2.detach_volume

  def patched(**kwargs):
    if "InstanceId" not in kwargs:
      vol = gvm.ec2.describe_volumes(VolumeIds=[kwargs["VolumeId"]])["Volumes"][0]
      if vol["Attachments"]:
        kwargs["InstanceId"] = vol["Attachments"][0]["InstanceId"]
    return real_detach(**kwargs)

  return patch.object(gvm.ec2, "detach_volume", side_effect=patched)


def _describe_pre_detach_snapshots(volume_id: str) -> list[dict]:
  ec2 = boto3.client("ec2", region_name="us-east-1")
  resp = ec2.describe_snapshots(
    OwnerIds=["self"],
    Filters=[
      {"Name": "tag:Type", "Values": ["pre_detach"]},
      {"Name": "tag:VolumeId", "Values": [volume_id]},
    ],
  )
  return resp["Snapshots"]


def test_detach_creates_snapshot_for_writer_with_data(gvm):
  """Writer volume holding a graph must get a pre-detach snapshot."""
  instance_id = _create_test_instance()
  volume_id = _create_test_volume()
  _attach(volume_id, instance_id)
  _seed_registry(
    "test-volume-registry",
    volume_id,
    ["kgcustomer1234567"],
    status="attached",
    node_type="writer",
    tier="ladybug-standard",
    instance_id=instance_id,
  )

  with _moto_safe_detach_volume(gvm):
    gvm.detach_volume({"volume_id": volume_id})

  snaps = _describe_pre_detach_snapshots(volume_id)
  assert len(snaps) == 1
  tags = {t["Key"]: t["Value"] for t in snaps[0]["Tags"]}
  assert tags["AutoDelete"] == "true"
  assert tags["RetentionDays"] == "3"
  assert tags["NodeType"] == "writer"
  assert "kgcustomer1234567" in tags["Databases"]


def test_detach_creates_snapshot_for_shared_master(gvm):
  """Shared master holds SEC data — must be snapshotted."""
  instance_id = _create_test_instance()
  volume_id = _create_test_volume(size=300)
  _attach(volume_id, instance_id)
  _seed_registry(
    "test-volume-registry",
    volume_id,
    ["sec"],
    status="attached",
    node_type="shared_master",
    tier="ladybug-shared",
    instance_id=instance_id,
  )

  with _moto_safe_detach_volume(gvm):
    gvm.detach_volume({"volume_id": volume_id})

  snaps = _describe_pre_detach_snapshots(volume_id)
  assert len(snaps) == 1
  tags = {t["Key"]: t["Value"] for t in snaps[0]["Tags"]}
  assert tags["NodeType"] == "shared_master"
  assert "sec" in tags["Databases"]


def test_detach_shared_master_rolls_to_single_snapshot(gvm):
  """Shared master keeps only the newest pre-detach snapshot across park cycles.

  Each detach creates a fresh snapshot then prunes the prior one, so the master
  never accumulates a snapshot per nightly park — at most one exists at a time.
  """
  instance_id = _create_test_instance()
  volume_id = _create_test_volume(size=300)

  def _attach_and_seed():
    _attach(volume_id, instance_id)
    _seed_registry(
      "test-volume-registry",
      volume_id,
      ["sec"],
      status="attached",
      node_type="shared_master",
      tier="ladybug-shared",
      instance_id=instance_id,
    )

  # First park cycle → one snapshot.
  _attach_and_seed()
  with _moto_safe_detach_volume(gvm):
    gvm.detach_volume({"volume_id": volume_id})
  first = _describe_pre_detach_snapshots(volume_id)
  assert len(first) == 1
  first_id = first[0]["SnapshotId"]

  # Second park cycle → new snapshot created, the prior one pruned.
  _attach_and_seed()
  with _moto_safe_detach_volume(gvm):
    gvm.detach_volume({"volume_id": volume_id})

  snaps = _describe_pre_detach_snapshots(volume_id)
  assert len(snaps) == 1, "shared master must retain only the newest snapshot"
  assert snaps[0]["SnapshotId"] != first_id, "prior snapshot should be pruned"


def test_detach_writer_keeps_snapshot_history(gvm):
  """Writers are NOT rolled — each detach retains its own snapshot.

  Dedicated writers hold unique customer data with no guaranteed S3 backup and
  detach only on instance replacement, so the rolling-single prune must not
  touch them.
  """
  instance_id = _create_test_instance()
  volume_id = _create_test_volume()

  def _attach_and_seed():
    _attach(volume_id, instance_id)
    _seed_registry(
      "test-volume-registry",
      volume_id,
      ["kgcustomer1234567"],
      status="attached",
      node_type="writer",
      tier="ladybug-standard",
      instance_id=instance_id,
    )

  for _ in range(2):
    _attach_and_seed()
    with _moto_safe_detach_volume(gvm):
      gvm.detach_volume({"volume_id": volume_id})

  snaps = _describe_pre_detach_snapshots(volume_id)
  assert len(snaps) == 2, "writer snapshots must not be pruned to a single"


def test_detach_skips_snapshot_for_shared_replica(gvm):
  """Shared replicas hydrate from S3 on boot — must NOT be snapshotted.

  In practice replicas don't even reach this code path (no /dev/xvdf, no
  TerminationLifecycleHook), but defense-in-depth: the filter must reject them
  if a future refactor wires them through.
  """
  instance_id = _create_test_instance()
  volume_id = _create_test_volume()
  _attach(volume_id, instance_id)
  _seed_registry(
    "test-volume-registry",
    volume_id,
    ["sec"],  # Even with data — replicas are still skipped.
    status="attached",
    node_type="shared_replica",
    tier="ladybug-shared",
    instance_id=instance_id,
  )

  with _moto_safe_detach_volume(gvm):
    gvm.detach_volume({"volume_id": volume_id})

  snaps = _describe_pre_detach_snapshots(volume_id)
  assert snaps == [], "shared_replica volume must not be snapshotted"


def test_detach_skips_snapshot_for_empty_pool_volume(gvm):
  """Volumes with no databases (spare pool) have nothing worth snapshotting."""
  instance_id = _create_test_instance()
  volume_id = _create_test_volume()
  _attach(volume_id, instance_id)
  _seed_registry(
    "test-volume-registry",
    volume_id,
    [],  # empty pool volume
    status="attached",
    node_type="writer",
    tier="ladybug-standard",
    instance_id=instance_id,
  )

  with _moto_safe_detach_volume(gvm):
    gvm.detach_volume({"volume_id": volume_id})

  snaps = _describe_pre_detach_snapshots(volume_id)
  assert snaps == [], "empty pool volume must not be snapshotted"


def test_detach_proceeds_when_snapshot_creation_fails(gvm):
  """A snapshot failure must not block the detach itself."""
  instance_id = _create_test_instance()
  volume_id = _create_test_volume()
  _attach(volume_id, instance_id)
  _seed_registry(
    "test-volume-registry",
    volume_id,
    ["kgcustomer1234567"],
    status="attached",
    node_type="writer",
    tier="ladybug-standard",
    instance_id=instance_id,
  )

  with (
    patch.object(
      gvm.ec2, "create_snapshot", side_effect=RuntimeError("snapshot api down")
    ),
    patch.object(gvm.sns, "publish") as mock_publish,
    _moto_safe_detach_volume(gvm),
  ):
    result = gvm.detach_volume({"volume_id": volume_id})

  # Detach must have succeeded despite the snapshot failure.
  assert result["statusCode"] == 200
  ddb = boto3.resource("dynamodb", region_name="us-east-1")
  item = ddb.Table("test-volume-registry").get_item(Key={"volume_id": volume_id})[
    "Item"
  ]
  assert item["status"] == "available"
  # And the SNS alert must have fired — that's the only signal that a snapshot
  # failed, so without this we'd lose snapshots silently for high-graph writers.
  assert mock_publish.called, "send_alert(SNS publish) must fire on snapshot failure"
  alert_subject = mock_publish.call_args.kwargs.get("Subject", "")
  assert "Snapshot Failed" in alert_subject


def test_cleanup_snapshots_honors_retention_days_tag(gvm):
  """A snapshot tagged RetentionDays=3 must be deleted at 3 days, not 7.

  Without the per-snapshot tag, all AutoDelete=true snapshots used the global
  RETENTION_DAYS env (7 days), so pre-detach snapshots would linger beyond
  their intended 3-day window.
  """
  ec2 = boto3.client("ec2", region_name="us-east-1")
  vol_3day = _create_test_volume()
  vol_7day = _create_test_volume()

  s3 = ec2.create_snapshot(
    VolumeId=vol_3day,
    Description="3-day retention",
    TagSpecifications=[
      {
        "ResourceType": "snapshot",
        "Tags": [
          {"Key": "Environment", "Value": "test"},
          {"Key": "AutoDelete", "Value": "true"},
          {"Key": "RetentionDays", "Value": "3"},
        ],
      }
    ],
  )
  s7 = ec2.create_snapshot(
    VolumeId=vol_7day,
    Description="default retention (orphan-cleanup style)",
    TagSpecifications=[
      {
        "ResourceType": "snapshot",
        "Tags": [
          {"Key": "Environment", "Value": "test"},
          {"Key": "AutoDelete", "Value": "true"},
        ],
      }
    ],
  )

  # Move both snapshots' StartTime to 5 days ago.
  fake_now = datetime.now(UTC)
  fake_start = fake_now - timedelta(days=5)

  def fake_describe(*args, **kwargs):
    real = ec2.describe_snapshots(*args, **kwargs)
    for snap in real["Snapshots"]:
      snap["StartTime"] = fake_start
    return real

  with patch.object(gvm.ec2, "describe_snapshots", side_effect=fake_describe):
    result = gvm.cleanup_old_snapshots({})

  deleted = result["deleted_snapshots"]
  # The 3-day snapshot is past retention (5 > 3) → deleted.
  # The 7-day default snapshot is within retention (5 < 7) → kept.
  assert s3["SnapshotId"] in deleted
  assert s7["SnapshotId"] not in deleted


# ---------------------------------------------------------------------------
# Atomic volume claiming
#
# Selection is a scan plus a deterministic sort, so concurrent launches in the
# same AZ and tier converge on the same candidate. EC2 stops the double-attach,
# but the loser's retry loop then waits out a volume that will never free and
# fails the instance — which is why the claim has to be atomic before any
# concurrency (batched rolling update, ASG instance refresh, spot reclaim).
# ---------------------------------------------------------------------------


def _registry_item(volume_id: str) -> dict:
  ddb = boto3.resource("dynamodb", region_name="us-east-1")
  return ddb.Table("test-volume-registry").get_item(Key={"volume_id": volume_id})[
    "Item"
  ]


def test_claim_volume_takes_an_available_volume(gvm):
  volume_id = _create_test_volume()
  _seed_registry("test-volume-registry", volume_id, [])

  assert gvm.claim_volume(volume_id, "i-winner") is True

  item = _registry_item(volume_id)
  assert item["status"] == "claiming"
  assert item["instance_id"] == "i-winner"
  assert "claimed_at" in item


def test_claim_volume_loses_to_an_earlier_claim(gvm):
  """The second caller must lose rather than both proceeding to attach."""
  volume_id = _create_test_volume()
  _seed_registry("test-volume-registry", volume_id, [])

  assert gvm.claim_volume(volume_id, "i-first") is True
  assert gvm.claim_volume(volume_id, "i-second") is False

  # The winner keeps it; the loser must not have overwritten the owner.
  assert _registry_item(volume_id)["instance_id"] == "i-first"


def test_claim_volume_loses_to_an_attached_volume(gvm):
  volume_id = _create_test_volume()
  _seed_registry("test-volume-registry", volume_id, [], status="attached")

  assert gvm.claim_volume(volume_id, "i-late") is False


def test_launch_falls_through_when_a_candidate_is_claimed_concurrently(gvm):
  """A lost claim must advance to the next candidate, not fail the instance.

  This is the regression that makes concurrent replacement safe: previously the
  loser called attach_and_register_volume on a volume another instance held,
  burned its retry budget on VolumeInUse, and raised.
  """
  instance_id = _create_test_instance()
  taken = _create_test_volume()
  free = _create_test_volume()
  # Both carry data so they sort into the same preference bucket.
  _seed_registry("test-volume-registry", taken, ["kg_taken"])
  _seed_registry("test-volume-registry", free, ["kg_free"])

  real_claim = gvm.claim_volume
  first = {"seen": False}

  def claim_but_lose_the_first(volume_id, iid):
    """Simulate a concurrent winner taking whichever volume we try first."""
    if not first["seen"]:
      first["seen"] = True
      real_claim(volume_id, "i-someone-else")
      return False
    return real_claim(volume_id, iid)

  with patch.object(gvm, "claim_volume", side_effect=claim_but_lose_the_first):
    result = gvm.handle_instance_launch(
      {"instance_id": instance_id, "node_type": "writer", "tier": "ladybug-standard"}
    )

  assert result["statusCode"] == 200
  # It attached the volume it could actually get, not the contested one.
  assert result["volume_id"] in {taken, free}
  assert _registry_item(result["volume_id"])["instance_id"] == instance_id
  assert _registry_item(result["volume_id"])["status"] == "attached"


def test_attach_failure_releases_the_claim(gvm):
  """A failed attach must return the volume to the pool, not strand it."""
  instance_id = _create_test_instance()
  volume_id = _create_test_volume()
  _seed_registry("test-volume-registry", volume_id, ["kg_a"])

  with patch.object(
    gvm, "attach_and_register_volume", side_effect=RuntimeError("attach exploded")
  ):
    with pytest.raises(RuntimeError):
      gvm.handle_instance_launch(
        {"instance_id": instance_id, "node_type": "writer", "tier": "ladybug-standard"}
      )

  item = _registry_item(volume_id)
  assert item["status"] == "available"
  assert "claimed_at" not in item


def test_release_claim_will_not_undo_someone_elses_claim(gvm):
  volume_id = _create_test_volume()
  _seed_registry("test-volume-registry", volume_id, [])

  gvm.claim_volume(volume_id, "i-owner")
  gvm.release_claim(volume_id, "i-not-the-owner")

  item = _registry_item(volume_id)
  assert item["status"] == "claiming"
  assert item["instance_id"] == "i-owner"


def test_reclaim_stale_claims_frees_a_stranded_volume(gvm):
  """A claim whose Lambda died must not hide the volume forever.

  Scans filter on `available`, so without this sweep a stranded claim takes the
  volume — and the graph it holds — out of circulation permanently.
  """
  volume_id = _create_test_volume()
  _seed_registry("test-volume-registry", volume_id, ["kg_stranded"])
  gvm.claim_volume(volume_id, "i-died")

  stale = (
    datetime.now(UTC) - timedelta(seconds=gvm.STALE_CLAIM_SECONDS + 60)
  ).isoformat()
  ddb = boto3.resource("dynamodb", region_name="us-east-1")
  ddb.Table("test-volume-registry").update_item(
    Key={"volume_id": volume_id},
    UpdateExpression="SET claimed_at = :t",
    ExpressionAttributeValues={":t": stale},
  )

  assert gvm.reclaim_stale_claims("us-east-1c", "ladybug-standard") == 1

  item = _registry_item(volume_id)
  assert item["status"] == "available"
  assert item["databases"] == ["kg_stranded"]


def test_reclaim_leaves_a_fresh_claim_alone(gvm):
  """An in-flight attach must never be stolen out from under itself."""
  volume_id = _create_test_volume()
  _seed_registry("test-volume-registry", volume_id, [])
  gvm.claim_volume(volume_id, "i-still-working")

  assert gvm.reclaim_stale_claims("us-east-1c", "ladybug-standard") == 0
  assert _registry_item(volume_id)["status"] == "claiming"


def test_ordered_candidates_preserves_preference_order(gvm):
  """Matching databases first, then any data, then empty — unchanged from before."""
  empty = {"volume_id": "vol-empty", "databases": []}
  other_data = {"volume_id": "vol-other", "databases": ["kg_other"]}
  matching = {"volume_id": "vol-match", "databases": ["kg_mine"]}

  ordered = gvm.ordered_volume_candidates([empty, other_data, matching], ["kg_mine"])

  assert [v["volume_id"] for v, _ in ordered] == [
    "vol-match",
    "vol-other",
    "vol-empty",
  ]
  # The empty volume registers the caller's databases; the others keep their own.
  assert ordered[0][1] == ["kg_mine"]
  assert ordered[1][1] == ["kg_other"]
  assert ordered[2][1] == ["kg_mine"]


# ---------------------------------------------------------------------------
# Shared-master claim discipline + claim-leak fixes
#
# The atomic-claim fix originally covered only the writer scan path. The
# shared-master SEC branch attached without claiming (two concurrent master
# launches both saw `available` and one burned the VolumeInUse retry loop),
# one attach failure path returned a 500 dict instead of raising (so the
# caller's except-based release never fired and the volume stayed `claiming`),
# and the manual sync_registry action force-flipped in-flight rows.
# ---------------------------------------------------------------------------


def _seed_sec_volume(volume_id: str, status: str = "available") -> None:
  _seed_registry(
    "test-volume-registry",
    volume_id,
    ["sec"],
    status=status,
    node_type="shared_master",
    tier="ladybug-shared",
  )


def test_shared_master_claims_sec_volume_before_attach(gvm):
  instance_id = _create_test_instance()
  volume_id = _create_test_volume()
  _seed_sec_volume(volume_id)

  result = gvm.handle_instance_launch(
    {"instance_id": instance_id, "node_type": "shared_master", "tier": "ladybug-shared"}
  )

  assert result["statusCode"] == 200
  assert result["volume_id"] == volume_id
  item = _registry_item(volume_id)
  assert item["status"] == "attached"
  assert item["instance_id"] == instance_id


def test_shared_master_lost_claim_does_not_attach_the_contested_volume(gvm):
  """A concurrent master already claimed the SEC volume: the loser must not
  attach it (the pre-fix behavior burned the VolumeInUse retry loop and
  failed the boot)."""
  instance_id = _create_test_instance()
  volume_id = _create_test_volume()
  _seed_sec_volume(volume_id)
  # A concurrent launch wins the claim first.
  assert gvm.claim_volume(volume_id, "i-concurrent-master") is True

  result = gvm.handle_instance_launch(
    {"instance_id": instance_id, "node_type": "shared_master", "tier": "ladybug-shared"}
  )

  # The loser fell through to minting a fresh volume; the contested volume
  # still belongs to the concurrent winner.
  assert result["statusCode"] == 200
  assert result["volume_id"] != volume_id
  assert _registry_item(volume_id)["instance_id"] == "i-concurrent-master"


def test_shared_master_attach_failure_releases_the_claim(gvm):
  instance_id = _create_test_instance()
  volume_id = _create_test_volume()
  _seed_sec_volume(volume_id)

  with patch.object(
    gvm, "attach_and_register_volume", side_effect=RuntimeError("attach exploded")
  ):
    with pytest.raises(RuntimeError):
      gvm.handle_instance_launch(
        {
          "instance_id": instance_id,
          "node_type": "shared_master",
          "tier": "ladybug-shared",
        }
      )

  item = _registry_item(volume_id)
  assert item["status"] == "available"
  assert "claimed_at" not in item


def test_instance_waiter_failure_raises_and_releases_the_claim(gvm):
  """The instance_running waiter failure used to *return* a 500 dict, so the
  writer path's except-based release never fired and the volume sat in
  `claiming` — invisible to the pool — while the ASG replacement minted a
  fresh empty volume next to the real one."""
  instance_id = _create_test_instance()
  volume_id = _create_test_volume()
  _seed_registry("test-volume-registry", volume_id, ["kg_a"])

  real_get_waiter = gvm.ec2.get_waiter

  class _FailingWaiter:
    def wait(self, **kwargs):
      raise RuntimeError("instance went to shutting-down")

  def get_waiter(name):
    if name == "instance_running":
      return _FailingWaiter()
    return real_get_waiter(name)

  with patch.object(gvm.ec2, "get_waiter", side_effect=get_waiter):
    with pytest.raises(RuntimeError):
      gvm.handle_instance_launch(
        {"instance_id": instance_id, "node_type": "writer", "tier": "ladybug-standard"}
      )

  item = _registry_item(volume_id)
  assert item["status"] == "available"
  assert "claimed_at" not in item


def _tag_for_sync(volume_id: str) -> None:
  ec2 = boto3.client("ec2", region_name="us-east-1")
  ec2.create_tags(
    Resources=[volume_id],
    Tags=[
      {"Key": "Environment", "Value": "test"},
      {"Key": "ManagedBy", "Value": "GraphVolumeManager"},
    ],
  )


def test_sync_registry_leaves_claiming_volumes_alone(gvm):
  """The manual sync_registry action saw an unattached-in-EC2 volume whose
  registry row said `claiming` and force-flipped it to `available` —
  letting a concurrent launch double-claim it mid-attach."""
  volume_id = _create_test_volume()
  _tag_for_sync(volume_id)
  _seed_registry(
    "test-volume-registry", volume_id, ["kg_a"], status="claiming", instance_id="i-mid"
  )

  result = gvm.sync_registry_with_ec2({})

  assert result["statusCode"] == 200
  item = _registry_item(volume_id)
  assert item["status"] == "claiming"
  assert item["instance_id"] == "i-mid"


def test_sync_registry_leaves_expanding_volumes_alone(gvm):
  volume_id = _create_test_volume()
  _tag_for_sync(volume_id)
  _seed_registry(
    "test-volume-registry",
    volume_id,
    ["kg_a"],
    status="expanding",
    instance_id="i-resizing",
  )

  gvm.sync_registry_with_ec2({})

  assert _registry_item(volume_id)["status"] == "expanding"


def test_sync_registry_still_corrects_genuine_mismatches(gvm):
  """The guard must not neuter the action: a plain attached/available
  mismatch still gets corrected."""
  volume_id = _create_test_volume()
  _tag_for_sync(volume_id)
  _seed_registry(
    "test-volume-registry", volume_id, ["kg_a"], status="attached", instance_id="i-gone"
  )

  gvm.sync_registry_with_ec2({})

  item = _registry_item(volume_id)
  assert item["status"] == "available"
  assert item["instance_id"] == "unattached"


# ---------------------------------------------------------------------------
# Volume performance spec: created to spec, and existing volumes raised to the
# tier floor on every attach (a volume outlives many instances, so a creation-
# time setting alone would never reach the long-lived shared master volume).
# ---------------------------------------------------------------------------


def _volume_perf(volume_id: str) -> tuple[int, int]:
  ec2 = boto3.client("ec2", region_name="us-east-1")
  vol = ec2.describe_volumes(VolumeIds=[volume_id])["Volumes"][0]
  return vol["Iops"], vol["Throughput"]


def _create_test_volume_with_perf(iops: int, throughput: int) -> str:
  ec2 = boto3.client("ec2", region_name="us-east-1")
  resp = ec2.create_volume(
    AvailabilityZone="us-east-1c",
    Size=200,
    VolumeType="gp3",
    Iops=iops,
    Throughput=throughput,
    Encrypted=True,
  )
  return resp["VolumeId"]


def test_reconcile_raises_shared_volume_to_tier_floor(gvm):
  volume_id = _create_test_volume_with_perf(3000, 125)
  _seed_sec_volume(volume_id)

  result = gvm.reconcile_volume_performance(volume_id, "ladybug-shared")

  spec = gvm.TIER_VOLUME_SPEC["ladybug-shared"]
  assert result["reconciled"] is True
  assert _volume_perf(volume_id) == (spec["iops"], spec["throughput"])
  item = _registry_item(volume_id)
  assert int(item["iops"]) == spec["iops"]
  assert int(item["throughput"]) == spec["throughput"]
  assert "last_performance_reconcile" in item


def test_reconcile_never_lowers_a_volume_above_spec(gvm):
  """The spec is a floor: a hand-raised volume must not be clamped down."""
  volume_id = _create_test_volume_with_perf(16000, 1000)
  _seed_sec_volume(volume_id)

  result = gvm.reconcile_volume_performance(volume_id, "ladybug-shared")

  assert result == {"reconciled": False, "reason": "at_or_above_spec"}
  assert _volume_perf(volume_id) == (16000, 1000)


def test_reconcile_is_a_noop_for_baseline_tiers(gvm):
  volume_id = _create_test_volume_with_perf(3000, 125)
  _seed_registry("test-volume-registry", volume_id, ["kg_a"], tier="ladybug-standard")

  result = gvm.reconcile_volume_performance(volume_id, "ladybug-standard")

  assert result["reason"] == "at_or_above_spec"
  assert _volume_perf(volume_id) == (3000, 125)


def test_reconcile_defers_when_a_modification_is_in_flight(gvm):
  """EBS refuses overlapping modifications (a size expansion, or the six-hour
  cooldown after an earlier change). The reconcile must log and step aside,
  never raise into the attach path."""
  volume_id = _create_test_volume_with_perf(3000, 125)
  _seed_sec_volume(volume_id)
  err = ClientError(
    {"Error": {"Code": "IncorrectModificationState", "Message": "in progress"}},
    "ModifyVolume",
  )

  with patch.object(gvm.ec2, "modify_volume", side_effect=err):
    result = gvm.reconcile_volume_performance(volume_id, "ladybug-shared")

  assert result == {"reconciled": False, "reason": "IncorrectModificationState"}
  assert _volume_perf(volume_id) == (3000, 125)


def test_shared_master_launch_raises_existing_sec_volume_on_attach(gvm):
  """The real target: the persistent SEC volume, minted at baseline long ago,
  is raised to the current floor when the master next wakes and reattaches."""
  instance_id = _create_test_instance()
  volume_id = _create_test_volume_with_perf(3000, 125)
  _seed_sec_volume(volume_id)

  result = gvm.handle_instance_launch(
    {"instance_id": instance_id, "node_type": "shared_master", "tier": "ladybug-shared"}
  )

  spec = gvm.TIER_VOLUME_SPEC["ladybug-shared"]
  assert result["statusCode"] == 200
  assert result["volume_id"] == volume_id
  assert _volume_perf(volume_id) == (spec["iops"], spec["throughput"])
  assert _registry_item(volume_id)["status"] == "attached"


def test_shared_master_launch_still_attaches_when_reconcile_is_deferred(gvm):
  instance_id = _create_test_instance()
  volume_id = _create_test_volume_with_perf(3000, 125)
  _seed_sec_volume(volume_id)
  err = ClientError(
    {"Error": {"Code": "IncorrectModificationState", "Message": "in progress"}},
    "ModifyVolume",
  )

  with patch.object(gvm.ec2, "modify_volume", side_effect=err):
    result = gvm.handle_instance_launch(
      {
        "instance_id": instance_id,
        "node_type": "shared_master",
        "tier": "ladybug-shared",
      }
    )

  assert result["statusCode"] == 200
  assert result["volume_id"] == volume_id
  assert _registry_item(volume_id)["status"] == "attached"
  assert _volume_perf(volume_id) == (3000, 125)


def test_new_shared_volume_is_created_to_spec(gvm):
  instance_id = _create_test_instance()

  result = gvm.handle_instance_launch(
    {"instance_id": instance_id, "node_type": "shared_master", "tier": "ladybug-shared"}
  )

  spec = gvm.TIER_VOLUME_SPEC["ladybug-shared"]
  assert result["statusCode"] == 200
  assert _volume_perf(result["volume_id"]) == (spec["iops"], spec["throughput"])
  item = _registry_item(result["volume_id"])
  assert int(item["iops"]) == spec["iops"]
  assert int(item["throughput"]) == spec["throughput"]


def test_new_writer_volume_stays_at_baseline(gvm):
  instance_id = _create_test_instance()

  result = gvm.handle_instance_launch(
    {
      "instance_id": instance_id,
      "node_type": "writer",
      "tier": "ladybug-standard",
      "databases": ["kg_new"],
    }
  )

  assert result["statusCode"] == 200
  assert _volume_perf(result["volume_id"]) == (3000, 125)
