"""Tests for the instance busy-lease primitive, against a moto DynamoDB table.

Covers the lease lifecycle (added on entry, removed on exit even on exception),
per-lease staleness (a leaked lease expires on its own clock however busy the
instance stays), graceful handling of a missing instance_id, and swallowing of
DynamoDB failures so a broken signal never blocks real work.
"""

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_aws

from robosystems.config import env
from robosystems.middleware.graph import instance_busy as ib

pytestmark = pytest.mark.unit

INSTANCE = "i-abc123"


@pytest.fixture
def table(monkeypatch):
  monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
  monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
  with mock_aws():
    resource = boto3.resource("dynamodb", region_name="us-east-1")
    tbl = resource.create_table(
      TableName=env.INSTANCE_REGISTRY_TABLE,
      KeySchema=[{"AttributeName": "instance_id", "KeyType": "HASH"}],
      AttributeDefinitions=[{"AttributeName": "instance_id", "AttributeType": "S"}],
      BillingMode="PAY_PER_REQUEST",
    )
    tbl.put_item(Item={"instance_id": INSTANCE, "status": "healthy"})
    with patch.object(ib, "get_dynamodb_resource", return_value=resource):
      yield tbl


def _leases(tbl) -> set[str]:
  item = tbl.get_item(Key={"instance_id": INSTANCE}).get("Item", {})
  return set(item.get(ib.LEASE_ATTRIBUTE, set()))


def _lease_started(hours_ago: float, kind: str = "materialization") -> str:
  started = datetime.now(UTC) - timedelta(hours=hours_ago)
  return f"{started.isoformat(timespec='seconds')}|{kind}|deadbeef0000"


class TestLeaseLifecycle:
  async def test_lease_held_for_the_block_and_released_after(self, table):
    async with ib.instance_busy(INSTANCE, ib.OP_KIND_MATERIALIZATION):
      (lease,) = _leases(table)
      assert lease.split("|")[1] == ib.OP_KIND_MATERIALIZATION
    assert _leases(table) == set()

  async def test_lease_released_when_body_raises(self, table):
    with pytest.raises(ValueError, match="boom"):
      async with ib.instance_busy(INSTANCE, ib.OP_KIND_SEC_STAGING):
        raise ValueError("boom")
    assert _leases(table) == set()

  async def test_concurrent_ops_hold_separate_leases(self, table):
    async with ib.instance_busy(INSTANCE, ib.OP_KIND_MATERIALIZATION):
      async with ib.instance_busy(INSTANCE, ib.OP_KIND_BULK_TABLE_INSERT):
        assert len(_leases(table)) == 2
      (remaining,) = _leases(table)
      assert remaining.split("|")[1] == ib.OP_KIND_MATERIALIZATION
    assert _leases(table) == set()

  def test_sync_variant(self, table):
    with ib.instance_busy_sync(INSTANCE, ib.OP_KIND_DAGSTER_MATERIALIZATION):
      assert len(_leases(table)) == 1
    assert _leases(table) == set()

  async def test_begin_returns_the_lease_end_releases(self, table):
    lease = await ib.begin_destructive_op(INSTANCE, ib.OP_KIND_SEC_STAGING)
    assert _leases(table) == {lease}
    await ib.end_destructive_op(INSTANCE, lease)
    assert _leases(table) == set()

  async def test_end_releases_only_its_own_lease(self, table):
    first = await ib.begin_destructive_op(INSTANCE, ib.OP_KIND_MATERIALIZATION)
    second = await ib.begin_destructive_op(INSTANCE, ib.OP_KIND_MATERIALIZATION)
    await ib.end_destructive_op(INSTANCE, first)
    assert _leases(table) == {second}

  async def test_empty_lease_or_instance_is_a_no_op(self, table):
    async with ib.instance_busy("", ib.OP_KIND_BULK_TABLE_CREATE):
      pass
    await ib.end_destructive_op(INSTANCE, "")
    assert await ib.begin_destructive_op("", ib.OP_KIND_MATERIALIZATION) == ""
    assert _leases(table) == set()


class TestStaleness:
  """The failure the counter had: a leak kept alive by other ops' heartbeats."""

  async def test_leaked_lease_is_pruned_by_the_next_acquire(self, table):
    leaked = _lease_started(hours_ago=7)
    table.update_item(
      Key={"instance_id": INSTANCE},
      UpdateExpression=f"ADD {ib.LEASE_ATTRIBUTE} :l",
      ExpressionAttributeValues={":l": {leaked}},
    )
    async with ib.instance_busy(INSTANCE, ib.OP_KIND_MATERIALIZATION):
      assert leaked not in _leases(table)
    assert _leases(table) == set()

  async def test_live_lease_survives_other_ops(self, table):
    live = _lease_started(hours_ago=5)
    table.update_item(
      Key={"instance_id": INSTANCE},
      UpdateExpression=f"ADD {ib.LEASE_ATTRIBUTE} :l",
      ExpressionAttributeValues={":l": {live}},
    )
    async with ib.instance_busy(INSTANCE, ib.OP_KIND_MATERIALIZATION):
      pass
    assert _leases(table) == {live}

  def test_unparseable_lease_is_stale(self):
    assert ib._is_stale("not-a-timestamp|materialization|x", now=0)


class TestFailuresNeverBlockWork:
  @pytest.fixture
  def failing(self):
    tbl = MagicMock()
    tbl.update_item.side_effect = ClientError(
      error_response={"Error": {"Code": "ThrottlingException", "Message": "slow"}},
      operation_name="UpdateItem",
    )
    resource = MagicMock()
    resource.Table.return_value = tbl
    with patch.object(ib, "get_dynamodb_resource", return_value=resource):
      yield tbl

  async def test_acquire_failure_is_swallowed(self, failing):
    async with ib.instance_busy(INSTANCE, ib.OP_KIND_MATERIALIZATION):
      pass
    # The failed acquire wrote nothing, so there is nothing to release.
    assert failing.update_item.call_count == 1

  async def test_begin_failure_returns_no_lease(self, failing):
    assert await ib.begin_destructive_op(INSTANCE, ib.OP_KIND_MATERIALIZATION) == ""

  async def test_release_failure_is_swallowed(self, failing):
    await ib.end_destructive_op(INSTANCE, _lease_started(hours_ago=0))

  def test_sync_failure_is_swallowed(self, failing):
    with ib.instance_busy_sync(INSTANCE, ib.OP_KIND_SEC_STAGING):
      pass

  async def test_body_exception_wins_over_ddb_failure(self, failing):
    with pytest.raises(ValueError, match="business error"):
      async with ib.instance_busy(INSTANCE, ib.OP_KIND_MATERIALIZATION):
        raise ValueError("business error")


class TestResolveInstanceIdForGraph:
  """Resolver for orchestration callers that have a graph_id but no client."""

  @staticmethod
  def _factory(create_client):
    mock_factory = MagicMock()
    mock_factory.create_client = create_client
    return patch.dict(
      "sys.modules",
      {
        "robosystems.graph_api.client.factory": MagicMock(
          GraphClientFactory=mock_factory
        )
      },
    )

  @staticmethod
  def _client(instance_id, closes):
    client = MagicMock()
    client._instance_id = instance_id

    async def _close():
      closes.append(True)

    client.close = _close
    return client

  async def test_returns_the_client_instance_id_and_closes_it(self):
    closes: list[bool] = []
    client = self._client("i-happy", closes)

    async def create(**_kwargs):
      return client

    with self._factory(create):
      assert await ib.resolve_instance_id_for_graph("kg_test") == "i-happy"
    assert closes == [True]

  async def test_missing_instance_id_is_empty(self):
    client = self._client(None, [])

    async def create(**_kwargs):
      return client

    with self._factory(create):
      assert await ib.resolve_instance_id_for_graph("kg_test") == ""

  async def test_factory_failure_is_empty(self):
    async def create(**_kwargs):
      raise RuntimeError("allocation manager down")

    with self._factory(create):
      assert await ib.resolve_instance_id_for_graph("kg_test") == ""
