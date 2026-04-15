"""Tests for the instance busy counter primitive.

Covers lifecycle semantics (increment on entry, decrement on exit even on
exception), graceful handling of missing instance_id, and swallowing of
DynamoDB failures so a broken counter never blocks real work.
"""

from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from robosystems.middleware.graph import instance_busy as ib


@pytest.fixture
def mock_table():
  """Mock DynamoDB Table with a no-op update_item."""
  table = MagicMock()
  table.update_item = MagicMock(return_value={})
  return table


@pytest.fixture
def mock_resource(mock_table):
  """Mock DynamoDB resource whose Table() returns our mock_table."""
  resource = MagicMock()
  resource.Table = MagicMock(return_value=mock_table)
  return resource


@pytest.fixture
def patched_ddb(mock_resource, mock_table):
  """Patch get_dynamodb_resource to return the mock resource."""
  with patch.object(ib, "get_dynamodb_resource", return_value=mock_resource):
    yield mock_table


class TestInstanceBusyAsync:
  """Async context manager lifecycle and error handling."""

  async def test_increment_on_entry_decrement_on_exit(self, patched_ddb):
    async with ib.instance_busy("i-abc123", ib.OP_KIND_MATERIALIZATION):
      # After entry, exactly one update_item call with delta=+1
      assert patched_ddb.update_item.call_count == 1
      kwargs = patched_ddb.update_item.call_args.kwargs
      assert kwargs["Key"] == {"instance_id": "i-abc123"}
      assert kwargs["ExpressionAttributeValues"][":delta"] == 1
      assert kwargs["ExpressionAttributeValues"][":kind"] == ib.OP_KIND_MATERIALIZATION
      assert "active_destructive_ops" in kwargs["UpdateExpression"]
      assert "last_destructive_op_at" in kwargs["UpdateExpression"]
      assert "last_destructive_op_kind" in kwargs["UpdateExpression"]

    # After exit, second call with delta=-1
    assert patched_ddb.update_item.call_count == 2
    exit_kwargs = patched_ddb.update_item.call_args.kwargs
    assert exit_kwargs["ExpressionAttributeValues"][":delta"] == -1
    assert exit_kwargs["Key"] == {"instance_id": "i-abc123"}

  async def test_decrement_runs_when_body_raises(self, patched_ddb):
    with pytest.raises(ValueError, match="boom"):
      async with ib.instance_busy("i-abc123", ib.OP_KIND_SEC_STAGING):
        raise ValueError("boom")

    # Both increment and decrement should still have run
    assert patched_ddb.update_item.call_count == 2
    assert (
      patched_ddb.update_item.call_args_list[0].kwargs["ExpressionAttributeValues"][
        ":delta"
      ]
      == 1
    )
    assert (
      patched_ddb.update_item.call_args_list[1].kwargs["ExpressionAttributeValues"][
        ":delta"
      ]
      == -1
    )

  async def test_empty_instance_id_is_skipped(self, patched_ddb):
    async with ib.instance_busy("", ib.OP_KIND_BULK_TABLE_CREATE):
      pass
    # No update_item calls at all when instance_id is empty
    assert patched_ddb.update_item.call_count == 0

  async def test_client_error_on_increment_is_swallowed(self, patched_ddb):
    patched_ddb.update_item.side_effect = ClientError(
      error_response={"Error": {"Code": "ThrottlingException", "Message": "slow"}},
      operation_name="UpdateItem",
    )
    # Should NOT raise — a broken counter must not block real work
    async with ib.instance_busy("i-abc123", ib.OP_KIND_MATERIALIZATION):
      pass
    # Both enter and exit were attempted (even though enter failed)
    assert patched_ddb.update_item.call_count == 2

  async def test_generic_exception_on_increment_is_swallowed(self, patched_ddb):
    patched_ddb.update_item.side_effect = RuntimeError("network gone")
    async with ib.instance_busy("i-abc123", ib.OP_KIND_MATERIALIZATION):
      pass
    assert patched_ddb.update_item.call_count == 2

  async def test_client_error_on_decrement_is_swallowed(self, patched_ddb):
    # Increment succeeds, decrement fails — still must not raise
    call_count = {"n": 0}

    def flaky_update(**_kwargs):
      call_count["n"] += 1
      if call_count["n"] == 2:
        raise ClientError(
          error_response={"Error": {"Code": "InternalError", "Message": "x"}},
          operation_name="UpdateItem",
        )
      return {}

    patched_ddb.update_item.side_effect = flaky_update

    async with ib.instance_busy("i-abc123", ib.OP_KIND_EXTENSIONS_MATERIALIZE):
      pass

    assert call_count["n"] == 2

  async def test_body_exception_still_propagates_on_ddb_failure(self, patched_ddb):
    """If DDB fails AND the body raises, the body exception wins."""
    patched_ddb.update_item.side_effect = ClientError(
      error_response={"Error": {"Code": "ThrottlingException", "Message": "x"}},
      operation_name="UpdateItem",
    )
    with pytest.raises(ValueError, match="business error"):
      async with ib.instance_busy("i-abc123", ib.OP_KIND_MATERIALIZATION):
        raise ValueError("business error")

  async def test_table_name_is_instance_registry(self, patched_ddb, mock_resource):
    from robosystems.config import env

    async with ib.instance_busy("i-abc123", ib.OP_KIND_MATERIALIZATION):
      pass
    mock_resource.Table.assert_called_with(env.INSTANCE_REGISTRY_TABLE)


class TestInstanceBusySync:
  """Synchronous variant for sync call sites (Dagster ops)."""

  def test_sync_increment_and_decrement(self, patched_ddb):
    with ib.instance_busy_sync("i-sync", ib.OP_KIND_DAGSTER_MATERIALIZATION):
      assert patched_ddb.update_item.call_count == 1
      assert (
        patched_ddb.update_item.call_args.kwargs["ExpressionAttributeValues"][":delta"]
        == 1
      )

    assert patched_ddb.update_item.call_count == 2
    assert (
      patched_ddb.update_item.call_args.kwargs["ExpressionAttributeValues"][":delta"]
      == -1
    )

  def test_sync_decrements_on_exception(self, patched_ddb):
    with pytest.raises(RuntimeError, match="sync boom"):
      with ib.instance_busy_sync("i-sync", ib.OP_KIND_DAGSTER_MATERIALIZATION):
        raise RuntimeError("sync boom")

    assert patched_ddb.update_item.call_count == 2

  def test_sync_swallows_ddb_failure(self, patched_ddb):
    patched_ddb.update_item.side_effect = ClientError(
      error_response={"Error": {"Code": "ThrottlingException", "Message": "x"}},
      operation_name="UpdateItem",
    )
    # Must not raise
    with ib.instance_busy_sync("i-sync", ib.OP_KIND_SEC_STAGING):
      pass

  def test_sync_skips_empty_instance_id(self, patched_ddb):
    with ib.instance_busy_sync("", ib.OP_KIND_BULK_TABLE_CREATE):
      pass
    assert patched_ddb.update_item.call_count == 0


class TestImperativeHelpers:
  """Imperative begin/end helpers for call sites where a context manager
  would require heavy re-indentation."""

  async def test_begin_increments(self, patched_ddb):
    await ib.begin_destructive_op("i-imp", ib.OP_KIND_MATERIALIZATION)
    assert patched_ddb.update_item.call_count == 1
    assert (
      patched_ddb.update_item.call_args.kwargs["ExpressionAttributeValues"][":delta"]
      == 1
    )

  async def test_end_decrements(self, patched_ddb):
    await ib.end_destructive_op("i-imp", ib.OP_KIND_MATERIALIZATION)
    assert patched_ddb.update_item.call_count == 1
    assert (
      patched_ddb.update_item.call_args.kwargs["ExpressionAttributeValues"][":delta"]
      == -1
    )

  async def test_begin_end_pair(self, patched_ddb):
    """Typical try/finally pair semantic."""
    await ib.begin_destructive_op("i-pair", ib.OP_KIND_SEC_STAGING)
    try:
      pass  # work would happen here
    finally:
      await ib.end_destructive_op("i-pair", ib.OP_KIND_SEC_STAGING)

    assert patched_ddb.update_item.call_count == 2
    deltas = [
      call.kwargs["ExpressionAttributeValues"][":delta"]
      for call in patched_ddb.update_item.call_args_list
    ]
    assert deltas == [1, -1]

  async def test_begin_swallows_ddb_failure(self, patched_ddb):
    patched_ddb.update_item.side_effect = ClientError(
      error_response={"Error": {"Code": "ThrottlingException", "Message": "x"}},
      operation_name="UpdateItem",
    )
    # Must not raise
    await ib.begin_destructive_op("i-imp", ib.OP_KIND_MATERIALIZATION)

  async def test_end_swallows_ddb_failure(self, patched_ddb):
    patched_ddb.update_item.side_effect = ClientError(
      error_response={"Error": {"Code": "ThrottlingException", "Message": "x"}},
      operation_name="UpdateItem",
    )
    await ib.end_destructive_op("i-imp", ib.OP_KIND_MATERIALIZATION)


class TestOpKindConstants:
  """Sanity check that the op_kind labels are stable string constants."""

  def test_op_kinds_are_strings(self):
    assert isinstance(ib.OP_KIND_MATERIALIZATION, str)
    assert isinstance(ib.OP_KIND_DAGSTER_MATERIALIZATION, str)
    assert isinstance(ib.OP_KIND_SEC_STAGING, str)
    assert isinstance(ib.OP_KIND_EXTENSIONS_MATERIALIZE, str)
    assert isinstance(ib.OP_KIND_BULK_TABLE_CREATE, str)

  def test_op_kinds_are_unique(self):
    kinds = {
      ib.OP_KIND_MATERIALIZATION,
      ib.OP_KIND_DAGSTER_MATERIALIZATION,
      ib.OP_KIND_SEC_STAGING,
      ib.OP_KIND_EXTENSIONS_MATERIALIZE,
      ib.OP_KIND_BULK_TABLE_CREATE,
    }
    assert len(kinds) == 5
