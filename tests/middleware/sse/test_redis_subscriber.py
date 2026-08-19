"""Unit tests for the Redis pub/sub subscriber module."""

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from robosystems.middleware.sse.event_storage import EventType, SSEEvent
from robosystems.middleware.sse.redis_subscriber import (
  RedisEventSubscriber,
  get_redis_subscriber,
  start_redis_subscriber,
  stop_redis_subscriber,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def subscriber():
  """Create a fresh RedisEventSubscriber instance."""
  return RedisEventSubscriber()


@pytest.fixture
def mock_pubsub():
  """Create a mock Redis PubSub object."""
  pubsub = AsyncMock()
  pubsub.subscribe = AsyncMock()
  pubsub.unsubscribe = AsyncMock()
  pubsub.close = AsyncMock()
  pubsub.get_message = AsyncMock(return_value=None)  # accepts timeout= kwarg
  return pubsub


@pytest.fixture
def mock_redis_client(mock_pubsub):
  """Create a mock async Redis client.

  Note: pubsub() is a synchronous method on the redis.asyncio.Redis client,
  so we use MagicMock for it rather than AsyncMock.
  """
  client = MagicMock()
  client.pubsub.return_value = mock_pubsub
  client.close = AsyncMock()
  return client


@pytest.fixture
def started_subscriber(subscriber, mock_redis_client, mock_pubsub):
  """Create a subscriber that has been "started" with mocked Redis."""
  subscriber.redis_client = mock_redis_client
  subscriber.pubsub = mock_pubsub
  subscriber._running = True
  return subscriber


@pytest.fixture(autouse=True)
def reset_global_subscriber():
  """Reset the global subscriber singleton between tests."""
  import robosystems.middleware.sse.redis_subscriber as mod

  original = mod._redis_subscriber
  mod._redis_subscriber = None
  yield
  mod._redis_subscriber = original


def _make_sse_event_dict(
  event_type=EventType.OPERATION_PROGRESS,
  operation_id="op-123",
  sequence_number=1,
):
  """Helper to create a serialized SSEEvent dict."""
  return {
    "event_type": event_type.value if hasattr(event_type, "value") else event_type,
    "operation_id": operation_id,
    "timestamp": "2024-01-01T00:00:00Z",
    "data": {"message": "test progress"},
    "sequence_number": sequence_number,
  }


# ---------------------------------------------------------------------------
# RedisEventSubscriber.__init__
# ---------------------------------------------------------------------------


class TestRedisEventSubscriberInit:
  @pytest.mark.unit
  def test_initial_state(self, subscriber):
    assert subscriber.redis_client is None
    assert subscriber.pubsub is None
    assert subscriber.subscriptions == {}
    assert subscriber._running is False
    assert subscriber._task is None


# ---------------------------------------------------------------------------
# RedisEventSubscriber.start
# ---------------------------------------------------------------------------


class TestStart:
  @pytest.mark.unit
  async def test_start_creates_client_and_pubsub(self, subscriber):
    mock_client = MagicMock()
    mock_ps = AsyncMock()
    mock_client.pubsub.return_value = mock_ps
    mock_client.close = AsyncMock()

    with patch(
      "robosystems.middleware.sse.redis_subscriber.create_async_redis_client",
      return_value=mock_client,
    ):
      await subscriber.start()

    assert subscriber.redis_client is mock_client
    assert subscriber.pubsub is mock_ps
    assert subscriber._running is True
    assert subscriber._task is not None

    # Cleanup
    subscriber._running = False
    if subscriber._task:
      subscriber._task.cancel()
      try:
        await subscriber._task
      except asyncio.CancelledError:
        pass

  @pytest.mark.unit
  async def test_start_when_already_running_is_noop(self, started_subscriber):
    original_client = started_subscriber.redis_client
    with patch(
      "robosystems.middleware.sse.redis_subscriber.create_async_redis_client",
    ) as mock_create:
      await started_subscriber.start()
      mock_create.assert_not_called()
    assert started_subscriber.redis_client is original_client


# ---------------------------------------------------------------------------
# RedisEventSubscriber.stop
# ---------------------------------------------------------------------------


class TestStop:
  @pytest.mark.unit
  async def test_stop_cleans_up_resources(
    self, started_subscriber, mock_pubsub, mock_redis_client
  ):
    # Create a task to cancel
    async def dummy():
      await asyncio.sleep(100)

    started_subscriber._task = asyncio.create_task(dummy())

    await started_subscriber.stop()

    assert started_subscriber._running is False
    mock_pubsub.unsubscribe.assert_called_once()
    mock_pubsub.close.assert_called_once()
    mock_redis_client.close.assert_called_once()

  @pytest.mark.unit
  async def test_stop_when_not_running_is_noop(self, subscriber):
    await subscriber.stop()
    # No error, nothing happens

  @pytest.mark.unit
  async def test_stop_without_pubsub(self, subscriber):
    subscriber._running = True
    subscriber.pubsub = None
    subscriber.redis_client = None
    subscriber._task = None
    await subscriber.stop()
    assert subscriber._running is False

  @pytest.mark.unit
  async def test_stop_handles_task_cancellation(
    self, started_subscriber, mock_pubsub, mock_redis_client
  ):
    cancelled_task = AsyncMock()
    cancelled_task.cancel = MagicMock()
    # Simulate CancelledError when awaiting
    cancelled_task.__await__ = MagicMock(side_effect=asyncio.CancelledError)

    # Use a real task to test cancel behavior
    async def dummy():
      await asyncio.sleep(100)

    started_subscriber._task = asyncio.create_task(dummy())
    await started_subscriber.stop()
    assert started_subscriber._running is False


# ---------------------------------------------------------------------------
# RedisEventSubscriber.subscribe_to_operation
# ---------------------------------------------------------------------------


class TestSubscribeToOperation:
  @pytest.mark.unit
  async def test_subscribes_to_channel(self, started_subscriber, mock_pubsub):
    await started_subscriber.subscribe_to_operation("op-123")
    mock_pubsub.subscribe.assert_called_once_with("sse:events:op-123")
    assert "sse:events:op-123" in started_subscriber.subscriptions

  @pytest.mark.unit
  async def test_no_duplicate_subscription(self, started_subscriber, mock_pubsub):
    await started_subscriber.subscribe_to_operation("op-123")
    await started_subscriber.subscribe_to_operation("op-123")
    # Only called once
    mock_pubsub.subscribe.assert_called_once()

  @pytest.mark.unit
  async def test_multiple_operations(self, started_subscriber, mock_pubsub):
    await started_subscriber.subscribe_to_operation("op-1")
    await started_subscriber.subscribe_to_operation("op-2")
    assert len(started_subscriber.subscriptions) == 2
    assert mock_pubsub.subscribe.call_count == 2

  @pytest.mark.unit
  async def test_subscribe_without_pubsub_logs_error(self, subscriber):
    subscriber.pubsub = None
    # Should not raise
    await subscriber.subscribe_to_operation("op-no-pubsub")
    assert len(subscriber.subscriptions) == 0


# ---------------------------------------------------------------------------
# RedisEventSubscriber.unsubscribe_from_operation
# ---------------------------------------------------------------------------


class TestUnsubscribeFromOperation:
  @pytest.mark.unit
  async def test_unsubscribes_from_channel(self, started_subscriber, mock_pubsub):
    started_subscriber.subscriptions["sse:events:op-123"] = True
    await started_subscriber.unsubscribe_from_operation("op-123")
    mock_pubsub.unsubscribe.assert_called_once_with("sse:events:op-123")
    assert "sse:events:op-123" not in started_subscriber.subscriptions

  @pytest.mark.unit
  async def test_unsubscribe_nonexistent_channel_is_noop(
    self, started_subscriber, mock_pubsub
  ):
    await started_subscriber.unsubscribe_from_operation("op-nonexistent")
    mock_pubsub.unsubscribe.assert_not_called()

  @pytest.mark.unit
  async def test_unsubscribe_without_pubsub(self, subscriber):
    subscriber.pubsub = None
    # Should not raise
    await subscriber.unsubscribe_from_operation("op-123")


# ---------------------------------------------------------------------------
# RedisEventSubscriber._listen_for_events
# ---------------------------------------------------------------------------


class TestListenForEvents:
  @pytest.mark.unit
  async def test_get_message_carries_its_own_blocking_timeout(
    self, started_subscriber, mock_pubsub
  ):
    """The timeout must be `get_message`'s own argument. With redis-py's default
    (`timeout=0.0`) the call returns immediately, and the `continue` on a None
    message spins the listener at full speed whenever any stream is open —
    ~37% of a core, measured. Wrapping an instantly-returning call in
    `wait_for` throttles nothing, which is how this ran for months."""
    seen: list[dict] = []

    async def mock_get_message(ignore_subscribe_messages=True, timeout=None):
      seen.append({"ignore": ignore_subscribe_messages, "timeout": timeout})
      started_subscriber._running = False
      return None

    mock_pubsub.get_message = mock_get_message
    started_subscriber.subscriptions["sse:events:op-123"] = True

    await started_subscriber._listen_for_events()

    assert seen, "listener never polled"
    assert seen[0]["timeout"] is not None and seen[0]["timeout"] > 0

  @pytest.mark.unit
  async def test_processes_valid_message(self, started_subscriber, mock_pubsub):
    """Test that a valid Redis message is parsed and broadcast."""
    event_dict = _make_sse_event_dict()
    message = {
      "type": "message",
      "channel": "sse:events:op-123",
      "data": json.dumps(event_dict),
    }

    call_count = 0

    async def mock_get_message(ignore_subscribe_messages=True, timeout=None):
      nonlocal call_count
      call_count += 1
      if call_count == 1:
        return message
      # Stop the listener after first message
      started_subscriber._running = False
      return None

    mock_pubsub.get_message = mock_get_message
    started_subscriber.subscriptions["sse:events:op-123"] = True

    mock_conn_manager = AsyncMock()
    mock_conn_manager.broadcast_event = AsyncMock()

    with patch(
      "robosystems.middleware.sse.redis_subscriber.get_connection_manager",
      return_value=mock_conn_manager,
    ):
      await started_subscriber._listen_for_events()

    mock_conn_manager.broadcast_event.assert_called_once()
    broadcast_call = mock_conn_manager.broadcast_event.call_args
    assert broadcast_call[0][0] == "op-123"
    event = broadcast_call[0][1]
    assert isinstance(event, SSEEvent)
    assert event.operation_id == "op-123"

  @pytest.mark.unit
  async def test_skips_non_sse_channels(self, started_subscriber, mock_pubsub):
    """Test that messages from non-SSE channels are ignored."""
    message = {
      "type": "message",
      "channel": "other:channel:123",
      "data": json.dumps({"key": "value"}),
    }

    call_count = 0

    async def mock_get_message(ignore_subscribe_messages=True, timeout=None):
      nonlocal call_count
      call_count += 1
      if call_count == 1:
        return message
      started_subscriber._running = False
      return None

    mock_pubsub.get_message = mock_get_message
    started_subscriber.subscriptions["other:channel:123"] = True

    mock_conn_manager = AsyncMock()

    with patch(
      "robosystems.middleware.sse.redis_subscriber.get_connection_manager",
      return_value=mock_conn_manager,
    ):
      await started_subscriber._listen_for_events()

    mock_conn_manager.broadcast_event.assert_not_called()

  @pytest.mark.unit
  async def test_handles_malformed_json(self, started_subscriber, mock_pubsub):
    """Test that malformed JSON data is handled gracefully."""
    message = {
      "type": "message",
      "channel": "sse:events:op-bad",
      "data": "NOT VALID JSON{{{",
    }

    call_count = 0

    async def mock_get_message(ignore_subscribe_messages=True, timeout=None):
      nonlocal call_count
      call_count += 1
      if call_count == 1:
        return message
      started_subscriber._running = False
      return None

    mock_pubsub.get_message = mock_get_message
    started_subscriber.subscriptions["sse:events:op-bad"] = True

    mock_conn_manager = AsyncMock()

    with patch(
      "robosystems.middleware.sse.redis_subscriber.get_connection_manager",
      return_value=mock_conn_manager,
    ):
      # Should not raise
      await started_subscriber._listen_for_events()

    mock_conn_manager.broadcast_event.assert_not_called()

  @pytest.mark.unit
  async def test_handles_invalid_event_data(self, started_subscriber, mock_pubsub):
    """Test that valid JSON but invalid SSEEvent data is handled."""
    message = {
      "type": "message",
      "channel": "sse:events:op-invalid",
      "data": json.dumps({"not": "an SSE event"}),
    }

    call_count = 0

    async def mock_get_message(ignore_subscribe_messages=True, timeout=None):
      nonlocal call_count
      call_count += 1
      if call_count == 1:
        return message
      started_subscriber._running = False
      return None

    mock_pubsub.get_message = mock_get_message
    started_subscriber.subscriptions["sse:events:op-invalid"] = True

    mock_conn_manager = AsyncMock()

    with patch(
      "robosystems.middleware.sse.redis_subscriber.get_connection_manager",
      return_value=mock_conn_manager,
    ):
      # Should not raise (caught by except Exception)
      await started_subscriber._listen_for_events()

    mock_conn_manager.broadcast_event.assert_not_called()

  @pytest.mark.unit
  async def test_sleeps_when_no_subscriptions(self, started_subscriber, mock_pubsub):
    """Test that listener sleeps when there are no subscriptions."""
    started_subscriber.subscriptions = {}  # No subscriptions

    call_count = 0

    async def mock_get_message(ignore_subscribe_messages=True, timeout=None):
      # Should not be called since there are no subscriptions
      raise AssertionError("get_message should not be called with no subscriptions")

    mock_pubsub.get_message = mock_get_message

    # Stop after a brief run
    original_sleep = asyncio.sleep

    async def limited_sleep(duration):
      nonlocal call_count
      call_count += 1
      if call_count >= 2:
        started_subscriber._running = False
      await original_sleep(0)

    mock_conn_manager = AsyncMock()

    with (
      patch(
        "robosystems.middleware.sse.redis_subscriber.get_connection_manager",
        return_value=mock_conn_manager,
      ),
      patch("asyncio.sleep", side_effect=limited_sleep),
    ):
      await started_subscriber._listen_for_events()

    mock_conn_manager.broadcast_event.assert_not_called()

  @pytest.mark.unit
  async def test_sleeps_when_pubsub_is_none(self, started_subscriber):
    """Test that listener sleeps when pubsub becomes None."""
    started_subscriber.subscriptions = {"sse:events:op-1": True}
    started_subscriber.pubsub = None

    call_count = 0
    original_sleep = asyncio.sleep

    async def limited_sleep(duration):
      nonlocal call_count
      call_count += 1
      if call_count >= 2:
        started_subscriber._running = False
      await original_sleep(0)

    mock_conn_manager = AsyncMock()

    with (
      patch(
        "robosystems.middleware.sse.redis_subscriber.get_connection_manager",
        return_value=mock_conn_manager,
      ),
      patch("asyncio.sleep", side_effect=limited_sleep),
    ):
      await started_subscriber._listen_for_events()

  @pytest.mark.unit
  async def test_handles_timeout_gracefully(self, started_subscriber, mock_pubsub):
    """Test that TimeoutError from get_message is handled gracefully."""
    call_count = 0

    async def mock_get_message(ignore_subscribe_messages=True, timeout=None):
      nonlocal call_count
      call_count += 1
      if call_count <= 1:
        raise TimeoutError("wait_for timeout")
      started_subscriber._running = False
      return None

    mock_pubsub.get_message = mock_get_message
    started_subscriber.subscriptions["sse:events:op-1"] = True

    mock_conn_manager = AsyncMock()

    with patch(
      "robosystems.middleware.sse.redis_subscriber.get_connection_manager",
      return_value=mock_conn_manager,
    ):
      # Should not raise
      await started_subscriber._listen_for_events()

  @pytest.mark.unit
  async def test_handles_cancelled_error(self, started_subscriber, mock_pubsub):
    """Test that CancelledError exits the listener cleanly."""

    async def mock_get_message(ignore_subscribe_messages=True, timeout=None):
      raise asyncio.CancelledError()

    mock_pubsub.get_message = mock_get_message
    started_subscriber.subscriptions["sse:events:op-1"] = True

    mock_conn_manager = AsyncMock()

    with patch(
      "robosystems.middleware.sse.redis_subscriber.get_connection_manager",
      return_value=mock_conn_manager,
    ):
      # Should exit cleanly without raising
      await started_subscriber._listen_for_events()

  @pytest.mark.unit
  async def test_handles_general_exception_with_retry(
    self, started_subscriber, mock_pubsub
  ):
    """Test that general exceptions cause a brief sleep and retry."""
    call_count = 0

    async def mock_get_message(ignore_subscribe_messages=True, timeout=None):
      nonlocal call_count
      call_count += 1
      if call_count == 1:
        raise ConnectionError("Redis disconnected")
      started_subscriber._running = False
      return None

    mock_pubsub.get_message = mock_get_message
    started_subscriber.subscriptions["sse:events:op-1"] = True

    mock_conn_manager = AsyncMock()

    with patch(
      "robosystems.middleware.sse.redis_subscriber.get_connection_manager",
      return_value=mock_conn_manager,
    ):
      await started_subscriber._listen_for_events()

    # It should have continued past the error (call_count should be 2)
    assert call_count == 2

  @pytest.mark.unit
  async def test_skips_none_messages(self, started_subscriber, mock_pubsub):
    """Test that None messages from get_message are skipped."""
    call_count = 0

    async def mock_get_message(ignore_subscribe_messages=True, timeout=None):
      nonlocal call_count
      call_count += 1
      if call_count <= 2:
        return None
      started_subscriber._running = False
      return None

    mock_pubsub.get_message = mock_get_message
    started_subscriber.subscriptions["sse:events:op-1"] = True

    mock_conn_manager = AsyncMock()

    with patch(
      "robosystems.middleware.sse.redis_subscriber.get_connection_manager",
      return_value=mock_conn_manager,
    ):
      await started_subscriber._listen_for_events()

    mock_conn_manager.broadcast_event.assert_not_called()
    assert call_count >= 2

  @pytest.mark.unit
  async def test_extracts_operation_id_from_channel(
    self, started_subscriber, mock_pubsub
  ):
    """Test correct operation_id extraction from channel name."""
    op_id = "my-complex-op-id-with-dashes"
    event_dict = _make_sse_event_dict(operation_id=op_id)
    message = {
      "type": "message",
      "channel": f"sse:events:{op_id}",
      "data": json.dumps(event_dict),
    }

    call_count = 0

    async def mock_get_message(ignore_subscribe_messages=True, timeout=None):
      nonlocal call_count
      call_count += 1
      if call_count == 1:
        return message
      started_subscriber._running = False
      return None

    mock_pubsub.get_message = mock_get_message
    started_subscriber.subscriptions[f"sse:events:{op_id}"] = True

    mock_conn_manager = AsyncMock()
    mock_conn_manager.broadcast_event = AsyncMock()

    with patch(
      "robosystems.middleware.sse.redis_subscriber.get_connection_manager",
      return_value=mock_conn_manager,
    ):
      await started_subscriber._listen_for_events()

    broadcast_call = mock_conn_manager.broadcast_event.call_args
    assert broadcast_call[0][0] == op_id


# ---------------------------------------------------------------------------
# Module-level functions
# ---------------------------------------------------------------------------


class TestGetRedisSubscriber:
  @pytest.mark.unit
  def test_returns_instance(self):
    subscriber = get_redis_subscriber()
    assert isinstance(subscriber, RedisEventSubscriber)

  @pytest.mark.unit
  def test_returns_same_instance(self):
    sub1 = get_redis_subscriber()
    sub2 = get_redis_subscriber()
    assert sub1 is sub2

  @pytest.mark.unit
  def test_creates_new_after_reset(self):
    import robosystems.middleware.sse.redis_subscriber as mod

    sub1 = get_redis_subscriber()
    mod._redis_subscriber = None
    sub2 = get_redis_subscriber()
    assert sub1 is not sub2


class TestStartRedisSubscriber:
  @pytest.mark.unit
  async def test_starts_global_subscriber(self):
    mock_client = MagicMock()
    mock_ps = AsyncMock()
    mock_client.pubsub.return_value = mock_ps
    mock_client.close = AsyncMock()

    with patch(
      "robosystems.middleware.sse.redis_subscriber.create_async_redis_client",
      return_value=mock_client,
    ):
      await start_redis_subscriber()

    subscriber = get_redis_subscriber()
    assert subscriber._running is True

    # Cleanup
    subscriber._running = False
    if subscriber._task:
      subscriber._task.cancel()
      try:
        await subscriber._task
      except asyncio.CancelledError:
        pass


class TestStopRedisSubscriber:
  @pytest.mark.unit
  async def test_stops_global_subscriber(self):
    subscriber = get_redis_subscriber()
    subscriber._running = True
    subscriber.pubsub = AsyncMock()
    subscriber.redis_client = AsyncMock()
    subscriber._task = None

    await stop_redis_subscriber()
    assert subscriber._running is False
