from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from robosystems.operations.graph.graph_creation_service import (
  GraphCreationConfig,
  GraphCreationService,
)


def _config():
  return GraphCreationConfig(
    user_id="u1", tier="ladybug-standard", graph_name="G", graph_type="generic"
  )


@pytest.mark.asyncio
async def test_create_credits_does_not_retry_deterministic_valueerror():
  """A config ValueError (unknown tier / disallowed combo) can never succeed on
  retry — it must return immediately without burning the backoff sleeps."""
  service = GraphCreationService()

  fake_service = MagicMock()
  fake_service.create_graph_credits.side_effect = ValueError("Unknown tier")

  with (
    patch(
      "robosystems.operations.graph.credit_service.CreditService",
      return_value=fake_service,
    ),
    patch(
      "robosystems.database.get_db_session",
      side_effect=lambda: iter([MagicMock()]),
    ),
    patch("asyncio.sleep", new_callable=AsyncMock) as sleep_mock,
  ):
    await service._create_credits("kg1", _config())

  fake_service.create_graph_credits.assert_called_once()  # no retry
  sleep_mock.assert_not_called()  # no backoff burned


@pytest.mark.asyncio
async def test_create_credits_retries_transient_error():
  """A transient (non-ValueError) failure retries up to 3x with backoff."""
  service = GraphCreationService()

  fake_service = MagicMock()
  fake_service.create_graph_credits.side_effect = RuntimeError("connection reset")

  with (
    patch(
      "robosystems.operations.graph.credit_service.CreditService",
      return_value=fake_service,
    ),
    patch(
      "robosystems.database.get_db_session",
      side_effect=lambda: iter([MagicMock()]),
    ),
    patch("asyncio.sleep", new_callable=AsyncMock) as sleep_mock,
  ):
    await service._create_credits("kg1", _config())

  assert fake_service.create_graph_credits.call_count == 3
  assert sleep_mock.await_count == 3
