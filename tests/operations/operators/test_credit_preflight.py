"""Credits are checked before an operator runs, and billing failures stop it.

Credits are consumed *after* each Bedrock call returns, so the pre-flight is the
only thing bounding spend. It lives in the execution adapters because two of the
three API strategies (SSE, background queue) call them directly and never build
an orchestrator.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from robosystems.operations.operators.base import (
  OperatorCapability,
  OperatorMode,
  OperatorResult,
  OperatorSpec,
)
from robosystems.operations.operators.credit_preflight import (
  InsufficientOperatorCreditsError,
  check_operator_credits,
  enforce_operator_credits,
  estimate_operator_credits,
)

GRAPH_ID = "kg01234567890abcdef"
USER_ID = "usr_test123"


def _operator(*, requires_credits: bool = True, name: str = "Test Operator"):
  operator = MagicMock()
  operator.spec = OperatorSpec(
    name=name,
    description="test",
    capabilities=[OperatorCapability.CUSTOM],
    requires_credits=requires_credits,
    read_only=True,
  )
  operator.run = AsyncMock(return_value=OperatorResult(content="ok"))
  return operator


def _credit_service(result: dict):
  service = MagicMock()
  service.check_credit_balance.return_value = result
  return patch(
    "robosystems.operations.graph.credit_service.CreditService",
    return_value=service,
  )


class TestEnforceOperatorCredits:
  def test_sufficient_balance_allows_the_run(self) -> None:
    with _credit_service({"has_sufficient_credits": True, "available_credits": 500.0}):
      enforce_operator_credits(
        _operator(), GRAPH_ID, USER_ID, MagicMock(), OperatorMode.STANDARD
      )

  def test_insufficient_balance_raises_with_the_numbers(self) -> None:
    with _credit_service({"has_sufficient_credits": False, "available_credits": 5.0}):
      with pytest.raises(InsufficientOperatorCreditsError) as exc:
        enforce_operator_credits(
          _operator(), GRAPH_ID, USER_ID, MagicMock(), OperatorMode.STANDARD
        )

    assert exc.value.available_credits == 5.0
    assert exc.value.estimated_credits > 0

  def test_operator_not_requiring_credits_is_skipped(self) -> None:
    with _credit_service({"has_sufficient_credits": False}) as service:
      enforce_operator_credits(
        _operator(requires_credits=False),
        GRAPH_ID,
        USER_ID,
        MagicMock(),
        OperatorMode.STANDARD,
      )

    service.assert_not_called()

  def test_no_session_skips_rather_than_denying(self) -> None:
    """Contexts without the platform DB (tests) must not be blocked; the
    adapter logs that case separately because it also means consumption
    cannot be recorded."""
    enforce_operator_credits(
      _operator(), GRAPH_ID, USER_ID, None, OperatorMode.STANDARD
    )


class TestPreflightFailsClosed:
  def test_an_error_resolving_the_balance_denies_the_run(self) -> None:
    """The normal 'no credit pool' and 'no shared-repo access' cases return a
    well-formed negative answer, so reaching the exception path means the
    balance genuinely could not be established — which used to mean free AI."""
    with patch(
      "robosystems.operations.graph.credit_service.CreditService",
      side_effect=RuntimeError("database unreachable"),
    ):
      result = check_operator_credits(
        _operator(), GRAPH_ID, USER_ID, MagicMock(), OperatorMode.STANDARD
      )

    assert result["has_sufficient_credits"] is False
    assert "database unreachable" in result["error"]

  def test_enforce_raises_when_the_check_errors(self) -> None:
    with patch(
      "robosystems.operations.graph.credit_service.CreditService",
      side_effect=RuntimeError("boom"),
    ):
      with pytest.raises(InsufficientOperatorCreditsError):
        enforce_operator_credits(
          _operator(), GRAPH_ID, USER_ID, MagicMock(), OperatorMode.STANDARD
        )


class TestEstimation:
  def test_extended_mode_costs_more_than_quick(self) -> None:
    operator = _operator()
    quick = estimate_operator_credits(operator, OperatorMode.QUICK)
    extended = estimate_operator_credits(operator, OperatorMode.EXTENDED)
    assert extended > quick > Decimal(0)

  def test_financial_operators_are_estimated_higher(self) -> None:
    plain = estimate_operator_credits(_operator(name="Plain"), OperatorMode.STANDARD)
    financial = estimate_operator_credits(
      _operator(name="Financial Operator"), OperatorMode.STANDARD
    )
    assert financial > plain


class TestAdaptersRunThePreflight:
  """Asserted through the adapters, and by proving tool access is never
  constructed — the check has to happen before anything can spend."""

  @pytest.mark.asyncio
  async def test_api_adapter_denies_an_underfunded_graph(self) -> None:
    from robosystems.operations.operators.adapters import api

    user = MagicMock()
    user.id = USER_ID

    with (
      patch.object(
        api,
        "enforce_operator_credits",
        side_effect=InsufficientOperatorCreditsError("Test Operator", 10.0, 1.0),
      ),
      patch.object(api, "HttpToolAccess") as tool_access,
      patch.object(api, "get_ai_client"),
      patch.object(api, "TrackedAIClient"),
    ):
      with pytest.raises(InsufficientOperatorCreditsError):
        await api.run_operator_api(
          operator=_operator(), graph_id=GRAPH_ID, user=user, query="anything"
        )

    tool_access.assert_not_called()

  @pytest.mark.asyncio
  async def test_worker_adapter_denies_an_underfunded_graph(self) -> None:
    from robosystems.operations.operators.adapters import worker

    with (
      patch.object(
        worker,
        "enforce_operator_credits",
        side_effect=InsufficientOperatorCreditsError("Test Operator", 10.0, 1.0),
      ),
      patch.object(worker, "SessionFactory"),
      patch.object(worker, "HttpToolAccess") as tool_access,
      patch.object(worker, "get_ai_client"),
      patch.object(worker, "TrackedAIClient"),
      patch.object(worker, "FactoryCreditConsumer"),
    ):
      with pytest.raises(InsufficientOperatorCreditsError):
        await worker.run_operator_worker(
          operator=_operator(),
          task_id="task_1",
          graph_id=GRAPH_ID,
          user_id=USER_ID,
          params={},
          manager=AsyncMock(),
        )

    tool_access.assert_not_called()


class TestUnbilledCallsStopTheLoop:
  """A tool-use loop can make dozens of calls. If one cannot be billed, every
  later call in the same run would be unbilled too."""

  @pytest.fixture
  def ai_response(self):
    response = MagicMock()
    response.input_tokens = 100
    response.output_tokens = 50
    response.cache_read_input_tokens = 0
    response.cache_creation_input_tokens = 0
    response.model = "claude-sonnet"
    return response

  def _tracked(self, consumer, ai_response):
    from robosystems.operations.operators.tracked_ai import TrackedAIClient

    ai = MagicMock()
    ai.create_message = AsyncMock(return_value=ai_response)
    return TrackedAIClient(
      ai_client=ai, graph_id=GRAPH_ID, user_id=USER_ID, credit_consumer=consumer
    ), ai

  @pytest.mark.asyncio
  async def test_zero_credits_is_not_treated_as_a_failure(self, ai_response) -> None:
    """A call billed at zero is a cheap call, not a broken one — the consumers
    signal failure by raising."""
    consumer = MagicMock()
    consumer.consume = AsyncMock(return_value=0.0)
    tracked, ai = self._tracked(consumer, ai_response)

    await tracked.create_message(messages=[])
    await tracked.create_message(messages=[])

    assert ai.create_message.await_count == 2
    assert tracked.total_credits == 0.0

  @pytest.mark.asyncio
  async def test_first_unbilled_call_still_returns_its_answer(
    self, ai_response
  ) -> None:
    """Bedrock has been paid for it either way; discarding the answer would
    waste the money without recovering it."""
    from robosystems.operations.operators.credit_consumer import (
      CreditConsumptionError,
    )

    consumer = MagicMock()
    consumer.consume = AsyncMock(side_effect=CreditConsumptionError("no pool"))
    tracked, _ = self._tracked(consumer, ai_response)

    assert await tracked.create_message(messages=[]) is ai_response

  @pytest.mark.asyncio
  async def test_the_next_call_is_refused(self, ai_response) -> None:
    from robosystems.operations.operators.credit_consumer import (
      CreditConsumptionError,
    )
    from robosystems.operations.operators.tracked_ai import UnbilledAICallError

    consumer = MagicMock()
    consumer.consume = AsyncMock(side_effect=CreditConsumptionError("no pool"))
    tracked, ai = self._tracked(consumer, ai_response)

    await tracked.create_message(messages=[])
    with pytest.raises(UnbilledAICallError):
      await tracked.create_message(messages=[])

    # Refused before spending, not after.
    assert ai.create_message.await_count == 1

  @pytest.mark.asyncio
  async def test_cache_tokens_are_accumulated_and_forwarded(self) -> None:
    """With caching on, `input_tokens` is only the uncached remainder — the
    cache counts must reach both the run totals and the consumer, or 90%+ of
    the input goes unbilled."""
    response = MagicMock()
    response.input_tokens = 337
    response.output_tokens = 50
    response.cache_read_input_tokens = 3905
    response.cache_creation_input_tokens = 12
    response.model = "claude-sonnet"

    consumer = MagicMock()
    consumer.consume = AsyncMock(return_value=1.5)
    tracked, _ = self._tracked(consumer, response)

    await tracked.create_message(messages=[])
    await tracked.create_message(messages=[])

    assert tracked.total_tokens == {
      "input": 674,
      "output": 100,
      "cache_read": 7810,
      "cache_write": 24,
    }
    kwargs = consumer.consume.await_args.kwargs
    assert kwargs["cache_read_input_tokens"] == 3905
    assert kwargs["cache_creation_input_tokens"] == 12


class TestConsumersSignalFailureByRaising:
  @pytest.mark.asyncio
  async def test_session_consumer_raises_on_failure(self) -> None:
    from robosystems.operations.operators.credit_consumer import (
      CreditConsumptionError,
      SessionCreditConsumer,
    )

    service = MagicMock()
    service.consume_ai_tokens.return_value = {"success": False, "error": "no pool"}

    with patch(
      "robosystems.operations.graph.credit_service.CreditService",
      return_value=service,
    ):
      with pytest.raises(CreditConsumptionError, match="no pool"):
        await SessionCreditConsumer(MagicMock()).consume(
          graph_id=GRAPH_ID,
          user_id=USER_ID,
          input_tokens=100,
          output_tokens=50,
          model="claude-sonnet",
          operation_description="test",
        )

  @pytest.mark.asyncio
  async def test_session_consumer_returns_the_amount_on_success(self) -> None:
    from robosystems.operations.operators.credit_consumer import SessionCreditConsumer

    service = MagicMock()
    service.consume_ai_tokens.return_value = {
      "success": True,
      "credits_consumed": 12.5,
    }

    with patch(
      "robosystems.operations.graph.credit_service.CreditService",
      return_value=service,
    ):
      credits = await SessionCreditConsumer(MagicMock()).consume(
        graph_id=GRAPH_ID,
        user_id=USER_ID,
        input_tokens=100,
        output_tokens=50,
        model="claude-sonnet",
        operation_description="test",
      )

    assert credits == 12.5

  @pytest.mark.asyncio
  async def test_session_consumer_forwards_cache_counts_to_the_meter(self) -> None:
    from robosystems.operations.operators.credit_consumer import SessionCreditConsumer

    service = MagicMock()
    service.consume_ai_tokens.return_value = {"success": True, "credits_consumed": 2.0}

    with patch(
      "robosystems.operations.graph.credit_service.CreditService",
      return_value=service,
    ):
      await SessionCreditConsumer(MagicMock()).consume(
        graph_id=GRAPH_ID,
        user_id=USER_ID,
        input_tokens=337,
        output_tokens=50,
        model="claude-sonnet",
        operation_description="test",
        cache_read_input_tokens=3905,
        cache_creation_input_tokens=12,
      )

    kwargs = service.consume_ai_tokens.call_args.kwargs
    assert kwargs["cache_read_input_tokens"] == 3905
    assert kwargs["cache_creation_input_tokens"] == 12
