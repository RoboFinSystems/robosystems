"""Credit consumer implementations for different execution contexts.

CreditConsumer is a protocol — each execution context provides an
implementation that knows how to obtain a database session and call
CreditService.consume_ai_tokens().

- SessionCreditConsumer: API context (reuses the request's db session)
- FactoryCreditConsumer: Worker context (creates sessions per call)
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from robosystems.logger import logger


@runtime_checkable
class CreditConsumer(Protocol):
  """Protocol for credit consumption — adapted per execution context."""

  async def consume(
    self,
    graph_id: str,
    user_id: str,
    input_tokens: int,
    output_tokens: int,
    model: str,
    operation_description: str,
  ) -> float:
    """Consume credits for an AI call. Returns credits consumed."""
    ...


class SessionCreditConsumer:
  """For API context: uses the injected SQLAlchemy session.

  The session is the same one used by the request lifecycle, ensuring
  credit consumption is part of the same transaction scope.
  """

  def __init__(self, db_session) -> None:
    self._session = db_session

  async def consume(
    self,
    graph_id: str,
    user_id: str,
    input_tokens: int,
    output_tokens: int,
    model: str,
    operation_description: str,
  ) -> float:
    from robosystems.operations.graph.credit_service import CreditService

    service = CreditService(self._session)
    result = service.consume_ai_tokens(
      graph_id=graph_id,
      input_tokens=input_tokens,
      output_tokens=output_tokens,
      model=model,
      operation_description=operation_description,
      user_id=user_id,
    )

    if result.get("success"):
      return float(result.get("credits_consumed", 0))

    logger.warning(f"Credit consumption failed: {result.get('error', 'Unknown')}")
    return 0.0


class FactoryCreditConsumer:
  """For worker context: creates its own sessions per call.

  Each consumption is an independent transaction, matching the worker's
  per-batch processing model where partial progress is committed.
  """

  async def consume(
    self,
    graph_id: str,
    user_id: str,
    input_tokens: int,
    output_tokens: int,
    model: str,
    operation_description: str,
  ) -> float:
    from robosystems.db.platform import SessionFactory
    from robosystems.operations.graph.credit_service import CreditService

    session = SessionFactory()
    try:
      service = CreditService(session)
      result = service.consume_ai_tokens(
        graph_id=graph_id,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        model=model,
        operation_description=operation_description,
        user_id=user_id,
      )
      return float(result.get("credits_consumed", 0))
    except Exception as e:
      logger.warning(f"Credit consumption failed (worker): {e}")
      return 0.0
    finally:
      session.close()


class NoOpCreditConsumer:
  """For tests or contexts where credits should not be consumed."""

  async def consume(
    self,
    graph_id: str,
    user_id: str,
    input_tokens: int,
    output_tokens: int,
    model: str,
    operation_description: str,
  ) -> float:
    return 0.0
