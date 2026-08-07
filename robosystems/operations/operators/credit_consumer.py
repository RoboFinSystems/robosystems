"""Credit consumers — one per execution context.

`CreditConsumer` is a protocol; each implementation differs only in how it
obtains a database session before calling `CreditService.consume_ai_tokens`.

- `SessionCreditConsumer`: API context (reuses the request's db session)
- `FactoryCreditConsumer`: worker context (creates a session per call)
- `NoOpCreditConsumer`: tests and any context that must not bill
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable


class CreditConsumptionError(Exception):
  """Raised when credits for a completed AI call could not be recorded.

  The failure has to be raised rather than reported as 0.0 credits: a cheap
  call legitimately rounds to zero, so a shared return value would leave the
  caller unable to tell unbounded unbilled spend from an inexpensive request.
  """


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
    """Consume credits for an AI call. Returns credits consumed.

    Raises:
        CreditConsumptionError: if the consumption could not be recorded.
    """


class SessionCreditConsumer:
  """API context: bills on the request's own session.

  Sharing the request's transaction scope means consumption commits or rolls
  back with the rest of the request.
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

    raise CreditConsumptionError(str(result.get("error", "Unknown")))


class FactoryCreditConsumer:
  """Worker context: one short-lived session, and one transaction, per call.

  Independent transactions match the worker's per-batch model, where partial
  progress is committed — a long run that dies partway is still billed for the
  calls it actually made.
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

      if result.get("success"):
        return float(result.get("credits_consumed", 0))

      raise CreditConsumptionError(str(result.get("error", "Unknown")))
    except CreditConsumptionError:
      raise
    except Exception as e:
      raise CreditConsumptionError(str(e)) from e
    finally:
      session.close()


class NoOpCreditConsumer:
  """Bills nothing. Tests only — anything user-facing must record spend."""

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
