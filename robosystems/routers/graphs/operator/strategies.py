"""Operator execution strategies.

Selects an execution strategy for an Operator run from its execution profile,
the client's capabilities, and current system state.

"Operator" is the AI-executor concept (Claude/MCP), distinct from the REA
``Agent`` (counterparty).
"""

from enum import Enum
from typing import Any

from robosystems.logger import logger
from robosystems.middleware.graph.execution_strategies import (
  BaseClientDetector,
)
from robosystems.middleware.graph.execution_strategies import (
  ResponseMode as BaseResponseMode,
)
from robosystems.operations.operators.base import ExecutionProfile, OperatorMode


class OperatorExecutionStrategy(Enum):
  """Operator execution strategies."""

  SYNC_IMMEDIATE = "sync_immediate"
  SSE_STREAMING = "sse_streaming"
  BACKGROUND_QUEUE = "background_queue"


ResponseMode = BaseResponseMode


class OperatorClientDetector(BaseClientDetector):
  """Detect client type and capabilities for operator requests."""

  @classmethod
  def detect_client_type(cls, headers: dict[str, str]) -> dict[str, Any]:
    """Detect client type and capabilities from request headers."""
    base_info = cls.detect_client_capabilities(headers)

    return {
      "is_testing_tool": base_info["is_testing_tool"],
      "is_browser": base_info["is_browser"],
      "is_interactive": base_info["is_interactive"],
      "user_agent": base_info["user_agent"],
      "capabilities": {
        "sse": base_info["supports_sse"],
        "ndjson": base_info["supports_ndjson"],
        "streaming": base_info["supports_sse"] or base_info["supports_ndjson"],
      },
      "prefers_async": "respond-async" in headers.get("prefer", ""),
      "prefers_stream": "stream" in headers.get("prefer", ""),
    }


class OperatorStrategySelector:
  """Select execution strategy for operator operations."""

  @classmethod
  def select_strategy(
    cls,
    execution_profile: ExecutionProfile | None,
    client_info: dict[str, Any],
    mode_override: ResponseMode | None = None,
    force_extended: bool = False,
  ) -> tuple[OperatorExecutionStrategy, dict[str, Any]]:
    """Select the execution strategy for an operator run.

    Returns the strategy paired with the metadata explaining the choice.
    """
    metadata = {
      "selection_reason": "",
      "estimated_time": execution_profile.avg_time if execution_profile else 10,
      "client_type": "testing" if client_info["is_testing_tool"] else "production",
    }

    # If no execution profile, default to medium strategy
    if not execution_profile:
      logger.warning("No execution profile provided, defaulting to SSE streaming")
      metadata["selection_reason"] = "No execution profile, defaulting to SSE"
      return OperatorExecutionStrategy.SSE_STREAMING, metadata

    estimated_time = execution_profile.avg_time

    # Force modes
    if mode_override == ResponseMode.SYNC:
      if estimated_time > 30:
        logger.warning(
          f"Sync mode requested but estimated time is {estimated_time}s - may timeout"
        )
        metadata["selection_reason"] = "Sync mode forced (may timeout)"
      else:
        metadata["selection_reason"] = "Sync mode forced"
      return OperatorExecutionStrategy.SYNC_IMMEDIATE, metadata

    if mode_override == ResponseMode.ASYNC or client_info.get("prefers_async"):
      metadata["selection_reason"] = "Async mode preferred or forced"
      return OperatorExecutionStrategy.BACKGROUND_QUEUE, metadata

    # Extended analysis always goes to background queue
    if force_extended:
      metadata["selection_reason"] = "Extended analysis forced"
      return OperatorExecutionStrategy.BACKGROUND_QUEUE, metadata

    # Testing tools get sync for fast operations
    if client_info["is_testing_tool"]:
      if estimated_time < 30:
        metadata["selection_reason"] = "Testing tool with fast operation"
        return OperatorExecutionStrategy.SYNC_IMMEDIATE, metadata
      else:
        logger.warning(
          f"Testing tool requesting {estimated_time}s operation - using SSE"
        )
        metadata["selection_reason"] = "Testing tool with slow operation, using SSE"
        return OperatorExecutionStrategy.SSE_STREAMING, metadata

    # Time-based selection for production clients
    if estimated_time < 5:
      # Fast operations: immediate sync response
      metadata["selection_reason"] = f"Fast operation ({estimated_time}s average)"
      return OperatorExecutionStrategy.SYNC_IMMEDIATE, metadata

    elif estimated_time < 30:
      # Medium operations: API with SSE progress
      if client_info["capabilities"]["sse"]:
        metadata["selection_reason"] = (
          f"Medium operation ({estimated_time}s) with SSE support"
        )
        return OperatorExecutionStrategy.SSE_STREAMING, metadata
      else:
        metadata["selection_reason"] = (
          f"Medium operation ({estimated_time}s) without SSE, using sync"
        )
        return OperatorExecutionStrategy.SYNC_IMMEDIATE, metadata

    else:
      # Long operations: background queue with SSE monitoring
      metadata["selection_reason"] = (
        f"Long operation ({estimated_time}s), queuing to background"
      )
      return OperatorExecutionStrategy.BACKGROUND_QUEUE, metadata

  @classmethod
  def should_use_background(
    cls,
    execution_profile: ExecutionProfile | None,
    mode: OperatorMode,
  ) -> bool:
    """True when the operator's profile calls for the background queue rather
    than inline API execution."""
    if not execution_profile:
      return False

    # Extended mode always goes to background queue
    if mode == OperatorMode.EXTENDED:
      return True

    # Standard/Quick modes use avg_time threshold
    return execution_profile.avg_time >= 30
