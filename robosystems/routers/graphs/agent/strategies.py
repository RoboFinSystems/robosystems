"""
Agent execution strategies and utilities.

This module provides intelligent strategy selection for agent execution
based on execution profiles, client capabilities, and system state.
"""

from enum import Enum
from typing import Dict, Any, Optional, Tuple

from robosystems.operations.agents.base import AgentMode, ExecutionProfile
from robosystems.middleware.graph.execution_strategies import (
  BaseClientDetector,
  ResponseMode as BaseResponseMode,
)
from robosystems.logger import logger


class AgentExecutionStrategy(Enum):
  """Agent execution strategies."""

  SYNC_IMMEDIATE = "sync_immediate"
  SSE_STREAMING = "sse_streaming"
  CELERY_QUEUE_STREAM = "celery_queue_stream"


ResponseMode = BaseResponseMode


class AgentClientDetector(BaseClientDetector):
  """Detect client type and capabilities for agent requests."""

  @classmethod
  def detect_client_type(cls, headers: Dict[str, str]) -> Dict[str, Any]:
    """
    Detect client type and capabilities from request headers.

    Args:
        headers: Request headers dictionary

    Returns:
        Dictionary with client detection results
    """
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


class AgentStrategySelector:
  """Select execution strategy for agent operations."""

  @classmethod
  def select_strategy(
    cls,
    execution_profile: Optional[ExecutionProfile],
    client_info: Dict[str, Any],
    mode_override: Optional[ResponseMode] = None,
    force_extended: bool = False,
  ) -> Tuple[AgentExecutionStrategy, Dict[str, Any]]:
    """
    Select optimal execution strategy for agent.

    Args:
        execution_profile: Agent's execution time profile
        client_info: Client capabilities and preferences
        mode_override: Optional mode override from request
        force_extended: Force extended analysis

    Returns:
        Tuple of (strategy, metadata)
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
      return AgentExecutionStrategy.SSE_STREAMING, metadata

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
      return AgentExecutionStrategy.SYNC_IMMEDIATE, metadata

    if mode_override == ResponseMode.ASYNC or client_info.get("prefers_async"):
      metadata["selection_reason"] = "Async mode preferred or forced"
      return AgentExecutionStrategy.CELERY_QUEUE_STREAM, metadata

    # Extended analysis always goes to Celery
    if force_extended:
      metadata["selection_reason"] = "Extended analysis forced"
      return AgentExecutionStrategy.CELERY_QUEUE_STREAM, metadata

    # Testing tools get sync for fast operations
    if client_info["is_testing_tool"]:
      if estimated_time < 30:
        metadata["selection_reason"] = "Testing tool with fast operation"
        return AgentExecutionStrategy.SYNC_IMMEDIATE, metadata
      else:
        logger.warning(
          f"Testing tool requesting {estimated_time}s operation - using SSE"
        )
        metadata["selection_reason"] = "Testing tool with slow operation, using SSE"
        return AgentExecutionStrategy.SSE_STREAMING, metadata

    # Time-based selection for production clients
    if estimated_time < 5:
      # Fast operations: immediate sync response
      metadata["selection_reason"] = f"Fast operation ({estimated_time}s average)"
      return AgentExecutionStrategy.SYNC_IMMEDIATE, metadata

    elif estimated_time < 30:
      # Medium operations: API with SSE progress
      if client_info["capabilities"]["sse"]:
        metadata["selection_reason"] = (
          f"Medium operation ({estimated_time}s) with SSE support"
        )
        return AgentExecutionStrategy.SSE_STREAMING, metadata
      else:
        metadata["selection_reason"] = (
          f"Medium operation ({estimated_time}s) without SSE, using sync"
        )
        return AgentExecutionStrategy.SYNC_IMMEDIATE, metadata

    else:
      # Long operations: Celery worker with SSE monitoring
      metadata["selection_reason"] = (
        f"Long operation ({estimated_time}s), queuing to worker"
      )
      return AgentExecutionStrategy.CELERY_QUEUE_STREAM, metadata

  @classmethod
  def should_use_celery(
    cls,
    execution_profile: Optional[ExecutionProfile],
    mode: AgentMode,
  ) -> bool:
    """
    Quick check if agent should use Celery based on execution profile.

    Args:
        execution_profile: Agent's execution time profile
        mode: Agent execution mode

    Returns:
        True if should use Celery, False for API execution
    """
    if not execution_profile:
      return False

    # Extended mode always goes to Celery
    if mode == AgentMode.EXTENDED:
      return True

    # Standard/Quick modes use avg_time threshold
    return execution_profile.avg_time >= 30
