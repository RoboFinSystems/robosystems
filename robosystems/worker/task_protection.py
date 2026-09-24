"""ECS scale-in protection for the background worker.

A worker killed by scale-in mid-task is only requeued after the full task
timeout, so the worker protects itself while busy. Best-effort (failures are
logged, never raised) and a no-op outside ECS.
"""

import asyncio
import json
import os
import urllib.request
from typing import Any

from robosystems.logger import get_logger

logger = get_logger(__name__)

METADATA_URI_ENV = "ECS_CONTAINER_METADATA_URI_V4"

# Must exceed the longest task timeout, and twice the budget of any
# run_blocking task (its join grace is one more budget). A failed unprotect
# self-expires with the lease.
PROTECTION_EXPIRES_MINUTES = 90

METADATA_TIMEOUT_SECONDS = 2

# Shorter tasks drain within the SIGTERM grace (StopTimeout 119s).
PROTECT_MIN_TIMEOUT_SECONDS = 120

# Consecutive metadata-fetch failures before protection is disabled.
MAX_METADATA_FAILURES = 10


class TaskProtectionManager:
  """Marks the running ECS task protected/unprotected from scale-in.

  Disabled (no-op) when not running as an ECS task. Call protect() when a task
  starts and unprotect() when it finishes. The worker processes one task at a
  time, so no refcounting is needed.
  """

  def __init__(self) -> None:
    self._metadata_uri = os.environ.get(METADATA_URI_ENV)
    self._enabled = bool(self._metadata_uri)
    self._cluster: str | None = None
    self._task_arn: str | None = None
    self._ecs_client: Any = None
    self._metadata_failures = 0

  async def protect(self) -> None:
    await self._set_protection(True)

  async def unprotect(self) -> None:
    await self._set_protection(False)

  async def _set_protection(self, enabled: bool) -> None:
    if not self._enabled:
      return
    try:
      await asyncio.to_thread(self._set_protection_sync, enabled)
    except Exception as e:
      # Best-effort: never let protection failures break task processing.
      logger.warning(f"Task scale-in protection (enabled={enabled}) failed: {e}")

  def _set_protection_sync(self, enabled: bool) -> None:
    # Metadata is fetched once and cached, so _load_metadata (and its
    # retry-then-disable logic) only runs until the first success.
    if self._task_arn is None and not self._load_metadata():
      return

    kwargs: dict[str, Any] = {
      "cluster": self._cluster,
      "tasks": [self._task_arn],
      "protectionEnabled": enabled,
    }
    # expiresInMinutes is only valid when enabling protection.
    if enabled:
      kwargs["expiresInMinutes"] = PROTECTION_EXPIRES_MINUTES

    response = self._get_ecs_client().update_task_protection(**kwargs)
    failures = response.get("failures") or []
    if failures:
      logger.warning(f"Task scale-in protection update had failures: {failures}")
    else:
      logger.debug(
        f"Set task scale-in protection enabled={enabled} for {self._task_arn}"
      )

  def _load_metadata(self) -> bool:
    """Fetch cluster + task ARN from the ECS metadata endpoint.

    Returns True on success. A transient failure returns False but leaves the
    manager enabled so the next call retries; only repeated failures disable it.
    """
    try:
      with urllib.request.urlopen(
        f"{self._metadata_uri}/task", timeout=METADATA_TIMEOUT_SECONDS
      ) as resp:
        meta = json.loads(resp.read())
      self._cluster = meta["Cluster"]
      self._task_arn = meta["TaskARN"]
      self._metadata_failures = 0
      return True
    except Exception as e:
      self._metadata_failures += 1
      if self._metadata_failures >= MAX_METADATA_FAILURES:
        self._enabled = False
        logger.warning(
          f"Could not read ECS task metadata after {self._metadata_failures} "
          f"attempts; scale-in protection disabled: {e}"
        )
      else:
        logger.warning(f"Could not read ECS task metadata; will retry: {e}")
      return False

  def _get_ecs_client(self) -> Any:
    if self._ecs_client is None:
      import boto3
      from botocore.config import Config

      # Tight timeouts + capped retries so a slow/throttled ECS control plane
      # can't stall task processing (protect() is awaited before the handler
      # runs). Worst case well under 20s rather than botocore's ~5-min default.
      self._ecs_client = boto3.client(
        "ecs",
        config=Config(
          connect_timeout=2,
          read_timeout=5,
          retries={"max_attempts": 2, "mode": "standard"},
        ),
      )
    return self._ecs_client
