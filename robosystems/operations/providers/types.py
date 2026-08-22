"""Shared types for the connection-provider contract."""

from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class SyncOutcome:
  """What a provider's `sync` actually did.

  The distinction is load-bearing for every caller. `dispatched` means a run
  exists and `task_id` identifies it, so the operation is genuinely pending
  and worth polling. `unsupported` means the provider has nothing to pull —
  reporting that as pending would hand back an operation id for work nobody
  is doing and point the client at a poll that never terminates.

  It also decides who releases the per-connection sync lock: a dispatched run
  releases it when it finishes, anything else has already finished and the
  dispatcher must release it or the lock sits until its TTL.
  """

  status: Literal["dispatched", "unsupported"]
  task_id: str | None = None
  message: str | None = None

  @property
  def dispatched(self) -> bool:
    """Whether a run outlives this call and will report its own completion."""
    return self.status == "dispatched"
