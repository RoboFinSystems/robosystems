"""Event Block commands — create, update, and preview operations.

Re-exports the public command surface so callers import a stable module
path regardless of internal layout.
"""

from robosystems.operations.locking import RowLockedError

from .commands import (
  DuplicateEventError,
  EventNotFoundError,
  EventNotPublishableError,
  InvalidEventTransitionError,
  create_event_block,
  execute_event_block,
  preview_event_block,
  update_event_block,
)

# The lock policy moved to `operations/locking.py` and its error was renamed,
# since it now guards entries, fiscal periods and reports as well as events.
# This module's contract is a stable import path, so the old name keeps
# resolving rather than breaking on the release that moved it. Deprecated —
# import `RowLockedError` from `robosystems.operations.locking`.
EventLockedError = RowLockedError

__all__ = [
  "DuplicateEventError",
  "EventLockedError",  # deprecated alias for RowLockedError
  "EventNotFoundError",
  "EventNotPublishableError",
  "InvalidEventTransitionError",
  "RowLockedError",
  "create_event_block",
  "execute_event_block",
  "preview_event_block",
  "update_event_block",
]
