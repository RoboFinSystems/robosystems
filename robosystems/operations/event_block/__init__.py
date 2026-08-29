"""Event Block commands — create, update, and preview operations.

Re-exports the public command surface so callers import a stable module
path regardless of internal layout.
"""

from robosystems.operations.locking import RowLockedError

from .commands import (
  DuplicateEventError,
  EventEffectsAlreadyLandedError,
  EventNotFoundError,
  EventNotPublishableError,
  InvalidEventTransitionError,
  create_event_block,
  execute_event_block,
  preview_event_block,
  update_event_block,
)

# Deprecated alias — import `RowLockedError` from
# `robosystems.operations.locking`, which owns the lock policy for entries,
# fiscal periods and reports as well as events.
EventLockedError = RowLockedError

__all__ = [
  "DuplicateEventError",
  "EventEffectsAlreadyLandedError",
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
