"""Event types only their own operation may write.

Each is a record another check reads as evidence, so the general event
operations must not create one or alter one.
"""

from __future__ import annotations

RESERVED_EVENT_TYPES: frozenset[str] = frozenset(
  {
    "reconciliation_signed_off",
    "reconciliation_policy_changed",
    "balance_observed",
  }
)


class ReservedEventTypeError(ValueError):
  def __init__(self, event_type: str) -> None:
    super().__init__(
      f"event_type={event_type!r} is written only by its own operation and "
      "cannot be created or changed through the event operations."
    )
    self.event_type = event_type


def refuse_reserved_event_type(event_type: str) -> None:
  if event_type in RESERVED_EVENT_TYPES:
    raise ReservedEventTypeError(event_type)
