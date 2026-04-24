"""Types for the Event Block Python handler registry."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field

from pydantic import BaseModel
from sqlalchemy.orm import Session

from robosystems.models.api.event_block import CreateEventBlockRequest
from robosystems.models.extensions.roboledger.event import Event


@dataclass
class HandlerResult:
  """What rows the handler created — used for audit + envelope."""

  entry_ids: list[str] = field(default_factory=list)
  transaction_ids: list[str] = field(default_factory=list)


@dataclass
class HandlerPreview:
  """Dry-run result from a Python handler."""

  would_succeed: bool
  planned_entries: list[dict] = field(default_factory=list)
  computed_values: dict = field(default_factory=dict)
  validation_errors: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class EventBlockPythonHandler:
  """A hub-defined Python handler for one event_type.

  The handler declares:
  - `event_type`: the discriminator matched against CreateEventBlockRequest.event_type
  - `metadata_schema`: Pydantic model that validates event.metadata before dispatch
  - `target_status`: the event status after a successful dispatch
    (e.g., 'fulfilled' for terminal events like asset_disposed, 'classified'
     for events that produce further work)
  - `dispatch`: the real execution path — reads, computes, writes
  - `dispatch_preview`: the dry-run path — reads, computes, returns plan only
  """

  event_type: str
  display_name: str
  metadata_schema: type[BaseModel]
  target_status: str
  dispatch: Callable[[Session, Event, BaseModel, str], HandlerResult]
  dispatch_preview: Callable[
    [Session, CreateEventBlockRequest, BaseModel], HandlerPreview
  ]


class HandlerMetadataValidationError(Exception):
  """Raised when event.metadata doesn't match the handler's metadata_schema."""
