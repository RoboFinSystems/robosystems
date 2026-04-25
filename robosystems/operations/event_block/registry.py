"""Handler registry — resolve the best matching EventHandler for an event.

Resolution algorithm:
1. Query event_handlers where event_type matches and is_active=True.
2. Filter by event_category, match_source, match_agent_type,
   match_resource_type (null fields act as wildcards — match anything).
3. Apply match_metadata_expression (simple equality map).
4. Require approval for AI-suggested handlers (suggested_by='ai'
   without approved_by is excluded, matching the MappingAssociation rule).
5. Return the row with the highest priority. Tie → HandlerAmbiguousError.
"""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from robosystems.models.extensions.roboledger.event_handler import EventHandler


class HandlerNotFoundError(Exception):
  """No active, approved handler matches this event."""

  def __init__(self, event_type: str, event_category: str | None = None) -> None:
    super().__init__(
      f"No active handler found for event_type='{event_type}'"
      + (f", event_category='{event_category}'" if event_category else "")
    )
    self.event_type = event_type
    self.event_category = event_category


class HandlerAmbiguousError(Exception):
  """Multiple handlers tied at the same top priority."""

  def __init__(self, event_type: str, handlers: list[EventHandler]) -> None:
    names = [h.name for h in handlers]
    super().__init__(
      f"Ambiguous handlers for event_type='{event_type}': "
      f"multiple rows share priority {handlers[0].priority}: {names}. "
      "Assign unique priorities to disambiguate."
    )
    self.handlers = handlers


def _matches_metadata_expression(
  expression: dict | None,
  event_metadata: dict,
) -> bool:
  """Simple dot-path equality match against event.metadata.

  Three supported path forms:
  - ``"foo"`` — root-relative key inside event.metadata (``metadata["foo"]``).
  - ``"metadata.foo"`` — same target as ``"foo"``; the ``metadata.`` prefix
    is stripped before traversal so DSL authors can write either form.
  - ``"metadata"`` — bare literal that compares the entire metadata dict to
    the expected value.
  """
  if not expression:
    return True
  for path, expected in expression.items():
    if path == "metadata":
      # Bare ``metadata`` literal: compare the whole metadata dict.
      node = event_metadata
      if node != expected:
        return False
      continue
    if path.startswith("metadata."):
      path = path.removeprefix("metadata.")
    parts = path.split(".")
    node: object = event_metadata
    for part in parts:
      if not isinstance(node, dict) or part not in node:
        return False
      node = node[part]
    if node != expected:
      return False
  return True


def resolve_handler(
  session: Session,
  *,
  event_type: str,
  event_category: str | None,
  source: str | None,
  agent_type: str | None,
  resource_type: str | None,
  metadata: dict,
) -> EventHandler:
  """Find the highest-priority matching handler for the given event fields.

  Args:
      session: SQLAlchemy session (tenant-scoped via extensions_session).
      event_type: Required — must match handler.event_type exactly.
      event_category: Matched against handler.event_category (null = wildcard).
      source: Matched against handler.match_source (null = wildcard).
      agent_type: Matched against handler.match_agent_type (null = wildcard).
      resource_type: Matched against handler.match_resource_type (null = wildcard).
      metadata: Event metadata dict for match_metadata_expression evaluation.

  Returns:
      The highest-priority matching EventHandler row.

  Raises:
      HandlerNotFoundError: No active handler matched.
      HandlerAmbiguousError: Multiple handlers tied at top priority.
  """
  candidates = (
    session.execute(
      select(EventHandler)
      .where(EventHandler.event_type == event_type)
      .where(EventHandler.is_active.is_(True))
      .order_by(EventHandler.priority.desc())
    )
    .scalars()
    .all()
  )

  matched: list[EventHandler] = []
  for handler in candidates:
    # Skip unapproved AI-suggested handlers
    if handler.suggested_by == "ai" and handler.approved_by is None:
      continue

    # Wildcard match: handler field null means "match any"
    if handler.event_category is not None and handler.event_category != event_category:
      continue
    if handler.match_source is not None and handler.match_source != source:
      continue
    if handler.match_agent_type is not None and handler.match_agent_type != agent_type:
      continue
    if (
      handler.match_resource_type is not None
      and handler.match_resource_type != resource_type
    ):
      continue
    if not _matches_metadata_expression(handler.match_metadata_expression, metadata):
      continue

    matched.append(handler)

  if not matched:
    raise HandlerNotFoundError(event_type, event_category)

  # Handlers are pre-sorted by priority DESC; check for tie at top
  top_priority = matched[0].priority
  top_group = [h for h in matched if h.priority == top_priority]
  if len(top_group) > 1:
    raise HandlerAmbiguousError(event_type, top_group)

  return top_group[0]
