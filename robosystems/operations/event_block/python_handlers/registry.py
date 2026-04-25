"""Event Block Python handler registry — discriminator-keyed dispatch table.

Hub-defined handlers for complex event workflows. Each registry value is an
EventBlockPythonHandler frozen dataclass. Resolution happens in
`create_event_block` (commands.py) before the DSL fallback.

Adding a new handler:
  1. Create a new module under python_handlers/ (e.g., revenue_recognition.py)
  2. Define its EventBlockPythonHandler constant
  3. Register it below
"""

from __future__ import annotations

from .asset_disposed import ASSET_DISPOSED_HANDLER
from .journal_entry_recorded import JOURNAL_ENTRY_RECORDED_HANDLER
from .journal_entry_reversed import JOURNAL_ENTRY_REVERSED_HANDLER
from .schedule_created import SCHEDULE_CREATED_HANDLER
from .schedule_entry_due import SCHEDULE_ENTRY_DUE_HANDLER
from .types import EventBlockPythonHandler

EVENT_BLOCK_PYTHON_REGISTRY: dict[str, EventBlockPythonHandler] = {
  ASSET_DISPOSED_HANDLER.event_type: ASSET_DISPOSED_HANDLER,
  SCHEDULE_CREATED_HANDLER.event_type: SCHEDULE_CREATED_HANDLER,
  SCHEDULE_ENTRY_DUE_HANDLER.event_type: SCHEDULE_ENTRY_DUE_HANDLER,
  JOURNAL_ENTRY_RECORDED_HANDLER.event_type: JOURNAL_ENTRY_RECORDED_HANDLER,
  JOURNAL_ENTRY_REVERSED_HANDLER.event_type: JOURNAL_ENTRY_REVERSED_HANDLER,
}


def get_python_handler(event_type: str) -> EventBlockPythonHandler | None:
  """Look up a Python handler by event_type, or None if not registered."""
  return EVENT_BLOCK_PYTHON_REGISTRY.get(event_type)


__all__ = ["EVENT_BLOCK_PYTHON_REGISTRY", "get_python_handler"]
