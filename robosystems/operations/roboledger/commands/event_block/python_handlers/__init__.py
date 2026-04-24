"""Python handler registry for Event Blocks.

Hub-defined handlers for complex event workflows that can't be expressed as
{{ event.amount }} DSL templates in the event_handlers table. Each handler
is a Python module that reads state, computes derived values, and writes
multiple rows atomically.

Resolution order in create_event_block:
  1. Python registry (here) — wins for hub-defined event types
  2. DSL registry (event_handlers table) — tenant-configurable simple handlers

Current handlers:
  - asset_disposed: atomic schedule truncation + balanced 4-leg disposal entry
  - schedule_entry_due: draft a closing entry from a schedule's period fact
  - manual_adjustment: balanced free-form draft entry, not tied to a schedule
  - journal_entry_recorded: balanced journal entry (draft or posted)
  - transaction_recorded: standalone business-event Transaction
"""

from .registry import EVENT_BLOCK_PYTHON_REGISTRY, get_python_handler
from .types import EventBlockPythonHandler, HandlerPreview, HandlerResult

__all__ = [
  "EVENT_BLOCK_PYTHON_REGISTRY",
  "EventBlockPythonHandler",
  "HandlerPreview",
  "HandlerResult",
  "get_python_handler",
]
