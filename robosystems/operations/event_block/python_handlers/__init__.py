"""Python handler registry for Event Blocks.

Hub-defined handlers for complex event workflows that can't be expressed as
{{ event.amount }} DSL templates in the event_handlers table. Each handler
is a Python module that reads state, computes derived values, and writes
multiple rows atomically.

Resolution order in create_event_block:
  1. Python registry (here) — wins for hub-defined event types
  2. DSL registry (event_handlers table) — tenant-configurable simple handlers

Current handlers:
  - journal_entry_recorded: manual GL write — covers any entry_type
    (standard / adjusting / closing / reversing) and either status
    (draft / posted). Single surface for "I'm recording a journal entry."
  - journal_entry_reversed: a posted journal entry was reversed —
    handler creates the offsetting reversing entry, marks the original
    as `status='reversed'`, and links both rows to this event.
  - schedule_entry_due: a schedule period matured — handler drafts the
    closing entry from the schedule's pre-computed fact.
  - asset_disposed: an asset left the business — handler atomically
    truncates the depreciation schedule, drops its SumEquals rule, and
    posts the balanced disposal entry.
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
