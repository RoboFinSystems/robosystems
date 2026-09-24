"""Python handlers for Event Blocks: platform-defined workflows too complex for
the tenant DSL templates in ``event_handlers``. They take precedence over the
DSL registry; see ``registry.py`` for the full list.
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
