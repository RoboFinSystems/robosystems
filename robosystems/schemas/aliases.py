"""Schema-extension name aliases.

``memory`` is an accepted alias for the ``knowledge`` extension: subgraphs
persisted with ``schema_extensions=["memory"]`` must still resolve. Every
extension loader routes names through :func:`resolve_extension_alias` before
importing the module.
"""

from __future__ import annotations

# old extension name -> canonical module name
EXTENSION_ALIASES: dict[str, str] = {"memory": "knowledge"}


def resolve_extension_alias(name: str) -> str:
  """Map a legacy extension name to its canonical module name (identity if none)."""
  return EXTENSION_ALIASES.get(name, name)
