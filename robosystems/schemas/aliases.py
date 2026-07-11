"""Legacy schema-extension name aliases.

The ``memory`` extension was renamed to ``knowledge`` when semantic memory
moved to LanceDB (the "memory" label now belongs to the LanceDB vector store,
not the structural subgraph schema). Any subgraph persisted with
``schema_extensions=["memory"]`` must still resolve, so every extension loader
routes names through :func:`resolve_extension_alias` before importing the module.
"""

from __future__ import annotations

# old extension name -> canonical module name
EXTENSION_ALIASES: dict[str, str] = {"memory": "knowledge"}


def resolve_extension_alias(name: str) -> str:
  """Map a legacy extension name to its canonical module name (identity if none)."""
  return EXTENSION_ALIASES.get(name, name)
