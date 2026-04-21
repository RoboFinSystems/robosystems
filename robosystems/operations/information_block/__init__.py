"""Information Block operations — registry, construction, and reads.

Public surface:

- :class:`BlockTypeRegistryEntry` — the registry entry descriptor.
- :data:`REGISTRY` — the ``dict[str, BlockTypeRegistryEntry]`` of all
  registered block types.
- :func:`create_information_block` — the generic write command.
- :func:`get_information_block` / :func:`list_information_blocks` —
  the reads.

See ``local/docs/specs/information-block.md`` for the architectural
context and ``docs/specs/financial-viewer.md`` for the companion View
layer.
"""

from __future__ import annotations

from robosystems.operations.information_block.commands import (
  create_information_block,
)
from robosystems.operations.information_block.reads import (
  get_information_block,
  list_information_blocks,
)
from robosystems.operations.information_block.registry import (
  REGISTRY,
  SCHEDULE_BLOCK,
  get,
  list_registered,
)
from robosystems.operations.information_block.types import (
  BlockTypeRegistryEntry,
  ConstructionMode,
)

__all__ = [
  "REGISTRY",
  "SCHEDULE_BLOCK",
  "BlockTypeRegistryEntry",
  "ConstructionMode",
  "create_information_block",
  "get",
  "get_information_block",
  "list_information_blocks",
  "list_registered",
]
