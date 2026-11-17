"""What an MCP route serves of a graph's tools.

The universal and per-graph routes serve every tool as it is. The
`/v1/mcp/roboledger` route serves the same tools, less
`ROBOLEDGER_ROUTE_TOOL_EXCLUSIONS`: what a chat client cannot use, such as
subgraph and workspace administration.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .manager import ROBOLEDGER_ROUTE_TOOL_EXCLUSIONS


@dataclass(frozen=True)
class RouteProfile:
  """A route's cut of the tool list: the names it withholds."""

  excluded_tools: frozenset[str] = frozenset()

  def serve(
    self, tools: list[dict[str, Any]], also_excluded: frozenset[str] = frozenset()
  ) -> list[dict[str, Any]]:
    """The tool definitions this route lists, filtered before instructions
    are built so they never name a withheld tool."""
    excluded = self.excluded_tools | also_excluded
    return [t for t in tools if t.get("name") not in excluded]

  def withholds(self, name: str) -> bool:
    return name in self.excluded_tools


UNIVERSAL_ROUTE = RouteProfile()
ROBOLEDGER_ROUTE = RouteProfile(excluded_tools=ROBOLEDGER_ROUTE_TOOL_EXCLUSIONS)
