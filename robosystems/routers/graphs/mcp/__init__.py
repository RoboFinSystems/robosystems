"""MCP (Model Context Protocol) for a graph: the Streamable HTTP transport.

`remote.py` serves the JSON-RPC routes (per-graph, OAuth-only `/v1/mcp`, and
`/v1/mcp/roboledger`); `execute.py` holds the authorization gauntlet, with
`strategies.py`, `streaming.py` and `handlers.py` as collaborators.
"""

from .remote import agnostic_router, roboledger_router
from .remote import router as remote_router

__all__ = ["agnostic_router", "remote_router", "roboledger_router"]
