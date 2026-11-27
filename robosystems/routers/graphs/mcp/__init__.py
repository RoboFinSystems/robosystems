"""MCP (Model Context Protocol) for a graph: the Streamable HTTP transport.

`remote.py` serves the JSON-RPC routes that carry one graph (per-graph,
OAuth-only `/v1/mcp`, and `/v1/mcp/roboledger`); `catalog.py` serves
`/v1/mcp/graphs`, where each call names its graph. `execute.py` holds the
authorization gauntlet, with `strategies.py`, `streaming.py` and `handlers.py`
as collaborators.
"""

from .catalog import graphs_router
from .remote import agnostic_router, roboledger_router
from .remote import router as remote_router

__all__ = ["agnostic_router", "graphs_router", "remote_router", "roboledger_router"]
