"""MCP (Model Context Protocol) for a graph — the Streamable HTTP transport.

- `remote.py` serves the JSON-RPC transport at `POST /v1/graphs/{graph_id}/mcp`
  (``remote_router``) and the graph-agnostic, OAuth-only `POST /v1/mcp`
  (``agnostic_router``), both schema-excluded so the envelope never reaches
  the generated SDKs.
- `execute.py` holds the authorization gauntlet and execution helpers the
  transport runs every call through; `strategies.py`, `streaming.py` and
  `handlers.py` are its collaborators.

The REST tool endpoints (`GET /mcp/tools`, `POST /mcp/call-tool`) were removed
in the same release that added OAuth: every MCP client speaks the transport.
"""

from .remote import agnostic_router
from .remote import router as remote_router

__all__ = ["agnostic_router", "remote_router"]
