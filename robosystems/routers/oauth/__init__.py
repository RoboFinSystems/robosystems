"""MCP OAuth 2.1 authorization server routes.

Absolute paths: the RFC 8414 / RFC 9728 discovery documents live under
``/.well-known`` at the origin root; the endpoints under ``/v1/oauth``.
"""

from .server import router

__all__ = ["router"]
