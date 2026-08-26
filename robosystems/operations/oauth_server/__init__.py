"""MCP OAuth 2.1 authorization server — the operations kernel.

The wire surface (``routers/oauth``) is thin; every decision lives here:

- ``resources`` — the two protected resources (``/v1/mcp`` and the per-graph
  URLs), their canonical form, and the RFC 8414 / RFC 9728 metadata.
- ``clients`` — redirect-URI matching, RFC 7591 registration validation,
  client authentication at the token endpoint.
- ``authorization`` — the authorize → login home → consent → code leg
  (pending requests and single-use codes live in Valkey).
- ``tokens`` — code exchange, refresh rotation with family revocation,
  RFC 7009 revocation.

Token *validation* on the resource routes is in ``middleware/auth/oauth.py``,
next to the API-key validator it mirrors.
"""
