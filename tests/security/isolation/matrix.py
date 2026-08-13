"""Surface definitions for the cross-tenant matrix.

Paths carry `{graph_id}`, filled with the *victim* (target) graph at run
time. Reads are classified by the fingerprint oracle; writes by acceptance;
GraphQL and MCP have their own probes. Kept in one place so extending the
matrix is a data edit, not a code change.
"""

from __future__ import annotations

# Graph-scoped GET reads. Some carry data on a fresh graph (info, schema,
# members, limits, credits); some are legitimately empty (tables, backups,
# subgraphs) — the oracle handles both (empty-for-owner is valid).
REST_READS: list[str] = [
  "/v1/graphs/{graph_id}/info",
  "/v1/graphs/{graph_id}/schema",
  "/v1/graphs/{graph_id}/limits",
  "/v1/graphs/{graph_id}/credits",
  "/v1/graphs/{graph_id}/members",
  "/v1/graphs/{graph_id}/tables",
  "/v1/graphs/{graph_id}/backups",
  "/v1/graphs/{graph_id}/subgraphs",
]

# Cypher over the REST query path — read semantics, the strongest data probe:
# it returns an actual node count from the owner's graph.
CYPHER_READ_PATH = "/v1/graphs/{graph_id}/query/cypher"
CYPHER_READ_BODY = {"query": "MATCH (n) RETURN count(n) AS c", "parameters": {}}

# Graph-scoped writes. Any 2xx from a non-owner is a leak. Bodies are minimal
# and land in the *victim test tenant's* graph, so even an accepted write has
# no real-data blast radius.
REST_WRITES: list[dict] = [
  {
    "label": "update-graph-metadata",
    "path": "/v1/graphs/{graph_id}/operations/update-graph-metadata",
    "body": {"description": "iso-harness cross-tenant write probe"},
  },
  {
    "label": "add-member",
    "path": "/v1/graphs/{graph_id}/members",
    # user_id is filled with the attacker's own id at run time
    "body": {"user_id": "{attacker_user_id}", "role": "admin"},
  },
]

# MCP is always mounted (no domain flag). read-graph-cypher is a data probe;
# write-graph-cypher is a cross-tenant write probe.
MCP_PATH = "/v1/graphs/{graph_id}/mcp/call-tool"
MCP_READ_BODY = {
  "name": "read-graph-cypher",
  "arguments": {"query": "MATCH (n) RETURN count(n) AS c", "parameters": {}},
}
MCP_WRITE_BODY = {
  "name": "write-graph-cypher",
  "arguments": {"query": "CREATE (n:IsoHarnessProbe {marker: 1}) RETURN n"},
}

# GraphQL is flag-gated (EXTENSIONS_GRAPHQL_ENABLED AND a domain flag). The
# `hello` field is the always-on auth probe; it echoes the caller, so it is an
# access-control probe, not a data-leak probe (see classify_graphql).
GRAPHQL_PATH = "/extensions/{graph_id}/graphql"
GRAPHQL_BODY = {"query": "{ hello }"}
