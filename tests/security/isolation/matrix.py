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
    "label": "add-member",
    "path": "/v1/graphs/{graph_id}/members",
    # user_id is filled with the attacker's own id at run time
    "body": {"user_id": "{attacker_user_id}", "role": "admin"},
  },
]

# Destructive / sensitive core graph operations — the ones where a cross-tenant
# acceptance would be worst (another tenant deleting, re-tiering, or backing up
# your graph). Attacker-only probe; authz should deny before the body matters,
# so empty bodies suffice (a 422 instead of 403 is itself a finding).
CORE_OP_PATH = "/v1/graphs/{graph_id}/operations/{op}"
CORE_OP_WRITES: list[str] = [
  "update-graph-metadata",
  "create-backup",
  "materialize",
  "create-subgraph",
  "change-tier",
  "delete-graph",
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

# GraphQL is flag-gated (EXTENSIONS_GRAPHQL_ENABLED AND a domain flag). Two
# probes: `hello` echoes the caller (access-control probe), while `entity` reads
# actual graph data (a real data-leak probe — a cross-tenant entity payload is a
# leak). On a roboledger graph the entity is the initial_entity created at setup.
GRAPHQL_PATH = "/extensions/{graph_id}/graphql"
GRAPHQL_HELLO_BODY = {"query": "{ hello }"}
GRAPHQL_DATA_BODY = {"query": "{ entity { name } }"}

# The extensions command/view surfaces for both product domains (the graphs are
# provisioned with both extensions). Any 2xx from a non-owner is a leak; empty
# bodies suffice for a denial probe — authz (get_current_user_with_graph /
# require_graph_write_role) should turn a non-owner away before the body ever
# matters. A 422 instead of 403 is itself a finding (validation ran before
# authz) and lands as INCONCLUSIVE. Confirms each domain's operations router is
# wired with the same graph-access gate, not just roboledger's.
EXTENSIONS_OP_PATH = "/extensions/{domain}/{graph_id}/operations/{op}"
EXTENSIONS_OPS: list[dict] = [
  {"domain": "roboledger", "op": "build-fact-grid"},  # view / read
  {"domain": "roboledger", "op": "compute-metrics"},  # read / compute
  {"domain": "roboledger", "op": "create-event-block"},  # write
  {"domain": "roboledger", "op": "create-report"},  # write
  {"domain": "roboledger", "op": "close-period"},  # sensitive write
  {"domain": "roboinvestor", "op": "create-portfolio-block"},  # write
  {"domain": "roboinvestor", "op": "create-security"},  # write
  {"domain": "roboinvestor", "op": "update-portfolio-block"},  # write
  {"domain": "roboinvestor", "op": "delete-security"},  # sensitive write
]
