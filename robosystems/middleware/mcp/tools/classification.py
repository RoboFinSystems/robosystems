"""Which MCP tools and REST operations read and which mutate — one
definition, shared by authorization (viewers may call only reads) and the
mutation audit on every surface. Tools and operations share names."""

# Write classification is fail-closed: any tool not in this allowlist is a
# write and needs the member/admin role, so every new tool (including
# registrar-generated command ops) defaults to write. The `read-*-cypher` tools
# are absent on purpose: the StatementKernel classifies them per statement.
# A new read tool must be added here, or viewers can't call it.
READ_ONLY_MCP_TOOLS: frozenset[str] = frozenset(
  {
    # Graph introspection / exploration
    "get-graph-info",
    "get-graph-schema",
    "get-graphql-schema",
    "get-graph-sync-status",
    "get-example-queries",
    "query-graphql",
    "list-subgraphs",
    # Financial analysis / reporting reads
    "financial-statement-analysis",
    "live-financial-statement",
    "build-fact-grid",
    "resolve-element",
    "disclosures",
    "information-block",
    "describe-filing",
    "search-text",
    "read-text",
    "get-report-bundle",
    # Fiscal calendar / close reads
    "get-fiscal-calendar",
    "get-period-close-status",
    "get-close-playbook",
    "list-period-drafts",
    # Mapping reads
    "get-unmapped-elements",
    "suggest-mapping",
    "list-mapping-structures",
    "get-mapping-summary",
    # Agent reads
    "get-agent",
    "list-agents",
    "agent-activity",
    # Event block / handler reads
    "get-event-block",
    "list-event-blocks",
    "get-event-handler",
    "list-event-handlers",
    # Dry runs: they plan journal lines and persist nothing.
    "preview-event-block",
    "preview-reconciling-item",
    # Information block reads
    "get-information-block",
    "list-information-blocks",
    # Document / memory reads
    "get-document",
    "list-documents",
    "get-document-section",
    "search-documents",
    "recall",
  }
)

# Cypher read tools are classified per statement (`assert_read_only_cypher`
# guards every path), so they stay out of the allowlist above.
CYPHER_READ_TOOLS: frozenset[str] = frozenset(
  {"read-graph-cypher", "read-neo4j-cypher", "read-ladybug-cypher"}
)


def is_mutating_tool(name: str) -> bool:
  """Fail-closed: anything not known to be a read is a mutation."""
  return name not in READ_ONLY_MCP_TOOLS and name not in CYPHER_READ_TOOLS
