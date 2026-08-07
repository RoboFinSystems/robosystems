"""
Per-graph MCP instructions generator.

Produces the routing guidance handed to MCP clients via the server's
`instructions` handshake field (and the `resolve-subgraph` tool result). The
string is always in the connected agent's context, so it stays short and points
at the live tool *families* rather than restating each tool's schema — the
discovery layer that tells an agent which tool to reach for, given an intent.

Two content sources, split by graph stability:

- Shared repositories pass an authored string (`manifest.agent_instructions`),
  used verbatim — their tool set is fixed and curated, so hand-authored copy is
  lowest-risk and gives full editorial control.
- Entity and generic graphs are generated here from the resolved tool-name set.
  Branching on the names that actually came back means the guidance can never
  reference a tool the graph doesn't expose, and stays consistent-by-
  construction with the gating in ``GraphMCPTools``.
"""

from __future__ import annotations


def _block(*lines: str) -> str:
  """Join non-empty lines with newlines into a single section block."""
  return "\n".join(line for line in lines if line)


def build_instructions(
  *,
  graph_id: str,
  tool_names: set[str],
  is_shared_repo: bool,
  read_only: bool,
  authored_override: str | None = None,
) -> str | None:
  """Build the per-graph instructions string for an MCP client.

  Args:
      graph_id: The active graph identifier.
      tool_names: Names of the tools actually exposed for this graph.
      is_shared_repo: Whether this is a shared repository (or subgraph of one).
      read_only: Whether write operations are disabled for this graph.
      authored_override: Curated text (shared-repo manifest); used verbatim.

  Returns:
      The instructions string, or None if there is nothing useful to say.
  """
  if authored_override and authored_override.strip():
    return authored_override.strip()

  has = tool_names.__contains__
  has_ledger = (
    has("get-close-playbook") or has("suggest-mapping") or has("get-unmapped-elements")
  )
  has_portfolio = has("create-portfolio-block") or has("create-security")

  # Header — adapt to the graph's category.
  if is_shared_repo:
    header = (
      f"Connected to shared repository `{graph_id}` — curated, READ-ONLY data "
      "for analysis and exploration."
    )
  elif has_ledger:
    header = (
      f"Connected to a RoboLedger entity graph `{graph_id}` — a live general "
      "ledger with XBRL-grade financial reporting. Reads and writes."
    )
  elif has_portfolio:
    header = (
      f"Connected to a RoboInvestor entity graph `{graph_id}` — portfolio and "
      "investment tracking."
    )
  else:
    header = (
      f"Connected to a custom property graph `{graph_id}`. No financial schema "
      "extension is installed."
    )

  sections: list[str] = [header]

  # Close / month-end — anchored on the playbook tool.
  if has("get-close-playbook"):
    close_lines = [
      "CLOSE / MONTH-END",
      (
        "- Before setting up or running a close, call `get-close-playbook` "
        "FIRST — it lays out the exact tool sequence, the setup decisions, "
        "and the gotchas. Don't improvise the close from individual tool "
        "schemas."
      ),
    ]
    orient_tools = [
      t for t in ("get-fiscal-calendar", "get-period-close-status") if has(t)
    ]
    if orient_tools:
      orient = "- Orient with " + " and ".join(f"`{t}`" for t in orient_tools)
      if has("get-graph-sync-status"):
        orient += "; check data freshness with `get-graph-sync-status`."
      else:
        orient += "."
      close_lines.append(orient)
    if has("sync-connection"):
      close_lines.append(
        "- Stale source data (`sync_stale` blocker)? Pull fresh data with "
        "`sync-connection`, verify the outcome via `get-graph-sync-status` "
        "(connections[].last_sync_result), then re-check "
        "`get-fiscal-calendar`."
      )
    sections.append(_block(*close_lines))

  # Chart-of-accounts mapping.
  if has("suggest-mapping") or has("get-unmapped-elements"):
    sections.append(
      _block(
        "CHART OF ACCOUNTS / MAPPING",
        "- See what's unmapped → `get-unmapped-elements`; get suggestions → "
        "`suggest-mapping`; inspect structures → `list-mapping-structures` / "
        "`get-mapping-summary`.",
      )
    )

  # Reporting & analysis — only the bits that are live.
  report_bits: list[str] = []
  if has("live-financial-statement"):
    report_bits.append("live statements → `live-financial-statement`")
  if has("build-fact-grid"):
    report_bits.append("multidimensional pivots → `build-fact-grid`")
  if has("financial-statement-analysis"):
    report_bits.append("company analysis → `financial-statement-analysis`")
  if report_bits:
    sections.append(_block("REPORTING & ANALYSIS", "- " + "; ".join(report_bits) + "."))

  # Portfolios & securities.
  if has_portfolio:
    sections.append(
      _block(
        "PORTFOLIOS & SECURITIES",
        "- Portfolios → `create-portfolio-block` / `update-portfolio-block`; "
        "securities → `create-security` / `update-security`.",
      )
    )

  # Exploration / querying — present on essentially every graph.
  explore_lines = ["EXPLORE"]
  explore_start = [t for t in ("get-graph-schema", "get-example-queries") if has(t)]
  if explore_start:
    explore_lines.append(
      "- Start with " + " and ".join(f"`{t}`" for t in explore_start) + "."
    )
  query_bits: list[str] = []
  if has("query-graphql"):
    query_bits.append("typed reads → `query-graphql`")
  if has("read-graph-cypher"):
    query_bits.append("raw traversal → `read-graph-cypher`")
  if query_bits:
    explore_lines.append("- " + "; ".join(query_bits) + ".")
  if not read_only and has("write-graph-cypher"):
    explore_lines.append(
      "- Writes → `write-graph-cypher`, `add-node-table`, `add-relationship-table`."
    )
  if len(explore_lines) > 1:
    sections.append(_block(*explore_lines))

  # Semantic memory — durable notes the agent writes and recalls across sessions.
  if has("recall") or has("remember"):
    mem_lines = ["MEMORY"]
    if has("recall"):
      mem_lines.append(
        "- Recall what you've stored before → `recall` (ranked semantic search "
        "over this graph's memory); check it before re-deriving context you may "
        "already know."
      )
    if has("remember") and not read_only:
      mem_lines.append(
        "- Persist a durable fact or decision → `remember`; remove a stale one → "
        "`forget`."
      )
    if len(mem_lines) > 1:
      sections.append(_block(*mem_lines))

  # Tenant-specific procedure docs (the per-graph layer the generic playbooks
  # defer to) — only meaningful on a writable entity graph.
  if has("search-documents") and not is_shared_repo:
    sections.append(
      "ALSO run `search-documents` for this company's own procedure and policy "
      "docs — they capture tenant-specific accounts and quirks the generic "
      "playbooks can't."
    )

  if is_shared_repo:
    sections.append(
      "This is shared read-only data: period close, mapping, and write "
      "operations are unavailable here."
    )

  result = "\n\n".join(s for s in sections if s).strip()
  return result or None
