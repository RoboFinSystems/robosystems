"""The MCP `instructions` handshake text for a graph.

It sits in the agent's context all session, so it stays short and routes by
tool family. Shared repositories supply authored text; other graphs are
generated from the exposed tool names, so the text never names a tool the
graph lacks.
"""

from __future__ import annotations


def _block(*lines: str) -> str:
  return "\n".join(line for line in lines if line)


def build_instructions(
  *,
  graph_id: str,
  tool_names: set[str],
  is_shared_repo: bool,
  read_only: bool,
  authored_override: str | None = None,
) -> str | None:
  """`authored_override` is used verbatim. None when there is nothing to say."""
  if authored_override and authored_override.strip():
    return authored_override.strip()

  has = tool_names.__contains__
  has_ledger = (
    has("get-close-playbook") or has("suggest-mapping") or has("get-unmapped-elements")
  )
  has_portfolio = has("create-portfolio-block") or has("create-security")

  # Header by graph category.
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

  # The inbox loop for bank-feed lines: classify, then commit.
  if has("update-event-block") and has("list-event-blocks"):
    inbox_lines = [
      "INBOX / BANK FEED",
      (
        "- A bank feed (Mercury, Plaid) lands every posted transaction `captured` "
        "with `metadata.suggested_element_id` + `suggested_account_name`; "
        "nothing posts until it is classified. Find them → "
        "`list-event-blocks(source='mercury' or 'plaid', status='captured')`."
      ),
      (
        "- Classify one → `update-event-block(event_id, "
        "transition_to='classified', metadata_patch={classified_element_id, "
        "classified_by: 'ai', basis})`. A split: `classified_allocations: "
        "[{element_id, amount}]` — positive cents summing to the line's "
        "absolute amount. To take the suggestion as-is: "
        "`metadata_patch={accept_suggestion: true}` (only when "
        "`suggested_element_id` is set; a name-only suggestion needs a "
        "choice). Classifying validates the choice — one that resolves no "
        "account is refused there, with the reason."
      ),
      (
        "- Post it → `transition_to='committed'` (a person, or you when "
        "asked): the handler writes DR/CR against the linked bank account as "
        "a draft that close posts. An unclassified bank line is refused at "
        "commit — never force it. Internal transfers are pre-classified "
        "(both bank legs known) and commit as they are."
      ),
    ]
    if has("recall") and has("remember"):
      inbox_lines.append(
        "- `recall` the counterparty before deciding, and `remember` each "
        "decision ('Stripe payouts → Subscription revenue') so the next line "
        "from that counterparty classifies itself."
      )
    if has("preview-event-block"):
      inbox_lines.append(
        "- Unsure what a commit would write? `preview-event-block` with the "
        "line's fields (from `get-event-block`) and your classification in "
        "`metadata` shows the planned entry."
      )
    sections.append(_block(*inbox_lines))

  # Reporting & analysis — only the bits that are live.
  report_bits: list[str] = []
  if has("live-financial-statement"):
    report_bits.append("live statements → `live-financial-statement`")
  if has("build-fact-grid"):
    report_bits.append("multidimensional pivots → `build-fact-grid`")
  if has("financial-statement-analysis"):
    report_bits.append("company analysis → `financial-statement-analysis`")
  if has("disclosures") and has("information-block"):
    report_bits.append(
      "a report's sections → `disclosures` (the map, cheap) then "
      "`information-block` (one section whole: breakdowns, footing, text)"
    )
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

  # Tenant procedure docs, which the generic playbooks defer to.
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

  # Docs as URLs, so a model can cite them.
  if is_shared_repo:
    # Only reached by a future repository without authored instructions;
    # the SEC guide is named for `sec` alone.
    doc_lines = (
      [
        "- https://robosystems.ai/docs/guides/sec-filings — what to ask of the "
        "filings, and the rules that decide whether a number is right"
      ]
      if graph_id == "sec"
      else [
        "- https://robosystems.ai/docs/guides — the platform: connecting a "
        "client, graph access, shared repositories, credits"
      ]
    )
  elif has_ledger:
    doc_lines = [
      "- https://roboledger.ai/docs — what RoboLedger does with a company's "
      "books: reporting, planning, the close, and what writes back to QuickBooks",
      "- https://robosystems.ai/docs/guides — the platform itself: connecting a "
      "client, graph access, SEC filings, credits",
    ]
  else:
    doc_lines = [
      "- https://robosystems.ai/docs/guides — the platform: connecting a "
      "client, graph access, SEC filings, credits"
    ]
  sections.append(_block("DOCUMENTATION", *doc_lines))

  result = "\n\n".join(s for s in sections if s).strip()
  return result or None
