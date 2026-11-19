"""Tests for the per-graph MCP instructions generator.

The generator is a pure function over the resolved tool-name set. The core
invariant — and the reason it's generated rather than hand-authored for entity
graphs — is that it can never reference a tool the graph doesn't expose. These
tests pin that invariant plus the per-category routing.
"""

from __future__ import annotations

from robosystems.middleware.mcp.tools.instructions import build_instructions

# Representative tool sets per graph category.
_CORE = {"read-graph-cypher", "get-graph-schema", "get-example-queries"}
_LEDGER = _CORE | {
  "get-close-playbook",
  "get-fiscal-calendar",
  "get-period-close-status",
  "get-graph-sync-status",
  "get-unmapped-elements",
  "suggest-mapping",
  "list-mapping-structures",
  "get-mapping-summary",
  "live-financial-statement",
  "build-fact-grid",
  "financial-statement-analysis",
  "disclosures",
  "information-block",
  "search-documents",
  "write-graph-cypher",
  "query-graphql",
}
_PORTFOLIO = _CORE | {"create-portfolio-block", "create-security", "query-graphql"}


class TestAuthoredOverride:
  def test_shared_repo_uses_authored_verbatim(self) -> None:
    authored = "Connected to `sec`.\n\nSTART HERE\n- do the thing."
    out = build_instructions(
      graph_id="sec",
      tool_names=_CORE,
      is_shared_repo=True,
      read_only=True,
      authored_override=authored,
    )
    assert out == authored

  def test_blank_override_falls_through_to_generated(self) -> None:
    out = build_instructions(
      graph_id="sec",
      tool_names=_CORE,
      is_shared_repo=True,
      read_only=True,
      authored_override="   ",
    )
    assert out is not None
    assert "READ-ONLY" in out
    assert "https://robosystems.ai/docs/guides/sec-filings" in out

  def test_another_shared_repo_is_not_sent_to_the_sec_guide(self) -> None:
    out = build_instructions(
      graph_id="edgar_eu",
      tool_names=_CORE,
      is_shared_repo=True,
      read_only=True,
    )
    assert out is not None
    assert "sec-filings" not in out
    assert "https://robosystems.ai/docs/guides" in out


class TestLedgerGraph:
  def _out(self) -> str:
    out = build_instructions(
      graph_id="kgLEDGER",
      tool_names=_LEDGER,
      is_shared_repo=False,
      read_only=False,
    )
    assert out is not None
    return out

  def test_header_identifies_roboledger(self) -> None:
    assert "RoboLedger entity graph" in self._out()

  def test_close_routes_to_playbook_first(self) -> None:
    out = self._out()
    assert "`get-close-playbook` FIRST" in out
    assert "get-fiscal-calendar" in out

  def test_mapping_section_present(self) -> None:
    assert "`suggest-mapping`" in self._out()

  def test_writes_listed_when_writable(self) -> None:
    assert "`write-graph-cypher`" in self._out()

  def test_tenant_docs_hint_present(self) -> None:
    assert "search-documents" in self._out()

  def test_public_docs_are_offered_as_urls(self) -> None:
    out = self._out()
    assert "DOCUMENTATION" in out
    assert "https://roboledger.ai/docs" in out
    assert "https://robosystems.ai/docs/guides" in out

  def test_sections_route_to_the_map_then_the_block(self) -> None:
    out = self._out()
    assert "`disclosures` (the map, cheap)" in out
    assert "`information-block`" in out

  def test_the_pair_is_named_only_together(self) -> None:
    out = build_instructions(
      graph_id="kgLEDGER",
      tool_names=_LEDGER - {"information-block"},
      is_shared_repo=False,
      read_only=False,
    )
    assert out is not None
    assert "`disclosures`" not in out


class TestGenericGraph:
  def test_generic_header_and_no_ledger_routing(self) -> None:
    out = build_instructions(
      graph_id="kgPLAIN",
      tool_names=_CORE,
      is_shared_repo=False,
      read_only=False,
    )
    assert out is not None
    assert "custom property graph" in out
    # The invariant: tools not in the set are never named.
    assert "get-close-playbook" not in out
    assert "suggest-mapping" not in out
    assert "write-graph-cypher" not in out


class TestPortfolioGraph:
  def test_header_identifies_roboinvestor(self) -> None:
    out = build_instructions(
      graph_id="kgPORT",
      tool_names=_PORTFOLIO,
      is_shared_repo=False,
      read_only=False,
    )
    assert out is not None
    assert "RoboInvestor entity graph" in out
    assert "`create-security`" in out


class TestSharedRepoGenerated:
  def test_read_only_footer_and_no_writes(self) -> None:
    out = build_instructions(
      graph_id="custrepo",
      tool_names=_CORE | {"write-graph-cypher"},
      is_shared_repo=True,
      read_only=True,
    )
    assert out is not None
    assert "shared read-only data" in out
    # read_only suppresses the write routing even if the tool name leaks in.
    assert "`write-graph-cypher`" not in out


class TestInvariant:
  def test_only_present_tools_are_referenced(self) -> None:
    # A ledger graph missing the sync-status tool must not mention it.
    tools = _LEDGER - {"get-graph-sync-status"}
    out = build_instructions(
      graph_id="kgLEDGER",
      tool_names=tools,
      is_shared_repo=False,
      read_only=False,
    )
    assert out is not None
    assert "get-graph-sync-status" not in out
    # but the orient line still works off the remaining tools
    assert "get-fiscal-calendar" in out


class TestInboxRouting:
  _INBOX = _LEDGER | {
    "list-event-blocks",
    "update-event-block",
    "preview-event-block",
    "recall",
    "remember",
  }

  def _build(self, tools):
    return build_instructions(
      graph_id="kg1", tool_names=tools, is_shared_repo=False, read_only=False
    )

  def test_bank_feed_loop_present_with_the_write_tools(self) -> None:
    out = self._build(self._INBOX)
    assert "INBOX / BANK FEED" in out
    assert "transition_to='classified'" in out
    assert "accept_suggestion: true" in out
    assert "refused at commit" in out
    assert "`recall` the counterparty" in out
    assert "`preview-event-block`" in out

  def test_bank_feed_loop_absent_without_update_tool(self) -> None:
    out = self._build(_LEDGER | {"list-event-blocks"})
    assert "INBOX / BANK FEED" not in out

  def test_memory_and_preview_lines_track_their_tools(self) -> None:
    out = self._build(_LEDGER | {"list-event-blocks", "update-event-block"})
    assert "INBOX / BANK FEED" in out
    assert "`recall`" not in out
    assert "`preview-event-block`" not in out


_PARENT = {"id": "ent_parent", "name": "Harbor Holdings", "is_group_parent": True}
_SUB = {"id": "ent_sub", "name": "Maple Court LLC", "is_group_parent": False}


class TestEntities:
  def _ledger(self, entities, extra=()):
    return build_instructions(
      graph_id="kg_x",
      tool_names=_LEDGER | set(extra),
      is_shared_repo=False,
      read_only=False,
      entities=entities,
    )

  def test_a_group_names_its_parent_and_subsidiaries_and_the_rule(self) -> None:
    out = self._ledger([_PARENT, _SUB], extra={"create-entity", "get-graph-info"})
    assert out is not None
    assert "ENTITIES" in out
    assert "Group parent: `Harbor Holdings` (`ent_parent`)" in out
    assert "Subsidiaries: `Maple Court LLC` (`ent_sub`)" in out
    assert "omitted, it acts on the group parent" in out
    assert "`create-entity` adds one." in out
    assert "`get-graph-info` lists them." in out

  def test_one_entity_is_named_with_the_way_to_add_another(self) -> None:
    out = self._ledger([_PARENT], extra={"create-entity"})
    assert out is not None
    assert (
      "One entity so far: `Harbor Holdings` (`ent_parent`), the group parent." in out
    )
    assert "`create-entity` adds one." in out
    assert "Subsidiaries:" not in out

  def test_tools_the_graph_lacks_are_not_named(self) -> None:
    out = self._ledger([_PARENT, _SUB])
    assert out is not None
    assert "create-entity" not in out
    assert "get-graph-info" not in out

  def test_without_the_group_nothing_is_said(self) -> None:
    out = self._ledger(None)
    assert out is not None
    assert "ENTITIES" not in out
    assert "ENTITIES" not in (
      build_instructions(
        graph_id="kg_x",
        tool_names=_PORTFOLIO,
        is_shared_repo=False,
        read_only=False,
        entities=[_PARENT],
      )
      or ""
    )


_ADJUSTMENTS = {
  "id": "ent_cons",
  "name": "Consolidation Adjustments",
  "is_group_parent": False,
  "entity_role": "consolidation",
  "consolidation_method": "none",
}
_KIOSK = {
  "id": "ent_kiosk",
  "name": "Pier Market Kiosk LLC",
  "is_group_parent": False,
  "consolidation_method": "equity",
}


class TestConsolidation:
  def _ledger(self, entities, extra=()):
    return build_instructions(
      graph_id="kg_x",
      tool_names=_LEDGER | set(extra),
      is_shared_repo=False,
      read_only=False,
      entities=entities,
    )

  def test_the_consolidation_entity_and_the_scopes_are_explained(self) -> None:
    out = self._ledger([_PARENT, _SUB, _KIOSK, _ADJUSTMENTS])
    assert out is not None
    assert "Consolidation entity: `Consolidation Adjustments` (`ent_cons`)." in out
    assert "`Pier Market Kiosk LLC` (`ent_kiosk`) (equity method, not summed)" in out
    assert "Subsidiaries: `Maple Court LLC` (`ent_sub`), `Pier" in out
    assert "CONSOLIDATION" in out
    assert "is the group's consolidation entity" in out
    assert "`live-financial-statement` on the group parent takes `scope`" in out
    # The tag rule names the journal tool, which this set lacks.
    assert "flow_qname" not in out

  def test_the_tag_rule_rides_the_journal_tool(self) -> None:
    out = self._ledger([_PARENT, _ADJUSTMENTS], extra={"update-journal-entry"})
    assert out is not None
    assert "carries a `flow_qname` counts" in out

  def test_the_worksheet_rides_its_tool(self) -> None:
    out = self._ledger([_PARENT, _ADJUSTMENTS], extra={"consolidation-worksheet"})
    assert out is not None
    assert "`consolidation-worksheet` lays the group's statement out" in out
    without = self._ledger([_PARENT, _ADJUSTMENTS])
    assert without is not None
    assert "consolidation-worksheet" not in without

  def test_a_plain_group_says_nothing_about_consolidation(self) -> None:
    out = self._ledger([_PARENT, _SUB])
    assert out is not None
    assert "CONSOLIDATION" not in out
    assert "not summed" not in out

  def test_a_cost_member_and_a_departed_member_are_marked(self) -> None:
    stake = {
      "id": "ent_stake",
      "name": "Stake Co",
      "is_group_parent": False,
      "consolidation_method": "cost",
    }
    former = {
      "id": "ent_former",
      "name": "Old Mill LLC",
      "is_group_parent": False,
      "consolidation_method": "full",
      "left_on": "2026-06-30",
    }
    out = self._ledger([_PARENT, stake, former])
    assert out is not None
    assert "`Stake Co` (`ent_stake`) (an investment at cost, not summed)" in out
    assert "`Old Mill LLC` (`ent_former`) (left the group 2026-06)" in out

  def test_no_bare_header_when_no_line_survives(self) -> None:
    out = build_instructions(
      graph_id="kg_x",
      tool_names=_LEDGER - {"live-financial-statement"},
      is_shared_repo=False,
      read_only=False,
      entities=[_PARENT, _KIOSK],
    )
    assert out is not None
    assert "CONSOLIDATION" not in out
    assert "(equity method, not summed)" in out


class TestNamedReadsRideTheirTools:
  """Each named ledger read is a tool of its own, and its line names it only
  where the graph serves it: the RoboLedger route serves no generic query."""

  _INBOX = _LEDGER | {"list-event-blocks", "update-event-block", "aged-balances"}
  _READS = {
    "get-account-register",
    "list-flow-candidates",
    "get-classification-stats",
    "list-discharge-candidates",
  }

  def _build(self, tools):
    return build_instructions(
      graph_id="kg1", tool_names=tools, is_shared_repo=False, read_only=False
    )

  def test_each_read_is_named_by_its_tool(self) -> None:
    out = self._build(self._INBOX | self._READS)
    assert "`get-account-register(account_id, start_date, end_date, entity_id)`" in out
    assert "`list-flow-candidates(entity_id)`" in out
    assert "`get-classification-stats(period)`" in out
    assert "`list-discharge-candidates(event_id)`" in out

  def test_no_read_routes_through_the_generic_query(self) -> None:
    out = self._build(self._INBOX | self._READS)
    for field in (
      "accountRegister",
      "flowCandidates",
      "classificationStats",
      "dischargeCandidates",
    ):
      assert field not in out
    assert "over `query-graphql`" not in out

  def test_without_the_reads_their_lines_stand_without_them(self) -> None:
    out = self._build((self._INBOX | self._READS) - self._READS - {"query-graphql"})
    for read in self._READS:
      assert read not in out
    assert "graphql" not in out.lower()
    # The surrounding guidance survives the cut.
    assert "`classified_flow_qname`" in out
    assert "`discharge_allocations: [{obligation_event_id, amount}]`" in out


class TestTheBooksAsTheyStand:
  """The live-ledger reads are named where the close and the chart are
  discussed, each only when the graph serves it."""

  _READS = {
    "get-trial-balance",
    "list-journal-entries",
    "list-reconciliations",
    "list-accounts",
    "search-library-elements",
    "get-mapped-trial-balance",
  }

  def _build(self, tools):
    return build_instructions(
      graph_id="kg1", tool_names=tools, is_shared_repo=False, read_only=False
    )

  def test_the_close_names_the_ledger_reads(self) -> None:
    out = self._build(_LEDGER | self._READS)
    assert "`get-trial-balance`" in out
    assert "`list-journal-entries` (`status='draft'`" in out
    assert "`list-reconciliations(period)`" in out
    assert "`list-accounts`" in out
    assert "`search-library-elements`" in out
    assert "`get-mapped-trial-balance`" in out

  def test_without_them_nothing_is_said(self) -> None:
    out = self._build(_LEDGER)
    for read in self._READS:
      assert read not in out
    assert "The books as they stand" not in out
    assert "The chart itself" not in out

  def test_one_read_alone_is_named_alone(self) -> None:
    out = self._build(_LEDGER | {"get-trial-balance"})
    assert "`get-trial-balance`." in out
    assert "list-journal-entries" not in out


class TestReceivingReports:
  """A graph that can ingest a report is told how: values, not layout."""

  _RECEIVE = _PORTFOLIO | {
    "create-issuer",
    "ingest-report",
    "search-library-elements",
    "list-reports",
    "disclosures",
    "information-block",
  }

  def _build(self, tools):
    return build_instructions(
      graph_id="kg1", tool_names=tools, is_shared_repo=False, read_only=False
    )

  def test_the_route_from_a_document_to_a_report_is_named(self) -> None:
    out = self._build(self._RECEIVE)
    assert "RECEIVING REPORTS" in out
    assert "`create-issuer`" in out
    assert "`layout: library`" in out
    assert "sign of its concept" in out
    assert "`whole_library: true` and `brief: true`" in out
    assert "`dry_run: true`" in out
    assert "`list-reports(entity_id)`" in out

  def test_a_graph_that_cannot_ingest_is_told_nothing(self) -> None:
    assert "RECEIVING REPORTS" not in self._build(_PORTFOLIO)
