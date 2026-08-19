"""Tests for extensions materialization pipeline."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from robosystems.operations.extensions.materialize import (
  NODE_TABLES,
  RELATIONSHIP_TABLES,
  TABLE_EXTENSIONS,
  MaterializeResult,
  _filter_tables_for_extensions,
  _staging_sql,
  build_postgres_connstr,
  validate_materializer_against_schema,
)


def _env():
  from robosystems.config import env

  return env


GRAPH_ID = "kg01234567890abcdef"
ENTITY_ID = "entity_kg01234567890abcdef"
CONNSTR = "dbname=extensions user=postgres password=postgres host=pg port=5432"


class TestBuildPostgresConnstr:
  def test_parses_url(self):
    with patch.object(
      _env(),
      "EXTENSIONS_DATABASE_URL",
      "postgresql://myuser:mypass@myhost:5433/extensions",
    ):
      result = build_postgres_connstr()

    assert "dbname=extensions" in result
    assert "user=myuser" in result
    assert "password=mypass" in result
    assert "host=myhost" in result
    assert "port=5433" in result

  def test_defaults_for_minimal_url(self):
    with patch.object(
      _env(),
      "EXTENSIONS_DATABASE_URL",
      "postgresql://localhost/",
    ):
      result = build_postgres_connstr()

    assert "host=localhost" in result
    assert "port=5432" in result

  def test_extracts_dbname_from_path(self):
    with patch.object(
      _env(),
      "EXTENSIONS_DATABASE_URL",
      "postgresql://postgres:postgres@pg:5432/extensions",
    ):
      result = build_postgres_connstr()

    assert "dbname=extensions" in result


class TestStagingSql:
  def test_generates_all_tables(self):
    tables = _staging_sql(GRAPH_ID, ENTITY_ID, CONNSTR)
    expected = set(NODE_TABLES + RELATIONSHIP_TABLES)
    assert set(tables.keys()) == expected

  def test_node_tables_use_postgres_scan(self):
    tables = _staging_sql(GRAPH_ID, ENTITY_ID, CONNSTR)
    for name in ["Entity", "Element", "Transaction", "Entry", "LineItem", "Dimension"]:
      assert "postgres_scan" in tables[name]
      assert CONNSTR in tables[name]
      assert GRAPH_ID in tables[name]

  def test_entity_table_references_entities(self):
    tables = _staging_sql(GRAPH_ID, ENTITY_ID, CONNSTR)
    assert "'entities'" in tables["Entity"]

  def test_element_uses_qname_prefix(self):
    tables = _staging_sql(GRAPH_ID, ENTITY_ID, CONNSTR)
    sql = tables["Element"]
    # Should derive prefix from external_source, not hardcode
    assert "CASE" in sql
    assert "'qb:'" in sql
    assert "'rl:'" in sql

  def test_element_keeps_canonical_taxonomy_qname(self):
    # Library/taxonomy concepts (rs-gaap, fac, cm, …) already carry a
    # canonical namespaced qname; the Element staging must emit it verbatim.
    # Re-prefixing would yield 'rl:rs-gaap:X', which no canonical consumer
    # recognises. Native/import/system CoA accounts still fall through to
    # the 'rl:' tenant prefix.
    sql = _staging_sql(GRAPH_ID, ENTITY_ID, CONNSTR)["Element"]
    assert "COALESCE(e.qname, e.code)" in sql
    assert "NOT IN ('native', 'import', 'system')" in sql

  def test_element_reads_from_elements(self):
    tables = _staging_sql(GRAPH_ID, ENTITY_ID, CONNSTR)
    assert "'elements'" in tables["Element"]

  def test_amounts_converted_to_dollars(self):
    tables = _staging_sql(GRAPH_ID, ENTITY_ID, CONNSTR)
    # Transaction amounts and LineItem amounts should divide by 100
    assert "/ 100.0" in tables["Transaction"]
    assert "/ 100.0" in tables["LineItem"]

  def test_relationship_tables_have_src_dst(self):
    tables = _staging_sql(GRAPH_ID, ENTITY_ID, CONNSTR)
    for name in RELATIONSHIP_TABLES:
      sql = tables[name]
      assert "AS src" in sql
      assert "AS dst" in sql

  def test_entity_has_transaction_uses_entity_id(self):
    tables = _staging_sql(GRAPH_ID, ENTITY_ID, CONNSTR)
    assert ENTITY_ID in tables["ENTITY_HAS_TRANSACTION"]

  def test_structure_table_is_static(self):
    tables = _staging_sql(GRAPH_ID, ENTITY_ID, CONNSTR)
    assert f"'{GRAPH_ID}_coa'" in tables["Structure"]
    assert "ChartOfAccounts" in tables["Structure"]


class TestNonnumericFactStaging:
  """Migration 0021 closed the OLTP/graph fact asymmetry — the staging SQL
  must pass the non-numeric slots through instead of hardcoding them."""

  def test_fact_value_coalesces_string_value(self):
    sql = _staging_sql(GRAPH_ID, ENTITY_ID, CONNSTR)["Fact"]
    # Columns are `f.`-qualified since the projection became a join (the
    # dimension count), so match on the shape rather than the prefix.
    assert "COALESCE(f.string_value, CAST(f.value AS VARCHAR))" in sql

  def test_fact_type_and_value_type_pass_through(self):
    sql = _staging_sql(GRAPH_ID, ENTITY_ID, CONNSTR)["Fact"]
    # Passed through from the OLTP columns, not hardcoded literals.
    assert "'Numeric'" not in sql.replace("fact_type = 'Numeric'", "")
    assert "'inline'" not in sql
    assert "fact_type" in sql
    assert "value_type" in sql
    assert "content_type" in sql

  def test_fact_decimals_fallback_only_for_numeric(self):
    sql = _staging_sql(GRAPH_ID, ENTITY_ID, CONNSTR)["Fact"]
    # Legacy '-2' preserved for numeric rows with unspecified decimals;
    # Nonnumeric rows pass NULL through (no @decimals in XBRL).
    assert "CASE WHEN f.fact_type = 'Numeric'" in sql
    assert "COALESCE(f.decimals, '-2')" in sql

  def test_unit_table_derives_from_fact_units(self):
    sql = _staging_sql(GRAPH_ID, ENTITY_ID, CONNSTR)["Unit"]
    assert "'unit_' || lower(unit)" in sql
    assert "'iso4217:' || unit" in sql
    assert "WHERE fact_type = 'Numeric'" in sql
    # Static USD fallback keeps the legacy node for fact-less graphs.
    assert "'unit_usd'" in sql

  def test_fact_has_unit_excludes_nonnumeric(self):
    sql = _staging_sql(GRAPH_ID, ENTITY_ID, CONNSTR)["FACT_HAS_UNIT"]
    assert "'unit_' || lower(unit)" in sql
    assert "WHERE fact_type = 'Numeric'" in sql

  def test_element_item_type_passes_through_with_derived_flags(self):
    sql = _staging_sql(GRAPH_ID, ENTITY_ID, CONNSTR)["Element"]
    assert "e.item_type" in sql
    assert "COALESCE(e.item_type = 'text_block', false)" in sql
    # NULL item_type keeps the legacy numeric default via COALESCE(..., true).
    assert (
      "COALESCE(e.item_type IN ('monetary', 'shares', 'decimal', 'integer'), true)"
      in sql
    )


class TestEdgeForeignKeyGuards:
  """#757 — coa_mapping structures are materialized so curated 'mapping'
  associations have a valid parent Structure, and edge tables semi-join their
  node sets so one dangling FK can't fail the whole (transactional) COPY and
  zero out the table."""

  def test_structure_no_longer_excludes_coa_mapping(self):
    tables = _staging_sql(GRAPH_ID, ENTITY_ID, CONNSTR)
    sql = tables["Structure"]
    # coa_mapping must NOT be in the exclusion list — its 'mapping' associations
    # reference it as their parent structure.
    assert "'coa_mapping'" not in sql
    # the real chart_of_accounts is still excluded (synthetic node replaces it).
    assert "'chart_of_accounts'" in sql

  def test_structure_has_association_guards_dangling_structure(self):
    tables = _staging_sql(GRAPH_ID, ENTITY_ID, CONNSTR)
    sql = tables["STRUCTURE_HAS_ASSOCIATION"]
    assert "structure_id IN (SELECT identifier FROM Structure)" in sql

  def test_element_has_trait_guards_dangling_refs(self):
    tables = _staging_sql(GRAPH_ID, ENTITY_ID, CONNSTR)
    sql = tables["ELEMENT_HAS_TRAIT"]
    assert "element_id IN (SELECT identifier FROM Element)" in sql
    assert 'trait_id IN (SELECT identifier FROM "Trait")' in sql


class TestREAStaging:
  """REA primitives — Agent + Event nodes + 7 base edges + the
  EVENT_TRIGGERS_TRANSACTION McCarthy bridge edge.
  """

  def test_agent_table_sources_from_agents(self):
    tables = _staging_sql(GRAPH_ID, ENTITY_ID, CONNSTR)
    assert "Agent" in tables
    assert "'agents'" in tables["Agent"]
    # JSONB columns intentionally NOT materialized to the graph.
    assert "address" not in tables["Agent"]
    assert "metadata" not in tables["Agent"]

  def test_event_table_sources_from_events_with_event_action(self):
    tables = _staging_sql(GRAPH_ID, ENTITY_ID, CONNSTR)
    assert "Event" in tables
    assert "'events'" in tables["Event"]
    assert "event_action" in tables["Event"]

  def test_event_amount_converted_from_cents(self):
    """Event.amount mirrors Transaction.amount — BigInteger cents → DOUBLE
    currency-major."""
    tables = _staging_sql(GRAPH_ID, ENTITY_ID, CONNSTR)
    assert "CAST(amount AS DOUBLE) / 100.0" in tables["Event"]

  def test_ledger_spine_carries_is_live_flag(self):
    """Every ledger-spine node materializes an is_live boolean so ad-hoc /
    AI Cypher can restrict to live rows with one uniform `WHERE n.is_live`
    instead of a non-uniform per-node status filter."""
    tables = _staging_sql(GRAPH_ID, ENTITY_ID, CONNSTR)
    assert "(status NOT IN ('voided', 'superseded')) AS is_live" in tables["Event"]
    assert "(status <> 'void')" in tables["Transaction"]
    assert "(status = 'posted')" in tables["Entry"]
    for node in ("Event", "Transaction", "Entry", "LineItem"):
      assert "is_live" in tables[node]

  def test_line_item_is_live_denormalizes_parent_entry(self):
    """LineItem has no status column; its liveness is its parent Entry's,
    joined in at materialization so `WHERE li.is_live` needs no Entry hop.
    entry_id is NOT NULL, so the inner join drops no rows."""
    sql = _staging_sql(GRAPH_ID, ENTITY_ID, CONNSTR)["LineItem"]
    assert "(e.status = 'posted')" in sql
    assert "'entries'" in sql
    assert "e.id = li.entry_id" in sql

  def test_entry_provenance_materialized_and_in_schema(self):
    """Entry.provenance (origin: manual_entry / schedule_derived / etc.) is
    carried into the graph AND declared as a schema Property. The materializer
    has always SELECTed it, but a missing schema Property meant the by-name
    graph loader silently dropped it — this guards against that regressing."""
    from robosystems.schemas.extensions.roboledger import TRANSACTION_NODES

    assert "provenance" in _staging_sql(GRAPH_ID, ENTITY_ID, CONNSTR)["Entry"]
    entry_node = next(n for n in TRANSACTION_NODES if n.name == "Entry")
    assert "provenance" in {p.name for p in entry_node.properties}

  def test_factset_type_and_provenance_materialized_and_in_schema(self):
    """FactSet.factset_type + provenance are carried into the graph AND declared
    as schema Properties. The graph FactSet node was degenerate ({identifier}
    only) and materialize collapsed the OLTP FactSet to `SELECT id AS identifier`,
    silently dropping provenance — this alignment carries both columns. A column
    present in the staging SELECT but missing as a schema Property is dropped by
    the by-name graph loader (same failure class as Entry.provenance), so guard
    both ends stay in sync."""
    from robosystems.schemas.extensions.roboledger import REPORTING_NODES

    sql = _staging_sql(GRAPH_ID, ENTITY_ID, CONNSTR)["FactSet"]
    assert "factset_type" in sql
    assert "provenance" in sql
    factset_node = next(n for n in REPORTING_NODES if n.name == "FactSet")
    prop_names = {p.name for p in factset_node.properties}
    assert "factset_type" in prop_names
    assert "provenance" in prop_names

  def test_entity_has_agent_and_event_fan_out_from_entity_id(self):
    tables = _staging_sql(GRAPH_ID, ENTITY_ID, CONNSTR)
    assert ENTITY_ID in tables["ENTITY_HAS_AGENT"]
    assert ENTITY_ID in tables["ENTITY_HAS_EVENT"]

  def test_event_involves_agent_filters_null_agent_id(self):
    tables = _staging_sql(GRAPH_ID, ENTITY_ID, CONNSTR)
    sql = tables["EVENT_INVOLVES_AGENT"]
    assert "agent_id IS NOT NULL" in sql

  def test_event_self_ref_edges_filter_nulls(self):
    tables = _staging_sql(GRAPH_ID, ENTITY_ID, CONNSTR)
    for edge, col in (
      ("EVENT_OBLIGATED_BY_EVENT", "obligated_by_event_id"),
      ("EVENT_DISCHARGES_EVENT", "discharges_event_id"),
      ("EVENT_REPLACES_EVENT", "replaces_event_id"),
    ):
      sql = tables[edge]
      assert f"{col} IS NOT NULL" in sql, f"{edge} should filter on {col}"

  def test_event_triggers_transaction_uses_triggered_by_event_id(self):
    """McCarthy bridge — `transactions.triggered_by_event_id` is the src;
    transaction id is the dst. Only populated for transactions originating
    from an Event (manual-only transactions have no event)."""
    tables = _staging_sql(GRAPH_ID, ENTITY_ID, CONNSTR)
    sql = tables["EVENT_TRIGGERS_TRANSACTION"]
    assert "triggered_by_event_id" in sql
    assert "AS src" in sql
    assert "transactions" in sql
    assert "triggered_by_event_id IS NOT NULL" in sql


class TestMaterializeResult:
  def test_defaults(self):
    result = MaterializeResult(graph_id=GRAPH_ID)
    assert result.status == "success"
    assert result.tables_staged == []
    assert result.tables_materialized == []
    assert result.total_rows == 0
    assert result.duration_ms == 0
    assert result.errors == []


class TestTableOrdering:
  def test_nodes_before_relationships(self):
    """Node tables should be listed before relationship tables."""
    all_tables = NODE_TABLES + RELATIONSHIP_TABLES
    node_end = max(all_tables.index(n) for n in NODE_TABLES)
    rel_start = min(all_tables.index(r) for r in RELATIONSHIP_TABLES)
    assert node_end < rel_start

  def test_entity_is_first_node(self):
    assert NODE_TABLES[0] == "Entity"

  def test_node_categories(self):
    """Verify NODE_TABLES contains the expected per-extension breakdown.

    Category-level checks beat a hardcoded total count because they produce
    an informative failure message when a new node is added — you can see
    *which* category grew unexpectedly, not just that the number changed.
    """
    by_extension: dict[str, list[str]] = {
      "base": [],
      "roboledger": [],
      "roboinvestor": [],
    }
    for name in NODE_TABLES:
      ext = TABLE_EXTENSIONS.get(name, "base")
      by_extension[ext].append(name)

    # Base ontology nodes universally applicable to the extensions pipeline
    assert set(by_extension["base"]) == {
      "Entity",
      "Element",
      "Dimension",
      "Structure",
      "Association",
      "Trait",
      "Taxonomy",
      "Period",
      "Unit",
      # REA primitives — universal across RoboX extensions
      "Agent",
      "Event",
    }
    # RoboLedger-specific: the three-level ledger + reporting layer nodes
    assert set(by_extension["roboledger"]) == {
      "Transaction",
      "Entry",
      "LineItem",
      "Report",
      "Fact",
      "FactSet",
    }
    # RoboInvestor-specific nodes
    assert set(by_extension["roboinvestor"]) == {
      "Portfolio",
      "Security",
      "Position",
    }

  def test_relationship_categories(self):
    """Same breakdown for RELATIONSHIP_TABLES — fails loud with the missing
    or extra rel names, not just a count diff."""
    by_extension: dict[str, list[str]] = {
      "base": [],
      "roboledger": [],
      "roboinvestor": [],
    }
    for name in RELATIONSHIP_TABLES:
      ext = TABLE_EXTENSIONS.get(name, "base")
      by_extension[ext].append(name)

    # Base ontology edges (taxonomy infrastructure + entity↔taxonomy + REA)
    assert set(by_extension["base"]) == {
      "ENTITY_HAS_TAXONOMY",
      "TAXONOMY_EXTENDS_TAXONOMY",
      "STRUCTURE_HAS_TAXONOMY",
      "STRUCTURE_HAS_ASSOCIATION",
      "ASSOCIATION_HAS_FROM_ELEMENT",
      "ASSOCIATION_HAS_TO_ELEMENT",
      "ELEMENT_HAS_TRAIT",
      # REA edges
      "ENTITY_HAS_AGENT",
      "ENTITY_HAS_EVENT",
      "EVENT_INVOLVES_AGENT",
      "EVENT_AFFECTS_RESOURCE",
      "EVENT_OBLIGATED_BY_EVENT",
      "EVENT_DISCHARGES_EVENT",
      "EVENT_REPLACES_EVENT",
    }
    # RoboLedger edges: three-level ledger + dimensional tags + reporting
    assert set(by_extension["roboledger"]) == {
      "ENTITY_HAS_TRANSACTION",
      "EVENT_TRIGGERS_TRANSACTION",  # McCarthy bridge
      "TRANSACTION_HAS_ENTRY",
      "ENTRY_HAS_LINE_ITEM",
      "LINE_ITEM_RELATES_TO_ELEMENT",
      "TRANSACTION_HAS_DIMENSION",
      "ENTRY_HAS_DIMENSION",
      "LINE_ITEM_HAS_DIMENSION",
      "ENTRY_FROM_SCHEDULE",
      "ENTITY_HAS_REPORT",
      "REPORT_USES_TAXONOMY",
      "REPORT_HAS_FACT",
      "FACT_HAS_ELEMENT",
      "FACT_HAS_PERIOD",
      "FACT_HAS_UNIT",
      "FACT_HAS_ENTITY",
      "STRUCTURE_HAS_FACT_SET",
      "REPORT_HAS_FACT_SET",
      "FACT_SET_CONTAINS_FACT",
    }
    # RoboInvestor edges (entity↔portfolio + security issuance + portfolio structure)
    assert set(by_extension["roboinvestor"]) == {
      "ENTITY_HAS_PORTFOLIO",
      "PORTFOLIO_HAS_POSITION",
      "POSITION_IN_SECURITY",
      "ENTITY_ISSUES_SECURITY",
    }


class TestSchemaConsistency:
  """Guardrails that keep materialize.py in sync with the schema layer.

  These tests are the drift detector for option A of the schema-awareness
  question — they don't make the materializer schema-driven, but they catch
  the common failure modes (forgotten edge wiring, stale references,
  missing extension mappings) at test time instead of at runtime.
  """

  def test_materializer_in_sync_with_schema(self):
    """Fail loud if any schema table is missing from the materializer (or
    vice versa), unless explicitly in the SEC-only / deferred allow-list."""
    # Raises RuntimeError with a clear diff if drift is detected.
    validate_materializer_against_schema()

  def test_every_materializer_table_has_extension_mapping(self):
    """Every entry in NODE_TABLES and RELATIONSHIP_TABLES must be in
    TABLE_EXTENSIONS — otherwise the extension-filter pass would treat it
    as 'base' by default, which can silently wrong-classify investor
    tables onto ledger-only graphs."""
    mapped = set(TABLE_EXTENSIONS.keys())
    unmapped = (set(NODE_TABLES) | set(RELATIONSHIP_TABLES)) - mapped
    assert not unmapped, (
      f"Tables missing from TABLE_EXTENSIONS: {sorted(unmapped)}. "
      f"Add each with its owning extension name."
    )

  def test_extension_filter_excludes_investor_from_ledger_only_graph(self):
    """On a roboledger-only graph, investor node and edge tables must not
    be staged (they don't exist in the LadybugDB schema and would cause
    'Table does not exist' errors)."""
    tables = _filter_tables_for_extensions(
      NODE_TABLES + RELATIONSHIP_TABLES, {"roboledger"}
    )
    investor_leak = {
      "Portfolio",
      "Security",
      "Position",
      "ENTITY_HAS_PORTFOLIO",
      "PORTFOLIO_HAS_POSITION",
      "POSITION_IN_SECURITY",
      "ENTITY_ISSUES_SECURITY",
    } & set(tables)
    assert not investor_leak, (
      f"Investor tables leaked into ledger-only filter: {sorted(investor_leak)}"
    )

  def test_extension_filter_excludes_ledger_from_investor_only_graph(self):
    """Symmetrically — ledger-specific tables must not leak into an
    investor-only materialization."""
    tables = _filter_tables_for_extensions(
      NODE_TABLES + RELATIONSHIP_TABLES, {"roboinvestor"}
    )
    ledger_leak = {
      "Transaction",
      "Entry",
      "LineItem",
      "Report",
      "Fact",
      "FactSet",
      "ENTITY_HAS_TRANSACTION",
      "REPORT_HAS_FACT",
    } & set(tables)
    assert not ledger_leak, (
      f"Ledger tables leaked into investor-only filter: {sorted(ledger_leak)}"
    )

  def test_extension_filter_base_always_included(self):
    """Base tables (Entity, Taxonomy, ENTITY_HAS_TAXONOMY, etc.) must
    materialize regardless of which extension is enabled."""
    base_tables = {"Entity", "Element", "Taxonomy", "ENTITY_HAS_TAXONOMY"}
    for enabled in ({"roboledger"}, {"roboinvestor"}, {"roboledger", "roboinvestor"}):
      tables = set(
        _filter_tables_for_extensions(NODE_TABLES + RELATIONSHIP_TABLES, enabled)
      )
      missing = base_tables - tables
      assert not missing, (
        f"Base tables missing from filter with enabled={enabled}: {sorted(missing)}"
      )


class TestExtensionsMaterializer:
  @pytest.mark.asyncio
  async def test_materialize_success(self):
    from robosystems.operations.extensions.materialize import ExtensionsMaterializer

    mock_client = AsyncMock()
    mock_client.database_exists.return_value = True
    mock_client.execute_write.return_value = {"success": True}
    mock_client.materialize_table.return_value = {"rows_ingested": 10}

    materializer = ExtensionsMaterializer()

    with (
      patch(
        "robosystems.graph_api.client.factory.get_graph_client",
        return_value=mock_client,
      ),
      patch.object(
        _env(),
        "EXTENSIONS_DATABASE_URL",
        "postgresql://postgres:postgres@pg:5432/extensions",
      ),
    ):
      mock_client.__aenter__ = AsyncMock(return_value=mock_client)
      mock_client.__aexit__ = AsyncMock(return_value=False)

      result = await materializer.materialize(GRAPH_ID)

    assert result.status == "success"
    assert result.total_rows > 0
    assert len(result.tables_staged) > 0
    assert len(result.tables_materialized) > 0

  @pytest.mark.asyncio
  async def test_materialize_client_failure(self):
    from robosystems.operations.extensions.materialize import ExtensionsMaterializer

    materializer = ExtensionsMaterializer()

    with patch(
      "robosystems.graph_api.client.factory.get_graph_client",
      side_effect=Exception("Connection refused"),
    ):
      result = await materializer.materialize(GRAPH_ID)

    assert result.status == "error"
    assert len(result.errors) > 0
    assert "Connection refused" in result.errors[0]

  @pytest.mark.asyncio
  async def test_default_entity_id(self):
    from robosystems.operations.extensions.materialize import ExtensionsMaterializer

    mock_client = AsyncMock()
    mock_client.database_exists.return_value = False
    mock_client.create_database.return_value = {"success": True}
    mock_client.install_schema.return_value = {"success": True}
    mock_client.execute_write.return_value = {"success": True}
    mock_client.materialize_table.return_value = {"rows_ingested": 0}

    materializer = ExtensionsMaterializer()

    with (
      patch(
        "robosystems.graph_api.client.factory.get_graph_client",
        return_value=mock_client,
      ),
      patch.object(
        _env(),
        "EXTENSIONS_DATABASE_URL",
        "postgresql://postgres:postgres@pg:5432/extensions",
      ),
      patch(
        "robosystems.schemas.loader.get_contextual_schema_loader",
      ) as mock_loader,
    ):
      mock_loader.return_value.nodes = {}
      mock_loader.return_value.relationships = {}
      mock_client.__aenter__ = AsyncMock(return_value=mock_client)
      mock_client.__aexit__ = AsyncMock(return_value=False)

      result = await materializer.materialize(GRAPH_ID)

    assert result.graph_id == GRAPH_ID

  @pytest.mark.asyncio
  async def test_get_graph_extensions_strips_wip_suffix(self):
    """Blue-green builds a transient ``{graph_id}-wip`` database that is not a
    row in the platform DB. ``_get_graph_extensions`` must strip the ``-wip``
    suffix and resolve the source graph so the WIP inherits the source's full
    extension set — otherwise the lookup misses, falls back to roboledger-only,
    and materializing roboinvestor tables (Portfolio, …) fails."""
    from robosystems.operations.extensions.materialize import ExtensionsMaterializer

    base_id = "kg19ed34f81c37ba3f31fa"
    captured = {}

    class _FakeGraph:
      schema_extensions = ["roboledger", "roboinvestor"]

    class _FakeResult:
      def scalar_one_or_none(self):
        return _FakeGraph() if captured.get("id") == base_id else None

    class _FakeSession:
      def __enter__(self):
        return self

      def __exit__(self, *exc):
        return False

      def execute(self, stmt):
        captured["id"] = next(iter(stmt.compile().params.values()))
        return _FakeResult()

    materializer = ExtensionsMaterializer()
    with patch(
      "robosystems.db.platform.SessionFactory",
      return_value=_FakeSession(),
    ):
      exts = await materializer._get_graph_extensions(f"{base_id}-wip")

    # -wip stripped before the lookup, so the source graph's extensions resolve
    assert captured["id"] == base_id
    assert exts == ["roboledger", "roboinvestor"]


class TestPartialStatusAndSwapGate:
  """A failed edge COPY must not ship as success.

  Edge failures are deliberately non-fatal for the remaining tables, but a
  graph missing a whole relationship table (ENTRY_HAS_LINE_ITEM,
  TRANSACTION_HAS_ENTRY) renders empty statements — the blue-green swap and
  the Dagster consumer both gate on status, so 'partial' has to be a status
  they can see, not a note buried in result.errors.
  """

  def _materializer(self):
    from robosystems.operations.extensions.materialize import (
      ExtensionsMaterializer,
    )

    return ExtensionsMaterializer()

  @pytest.mark.asyncio
  async def test_edge_failure_marks_result_partial(self):
    materializer = self._materializer()
    result = MaterializeResult(graph_id=GRAPH_ID)
    result.tables_staged = ["Entity", "ENTRY_HAS_LINE_ITEM", "TRANSACTION_HAS_ENTRY"]

    async def materialize_table(graph_id, table_name, timeout, source_graph_id=None):
      if table_name == "ENTRY_HAS_LINE_ITEM":
        raise RuntimeError("COPY exploded")
      return {"rows_ingested": 10}

    client = AsyncMock()
    client.materialize_table.side_effect = materialize_table

    await materializer._materialize_tables(client, GRAPH_ID, result)

    assert result.status == "partial"
    assert len(result.errors) == 1
    # The failure is non-fatal: the remaining edge table still materialized.
    assert result.tables_materialized == ["Entity", "TRANSACTION_HAS_ENTRY"]

  @pytest.mark.asyncio
  async def test_node_failure_still_fatal(self):
    materializer = self._materializer()
    result = MaterializeResult(graph_id=GRAPH_ID)
    result.tables_staged = ["Entity", "ENTRY_HAS_LINE_ITEM"]

    client = AsyncMock()
    client.materialize_table.side_effect = RuntimeError("COPY exploded")

    with pytest.raises(RuntimeError):
      await materializer._materialize_tables(client, GRAPH_ID, result)

    assert result.status == "failed"

  async def _run_blue_green(self, final_status: str, lock=None):
    """Drive _materialize_blue_green with a stubbed pipeline."""
    materializer = self._materializer()
    result = MaterializeResult(graph_id=GRAPH_ID)

    async def fake_materialize_tables(
      client, graph_id, res, source_graph_id=None, lock=None
    ):
      res.status = final_status

    client = AsyncMock()
    client.database_exists.return_value = False

    if lock is None:
      lock = _held_lock()

    with (
      patch.object(materializer, "_ensure_database", new=AsyncMock()),
      patch.object(
        materializer, "_get_graph_extensions", new=AsyncMock(return_value=[])
      ),
      patch.object(materializer, "_stage_tables", new=AsyncMock()),
      patch.object(materializer, "_materialize_tables", new=fake_materialize_tables),
      patch(
        "robosystems.operations.extensions.materialize.build_postgres_connstr",
        return_value=CONNSTR,
      ),
    ):
      await materializer._materialize_blue_green(
        client, GRAPH_ID, ENTITY_ID, result, lock
      )

    return client

  @pytest.mark.asyncio
  async def test_clean_build_swaps(self):
    client = await self._run_blue_green("success")
    client.swap_database.assert_called_once()

  @pytest.mark.asyncio
  async def test_partial_build_does_not_swap(self):
    client = await self._run_blue_green("partial")
    client.swap_database.assert_not_called()
    # WIP is abandoned so the last good graph stays active.
    client.delete_database.assert_called_once()

  @pytest.mark.asyncio
  async def test_error_build_does_not_swap(self):
    client = await self._run_blue_green("error")
    client.swap_database.assert_not_called()

  @pytest.mark.asyncio
  async def test_clean_build_passes_lock_token_to_swap(self):
    lock = _held_lock(token="tok-123")
    client = await self._run_blue_green("success", lock=lock)
    client.swap_database.assert_called_once_with(GRAPH_ID, lock_token="tok-123")

  @pytest.mark.asyncio
  async def test_lapsed_lock_aborts_before_swap(self):
    """A lock that lapsed mid-run (extend sees a token mismatch) must abort
    the run and abandon the WIP rather than swap with a stale token — another
    run may now hold the lock and be building against the same graph."""
    from robosystems.operations.extensions.materialize import (
      MaterializationLockError,
    )

    lock = _held_lock()
    lock.extend.return_value = False

    with pytest.raises(MaterializationLockError):
      await self._run_blue_green("success", lock=lock)

  @pytest.mark.asyncio
  async def test_lapsed_lock_abandons_wip(self):
    from robosystems.operations.extensions.materialize import (
      MaterializationLockError,
    )

    materializer = self._materializer()
    result = MaterializeResult(graph_id=GRAPH_ID)
    lock = _held_lock(token="tok-abc")
    lock.extend.return_value = False

    async def fake_materialize_tables(
      client, graph_id, res, source_graph_id=None, lock=None
    ):
      res.status = "success"

    client = AsyncMock()
    # WIP does not exist before the build; exists once the build is abandoned.
    client.database_exists.side_effect = [False, True]

    with (
      patch.object(materializer, "_ensure_database", new=AsyncMock()),
      patch.object(
        materializer, "_get_graph_extensions", new=AsyncMock(return_value=[])
      ),
      patch.object(materializer, "_stage_tables", new=AsyncMock()),
      patch.object(materializer, "_materialize_tables", new=fake_materialize_tables),
      patch(
        "robosystems.operations.extensions.materialize.build_postgres_connstr",
        return_value=CONNSTR,
      ),
      pytest.raises(MaterializationLockError),
    ):
      await materializer._materialize_blue_green(
        client, GRAPH_ID, ENTITY_ID, result, lock
      )

    client.swap_database.assert_not_called()
    client.delete_database.assert_called_once_with(
      f"{GRAPH_ID}-wip", preserve_duckdb=True, lock_token="tok-abc"
    )

  @pytest.mark.asyncio
  async def test_extend_backend_error_is_tolerated(self):
    """A Valkey blip during extend is not a lost lock: the key still carries
    its remaining TTL and competing acquires fail closed while Valkey is down."""
    lock = _held_lock()
    lock.extend.side_effect = ConnectionError("valkey blip")

    client = await self._run_blue_green("success", lock=lock)
    client.swap_database.assert_called_once()

  @pytest.mark.asyncio
  async def test_lock_refreshed_per_materialized_table(self):
    materializer = self._materializer()
    result = MaterializeResult(graph_id=GRAPH_ID)
    result.tables_staged = ["Entity", "Element", "ENTRY_HAS_LINE_ITEM"]
    lock = _held_lock()

    client = AsyncMock()
    client.materialize_table.return_value = {"rows_ingested": 1}

    await materializer._materialize_tables(client, GRAPH_ID, result, lock=lock)

    assert lock.extend.await_count == 3

  @pytest.mark.asyncio
  async def test_lapsed_lock_stops_materialize_tables_before_next_copy(self):
    from robosystems.operations.extensions.materialize import (
      MaterializationLockError,
    )

    materializer = self._materializer()
    result = MaterializeResult(graph_id=GRAPH_ID)
    result.tables_staged = ["Entity", "Element"]
    lock = _held_lock()
    lock.extend.side_effect = [True, False]

    client = AsyncMock()
    client.materialize_table.return_value = {"rows_ingested": 1}

    with pytest.raises(MaterializationLockError):
      await materializer._materialize_tables(client, GRAPH_ID, result, lock=lock)

    # The second table never COPYs once the lock is known to be gone.
    assert client.materialize_table.await_count == 1


def _held_lock(token: str = "tok"):
  """A MaterializationLock stand-in that is held and extends cleanly."""
  lock = AsyncMock()
  lock.token = token
  lock.lock_key = f"materialize_lock:{GRAPH_ID}"
  lock.acquired = True
  lock.acquire.return_value = True
  lock.extend.return_value = True
  lock.release.return_value = True
  lock.last_backend_error = None
  return lock


class TestMaterializeLockGate:
  """``materialize`` takes the per-graph lock around BOTH build paths and
  fails closed when it cannot — an unlocked double-writer silently duplicates
  relationship-table edges, whereas a refused run is retried by the sensor."""

  def _client(self, db_exists: bool):
    client = AsyncMock()
    client.database_exists.return_value = db_exists
    client.execute_write.return_value = {"success": True}
    client.materialize_table.return_value = {"rows_ingested": 1}
    client.create_database.return_value = {"success": True}
    client.install_schema.return_value = {"success": True}
    client.__aenter__ = AsyncMock(return_value=client)
    client.__aexit__ = AsyncMock(return_value=False)
    client._instance_id = "i-test"
    return client

  async def _run(self, db_exists: bool, lock):
    from robosystems.operations.extensions.materialize import ExtensionsMaterializer

    client = self._client(db_exists)
    schema_loader = MagicMock()
    schema_loader.return_value.nodes = {}
    schema_loader.return_value.relationships = {}
    materializer = ExtensionsMaterializer()

    with (
      patch(
        "robosystems.graph_api.client.factory.get_graph_client",
        return_value=client,
      ),
      patch.object(
        _env(),
        "EXTENSIONS_DATABASE_URL",
        "postgresql://postgres:postgres@pg:5432/extensions",
      ),
      patch(
        "robosystems.graph_api.core.ladybug.materialization_lock.MaterializationLock",
        MagicMock(return_value=lock),
      ),
      patch(
        "robosystems.config.valkey_registry.create_async_redis_client",
        return_value=AsyncMock(),
      ),
      patch(
        "robosystems.middleware.graph.instance_busy.begin_destructive_op",
        new=AsyncMock(),
      ),
      patch(
        "robosystems.middleware.graph.instance_busy.end_destructive_op",
        new=AsyncMock(),
      ),
      patch("robosystems.schemas.loader.get_contextual_schema_loader", schema_loader),
    ):
      result = await materializer.materialize(GRAPH_ID)

    return client, result

  @pytest.mark.asyncio
  async def test_first_build_path_acquires_lock(self):
    lock = _held_lock()
    client, result = await self._run(db_exists=False, lock=lock)

    lock.acquire.assert_awaited_once()
    lock.release.assert_awaited_once()
    # No database yet -> direct path (create + COPY in place), no swap.
    client.create_database.assert_awaited()
    client.swap_database.assert_not_called()
    assert result.status == "success"

  @pytest.mark.asyncio
  async def test_rebuild_path_acquires_lock_and_passes_token(self):
    lock = _held_lock(token="tok-bg")
    client, result = await self._run(db_exists=True, lock=lock)

    lock.acquire.assert_awaited_once()
    lock.release.assert_awaited_once()
    client.swap_database.assert_awaited_once_with(GRAPH_ID, lock_token="tok-bg")
    assert result.status == "success"

  @pytest.mark.asyncio
  async def test_lock_held_by_another_run_fails_closed(self):
    lock = _held_lock()
    lock.acquire.return_value = False
    lock.acquired = False
    client, result = await self._run(db_exists=False, lock=lock)

    assert result.status == "error"
    assert "held by another run" in result.errors[0]
    client.create_database.assert_not_called()
    client.execute_write.assert_not_called()

  @pytest.mark.asyncio
  async def test_lock_backend_outage_fails_closed(self):
    """A Valkey outage must NOT degrade to an unlocked run, and the message
    must say the lock service is unavailable, not that another run holds it."""
    lock = _held_lock()
    lock.acquire.return_value = False
    lock.acquired = False
    lock.last_backend_error = "Connection refused"
    client, result = await self._run(db_exists=True, lock=lock)

    assert result.status == "error"
    assert "lock service unavailable" in result.errors[0]
    assert "held by another run" not in result.errors[0]
    client.execute_write.assert_not_called()
    client.swap_database.assert_not_called()

  @pytest.mark.asyncio
  async def test_lock_construction_failure_fails_closed(self):
    """A lock that cannot even be constructed (Valkey client factory raising)
    used to proceed unlocked; it must now refuse the run."""
    from robosystems.operations.extensions.materialize import ExtensionsMaterializer

    client = self._client(db_exists=False)
    materializer = ExtensionsMaterializer()

    with (
      patch(
        "robosystems.graph_api.client.factory.get_graph_client",
        return_value=client,
      ),
      patch(
        "robosystems.config.valkey_registry.create_async_redis_client",
        side_effect=RuntimeError("no valkey"),
      ),
      patch(
        "robosystems.middleware.graph.instance_busy.begin_destructive_op",
        new=AsyncMock(),
      ),
      patch(
        "robosystems.middleware.graph.instance_busy.end_destructive_op",
        new=AsyncMock(),
      ),
    ):
      result = await materializer.materialize(GRAPH_ID)

    assert result.status == "error"
    assert "lock service unavailable" in result.errors[0]
    client.create_database.assert_not_called()


class TestScenarioExclusion:
  """Forecast scenario fact sets must not reach the graph.

  `FactSet.scenario_id` is the scenario axis — NULL means actuals, and
  `forecast_compute` stamps every scenario month with a non-NULL
  scenario_id. The graph schema carries no scenario discriminator, so a
  materialized forecast would blend plan into history for every graph
  reader (fact grids, Cypher, analytical views). Until an OLAP scenario
  leg exists, every fact/fact_set projection must exclude scenario rows.
  """

  GUARDED_FACT_TABLES = [
    "Fact",
    "Period",
    "Unit",
    "FACT_HAS_ELEMENT",
    "FACT_HAS_PERIOD",
    "FACT_HAS_UNIT",
    "FACT_SET_CONTAINS_FACT",
  ]
  FACT_SET_TABLES = ["FactSet", "STRUCTURE_HAS_FACT_SET", "REPORT_HAS_FACT_SET"]
  JOINED_FACT_TABLES = ["REPORT_HAS_FACT", "FACT_HAS_ENTITY"]

  def test_fact_projections_route_through_scenario_guard(self):
    tables = _staging_sql(GRAPH_ID, ENTITY_ID, CONNSTR)
    for name in self.GUARDED_FACT_TABLES:
      assert "sfs.scenario_id IS NULL" in tables[name], (
        f"{name} reads facts without the scenario guard"
      )

  def test_fact_set_projections_filter_scenario(self):
    tables = _staging_sql(GRAPH_ID, ENTITY_ID, CONNSTR)
    for name in self.FACT_SET_TABLES:
      assert "scenario_id IS NULL" in tables[name], (
        f"{name} projects fact_sets without filtering scenario rows"
      )

  def test_joined_fact_projections_filter_scenario(self):
    tables = _staging_sql(GRAPH_ID, ENTITY_ID, CONNSTR)
    for name in self.JOINED_FACT_TABLES:
      assert "fs.scenario_id IS NULL" in tables[name], (
        f"{name} joins fact_sets without filtering scenario rows"
      )

  def test_fact_has_entity_filters_both_union_branches(self):
    sql = _staging_sql(GRAPH_ID, ENTITY_ID, CONNSTR)["FACT_HAS_ENTITY"]
    assert sql.count("fs.scenario_id IS NULL") == 2, (
      "FACT_HAS_ENTITY has two UNION branches; both must filter scenarios"
    )

  def test_has_dimensions_is_derived_not_hardcoded(self):
    """The flag four graph read paths filter consolidated totals on.

    It read `false` / `0` for every fact, so the "consolidated totals only"
    contract was being upheld by the scenario exclusion upstream rather
    than by the flag itself — a filter that passes everything the moment
    anything dimensioned reaches the graph. Nothing dimensioned does today
    (only scenario facts carry a junction row, and those are excluded), so
    deriving it changes no output; it changes what happens when that stops
    being true.
    """
    sql = _staging_sql(GRAPH_ID, ENTITY_ID, CONNSTR)["Fact"]
    assert "false                           AS has_dimensions" not in sql
    assert "0::BIGINT                       AS dimension_count" not in sql
    assert "fact_dimensions" in sql, (
      "has_dimensions must come from the junction, not a literal"
    )
    assert "COALESCE(fd.n, 0) > 0" in sql
    assert "COALESCE(fd.n, 0)::BIGINT" in sql

  def test_no_unguarded_facts_scan_anywhere(self):
    """Completeness sweep: any projection reading facts or fact_sets must
    carry a scenario filter — catches future projections added without one."""
    tables = _staging_sql(GRAPH_ID, ENTITY_ID, CONNSTR)
    for name, sql in tables.items():
      if "'facts'" in sql or "'fact_sets'" in sql:
        assert "scenario_id IS NULL" in sql, (
          f"{name} reads facts/fact_sets without a scenario filter"
        )
