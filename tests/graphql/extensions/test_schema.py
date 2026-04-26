"""Schema composition + SDL shape tests.

Verifies the Query root is composed from the ledger + investor resolver
classes and every expected field is reachable from the schema. These
catch wiring regressions early (a missing resolver registration, a
stray `dict`-typed field, a recursive type that doesn't resolve).
"""

from __future__ import annotations

from unittest.mock import patch

from robosystems.graphql.schema import schema


class TestSchemaComposition:
  def test_schema_compiles(self) -> None:
    sdl = str(schema)
    assert "type Query" in sdl
    assert len(sdl) > 1000  # guard against an empty schema regression

  def test_all_ledger_queries_are_exposed(self) -> None:
    """Every ledger query on the Query root.

    The endpoint is graph-scoped at the URL level, so resolvers don't
    take `graphId` as an argument. Fields with no other args appear as
    `entity: LedgerEntity` rather than `entity(graphId: ID!): …`.
    """
    sdl = str(schema)
    start = sdl.find("type Query {")
    assert start >= 0
    end = sdl.find("}", start)
    query_block = sdl[start:end]
    for field_name in [
      "entity",
      "entities",
      "summary",
      "accounts",
      "accountTree",
      "accountRollups",
      "trialBalance",
      "transactions",
      "transaction",
      "taxonomies",
      "reportingTaxonomy",
      "elements",
      "unmappedElements",
      "structures",
      "mappings",
      "mapping",
      "mappingCoverage",
      "mappedTrialBalance",
      "periodCloseStatus",
      "fiscalCalendar",
      "periodDrafts",
      "closingBookStructures",
      "reports",
      "report",
      "statement",
      "publishLists",
      "publishList",
    ]:
      # Match `fieldName:` (no args) or `fieldName(` (args present).
      present = f"{field_name}:" in query_block or f"{field_name}(" in query_block
      assert present, f"Missing ledger query field: {field_name}"

  def test_all_investor_queries_are_exposed(self) -> None:
    sdl = str(schema)
    start = sdl.find("type Query {")
    assert start >= 0
    end = sdl.find("}", start)
    query_block = sdl[start:end]
    for field_name in [
      "portfolios",
      "portfolioBlock",
      "securities",
      "security",
      "positions",
      "position",
      "holdings",
    ]:
      present = f"{field_name}:" in query_block or f"{field_name}(" in query_block
      assert present, f"Missing investor query field: {field_name}"

  def test_query_root_has_no_graph_id_args(self) -> None:
    """No Query field should take `graphId` — that's a URL path param now."""
    sdl = str(schema)
    start = sdl.find("type Query {")
    assert start >= 0
    end = sdl.find("}", start)
    query_block = sdl[start:end]
    assert "graphId" not in query_block, (
      "graphId should not appear as a query argument — it's now a URL path "
      "parameter (`/extensions/{graph_id}/graphql`). See `graphql/context.py` "
      "and `graphql/resolvers/*.py`."
    )

  def test_account_tree_node_is_recursive(self) -> None:
    """`children: [AccountTreeNode!]!` must round-trip the self-reference."""
    sdl = str(schema)
    # Find the AccountTreeNode type block
    assert "type AccountTreeNode {" in sdl
    start = sdl.find("type AccountTreeNode {")
    end = sdl.find("}", start)
    block = sdl[start:end]
    # children should reference AccountTreeNode itself
    assert "children: [AccountTreeNode!]!" in block

  def test_security_terms_uses_json_scalar(self) -> None:
    """`Security.terms` is hand-written because `dict` needs the JSON scalar."""
    sdl = str(schema)
    start = sdl.find("type Security {")
    assert start >= 0
    end = sdl.find("}", start)
    block = sdl[start:end]
    assert "terms: JSON!" in block

  def test_hello_field_kept_for_auth_probe(self) -> None:
    sdl = str(schema)
    assert "hello: String!" in sdl

  def test_pagination_info_is_registered(self) -> None:
    """PaginationInfo is the shared page envelope — must appear in SDL."""
    sdl = str(schema)
    assert "type PaginationInfo {" in sdl
    start = sdl.find("type PaginationInfo {")
    end = sdl.find("}", start)
    block = sdl[start:end]
    assert "total: Int!" in block
    assert "limit: Int!" in block
    assert "offset: Int!" in block
    assert "hasMore: Boolean!" in block


class TestSchemaPerDomainFlagGating:
  """`_build_query_type` must skip disabled domains entirely.

  Before: `Query(LedgerQuery, InvestorQuery)` was unconditional, so a
  ledger-only deployment still advertised investor fields and failed
  them at runtime with `INVESTOR_NOT_INITIALIZED`. Now the schema
  honestly reflects which domains are enabled, so SDK introspection
  matches what works at runtime.
  """

  def _build_sdl_with_flags(self, *, ledger: bool, investor: bool) -> str:
    """Rebuild the Query type with the given flag combo and return SDL."""
    import strawberry

    from robosystems.graphql.schema import _build_query_type

    with (
      patch("robosystems.graphql.schema.env.ROBOLEDGER_ENABLED", ledger),
      patch("robosystems.graphql.schema.env.ROBOINVESTOR_ENABLED", investor),
    ):
      query_type = _build_query_type()
      built = strawberry.Schema(query=query_type)
      return str(built)

  def test_ledger_only_deployment_excludes_investor_fields(self) -> None:
    sdl = self._build_sdl_with_flags(ledger=True, investor=False)
    start = sdl.find("type Query {")
    end = sdl.find("}", start)
    query_block = sdl[start:end]

    # Ledger fields present
    assert "entity:" in query_block or "entity(" in query_block
    assert "trialBalance" in query_block
    # Investor fields absent
    assert "portfolios" not in query_block
    assert "holdings" not in query_block
    # Probe always present
    assert "hello" in query_block

  def test_investor_only_deployment_excludes_ledger_fields(self) -> None:
    sdl = self._build_sdl_with_flags(ledger=False, investor=True)
    start = sdl.find("type Query {")
    end = sdl.find("}", start)
    query_block = sdl[start:end]

    # Investor fields present
    assert "portfolios" in query_block
    assert "holdings" in query_block
    # Ledger fields absent
    assert "trialBalance" not in query_block
    assert "fiscalCalendar" not in query_block
    # Probe always present
    assert "hello" in query_block

  def test_both_disabled_only_exposes_hello_probe(self) -> None:
    sdl = self._build_sdl_with_flags(ledger=False, investor=False)
    start = sdl.find("type Query {")
    end = sdl.find("}", start)
    query_block = sdl[start:end]

    assert "hello" in query_block
    assert "trialBalance" not in query_block
    assert "portfolios" not in query_block
