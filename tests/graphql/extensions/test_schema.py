"""Schema composition + SDL shape tests.

Verifies the Query root is composed from the ledger + investor resolver
classes and every expected field is reachable from the schema. These
catch wiring regressions early (a missing resolver registration, a
stray `dict`-typed field, a recursive type that doesn't resolve).
"""

from __future__ import annotations

from robosystems.graphql.schema import schema


class TestSchemaComposition:
  def test_schema_compiles(self) -> None:
    sdl = str(schema)
    assert "type Query" in sdl
    assert len(sdl) > 1000  # guard against an empty schema regression

  def test_all_ledger_queries_are_exposed(self) -> None:
    sdl = str(schema)
    # Sample the core ledger queries — if any are missing, the Query
    # inheritance from LedgerQuery has regressed.
    for field in [
      "entity(",
      "entities(",
      "summary(",
      "accounts(",
      "accountTree(",
      "accountRollups(",
      "trialBalance(",
      "transactions(",
      "transaction(",
      "taxonomies(",
      "reportingTaxonomy(",
      "elements(",
      "unmappedElements(",
      "structures(",
      "mappings(",
      "mapping(",
      "mappingCoverage(",
      "schedules(",
      "scheduleFacts(",
      "periodCloseStatus(",
      "fiscalCalendar(",
      "periodDrafts(",
      "closingBookStructures(",
    ]:
      assert field in sdl, f"Missing ledger query: {field}"

  def test_all_investor_queries_are_exposed(self) -> None:
    sdl = str(schema)
    for field in [
      "portfolios(",
      "portfolio(",
      "securities(",
      "security(",
      "positions(",
      "position(",
      "holdings(",
    ]:
      assert field in sdl, f"Missing investor query: {field}"

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

  def test_schedule_summary_uses_json_scalars(self) -> None:
    sdl = str(schema)
    start = sdl.find("type ScheduleSummary {")
    assert start >= 0
    end = sdl.find("}", start)
    block = sdl[start:end]
    # both dict-typed fields map to optional JSON
    assert "entryTemplate: JSON" in block
    assert "scheduleMetadata: JSON" in block

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
