"""Taxonomy scoping of the CoA-facing reads.

``Element.source`` alone cannot discriminate a Chart-of-Accounts
account: envelope-authored framework concepts (``reporting_extension``
/ ``custom_ontology`` taxonomy blocks) share ``source='native'`` with
natively-authored CoA accounts, so a source-only filter leaks tenant
framework concepts (disclosure members, policy text blocks) into every
CoA surface — the accounts page, the mapping-coverage denominator, and
the unmapped-elements feed that drives auto-mapping.

``coa_element_clause`` adds the taxonomy discriminator: an element is
CoA if its source matches AND it either has no taxonomy (adapter
imports set ``taxonomy_id=NULL``) or belongs to a
``taxonomy_type='chart_of_accounts'`` taxonomy.

These tests verify the discriminator is present in the compiled SQL of
each CoA-scoped read. End-to-end correctness is exercised by the
integration tests.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from sqlalchemy.dialects import postgresql

from robosystems.operations.roboledger.reads import accounts as reads_accounts
from robosystems.operations.roboledger.reads import taxonomies as reads_taxonomies


def _compiled_sql(call_args) -> str:
  stmt = call_args[0][0]
  return str(stmt.compile(dialect=postgresql.dialect()))


def _assert_taxonomy_guard(sql: str) -> None:
  """The guard: taxonomy_id IS NULL OR taxonomy_id IN (CoA taxonomies)."""
  assert "elements.taxonomy_id IS NULL" in sql, (
    f"Expected NULL-taxonomy arm (adapter-imported accounts) in SQL, got:\n{sql}"
  )
  assert "taxonomies.taxonomy_type" in sql, (
    f"Expected taxonomy_type subquery arm in SQL, got:\n{sql}"
  )


def _session_with_no_rows() -> MagicMock:
  session = MagicMock()
  session.execute.return_value.scalars.return_value.all.return_value = []
  return session


class TestCoaElementClause:
  def test_clause_carries_source_and_taxonomy_discriminator(self):
    clause = reads_accounts.coa_element_clause()
    sql = str(clause.compile(dialect=postgresql.dialect()))
    assert "elements.source IN" in sql
    _assert_taxonomy_guard(sql)


class TestAccountReadsScoped:
  def test_get_account_tree_excludes_framework_concepts(self):
    session = _session_with_no_rows()
    reads_accounts.get_account_tree(session)
    _assert_taxonomy_guard(_compiled_sql(session.execute.call_args))

  def test_list_accounts_excludes_framework_concepts(self):
    session = _session_with_no_rows()
    session.execute.return_value.scalar.return_value = 0
    reads_accounts.list_accounts(session)
    # Both the count query and the row query must carry the guard.
    for call in session.execute.call_args_list:
      _assert_taxonomy_guard(_compiled_sql(call))


class TestTaxonomyReadsScoped:
  def test_count_coa_elements_excludes_framework_concepts(self):
    session = MagicMock()
    session.execute.return_value.scalar.return_value = 0
    reads_taxonomies.count_coa_elements(session)
    _assert_taxonomy_guard(_compiled_sql(session.execute.call_args))

  def test_list_unmapped_elements_excludes_framework_concepts(self):
    session = _session_with_no_rows()
    reads_taxonomies.list_unmapped_elements(session)
    # First execute is the CoA-element query; the second loads mapped ids.
    _assert_taxonomy_guard(_compiled_sql(session.execute.call_args_list[0]))


class TestRawSqlCountsScoped:
  def test_account_rollups_unmapped_sql_has_guard(self):
    from robosystems.operations.roboledger.reads import account_rollups

    sql = account_rollups._UNMAPPED_SQL.text
    assert "taxonomy_id IS NULL" in sql
    assert "taxonomy_type = 'chart_of_accounts'" in sql

  def test_fact_grid_count_unmapped_has_guard(self):
    import inspect

    from robosystems.operations.roboledger.reports import fact_grid

    src = inspect.getsource(fact_grid._count_unmapped)
    assert "taxonomy_id IS NULL" in src
    assert "taxonomy_type = 'chart_of_accounts'" in src
