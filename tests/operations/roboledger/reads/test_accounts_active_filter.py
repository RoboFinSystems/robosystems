"""Active-filter behavior of ``get_account_tree``.

The QB adapter now pulls active + inactive accounts (to preserve FK
integrity for historical journal lines referencing deactivated
accounts). Without an active-only filter on the CoA-facing reads, the
chart-of-accounts UI surfaces dozens of "(deleted)" QB accounts that
clutter every list. ``get_account_tree`` defaults to ``include_inactive=False``
to keep the standard UX clean; pass ``True`` for admin / cleanup
contexts.

These tests verify (a) the SQL compiles against a real Postgres
dialect, (b) the ``is_active`` filter is present by default, and
(c) the filter disappears when ``include_inactive=True``. End-to-end
correctness is exercised by the integration tests.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from sqlalchemy.dialects import postgresql

from robosystems.operations.roboledger.reads import accounts as reads_accounts


def _compiled_sql(call_args) -> str:
  """Render the SQLAlchemy Select that ``session.execute`` was called
  with as a Postgres-dialect SQL string."""
  stmt = call_args[0][0]
  return str(stmt.compile(dialect=postgresql.dialect()))


class TestGetAccountTreeActiveFilter:
  def _session_with_no_rows(self) -> MagicMock:
    """Session that returns an empty rowset — we're inspecting the
    compiled SQL, not the result handling."""
    session = MagicMock()
    session.execute.return_value.scalars.return_value.all.return_value = []
    return session

  def test_default_excludes_inactive(self):
    """Default call (no include_inactive arg) must filter is_active=true."""
    session = self._session_with_no_rows()
    reads_accounts.get_account_tree(session)
    sql = _compiled_sql(session.execute.call_args)
    # Postgres dialect emits `elements.is_active IS true` for `is_(True)`.
    assert "elements.is_active IS true" in sql, (
      f"Expected `elements.is_active IS true` predicate in default-mode SQL, got:\n{sql}"
    )

  def test_include_inactive_drops_filter(self):
    """``include_inactive=True`` must not add the is_active predicate."""
    session = self._session_with_no_rows()
    reads_accounts.get_account_tree(session, include_inactive=True)
    sql = _compiled_sql(session.execute.call_args)
    # Check the WHERE clause specifically — `is_active` will still appear
    # in the SELECT column list since it's a column on the Element model.
    where_clause = sql.split("WHERE", 1)[1] if "WHERE" in sql else ""
    assert "is_active" not in where_clause, (
      f"Expected no is_active filter in WHERE when include_inactive=True, got WHERE:\n{where_clause}"
    )

  def test_keyword_only_argument(self):
    """``include_inactive`` is keyword-only — protects against
    positional-arg confusion if a future param is added before it."""
    session = self._session_with_no_rows()
    # Positional call must fail (no positional kwargs allowed past session).
    try:
      reads_accounts.get_account_tree(session, True)  # type: ignore[misc]
    except TypeError:
      pass
    else:  # pragma: no cover
      raise AssertionError(
        "Positional argument should have raised TypeError; "
        "include_inactive must be keyword-only."
      )

  def test_empty_result_no_roots(self):
    """Sanity: with no rows, response has empty roots + total=0."""
    session = self._session_with_no_rows()
    response = reads_accounts.get_account_tree(session)
    assert response.roots == []
    assert response.total_accounts == 0
