"""Unit tests for ``assert_accounts_postable`` — the inactive-account guard
behind every authored posting (journal entries, event handlers)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from sqlalchemy.dialects import postgresql

from robosystems.operations.roboledger.commands._guards import (
  InactiveAccountError,
  UnknownAccountError,
  assert_accounts_postable,
)


def _session_with_inactive(rows, *, missing=()):
  """A session whose query returns the retired accounts in ``rows``, and
  whose lookups find every account but those in ``missing``."""
  session = MagicMock()
  session.execute.return_value.all.return_value = rows
  session.get.side_effect = lambda _model, eid: None if eid in missing else object()
  return session


@pytest.mark.unit
class TestAssertAccountsPostable:
  def test_passes_when_no_line_names_a_retired_account(self) -> None:
    session = _session_with_inactive([])
    assert_accounts_postable(session, ["elem_a", "elem_b"])
    statement = session.execute.call_args.args[0]
    sql = str(
      statement.compile(
        dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}
      )
    )
    assert "elements.is_active IS false" in sql
    assert "'elem_a'" in sql and "'elem_b'" in sql

  def test_refuses_and_names_an_account_that_does_not_exist(self) -> None:
    """A choice that resolves no account is a refusal, not a foreign-key
    error from the database; checked before the retired-account query."""
    session = _session_with_inactive([], missing={"elem_b", "elem_c"})
    with pytest.raises(UnknownAccountError) as exc:
      assert_accounts_postable(session, ["elem_a", "elem_b", "elem_c"])
    assert exc.value.element_ids == ["elem_b", "elem_c"]
    assert "elem_b, elem_c" in str(exc.value)
    assert issubclass(UnknownAccountError, ValueError)
    session.execute.assert_not_called()

  def test_refuses_and_names_every_retired_account(self) -> None:
    session = _session_with_inactive(
      [("elem_a", "1000", "Operating Checking"), ("elem_c", "6100", None)]
    )
    with pytest.raises(InactiveAccountError) as exc:
      assert_accounts_postable(session, ["elem_a", "elem_b", "elem_c"])
    message = str(exc.value)
    assert "1000 Operating Checking (elem_a)" in message
    assert "6100 (elem_c)" in message
    assert "is_active=true" in message
    assert exc.value.accounts == [
      ("elem_a", "1000", "Operating Checking"),
      ("elem_c", "6100", None),
    ]

  def test_is_a_value_error_so_the_envelope_maps_it_to_422(self) -> None:
    assert issubclass(InactiveAccountError, ValueError)

  def test_a_synced_ledgers_own_history_replays_untouched(self) -> None:
    """QuickBooks retires accounts after they carry activity and a full
    rebuild replays that history; the guard must not block it."""
    session = _session_with_inactive([("elem_a", "1000", "Old Checking")])
    assert_accounts_postable(session, ["elem_a"], source="quickbooks")
    assert_accounts_postable(session, ["elem_a"], source="QuickBooks")
    session.execute.assert_not_called()

  def test_other_sources_are_authored_and_checked(self) -> None:
    session = _session_with_inactive([("elem_a", "1000", "Old Checking")])
    with pytest.raises(InactiveAccountError):
      assert_accounts_postable(session, ["elem_a"], source="mercury")
    with pytest.raises(InactiveAccountError):
      assert_accounts_postable(session, ["elem_a"], source="native")

  def test_empty_or_blank_ids_skip_the_query(self) -> None:
    session = _session_with_inactive([])
    assert_accounts_postable(session, [])
    assert_accounts_postable(session, ["", None])  # type: ignore[list-item]
    session.execute.assert_not_called()

  def test_duplicate_ids_are_queried_once(self) -> None:
    session = _session_with_inactive([])
    assert_accounts_postable(session, ["elem_a", "elem_a", "elem_b"])
    statement = session.execute.call_args.args[0]
    sql = str(
      statement.compile(
        dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}
      )
    )
    assert sql.count("'elem_a'") == 1
