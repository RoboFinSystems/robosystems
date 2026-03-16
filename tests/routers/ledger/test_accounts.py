"""Unit tests for ledger account endpoints."""

from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from robosystems.routers.ledger.accounts import get_account_tree, list_accounts

MODULE = "robosystems.routers.ledger.accounts"
GRAPH_ID = "kg01234567890abcdef"


def _make_user():
  user = MagicMock()
  user.id = "usr_test123"
  return user


def _make_account(
  id="acct_1",
  code="1000",
  name="Cash",
  classification="asset",
  sub_classification=None,
  balance_type="debit",
  parent_id=None,
  depth=0,
  currency="USD",
  is_active=True,
  is_placeholder=False,
  external_id=None,
  external_source=None,
  description=None,
):
  acct = MagicMock()
  acct.id = id
  acct.code = code
  acct.name = name
  acct.description = description
  acct.classification = classification
  acct.sub_classification = sub_classification
  acct.balance_type = balance_type
  acct.parent_id = parent_id
  acct.depth = depth
  acct.currency = currency
  acct.is_active = is_active
  acct.is_placeholder = is_placeholder
  acct.external_id = external_id
  acct.external_source = external_source
  return acct


class TestListAccounts:
  @pytest.mark.asyncio
  async def test_returns_accounts(self):
    mock_session = MagicMock()
    mock_session.execute.return_value.scalar.return_value = 2
    mock_session.execute.return_value.scalars.return_value.all.return_value = [
      _make_account(id="acct_1", code="1000", name="Cash"),
      _make_account(id="acct_2", code="2000", name="AP", classification="liability"),
    ]

    with patch(f"{MODULE}.oltp_session") as mock_oltp:
      mock_oltp.return_value.__enter__ = MagicMock(return_value=mock_session)
      mock_oltp.return_value.__exit__ = MagicMock(return_value=False)

      result = await list_accounts(
        graph_id=GRAPH_ID,
        limit=100,
        offset=0,
        current_user=_make_user(),
        _rate_limit=None,
      )

    assert len(result.accounts) == 2
    assert result.accounts[0].code == "1000"
    assert result.pagination.total == 2
    assert result.pagination.has_more is False

  @pytest.mark.asyncio
  async def test_classification_filter(self):
    mock_session = MagicMock()
    mock_session.execute.return_value.scalar.return_value = 1
    mock_session.execute.return_value.scalars.return_value.all.return_value = [
      _make_account(classification="asset"),
    ]

    with patch(f"{MODULE}.oltp_session") as mock_oltp:
      mock_oltp.return_value.__enter__ = MagicMock(return_value=mock_session)
      mock_oltp.return_value.__exit__ = MagicMock(return_value=False)

      result = await list_accounts(
        graph_id=GRAPH_ID,
        classification="asset",
        limit=100,
        offset=0,
        current_user=_make_user(),
        _rate_limit=None,
      )

    assert len(result.accounts) == 1
    assert result.accounts[0].classification == "asset"

  @pytest.mark.asyncio
  async def test_empty_ledger(self):
    mock_session = MagicMock()
    mock_session.execute.return_value.scalar.return_value = 0
    mock_session.execute.return_value.scalars.return_value.all.return_value = []

    with patch(f"{MODULE}.oltp_session") as mock_oltp:
      mock_oltp.return_value.__enter__ = MagicMock(return_value=mock_session)
      mock_oltp.return_value.__exit__ = MagicMock(return_value=False)

      result = await list_accounts(
        graph_id=GRAPH_ID,
        limit=100,
        offset=0,
        current_user=_make_user(),
        _rate_limit=None,
      )

    assert len(result.accounts) == 0
    assert result.pagination.total == 0

  @pytest.mark.asyncio
  async def test_schema_not_found_returns_404(self):
    with patch(f"{MODULE}.oltp_session") as mock_oltp:
      mock_oltp.side_effect = ValueError("Invalid graph_id")

      with pytest.raises(HTTPException) as exc_info:
        await list_accounts(
          graph_id=GRAPH_ID,
          limit=100,
          offset=0,
          current_user=_make_user(),
          _rate_limit=None,
        )

      assert exc_info.value.status_code == 404

  @pytest.mark.asyncio
  async def test_pagination(self):
    mock_session = MagicMock()
    mock_session.execute.return_value.scalar.return_value = 50
    mock_session.execute.return_value.scalars.return_value.all.return_value = [
      _make_account(id=f"acct_{i}", code=str(1000 + i)) for i in range(10)
    ]

    with patch(f"{MODULE}.oltp_session") as mock_oltp:
      mock_oltp.return_value.__enter__ = MagicMock(return_value=mock_session)
      mock_oltp.return_value.__exit__ = MagicMock(return_value=False)

      result = await list_accounts(
        graph_id=GRAPH_ID,
        limit=10,
        offset=0,
        current_user=_make_user(),
        _rate_limit=None,
      )

    assert result.pagination.total == 50
    assert result.pagination.limit == 10
    assert result.pagination.has_more is True


class TestAccountTree:
  @pytest.mark.asyncio
  async def test_builds_tree(self):
    accounts = [
      _make_account(id="acct_1", code="1000", name="Assets", parent_id=None, depth=0),
      _make_account(id="acct_2", code="1100", name="Cash", parent_id="acct_1", depth=1),
      _make_account(id="acct_3", code="1200", name="AR", parent_id="acct_1", depth=1),
    ]

    mock_session = MagicMock()
    mock_session.execute.return_value.scalars.return_value.all.return_value = accounts

    with patch(f"{MODULE}.oltp_session") as mock_oltp:
      mock_oltp.return_value.__enter__ = MagicMock(return_value=mock_session)
      mock_oltp.return_value.__exit__ = MagicMock(return_value=False)

      result = await get_account_tree(
        graph_id=GRAPH_ID,
        current_user=_make_user(),
        _rate_limit=None,
      )

    assert len(result.roots) == 1
    assert result.roots[0].name == "Assets"
    assert len(result.roots[0].children) == 2
    assert result.total_accounts == 3

  @pytest.mark.asyncio
  async def test_empty_tree(self):
    mock_session = MagicMock()
    mock_session.execute.return_value.scalars.return_value.all.return_value = []

    with patch(f"{MODULE}.oltp_session") as mock_oltp:
      mock_oltp.return_value.__enter__ = MagicMock(return_value=mock_session)
      mock_oltp.return_value.__exit__ = MagicMock(return_value=False)

      result = await get_account_tree(
        graph_id=GRAPH_ID,
        current_user=_make_user(),
        _rate_limit=None,
      )

    assert len(result.roots) == 0
    assert result.total_accounts == 0

  @pytest.mark.asyncio
  async def test_multiple_roots(self):
    accounts = [
      _make_account(id="acct_1", code="1000", name="Assets", parent_id=None),
      _make_account(id="acct_2", code="2000", name="Liabilities", parent_id=None),
    ]

    mock_session = MagicMock()
    mock_session.execute.return_value.scalars.return_value.all.return_value = accounts

    with patch(f"{MODULE}.oltp_session") as mock_oltp:
      mock_oltp.return_value.__enter__ = MagicMock(return_value=mock_session)
      mock_oltp.return_value.__exit__ = MagicMock(return_value=False)

      result = await get_account_tree(
        graph_id=GRAPH_ID,
        current_user=_make_user(),
        _rate_limit=None,
      )

    assert len(result.roots) == 2
