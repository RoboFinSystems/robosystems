"""Unit tests for ledger transaction endpoints."""

from datetime import date
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from robosystems.routers.ledger.transactions import get_transaction, list_transactions

MODULE = "robosystems.routers.ledger.transactions"
GRAPH_ID = "kg01234567890abcdef"


def _make_user():
  user = MagicMock()
  user.id = "usr_test123"
  return user


def _make_txn(
  id="txn_1",
  number="1001",
  type="invoice",
  category=None,
  amount=10000,
  currency="USD",
  txn_date=None,
  due_date=None,
  merchant_name="Acme Corp",
  reference_number=None,
  description="Test invoice",
  source="quickbooks",
  source_id="qb_123",
  status="posted",
  posted_at=None,
):
  txn = MagicMock()
  txn.id = id
  txn.number = number
  txn.type = type
  txn.category = category
  txn.amount = amount
  txn.currency = currency
  txn.date = txn_date or date(2025, 1, 15)
  txn.due_date = due_date
  txn.merchant_name = merchant_name
  txn.reference_number = reference_number
  txn.description = description
  txn.source = source
  txn.source_id = source_id
  txn.status = status
  txn.posted_at = posted_at
  return txn


def _make_entry(
  id="je_1",
  number=None,
  type="standard",
  posting_date=None,
  memo=None,
  status="posted",
  posted_at=None,
):
  entry = MagicMock()
  entry.id = id
  entry.number = number
  entry.type = type
  entry.posting_date = posting_date or date(2025, 1, 15)
  entry.memo = memo
  entry.status = status
  entry.posted_at = posted_at
  return entry


def _make_line_item_row(
  li_id="li_1",
  account_id="acct_1",
  debit=5000,
  credit=0,
  description=None,
  line_order=0,
  acct_name="Cash",
  acct_code="1000",
):
  li = MagicMock()
  li.id = li_id
  li.account_id = account_id
  li.debit_amount = debit
  li.credit_amount = credit
  li.description = description
  li.line_order = line_order
  return (li, acct_name, acct_code)


class TestListTransactions:
  @pytest.mark.asyncio
  async def test_returns_transactions(self):
    mock_session = MagicMock()
    mock_session.execute.return_value.scalar.return_value = 2
    mock_session.execute.return_value.scalars.return_value.all.return_value = [
      _make_txn(id="txn_1", amount=10000),
      _make_txn(id="txn_2", amount=20000),
    ]

    with patch(f"{MODULE}.oltp_session") as mock_oltp:
      mock_oltp.return_value.__enter__ = MagicMock(return_value=mock_session)
      mock_oltp.return_value.__exit__ = MagicMock(return_value=False)

      result = await list_transactions(
        graph_id=GRAPH_ID,
        limit=100,
        offset=0,
        current_user=_make_user(),
        _rate_limit=None,
      )

    assert len(result.transactions) == 2
    assert result.transactions[0].amount == 100.0
    assert result.transactions[1].amount == 200.0

  @pytest.mark.asyncio
  async def test_cents_to_dollars_conversion(self):
    mock_session = MagicMock()
    mock_session.execute.return_value.scalar.return_value = 1
    mock_session.execute.return_value.scalars.return_value.all.return_value = [
      _make_txn(amount=12345),
    ]

    with patch(f"{MODULE}.oltp_session") as mock_oltp:
      mock_oltp.return_value.__enter__ = MagicMock(return_value=mock_session)
      mock_oltp.return_value.__exit__ = MagicMock(return_value=False)

      result = await list_transactions(
        graph_id=GRAPH_ID,
        limit=100,
        offset=0,
        current_user=_make_user(),
        _rate_limit=None,
      )

    assert result.transactions[0].amount == 123.45

  @pytest.mark.asyncio
  async def test_date_filters(self):
    mock_session = MagicMock()
    mock_session.execute.return_value.scalar.return_value = 1
    mock_session.execute.return_value.scalars.return_value.all.return_value = [
      _make_txn(),
    ]

    with patch(f"{MODULE}.oltp_session") as mock_oltp:
      mock_oltp.return_value.__enter__ = MagicMock(return_value=mock_session)
      mock_oltp.return_value.__exit__ = MagicMock(return_value=False)

      result = await list_transactions(
        graph_id=GRAPH_ID,
        start_date=date(2025, 1, 1),
        end_date=date(2025, 12, 31),
        limit=100,
        offset=0,
        current_user=_make_user(),
        _rate_limit=None,
      )

    assert len(result.transactions) == 1

  @pytest.mark.asyncio
  async def test_schema_not_found_returns_404(self):
    with patch(f"{MODULE}.oltp_session") as mock_oltp:
      mock_oltp.side_effect = ValueError("Invalid graph_id")

      with pytest.raises(HTTPException) as exc_info:
        await list_transactions(
          graph_id=GRAPH_ID,
          limit=100,
          offset=0,
          current_user=_make_user(),
          _rate_limit=None,
        )

      assert exc_info.value.status_code == 404


class TestGetTransaction:
  @pytest.mark.asyncio
  async def test_returns_detail_with_entries(self):
    txn = _make_txn()
    entry = _make_entry()
    li_rows = [
      _make_line_item_row(
        li_id="li_1", debit=5000, credit=0, acct_name="Cash", acct_code="1000"
      ),
      _make_line_item_row(
        li_id="li_2", debit=0, credit=5000, acct_name="Revenue", acct_code="4000"
      ),
    ]

    mock_session = MagicMock()
    # First call: get transaction
    # Second call: get entries
    # Third call: get line items for entry
    mock_session.execute.side_effect = [
      MagicMock(scalar_one_or_none=MagicMock(return_value=txn)),
      MagicMock(
        scalars=MagicMock(return_value=MagicMock(all=MagicMock(return_value=[entry])))
      ),
      MagicMock(all=MagicMock(return_value=li_rows)),
    ]

    with patch(f"{MODULE}.oltp_session") as mock_oltp:
      mock_oltp.return_value.__enter__ = MagicMock(return_value=mock_session)
      mock_oltp.return_value.__exit__ = MagicMock(return_value=False)

      result = await get_transaction(
        graph_id=GRAPH_ID,
        transaction_id="txn_1",
        current_user=_make_user(),
        _rate_limit=None,
      )

    assert result.id == "txn_1"
    assert result.amount == 100.0
    assert len(result.entries) == 1
    assert len(result.entries[0].line_items) == 2
    assert result.entries[0].line_items[0].debit_amount == 50.0
    assert result.entries[0].line_items[0].account_name == "Cash"

  @pytest.mark.asyncio
  async def test_not_found_returns_404(self):
    mock_session = MagicMock()
    mock_session.execute.return_value.scalar_one_or_none.return_value = None

    with patch(f"{MODULE}.oltp_session") as mock_oltp:
      mock_oltp.return_value.__enter__ = MagicMock(return_value=mock_session)
      mock_oltp.return_value.__exit__ = MagicMock(return_value=False)

      with pytest.raises(HTTPException) as exc_info:
        await get_transaction(
          graph_id=GRAPH_ID,
          transaction_id="txn_missing",
          current_user=_make_user(),
          _rate_limit=None,
        )

      assert exc_info.value.status_code == 404
