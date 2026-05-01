"""Tests for OLTPLoader — generic OLTP loading from DuckDB to PostgreSQL."""

from __future__ import annotations

from datetime import UTC, date, datetime
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture
def mock_duckdb_data():
  """Sample dbt output data as lists of dicts."""
  accounts = [
    {
      "external_id": "1",
      "external_source": "quickbooks",
      "code": "Cash",
      "name": "Cash",
      "description": "Cash and equivalents",
      "classification": "asset",
      "sub_classification": "Bank",
      "balance_type": "debit",
      "external_parent_id": None,
      "depth": 0,
      "path": "",
      "currency": "USD",
      "is_active": True,
      "is_placeholder": False,
      "metadata": "{}",
    },
    {
      "external_id": "2",
      "external_source": "quickbooks",
      "code": "Revenue",
      "name": "Revenue",
      "description": "Service revenue",
      "classification": "revenue",
      "sub_classification": "ServiceFeeIncome",
      "balance_type": "credit",
      "external_parent_id": None,
      "depth": 0,
      "path": "",
      "currency": "USD",
      "is_active": True,
      "is_placeholder": False,
      "metadata": "{}",
    },
  ]

  transactions = [
    {
      "external_id": "JournalEntry_100",
      "number": "JE-100",
      "type": "JournalEntry",
      "category": None,
      "amount": 50000,
      "currency": "USD",
      "date": date(2026, 1, 15),
      "due_date": None,
      "merchant_name": None,
      "reference_number": "JE-100",
      "description": "Service revenue",
      "source": "quickbooks",
      "source_id": "JournalEntry_100",
      "status": "posted",
      "metadata": "{}",
    },
  ]

  entries = [
    {
      "external_id": "JournalEntry_100",
      "external_transaction_id": "JournalEntry_100",
      "number": "JE-100",
      "type": "standard",
      "posting_date": date(2026, 1, 15),
      "memo": "Service revenue",
      "status": "posted",
      "metadata": "{}",
    },
  ]

  line_items = [
    {
      "entry_external_id": "JournalEntry_100",
      "element_external_id": "1",
      "debit_amount": 50000,
      "credit_amount": 0,
      "description": "Cash received",
      "line_order": 1,
      "metadata": "{}",
    },
    {
      "entry_external_id": "JournalEntry_100",
      "element_external_id": "2",
      "debit_amount": 0,
      "credit_amount": 50000,
      "description": "Revenue earned",
      "line_order": 2,
      "metadata": "{}",
    },
  ]

  dimensions = [
    {
      "external_id": "1",
      "dimension_type": "department",
      "name": "Engineering",
      "value": "1",
    },
  ]

  return {
    "elements": accounts,
    "transactions": transactions,
    "entries": entries,
    "line_items": line_items,
    "dimensions": dimensions,
  }


def _make_duckdb_mock(tables: dict[str, list[dict]]):
  """Create a DuckDB connection mock that returns the given tables."""
  mock_con = MagicMock()
  table_names = list(tables.keys())

  def execute_side_effect(query):
    result = MagicMock()
    if query == "SHOW TABLES":
      result.fetchall.return_value = [(t,) for t in table_names]
    else:
      for t in table_names:
        if f"SELECT * FROM {t}" in query:
          rows = tables[t]
          if rows:
            columns = list(rows[0].keys())
            result.description = [(c,) for c in columns]
            result.fetchall.return_value = [tuple(r.values()) for r in rows]
          else:
            result.description = []
            result.fetchall.return_value = []
          break
      else:
        result.description = []
        result.fetchall.return_value = []
    return result

  mock_con.execute.side_effect = execute_side_effect
  return mock_con


class TestOLTPLoader:
  """Test the OLTPLoader class."""

  @patch("robosystems.db.extensions.provision_tenant_schema")
  @patch("robosystems.db.extensions.extensions_session")
  @patch("duckdb.connect")
  def test_load_calls_provision_tenant_schema(
    self,
    mock_duckdb_connect,
    mock_ext_session,
    mock_provision,
    mock_duckdb_data,
  ):
    """Loader provisions the tenant schema before inserting."""
    from robosystems.operations.extensions.loader import OLTPLoader

    mock_duckdb_connect.return_value = _make_duckdb_mock({})

    mock_session = MagicMock()
    mock_ext_session.return_value.__enter__ = MagicMock(return_value=mock_session)
    mock_ext_session.return_value.__exit__ = MagicMock(return_value=False)

    loader = OLTPLoader()
    loader.load(
      graph_id="kg0123456789abcdef",
      source="quickbooks",
      connection_id="conn_123",
      duckdb_path="/tmp/test.duckdb",
      created_by="user_123",
    )

    mock_provision.assert_called_once_with("kg0123456789abcdef")

  @patch("robosystems.db.extensions.provision_tenant_schema")
  @patch("robosystems.db.extensions.extensions_session")
  @patch("duckdb.connect")
  def test_load_reads_all_tables(
    self,
    mock_duckdb_connect,
    mock_ext_session,
    mock_provision,
    mock_duckdb_data,
  ):
    """Loader reads all OLTP tables from DuckDB and reports counts."""
    from robosystems.operations.extensions.loader import OLTPLoader

    mock_duckdb_connect.return_value = _make_duckdb_mock(mock_duckdb_data)

    mock_session = MagicMock()
    mock_ext_session.return_value.__enter__ = MagicMock(return_value=mock_session)
    mock_ext_session.return_value.__exit__ = MagicMock(return_value=False)

    loader = OLTPLoader()
    result = loader.load(
      graph_id="kg0123456789abcdef",
      source="quickbooks",
      connection_id="conn_123",
      duckdb_path="/tmp/test.duckdb",
      created_by="user_123",
    )

    # Phase 2: transactional dbt rows are captured as a single event_block
    # per QB transaction (apply_handlers=False). GL rows (Transaction /
    # Entry / LineItem) get created later by the handler at approval time;
    # the sync itself produces 0 of them.
    assert result.elements == 2
    assert result.transactions == 0
    assert result.entries == 0
    assert result.line_items == 0
    assert result.dimensions == 1
    assert result.events_captured == 1
    assert result.events_updated == 0
    # Elements + dimensions + captured event = 4
    assert result.total_rows == 4

  @patch("robosystems.db.extensions.provision_tenant_schema")
  @patch("robosystems.db.extensions.extensions_session")
  @patch("duckdb.connect")
  def test_load_deletes_before_insert(
    self,
    mock_duckdb_connect,
    mock_ext_session,
    mock_provision,
  ):
    """Loader deletes existing data for source + connection_id."""
    from robosystems.operations.extensions.loader import OLTPLoader

    mock_duckdb_connect.return_value = _make_duckdb_mock({})

    mock_session = MagicMock()
    mock_ext_session.return_value.__enter__ = MagicMock(return_value=mock_session)
    mock_ext_session.return_value.__exit__ = MagicMock(return_value=False)

    loader = OLTPLoader()
    loader.load(
      graph_id="kg0123456789abcdef",
      source="quickbooks",
      connection_id="conn_123",
      duckdb_path="/tmp/test.duckdb",
      created_by="user_123",
    )

    assert mock_session.query.called
    assert mock_session.flush.called

  @patch("robosystems.db.extensions.provision_tenant_schema")
  @patch("robosystems.db.extensions.extensions_session")
  @patch("duckdb.connect")
  def test_load_handles_missing_tables(
    self,
    mock_duckdb_connect,
    mock_ext_session,
    mock_provision,
  ):
    """Loader gracefully handles missing tables in DuckDB."""
    from robosystems.operations.extensions.loader import OLTPLoader

    only_accounts = [
      {
        "external_id": "1",
        "external_source": "quickbooks",
        "code": "Cash",
        "name": "Cash",
        "description": None,
        "classification": "asset",
        "sub_classification": None,
        "balance_type": "debit",
        "external_parent_id": None,
        "depth": 0,
        "path": "",
        "currency": "USD",
        "is_active": True,
        "is_placeholder": False,
        "metadata": "{}",
      }
    ]

    mock_duckdb_connect.return_value = _make_duckdb_mock({"elements": only_accounts})

    mock_session = MagicMock()
    mock_ext_session.return_value.__enter__ = MagicMock(return_value=mock_session)
    mock_ext_session.return_value.__exit__ = MagicMock(return_value=False)

    loader = OLTPLoader()
    result = loader.load(
      graph_id="kg0123456789abcdef",
      source="quickbooks",
      connection_id="conn_123",
      duckdb_path="/tmp/test.duckdb",
      created_by="user_123",
    )

    assert result.elements == 1
    assert result.transactions == 0
    assert result.entries == 0
    assert result.line_items == 0

  @patch("robosystems.db.extensions.provision_tenant_schema")
  @patch("robosystems.db.extensions.extensions_session")
  @patch("duckdb.connect")
  def test_load_silently_drops_orphan_line_items(
    self,
    mock_duckdb_connect,
    mock_ext_session,
    mock_provision,
  ):
    """Phase 2: line_items without a parent entry/transaction are
    silently dropped during the per-transaction grouping. The canonical
    record is the event_block; orphan lines have no transaction context
    to attach to and aren't actionable, so they don't generate errors —
    the upstream dbt staging is responsible for ensuring entry/txn rows
    exist before line_items reference them."""
    from robosystems.operations.extensions.loader import OLTPLoader

    orphan_lines = [
      {
        "entry_external_id": "nonexistent_entry",
        "element_external_id": "nonexistent_account",
        "debit_amount": 10000,
        "credit_amount": 0,
        "description": "Test",
        "line_order": 1,
        "metadata": "{}",
      }
    ]

    mock_duckdb_connect.return_value = _make_duckdb_mock({"line_items": orphan_lines})

    mock_session = MagicMock()
    mock_session.query.return_value.filter.return_value.all.return_value = []
    mock_ext_session.return_value.__enter__ = MagicMock(return_value=mock_session)
    mock_ext_session.return_value.__exit__ = MagicMock(return_value=False)

    loader = OLTPLoader()
    result = loader.load(
      graph_id="kg0123456789abcdef",
      source="quickbooks",
      connection_id="conn_123",
      duckdb_path="/tmp/test.duckdb",
      created_by="user_123",
    )

    # Orphan lines silently dropped; no events captured, no errors.
    assert result.events_captured == 0
    assert result.events_updated == 0
    assert result.line_items == 0
    assert len(result.errors) == 0

  def test_load_result_total_rows(self):
    """LoadResult.total_rows sums all table counts plus event counts."""
    from robosystems.operations.extensions.loader import LoadResult

    result = LoadResult(
      graph_id="kg123",
      source="quickbooks",
      connection_id="conn_1",
      elements=10,
      transactions=5,
      entries=5,
      line_items=20,
      dimensions=3,
      events_captured=2,
      events_updated=1,
    )
    # 10 + 5 + 5 + 20 + 3 + 2 + 1 = 46
    assert result.total_rows == 46


class TestCaptureTransactionsAsEvents:
  """Phase 2 capture path — UPSERT semantics + post-approval safety."""

  def _dbt_data(self) -> dict:
    """Single QB JournalEntry with two line items (one debit, one credit)."""
    return {
      "transactions": [
        {
          "external_id": "JE_100",
          "external_source": "quickbooks",
          "number": "100",
          "type": "JournalEntry",
          "category": None,
          "amount": 5000,
          "currency": "USD",
          "date": date(2026, 1, 15),
          "due_date": None,
          "merchant_name": "Acme Corp",
          "reference_number": "REF-100",
          "description": "Office supplies",
          "source_id": "JE_100",
          "status": "posted",
        }
      ],
      "entries": [
        {
          "external_id": "JE_100",
          "external_transaction_id": "JE_100",
          "external_source": "quickbooks",
          "number": "100",
          "type": "standard",
          "posting_date": date(2026, 1, 15),
          "memo": "Office supplies",
          "status": "posted",
        }
      ],
      "line_items": [
        {
          "entry_external_id": "JE_100",
          "element_external_id": "elem_5000",
          "debit_amount": 5000,
          "credit_amount": 0,
          "description": "Office supplies expense",
          "line_order": 1,
        },
        {
          "entry_external_id": "JE_100",
          "element_external_id": "elem_1000",
          "debit_amount": 0,
          "credit_amount": 5000,
          "description": "Cash",
          "line_order": 2,
        },
      ],
    }

  def test_capture_inserts_one_event_per_transaction(self):
    """Each QB transaction → one Event row with status='captured'."""
    from robosystems.operations.extensions.loader import OLTPLoader

    session = MagicMock()
    session.query.return_value.filter.return_value.all.return_value = []

    loader = OLTPLoader()
    _capture_result = loader._capture_transactions_as_events(
      session,
      self._dbt_data(),
      source="quickbooks",
      connection_id="conn_1",
      created_by="user_1",
      now=datetime.now(UTC),
    )

    assert _capture_result.inserted == 1
    assert _capture_result.updated == 0
    # add_all called with one Event row
    add_all_calls = session.add_all.call_args_list
    assert len(add_all_calls) == 1
    events_added = add_all_calls[0][0][0]
    assert len(events_added) == 1
    evt = events_added[0]
    assert evt.event_type == "journal_entry_recorded"
    assert evt.status == "captured"
    assert evt.source == "quickbooks"
    assert evt.external_id == "JE_100"
    assert evt.amount == 5000
    # Entries + line_items packed in metadata
    assert evt.metadata_["connection_id"] == "conn_1"
    assert len(evt.metadata_["entries"]) == 1
    assert len(evt.metadata_["entries"][0]["line_items"]) == 2

  def test_capture_updates_existing_captured_event(self):
    """Re-sync of the same QB transaction UPSERTs in place — no
    duplicate row, no new ULID, status stays 'captured'."""
    from robosystems.operations.extensions.loader import OLTPLoader

    existing_event = MagicMock()
    existing_event.external_id = "JE_100"
    existing_event.status = "captured"
    existing_event.id = "evt_existing"

    session = MagicMock()
    session.query.return_value.filter.return_value.all.return_value = [existing_event]

    loader = OLTPLoader()
    _capture_result = loader._capture_transactions_as_events(
      session,
      self._dbt_data(),
      source="quickbooks",
      connection_id="conn_1",
      created_by="user_1",
      now=datetime.now(UTC),
    )

    assert _capture_result.inserted == 0
    assert _capture_result.updated == 1
    # The existing row was mutated, not replaced
    assert existing_event.amount == 5000
    assert existing_event.description == "Office supplies"
    # No new Event was added
    session.add_all.assert_not_called()

  def test_capture_leaves_committed_event_alone(self):
    """A handler-approved event (status='committed' / 'fulfilled') must
    not be overwritten by a re-sync — that would silently undo the
    user's approval and detach the resulting GL rows."""
    from robosystems.operations.extensions.loader import OLTPLoader

    committed_event = MagicMock()
    committed_event.external_id = "JE_100"
    committed_event.status = "fulfilled"  # handler ran already
    committed_event.id = "evt_committed"
    original_metadata = {"frozen": "by-approval"}
    committed_event.metadata_ = original_metadata

    session = MagicMock()
    session.query.return_value.filter.return_value.all.return_value = [committed_event]

    loader = OLTPLoader()
    _capture_result = loader._capture_transactions_as_events(
      session,
      self._dbt_data(),
      source="quickbooks",
      connection_id="conn_1",
      created_by="user_1",
      now=datetime.now(UTC),
    )

    # Neither inserted nor counted as updated — the row is frozen.
    assert _capture_result.inserted == 0
    assert _capture_result.updated == 0
    assert committed_event.metadata_ is original_metadata
    session.add_all.assert_not_called()

  def test_capture_drops_orphan_entries_and_line_items(self):
    """Entries without a matching transaction, and line_items without a
    matching entry, are silently dropped — they have no transaction
    context and aren't actionable."""
    from robosystems.operations.extensions.loader import OLTPLoader

    dbt = {
      "transactions": [
        {
          "external_id": "JE_100",
          "external_source": "quickbooks",
          "number": "100",
          "type": "JournalEntry",
          "amount": 5000,
          "currency": "USD",
          "date": date(2026, 1, 15),
          "source_id": "JE_100",
        }
      ],
      "entries": [
        # Orphan: parent transaction doesn't exist
        {
          "external_id": "ORPHAN_E",
          "external_transaction_id": "MISSING_TXN",
          "external_source": "quickbooks",
          "type": "standard",
          "posting_date": date(2026, 1, 15),
        },
      ],
      "line_items": [
        # Orphan: parent entry doesn't exist
        {
          "entry_external_id": "ORPHAN_E",
          "element_external_id": "elem_5000",
          "debit_amount": 5000,
          "credit_amount": 0,
          "line_order": 1,
        },
      ],
    }

    session = MagicMock()
    session.query.return_value.filter.return_value.all.return_value = []

    loader = OLTPLoader()
    _capture_result = loader._capture_transactions_as_events(
      session,
      dbt,
      source="quickbooks",
      connection_id="conn_1",
      created_by="user_1",
      now=datetime.now(UTC),
    )

    # Hardening: a transaction whose entries all drop out is itself
    # dropped — capturing it would just produce an unapprovable inbox row.
    assert _capture_result.inserted == 0
    assert _capture_result.updated == 0
    assert _capture_result.dropped_empty_transactions == 1
    session.add_all.assert_not_called()


class TestCaptureAutoCommit:
  """Source-of-truth auto-commit behavior — QB-sourced events fire the
  handler immediately and land ``status='committed'``; native sources
  stay ``captured`` for human approval."""

  def _dbt_data(self) -> dict:
    """Reuse TestCaptureTransactionsAsEvents._dbt_data shape inline."""
    return {
      "transactions": [
        {
          "external_id": "JE_100",
          "external_source": "quickbooks",
          "number": "100",
          "type": "JournalEntry",
          "amount": 5000,
          "currency": "USD",
          "date": date(2026, 1, 15),
          "source_id": "JE_100",
        }
      ],
      "entries": [
        {
          "external_id": "JE_100",
          "external_transaction_id": "JE_100",
          "external_source": "quickbooks",
          "type": "standard",
          "posting_date": date(2026, 1, 15),
          "memo": "Office supplies",
        }
      ],
      "line_items": [
        {
          "entry_external_id": "JE_100",
          "element_external_id": "elem_5000",
          "debit_amount": 5000,
          "credit_amount": 0,
          "line_order": 1,
        },
        {
          "entry_external_id": "JE_100",
          "element_external_id": "elem_1000",
          "debit_amount": 0,
          "credit_amount": 5000,
          "line_order": 2,
        },
      ],
    }

  def test_qb_source_auto_commits_on_successful_dispatch(self):
    """QB-sourced events whose handler dispatches cleanly land
    status='committed' and increment auto_committed."""
    from unittest.mock import patch

    from robosystems.operations.extensions.loader import OLTPLoader

    session = MagicMock()
    session.query.return_value.filter.return_value.all.return_value = []

    loader = OLTPLoader()

    with patch(
      "robosystems.operations.extensions.loader._fire_handler_on_commit"
    ) as fire:
      result = loader._capture_transactions_as_events(
        session,
        self._dbt_data(),
        source="quickbooks",
        connection_id="conn_1",
        created_by="user_1",
        now=datetime.now(UTC),
      )

    fire.assert_called_once()
    assert result.inserted == 1
    assert result.auto_committed == 1
    assert result.dispatch_failed == 0
    # The event added to the session has status flipped to 'committed'
    evt = session.add_all.call_args.args[0][0]
    assert evt.status == "committed"

  def test_qb_source_falls_back_to_captured_on_dispatch_failure(self):
    """If the handler raises (e.g., element_external_id unmapped), the
    event is left at status='captured' and dispatch_failed is incremented."""
    from unittest.mock import patch

    from robosystems.operations.extensions.loader import OLTPLoader

    session = MagicMock()
    session.query.return_value.filter.return_value.all.return_value = []

    loader = OLTPLoader()

    with patch(
      "robosystems.operations.extensions.loader._fire_handler_on_commit",
      side_effect=RuntimeError("element_external_id not resolved"),
    ):
      result = loader._capture_transactions_as_events(
        session,
        self._dbt_data(),
        source="quickbooks",
        connection_id="conn_1",
        created_by="user_1",
        now=datetime.now(UTC),
      )

    assert result.inserted == 1
    assert result.auto_committed == 0
    assert result.dispatch_failed == 1
    evt = session.add_all.call_args.args[0][0]
    assert evt.status == "captured"

  def test_native_source_does_not_auto_commit(self):
    """Manual / schedule / system / AI sources go through the inbox —
    handler dispatch is NOT fired on capture, regardless of mapping."""
    from unittest.mock import patch

    from robosystems.operations.extensions.loader import OLTPLoader

    session = MagicMock()
    session.query.return_value.filter.return_value.all.return_value = []

    loader = OLTPLoader()
    dbt = self._dbt_data()
    # Switch the source on the data + the call.
    dbt["transactions"][0]["external_source"] = "manual"
    dbt["entries"][0]["external_source"] = "manual"

    with patch(
      "robosystems.operations.extensions.loader._fire_handler_on_commit"
    ) as fire:
      result = loader._capture_transactions_as_events(
        session,
        dbt,
        source="manual",
        connection_id="conn_1",
        created_by="user_1",
        now=datetime.now(UTC),
      )

    fire.assert_not_called()
    assert result.inserted == 1
    assert result.auto_committed == 0
    assert result.dispatch_failed == 0
    evt = session.add_all.call_args.args[0][0]
    assert evt.status == "captured"


class TestCaptureHardening:
  """Defaults + drop counters that keep handler validation passing on
  real QB data (memo/posting_date can be None in the wild; zero-amount
  filtering can leave entries with <2 lines)."""

  def _txn(self, **txn_overrides) -> dict:
    return {
      "external_id": "JE_900",
      "external_source": "quickbooks",
      "number": "900",
      "type": "Invoice",
      "amount": 5000,
      "currency": "USD",
      "date": date(2026, 1, 15),
      "source_id": "JE_900",
      **txn_overrides,
    }

  def _entry(self, **overrides) -> dict:
    return {
      "external_id": "JE_900",
      "external_transaction_id": "JE_900",
      "external_source": "quickbooks",
      "number": "900",
      "type": "standard",
      "posting_date": date(2026, 1, 15),
      "memo": "Sale to Acme",
      "status": "posted",
      **overrides,
    }

  def _balanced_lines(self) -> list[dict]:
    return [
      {
        "entry_external_id": "JE_900",
        "element_external_id": "elem_ar",
        "debit_amount": 5000,
        "credit_amount": 0,
        "line_order": 1,
      },
      {
        "entry_external_id": "JE_900",
        "element_external_id": "elem_revenue",
        "debit_amount": 0,
        "credit_amount": 5000,
        "line_order": 2,
      },
    ]

  def _run(self, dbt: dict):
    from robosystems.operations.extensions.loader import OLTPLoader

    session = MagicMock()
    session.query.return_value.filter.return_value.all.return_value = []
    loader = OLTPLoader()
    cap = loader._capture_transactions_as_events(
      session,
      dbt,
      source="quickbooks",
      connection_id="conn_1",
      created_by="user_1",
      now=datetime.now(UTC),
    )
    return session, cap

  def _captured_event(self, session) -> object:
    return session.add_all.call_args_list[0][0][0][0]

  def test_memo_falls_back_to_doc_number_when_qb_returns_none(self):
    dbt = {
      "transactions": [self._txn()],
      "entries": [self._entry(memo=None)],  # ← QB sometimes omits memo
      "line_items": self._balanced_lines(),
    }
    session, cap = self._run(dbt)
    assert cap.inserted == 1
    metadata_entries = self._captured_event(session).metadata_["entries"]
    assert metadata_entries[0]["memo"] == "900"  # falls back to qb_doc_number

  def test_memo_falls_back_to_synthetic_when_no_memo_or_doc_number(self):
    dbt = {
      "transactions": [self._txn(number=None)],
      "entries": [self._entry(memo=None)],
      "line_items": self._balanced_lines(),
    }
    session, cap = self._run(dbt)
    assert cap.inserted == 1
    metadata_entries = self._captured_event(session).metadata_["entries"]
    assert metadata_entries[0]["memo"] == "QB Invoice JE_900"

  def test_posting_date_falls_back_to_occurred_at(self):
    dbt = {
      "transactions": [self._txn()],
      "entries": [self._entry(posting_date=None)],
      "line_items": self._balanced_lines(),
    }
    session, cap = self._run(dbt)
    assert cap.inserted == 1
    metadata_entries = self._captured_event(session).metadata_["entries"]
    # txn date is 2026-01-15; occurred_at fallback uses .date().isoformat()
    assert metadata_entries[0]["posting_date"] == "2026-01-15"

  def test_entry_with_one_line_after_zero_filter_is_dropped(self):
    """QB sometimes emits a tax line + one zero-amount placeholder. After
    zero-filter the entry has 1 line and can't balance — drop it."""
    dbt = {
      "transactions": [self._txn()],
      "entries": [
        self._entry(),
        # Second entry has only one non-zero line
        self._entry(external_id="JE_900_B"),
      ],
      "line_items": [
        *self._balanced_lines(),
        {
          "entry_external_id": "JE_900_B",
          "element_external_id": "elem_tax",
          "debit_amount": 100,
          "credit_amount": 0,
          "line_order": 1,
        },
        # Zero-amount line gets filtered, leaving only one
        {
          "entry_external_id": "JE_900_B",
          "element_external_id": "elem_tax_payable",
          "debit_amount": 0,
          "credit_amount": 0,
          "line_order": 2,
        },
      ],
    }
    session, cap = self._run(dbt)
    assert cap.inserted == 1  # Transaction still has one valid entry
    assert cap.dropped_unbalanced_entries == 1
    metadata_entries = self._captured_event(session).metadata_["entries"]
    assert len(metadata_entries) == 1
    assert metadata_entries[0]["external_id"] == "JE_900"

  def test_transaction_with_all_unbalanced_entries_is_dropped(self):
    """Edge: every entry filters down to <2 lines → drop the whole
    transaction (otherwise we'd produce an unapprovable inbox row)."""
    dbt = {
      "transactions": [self._txn()],
      "entries": [self._entry()],
      "line_items": [
        # Single line, one of which is zero-amount → filtered → only 1 line
        {
          "entry_external_id": "JE_900",
          "element_external_id": "elem_x",
          "debit_amount": 100,
          "credit_amount": 0,
          "line_order": 1,
        },
        {
          "entry_external_id": "JE_900",
          "element_external_id": "elem_y",
          "debit_amount": 0,
          "credit_amount": 0,
          "line_order": 2,
        },
      ],
    }
    session, cap = self._run(dbt)
    assert cap.inserted == 0
    assert cap.dropped_unbalanced_entries == 1
    assert cap.dropped_empty_transactions == 1
    session.add_all.assert_not_called()


class TestCaptureAgentsFromQB:
  """Phase 2: agent UPSERT keyed on (connection_id, source, external_id)."""

  def _agents_data(self) -> dict:
    return {
      "agents": [
        {
          "id": "qb_cust_1",
          "agent_type": "customer",
          "name": "Acme Corp",
          "legal_name": "Acme Corporation",
          "email": "ap@acme.example",
          "phone": "+1-555-0100",
          # JSON-stringified at parquet layer — loader json.loads back to dict
          "address": '{"line1": "1 Main", "city": "Seattle"}',
          "tax_id": "12-3456789",
          "is_1099_recipient": False,
          "is_active": True,
        },
        {
          "id": "qb_vend_1",
          "agent_type": "vendor",
          "name": "Office Depot",
          "legal_name": "",
          "email": "",
          "phone": "",
          "address": "{}",
          "tax_id": "",
          "is_1099_recipient": True,
          "is_active": True,
        },
      ],
    }

  def test_capture_inserts_new_agents(self):
    """Fresh agents → inserted; lookup map populated."""
    from robosystems.operations.extensions.loader import OLTPLoader

    session = MagicMock()
    session.query.return_value.filter.return_value.all.return_value = []

    loader = OLTPLoader()
    out = loader._capture_agents_from_qb(
      session,
      self._agents_data(),
      source="quickbooks",
      connection_id="conn_a",
      created_by="user_1",
      now=datetime.now(UTC),
    )

    assert out.inserted == 2
    assert out.updated == 0
    assert "qb_cust_1" in out.external_to_id
    assert "qb_vend_1" in out.external_to_id
    assert out.external_to_id["qb_cust_1"].startswith("agt_")
    assert session.add_all.called

  def test_capture_updates_existing_agent_in_place(self):
    """Re-sync of the same agent UPSERTs in place — no new row, no new ULID."""
    from robosystems.operations.extensions.loader import OLTPLoader

    existing = MagicMock()
    existing.external_id = "qb_cust_1"
    existing.id = "agt_existing"

    session = MagicMock()
    session.query.return_value.filter.return_value.all.return_value = [existing]

    loader = OLTPLoader()
    dbt = {
      "agents": [
        {
          "id": "qb_cust_1",
          "agent_type": "customer",
          "name": "Acme Corp Renamed",
          "legal_name": "",
          "email": "new@acme.example",
          "phone": "",
          "address": "{}",
          "tax_id": "",
          "is_1099_recipient": False,
          "is_active": True,
        },
      ],
    }

    out = loader._capture_agents_from_qb(
      session,
      dbt,
      source="quickbooks",
      connection_id="conn_a",
      created_by="user_1",
      now=datetime.now(UTC),
    )

    assert out.inserted == 0
    assert out.updated == 1
    assert existing.name == "Acme Corp Renamed"
    assert existing.email == "new@acme.example"
    assert out.external_to_id["qb_cust_1"] == "agt_existing"

  def test_capture_returns_empty_when_no_agents_table(self):
    """No agents mart in dbt_data → no-op, no DB calls."""
    from robosystems.operations.extensions.loader import OLTPLoader

    session = MagicMock()
    loader = OLTPLoader()
    out = loader._capture_agents_from_qb(
      session,
      {},  # no "agents" key
      source="quickbooks",
      connection_id="conn_a",
      created_by="user_1",
      now=datetime.now(UTC),
    )

    assert out.inserted == 0
    assert out.updated == 0
    assert out.external_to_id == {}
    session.add_all.assert_not_called()

  def test_capture_scopes_lookup_by_connection_id(self):
    """Agent lookup filters by (source, connection_id, external_id) so two
    QB connections on the same graph don't share agents."""
    from robosystems.models.extensions.roboledger import Agent
    from robosystems.operations.extensions.loader import OLTPLoader

    session = MagicMock()
    session.query.return_value.filter.return_value.all.return_value = []

    loader = OLTPLoader()
    loader._capture_agents_from_qb(
      session,
      self._agents_data(),
      source="quickbooks",
      connection_id="conn_specific",
      created_by="user_1",
      now=datetime.now(UTC),
    )

    # Inspect the filter call — the WHERE clause should reference
    # connection_id, source, and external_id.
    session.query.assert_called_with(Agent)


class TestCaptureWithEventTypeAndAgent:
  """Phase 2: source-class fidelity + agent linkage from the transactions mart."""

  def _dbt_with_invoice(self) -> dict:
    """Invoice transaction with event_type / agent_external_id from header join."""
    return {
      "transactions": [
        {
          "external_id": "Invoice_42",
          "external_source": "quickbooks",
          "number": "INV-42",
          "type": "Invoice",
          "category": None,
          "amount": 100000,
          "currency": "USD",
          "date": date(2026, 2, 1),
          "due_date": None,
          "merchant_name": None,
          "reference_number": "INV-42",
          "description": "Consulting services",
          "source_id": "Invoice_42",
          "status": "posted",
          # Phase 2 columns from the per-class header join in transactions.sql:
          "event_type": "invoice_issued",
          "event_category": "sales",
          "agent_external_id": "qb_cust_1",
          "agent_type": "customer",
        }
      ],
      "entries": [
        {
          "external_id": "Invoice_42",
          "external_transaction_id": "Invoice_42",
          "number": "INV-42",
          "type": "standard",
          "posting_date": date(2026, 2, 1),
          "memo": "Consulting services",
          "status": "posted",
        }
      ],
      "line_items": [
        {
          "entry_external_id": "Invoice_42",
          "element_external_id": "elem_AR",
          "debit_amount": 100000,
          "credit_amount": 0,
          "description": "AR debit",
          "line_order": 1,
        },
        {
          "entry_external_id": "Invoice_42",
          "element_external_id": "elem_Revenue",
          "debit_amount": 0,
          "credit_amount": 100000,
          "description": "Revenue credit",
          "line_order": 2,
        },
      ],
    }

  def test_event_carries_class_specific_event_type(self):
    """Invoice transaction → event_type='invoice_issued', category='sales'."""
    from robosystems.operations.extensions.loader import OLTPLoader

    session = MagicMock()
    session.query.return_value.filter.return_value.all.return_value = []

    loader = OLTPLoader()
    loader._capture_transactions_as_events(
      session,
      self._dbt_with_invoice(),
      source="quickbooks",
      connection_id="conn_1",
      created_by="user_1",
      now=datetime.now(UTC),
      agent_lookup={"qb_cust_1": "agt_resolved"},
    )

    evt = session.add_all.call_args_list[0][0][0][0]
    assert evt.event_type == "invoice_issued"
    assert evt.event_category == "sales"
    assert evt.agent_id == "agt_resolved"
    assert evt.metadata_["qb_source_class"] == "Invoice"
    assert evt.metadata_["qb_agent_external_id"] == "qb_cust_1"

  def test_missing_agent_in_lookup_falls_back_to_null(self):
    """If header references an agent_external_id but it's not in the
    UPSERTed lookup (soft-deleted in QB), capture with agent_id=None."""
    from robosystems.operations.extensions.loader import OLTPLoader

    session = MagicMock()
    session.query.return_value.filter.return_value.all.return_value = []

    loader = OLTPLoader()
    loader._capture_transactions_as_events(
      session,
      self._dbt_with_invoice(),
      source="quickbooks",
      connection_id="conn_1",
      created_by="user_1",
      now=datetime.now(UTC),
      agent_lookup={},  # Empty — agent gone from QB
    )

    evt = session.add_all.call_args_list[0][0][0][0]
    assert evt.event_type == "invoice_issued"
    assert evt.agent_id is None
    # Event still captured (not skipped) so the user can review in inbox
    assert evt.status == "captured"

  def test_legacy_transaction_without_event_type_defaults_to_journal(self):
    """Existing fixture data without event_type → defaults to journal_entry_recorded."""
    from robosystems.operations.extensions.loader import OLTPLoader

    dbt = self._dbt_with_invoice()
    # Strip Phase 2 columns
    dbt["transactions"][0].pop("event_type", None)
    dbt["transactions"][0].pop("event_category", None)
    dbt["transactions"][0].pop("agent_external_id", None)

    session = MagicMock()
    session.query.return_value.filter.return_value.all.return_value = []

    loader = OLTPLoader()
    loader._capture_transactions_as_events(
      session,
      dbt,
      source="quickbooks",
      connection_id="conn_1",
      created_by="user_1",
      now=datetime.now(UTC),
    )

    evt = session.add_all.call_args_list[0][0][0][0]
    assert evt.event_type == "journal_entry_recorded"
    assert evt.event_category == "adjustment"
    assert evt.agent_id is None
