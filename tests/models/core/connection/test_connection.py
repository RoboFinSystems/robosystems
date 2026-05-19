# pyright: reportGeneralTypeIssues=false, reportArgumentType=false, reportOperatorIssue=false, reportOptionalMemberAccess=false
"""Tests for Connection model."""

from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest
from sqlalchemy.exc import SQLAlchemyError

from robosystems.models.core.connection.connection import Connection, ConnectionStatus


@pytest.mark.unit
class TestConnectionStatus:
  def test_enum_values(self):
    assert ConnectionStatus.PENDING_OAUTH == "pending_oauth"
    assert ConnectionStatus.CONNECTED == "connected"
    assert ConnectionStatus.ERROR == "error"
    assert ConnectionStatus.NEEDS_REAUTH == "needs_reauth"
    assert ConnectionStatus.DISCONNECTED == "disconnected"

  def test_is_string_enum(self):
    assert isinstance(ConnectionStatus.CONNECTED, str)
    assert ConnectionStatus.CONNECTED == "connected"


@pytest.mark.unit
class TestConnectionCreate:
  def test_creates_with_all_fields(self):
    session = MagicMock()
    conn = Connection.create(
      graph_id="kg_test",
      user_id="usr_1",
      provider="quickbooks",
      session=session,
      status=ConnectionStatus.CONNECTED,
      realm_id="realm123",
      item_id="item456",
      cik="0001234567",
      entity_name="Acme Corp",
      institution_name="Chase",
      auto_sync_enabled=False,
    )

    assert conn.graph_id == "kg_test"
    assert conn.user_id == "usr_1"
    assert conn.provider == "quickbooks"
    assert conn.status == ConnectionStatus.CONNECTED
    assert conn.realm_id == "realm123"
    assert conn.item_id == "item456"
    assert conn.cik == "0001234567"
    assert conn.entity_name == "Acme Corp"
    assert conn.institution_name == "Chase"
    assert conn.auto_sync_enabled is False
    session.add.assert_called_once_with(conn)
    session.commit.assert_called_once()
    session.refresh.assert_called_once_with(conn)

  def test_creates_with_minimal_fields(self):
    session = MagicMock()
    conn = Connection.create(
      graph_id="kg_test",
      user_id="usr_1",
      provider="quickbooks",
      session=session,
    )

    assert conn.graph_id == "kg_test"
    assert conn.provider == "quickbooks"
    assert conn.status == ConnectionStatus.PENDING_OAUTH
    assert conn.realm_id is None
    assert conn.item_id is None
    assert conn.cik is None
    assert conn.auto_sync_enabled is True

  def test_rollback_on_sqlalchemy_error(self):
    session = MagicMock()
    session.commit.side_effect = SQLAlchemyError("db error")

    with pytest.raises(SQLAlchemyError):
      Connection.create(
        graph_id="kg_test",
        user_id="usr_1",
        provider="quickbooks",
        session=session,
      )

    session.rollback.assert_called_once()


@pytest.mark.unit
class TestConnectionGetById:
  def test_returns_connection(self):
    """The lookup chain is `.filter(id).filter(deleted_at IS NULL).first()`
    by default — Phase 3 B6 added the deleted_at filter."""
    session = MagicMock()
    mock_conn = MagicMock(spec=Connection)
    # Chain: query → filter (id) → filter (deleted_at) → first
    session.query.return_value.filter.return_value.filter.return_value.first.return_value = mock_conn

    result = Connection.get_by_id("conn_abc", session)

    assert result is mock_conn

  def test_returns_none_when_not_found(self):
    session = MagicMock()
    session.query.return_value.filter.return_value.filter.return_value.first.return_value = None

    result = Connection.get_by_id("nonexistent", session)

    assert result is None


@pytest.mark.unit
class TestConnectionGetByGraphAndProvider:
  def test_returns_connections(self):
    """Chain: query → filter (graph_id+provider) → filter (deleted_at)
    → order_by → all."""
    session = MagicMock()
    mock_conns = [MagicMock(spec=Connection), MagicMock(spec=Connection)]
    session.query.return_value.filter.return_value.filter.return_value.order_by.return_value.all.return_value = mock_conns

    result = Connection.get_by_graph_and_provider("kg_test", "quickbooks", session)

    assert len(result) == 2

  def test_returns_empty_list(self):
    session = MagicMock()
    session.query.return_value.filter.return_value.filter.return_value.order_by.return_value.all.return_value = []

    result = Connection.get_by_graph_and_provider("kg_test", "quickbooks", session)

    assert result == []


@pytest.mark.unit
class TestConnectionGetAllForGraph:
  def test_returns_all_connections(self):
    session = MagicMock()
    mock_conns = [MagicMock(spec=Connection)]
    session.query.return_value.filter.return_value.filter.return_value.order_by.return_value.all.return_value = mock_conns

    result = Connection.get_all_for_graph("kg_test", session)

    assert len(result) == 1


@pytest.mark.unit
class TestConnectionListFiltered:
  def test_no_filters(self):
    """Even with no explicit filters, the deleted_at IS NULL filter
    runs by default (Phase 3 B6)."""
    session = MagicMock()
    query = session.query.return_value
    # query.filter(deleted_at IS NULL) → order_by → all
    query.filter.return_value.order_by.return_value.all.return_value = []

    result = Connection.list_filtered(session=session)

    assert result == []

  def test_all_filters(self):
    session = MagicMock()
    mock_conn = MagicMock(spec=Connection)
    query = session.query.return_value
    query.filter.return_value = query
    query.order_by.return_value.all.return_value = [mock_conn]

    result = Connection.list_filtered(
      session=session,
      graph_id="kg_test",
      user_id="usr_1",
      provider="quickbooks",
    )

    assert len(result) == 1

  def test_graph_id_only(self):
    session = MagicMock()
    query = session.query.return_value
    query.filter.return_value = query
    query.order_by.return_value.all.return_value = []

    Connection.list_filtered(session=session, graph_id="kg_test")

    # filter should have been called (for graph_id)
    query.filter.assert_called()


@pytest.mark.unit
class TestConnectionUpdateStatus:
  def test_updates_status(self):
    session = MagicMock()
    conn = Connection(
      graph_id="kg_test",
      user_id="usr_1",
      provider="quickbooks",
      status="pending_oauth",
    )

    conn.update_status("connected", session)

    assert conn.status == "connected"
    assert conn.updated_at is not None
    session.commit.assert_called_once()
    session.refresh.assert_called_once_with(conn)

  def test_rollback_on_error(self):
    session = MagicMock()
    session.commit.side_effect = SQLAlchemyError("db error")
    conn = Connection(
      graph_id="kg_test",
      user_id="usr_1",
      provider="quickbooks",
      status="pending_oauth",
    )

    with pytest.raises(SQLAlchemyError):
      conn.update_status("error", session)

    session.rollback.assert_called_once()


@pytest.mark.unit
class TestConnectionUpdateLastSync:
  def test_updates_last_sync(self):
    session = MagicMock()
    conn = Connection(
      graph_id="kg_test",
      user_id="usr_1",
      provider="quickbooks",
    )

    conn.update_last_sync(session)

    assert conn.last_sync is not None
    assert conn.updated_at is not None
    session.commit.assert_called_once()

  def test_rollback_on_error(self):
    session = MagicMock()
    session.commit.side_effect = SQLAlchemyError("db error")
    conn = Connection(
      graph_id="kg_test",
      user_id="usr_1",
      provider="quickbooks",
    )

    with pytest.raises(SQLAlchemyError):
      conn.update_last_sync(session)

    session.rollback.assert_called_once()


@pytest.mark.unit
class TestConnectionUpdateMetadata:
  def test_updates_all_fields(self):
    session = MagicMock()
    conn = Connection(
      graph_id="kg_test",
      user_id="usr_1",
      provider="quickbooks",
    )

    conn.update_metadata(
      session,
      status="connected",
      realm_id="realm_new",
      item_id="item_new",
      cik="0009999999",
      entity_name="New Corp",
      institution_name="Wells Fargo",
      auto_sync_enabled=False,
    )

    assert conn.status == "connected"
    assert conn.realm_id == "realm_new"
    assert conn.item_id == "item_new"
    assert conn.cik == "0009999999"
    assert conn.entity_name == "New Corp"
    assert conn.institution_name == "Wells Fargo"
    assert conn.auto_sync_enabled is False
    session.commit.assert_called_once()

  def test_updates_partial_fields(self):
    session = MagicMock()
    conn = Connection(
      graph_id="kg_test",
      user_id="usr_1",
      provider="quickbooks",
      realm_id="original",
    )

    conn.update_metadata(session, entity_name="Updated Corp")

    assert conn.entity_name == "Updated Corp"
    assert conn.realm_id == "original"  # unchanged
    session.commit.assert_called_once()

  def test_none_values_not_applied(self):
    session = MagicMock()
    conn = Connection(
      graph_id="kg_test",
      user_id="usr_1",
      provider="quickbooks",
      realm_id="keep_this",
    )

    conn.update_metadata(session, realm_id=None)

    # None means "don't update", so realm_id stays
    assert conn.realm_id == "keep_this"

  def test_rollback_on_error(self):
    session = MagicMock()
    session.commit.side_effect = SQLAlchemyError("db error")
    conn = Connection(
      graph_id="kg_test",
      user_id="usr_1",
      provider="quickbooks",
    )

    with pytest.raises(SQLAlchemyError):
      conn.update_metadata(session, status="error")

    session.rollback.assert_called_once()


@pytest.mark.unit
class TestConnectionDelete:
  def test_deletes_connection(self):
    session = MagicMock()
    conn = Connection(
      graph_id="kg_test",
      user_id="usr_1",
      provider="quickbooks",
    )

    conn.delete(session)

    session.delete.assert_called_once_with(conn)
    session.commit.assert_called_once()

  def test_rollback_on_error(self):
    session = MagicMock()
    session.commit.side_effect = SQLAlchemyError("db error")
    conn = Connection(
      graph_id="kg_test",
      user_id="usr_1",
      provider="quickbooks",
    )

    with pytest.raises(SQLAlchemyError):
      conn.delete(session)

    session.rollback.assert_called_once()


@pytest.mark.unit
class TestConnectionToDict:
  def test_full_to_dict(self):
    now = datetime.now(UTC)
    conn = Connection(
      graph_id="kg_test",
      user_id="usr_1",
      provider="quickbooks",
      status="connected",
      realm_id="realm123",
      item_id=None,
      cik=None,
      entity_name="Acme Corp",
      institution_name=None,
      auto_sync_enabled=True,
      last_sync=now,
      created_at=now,
      updated_at=now,
    )
    conn.id = "conn_abc123"

    result = conn.to_dict()

    assert result["connection_id"] == "conn_abc123"
    assert result["provider"] == "quickbooks"
    assert result["status"] == "connected"
    assert result["entity_id"] == "kg_test"
    assert result["graph_id"] == "kg_test"
    assert result["user_id"] == "usr_1"
    assert result["metadata"]["realm_id"] == "realm123"
    assert result["metadata"]["entity_name"] == "Acme Corp"
    assert result["metadata"]["auto_sync_enabled"] is True
    assert result["metadata"]["last_sync"] == now.isoformat()
    assert result["created_at"] == now
    assert result["updated_at"] == now

  def test_to_dict_no_last_sync(self):
    conn = Connection(
      graph_id="kg_test",
      user_id="usr_1",
      provider="quickbooks",
      status="pending_oauth",
      last_sync=None,
      created_at=datetime.now(UTC),
      updated_at=datetime.now(UTC),
    )
    conn.id = "conn_xyz"

    result = conn.to_dict()

    assert result["metadata"]["last_sync"] is None


@pytest.mark.unit
class TestConnectionRepr:
  def test_repr(self):
    conn = Connection(
      graph_id="kg_test",
      user_id="usr_1",
      provider="quickbooks",
    )
    conn.id = "conn_abc"

    assert repr(conn) == "<Connection conn_abc quickbooks graph=kg_test>"


@pytest.mark.unit
class TestSoftDelete:
  """Phase 3 B6 — soft_delete + restore preserve the row, lookups
  filter `deleted_at IS NULL` by default."""

  def test_soft_delete_sets_deleted_at(self):
    conn = Connection(
      graph_id="kg_test",
      user_id="usr_1",
      provider="quickbooks",
    )
    conn.id = "conn_abc"
    conn.deleted_at = None
    session = MagicMock()

    conn.soft_delete(session)

    assert conn.deleted_at is not None
    session.commit.assert_called_once()
    session.refresh.assert_called_once_with(conn)

  def test_restore_clears_deleted_at(self):
    conn = Connection(
      graph_id="kg_test",
      user_id="usr_1",
      provider="quickbooks",
    )
    conn.id = "conn_abc"
    conn.deleted_at = datetime.now(UTC)
    session = MagicMock()

    conn.restore(session)

    assert conn.deleted_at is None
    session.commit.assert_called_once()

  def test_get_by_id_filters_soft_deleted_by_default(self):
    session = MagicMock()
    query = session.query.return_value
    filter1 = query.filter.return_value  # WHERE id = ...
    filter2 = filter1.filter.return_value  # WHERE deleted_at IS NULL
    filter2.first.return_value = None

    Connection.get_by_id("conn_x", session)

    # Two .filter() calls expected — id match + deleted_at IS NULL.
    assert query.filter.called
    assert filter1.filter.called

  def test_get_by_id_include_deleted_skips_deleted_at_filter(self):
    session = MagicMock()
    query = session.query.return_value
    filter1 = query.filter.return_value
    filter1.first.return_value = None

    Connection.get_by_id("conn_x", session, include_deleted=True)

    # Only the id filter — no chained deleted_at filter.
    assert query.filter.called
    filter1.filter.assert_not_called()

  def test_find_soft_deleted_for_realm(self):
    """The reuse-on-re-OAuth lookup keys on (graph_id, provider, realm_id)
    AND deleted_at IS NOT NULL, ordered by deleted_at desc."""
    session = MagicMock()

    Connection.find_soft_deleted_for_realm(
      graph_id="kg_test",
      provider="quickbooks",
      realm_id="9130355906016526",
      session=session,
    )

    # Confirm the query chain is what we expect.
    session.query.assert_called_once_with(Connection)
    # The filter chain ends with .order_by().first().
    query = session.query.return_value
    assert query.filter.called
    assert query.filter.return_value.order_by.called


@pytest.mark.unit
class TestConnectionToDictWithSoftDelete:
  def test_to_dict_includes_deleted_at(self):
    conn = Connection(
      graph_id="kg_test",
      user_id="usr_1",
      provider="quickbooks",
      created_at=datetime.now(UTC),
      updated_at=datetime.now(UTC),
    )
    conn.id = "conn_xyz"
    conn.deleted_at = None

    result = conn.to_dict()

    assert "deleted_at" in result
    assert result["deleted_at"] is None
