"""Unit tests for ledger entity endpoints."""

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException
from sqlalchemy.exc import ProgrammingError

from robosystems.routers.ledger.entity import get_entity, update_entity

MODULE = "robosystems.routers.ledger.entity"
GRAPH_ID = "kg01234567890abcdef"


def _make_user():
  user = MagicMock()
  user.id = "usr_test123"
  return user


def _make_entity(**overrides):
  defaults = {
    "id": "entity_kg01234567890abcdef",
    "name": "Acme Corp",
    "legal_name": "Acme Corporation LLC",
    "uri": "https://robosystems.ai/entities#kg01234567890abcdef",
    "cik": None,
    "ticker": None,
    "exchange": None,
    "sic": None,
    "sic_description": None,
    "category": None,
    "state_of_incorporation": "DE",
    "fiscal_year_end": "1231",
    "tax_id": None,
    "lei": None,
    "industry": None,
    "entity_type": None,
    "phone": None,
    "website": None,
    "status": "active",
    "is_parent": True,
    "parent_entity_id": None,
    "source": "native",
    "source_id": None,
    "connection_id": None,
    "address_line1": None,
    "address_city": None,
    "address_state": None,
    "address_postal_code": None,
    "address_country": None,
    "created_at": datetime(2026, 1, 1, tzinfo=UTC),
    "updated_at": datetime(2026, 1, 1, tzinfo=UTC),
  }
  defaults.update(overrides)
  entity = MagicMock()
  for k, v in defaults.items():
    setattr(entity, k, v)
  return entity


class TestGetEntity:
  @pytest.mark.asyncio
  async def test_returns_entity(self):
    mock_session = MagicMock()
    mock_session.query.return_value.filter.return_value.first.return_value = (
      _make_entity()
    )

    with patch(f"{MODULE}.extensions_session") as mock_ext:
      mock_ext.return_value.__enter__ = MagicMock(return_value=mock_session)
      mock_ext.return_value.__exit__ = MagicMock(return_value=False)

      result = await get_entity(graph_id=GRAPH_ID, user=_make_user())

    assert result.id == "entity_kg01234567890abcdef"
    assert result.name == "Acme Corp"
    assert result.status == "active"
    assert result.source == "native"

  @pytest.mark.asyncio
  async def test_not_found_when_no_entity(self):
    mock_session = MagicMock()
    mock_session.query.return_value.filter.return_value.first.return_value = None

    with patch(f"{MODULE}.extensions_session") as mock_ext:
      mock_ext.return_value.__enter__ = MagicMock(return_value=mock_session)
      mock_ext.return_value.__exit__ = MagicMock(return_value=False)

      with pytest.raises(HTTPException) as exc:
        await get_entity(graph_id=GRAPH_ID, user=_make_user())

    assert exc.value.status_code == 404
    assert "No entity found" in exc.value.detail

  @pytest.mark.asyncio
  async def test_not_found_when_schema_missing(self):
    with patch(f"{MODULE}.extensions_session") as mock_ext:
      mock_ext.return_value.__enter__ = MagicMock(
        side_effect=ProgrammingError("stmt", {}, Exception("schema not found"))
      )

      with pytest.raises(HTTPException) as exc:
        await get_entity(graph_id=GRAPH_ID, user=_make_user())

    assert exc.value.status_code == 404
    assert "not initialized" in exc.value.detail

  @pytest.mark.asyncio
  async def test_not_found_on_invalid_graph_id(self):
    with patch(f"{MODULE}.extensions_session") as mock_ext:
      mock_ext.return_value.__enter__ = MagicMock(
        side_effect=ValueError("Invalid graph_id")
      )

      with pytest.raises(HTTPException) as exc:
        await get_entity(graph_id=GRAPH_ID, user=_make_user())

    assert exc.value.status_code == 404


class TestUpdateEntity:
  @pytest.mark.asyncio
  async def test_updates_provided_fields(self):
    entity = _make_entity()
    mock_session = MagicMock()
    mock_session.query.return_value.filter.return_value.first.return_value = entity

    with patch(f"{MODULE}.extensions_session") as mock_ext:
      mock_ext.return_value.__enter__ = MagicMock(return_value=mock_session)
      mock_ext.return_value.__exit__ = MagicMock(return_value=False)

      from robosystems.models.api.extensions.entity import UpdateEntityRequest

      body = UpdateEntityRequest(name="New Name", phone="555-1234")
      await update_entity(graph_id=GRAPH_ID, body=body, user=_make_user())

    assert entity.name == "New Name"
    assert entity.phone == "555-1234"
    mock_session.commit.assert_called_once()
    mock_session.refresh.assert_called_once_with(entity)

  @pytest.mark.asyncio
  async def test_rejects_empty_update(self):
    entity = _make_entity()
    mock_session = MagicMock()
    mock_session.query.return_value.filter.return_value.first.return_value = entity

    with patch(f"{MODULE}.extensions_session") as mock_ext:
      mock_ext.return_value.__enter__ = MagicMock(return_value=mock_session)
      mock_ext.return_value.__exit__ = MagicMock(return_value=False)

      from robosystems.models.api.extensions.entity import UpdateEntityRequest

      body = UpdateEntityRequest()
      with pytest.raises(HTTPException) as exc:
        await update_entity(graph_id=GRAPH_ID, body=body, user=_make_user())

    assert exc.value.status_code == 400
    assert "No fields provided" in exc.value.detail

  @pytest.mark.asyncio
  async def test_not_found_when_no_entity(self):
    mock_session = MagicMock()
    mock_session.query.return_value.filter.return_value.first.return_value = None

    with patch(f"{MODULE}.extensions_session") as mock_ext:
      mock_ext.return_value.__enter__ = MagicMock(return_value=mock_session)
      mock_ext.return_value.__exit__ = MagicMock(return_value=False)

      from robosystems.models.api.extensions.entity import UpdateEntityRequest

      body = UpdateEntityRequest(name="New Name")
      with pytest.raises(HTTPException) as exc:
        await update_entity(graph_id=GRAPH_ID, body=body, user=_make_user())

    assert exc.value.status_code == 404

  @pytest.mark.asyncio
  async def test_sets_updated_at_timestamp(self):
    entity = _make_entity()
    original_updated = entity.updated_at
    mock_session = MagicMock()
    mock_session.query.return_value.filter.return_value.first.return_value = entity

    with patch(f"{MODULE}.extensions_session") as mock_ext:
      mock_ext.return_value.__enter__ = MagicMock(return_value=mock_session)
      mock_ext.return_value.__exit__ = MagicMock(return_value=False)

      from robosystems.models.api.extensions.entity import UpdateEntityRequest

      body = UpdateEntityRequest(name="Updated")
      await update_entity(graph_id=GRAPH_ID, body=body, user=_make_user())

    assert entity.updated_at != original_updated
