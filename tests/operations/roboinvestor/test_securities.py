"""Tests for operations/roboinvestor/{reads,commands}/securities.py."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

from robosystems.models.api.extensions.investor import CreateSecurityRequest
from robosystems.operations.roboinvestor.commands.securities import (
  EntityNotFoundError,
  create_security,
  soft_delete_security,
  update_security,
)
from robosystems.operations.roboinvestor.reads.securities import (
  find_linked_entity,
  get_security,
  list_securities,
  security_to_response,
)


def _make_security(**overrides) -> MagicMock:
  defaults = {
    "id": "sec_01",
    "entity_id": "ent_01",
    "source_graph_id": None,
    "name": "Acme Class A",
    "security_type": "equity",
    "security_subtype": "common",
    "terms": {},
    "is_active": True,
    "authorized_shares": 1000000,
    "outstanding_shares": 500000,
    "created_at": datetime(2024, 1, 1, tzinfo=UTC),
    "updated_at": datetime(2024, 1, 1, tzinfo=UTC),
  }
  defaults.update(overrides)
  row = MagicMock()
  for k, v in defaults.items():
    setattr(row, k, v)
  return row


def _make_entity(**overrides) -> MagicMock:
  defaults = {"id": "ent_01", "name": "Acme Corp"}
  defaults.update(overrides)
  e = MagicMock()
  for k, v in defaults.items():
    setattr(e, k, v)
  return e


class TestSecurityToResponse:
  def test_maps_with_entity_name(self) -> None:
    resp = security_to_response(_make_security(), entity_name="Acme Corp")
    assert resp.id == "sec_01"
    assert resp.entity_name == "Acme Corp"
    assert resp.security_type == "equity"

  def test_maps_without_entity_name(self) -> None:
    resp = security_to_response(_make_security(entity_id=None))
    assert resp.entity_name is None


class TestFindLinkedEntity:
  def test_returns_existing_entity(self) -> None:
    session = MagicMock()
    # `MagicMock(name=...)` sets the mock's display name, not an attribute —
    # attribute-style assignment is what the ops code reads.
    row = MagicMock()
    row.id = "ent_42"
    row.name = "Linked Co"
    session.execute.return_value.first.return_value = row

    entity_id, entity_name = find_linked_entity(session, "kg_source_1")

    assert entity_id == "ent_42"
    assert entity_name == "Linked Co"

  def test_returns_none_when_not_yet_linked(self) -> None:
    session = MagicMock()
    session.execute.return_value.first.return_value = None

    entity_id, entity_name = find_linked_entity(session, "kg_source_2")

    assert entity_id is None
    assert entity_name is None


class TestListSecurities:
  def test_returns_paginated_results_with_entity_names(self) -> None:
    securities = [
      _make_security(id="sec_1", entity_id="ent_1"),
      _make_security(id="sec_2", entity_id="ent_2"),
    ]
    entities = [_make_entity(id="ent_1"), _make_entity(id="ent_2", name="Beta Co")]

    session = MagicMock()
    count_result = MagicMock()
    count_result.scalar.return_value = 2
    list_result = MagicMock()
    list_result.scalars.return_value.all.return_value = securities
    entity_result = MagicMock()
    entity_result.scalars.return_value.all.return_value = entities
    session.execute.side_effect = [count_result, list_result, entity_result]

    result = list_securities(session, limit=50)

    assert result.pagination.total == 2
    assert len(result.securities) == 2
    assert result.securities[0].entity_name == "Acme Corp"
    assert result.securities[1].entity_name == "Beta Co"

  def test_skips_entity_lookup_when_all_unlinked(self) -> None:
    securities = [_make_security(entity_id=None)]
    session = MagicMock()
    count_result = MagicMock()
    count_result.scalar.return_value = 1
    list_result = MagicMock()
    list_result.scalars.return_value.all.return_value = securities
    session.execute.side_effect = [count_result, list_result]

    result = list_securities(session)

    # Only 2 execute() calls — no entity batch lookup
    assert session.execute.call_count == 2
    assert result.securities[0].entity_name is None


class TestGetSecurity:
  def test_returns_response_with_entity_name(self) -> None:
    sec = _make_security()
    ent = _make_entity()
    session = MagicMock()
    sec_result = MagicMock()
    sec_result.scalar_one_or_none.return_value = sec
    ent_result = MagicMock()
    ent_result.scalar_one_or_none.return_value = ent
    session.execute.side_effect = [sec_result, ent_result]

    result = get_security(session, "sec_01")

    assert result is not None
    assert result.entity_name == "Acme Corp"

  def test_returns_none_when_missing(self) -> None:
    session = MagicMock()
    miss = MagicMock()
    miss.scalar_one_or_none.return_value = None
    session.execute.side_effect = [miss]

    assert get_security(session, "sec_missing") is None


class TestCreateSecurity:
  def test_creates_with_explicit_entity(self) -> None:
    session = MagicMock()
    ent = _make_entity()
    session.execute.return_value.scalar_one_or_none.return_value = ent

    fake_security = _make_security(name="New Stock")
    body = CreateSecurityRequest(
      entity_id="ent_01",
      name="New Stock",
      security_type="equity",
      security_subtype="common",
    )

    with patch(
      "robosystems.operations.roboinvestor.commands.securities.Security",
      return_value=fake_security,
    ):
      result = create_security(session, body, created_by="usr_1")

    assert result.entity_name == "Acme Corp"
    session.add.assert_called_once_with(fake_security)
    session.flush.assert_called_once()

  def test_raises_when_entity_missing(self) -> None:
    session = MagicMock()
    session.execute.return_value.scalar_one_or_none.return_value = None

    body = CreateSecurityRequest(
      entity_id="ent_missing",
      name="Bad",
      security_type="equity",
      security_subtype="common",
    )

    with pytest.raises(EntityNotFoundError):
      create_security(session, body, created_by="usr_1")
    session.add.assert_not_called()

  def test_auto_links_via_source_graph(self) -> None:
    session = MagicMock()
    linked_row = MagicMock()
    linked_row.id = "ent_42"
    linked_row.name = "Linked Co"
    lookup_result = MagicMock()
    lookup_result.first.return_value = linked_row
    session.execute.side_effect = [lookup_result]

    fake_security = _make_security(entity_id="ent_42")
    body = CreateSecurityRequest(
      source_graph_id="kg_source_99",
      name="Shared Stock",
      security_type="equity",
      security_subtype="common",
    )

    with patch(
      "robosystems.operations.roboinvestor.commands.securities.Security",
      return_value=fake_security,
    ):
      result = create_security(session, body, created_by="usr_1")

    assert result.entity_name == "Linked Co"
    session.add.assert_called_once()

  def test_unlinked_when_source_graph_has_no_match(self) -> None:
    session = MagicMock()
    lookup_result = MagicMock()
    lookup_result.first.return_value = None
    session.execute.side_effect = [lookup_result]

    fake_security = _make_security(entity_id=None, source_graph_id="kg_future")
    body = CreateSecurityRequest(
      source_graph_id="kg_future",
      name="Pending",
      security_type="equity",
      security_subtype="common",
    )

    with patch(
      "robosystems.operations.roboinvestor.commands.securities.Security",
      return_value=fake_security,
    ):
      result = create_security(session, body, created_by="usr_1")

    # No entity yet — stays unlinked, will be auto-linked when a report arrives
    assert result.entity_name is None


class TestUpdateSecurity:
  def test_applies_updates_and_returns_with_entity(self) -> None:
    from robosystems.models.api.extensions.investor import UpdateSecurityOperation

    row = _make_security()
    ent = _make_entity()
    session = MagicMock()
    sec_result = MagicMock()
    sec_result.scalar_one_or_none.return_value = row
    ent_result = MagicMock()
    ent_result.scalar_one_or_none.return_value = ent
    session.execute.side_effect = [sec_result, ent_result]

    result = update_security(
      session, UpdateSecurityOperation(security_id="sec_01", name="Renamed")
    )

    assert row.name == "Renamed"
    assert result.name == "Renamed"
    assert result.entity_name == "Acme Corp"

  def test_raises_when_missing(self) -> None:
    from robosystems.models.api.extensions.investor import UpdateSecurityOperation
    from robosystems.operations.roboinvestor.commands.securities import (
      SecurityNotFoundError,
    )

    session = MagicMock()
    miss = MagicMock()
    miss.scalar_one_or_none.return_value = None
    session.execute.side_effect = [miss]

    with pytest.raises(SecurityNotFoundError):
      update_security(
        session, UpdateSecurityOperation(security_id="sec_missing", name="X")
      )


class TestSoftDeleteSecurity:
  def test_sets_is_active_false(self) -> None:
    from robosystems.models.api.extensions.investor import DeleteSecurityOperation

    row = _make_security(is_active=True)
    session = MagicMock()
    session.execute.return_value.scalar_one_or_none.return_value = row

    result = soft_delete_security(
      session, DeleteSecurityOperation(security_id="sec_01")
    )
    assert result.deleted is True
    assert row.is_active is False

  def test_raises_when_missing(self) -> None:
    from robosystems.models.api.extensions.investor import DeleteSecurityOperation
    from robosystems.operations.roboinvestor.commands.securities import (
      SecurityNotFoundError,
    )

    session = MagicMock()
    session.execute.return_value.scalar_one_or_none.return_value = None

    with pytest.raises(SecurityNotFoundError):
      soft_delete_security(session, DeleteSecurityOperation(security_id="sec_missing"))
