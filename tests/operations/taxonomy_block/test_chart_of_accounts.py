"""Unit tests for chart_of_accounts._auto_link_entity."""

from __future__ import annotations

from unittest.mock import MagicMock

from robosystems.operations.taxonomy_block.chart_of_accounts import _auto_link_entity


def _entity(entity_id: str = "ent_1") -> MagicMock:
  e = MagicMock()
  e.id = entity_id
  return e


def _entity_taxonomy(taxonomy_id: str = "tx_1", is_primary: bool = True) -> MagicMock:
  et = MagicMock(spec=["entity_id", "taxonomy_id", "basis", "is_primary"])
  et.entity_id = "ent_1"
  et.taxonomy_id = taxonomy_id
  et.basis = "chart_of_accounts"
  et.is_primary = is_primary
  return et


class TestAutoLinkEntity:
  def test_no_entity_is_noop(self) -> None:
    """No entity in graph → nothing written."""
    session = MagicMock()
    session.execute.return_value.scalar_one_or_none.return_value = None
    _auto_link_entity(session, "tx_1")
    session.add.assert_not_called()
    session.flush.assert_not_called()

  def test_no_prior_link_creates_primary(self) -> None:
    """Entity exists but no CoA link yet → new primary row added."""
    entity = _entity()
    session = MagicMock()
    session.execute.return_value.scalar_one_or_none.side_effect = [entity, None]
    _auto_link_entity(session, "tx_1")
    session.add.assert_called_once()
    added = session.add.call_args[0][0]
    assert added.taxonomy_id == "tx_1"
    assert added.basis == "chart_of_accounts"
    assert added.is_primary is True

  def test_existing_primary_link_is_noop(self) -> None:
    """Link already exists and is primary → early return, no writes."""
    entity = _entity()
    existing = _entity_taxonomy(taxonomy_id="tx_1", is_primary=True)
    session = MagicMock()
    session.execute.return_value.scalar_one_or_none.side_effect = [entity, existing]
    _auto_link_entity(session, "tx_1")
    session.add.assert_not_called()
    session.flush.assert_not_called()

  def test_existing_non_primary_link_is_promoted(self) -> None:
    """Link exists but is non-primary → flip is_primary, no new row."""
    entity = _entity()
    existing = _entity_taxonomy(taxonomy_id="tx_1", is_primary=False)
    session = MagicMock()
    session.execute.return_value.scalar_one_or_none.side_effect = [entity, existing]
    _auto_link_entity(session, "tx_1")
    assert existing.is_primary is True
    session.add.assert_not_called()
    session.flush.assert_called()

  def test_new_primary_demotes_old_primary(self) -> None:
    """Creating a new primary link calls query().filter().update() to demote others."""
    entity = _entity()
    session = MagicMock()
    session.execute.return_value.scalar_one_or_none.side_effect = [entity, None]
    _auto_link_entity(session, "tx_new")
    session.query.assert_called()
    update_call = session.query.return_value.filter.return_value.update
    update_call.assert_called_once_with(
      {"is_primary": False}, synchronize_session=False
    )

  def test_idempotent_second_call(self) -> None:
    """Calling _auto_link_entity twice for the same taxonomy is a no-op on second call."""
    entity = _entity()
    existing = _entity_taxonomy(taxonomy_id="tx_1", is_primary=True)
    session = MagicMock()
    session.execute.return_value.scalar_one_or_none.side_effect = [
      entity,
      existing,
      entity,
      existing,
    ]
    _auto_link_entity(session, "tx_1")
    _auto_link_entity(session, "tx_1")
    session.add.assert_not_called()
