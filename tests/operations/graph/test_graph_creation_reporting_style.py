"""Wiring tests for entity-driven Reporting Style + entity_type at provision.

Reporting Style lives on the entity now, so ``_provision_entity`` is the seam:
it resolves the style from the create payload (precedence unit-tested in
``test_reporting_style_defaults``) and stamps both ``reporting_style_id`` and
``entity_type`` onto the ``LedgerEntity``. ``_persist_metadata`` no longer
touches reporting style — it's not a graph-level column anymore.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from robosystems.config.constants import ReportingStyleConstants as RS
from robosystems.operations.graph.graph_creation_service import (
  GraphCreationConfig,
  GraphCreationService,
)


def _config(entity_data: dict | None) -> GraphCreationConfig:
  return GraphCreationConfig(
    user_id="u1",
    tier="ladybug-standard",
    graph_name="G",
    graph_type="entity",
    schema_extensions=["roboledger"],
    entity_data=entity_data,
  )


async def _provision_entity_kwargs(entity_data: dict) -> dict:
  """Run ``_provision_entity`` with the DB layer mocked; return the kwargs
  passed to the ``LedgerEntity`` constructor."""
  svc = GraphCreationService()
  session = MagicMock()
  ext_cm = MagicMock()
  ext_cm.__enter__.return_value = session
  ext_cm.__exit__.return_value = False

  with (
    patch("robosystems.db.extensions.provision_tenant_schema"),
    patch("robosystems.db.extensions.extensions_session", return_value=ext_cm),
    patch("robosystems.models.extensions.entity.Entity") as m_entity,
  ):
    await svc._provision_entity("kg1", _config(entity_data))
    assert m_entity.called
    return m_entity.call_args.kwargs


class TestProvisionEntityStyleResolution:
  async def test_partnership_entity_pins_part_style(self) -> None:
    kwargs = await _provision_entity_kwargs(
      {"name": "P", "uri": "https://p.co", "entity_type": "partnership"}
    )
    assert kwargs["reporting_style_id"] == RS.PARTNERSHIP_STYLE_ID
    assert kwargs["entity_type"] == "partnership"

  async def test_llc_entity_pins_llc_style(self) -> None:
    kwargs = await _provision_entity_kwargs(
      {"name": "L", "uri": "https://l.co", "entity_type": "llc"}
    )
    assert kwargs["reporting_style_id"] == RS.LLC_STYLE_ID

  async def test_corporation_pins_default_style(self) -> None:
    kwargs = await _provision_entity_kwargs(
      {"name": "C", "uri": "https://c.co", "entity_type": "corporation"}
    )
    assert kwargs["reporting_style_id"] == RS.DEFAULT_STYLE_ID

  async def test_explicit_override_wins(self) -> None:
    kwargs = await _provision_entity_kwargs(
      {
        "name": "C",
        "uri": "https://c.co",
        "entity_type": "partnership",
        "reporting_style_id": "forced-style",
      }
    )
    assert kwargs["reporting_style_id"] == "forced-style"

  async def test_no_entity_type_uses_default(self) -> None:
    kwargs = await _provision_entity_kwargs({"name": "X", "uri": "https://x.co"})
    assert kwargs["reporting_style_id"] == RS.DEFAULT_STYLE_ID
