"""Wiring tests for entity-driven Reporting Style + entity_type at provision.

Covers the two seams the resolver feeds into:
- ``_persist_metadata`` resolves the style from the create payload and passes
  it to ``Graph.create`` (precedence is unit-tested in
  ``test_reporting_style_defaults``).
- ``_provision_entity`` persists ``entity_type`` onto the ``LedgerEntity``
  (previously dropped on the floor).
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from robosystems.config.constants import ReportingStyleConstants as RS
from robosystems.operations.graph.graph_creation_service import (
  GraphCreationConfig,
  GraphCreationService,
)

_SCHEMA_INFO = {
  "schema_type": "extensions",
  "schema_ddl": "CREATE ...",
  "schema_json": {"base": "base", "extensions": ["roboledger"]},
  "custom_schema_name": None,
  "custom_schema_version": None,
}


def _config(entity_data: dict | None) -> GraphCreationConfig:
  return GraphCreationConfig(
    user_id="u1",
    tier="ladybug-standard",
    graph_name="G",
    graph_type="entity",
    schema_extensions=["roboledger"],
    entity_data=entity_data,
  )


class TestPersistMetadataStyleResolution:
  def _run(self, entity_data: dict | None) -> dict:
    """Run _persist_metadata with the DB layer mocked; return Graph.create kwargs."""
    svc = GraphCreationService()
    location = MagicMock(instance_id="i-123")
    db = MagicMock()

    with (
      patch("robosystems.database.get_db_session", return_value=iter([db])),
      patch("robosystems.models.core.graph.Graph.create") as m_create,
      patch("robosystems.models.core.GraphSchema.create"),
      patch("robosystems.operations.graph.table_service.TableService") as m_table,
    ):
      m_table.return_value.create_tables_from_schema.return_value = []
      svc._persist_metadata(
        "kg1", "org1", location, _config(entity_data), "CREATE ...", _SCHEMA_INFO
      )
      assert m_create.called
      return m_create.call_args.kwargs

  def test_partnership_entity_pins_part_style(self) -> None:
    assert (
      self._run({"name": "P", "uri": "https://p.co", "entity_type": "partnership"})[
        "reporting_style_id"
      ]
      == RS.PARTNERSHIP_STYLE_ID
    )

  def test_llc_entity_pins_llc_style(self) -> None:
    assert (
      self._run({"name": "L", "uri": "https://l.co", "entity_type": "llc"})[
        "reporting_style_id"
      ]
      == RS.LLC_STYLE_ID
    )

  def test_corporation_pins_default_style(self) -> None:
    assert (
      self._run({"name": "C", "uri": "https://c.co", "entity_type": "corporation"})[
        "reporting_style_id"
      ]
      == RS.DEFAULT_STYLE_ID
    )

  def test_explicit_override_wins(self) -> None:
    assert (
      self._run(
        {
          "name": "C",
          "uri": "https://c.co",
          "entity_type": "partnership",
          "reporting_style_id": "forced-style",
        }
      )["reporting_style_id"]
      == "forced-style"
    )

  def test_generic_graph_no_entity_data_uses_default(self) -> None:
    assert self._run(None)["reporting_style_id"] == RS.DEFAULT_STYLE_ID


class TestProvisionEntityPersistsType:
  async def test_entity_type_is_written_to_ledger_entity(self) -> None:
    svc = GraphCreationService()
    config = _config(
      {"name": "Cascade", "uri": "https://x.co", "entity_type": "partnership"}
    )
    session = MagicMock()
    ext_cm = MagicMock()
    ext_cm.__enter__.return_value = session
    ext_cm.__exit__.return_value = False

    with (
      patch("robosystems.db.extensions.provision_tenant_schema") as m_provision,
      patch("robosystems.db.extensions.extensions_session", return_value=ext_cm),
      patch("robosystems.models.extensions.entity.Entity") as m_entity,
    ):
      await svc._provision_entity("kg1", config)
      m_provision.assert_called_once_with("kg1")
      assert m_entity.call_args.kwargs["entity_type"] == "partnership"
