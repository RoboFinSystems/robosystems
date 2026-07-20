"""Tests for the TAXONOMY_AUTHORING_ENABLED environment gate.

The gate lives at the taxonomy-block command dispatch chokepoint so one
check covers both the REST operations and the auto-generated MCP tools.
Gated types: ``reporting_extension`` + ``custom_ontology`` (framework
authoring). ``chart_of_accounts`` is never gated (core product path) and
delete stays open (content removal reduces liability).
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from robosystems.operations.taxonomy_block import commands as commands_mod
from robosystems.operations.taxonomy_block.commands import (
  GATED_TAXONOMY_TYPES,
  TaxonomyAuthoringDisabledError,
  create_taxonomy_block,
  delete_taxonomy_block,
  update_taxonomy_block,
)

FLAG = "robosystems.config.env.TAXONOMY_AUTHORING_ENABLED"


def _entry() -> MagicMock:
  entry = MagicMock()
  entry.dispatch_create.return_value = "tax_new"
  entry.dispatch_update.return_value = "tax_upd"
  entry.dispatch_delete.return_value = 0
  entry.dispatch_build_envelope.return_value = MagicMock()
  return entry


def _create_body(taxonomy_type: str) -> MagicMock:
  body = MagicMock()
  body.taxonomy_type = taxonomy_type
  return body


def _taxonomy(taxonomy_type: str) -> MagicMock:
  taxonomy = MagicMock()
  taxonomy.taxonomy_type = taxonomy_type
  taxonomy.name = "Tenant Framework"
  return taxonomy


class TestGateScope:
  def test_gated_set_is_exactly_the_framework_types(self) -> None:
    assert {"reporting_extension", "custom_ontology"} == GATED_TAXONOMY_TYPES


class TestFlagOff:
  @pytest.mark.parametrize("taxonomy_type", sorted(GATED_TAXONOMY_TYPES))
  def test_create_gated_type_raises_before_dispatch(
    self, monkeypatch: pytest.MonkeyPatch, taxonomy_type: str
  ) -> None:
    monkeypatch.setattr(FLAG, False)
    entry = _entry()
    with patch.object(commands_mod.registry_module, "get", return_value=entry):
      with pytest.raises(TaxonomyAuthoringDisabledError, match=taxonomy_type):
        create_taxonomy_block(MagicMock(), _create_body(taxonomy_type), "usr_1")
    entry.dispatch_create.assert_not_called()

  @pytest.mark.parametrize("taxonomy_type", sorted(GATED_TAXONOMY_TYPES))
  def test_update_gated_type_raises_before_dispatch(
    self, monkeypatch: pytest.MonkeyPatch, taxonomy_type: str
  ) -> None:
    monkeypatch.setattr(FLAG, False)
    entry = _entry()
    session = MagicMock()
    session.get.return_value = _taxonomy(taxonomy_type)
    body = MagicMock()
    body.taxonomy_id = "tax_1"
    with patch.object(commands_mod.registry_module, "get", return_value=entry):
      with pytest.raises(TaxonomyAuthoringDisabledError):
        update_taxonomy_block(session, body, "usr_1")
    entry.dispatch_update.assert_not_called()

  def test_create_chart_of_accounts_still_dispatches(
    self, monkeypatch: pytest.MonkeyPatch
  ) -> None:
    monkeypatch.setattr(FLAG, False)
    entry = _entry()
    with patch.object(commands_mod.registry_module, "get", return_value=entry):
      envelope = create_taxonomy_block(
        MagicMock(), _create_body("chart_of_accounts"), "usr_1"
      )
    entry.dispatch_create.assert_called_once()
    assert envelope is entry.dispatch_build_envelope.return_value

  def test_delete_gated_type_still_dispatches(
    self, monkeypatch: pytest.MonkeyPatch
  ) -> None:
    monkeypatch.setattr(FLAG, False)
    entry = _entry()
    session = MagicMock()
    session.get.return_value = _taxonomy("reporting_extension")
    body = MagicMock()
    body.taxonomy_id = "tax_1"
    body.cascade_facts = False
    with patch.object(commands_mod.registry_module, "get", return_value=entry):
      response = delete_taxonomy_block(session, body, "usr_1")
    entry.dispatch_delete.assert_called_once()
    assert response.taxonomy_id == "tax_1"


class TestFlagOn:
  @pytest.mark.parametrize("taxonomy_type", sorted(GATED_TAXONOMY_TYPES))
  def test_create_gated_type_dispatches(
    self, monkeypatch: pytest.MonkeyPatch, taxonomy_type: str
  ) -> None:
    monkeypatch.setattr(FLAG, True)
    entry = _entry()
    with patch.object(commands_mod.registry_module, "get", return_value=entry):
      envelope = create_taxonomy_block(
        MagicMock(), _create_body(taxonomy_type), "usr_1"
      )
    entry.dispatch_create.assert_called_once()
    assert envelope is entry.dispatch_build_envelope.return_value


class TestErrorMapWiring:
  def test_create_and_update_specs_map_disabled_to_403(self) -> None:
    from robosystems.routers.extensions.roboledger import operations as ops

    specs = {s.name: s for s in ops._registrar.registered_specs}
    for name in ("create-taxonomy-block", "update-taxonomy-block"):
      assert specs[name].error_map.get(TaxonomyAuthoringDisabledError) == 403
    assert (
      TaxonomyAuthoringDisabledError not in specs["delete-taxonomy-block"].error_map
    )
