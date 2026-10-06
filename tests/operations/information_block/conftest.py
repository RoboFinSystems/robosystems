"""Mocked sessions in this package script every ``execute`` call. A named
entity is passed through unqueried here, as before the single resolver; the
in-graph check on a named entity is covered by the ``entity_scope`` DB tests."""

import pytest

from robosystems.operations.information_block import forecast, forecast_compute, metrics
from robosystems.operations.roboledger import entity_scope


@pytest.fixture(autouse=True)
def _named_entity_passes_through(monkeypatch):
  def resolve(session, entity_id=None):
    return entity_id or entity_scope.resolve_entity_id(session)

  for module in (forecast, forecast_compute, metrics):
    monkeypatch.setattr(module, "resolve_entity_id", resolve)
