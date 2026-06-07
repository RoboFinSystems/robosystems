"""Tests for the Reporting Style → Network picker."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from robosystems.operations.roboledger.reports.network_picker import (
  NoNetworkForStatementTypeError,
  RenderNetwork,
  get_render_network,
)


@pytest.mark.unit
def test_get_render_network_returns_composed_network() -> None:
  """When a composition row exists and points at an active Structure,
  the picker returns its id/name/CAP."""
  session = MagicMock()
  row = MagicMock()
  row.id = "struct_is_multistep"
  row.name = "rs-gaap — Income Statement — Multi-step"
  row.concept_arrangement = "arithmetic"
  session.execute.return_value.fetchone.return_value = row

  network = get_render_network(session, "025f5d48-style", "income_statement")

  assert isinstance(network, RenderNetwork)
  assert network.structure_id == "struct_is_multistep"
  assert network.name == "rs-gaap — Income Statement — Multi-step"
  assert network.concept_arrangement == "arithmetic"


@pytest.mark.unit
def test_get_render_network_raises_when_composition_missing() -> None:
  """A Style with no row for this statement_type raises explicitly so
  the caller can decide whether to skip or fail closed."""
  session = MagicMock()
  session.execute.return_value.fetchone.return_value = None

  with pytest.raises(NoNetworkForStatementTypeError) as ctx:
    get_render_network(session, "511aeedc-banking", "equity_statement")
  assert ctx.value.reporting_style_id == "511aeedc-banking"
  assert ctx.value.statement_type == "equity_statement"


@pytest.mark.unit
def test_get_render_network_query_filters_active_structures() -> None:
  """The picker must join on `is_active = true` so a deactivated Network
  reads as missing — forces the caller to author a replacement rather
  than silently rendering against a stale Network."""
  session = MagicMock()
  row = MagicMock()
  row.id = "x"
  row.name = "x"
  row.concept_arrangement = None
  session.execute.return_value.fetchone.return_value = row
  get_render_network(session, "025f5d48-style", "balance_sheet")

  call_sql = str(session.execute.call_args[0][0])
  assert "is_active = true" in call_sql
  # And the join is against reporting_style_networks, not a heuristic.
  assert "reporting_style_networks" in call_sql
