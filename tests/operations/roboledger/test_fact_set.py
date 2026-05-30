"""Tests for the mandatory-at-emission FactSet provenance contract."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

import pytest
from sqlalchemy import event

from robosystems.models.api.fact_provenance import PivotProvenance
from robosystems.models.extensions.roboledger.fact_set import (
  FactSet,
  ProvenanceRequiredError,
  _require_provenance,
)
from robosystems.operations.roboledger.fact_set import create_fact_set


class TestBeforeInsertBackstop:
  def _fs(self, provenance) -> FactSet:
    fs = FactSet(
      structure_id="st_1",
      period_start=date(2026, 1, 1),
      period_end=date(2026, 1, 31),
      factset_type="report",
      entity_id="ent_1",
    )
    fs.provenance = provenance
    return fs

  def test_registered_for_before_insert_only(self) -> None:
    assert event.contains(FactSet, "before_insert", _require_provenance)
    assert not event.contains(FactSet, "before_update", _require_provenance)

  def test_raises_when_missing(self) -> None:
    with pytest.raises(ProvenanceRequiredError):
      _require_provenance(None, None, self._fs(None))

  def test_raises_when_not_a_dict(self) -> None:
    with pytest.raises(ProvenanceRequiredError):
      _require_provenance(None, None, self._fs("pivot"))

  def test_raises_when_no_origin_key(self) -> None:
    with pytest.raises(ProvenanceRequiredError):
      _require_provenance(None, None, self._fs({"mapping_id": "m"}))

  def test_passes_with_valid_descriptor(self) -> None:
    _require_provenance(
      None, None, self._fs({"origin": "pivot", "mapping_id": "m", "period": "p"})
    )


class TestCreateFactSetHelper:
  def test_stamps_provenance_and_adds(self) -> None:
    session = MagicMock()
    fs = create_fact_set(
      session,
      period_end=date(2026, 1, 31),
      period_start=date(2026, 1, 1),
      factset_type="report",
      entity_id="ent_1",
      provenance=PivotProvenance(mapping_id="map_1", period="2026-01-01/2026-01-31"),
      created_by="user_1",
      structure_id="st_1",
      report_id="rpt_1",
    )
    session.add.assert_called_once_with(fs)
    assert fs.provenance == {
      "origin": "pivot",
      "mapping_id": "map_1",
      "period": "2026-01-01/2026-01-31",
      "arc_type": None,
      "posting_filter": None,
    }
    assert fs.factset_type == "report"
    assert fs.report_id == "rpt_1"
    assert fs.created_by == "user_1"

  def test_honours_explicit_id(self) -> None:
    session = MagicMock()
    fs = create_fact_set(
      session,
      id="fs_explicit",
      period_end=date(2026, 1, 31),
      factset_type="schedule",
      entity_id="ent_1",
      provenance={
        "origin": "schedule",
        "structure_id": "st_1",
        "method": "straight_line",
      },
      created_by="user_1",
    )
    assert fs.id == "fs_explicit"
    assert fs.provenance["origin"] == "schedule"

  def test_rejects_invalid_provenance(self) -> None:
    from pydantic import ValidationError

    session = MagicMock()
    with pytest.raises(ValidationError):
      create_fact_set(
        session,
        period_end=date(2026, 1, 31),
        factset_type="report",
        entity_id="ent_1",
        provenance={"origin": "pivot"},  # missing mapping_id/period
        created_by="user_1",
      )
    session.add.assert_not_called()
