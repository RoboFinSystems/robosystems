"""The envelope's fact row carries the fact's own descriptor and when its
column froze, projected straight from the ORM row."""

from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from robosystems.models.extensions import Fact
from robosystems.operations.information_block.envelope import fact_to_lite

pytestmark = pytest.mark.unit


def test_fact_to_lite_carries_provenance_and_frozen_at() -> None:
  frozen_at = datetime(2026, 7, 2, 9, 0, tzinfo=UTC)
  descriptor = {"origin": "pivot", "mapping_id": "map_1", "period": "2026-06-30"}
  row = Fact(
    id="fact_1",
    element_id="el_cash",
    value=100.0,
    period_end=date(2026, 6, 30),
    period_type="instant",
    unit="USD",
    fact_type="Numeric",
    fact_scope="in_scope",
    entity_id="ent_1",
    fact_set_id="fs_1",
    provenance=descriptor,
    frozen_at=frozen_at,
  )
  lite = fact_to_lite(row)
  assert lite.provenance == descriptor
  assert lite.frozen_at == frozen_at


def test_a_live_row_with_no_descriptor_reads_unknown_and_live() -> None:
  row = Fact(
    id="fact_2",
    element_id="el_cash",
    value=1.0,
    period_end=date(2026, 7, 31),
    period_type="instant",
    unit="USD",
    fact_type="Numeric",
    fact_scope="in_scope",
    entity_id="ent_1",
    fact_set_id="fs_1",
  )
  lite = fact_to_lite(row)
  assert lite.provenance is None
  assert lite.frozen_at is None
