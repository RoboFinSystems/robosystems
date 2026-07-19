"""Shape tests for the non-numeric Fact model (migration 0021).

Metadata-level: pin the column shapes, Python-side defaults, and CHECK
constraint expressions the migration mirrors, so model/migration drift
surfaces without a database.
"""

from __future__ import annotations

from robosystems.models.extensions.roboledger.fact import Fact
from robosystems.models.extensions.roboledger.fact_set import FactSet
from robosystems.models.extensions.structure import (
  CONCEPT_ARRANGEMENT_VALUES,
  TEXT_BLOCK_CAPS,
)


def _check_sql(table, name: str) -> str:
  for c in table.constraints:
    if getattr(c, "name", None) == name:
      return str(c.sqltext)
  raise AssertionError(f"constraint {name} not found on {table.name}")


class TestFactColumns:
  def test_value_nullable_string_value_added(self) -> None:
    cols = Fact.__table__.c
    assert cols.value.nullable is True
    assert cols.string_value.nullable is True
    assert cols.content_type.nullable is True
    assert cols.decimals.nullable is True

  def test_discriminator_defaults(self) -> None:
    cols = Fact.__table__.c
    assert cols.fact_type.nullable is False
    assert cols.fact_type.default is not None
    assert cols.fact_type.default.arg == "Numeric"
    assert cols.value_type.nullable is False
    assert cols.value_type.default is not None
    assert cols.value_type.default.arg == "inline"

  def test_unit_keeps_usd_default(self) -> None:
    # Units apply to numeric facts only, but the column stays NOT NULL —
    # materialize skips the FACT_HAS_UNIT edge for Nonnumeric instead.
    col = Fact.__table__.c.unit
    assert col.nullable is False
    assert col.default is not None
    assert col.default.arg == "USD"


class TestFactChecks:
  def test_fact_type_closed_vocabulary(self) -> None:
    sql = _check_sql(Fact.__table__, "ck_facts_fact_type")
    assert "'Numeric'" in sql
    assert "'Nonnumeric'" in sql

  def test_value_type_closed_vocabulary(self) -> None:
    sql = _check_sql(Fact.__table__, "ck_facts_value_type")
    assert "'inline'" in sql
    assert "'external_resource'" in sql

  def test_value_shape_xor(self) -> None:
    """A fact is numeric XOR non-numeric — never both, never neither."""
    sql = _check_sql(Fact.__table__, "ck_facts_value_shape")
    assert "fact_type = 'Numeric' AND value IS NOT NULL AND string_value IS NULL" in sql
    assert (
      "fact_type = 'Nonnumeric' AND string_value IS NOT NULL AND value IS NULL" in sql
    )


class TestFactSetTypeVocabulary:
  def test_disclosure_admitted(self) -> None:
    sql = _check_sql(FactSet.__table__, "check_fact_set_type")
    for value in ("'report'", "'schedule'", "'custom'", "'disclosure'"):
      assert value in sql


class TestTextBlockCaps:
  def test_subset_of_canonical_vocabulary(self) -> None:
    assert set(CONCEPT_ARRANGEMENT_VALUES) >= TEXT_BLOCK_CAPS

  def test_level4_detail_excluded(self) -> None:
    # cm.xsd level 4 is the numeric detail table, not narrative.
    assert "level4_detail" not in TEXT_BLOCK_CAPS
    assert "text_block" in TEXT_BLOCK_CAPS
