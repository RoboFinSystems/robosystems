"""DuckDB data extraction for SEC graph analytics.

Extracts calculation arcs, presentation arcs, and element metadata
from DuckDB staging databases for use in graph analytics pipelines.
"""

from dataclasses import dataclass
from pathlib import Path

import duckdb


@dataclass(frozen=True)
class CalcArc:
  """A calculation arc between parent and child elements."""

  parent_qname: str
  child_qname: str
  weight: float
  structure_name: str


@dataclass(frozen=True)
class PresArc:
  """A presentation arc between parent and child elements."""

  parent_qname: str
  child_qname: str
  order: float
  structure_name: str


@dataclass
class ElementInfo:
  """Metadata for an XBRL element."""

  qname: str
  name: str
  period_type: str | None
  balance: str | None
  is_abstract: bool
  is_numeric: bool


class ArcExtractor:
  """Extracts arc relationships and element metadata from a DuckDB database.

  Opens the database read-only and provides methods to extract
  calculation arcs, presentation arcs, and element metadata.

  Args:
      db_path: Path to the DuckDB database file.
  """

  def __init__(self, db_path: str | Path) -> None:
    self._db_path = Path(db_path)
    if not self._db_path.exists():
      raise FileNotFoundError(f"Database not found: {self._db_path}")

  def _connect(self) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(self._db_path), read_only=True)

  def extract_calculation_arcs(self) -> list[CalcArc]:
    """Extract calculation arcs (summation-item relationships).

    Joins Structure -> STRUCTURE_HAS_ASSOCIATION -> Association ->
    ASSOCIATION_HAS_FROM_ELEMENT/TO_ELEMENT -> Element to build
    parent-child calculation relationships with weights.

    Returns:
        List of CalcArc with parent/child qnames, weight, and structure name.
    """
    sql = """
      SELECT
        parent_el.qname AS parent_qname,
        child_el.qname AS child_qname,
        COALESCE(a.weight, 1.0) AS weight,
        COALESCE(s.name, s.definition, '') AS structure_name
      FROM Association a
      JOIN ASSOCIATION_HAS_FROM_ELEMENT afrom
        ON a.identifier = afrom.src
      JOIN Element parent_el
        ON afrom.dst = parent_el.identifier
      JOIN ASSOCIATION_HAS_TO_ELEMENT ato
        ON a.identifier = ato.src
      JOIN Element child_el
        ON ato.dst = child_el.identifier
      JOIN STRUCTURE_HAS_ASSOCIATION sha
        ON a.identifier = sha.dst
      JOIN Structure s
        ON sha.src = s.identifier
      WHERE a.association_type = 'Calculation'
    """
    conn = self._connect()
    try:
      rows = conn.execute(sql).fetchall()
      return [
        CalcArc(
          parent_qname=row[0],
          child_qname=row[1],
          weight=row[2],
          structure_name=row[3],
        )
        for row in rows
      ]
    finally:
      conn.close()

  def extract_presentation_arcs(self) -> list[PresArc]:
    """Extract presentation arcs (parent-child display relationships).

    Returns:
        List of PresArc with parent/child qnames, order, and structure name.
    """
    sql = """
      SELECT
        parent_el.qname AS parent_qname,
        child_el.qname AS child_qname,
        COALESCE(a.order_value, 0.0) AS order_value,
        COALESCE(s.name, s.definition, '') AS structure_name
      FROM Association a
      JOIN ASSOCIATION_HAS_FROM_ELEMENT afrom
        ON a.identifier = afrom.src
      JOIN Element parent_el
        ON afrom.dst = parent_el.identifier
      JOIN ASSOCIATION_HAS_TO_ELEMENT ato
        ON a.identifier = ato.src
      JOIN Element child_el
        ON ato.dst = child_el.identifier
      JOIN STRUCTURE_HAS_ASSOCIATION sha
        ON a.identifier = sha.dst
      JOIN Structure s
        ON sha.src = s.identifier
      WHERE a.association_type = 'Presentation'
    """
    conn = self._connect()
    try:
      rows = conn.execute(sql).fetchall()
      return [
        PresArc(
          parent_qname=row[0],
          child_qname=row[1],
          order=row[2],
          structure_name=row[3],
        )
        for row in rows
      ]
    finally:
      conn.close()

  def extract_all_elements(self) -> dict[str, ElementInfo]:
    """Extract all element metadata from the database.

    Returns:
        Dict mapping qname to ElementInfo.
    """
    sql = """
      SELECT
        qname,
        name,
        period_type,
        balance,
        COALESCE(is_abstract, false) AS is_abstract,
        COALESCE(is_numeric, false) AS is_numeric
      FROM Element
      WHERE qname IS NOT NULL
    """
    conn = self._connect()
    try:
      rows = conn.execute(sql).fetchall()
      return {
        row[0]: ElementInfo(
          qname=row[0],
          name=row[1],
          period_type=row[2],
          balance=row[3],
          is_abstract=bool(row[4]),
          is_numeric=bool(row[5]),
        )
        for row in rows
      }
    finally:
      conn.close()
