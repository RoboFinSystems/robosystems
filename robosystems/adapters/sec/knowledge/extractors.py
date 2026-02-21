"""DuckDB data extraction for SEC graph knowledge artifacts.

Extracts deduplicated edges, element metadata, filing counts, and
structure compositions from DuckDB staging databases. All heavy
deduplication happens in DuckDB SQL to keep Python memory low.
"""

from dataclasses import dataclass
from pathlib import Path

import duckdb


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
  deduplicated edges, element metadata, filing counts, and
  structure compositions for knowledge artifact generation.

  Args:
      db_path: Path to the DuckDB database file.
      memory_limit: DuckDB memory limit (default "4GB"). Lower for large databases.
  """

  def __init__(self, db_path: str | Path, memory_limit: str = "4GB") -> None:
    self._db_path = Path(db_path)
    self._memory_limit = memory_limit
    if not self._db_path.exists():
      raise FileNotFoundError(f"Database not found: {self._db_path}")

  def _connect(self) -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(str(self._db_path), read_only=True)
    conn.execute(f"SET memory_limit = '{self._memory_limit}'")
    return conn

  def extract_deduplicated_edges(self) -> list[tuple[str, str, float, str]]:
    """Extract unique (parent, child) edges with aggregated metadata.

    Deduplicates across all filings in DuckDB SQL, reducing ~48.5M rows
    to ~1-3M unique edges. DuckDB handles the heavy GROUP BY with
    spill-to-disk; Python receives only the compact result.

    Returns:
        List of (parent_qname, child_qname, weight, association_type) tuples.
    """
    sql = """
      SELECT
        parent_el.qname AS parent_qname,
        child_el.qname AS child_qname,
        MAX(ABS(COALESCE(a.weight, 1.0))) AS weight,
        a.association_type
      FROM Association a
      JOIN ASSOCIATION_HAS_FROM_ELEMENT afrom ON a.identifier = afrom.src
      JOIN Element parent_el ON afrom.dst = parent_el.identifier
      JOIN ASSOCIATION_HAS_TO_ELEMENT ato ON a.identifier = ato.src
      JOIN Element child_el ON ato.dst = child_el.identifier
      WHERE a.association_type IN ('Calculation', 'Presentation')
      GROUP BY parent_el.qname, child_el.qname, a.association_type
    """
    conn = self._connect()
    try:
      rows = conn.execute(sql).fetchall()
      return [(row[0], row[1], row[2], row[3]) for row in rows]
    finally:
      conn.close()

  def extract_element_filing_counts(self) -> dict[str, int]:
    """Count distinct filings per element qname.

    Uses Element -> Fact -> Report chain to count filings since there
    is no direct ELEMENT_BELONGS_TO_FILING relationship. DuckDB handles
    the COUNT(DISTINCT) efficiently with spill-to-disk.

    Returns:
        Dict mapping qname to filing count.
    """
    sql = """
      SELECT
        e.qname,
        COUNT(DISTINCT r.identifier) AS filing_count
      FROM Element e
      JOIN FACT_HAS_ELEMENT fhe ON e.identifier = fhe.dst
      JOIN Fact f ON fhe.src = f.identifier
      JOIN REPORT_HAS_FACT rhf ON f.identifier = rhf.dst
      JOIN Report r ON rhf.src = r.identifier
      WHERE e.qname IS NOT NULL
      GROUP BY e.qname
    """
    conn = self._connect()
    try:
      rows = conn.execute(sql).fetchall()
      return {row[0]: row[1] for row in rows}
    finally:
      conn.close()

  def extract_structure_compositions(
    self,
  ) -> list[tuple[str, str | None, str, list[str]]]:
    """Extract each structure's element composition for fingerprinting.

    Returns:
        List of (structure_identifier, canonical_type, definition_hash, [element_qnames]).
        Groups elements per structure using STRUCTURE_HAS_ASSOCIATION ->
        Association -> Element.
    """
    sql = """
      SELECT
        s.identifier,
        s.canonical_type,
        MD5(COALESCE(s.definition, '')) AS definition_hash,
        LIST(DISTINCT el.qname ORDER BY el.qname) AS element_qnames
      FROM Structure s
      JOIN STRUCTURE_HAS_ASSOCIATION sha ON s.identifier = sha.src
      JOIN Association a ON sha.dst = a.identifier
      JOIN ASSOCIATION_HAS_TO_ELEMENT ato ON a.identifier = ato.src
      JOIN Element el ON ato.dst = el.identifier
      WHERE el.qname IS NOT NULL
      GROUP BY s.identifier, s.canonical_type, s.definition
    """
    conn = self._connect()
    try:
      rows = conn.execute(sql).fetchall()
      return [(row[0], row[1], row[2], row[3] if row[3] else []) for row in rows]
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
