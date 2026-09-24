"""Read-only DuckDB extractions for the knowledge artifacts; dedup happens in SQL
to keep Python memory low."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import duckdb

from robosystems.logger import logger

if TYPE_CHECKING:
  import pyarrow as pa


class ArcExtractor:
  """Each call opens its own read-only connection.

  ``threads`` defaults to 1: every thread builds its own hash-join partition,
  so fewer threads means lower peak memory.
  """

  def __init__(
    self,
    db_path: str | Path,
    memory_limit: str = "4GB",
    threads: int = 1,
  ) -> None:
    self._db_path = Path(db_path)
    self._memory_limit = memory_limit
    self._threads = threads
    if not self._db_path.exists():
      raise FileNotFoundError(f"Database not found: {self._db_path}")

  def _connect(self) -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(str(self._db_path), read_only=True)
    conn.execute(f"SET memory_limit = '{self._memory_limit}'")
    conn.execute(f"SET threads = {self._threads}")
    temp_dir = str(self._db_path.parent)
    conn.execute(f"SET temp_directory = '{temp_dir}'")
    # Lets the LIST(DISTINCT ...) aggregates spill instead of OOMing on the
    # full corpus; no caller depends on row order.
    conn.execute("SET preserve_insertion_order = false")
    return conn

  def extract_deduplicated_edges(self) -> list[tuple[str, str, float, str]]:
    """Unique (parent_qname, child_qname, weight, association_type) edges across filings."""
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

  def extract_graph_arrow(self) -> tuple[pa.Array, pa.Table]:
    """(nodes, edges) as Arrow for a zero-copy CSR build.

    ``nodes`` is qnames in node-id order; ``edges`` is (src, dst, weight) sorted
    by (src, dst). A calculation arc wins over a presentation arc (weight 0.5)
    between the same pair.
    """
    conn = self._connect()
    try:
      # Materialized once so both queries below skip the 4-table join.
      conn.execute("""
        CREATE TEMPORARY TABLE _raw_edges AS
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
      """)

      conn.execute("""
        CREATE TEMPORARY TABLE _node_index AS
        WITH all_qnames AS (
          SELECT DISTINCT qname FROM (
            SELECT parent_qname AS qname FROM _raw_edges
            UNION
            SELECT child_qname AS qname FROM _raw_edges
          )
        )
        SELECT qname, (ROW_NUMBER() OVER (ORDER BY qname)) - 1 AS node_id
        FROM all_qnames
      """)

      nodes_arrow = conn.execute(
        "SELECT qname FROM _node_index ORDER BY node_id"
      ).fetch_arrow_table()
      nodes = nodes_arrow.column("qname")

      edges_arrow = conn.execute("""
        WITH calc_edges AS (
          SELECT ni_p.node_id AS src, ni_c.node_id AS dst,
                 MAX(r.weight) AS weight
          FROM _raw_edges r
          JOIN _node_index ni_p ON r.parent_qname = ni_p.qname
          JOIN _node_index ni_c ON r.child_qname = ni_c.qname
          WHERE r.association_type = 'Calculation'
          GROUP BY ni_p.node_id, ni_c.node_id
        ),
        pres_edges AS (
          SELECT ni_p.node_id AS src, ni_c.node_id AS dst, 0.5 AS weight
          FROM _raw_edges r
          JOIN _node_index ni_p ON r.parent_qname = ni_p.qname
          JOIN _node_index ni_c ON r.child_qname = ni_c.qname
          WHERE r.association_type = 'Presentation'
          GROUP BY ni_p.node_id, ni_c.node_id
        ),
        combined AS (
          SELECT src, dst, weight FROM calc_edges
          UNION ALL
          SELECT p.src, p.dst, p.weight FROM pres_edges p
          WHERE NOT EXISTS (
            SELECT 1 FROM calc_edges c WHERE c.src = p.src AND c.dst = p.dst
          )
        )
        SELECT CAST(src AS BIGINT) AS src,
               CAST(dst AS BIGINT) AS dst,
               weight
        FROM combined
        ORDER BY src, dst
      """).fetch_arrow_table()

      return nodes, edges_arrow
    finally:
      conn.close()

  def extract_element_filing_counts(self) -> dict[str, int]:
    """Distinct filings per qname, bridged through the relationship tables so the
    Fact and Report tables are never scanned."""
    sql = """
      SELECT
        e.qname,
        COUNT(DISTINCT rhf.src) AS filing_count
      FROM Element e
      JOIN FACT_HAS_ELEMENT fhe ON e.identifier = fhe.dst
      JOIN REPORT_HAS_FACT rhf ON fhe.src = rhf.dst
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
    """(structure_id, canonical_type, definition_hash, [element_qnames]) per structure."""
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

  def extract_element_disclosure_types(self) -> dict[str, str]:
    """qname → its most frequent disclosure_mechanics type; {} without a Classification table."""
    sql = """
      WITH element_disclosures AS (
        SELECT e.qname, c.type AS disclosure_type, COUNT(*) AS freq
        FROM Classification c
        JOIN ASSOCIATION_HAS_CLASSIFICATION ahc ON c.identifier = ahc.dst
        JOIN Association a ON ahc.src = a.identifier
        JOIN ASSOCIATION_HAS_TO_ELEMENT ato ON a.identifier = ato.src
        JOIN Element e ON ato.dst = e.identifier
        WHERE c.source = 'disclosure_mechanics'
          AND e.qname IS NOT NULL
        GROUP BY e.qname, c.type

        UNION ALL

        SELECT e.qname, c.type AS disclosure_type, COUNT(*) AS freq
        FROM Classification c
        JOIN ASSOCIATION_HAS_CLASSIFICATION ahc ON c.identifier = ahc.dst
        JOIN Association a ON ahc.src = a.identifier
        JOIN ASSOCIATION_HAS_FROM_ELEMENT afrom ON a.identifier = afrom.src
        JOIN Element e ON afrom.dst = e.identifier
        WHERE c.source = 'disclosure_mechanics'
          AND e.qname IS NOT NULL
        GROUP BY e.qname, c.type
      ),
      ranked AS (
        SELECT qname, disclosure_type,
               SUM(freq) AS total_freq,
               ROW_NUMBER() OVER (PARTITION BY qname ORDER BY SUM(freq) DESC) AS rn
        FROM element_disclosures
        GROUP BY qname, disclosure_type
      )
      SELECT qname, disclosure_type
      FROM ranked
      WHERE rn = 1
    """
    conn = self._connect()
    try:
      tables = {row[0] for row in conn.execute("SHOW TABLES").fetchall()}
      if (
        "Classification" not in tables or "ASSOCIATION_HAS_CLASSIFICATION" not in tables
      ):
        return {}

      rows = conn.execute(sql).fetchall()
      return {row[0]: row[1] for row in rows}
    except Exception as e:
      logger.debug(f"extract_element_disclosure_types failed: {e}")
      return {}
    finally:
      conn.close()

  def extract_disclosure_compositions(
    self,
  ) -> list[tuple[str, str, str, list[str]]]:
    """(structure_id, disclosure_type, definition_hash, [element_qnames]) for
    disclosure_mechanics-labeled structures; [] without a Classification table.

    Disclosure-typed structures only: Statement elements would cause false
    matches in the disclosure classifier.
    """
    sql = """
      WITH disclosure_structures AS (
        SELECT DISTINCT
          s.identifier AS structure_id,
          c.type AS disclosure_type
        FROM Structure s
        JOIN STRUCTURE_HAS_ASSOCIATION sha ON s.identifier = sha.src
        JOIN Association a ON sha.dst = a.identifier
        JOIN ASSOCIATION_HAS_CLASSIFICATION ahc ON a.identifier = ahc.src
        JOIN Classification c ON ahc.dst = c.identifier
        WHERE c.source = 'disclosure_mechanics'
          AND s.type = 'Disclosure'
      )
      SELECT
        ds.structure_id,
        ds.disclosure_type,
        MD5(COALESCE(s.definition, '')) AS definition_hash,
        LIST(DISTINCT el.qname ORDER BY el.qname) AS element_qnames
      FROM disclosure_structures ds
      JOIN Structure s ON ds.structure_id = s.identifier
      JOIN STRUCTURE_HAS_ASSOCIATION sha ON s.identifier = sha.src
      JOIN Association a ON sha.dst = a.identifier
      JOIN ASSOCIATION_HAS_TO_ELEMENT ato ON a.identifier = ato.src
      JOIN Element el ON ato.dst = el.identifier
      WHERE el.qname IS NOT NULL
      GROUP BY ds.structure_id, ds.disclosure_type, s.definition
    """
    conn = self._connect()
    try:
      tables = {row[0] for row in conn.execute("SHOW TABLES").fetchall()}
      if (
        "Classification" not in tables or "ASSOCIATION_HAS_CLASSIFICATION" not in tables
      ):
        return []

      rows = conn.execute(sql).fetchall()
      return [(row[0], row[1], row[2], row[3] if row[3] else []) for row in rows]
    except Exception as e:
      logger.debug(f"extract_disclosure_compositions failed: {e}")
      return []
    finally:
      conn.close()

  def extract_disclosure_root_elements(self) -> dict[str, set[str]]:
    """Calculation-root qname → disclosure types, the BFS seeds for statement
    classification; {} without a Classification table."""
    sql = """
      SELECT DISTINCT e.qname, c.type AS disclosure_type
      FROM Classification c
      JOIN ASSOCIATION_HAS_CLASSIFICATION ahc ON c.identifier = ahc.dst
      JOIN Association a ON ahc.src = a.identifier
      JOIN ASSOCIATION_HAS_FROM_ELEMENT afrom ON a.identifier = afrom.src
      JOIN Element e ON afrom.dst = e.identifier
      WHERE c.source = 'disclosure_mechanics'
        AND a.association_type = 'Calculation'
        AND a.root = 'True'
        AND e.qname IS NOT NULL
    """
    conn = self._connect()
    try:
      tables = {row[0] for row in conn.execute("SHOW TABLES").fetchall()}
      if (
        "Classification" not in tables or "ASSOCIATION_HAS_CLASSIFICATION" not in tables
      ):
        return {}

      rows = conn.execute(sql).fetchall()
      result: dict[str, set[str]] = {}
      for qname, dtype in rows:
        result.setdefault(qname, set()).add(dtype)
      return result
    except Exception as e:
      logger.debug(f"extract_disclosure_root_elements failed: {e}")
      return {}
    finally:
      conn.close()

  def extract_element_structure_membership(self) -> dict[str, dict[str, int]]:
    """qname → {canonical_type: structure count}, for the primary_statement vote."""
    sql = """
      WITH element_structures AS (
        SELECT e.qname, s.canonical_type, s.identifier
        FROM Structure s
        JOIN STRUCTURE_HAS_ASSOCIATION sha ON s.identifier = sha.src
        JOIN Association a ON sha.dst = a.identifier
        JOIN ASSOCIATION_HAS_TO_ELEMENT ato ON a.identifier = ato.src
        JOIN Element e ON ato.dst = e.identifier
        WHERE s.canonical_type IS NOT NULL AND e.qname IS NOT NULL

        UNION ALL

        SELECT e.qname, s.canonical_type, s.identifier
        FROM Structure s
        JOIN STRUCTURE_HAS_ASSOCIATION sha ON s.identifier = sha.src
        JOIN Association a ON sha.dst = a.identifier
        JOIN ASSOCIATION_HAS_FROM_ELEMENT afrom ON a.identifier = afrom.src
        JOIN Element e ON afrom.dst = e.identifier
        WHERE s.canonical_type IS NOT NULL AND e.qname IS NOT NULL
      )
      SELECT qname, canonical_type, COUNT(DISTINCT identifier) AS cnt
      FROM element_structures
      GROUP BY qname, canonical_type
    """
    conn = self._connect()
    try:
      tables = {row[0] for row in conn.execute("SHOW TABLES").fetchall()}
      if "Structure" not in tables or "STRUCTURE_HAS_ASSOCIATION" not in tables:
        return {}

      rows = conn.execute(sql).fetchall()
      result: dict[str, dict[str, int]] = {}
      for qname, canonical_type, count in rows:
        if qname not in result:
          result[qname] = {}
        result[qname][canonical_type] = count
      return result
    except Exception as e:
      logger.debug(f"extract_element_structure_membership failed: {e}")
      return {}
    finally:
      conn.close()
