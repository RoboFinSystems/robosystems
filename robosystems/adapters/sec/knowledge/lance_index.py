"""LanceDB vector search index builder.

Generates a LanceDB index of SEC element embeddings from a DuckDB staging
database. The index enables fast (~5ms) approximate nearest neighbor search
over 2.6M+ element embeddings using IVF-PQ indexing.

This is a knowledge artifact — built alongside element_knowledge.parquet,
structure_profiles.parquet, etc. in the sec_knowledge_artifacts Dagster asset.

Output: a Lance directory (tar.gz'd for S3 upload) containing:
  - elements table: qname, name, canonical_concept, canonical_confidence,
    classification, balance, vector (FLOAT32[384])
  - IVF-PQ index on the vector column (cosine metric)
"""

from __future__ import annotations

import gc
import logging
import shutil
import tarfile
import tempfile
from pathlib import Path

import duckdb

logger = logging.getLogger(__name__)


class LanceIndexBuilder:
  """Builds a LanceDB vector search index from DuckDB staging data.

  Queries the Element and FACT_HAS_ELEMENT tables to extract numeric,
  fact-linked elements with embeddings, then creates a LanceDB table
  with an IVF-PQ index for fast cosine similarity search.
  """

  def __init__(self, memory_limit: str = "8GB") -> None:
    self._memory_limit = memory_limit

  def build(self, db_path: str | Path, output_dir: Path | None = None) -> Path:
    """Build the LanceDB index from a DuckDB staging database.

    Args:
        db_path: Path to the DuckDB staging database.
        output_dir: Directory to write the lance index to.
            Defaults to a temp directory alongside db_path.

    Returns:
        Path to the lance directory.
    """
    import lancedb

    db_path = Path(db_path)

    if output_dir is None:
      output_dir = db_path.parent / "lance_index"
    output_dir.mkdir(parents=True, exist_ok=True)

    lance_path = output_dir / "elements.lance"

    # Clean up any prior build
    if lance_path.exists():
      shutil.rmtree(lance_path)

    logger.info(f"Building LanceDB index from {db_path}")

    # Extract fact-linked numeric elements with embeddings via DuckDB
    con = duckdb.connect(str(db_path), read_only=True)
    try:
      con.execute(f"SET memory_limit = '{self._memory_limit}'")

      # Count total elements for progress logging
      row = con.execute("SELECT COUNT(*) FROM Element").fetchone()
      total = row[0] if row else 0
      logger.info(f"Total elements in staging: {total:,}")

      logger.info("Querying fact-linked numeric elements with embeddings...")
      arrow_table = con.execute("""
        SELECT
          e.identifier,
          e.qname,
          e.name,
          e.canonical_concept,
          e.canonical_confidence,
          e.classification,
          e.balance,
          e.embedding::FLOAT[384] AS vector
        FROM Element e
        WHERE e.is_numeric = true
          AND e.is_textblock = false
          AND e.embedding IS NOT NULL
          AND e.identifier IN (
            SELECT DISTINCT dst FROM FACT_HAS_ELEMENT
          )
      """).fetch_arrow_table()

      row_count = arrow_table.num_rows
      logger.info(f"Extracted {row_count:,} elements for lance index")
    finally:
      con.close()

    if row_count == 0:
      logger.warning("No elements with embeddings found — skipping lance index build")
      return output_dir

    # Create LanceDB table
    logger.info(f"Creating LanceDB table at {output_dir}")
    db = lancedb.connect(str(output_dir))
    table = db.create_table("elements", data=arrow_table, mode="overwrite")

    # Free arrow table before building index
    del arrow_table
    gc.collect()

    # Build IVF-PQ index for fast approximate search
    # IVF-PQ requires more vectors than partitions; skip for small datasets
    if row_count > 1000:
      num_partitions = min(256, row_count // 10)
      logger.info(f"Building IVF-PQ index (cosine, {num_partitions} partitions)...")
      table.create_index(
        metric="cosine",
        num_partitions=num_partitions,
        num_sub_vectors=48,
      )
      logger.info("LanceDB index build complete")
    else:
      logger.info(
        f"Skipping IVF-PQ index ({row_count} rows too few, brute-force is fine)"
      )

    return output_dir

  def build_and_tar(self, db_path: str | Path, output_tar: Path | None = None) -> Path:
    """Build the LanceDB index and package as tar.gz.

    Args:
        db_path: Path to the DuckDB staging database.
        output_tar: Path for the output tar.gz file.
            Defaults to lance_index.tar.gz alongside db_path.

    Returns:
        Path to the tar.gz file.
    """
    db_path = Path(db_path)

    if output_tar is None:
      output_tar = db_path.parent / "lance_index.tar.gz"

    # Build to a temp directory, then tar.gz
    with tempfile.TemporaryDirectory(prefix="lance_build_") as tmpdir:
      lance_dir = self.build(db_path, output_dir=Path(tmpdir) / "lance")

      # Check if the index was actually built
      elements_dir = lance_dir / "elements.lance"
      if not elements_dir.exists():
        logger.warning("No lance index to package — skipping tar.gz")
        return output_tar

      logger.info(f"Packaging lance index to {output_tar}")
      with tarfile.open(str(output_tar), "w:gz") as tar:
        # Archive the contents of lance_dir so extraction gives sec/elements.lance/...
        tar.add(str(lance_dir), arcname="sec")

    tar_size_mb = output_tar.stat().st_size / (1024 * 1024)
    logger.info(f"Lance index tar.gz: {tar_size_mb:.1f} MB")
    return output_tar
