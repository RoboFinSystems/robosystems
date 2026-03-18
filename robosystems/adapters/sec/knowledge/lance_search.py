"""LanceDB vector search for SEC element resolution.

Provides fast (~5ms) approximate nearest neighbor search over element
embeddings. Used by the resolve-element MCP tool to find XBRL elements
matching natural-language financial concepts.

The search module lazy-loads the LanceDB index on first use and returns
None from get_instance() if the index directory doesn't exist, enabling
graceful fallback to canonical matching.
"""

from __future__ import annotations

import logging
import os

logger = logging.getLogger(__name__)


class LanceElementSearch:
  """Vector search over SEC XBRL elements using LanceDB.

  Singleton pattern with lazy loading — the LanceDB connection is opened
  on the first search call, not at import or instantiation time.
  """

  _instance: LanceElementSearch | None = None

  def __init__(self, lance_path: str) -> None:
    self._lance_path = lance_path
    self._table = None

  @classmethod
  def get_instance(cls) -> LanceElementSearch | None:
    """Get the singleton LanceElementSearch instance.

    Returns None if the lance index directory doesn't exist,
    allowing callers to fall back to canonical matching.
    """
    if cls._instance is not None:
      return cls._instance

    path = _resolve_lance_path()
    if path is None:
      return None

    cls._instance = cls(path)
    return cls._instance

  @classmethod
  def reset(cls) -> None:
    """Reset the singleton instance (for testing)."""
    cls._instance = None

  @property
  def table(self):
    """Lazy-load the LanceDB table on first access."""
    if self._table is None:
      import lancedb

      db = lancedb.connect(self._lance_path)
      try:
        self._table = db.open_table("elements")
        logger.info(
          f"LanceDB elements table loaded from {self._lance_path} "
          f"({self._table.count_rows():,} rows)"
        )
      except Exception:
        logger.warning(
          f"Failed to open LanceDB elements table at {self._lance_path}",
          exc_info=True,
        )
        raise
    return self._table

  def search(
    self,
    query_embedding: list[float],
    limit: int = 20,
  ) -> list[dict]:
    """Search for elements by embedding similarity.

    Args:
        query_embedding: Query vector (384-dim float list from fastembed).
        limit: Maximum number of results to return.

    Returns:
        List of dicts with keys: qname, name, canonical_concept,
        canonical_confidence, classification, balance, _distance.
        Results are sorted by distance (ascending = most similar).
    """
    try:
      results = (
        self.table.search(query_embedding)
        .limit(limit)
        .select(
          [
            "qname",
            "name",
            "canonical_concept",
            "canonical_confidence",
            "classification",
            "balance",
            "_distance",
          ]
        )
        .with_row_id(False)
        .to_list()
      )
      return results
    except Exception:
      logger.warning("LanceDB search failed", exc_info=True)
      return []


def _resolve_lance_path() -> str | None:
  """Resolve the path to the LanceDB index directory.

  Checks env.LANCE_INDEX_PATH for a directory containing an elements.lance
  subdirectory. Returns None if the index doesn't exist.
  """
  from robosystems.config import env

  base_path = env.LANCE_INDEX_PATH

  # Check for elements.lance subdirectory (standard structure)
  # The index is stored at {LANCE_INDEX_PATH}/sec/elements.lance/
  # but lancedb.connect() takes the parent: {LANCE_INDEX_PATH}/sec/
  sec_path = os.path.join(base_path, "sec")
  elements_lance = os.path.join(sec_path, "elements.lance")

  if os.path.isdir(elements_lance):
    logger.debug(f"LanceDB index found at {sec_path}")
    return sec_path

  # Also check base path directly (for dev/testing)
  elements_lance_direct = os.path.join(base_path, "elements.lance")
  if os.path.isdir(elements_lance_direct):
    logger.debug(f"LanceDB index found at {base_path}")
    return base_path

  logger.debug(f"No LanceDB index found at {base_path}")
  return None
