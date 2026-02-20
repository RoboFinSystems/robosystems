"""
Canonical taxonomy for XBRL semantic enrichment.

Provides ~40 core financial concepts with pre-computed embeddings.
Accepts an optional fastembed model to avoid redundant model instantiation.

Usage:
    from robosystems.adapters.sec.taxonomy import get_element_taxonomy, get_structure_taxonomy
    concepts = get_element_taxonomy()  # list[CanonicalConcept] with embeddings
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from robosystems.logger import logger

from .concepts import CanonicalConcept

if TYPE_CHECKING:
  from fastembed import TextEmbedding

_element_taxonomy: list[CanonicalConcept] | None = None
_structure_taxonomy: list[CanonicalConcept] | None = None


def _compute_embeddings(
  concepts: tuple[CanonicalConcept, ...],
  model: TextEmbedding | None = None,
) -> list[CanonicalConcept]:
  """Compute embeddings for all concepts using fastembed.

  Args:
      concepts: Tuple of canonical concepts to embed.
      model: Optional pre-loaded TextEmbedding model. If None, creates one.
  """
  if model is None:
    from fastembed import TextEmbedding as _TextEmbedding

    model = _TextEmbedding("BAAI/bge-small-en-v1.5")

  # Build text representations for embedding
  texts = []
  for c in concepts:
    parts = [c.display_name, c.description]
    if c.aliases:
      parts.extend(c.aliases)
    texts.append(" | ".join(parts))

  logger.info(f"Computing embeddings for {len(texts)} taxonomy concepts")
  embeddings = list(model.embed(texts))

  result = []
  for concept, emb in zip(concepts, embeddings, strict=True):
    result.append(
      CanonicalConcept(
        id=concept.id,
        display_name=concept.display_name,
        category=concept.category,
        description=concept.description,
        aliases=concept.aliases,
        expected_elements=concept.expected_elements,
        period_type=concept.period_type,
        balance=concept.balance,
        is_monetary=concept.is_monetary,
        embedding=emb.tolist(),
      )
    )
  return result


def get_element_taxonomy(
  model: TextEmbedding | None = None,
) -> list[CanonicalConcept]:
  """Get all element-level canonical concepts with embeddings.

  Args:
      model: Optional pre-loaded TextEmbedding model to avoid redundant instantiation.
  """
  global _element_taxonomy
  if _element_taxonomy is not None:
    return _element_taxonomy

  from .balance_sheet import BALANCE_SHEET_CONCEPTS
  from .cash_flow import CASH_FLOW_CONCEPTS
  from .income_statement import INCOME_STATEMENT_CONCEPTS

  all_concepts = INCOME_STATEMENT_CONCEPTS + BALANCE_SHEET_CONCEPTS + CASH_FLOW_CONCEPTS
  _element_taxonomy = _compute_embeddings(all_concepts, model=model)
  logger.info(f"Element taxonomy loaded: {len(_element_taxonomy)} concepts")
  return _element_taxonomy


def get_structure_taxonomy(
  model: TextEmbedding | None = None,
) -> list[CanonicalConcept]:
  """Get all structure-level canonical concepts with embeddings.

  Args:
      model: Optional pre-loaded TextEmbedding model to avoid redundant instantiation.
  """
  global _structure_taxonomy
  if _structure_taxonomy is not None:
    return _structure_taxonomy

  from .structures import STRUCTURE_CONCEPTS

  _structure_taxonomy = _compute_embeddings(STRUCTURE_CONCEPTS, model=model)
  logger.info(f"Structure taxonomy loaded: {len(_structure_taxonomy)} concepts")
  return _structure_taxonomy


__all__ = [
  "CanonicalConcept",
  "get_element_taxonomy",
  "get_structure_taxonomy",
]
