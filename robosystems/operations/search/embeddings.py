"""Platform-level text embedding service using fastembed.

Provides embedding generation for document indexing and semantic search.
Uses BAAI/bge-small-en-v1.5 (384 dimensions) which is included in the
Docker image at no cost — no external API calls or credits consumed.
"""

from __future__ import annotations

from robosystems.logger import logger

# Model constants
MODEL_NAME = "BAAI/bge-small-en-v1.5"
EMBEDDING_DIMENSIONS = 384
EMBEDDING_MODEL_ID = "fastembed"

# onnxruntime intra-op thread count. Pinned to 1 on purpose: in CPU-limited
# containers (ECS Fargate), letting onnxruntime auto-size its thread pool
# oversubscribes the CFS quota — the spin-wait threads contend instead of
# compute, making embedding ~5x SLOWER. Benchmarked on this workload:
# threads=None ~1280ms/section vs threads=1 ~240ms/section.
EMBEDDING_THREADS = 1


class EmbeddingService:
  """Text embedding using fastembed (included, no credits)."""

  def __init__(self) -> None:
    self._model = None

  @property
  def model(self):
    """Lazy-load the fastembed model on first use."""
    if self._model is None:
      from fastembed import TextEmbedding

      self._model = TextEmbedding(MODEL_NAME, threads=EMBEDDING_THREADS)
      logger.info(f"Loaded embedding model: {MODEL_NAME} ({EMBEDDING_DIMENSIONS}d)")
    return self._model

  def embed_batch(self, texts: list[str]) -> list[list[float]]:
    """Generate embeddings for a batch of texts.

    Args:
        texts: List of text strings to embed.

    Returns:
        List of embedding vectors (each 384 floats).
    """
    if not texts:
      return []
    embeddings = list(self.model.embed(texts))
    return [emb.tolist() for emb in embeddings]

  def embed_single(self, text: str) -> list[float]:
    """Generate an embedding for a single text string."""
    return self.embed_batch([text])[0]


# Lazy singleton
_service: EmbeddingService | None = None


def get_embedding_service() -> EmbeddingService:
  """Get the embedding service singleton."""
  global _service
  if _service is None:
    _service = EmbeddingService()
  return _service
