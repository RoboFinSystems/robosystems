"""Local fastembed text embeddings (bge-small, 384-dim): no API calls or credits."""

from __future__ import annotations

from robosystems.logger import logger

MODEL_NAME = "BAAI/bge-small-en-v1.5"
EMBEDDING_DIMENSIONS = 384
EMBEDDING_MODEL_ID = "fastembed"

# Auto-sized onnxruntime threads oversubscribe a CPU-limited container's CFS
# quota and run ~5x slower (measured ~1280 vs ~240 ms/section).
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
    """Embed a batch of texts, one 384-float vector per input."""
    if not texts:
      return []
    embeddings = list(self.model.embed(texts))
    return [emb.tolist() for emb in embeddings]

  def embed_single(self, text: str) -> list[float]:
    """Generate an embedding for a single text string."""
    return self.embed_batch([text])[0]


_service: EmbeddingService | None = None


def get_embedding_service() -> EmbeddingService:
  """Get the embedding service singleton."""
  global _service
  if _service is None:
    _service = EmbeddingService()
  return _service
