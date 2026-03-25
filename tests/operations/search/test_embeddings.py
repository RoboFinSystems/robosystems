"""Tests for the platform embedding service."""

from unittest.mock import MagicMock, patch

import numpy as np

from robosystems.operations.search.embeddings import (
  EMBEDDING_DIMENSIONS,
  EMBEDDING_MODEL_ID,
  EmbeddingService,
)


class TestEmbeddingService:
  def test_model_id(self):
    assert EMBEDDING_MODEL_ID == "fastembed"

  def test_dimensions(self):
    assert EMBEDDING_DIMENSIONS == 384

  @patch("robosystems.operations.search.embeddings.TextEmbedding", create=True)
  def test_embed_batch(self, mock_text_embedding_cls):
    mock_model = MagicMock()
    mock_model.embed.return_value = [
      np.zeros(384),
      np.ones(384),
    ]
    mock_text_embedding_cls.return_value = mock_model

    service = EmbeddingService()
    # Force fresh model load
    service._model = mock_model

    result = service.embed_batch(["hello", "world"])
    assert len(result) == 2
    assert len(result[0]) == 384
    assert result[0][0] == 0.0
    assert result[1][0] == 1.0

  def test_embed_batch_empty(self):
    service = EmbeddingService()
    result = service.embed_batch([])
    assert result == []

  @patch("robosystems.operations.search.embeddings.TextEmbedding", create=True)
  def test_embed_single(self, mock_text_embedding_cls):
    mock_model = MagicMock()
    mock_model.embed.return_value = [np.ones(384) * 0.5]
    mock_text_embedding_cls.return_value = mock_model

    service = EmbeddingService()
    service._model = mock_model

    result = service.embed_single("test text")
    assert len(result) == 384
    assert result[0] == 0.5

  def test_lazy_initialization(self):
    service = EmbeddingService()
    assert service._model is None
