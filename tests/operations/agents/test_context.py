"""
Test suite for context enricher and RAG functionality.

Tests semantic search, document retrieval, and context augmentation.
"""

from unittest.mock import AsyncMock, patch

import numpy as np
import pytest

from robosystems.operations.agents.context import (
  ContextEnricher,
  DocumentChunk,
  EmbeddingProvider,
  RAGConfig,
  SearchResult,
  VectorStore,
)


class TestRAGConfig:
  """Test RAG configuration."""

  def test_config_creation(self):
    """Test creating RAG configuration."""
    config = RAGConfig(
      enable_semantic_search=True,
      enable_entity_linking=True,
      enable_pattern_matching=True,
      embedding_provider=EmbeddingProvider.OPENAI,
      vector_store_type="ladybug",
      similarity_threshold=0.7,
      max_results=10,
      chunk_size=512,
      chunk_overlap=50,
    )

    assert config.enable_semantic_search is True
    assert config.embedding_provider == EmbeddingProvider.OPENAI
    assert config.similarity_threshold == 0.7
    assert config.chunk_size == 512

  def test_config_defaults(self):
    """Test default RAG configuration."""
    config = RAGConfig()

    assert config.enable_semantic_search is True
    assert config.enable_entity_linking is True
    assert config.enable_pattern_matching is False
    assert config.embedding_provider == EmbeddingProvider.LOCAL
    assert config.vector_store_type == "memory"
    assert config.similarity_threshold == 0.75
    assert config.max_results == 5


class TestEmbeddingProvider:
  """Test embedding provider enumeration."""

  def test_provider_values(self):
    """Test embedding provider values."""
    assert EmbeddingProvider.OPENAI.value == "openai"
    assert EmbeddingProvider.LOCAL.value == "local"
    assert EmbeddingProvider.SENTENCE_TRANSFORMERS.value == "sentence_transformers"
    assert EmbeddingProvider.CUSTOM.value == "custom"


class TestDocumentChunk:
  """Test document chunk structure."""

  def test_chunk_creation(self):
    """Test creating document chunk."""
    chunk = DocumentChunk(
      content="This is a test chunk",
      metadata={
        "source": "test_doc",
        "page": 1,
        "graph_id": "test_graph",
      },
      embedding=[0.1, 0.2, 0.3],
      chunk_id="chunk_001",
    )

    assert chunk.content == "This is a test chunk"
    assert chunk.metadata["source"] == "test_doc"
    assert len(chunk.embedding) == 3
    assert chunk.chunk_id == "chunk_001"

  def test_chunk_similarity(self):
    """Test chunk similarity calculation."""
    chunk1 = DocumentChunk(
      content="Test 1",
      embedding=np.array([1.0, 0.0, 0.0]),
    )
    chunk2 = DocumentChunk(
      content="Test 2",
      embedding=np.array([0.0, 1.0, 0.0]),
    )
    chunk3 = DocumentChunk(
      content="Test 3",
      embedding=np.array([1.0, 0.0, 0.0]),
    )

    # Same direction vectors have high similarity
    assert chunk1.similarity(chunk3) > 0.99

    # Orthogonal vectors have low similarity
    assert chunk1.similarity(chunk2) < 0.1


class TestSearchResult:
  """Test search result structure."""

  def test_result_creation(self):
    """Test creating search result."""
    chunk = DocumentChunk(content="Result content", embedding=[0.1, 0.2])

    result = SearchResult(
      chunk=chunk,
      score=0.85,
      relevance="high",
      explanation="Semantic match on key terms",
    )

    assert result.chunk.content == "Result content"
    assert result.score == 0.85
    assert result.relevance == "high"
    assert result.explanation and "Semantic match" in result.explanation


class MockVectorStore(VectorStore):
  """Mock vector store for testing."""

  def __init__(self):
    self.documents = []

  async def add_documents(self, chunks: list[DocumentChunk]):
    self.documents.extend(chunks)

  async def search(
    self, query_embedding: list[float], k: int = 5
  ) -> list[SearchResult]:
    # Simple mock search - return top k documents
    results = []
    for doc in self.documents[:k]:
      results.append(
        SearchResult(
          chunk=doc,
          score=0.8,
          relevance="medium",
        )
      )
    return results

  async def delete_documents(self, chunk_ids: list[str]):
    self.documents = [d for d in self.documents if d.chunk_id not in chunk_ids]


class TestContextEnricher:
  """Test context enricher functionality."""

  @pytest.fixture
  def mock_embedding_provider(self):
    """Create mock embedding provider."""
    with patch("robosystems.operations.agents.context.EmbeddingService") as mock:
      service = AsyncMock()
      service.embed_text = AsyncMock(return_value=[0.1, 0.2, 0.3, 0.4, 0.5])
      service.embed_batch = AsyncMock(return_value=[[0.1, 0.2, 0.3, 0.4, 0.5]] * 3)
      mock.return_value = service
      yield service

  @pytest.fixture
  def mock_vector_store(self):
    """Create mock vector store."""
    return MockVectorStore()

  @pytest.fixture
  def mock_lbug_client(self):
    """Create mock LadybugDB client."""
    with patch(
      "robosystems.middleware.mcp.create_graph_mcp_client", new_callable=AsyncMock
    ) as mock:
      client = AsyncMock()
      client.execute_query = AsyncMock(
        return_value=[
          {"content": "Doc 1", "metadata": {"type": "financial"}},
          {"content": "Doc 2", "metadata": {"type": "research"}},
        ]
      )
      mock.return_value = client
      yield client

  @pytest.fixture
  def enricher(self, mock_embedding_provider, mock_vector_store, mock_lbug_client):
    """Create context enricher instance."""
    config = RAGConfig(
      enable_semantic_search=True,
      enable_entity_linking=True,
      vector_store_type="memory",
    )
    enricher = ContextEnricher("test_graph", config=config)
    enricher.vector_store = mock_vector_store
    return enricher

  @pytest.mark.asyncio
  async def test_enricher_initialization(self):
    """Test context enricher initialization."""
    enricher = ContextEnricher("test_graph")

    assert enricher.graph_id == "test_graph"
    assert enricher.config is not None
    assert enricher.config.enable_semantic_search is True

  @pytest.mark.asyncio
  async def test_enrich_with_semantic_search(self, enricher, mock_embedding_provider):
    """Test context enrichment with semantic search."""
    # Add some documents to the store
    await enricher.vector_store.add_documents(
      [
        DocumentChunk(content="Financial report Q1", embedding=[0.1, 0.2]),
        DocumentChunk(content="Market analysis", embedding=[0.3, 0.4]),
      ]
    )

    context = await enricher.enrich(
      query="Show me financial data",
      context={"original": "value"},
    )

    assert "relevant_documents" in context
    assert len(context["relevant_documents"]) > 0
    assert context["original"] == "value"
    mock_embedding_provider.embed_text.assert_called()

  @pytest.mark.asyncio
  async def test_enrich_with_entity_linking(self, enricher):
    """Test context enrichment with entity linking."""
    with patch.object(enricher, "_extract_entities") as mock_extract:
      mock_extract.return_value = [
        {"entity": "Apple Inc.", "type": "ORG", "confidence": 0.95},
        {"entity": "Q1 2024", "type": "DATE", "confidence": 0.90},
      ]

      context = await enricher.enrich(
        query="Apple Inc. performance in Q1 2024",
        context={},
      )

      assert "linked_entities" in context
      assert len(context["linked_entities"]) == 2
      assert context["linked_entities"][0]["entity"] == "Apple Inc."

  @pytest.mark.asyncio
  async def test_enrich_with_pattern_matching(self, enricher):
    """Test context enrichment with pattern matching."""
    enricher.config.enable_pattern_matching = True

    with patch.object(enricher, "_find_patterns") as mock_patterns:
      mock_patterns.return_value = [
        {
          "pattern": "revenue growth",
          "matches": ["Q1: +15%", "Q2: +18%"],
          "trend": "increasing",
        }
      ]

      context = await enricher.enrich(
        query="What's the revenue growth trend?",
        context={},
      )

      assert "historical_patterns" in context
      assert context["historical_patterns"][0]["trend"] == "increasing"

  @pytest.mark.asyncio
  async def test_semantic_search(self, enricher):
    """Test semantic search functionality."""
    # Add test documents
    docs = [
      DocumentChunk(
        content="Financial analysis shows strong growth",
        embedding=[0.8, 0.2, 0.1],
        metadata={"type": "analysis"},
      ),
      DocumentChunk(
        content="Technical documentation for API",
        embedding=[0.1, 0.9, 0.2],
        metadata={"type": "docs"},
      ),
      DocumentChunk(
        content="Market trends indicate volatility",
        embedding=[0.7, 0.3, 0.2],
        metadata={"type": "market"},
      ),
    ]
    await enricher.vector_store.add_documents(docs)

    results = await enricher.semantic_search(
      query="Financial market analysis",
      k=2,
    )

    assert len(results) <= 2
    assert all(isinstance(r, SearchResult) for r in results)

  @pytest.mark.asyncio
  async def test_load_graph_documents(self, enricher, mock_lbug_client):
    """Test loading documents from graph database."""
    await enricher.load_graph_documents(
      node_types=["Document", "Report"],
      limit=100,
    )

    mock_lbug_client.execute_query.assert_called()
    # Documents should be added to vector store
    assert len(enricher.vector_store.documents) > 0

  @pytest.mark.asyncio
  async def test_entity_extraction(self, enricher):
    """Test entity extraction from text."""
    text = "Apple Inc. reported $100B revenue in Q1 2024 from iPhone sales."

    entities = await enricher._extract_entities(text)

    # Should extract company, money, date, and product entities
    entity_types = {e["type"] for e in entities}
    assert "ORG" in entity_types or "COMPANY" in entity_types
    assert "MONEY" in entity_types or "CURRENCY" in entity_types
    assert "DATE" in entity_types

  @pytest.mark.asyncio
  async def test_pattern_finding(self, enricher):
    """Test finding patterns in historical data."""
    patterns = await enricher._find_patterns(query="revenue growth over time")

    # Patterns should include trend information
    assert isinstance(patterns, list)
    if patterns:
      assert "pattern" in patterns[0]

  @pytest.mark.asyncio
  async def test_chunk_text(self, enricher):
    """Test text chunking functionality."""
    long_text = "This is a long text. " * 100  # Create long text

    chunks = enricher._chunk_text(
      long_text,
      chunk_size=50,
      chunk_overlap=10,
    )

    assert len(chunks) > 1
    # Check overlap exists
    for i in range(len(chunks) - 1):
      # Some characters should appear in consecutive chunks
      assert any(char in chunks[i + 1].content for char in chunks[i].content[-10:])

  @pytest.mark.asyncio
  async def test_rerank_results(self, enricher):
    """Test result reranking."""
    results = [
      SearchResult(
        chunk=DocumentChunk(content="Result 1", embedding=[]),
        score=0.7,
      ),
      SearchResult(
        chunk=DocumentChunk(content="Result 2", embedding=[]),
        score=0.8,
      ),
      SearchResult(
        chunk=DocumentChunk(content="Result 3", embedding=[]),
        score=0.6,
      ),
    ]

    reranked = enricher._rerank_results(
      results,
      query="test query",
      strategy="score",
    )

    # Should be sorted by score descending
    assert reranked[0].score == 0.8
    assert reranked[1].score == 0.7
    assert reranked[2].score == 0.6

  @pytest.mark.asyncio
  async def test_caching(self, enricher):
    """Test query result caching."""
    enricher.config.enable_caching = True
    enricher._cache = {}  # Initialize cache
    enricher._cache_timestamps = {}

    # First call
    await enricher.enrich(
      query="Cached query",
      context={},
    )

    # Second call with same query
    context2 = await enricher.enrich(
      query="Cached query",
      context={},
    )

    # Should return cached result
    assert context2.get("from_cache") is True

  @pytest.mark.asyncio
  async def test_parallel_enrichment(self, enricher):
    """Test parallel execution of enrichment tasks."""
    import time

    start_time = time.time()

    # All enrichment tasks should run in parallel
    context = await enricher.enrich(
      query="Complex query needing all enrichments",
      context={
        "enable_semantic_search": True,
        "enable_entity_linking": True,
        "enable_pattern_matching": True,
      },
    )

    elapsed = time.time() - start_time

    # Parallel execution should be faster than sequential
    assert elapsed < 2.0  # Assuming each task would take ~1s sequentially
    assert "relevant_documents" in context
    assert "linked_entities" in context

  @pytest.mark.asyncio
  async def test_graph_specific_context(self, enricher):
    """Test graph-specific context loading."""
    with patch.object(enricher, "_get_graph_metadata") as mock_metadata:
      mock_metadata.return_value = {
        "node_count": 1000,
        "relationship_count": 5000,
        "node_types": ["Entity", "Document", "Fact"],
      }

      context = await enricher.enrich(
        query="Tell me about this graph",
        context={"include_graph_metadata": True},
      )

      assert "graph_metadata" in context
      assert context["graph_metadata"]["node_count"] == 1000

  @pytest.mark.asyncio
  async def test_error_handling(self, enricher):
    """Test error handling in enrichment."""
    # Simulate vector store error
    with patch.object(
      enricher.vector_store,
      "search",
      side_effect=Exception("Vector store error"),
    ):
      context = await enricher.enrich(
        query="Test query",
        context={},
      )

      # Should handle error gracefully
      assert "enrichment_errors" in context
      assert "semantic_search" in context["enrichment_errors"]

  @pytest.mark.asyncio
  async def test_custom_embedding_function(self):
    """Test using custom embedding function."""

    async def custom_embed(text: str) -> list[float]:
      # Simple custom embedding based on text length
      return [len(text) / 100.0] * 5

    config = RAGConfig(
      embedding_provider=EmbeddingProvider.CUSTOM,
      custom_embedding_fn=custom_embed,
    )

    enricher = ContextEnricher("test_graph", config=config)

    embedding = await enricher._embed_text("Test text")

    assert len(embedding) == 5
    assert embedding[0] == len("Test text") / 100.0
