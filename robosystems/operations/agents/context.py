"""
Context enricher with RAG (Retrieval-Augmented Generation) capabilities.

Provides semantic search, document retrieval, and context augmentation.
"""

import asyncio
import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any

import numpy as np

from robosystems.logger import logger


class EmbeddingProvider(Enum):
  """Available embedding providers."""

  OPENAI = "openai"
  LOCAL = "local"
  SENTENCE_TRANSFORMERS = "sentence_transformers"
  CUSTOM = "custom"


@dataclass
class RAGConfig:
  """Configuration for RAG functionality."""

  enable_semantic_search: bool = True
  enable_entity_linking: bool = True
  enable_pattern_matching: bool = False
  embedding_provider: EmbeddingProvider = EmbeddingProvider.LOCAL
  vector_store_type: str = "memory"
  similarity_threshold: float = 0.75
  max_results: int = 5
  chunk_size: int = 512
  chunk_overlap: int = 50
  enable_caching: bool = False
  cache_ttl: int = 3600  # seconds
  custom_embedding_fn: Callable | None = None


@dataclass
class DocumentChunk:
  """Represents a chunk of document for indexing."""

  content: str
  metadata: dict[str, Any] = field(default_factory=dict)
  embedding: list[float] | None = None
  chunk_id: str | None = None
  timestamp: datetime = field(default_factory=datetime.utcnow)

  def similarity(self, other: "DocumentChunk") -> float:
    """Calculate cosine similarity with another chunk."""
    if self.embedding is None or other.embedding is None:
      return 0.0

    # Convert to numpy arrays
    a = np.array(self.embedding)
    b = np.array(other.embedding)

    # Cosine similarity
    cos_sim = np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b))
    return float(cos_sim)


@dataclass
class SearchResult:
  """Result from semantic search."""

  chunk: DocumentChunk
  score: float
  relevance: str = "medium"
  explanation: str | None = None


class VectorStore:
  """Abstract base for vector stores."""

  async def add_documents(self, chunks: list[DocumentChunk]):
    """Add document chunks to the store."""
    raise NotImplementedError

  async def search(
    self, query_embedding: list[float], k: int = 5
  ) -> list[SearchResult]:
    """Search for similar documents."""
    raise NotImplementedError

  async def delete_documents(self, chunk_ids: list[str]):
    """Delete documents by chunk IDs."""
    raise NotImplementedError


class MemoryVectorStore(VectorStore):
  """In-memory vector store implementation."""

  def __init__(self):
    self.documents: list[DocumentChunk] = []

  async def add_documents(self, chunks: list[DocumentChunk]):
    """Add documents to memory store."""
    for chunk in chunks:
      if chunk.chunk_id is None:
        chunk.chunk_id = self._generate_chunk_id(chunk.content)
    self.documents.extend(chunks)

  async def search(
    self, query_embedding: list[float], k: int = 5
  ) -> list[SearchResult]:
    """Search for similar documents in memory."""
    if not self.documents:
      return []

    # Calculate similarities
    query_chunk = DocumentChunk(content="", embedding=query_embedding)
    similarities = []

    for doc in self.documents:
      if doc.embedding:
        score = query_chunk.similarity(doc)
        similarities.append((doc, score))

    # Sort by score and take top k
    similarities.sort(key=lambda x: x[1], reverse=True)
    top_k = similarities[:k]

    # Convert to search results
    results = []
    for doc, score in top_k:
      relevance = "high" if score > 0.85 else "medium" if score > 0.7 else "low"
      results.append(
        SearchResult(
          chunk=doc,
          score=score,
          relevance=relevance,
        )
      )

    return results

  async def delete_documents(self, chunk_ids: list[str]):
    """Delete documents from memory store."""
    self.documents = [doc for doc in self.documents if doc.chunk_id not in chunk_ids]

  def _generate_chunk_id(self, content: str) -> str:
    """Generate a unique ID for a chunk."""
    return hashlib.md5(content.encode()).hexdigest()


class EmbeddingService:
  """Service for generating embeddings."""

  def __init__(self, provider: EmbeddingProvider, **kwargs):
    self.provider = provider
    self.config = kwargs

  async def embed_text(self, text: str) -> list[float]:
    """Generate embedding for text."""
    if self.provider == EmbeddingProvider.LOCAL:
      return await self._local_embedding(text)
    elif self.provider == EmbeddingProvider.CUSTOM:
      if "custom_fn" in self.config:
        return await self.config["custom_fn"](text)
      raise ValueError("Custom embedding function not provided")
    else:
      # Placeholder for other providers
      return await self._local_embedding(text)

  async def embed_batch(self, texts: list[str]) -> list[list[float]]:
    """Generate embeddings for multiple texts."""
    tasks = [self.embed_text(text) for text in texts]
    return await asyncio.gather(*tasks)

  async def _local_embedding(self, text: str) -> list[float]:
    """Generate embeddings using a hybrid approach.

    Uses TF-IDF-like features combined with semantic features
    for better representation without external dependencies.
    """
    import hashlib
    import math

    # Clean and prepare text
    text_lower = text.lower().strip()
    words = text_lower.split()
    word_count = len(words)
    char_count = len(text)

    # Initialize feature vector
    features = []

    # 1. Length features (normalized)
    features.append(min(char_count / 1000.0, 1.0))  # Character density
    features.append(min(word_count / 100.0, 1.0))  # Word density
    features.append(math.log1p(word_count) / 10.0)  # Log word count

    # 2. Semantic features
    # Financial keywords (domain-specific)
    financial_terms = [
      "revenue",
      "income",
      "asset",
      "liability",
      "equity",
      "profit",
      "loss",
      "cash",
      "debt",
      "margin",
    ]
    financial_score = sum(1 for term in financial_terms if term in text_lower)
    features.append(financial_score / 10.0)

    # Entity indicators
    has_numbers = any(char.isdigit() for char in text)
    has_currency = any(sym in text for sym in ["$", "€", "£", "¥"])
    has_percent = "%" in text
    features.append(1.0 if has_numbers else 0.0)
    features.append(1.0 if has_currency else 0.0)
    features.append(1.0 if has_percent else 0.0)

    # 3. Statistical features
    # Average word length
    avg_word_len = sum(len(word) for word in words) / max(word_count, 1)
    features.append(avg_word_len / 10.0)

    # Vocabulary richness (unique words ratio)
    unique_words = len(set(words))
    vocab_richness = unique_words / max(word_count, 1)
    features.append(vocab_richness)

    # 4. Hash-based features for consistency
    # Create deterministic pseudo-random features from text hash
    text_hash = hashlib.md5(text_lower.encode()).digest()
    for i in range(6):
      # Convert hash bytes to normalized floats
      byte_val = text_hash[i] if i < len(text_hash) else 0
      features.append(byte_val / 255.0)

    # 5. Punctuation and structure features
    sentence_count = text.count(".") + text.count("!") + text.count("?")
    features.append(min(sentence_count / 10.0, 1.0))

    # Question indicator
    is_question = text.strip().endswith("?")
    features.append(1.0 if is_question else 0.0)

    # 6. N-gram features (simplified bigram presence)
    common_bigrams = ["what is", "how to", "show me", "tell me", "find the"]
    bigram_score = sum(1 for bigram in common_bigrams if bigram in text_lower)
    features.append(bigram_score / 5.0)

    # Ensure fixed dimensionality (384 to match sentence-transformers)
    # Pad with deterministic values based on text hash
    target_dim = 384
    while len(features) < target_dim:
      # Use hash to generate padding values
      idx = len(features)
      hash_val = hashlib.md5(f"{text_lower}{idx}".encode()).digest()[0]
      features.append(hash_val / 510.0)  # Normalize to [0, 0.5]

    return features[:target_dim]


class EntityExtractor:
  """Extract entities from text."""

  async def extract(self, text: str) -> list[dict[str, Any]]:
    """Extract named entities from text."""
    # Simplified entity extraction
    entities = []

    # Look for common patterns
    import re

    # Company names (simplified)
    company_pattern = r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\s+(?:Inc|Corp|LLC|Ltd)\.?)\b"
    for match in re.finditer(company_pattern, text):
      entities.append(
        {
          "entity": match.group(1),
          "type": "ORG",
          "confidence": 0.8,
          "start": match.start(),
          "end": match.end(),
        }
      )

    # Dates (simplified)
    date_pattern = r"\b(Q[1-4]\s+\d{4}|\d{4})\b"
    for match in re.finditer(date_pattern, text):
      entities.append(
        {
          "entity": match.group(1),
          "type": "DATE",
          "confidence": 0.9,
          "start": match.start(),
          "end": match.end(),
        }
      )

    # Money amounts (simplified)
    money_pattern = r"\$[\d,]+(?:\.\d+)?[BMK]?\b"
    for match in re.finditer(money_pattern, text):
      entities.append(
        {
          "entity": match.group(0),
          "type": "MONEY",
          "confidence": 0.95,
          "start": match.start(),
          "end": match.end(),
        }
      )

    return entities


class PatternMatcher:
  """Find patterns in historical data."""

  async def find_patterns(
    self, query: str, historical_data: list[dict] | None = None
  ) -> list[dict[str, Any]]:
    """Find patterns related to the query."""
    patterns = []

    # Simplified pattern matching
    query_lower = query.lower()

    if "trend" in query_lower or "growth" in query_lower:
      patterns.append(
        {
          "pattern": "growth_trend",
          "description": "Historical growth pattern detected",
          "confidence": 0.7,
          "data_points": 12,
        }
      )

    if "seasonal" in query_lower:
      patterns.append(
        {
          "pattern": "seasonality",
          "description": "Seasonal variations detected",
          "confidence": 0.6,
          "period": "quarterly",
        }
      )

    return patterns


class ContextEnricher:
  """
  Enriches query context with RAG and semantic search.

  Provides document retrieval, entity extraction, and pattern matching.
  """

  def __init__(
    self,
    graph_id: str,
    config: RAGConfig | None = None,
  ):
    """
    Initialize context enricher.

    Args:
        graph_id: Graph database identifier
        config: RAG configuration
    """
    self.graph_id = graph_id
    self.config = config or RAGConfig()
    self.logger = logger

    # Initialize components
    self.embedding_service = self._init_embedding_service()
    self.vector_store = self._init_vector_store()
    self.entity_extractor = EntityExtractor()
    self.pattern_matcher = PatternMatcher()

    # Cache
    self._cache = {} if self.config.enable_caching else None
    self._cache_timestamps = {}

  def _init_embedding_service(self) -> EmbeddingService:
    """Initialize the embedding service."""
    if self.config.custom_embedding_fn:
      return EmbeddingService(
        EmbeddingProvider.CUSTOM,
        custom_fn=self.config.custom_embedding_fn,
      )
    return EmbeddingService(self.config.embedding_provider)

  def _init_vector_store(self) -> VectorStore:
    """Initialize the vector store based on configuration."""
    # For now, use memory store until subgraph implementation is ready
    # Future: Use env.AGENT_MEMORY_BACKEND to select implementation
    return MemoryVectorStore()

  async def enrich(
    self,
    query: str,
    context: dict[str, Any] | None = None,
  ) -> dict[str, Any]:
    """
    Enrich context with relevant information.

    Args:
        query: The user's query
        context: Initial context

    Returns:
        Enriched context dictionary
    """
    enriched = context.copy() if context else {}

    # Check cache
    if self._cache is not None:
      cache_key = self._get_cache_key(query)
      if cache_key in self._cache:
        if self._is_cache_valid(cache_key):
          cached = self._cache[cache_key].copy()
          cached["from_cache"] = True
          return cached

    # Parallel enrichment tasks
    tasks = []
    task_names = []

    if self.config.enable_semantic_search and enriched.get(
      "enable_semantic_search", True
    ):
      tasks.append(self.semantic_search(query, self.config.max_results))
      task_names.append("semantic_search")

    if self.config.enable_entity_linking and enriched.get(
      "enable_entity_linking", True
    ):
      tasks.append(self._extract_entities(query))
      task_names.append("entity_linking")

    if self.config.enable_pattern_matching or enriched.get(
      "enable_pattern_matching", False
    ):
      tasks.append(self._find_patterns(query))
      task_names.append("pattern_matching")

    if enriched.get("include_graph_metadata", False):
      tasks.append(self._get_graph_metadata())
      task_names.append("graph_metadata")

    # Execute enrichment tasks
    enriched["enrichment_errors"] = {}

    if tasks:
      results = await asyncio.gather(*tasks, return_exceptions=True)

      for task_name, result in zip(task_names, results, strict=False):
        if isinstance(result, Exception):
          self.logger.error(f"Enrichment task '{task_name}' failed: {result!s}")
          enriched["enrichment_errors"][task_name] = str(result)
        else:
          if task_name == "semantic_search":
            enriched["relevant_documents"] = [
              {
                "content": r.chunk.content,
                "score": r.score,
                "metadata": r.chunk.metadata,
              }
              for r in result
            ]
          elif task_name == "entity_linking":
            enriched["linked_entities"] = result
          elif task_name == "pattern_matching":
            enriched["historical_patterns"] = result
          elif task_name == "graph_metadata":
            enriched["graph_metadata"] = result

    # Cache result
    if self._cache is not None:
      cache_key = self._get_cache_key(query)
      self._cache[cache_key] = enriched.copy()
      self._cache_timestamps[cache_key] = datetime.utcnow()

    return enriched

  async def semantic_search(self, query: str, k: int = 5) -> list[SearchResult]:
    """
    Perform semantic search for relevant documents.

    Args:
        query: Search query
        k: Number of results to return

    Returns:
        List of search results
    """
    try:
      # Generate query embedding
      query_embedding = await self._embed_text(query)

      # Search vector store
      results = await self.vector_store.search(query_embedding, k)

      # Filter by similarity threshold
      filtered = [r for r in results if r.score >= self.config.similarity_threshold]

      # Rerank if needed
      if len(filtered) > 1:
        filtered = self._rerank_results(filtered, query)

      return filtered

    except Exception as e:
      self.logger.error(f"Semantic search failed: {e!s}")
      raise  # Re-raise to be caught by gather

  async def load_graph_documents(
    self,
    node_types: list[str] | None = None,
    limit: int = 1000,
  ):
    """
    Load documents from graph database.

    Args:
        node_types: Types of nodes to load
        limit: Maximum number of documents
    """
    try:
      from robosystems.middleware.mcp import create_graph_mcp_client

      client = await create_graph_mcp_client(graph_id=self.graph_id)

      # Query for documents with parameterization
      if node_types:
        # Use parameterized query for safety
        query = "MATCH (n) WHERE n.type IN $node_types RETURN n LIMIT $limit"
        params = {"node_types": node_types, "limit": limit}
      else:
        query = "MATCH (n) RETURN n LIMIT $limit"
        params = {"limit": limit}

      results = await client.execute_query(query, params)

      # Convert to document chunks
      chunks = []
      for result in results:
        content = json.dumps(result) if isinstance(result, dict) else str(result)
        chunk = await self._create_chunk(content, {"source": "graph"})
        chunks.append(chunk)

      # Add to vector store
      await self.vector_store.add_documents(chunks)

      self.logger.info(f"Loaded {len(chunks)} documents from graph")

    except Exception as e:
      self.logger.error(f"Failed to load graph documents: {e!s}")

  async def _extract_entities(self, text: str) -> list[dict[str, Any]]:
    """Extract entities from text."""
    try:
      return await self.entity_extractor.extract(text)
    except Exception as e:
      self.logger.error(f"Entity extraction failed: {e!s}")
      return []

  async def _find_patterns(self, query: str) -> list[dict[str, Any]]:
    """Find patterns in historical data."""
    try:
      return await self.pattern_matcher.find_patterns(query)
    except Exception as e:
      self.logger.error(f"Pattern matching failed: {e!s}")
      return []

  async def _get_graph_metadata(self) -> dict[str, Any]:
    """Get metadata about the graph."""
    try:
      from robosystems.middleware.mcp import create_graph_mcp_client

      client = await create_graph_mcp_client(graph_id=self.graph_id)

      # Get basic statistics
      node_count = await client.execute_query("MATCH (n) RETURN count(n) as count")
      rel_count = await client.execute_query(
        "MATCH ()-[r]->() RETURN count(r) as count"
      )

      return {
        "node_count": node_count[0]["count"] if node_count else 0,
        "relationship_count": rel_count[0]["count"] if rel_count else 0,
        "graph_id": self.graph_id,
      }
    except Exception as e:
      self.logger.error(f"Failed to get graph metadata: {e!s}")
      return {}

  async def _embed_text(self, text: str) -> list[float]:
    """Generate embedding for text."""
    return await self.embedding_service.embed_text(text)

  async def _create_chunk(
    self, content: str, metadata: dict[str, Any] | None = None
  ) -> DocumentChunk:
    """Create a document chunk with embedding."""
    embedding = await self._embed_text(content)
    return DocumentChunk(
      content=content,
      metadata=metadata or {},
      embedding=embedding,
    )

  def _chunk_text(
    self, text: str, chunk_size: int | None = None, chunk_overlap: int | None = None
  ) -> list[DocumentChunk]:
    """Split text into chunks."""
    chunk_size = chunk_size or self.config.chunk_size
    chunk_overlap = chunk_overlap or self.config.chunk_overlap

    chunks = []
    start = 0

    while start < len(text):
      end = min(start + chunk_size, len(text))
      chunk_text = text[start:end]

      chunks.append(
        DocumentChunk(
          content=chunk_text,
          metadata={
            "start": start,
            "end": end,
          },
        )
      )

      start += chunk_size - chunk_overlap

    return chunks

  def _rerank_results(
    self,
    results: list[SearchResult],
    query: str,
    strategy: str = "score",
  ) -> list[SearchResult]:
    """Rerank search results."""
    if strategy == "score":
      # Already sorted by score
      return sorted(results, key=lambda r: r.score, reverse=True)

    # Add other reranking strategies as needed
    return results

  def _get_cache_key(self, query: str) -> str:
    """Generate cache key for a query."""
    return hashlib.md5(f"{self.graph_id}:{query}".encode()).hexdigest()

  def _is_cache_valid(self, cache_key: str) -> bool:
    """Check if cached result is still valid."""
    if cache_key not in self._cache_timestamps:
      return False

    timestamp = self._cache_timestamps[cache_key]
    age = (datetime.utcnow() - timestamp).total_seconds()

    return age < self.config.cache_ttl
