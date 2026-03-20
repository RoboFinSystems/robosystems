"""Full-text search operations using OpenSearch.

Platform capability for searching unstructured text content (filing narratives,
text blocks, documents) scoped by graph_id for multi-tenant isolation.
"""

from .client import OpenSearchClient
from .service import SearchService, get_search_service

__all__ = ["OpenSearchClient", "SearchService", "get_search_service"]
