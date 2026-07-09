"""AI semantic memory — the operations kernel.

Transport-independent business logic for per-graph semantic memory (LanceDB).
REST routers and MCP tools are thin adapters over ``MemoryService``; the kernel
imports no FastAPI/MCP. Composes the platform ``EmbeddingService`` (local
fastembed, 384-dim, no credits) with a writer-routed ``GraphClient``.
"""

from .service import MemoryService, get_memory_service

__all__ = ["MemoryService", "get_memory_service"]
