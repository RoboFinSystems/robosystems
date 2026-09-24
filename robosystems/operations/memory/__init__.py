"""Per-graph semantic memory; REST and MCP are thin adapters over ``MemoryService``."""

from .service import MemoryService, get_memory_service

__all__ = ["MemoryService", "get_memory_service"]
