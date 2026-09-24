"""Graph engine interfaces, shared by core and middleware without an import cycle."""

from .engine import GraphEngineInterface, GraphOperation

__all__ = [
  "GraphEngineInterface",
  "GraphOperation",
]
