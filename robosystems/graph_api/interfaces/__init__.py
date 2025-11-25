"""
Shared interfaces for graph database engines.

This package contains abstract base classes and types that are shared
between the graph API core and middleware layers, preventing circular
dependencies.
"""

from .engine import GraphEngineInterface, GraphOperation

__all__ = [
  "GraphEngineInterface",
  "GraphOperation",
]
