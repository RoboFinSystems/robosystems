"""
Base interface for graph database engines.

This module re-exports interfaces from graph_api.interfaces for backward compatibility.
All new code should import from graph_api.interfaces directly.
"""

from robosystems.graph_api.interfaces import GraphEngineInterface, GraphOperation

__all__ = ["GraphEngineInterface", "GraphOperation"]
