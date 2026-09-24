"""Extensions GraphQL package: the read surface at /extensions/{graph_id}/graphql,
mounted in main.py behind EXTENSIONS_GRAPHQL_ENABLED.
"""

from robosystems.graphql.schema import schema

__all__ = ["schema"]
