"""Extensions GraphQL package.

Strawberry-based GraphQL schema served at /extensions/{graph_id}/graphql. Used for
read-heavy extensions queries (roboledger, roboinvestor) while mutations and
core platform concerns remain on the /v1/* REST surface.

The schema is assembled here and mounted onto the FastAPI app in main.py
behind the EXTENSIONS_GRAPHQL_ENABLED feature flag.
"""

from robosystems.graphql.schema import schema

__all__ = ["schema"]
