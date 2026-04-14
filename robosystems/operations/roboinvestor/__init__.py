"""RoboInvestor operations layer.

Read and write operations for portfolios, securities, positions, and
holdings. Pure functions that take an open SQLAlchemy `Session` —
callers (REST routers, GraphQL resolvers) own session lifetime and
translate domain exceptions into transport errors.
"""
