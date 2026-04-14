"""Read-side roboinvestor operations.

Pure functions that take an open SQLAlchemy `Session` plus arguments and
return Pydantic response models (or `None`). Callers — whether a REST
router or a GraphQL resolver — own the session lifetime and translate
`None`/exceptions into transport errors.
"""
