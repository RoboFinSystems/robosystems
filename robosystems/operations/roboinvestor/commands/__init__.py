"""Write-side roboinvestor operations.

Pure functions that take an open SQLAlchemy `Session` plus validated
arguments (dicts or Pydantic request models) and return Pydantic
response models. Callers own the session lifetime and translate
domain exceptions into transport errors.
"""
