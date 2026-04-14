"""Strawberry types for ledger (roboledger) extensions queries.

Each type wraps an existing Pydantic response model from
`robosystems.models.api.extensions.*` so the GraphQL schema is derived from
the REST surface. When a Pydantic field is added, the GraphQL field appears
automatically.

Strawberry's default `auto_camel_case=True` converts Python snake_case
fields to camelCase on the wire, so `legal_name` becomes `legalName` with
no manual reshape.
"""

from __future__ import annotations

import strawberry

from robosystems.models.api.extensions.entity import LedgerEntityResponse


@strawberry.experimental.pydantic.type(model=LedgerEntityResponse, all_fields=True)
class LedgerEntity:
  """The top-level ledger entity (company/organization) for a graph."""
