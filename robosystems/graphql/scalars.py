"""Scalars the extensions schema adds to GraphQL's own."""

from __future__ import annotations

from typing import Any, NewType

import strawberry

# GraphQL's `Int` is 32 bits, and a value outside it fails the whole read.
# Cents pass it at $21,474,836.47.
BigInt = NewType("BigInt", int)

SCALAR_MAP: dict[object, Any] = {
  BigInt: strawberry.scalar(
    name="BigInt",
    description=(
      "A whole number of any size, sent as a JSON number: an amount in cents, "
      "or a count of shares. GraphQL's `Int` stops at 2,147,483,647."
    ),
    serialize=int,
    parse_value=int,
  ),
}
