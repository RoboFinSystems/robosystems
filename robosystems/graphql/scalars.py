"""Scalars the extensions schema adds to GraphQL's own."""

from __future__ import annotations

import operator
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
    # `operator.index` takes an integer and nothing else: `int()` would
    # truncate a float that reached here by mistake.
    serialize=operator.index,
    parse_value=operator.index,
  ),
}
