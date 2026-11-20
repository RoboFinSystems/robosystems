"""An amount in cents outgrows GraphQL's 32-bit `Int` at $21,474,836.47, and
an `Int` over its range fails the whole read. Money and share counts cross
the boundary as `BigInt`."""

from __future__ import annotations

import re

import pytest
import strawberry
from pydantic import BaseModel
from strawberry.schema.config import StrawberryConfig

from robosystems.graphql.scalars import SCALAR_MAP
from robosystems.graphql.schema import schema
from robosystems.graphql.types._pydantic import pydantic_type
from robosystems.models.api.common import Cents

pytestmark = pytest.mark.unit

OVER_32_BITS = 2_500_000_000  # $25,000,000.00

_FIELD = re.compile(r"^\s+(\w+)(?:\(.*\))?: \[?(\w+)!?\]?!?$")
_TYPE = re.compile(r"^type (\w+)")
# How a field holding money or a quantity of shares is named. A count, a
# depth or an order stays an `Int`.
_WIDE_NAME = re.compile(
  r"(Cents|Amount|Debit|Credit|Balance|Basis|Value|Shares)$|^amount$|^monthResult$"
)


def _fields() -> list[tuple[str, str, str]]:
  found = []
  current = None
  for line in schema.as_str().splitlines():
    if match := _TYPE.match(line):
      current = match.group(1)
    elif current and (match := _FIELD.match(line)):
      found.append((current, match.group(1), match.group(2)))
  return found


def test_no_money_field_is_a_32_bit_int():
  narrow = [
    f"{owner}.{name}"
    for owner, name, scalar in _fields()
    if scalar == "Int" and _WIDE_NAME.search(name)
  ]
  assert narrow == []


def test_the_schema_carries_the_fields_this_was_found_on():
  wide = {f"{owner}.{name}" for owner, name, scalar in _fields() if scalar == "BigInt"}
  assert {
    "Position.costBasis",
    "Position.currentValue",
    "OpenItem.openCents",
    "AgingBuckets.totalCents",
    "DraftLineItem.debitAmount",
    "PeriodDrafts.totalDebit",
    "EventBlock.amount",
    "Security.outstandingShares",
  } <= wide


class _Sample(BaseModel):
  """A sample."""

  amount: Cents
  maybe: Cents | None = None
  many: list[Cents] = []
  count: int = 0


@pydantic_type(model=_Sample, all_fields=True)
class _SampleType:
  pass


@strawberry.type
class _Query:
  @strawberry.field
  def sample(self) -> _SampleType:
    return _SampleType.from_pydantic(  # type: ignore[attr-defined]
      _Sample(amount=OVER_32_BITS, maybe=-OVER_32_BITS, many=[OVER_32_BITS], count=3)
    )


def test_a_cents_field_derives_as_bigint_and_reads_past_32_bits():
  sample = strawberry.Schema(
    query=_Query, config=StrawberryConfig(scalar_map=SCALAR_MAP)
  )
  sdl = sample.as_str()
  assert "amount: BigInt!" in sdl
  assert "maybe: BigInt\n" in sdl
  assert "many: [BigInt!]!" in sdl
  assert "count: Int!" in sdl

  result = sample.execute_sync("{ sample { amount maybe many count } }")

  assert result.errors is None
  assert result.data == {
    "sample": {
      "amount": OVER_32_BITS,
      "maybe": -OVER_32_BITS,
      "many": [OVER_32_BITS],
      "count": 3,
    }
  }


def test_rest_still_describes_cents_as_an_integer():
  properties = _Sample.model_json_schema()["properties"]
  assert properties["amount"]["type"] == "integer"


def test_a_value_that_is_not_a_whole_number_is_refused_not_truncated():
  serialize = next(iter(SCALAR_MAP.values())).serialize
  assert serialize(OVER_32_BITS) == OVER_32_BITS
  with pytest.raises(TypeError):
    serialize(1250.75)
