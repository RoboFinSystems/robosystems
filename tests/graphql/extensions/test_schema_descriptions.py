"""Every query field and argument reaches clients documented.

The descriptions are not written on the fields: `_describe_from_docstrings`
copies each resolver's docstring onto its field, and a Google-style `Args:`
block onto its arguments, as the schema is composed. That indirection is what
lets a contributor document a new read by writing an ordinary docstring — and
it is also what makes the gap easy to reopen silently, because a resolver with
no docstring still compiles, still serves, and simply ships blank.

A second pass, `_camelize_field_references`, rewrites `snake_case` field
references in those descriptions to the name the field actually has on the
wire. Most of this prose is Pydantic docstrings shared with REST, where
`balance_type` is correct — `auto_camel_case` renames the field but nothing
renames the sentence pointing at it, so the same text is right on `/docs/api`
and wrong on `/docs/extensions/graphql` unless it is translated.

These tests pin both invariants. They read the schema the way a client does,
through introspection, so they fail for a field that is documented in Python
but not on the wire, and for a description that names something the wire does
not have.
"""

from __future__ import annotations

import re
from collections.abc import Iterator

import pytest
from strawberry.exceptions import StrawberryGraphQLError

from robosystems.graphql.resolvers._common import validate_pagination
from robosystems.graphql.schema import (
  _SNAKE_REFERENCE,
  COMMON_ARGUMENT_DESCRIPTIONS,
  _camel,
  schema,
)

INTROSPECTION = """
{
  __schema {
    queryType {
      fields { name description args { name description } }
    }
  }
}
"""


FULL_INTROSPECTION = """
{
  __schema {
    types {
      name
      description
      fields(includeDeprecated: true) {
        name
        description
        args { name description }
      }
      inputFields { name description }
    }
  }
}
"""


def _published_descriptions(types: list[dict]) -> Iterator[tuple[str, str, set[str]]]:
  """Every description the schema publishes, with the names valid where it sits.

  A description is checked against the names a reader could use *at that
  location*: a type's prose against its own fields, a field's against its
  siblings and its arguments, an argument's against the other arguments of
  that field. Scoping it this way is what keeps the check precise — a word
  like `invoice_issued` is an event-type value, not a field, and must be left
  alone.
  """
  for gql_type in types:
    fields = gql_type.get("fields") or []
    inputs = gql_type.get("inputFields") or []
    field_names = {f["name"] for f in fields} | {f["name"] for f in inputs}
    yield gql_type["name"], gql_type.get("description") or "", field_names
    for field in inputs:
      yield (
        f"{gql_type['name']}.{field['name']}",
        field.get("description") or "",
        field_names,
      )
    for field in fields:
      arg_names = {a["name"] for a in (field.get("args") or [])}
      yield (
        f"{gql_type['name']}.{field['name']}",
        field.get("description") or "",
        field_names | arg_names,
      )
      for argument in field.get("args") or []:
        yield (
          f"{gql_type['name']}.{field['name']}({argument['name']})",
          argument.get("description") or "",
          arg_names,
        )


@pytest.fixture(scope="module")
def schema_types() -> list[dict]:
  result = schema.execute_sync(FULL_INTROSPECTION)
  assert result.errors is None, result.errors
  assert result.data is not None
  return [
    t
    for t in result.data["__schema"]["types"]
    if not t["name"].startswith("__") and (t.get("fields") or t.get("inputFields"))
  ]


@pytest.fixture(scope="module")
def query_fields() -> list[dict]:
  result = schema.execute_sync(INTROSPECTION)
  assert result.errors is None, result.errors
  assert result.data is not None
  return result.data["__schema"]["queryType"]["fields"]


class TestSchemaDescriptions:
  def test_the_schema_actually_has_fields(self, query_fields: list[dict]) -> None:
    """Guard the other assertions: an empty schema would pass them vacuously."""
    assert len(query_fields) > 20

  def test_every_query_field_is_described(self, query_fields: list[dict]) -> None:
    undocumented = sorted(f["name"] for f in query_fields if not f["description"])
    assert undocumented == [], (
      "Query fields reach clients with no description: "
      f"{undocumented}. Give the resolver a docstring — "
      "`_describe_from_docstrings` publishes it."
    )

  def test_every_argument_is_described(self, query_fields: list[dict]) -> None:
    undocumented = sorted(
      f"{field['name']}.{arg['name']}"
      for field in query_fields
      for arg in field["args"]
      if not arg["description"]
    )
    assert undocumented == [], (
      "Query arguments reach clients with no description: "
      f"{undocumented}. Add a Google-style `Args:` entry to the resolver's "
      "docstring, or add the name to COMMON_ARGUMENT_DESCRIPTIONS when it "
      "means the same thing everywhere."
    )

  def test_no_args_block_leaks_into_a_field_description(
    self, query_fields: list[dict]
  ) -> None:
    """The `Args:` block is stripped from the prose, not published twice.

    Arguments render as their own table wherever the schema is documented, so
    a leaked block is duplicated text in the reference.
    """
    leaked = sorted(
      f["name"]
      for f in query_fields
      if f["description"] and "Args:" in f["description"]
    )
    assert leaked == []

  def test_descriptions_are_markdown_not_rest(self, query_fields: list[dict]) -> None:
    """Double backticks are reStructuredText and nothing renders them.

    These descriptions are published to GraphiQL, MCP clients and the SDK
    snapshot, all of which treat the text as markdown — so ``foo`` reaches a
    reader as literal backticks around the word.
    """
    offenders = sorted(
      f["name"] for f in query_fields if f["description"] and "``" in f["description"]
    )
    assert offenders == [], (
      f"Descriptions carry reStructuredText markup: {offenders}. "
      "Use single backticks — every surface renders markdown."
    )

  def test_the_first_paragraph_is_a_summary(self, query_fields: list[dict]) -> None:
    """A field's opening paragraph is what a listing shows.

    References render it as the one-line summary beside the field name, so a
    whole argument delivered as the opening paragraph becomes a wall of text
    in a list of one-liners. Detail belongs after a blank line.
    """
    overlong = 200
    offenders = []
    for field in query_fields:
      description = field["description"]
      if not description:
        continue
      first = description.split("\n\n")[0].replace("\n", " ").strip()
      if len(first) > overlong:
        offenders.append((field["name"], len(first)))
    assert offenders == [], (
      f"Opening paragraphs read as detail rather than a summary: {sorted(offenders)}. "
      "Lead with one sentence and put the rest after a blank line."
    )


class TestDescriptionsAreWrittenForTheWire:
  """The schema's prose is checked the way a reader consumes it.

  `TestSchemaDescriptions` above covers the 66 query fields — the index into
  the schema. These cover everything the index points at: the types, their
  fields, and every argument. That gap is not hypothetical. When only the
  query fields were pinned, 34 types and 10 type fields shipped
  reStructuredText that renders as literal backticks, and 36 descriptions
  named a field by a Python name the wire does not answer to.
  """

  def test_the_sweep_is_not_vacuous(self, schema_types: list[dict]) -> None:
    """Guard the two assertions below against an empty or shallow walk."""
    described = [text for _, text, _ in _published_descriptions(schema_types) if text]
    assert len(schema_types) > 50
    assert len(described) > 200

  def test_no_description_carries_rest_markup(self, schema_types: list[dict]) -> None:
    """Double backticks are reStructuredText and nothing renders them.

    These descriptions reach GraphiQL, the published reference, the SDK
    snapshot and the `get-graphql-schema` MCP tool an agent reads. All four
    treat the text as markdown, so ``foo`` arrives as literal backticks
    around the word.
    """
    offenders = sorted(
      where for where, text, _ in _published_descriptions(schema_types) if "``" in text
    )
    assert offenders == [], (
      f"Descriptions carry reStructuredText markup: {offenders}. "
      "Use single backticks — every surface renders markdown."
    )

  def test_no_description_names_a_field_by_its_python_name(
    self, schema_types: list[dict]
  ) -> None:
    """A reader who copies a name out of the prose must get a valid query.

    Most of this text is Pydantic docstrings that also serve REST, where the
    snake_case name is the right one. `_camelize_field_references` translates
    it on the way onto this surface; without that pass, `Account` tells a
    GraphQL reader to ask for `balance_type`, which the schema rejects.
    """
    offenders = []
    for where, text, names in _published_descriptions(schema_types):
      for match in _SNAKE_REFERENCE.finditer(text):
        snake = match.group(1)
        if _camel(snake) in names and snake not in names:
          offenders.append(f"{where}: `{snake}` should be `{_camel(snake)}`")
    assert sorted(offenders) == [], (
      "Descriptions name fields by their Python name, which the wire does "
      f"not answer to: {sorted(offenders)}."
    )


class TestThePublishedBoundsAreTheEnforcedOnes:
  """The pagination text is a promise the validator has to keep.

  `COMMON_ARGUMENT_DESCRIPTIONS` reaches GraphiQL, the published reference, the
  SDK snapshot and the `get-graphql-schema` MCP tool, on all 16 fields that take
  a `limit`. It used to spell "1-1000" out by hand, which would have gone on
  asserting itself on every one of them after someone edited `_MAX_LIMIT`.

  These tests check the published numbers against the guard's *behaviour* rather
  than against its constants, so they would still fail if the interpolation were
  reverted to a literal that no longer matched.
  """

  @staticmethod
  def _numbers(text: str) -> list[int]:
    return [int(n) for n in re.findall(r"\d+", text)]

  def test_the_published_limit_range_is_the_accepted_range(self) -> None:
    low, high = self._numbers(COMMON_ARGUMENT_DESCRIPTIONS["limit"])[:2]

    validate_pagination(low, 0)
    validate_pagination(high, 0)

    for rejected in (low - 1, high + 1):
      with pytest.raises(StrawberryGraphQLError) as caught:
        validate_pagination(rejected, 0)
      assert caught.value.extensions["code"] == "INVALID_PAGINATION"

  def test_the_published_offset_floor_is_the_accepted_floor(self) -> None:
    floor = self._numbers(COMMON_ARGUMENT_DESCRIPTIONS["offset"])[0]

    validate_pagination(1, floor)

    with pytest.raises(StrawberryGraphQLError) as caught:
      validate_pagination(1, floor - 1)
    assert caught.value.extensions["code"] == "INVALID_PAGINATION"

  def test_every_paginated_field_publishes_that_same_promise(
    self, query_fields: list[dict]
  ) -> None:
    """One wording, so a reader cannot find two answers on two fields."""
    published = {
      arg["description"]
      for field in query_fields
      for arg in field["args"]
      if arg["name"] == "limit"
    }
    assert published == {COMMON_ARGUMENT_DESCRIPTIONS["limit"]}


# Types whose fields are not yet documented, as of the day this guard landed.
#
# 579 fields across these 83 types, and the tail is long — the ten largest are
# only 31% of it — so there is no subset worth sweeping. They are paid down by
# whoever is already working in that domain and can write something true, rather
# than in one pass that would produce "The id of the account" 579 times. A
# vacuous description is worse than a blank one: it reads as done, so nobody
# comes back to it, and it still reaches GraphiQL, the reference, the SDK
# snapshot and the MCP schema tool.
#
# The list only shrinks. Documenting a type means deleting its line; a type not
# on the list has to be fully documented, so the gap cannot grow and a new type
# can never ship blank.
UNDOCUMENTED_TYPES: frozenset[str] = frozenset(
  {
    "Account",
    "AccountList",
    "AccountRollupGroup",
    "AccountRollupRow",
    "AccountRollups",
    "AccountTree",
    "AccountTreeNode",
    "Agent",
    "Artifact",
    "Association",
    "ChartTemplate",
    "CloseReceipt",
    "ClosingBookCategory",
    "ClosingBookItem",
    "ClosingBookStructures",
    "DraftEntry",
    "DraftLineItem",
    "Element",
    "ElementList",
    "EventBlock",
    "FiscalCalendar",
    "FiscalPeriodSummary",
    "InformationBlock",
    "InformationBlockChart",
    "InformationBlockChartPanel",
    "InformationBlockChartSeries",
    "InformationBlockConnection",
    "InformationBlockElement",
    "InformationBlockFact",
    "InformationBlockFactSet",
    "InformationBlockRendering",
    "InformationBlockRenderingPeriod",
    "InformationBlockRenderingRow",
    "InformationBlockRule",
    "InformationBlockValidation",
    "InformationBlockVerificationCategorySummary",
    "InformationBlockVerificationResult",
    "InformationBlockVerificationSummary",
    "InformationBlockViewProjections",
    "LedgerEntity",
    "LedgerEntry",
    "LedgerJournalEntry",
    "LedgerJournalEntryList",
    "LedgerLineItem",
    "LedgerSummary",
    "LedgerTransactionDetail",
    "LedgerTransactionList",
    "LedgerTransactionSummary",
    "LibraryAssociation",
    "LibraryElement",
    "LibraryElementArc",
    "LibraryElementTreeNode",
    "LibraryEquivalence",
    "LibraryStructure",
    "LibraryTaxonomy",
    "MappedTrialBalance",
    "MappedTrialBalanceRow",
    "MappingCoverage",
    "MappingDetail",
    "OpenBalanceByAgent",
    "PendingObligationDetail",
    "PeriodCloseItem",
    "PeriodCloseStatus",
    "PeriodDrafts",
    "PublishListList",
    "ReportPackage",
    "ReportPackageItem",
    "Security",
    "SecurityList",
    "Structure",
    "StructureList",
    "SuggestedTarget",
    "Taxonomy",
    "TaxonomyBlock",
    "TaxonomyBlockAssociation",
    "TaxonomyBlockElement",
    "TaxonomyBlockRule",
    "TaxonomyBlockStructure",
    "TaxonomyList",
    "TrialBalance",
    "TrialBalanceRow",
    "UnmappedElement",
    "UnreachableMappingType",
  }
)


class TestTheDocumentationGapOnlyShrinks:
  """A ratchet over the return shapes, not a demand that they all be written.

  `TestSchemaDescriptions` covers the 66 query fields — the index into the
  schema, which is complete. This covers the types that index points at, which
  are at 36%. Closing that is editorial work spread over whoever owns each
  domain, so the guard here is directional: it refuses new gaps and records
  progress, instead of failing until someone writes 579 descriptions.
  """

  def test_a_type_not_on_the_list_is_fully_documented(
    self, schema_types: list[dict]
  ) -> None:
    regressed = sorted(
      gql_type["name"]
      for gql_type in schema_types
      if gql_type["name"] not in UNDOCUMENTED_TYPES
      and any(not f.get("description") for f in (gql_type.get("fields") or []))
    )
    assert regressed == [], (
      f"These types have undocumented fields and are not on the allowlist: "
      f"{regressed}. Give each field a description — on a Pydantic-derived "
      "type that is `Field(description=...)`, which documents the REST schema "
      "too; on a hand-written Strawberry type it is "
      "`strawberry.field(description=...)`."
    )

  def test_the_list_carries_no_type_that_is_already_documented(
    self, schema_types: list[dict]
  ) -> None:
    """Deleting the line is how progress is recorded, so stale entries fail."""
    by_name = {t["name"]: t for t in schema_types}
    done = sorted(
      name
      for name in UNDOCUMENTED_TYPES
      if name in by_name
      and all(f.get("description") for f in (by_name[name].get("fields") or []))
    )
    assert done == [], (
      f"These types are fully documented and can come off the allowlist: {done}. "
      "Delete their lines — the list is the record of what is left."
    )

  def test_the_list_carries_no_type_the_schema_no_longer_has(
    self, schema_types: list[dict]
  ) -> None:
    """A renamed or deleted type must not leave a hole the ratchet ignores."""
    present = {t["name"] for t in schema_types}
    stale = sorted(name for name in UNDOCUMENTED_TYPES if name not in present)
    assert stale == [], (
      f"The allowlist names types this schema does not have: {stale}. "
      "Remove them, or the ratchet silently exempts nothing."
    )
