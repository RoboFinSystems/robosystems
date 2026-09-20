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

from collections.abc import Iterator

import pytest

from robosystems.graphql.schema import _SNAKE_REFERENCE, _camel, schema

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
