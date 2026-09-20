"""Every query field and argument reaches clients documented.

The descriptions are not written on the fields: `_describe_from_docstrings`
copies each resolver's docstring onto its field, and a Google-style `Args:`
block onto its arguments, as the schema is composed. That indirection is what
lets a contributor document a new read by writing an ordinary docstring — and
it is also what makes the gap easy to reopen silently, because a resolver with
no docstring still compiles, still serves, and simply ships blank.

These tests pin the invariant the shim exists for. They read the schema the way
a client does, through introspection, so they fail for a field that is
documented in Python but not on the wire.
"""

from __future__ import annotations

import pytest

from robosystems.graphql.schema import schema

INTROSPECTION = """
{
  __schema {
    queryType {
      fields { name description args { name description } }
    }
  }
}
"""


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
