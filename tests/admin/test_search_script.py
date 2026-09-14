"""Tests for the OpenSearch admin script template.

The script that runs on the bastion is a **string** built by ``str.format`` and
shipped over SSM, so neither ruff nor basedpyright ever sees it: a brace-escaping
slip or a wrong filter field fails for the first time in production, mid-delete.
These tests render the template the way ``_run_opensearch_script`` does and hold
the invariants the toolchain cannot.
"""

import ast
import textwrap

import pytest

from robosystems.admin.commands.search import _OPENSEARCH_QUERY_SCRIPT

ACTIONS = ["count", "query", "delete", "force-merge"]


def render(**overrides) -> str:
  """The script exactly as the dispatcher builds it."""
  params = {
    "host": "example.us-east-1.es.amazonaws.com",
    "region": "us-east-1",
    "index": "documents",
    "action": "count",
    "graph_id": "sec",
    "size": 10,
    "source_type": "",
    "before_date": "",
    "indexed_before": "",
    "dry_run": "true",
  }
  params.update(overrides)
  return _OPENSEARCH_QUERY_SCRIPT.format(**params)


def delete_filters(**overrides) -> list[dict]:
  """The filter list the delete branch builds, evaluated in isolation.

  Runs the real source lines rather than a restatement of them, so a change to
  the template's filter logic is caught instead of silently diverging from a
  copy kept in the test.
  """
  script = render(action="delete", **overrides)

  # Seed the module-level literals the branch reads (graph_id, source_type, ...).
  namespace: dict = {
    node.targets[0].id: node.value.value
    for node in ast.parse(script).body
    if isinstance(node, ast.Assign)
    and isinstance(node.targets[0], ast.Name)
    and isinstance(node.value, ast.Constant)
  }

  body = script.split('elif action == "delete":', 1)[1].split("elif action ==", 1)[0]
  lines = []
  for line in body.splitlines():
    if line.strip().startswith("query ="):
      break
    lines.append(line)

  # own source — running the real filter logic is the point, since a restatement
  # of it in the test would drift from the template without failing.
  exec(textwrap.dedent("\n".join(lines)), namespace)  # noqa: S102
  return namespace["filters"]


@pytest.mark.unit
@pytest.mark.parametrize("action", ACTIONS)
def test_template_renders_valid_python(action):
  """Every action's rendered script parses — catches brace-escaping slips."""
  ast.parse(render(action=action))


@pytest.mark.unit
def test_every_parameter_reaches_the_script_as_a_literal():
  """Each format key lands in a module-level assignment, not a stray placeholder.

  Absence checks on brace patterns would be wrong here: the generated script
  contains its own f-strings, so ``{host}`` legitimately appears inside them.
  Assert on the assignments instead.
  """
  script = render(
    action="delete",
    source_type="ixbrl_disclosure",
    before_date="2025-01-01",
    indexed_before="2026-09-13T18:00:00Z",
  )
  assignments = {
    node.targets[0].id: node.value.value
    for node in ast.parse(script).body
    if isinstance(node, ast.Assign)
    and isinstance(node.targets[0], ast.Name)
    and isinstance(node.value, ast.Constant)
  }
  assert assignments["host"] == "example.us-east-1.es.amazonaws.com"
  assert assignments["index_name"] == "documents"
  assert assignments["action"] == "delete"
  assert assignments["graph_id"] == "sec"
  assert assignments["source_type"] == "ixbrl_disclosure"
  assert assignments["before_date"] == "2025-01-01"
  assert assignments["indexed_before"] == "2026-09-13T18:00:00Z"


@pytest.mark.unit
def test_delete_always_filters_on_graph_id():
  """The index is shared with every tenant's uploads — this filter is load-bearing.

  A delete that loses it is not a bug, it is data loss, so assert it holds even
  when every optional narrowing filter is absent.
  """
  filters = delete_filters(source_type="", before_date="", indexed_before="")
  assert filters == [{"term": {"graph_id": "sec"}}]


@pytest.mark.unit
def test_delete_filters_indexed_before_on_indexed_at_not_filing_date():
  """The orphan sweep keys on when we last wrote the doc, not what it covers.

  Conflating the two fields deletes the wrong half of the corpus: filing_date is
  a property of the filing, indexed_at is stamped by bulk_index on every write.
  """
  filters = delete_filters(
    source_type="ixbrl_disclosure", indexed_before="2026-09-13T18:00:00Z"
  )
  assert {"range": {"indexed_at": {"lt": "2026-09-13T18:00:00Z"}}} in filters
  assert not any("filing_date" in str(f) for f in filters)


@pytest.mark.unit
def test_delete_filters_before_on_filing_date():
  """--before stays a filing_date trim; the new option did not repurpose it."""
  filters = delete_filters(source_type="narrative_section", before_date="2025-01-01")
  assert {"range": {"filing_date": {"lt": "2025-01-01"}}} in filters
  assert not any("indexed_at" in str(f) for f in filters)


@pytest.mark.unit
def test_delete_combines_every_filter():
  """All four narrowings compose rather than overwriting one another."""
  filters = delete_filters(
    source_type="ixbrl_disclosure",
    before_date="2025-01-01",
    indexed_before="2026-09-13T18:00:00Z",
  )
  assert filters == [
    {"term": {"graph_id": "sec"}},
    {"term": {"source_type": "ixbrl_disclosure"}},
    {"range": {"filing_date": {"lt": "2025-01-01"}}},
    {"range": {"indexed_at": {"lt": "2026-09-13T18:00:00Z"}}},
  ]
