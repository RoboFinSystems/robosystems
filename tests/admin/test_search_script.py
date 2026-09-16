"""Tests for the OpenSearch admin script template.

The script that runs on the bastion is a **string** built by ``str.format`` and
shipped over SSM, so neither ruff nor basedpyright ever sees it: a brace-escaping
slip or a wrong filter field fails for the first time in production, mid-delete.
These tests render the template the way ``_run_opensearch_script`` does and hold
the invariants the toolchain cannot.
"""

import ast
import json
import textwrap
import urllib.error

import pytest

from robosystems.admin.commands.search import _OPENSEARCH_QUERY_SCRIPT, _literal
from robosystems.operations.search.client import INDEX_MAPPING

# The dispatch values the template branches on — not the CLI subcommand names.
ACTIONS = ["count", "search", "delete", "force-merge", "recreate-index"]


def render(**overrides) -> str:
  """The script exactly as the dispatcher builds it, encoding included."""
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
    "mapping": "{}",
  }
  params.update(overrides)
  return _OPENSEARCH_QUERY_SCRIPT.format(
    size=int(params.pop("size")),
    **{key: _literal(value) for key, value in params.items()},
  )


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


def recreate_calls(*, dry_run: bool, mapping: dict) -> list[tuple]:
  """The recreate-index branch, run against a recording fake of ``query_os``.

  Same idea as ``delete_filters``: execute the template's own lines so a change
  to the branch is caught rather than mirrored by a copy kept here.
  """
  script = render(
    action="recreate-index",
    dry_run="true" if dry_run else "false",
    mapping=json.dumps(mapping),
  )
  calls: list[tuple] = []

  def query_os(path, body=None, method="POST"):
    calls.append((method, path, body))
    if path.endswith("/_search"):
      return {
        "hits": {"total": {"value": 3}},
        "aggregations": {"by_graph": {"buckets": [{"key": "sec", "doc_count": 3}]}},
      }
    if path.endswith("/_mapping"):
      return {
        "documents": {
          "mappings": {
            "properties": {
              "embedding": {
                "method": {"parameters": {"encoder": {"parameters": {"type": "fp16"}}}}
              }
            }
          }
        }
      }
    return {}

  namespace: dict = {
    node.targets[0].id: node.value.value
    for node in ast.parse(script).body
    if isinstance(node, ast.Assign)
    and isinstance(node.targets[0], ast.Name)
    and isinstance(node.value, ast.Constant)
  }
  namespace.update(
    {
      "json": json,
      "urllib": urllib,
      "dry_run": dry_run,
      "query_os": query_os,
      "print": lambda *_: None,
    }
  )
  body = script.split('elif action == "recreate-index":', 1)[1]
  exec(textwrap.dedent(body), namespace)  # noqa: S102
  return calls


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


@pytest.mark.unit
@pytest.mark.parametrize(
  "hostile",
  [
    '"; import os; os.system("id"); x = "',
    'sec" or True or "',
    "back\\slash",
    'quote"inside',
    "new\nline",
  ],
)
def test_a_parameter_cannot_escape_its_literal(hostile):
  """A parameter is data in the generated script, never code.

  The script is assembled by substitution and then run on the bastion, so a
  value that ended its own literal would have the remainder read as source.
  Assert the rendered script still parses AND that the value survives intact
  as the string it was — escaping that mangles the value is its own defect.
  """
  script = render(action="delete", graph_id=hostile)
  tree = ast.parse(script)

  assigned = next(
    node.value.value
    for node in tree.body
    if isinstance(node, ast.Assign)
    and isinstance(node.targets[0], ast.Name)
    and node.targets[0].id == "graph_id"
  )
  assert assigned == hostile


@pytest.mark.unit
def test_hostile_graph_id_still_lands_in_the_filter_verbatim():
  """Escaping happens at the source-literal boundary, not in the query."""
  hostile = 'sec" or True or "'
  filters = delete_filters(graph_id=hostile)
  assert filters == [{"term": {"graph_id": hostile}}]


@pytest.mark.unit
def test_recreate_dry_run_only_counts():
  """The confirm step reads the index; nothing is deleted."""
  calls = recreate_calls(dry_run=True, mapping={"mappings": {}})
  assert [method for method, _, _ in calls] == ["POST"]
  assert calls[0][1] == "/documents/_search"


@pytest.mark.unit
def test_recreate_deletes_then_creates_with_the_decoded_mapping():
  """DELETE and PUT happen in the same script, in that order — a missing index
  would otherwise be recreated by the next writer with a dynamic mapping."""
  mapping = {
    "mappings": {"properties": {"x": {"type": "keyword"}}},
    "settings": {"index.knn": True},
  }
  calls = recreate_calls(dry_run=False, mapping=mapping)

  assert [method for method, _, _ in calls] == ["POST", "DELETE", "PUT", "GET"]
  assert calls[1] == ("DELETE", "/documents", None)
  assert calls[2] == ("PUT", "/documents", mapping)


@pytest.mark.unit
def test_recreate_ships_the_checkout_mapping_intact():
  """The encoder crosses the literal boundary unchanged: what the checkout
  declares is what the bastion PUTs."""
  calls = recreate_calls(dry_run=False, mapping=INDEX_MAPPING)
  put_body = calls[2][2]

  assert put_body == json.loads(json.dumps(INDEX_MAPPING))
  encoder = put_body["mappings"]["properties"]["embedding"]["method"]["parameters"][
    "encoder"
  ]
  assert encoder == {"name": "sq", "parameters": {"type": "fp16", "clip": False}}


@pytest.mark.unit
def test_a_hostile_mapping_value_cannot_escape_its_literal():
  mapping = {"mappings": {"_meta": {"note": '"; import os; os.system("id"); x = "'}}}
  script = render(action="recreate-index", mapping=json.dumps(mapping))
  tree = ast.parse(script)

  assigned = next(
    node.value.value
    for node in tree.body
    if isinstance(node, ast.Assign)
    and isinstance(node.targets[0], ast.Name)
    and node.targets[0].id == "mapping"
  )
  assert json.loads(assigned) == mapping
