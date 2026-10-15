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
ACTIONS = [
  "count",
  "search",
  "delete",
  "force-merge",
  "settings",
  "warmup",
  "memory-optimized-on",
  "memory-optimized-off",
  "recreate-index",
]


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

  body = script.split('elif action == "delete":', 1)[1].split("\nelif action", 1)[0]
  lines = []
  for line in body.splitlines():
    if line.strip().startswith("query ="):
      break
    lines.append(line)

  # own source — running the real filter logic is the point, since a restatement
  # of it in the test would drift from the template without failing.
  exec(textwrap.dedent("\n".join(lines)), namespace)  # noqa: S102
  return namespace["filters"]


def recreate_calls(
  *, dry_run: bool, mapping: dict, refuse: tuple[str, str] | None = None
) -> list[tuple]:
  """The recreate-index branch, run against a recording fake of ``query_os``.

  Same idea as ``delete_filters``: execute the template's own lines so a change
  to the branch is caught rather than mirrored by a copy kept here. ``refuse``
  names a ``(method, path)`` the fake answers with HTTP 403, the way IAM does.
  """
  script = render(
    action="recreate-index",
    dry_run="true" if dry_run else "false",
    mapping=json.dumps(mapping),
  )
  calls: list[tuple] = []

  def query_os(path, body=None, method="POST"):
    calls.append((method, path, body))
    if refuse and (method, path) == refuse:
      raise urllib.error.HTTPError(path, 403, "Forbidden", None, None)  # type: ignore[arg-type]
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
def test_recreate_proves_both_verbs_on_a_probe_then_deletes_and_creates():
  """Both verbs and the mapping are exercised on a throwaway index first; only
  then DELETE and PUT on the real one, in that order and in the same script."""
  mapping = {
    "mappings": {"properties": {"x": {"type": "keyword"}}},
    "settings": {"index.knn": True},
  }
  calls = recreate_calls(dry_run=False, mapping=mapping)

  assert [(m, p) for m, p, _ in calls] == [
    ("POST", "/documents/_search"),
    ("DELETE", "/documents-recreate-probe"),
    ("PUT", "/documents-recreate-probe"),
    ("DELETE", "/documents-recreate-probe"),
    ("DELETE", "/documents"),
    ("PUT", "/documents"),
    ("GET", "/documents/_mapping"),
  ]
  assert calls[2][2] == mapping, "the probe is created with the real mapping"
  assert calls[5][2] == mapping


@pytest.mark.unit
@pytest.mark.parametrize("verb", ["DELETE", "PUT"])
def test_a_refused_verb_fails_before_the_live_index_is_touched(verb):
  """The 2026-09-16 near-miss: the bastion role lacked Delete and Put. A refusal
  must surface on the probe, with the live index still standing."""
  with pytest.raises(urllib.error.HTTPError):
    recreate_calls(
      dry_run=False,
      mapping={"mappings": {}},
      refuse=(verb, "/documents-recreate-probe"),
    )


@pytest.mark.unit
def test_a_refused_verb_leaves_no_call_against_the_live_index():
  calls: list[tuple] = []
  try:
    calls = recreate_calls(
      dry_run=False,
      mapping={"mappings": {}},
      refuse=("PUT", "/documents-recreate-probe"),
    )
  except urllib.error.HTTPError:
    pass
  # recreate_calls raised before returning; re-run the fake by hand to inspect
  # the calls it recorded up to the refusal.
  recorded: list[tuple] = []

  def query_os(path, body=None, method="POST"):
    recorded.append((method, path))
    if (method, path) == ("PUT", "/documents-recreate-probe"):
      raise urllib.error.HTTPError(path, 403, "Forbidden", None, None)  # type: ignore[arg-type]
    if path.endswith("/_search"):
      return {
        "hits": {"total": {"value": 0}},
        "aggregations": {"by_graph": {"buckets": []}},
      }
    return {}

  script = render(action="recreate-index", dry_run="false", mapping="{}")
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
      "dry_run": False,
      "query_os": query_os,
      "print": lambda *_: None,
    }
  )
  body = script.split('elif action == "recreate-index":', 1)[1]
  with pytest.raises(urllib.error.HTTPError):
    exec(textwrap.dedent(body), namespace)  # noqa: S102
  assert ("DELETE", "/documents") not in recorded
  assert ("PUT", "/documents") not in recorded
  assert calls == []


@pytest.mark.unit
def test_recreate_ships_the_checkout_mapping_intact():
  """The encoder crosses the literal boundary unchanged: what the checkout
  declares is what the bastion PUTs."""
  calls = recreate_calls(dry_run=False, mapping=INDEX_MAPPING)
  put_body = next(b for m, p, b in calls if (m, p) == ("PUT", "/documents"))

  assert put_body == json.loads(json.dumps(INDEX_MAPPING))
  encoder = put_body["mappings"]["properties"]["embedding"]["method"]["parameters"][
    "encoder"
  ]
  assert encoder == {
    "name": "sq",
    "parameters": {"type": "fp16", "bits": 16, "clip": False},
  }


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


def branch_output(action: str, responses: dict) -> tuple[dict, list[tuple]]:
  """Run one read-only branch against a fake ``query_os`` answering by path.

  A path missing from ``responses`` answers HTTP 403, the way IAM refuses.
  Returns what the branch printed and the calls it made.
  """
  script = render(action=action)
  calls: list[tuple] = []
  printed: list[str] = []

  def query_os(path, body=None, method="POST"):
    calls.append((method, path))
    if path not in responses:
      raise urllib.error.HTTPError(path, 403, "Forbidden", None, None)  # type: ignore[arg-type]
    return responses[path]

  namespace: dict = {
    "json": json,
    "urllib": urllib,
    "index_name": "documents",
    "query_os": query_os,
    "print": printed.append,
  }
  body = script.split(f'elif action == "{action}":', 1)[1].split("\nelif action", 1)[0]
  exec(textwrap.dedent(body), namespace)  # noqa: S102
  return json.loads(printed[0]), calls


@pytest.mark.unit
def test_settings_flags_the_four_pre_upgrade_blockers():
  live = {
    "index.knn": "true",
    "index.knn.space_type": "l2",
    "index.store.hybrid.mmap.extensions": ["vec"],
    "index.number_of_shards": "1",
  }
  out, calls = branch_output(
    "settings",
    {"/documents/_settings?flat_settings=true": {"documents-v2": {"settings": live}}},
  )
  assert calls == [("GET", "/documents/_settings?flat_settings=true")]
  assert out["upgrade_blockers"] == [
    "documents-v2: index.knn.space_type",
    "documents-v2: index.store.hybrid.mmap.extensions",
  ]
  assert out["settings"] == {"documents-v2": live}


@pytest.mark.unit
def test_settings_checks_every_index_behind_the_name():
  """An alias over two indices passes only if both do: the second one's
  blocker is reported even though the first is clean."""
  out, _ = branch_output(
    "settings",
    {
      "/documents/_settings?flat_settings=true": {
        "documents-a": {"settings": {"index.knn": "true"}},
        "documents-b": {"settings": {"index.knn.algo_param.m": "16"}},
      }
    },
  )
  assert out["upgrade_blockers"] == ["documents-b: index.knn.algo_param.m"]


@pytest.mark.unit
def test_settings_reports_no_blockers_for_the_declared_mapping_shape():
  out, _ = branch_output(
    "settings",
    {
      "/documents/_settings?flat_settings=true": {
        "documents": {"settings": {"index.knn": "true"}}
      }
    },
  )
  assert out["upgrade_blockers"] == []


@pytest.mark.unit
def test_warmup_reports_shards_and_cache_usage():
  out, calls = branch_output(
    "warmup",
    {
      "/_plugins/_knn/warmup/documents": {
        "_shards": {"total": 1, "successful": 1, "failed": 0}
      },
      "/_plugins/_knn/stats/graph_memory_usage_percentage": {
        "nodes": {"n1": {"graph_memory_usage_percentage": 67.9}}
      },
    },
  )
  assert [method for method, _ in calls] == ["GET", "GET"]
  assert out == {
    "shards": {"total": 1, "successful": 1, "failed": 0},
    "graph_memory_usage_percentage": {"n1": 67.9},
  }


@pytest.mark.unit
def test_warmup_still_reports_when_stats_are_refused():
  out, _ = branch_output(
    "warmup",
    {
      "/_plugins/_knn/warmup/documents": {
        "_shards": {"total": 1, "successful": 1, "failed": 0}
      }
    },
  )
  assert out["shards"]["successful"] == 1
  assert out["graph_memory_usage_percentage"] == {}


@pytest.mark.unit
def test_a_refused_warmup_reports_the_refusal_and_skips_the_stats():
  out, calls = branch_output("warmup", {})
  assert calls == [("GET", "/_plugins/_knn/warmup/documents")]
  assert out == {"error": "warmup refused: HTTP 403 Forbidden"}


def memory_optimized_calls(
  action: str,
  *,
  enabled: bool,
  dry_run: bool = False,
  refuse_put: bool = False,
  calls: list[tuple] | None = None,
) -> tuple[dict | None, list[tuple]]:
  """The memory-optimized branch, run against a fake domain that keeps the
  setting's state, so a read after the update sees what the update wrote."""
  script = render(action=action, dry_run="true" if dry_run else "false")
  state = {"enabled": enabled}
  calls = [] if calls is None else calls
  printed: list[str] = []

  def query_os(path, body=None, method="POST"):
    calls.append((method, path.split("?", 1)[0], body))
    if method == "PUT":
      if refuse_put:
        raise urllib.error.HTTPError(path, 403, "Forbidden", None, None)  # type: ignore[arg-type]
      state["enabled"] = body["index.knn.memory_optimized_search"]
    if path.startswith("/documents/_settings") and method == "GET":
      return {
        "documents": {
          "settings": {"index.knn": "true"},
          "defaults": {
            "index.knn.memory_optimized_search": str(state["enabled"]).lower()
          },
        }
      }
    return {}

  namespace: dict = {
    "json": json,
    "urllib": urllib,
    "index_name": "documents",
    "action": action,
    "dry_run": dry_run,
    "query_os": query_os,
    "print": printed.append,
  }
  body = script.split(
    'elif action in ("memory-optimized-on", "memory-optimized-off"):', 1
  )[1]
  body = body.split("\nelif action", 1)[0]
  exec(textwrap.dedent(body), namespace)  # noqa: S102
  return (json.loads(printed[0]) if printed else None), calls


@pytest.mark.unit
def test_memory_optimized_on_closes_updates_and_reopens_in_order():
  out, calls = memory_optimized_calls("memory-optimized-on", enabled=False)
  assert [(m, p) for m, p, _ in calls] == [
    ("GET", "/documents/_settings"),
    ("POST", "/documents/_close"),
    ("PUT", "/documents/_settings"),
    ("POST", "/documents/_open"),
    ("GET", "/_cluster/health/documents"),
    ("GET", "/documents/_settings"),
  ]
  assert calls[2][2] == {"index.knn.memory_optimized_search": True}
  assert out == {"before": False, "after": True, "changed": True}


@pytest.mark.unit
def test_memory_optimized_off_is_the_rollback():
  out, _ = memory_optimized_calls("memory-optimized-off", enabled=True)
  assert out == {"before": True, "after": False, "changed": True}


@pytest.mark.unit
@pytest.mark.parametrize(
  ("action", "enabled", "dry_run"),
  [("memory-optimized-on", True, False), ("memory-optimized-on", False, True)],
)
def test_memory_optimized_never_closes_when_nothing_changes(action, enabled, dry_run):
  """Already in the asked-for state, or a status read: one GET, no close."""
  out, calls = memory_optimized_calls(action, enabled=enabled, dry_run=dry_run)
  assert [m for m, _, _ in calls] == ["GET"]
  assert out == {"before": enabled, "after": enabled, "changed": False}


@pytest.mark.unit
def test_a_refused_update_still_reopens_the_index():
  """The update failing must not leave the index closed: a closed index is a
  search outage until someone notices."""
  calls: list[tuple] = []
  with pytest.raises(urllib.error.HTTPError):
    memory_optimized_calls(
      "memory-optimized-on", enabled=False, refuse_put=True, calls=calls
    )
  assert [(m, p) for m, p, _ in calls][-3:] == [
    ("PUT", "/documents/_settings"),
    ("POST", "/documents/_open"),
    ("GET", "/_cluster/health/documents"),
  ]
