"""Every parse of caller-supplied YAML refuses aliases."""

import re
from pathlib import Path

import pytest
import yaml

from robosystems.operations.search.markdown_parser import parse_frontmatter
from robosystems.schemas.runtime.custom import CustomSchemaParser, SchemaFormat
from robosystems.utils.yaml_input import load_untrusted_yaml

_ALIASED = "anchor: &a [x, x]\ncopy: *a\nname: s\nnodes: []\n"

# Files whose YAML is read from the deploy, never from a caller.
_TRUSTED_CONFIG_LOADS = {
  "robosystems/config/graph_tier.py",
  "robosystems/config/valkey_registry.py",
}

_REPO = Path(__file__).resolve().parents[2]


def _frontmatter_ignores_aliases(_request):
  metadata, _ = parse_frontmatter(f"---\n{_ALIASED}---\n# Body\n")
  assert metadata == {}


def _schema_parser_refuses_aliases(_request):
  with pytest.raises(yaml.YAMLError):
    CustomSchemaParser().parse(_ALIASED, SchemaFormat.YAML)


def _validate_route_refuses_aliases(request):
  client = request.getfixturevalue("client_with_mocked_auth")
  response = client.post(
    "/v1/graphs/schema/validate",
    json={"schema_definition": _ALIASED, "format": "yaml"},
  )
  assert response.status_code == 200
  body = response.json()
  assert body["valid"] is False
  assert any("aliases are not allowed" in error for error in body["errors"])


@pytest.mark.unit
@pytest.mark.parametrize(
  "entry_point",
  [
    _frontmatter_ignores_aliases,
    _schema_parser_refuses_aliases,
    _validate_route_refuses_aliases,
  ],
  ids=["document-frontmatter", "custom-schema-parser", "schema-validate-route"],
)
def test_user_yaml_entry_points_refuse_aliases(entry_point, request):
  entry_point(request)


@pytest.mark.unit
def test_plain_yaml_still_parses():
  assert load_untrusted_yaml("name: s\nnodes: [a, b]\n") == {
    "name": "s",
    "nodes": ["a", "b"],
  }


@pytest.mark.unit
def test_no_other_module_parses_yaml_directly():
  pattern = re.compile(
    r"\byaml\.(safe_load|load|full_load|unsafe_load)\w*\("
    r"|^\s*from\s+yaml\s+import\b.*\b(safe_)?load",
    re.MULTILINE,
  )
  offenders = sorted(
    str(path.relative_to(_REPO))
    for path in (_REPO / "robosystems").rglob("*.py")
    if pattern.search(path.read_text())
    and str(path.relative_to(_REPO))
    not in _TRUSTED_CONFIG_LOADS | {"robosystems/utils/yaml_input.py"}
  )
  assert offenders == []
