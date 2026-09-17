"""The product docs in ``docs/product/`` build cleanly and name only real tools.

Product pages are public claims about what the MCP server does. A page that
names a tool which was renamed or removed tells a reader, and the model reading
along with them, to call something that is not there. Tool descriptions have
been rewritten in bulk before, so the check is mechanical rather than a review
habit.

Tool names are collected statically, the same two places the server builds its
catalog from: hand-written tool definitions (a dict carrying both ``name`` and
``inputSchema``) under ``middleware/mcp/tools/``, and the ``OperationSpec``
declarations the registrar turns into write tools.

The number of names the pages mention is pinned exactly. A floor would pass if
the extraction silently stopped matching, which is how a guard like this goes
vacuous without anyone noticing.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

from robosystems.scripts import publish_docs

REPO_ROOT = Path(__file__).resolve().parents[2]
PRODUCT_DIR = REPO_ROOT / "docs" / "product"
TOOLS_DIR = REPO_ROOT / "robosystems" / "middleware" / "mcp" / "tools"

# Distinct tool names mentioned across every product page. Update it in the
# same change that adds or removes a mention.
EXPECTED_TOOL_MENTIONS = 0

# Backticked kebab-case words that look like tool names but are not meant as one.
NOT_TOOLS: frozenset[str] = frozenset()

_BACKTICKED = re.compile(r"`([a-z][a-z0-9]*(?:-[a-z0-9]+)+)`")


def _server_tool_names() -> set[str]:
  names: set[str] = set()
  for path in TOOLS_DIR.glob("*.py"):
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
      if not isinstance(node, ast.Dict):
        continue
      keys = [k.value if isinstance(k, ast.Constant) else None for k in node.keys]
      if "name" in keys and "inputSchema" in keys:
        value = node.values[keys.index("name")]
        if isinstance(value, ast.Constant) and isinstance(value.value, str):
          names.add(value.value)
  for path in (REPO_ROOT / "robosystems").rglob("*.py"):
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
      if not isinstance(node, ast.Call):
        continue
      func = node.func
      called = func.id if isinstance(func, ast.Name) else getattr(func, "attr", None)
      if called != "OperationSpec":
        continue
      for keyword in node.keywords:
        if keyword.arg == "name" and isinstance(keyword.value, ast.Constant):
          names.add(keyword.value.value)
  return names


def _mentioned_tool_names() -> dict[str, set[str]]:
  mentions: dict[str, set[str]] = {}
  for path in sorted(PRODUCT_DIR.glob("*/*.md")):
    if path.name == "README.md":
      continue
    for is_code, chunk in publish_docs.split_code(path.read_text(encoding="utf-8")):
      if is_code:
        continue
      for name in _BACKTICKED.findall(chunk):
        if name not in NOT_TOOLS:
          mentions.setdefault(name, set()).add(path.relative_to(PRODUCT_DIR).as_posix())
  return mentions


@pytest.mark.unit
def test_the_server_tool_catalog_is_found():
  names = _server_tool_names()
  assert {"close-period", "get-fiscal-calendar", "read-graph-cypher"} <= names
  assert len(names) > 50


@pytest.mark.unit
def test_product_pages_name_only_real_tools():
  known = _server_tool_names()
  unknown = {
    name: sorted(pages)
    for name, pages in _mentioned_tool_names().items()
    if name not in known
  }
  assert unknown == {}, (
    "product docs name tools the MCP server does not have (rename them, or add a "
    "deliberate non-tool to NOT_TOOLS)"
  )


@pytest.mark.unit
def test_tool_mentions_are_pinned():
  assert len(_mentioned_tool_names()) == EXPECTED_TOOL_MENTIONS


@pytest.mark.unit
def test_product_pages_build_without_errors():
  build = publish_docs.Build()
  publish_docs.build_product(PRODUCT_DIR, REPO_ROOT, build)
  assert build.errors == []
