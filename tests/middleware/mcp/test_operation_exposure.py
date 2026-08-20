"""A capability must be reachable, or say why it is not.

Three ways a built capability has silently failed to reach a client, all
observed in this codebase:

1. A tool class is written in full — definition, ``execute``, tested read
   underneath — and never instantiated in ``manager.py``. It has no tool name
   in any list and no dispatch branch, so no client can call it.
2. A tool is instantiated but its definition is never appended to the tool
   list, or it is listed but has no branch in ``call_tool``. Either half alone
   is a tool that advertises and fails, or works and is invisible.
3. A REST operation is mounted as a hand-written ``@router.post`` and simply
   never gets a matching MCP tool. Because nothing declares which operations
   are meant to reach MCP, this is indistinguishable from a deliberate hold.

Operations registered through ``OperationRegistrar`` are immune: one
``OperationSpec`` mounts the REST route and auto-generates the MCP tool, so
they cannot drift. These tests cover everything that does not use it, and push
the answer to (3) into :data:`MCP_EXPOSURE_HOLDS`, where a hold is a sentence a
human wrote rather than an absence nobody noticed.

The checks are static (AST + source scan) rather than behavioural because the
failure mode is *absence*: a runtime check can only exercise tools that were
wired, which is exactly the population that is never broken.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

from robosystems.middleware.mcp.exposure_policy import MCP_EXPOSURE_HOLDS

REPO_ROOT = Path(__file__).resolve().parents[3]
TOOLS_DIR = REPO_ROOT / "robosystems/middleware/mcp/tools"
MANAGER = TOOLS_DIR / "manager.py"
ROUTERS_DIR = REPO_ROOT / "robosystems/routers"
EXTENSIONS_ROUTERS = ROUTERS_DIR / "extensions"

# Classes in the tools package that are not themselves MCP tools.
NOT_A_TOOL = frozenset({"BaseTool", "GraphMCPTools"})

# Sanity floors. Every scan below must find a plausible population; a regex or
# layout change that silently matches nothing would make these tests vacuous
# and hide the very drift they exist to catch.
MIN_TOOL_CLASSES = 30
MIN_REST_OPERATIONS = 40
MIN_REGISTRAR_OPERATIONS = 25


def _parse(path: Path) -> ast.Module:
  return ast.parse(path.read_text())


def _tool_classes() -> dict[str, Path]:
  """Every public ``*Tool`` class defined in the MCP tools package."""
  found: dict[str, Path] = {}
  for path in sorted(TOOLS_DIR.glob("*.py")):
    for node in _parse(path).body:
      if not isinstance(node, ast.ClassDef):
        continue
      if node.name.startswith("_") or node.name in NOT_A_TOOL:
        continue
      if node.name.endswith("Tool"):
        found[node.name] = path
  return found


def _declared_tool_name(path: Path, class_name: str) -> str | None:
  """The ``name`` a class advertises from its ``get_tool_definition``.

  Read from the returned dict literal rather than by scanning the file for
  ``"name":``, which also matches JSON-schema property names.
  """
  for node in _parse(path).body:
    if not isinstance(node, ast.ClassDef) or node.name != class_name:
      continue
    for sub in node.body:
      if not isinstance(sub, ast.FunctionDef) or sub.name != "get_tool_definition":
        continue
      for inner in ast.walk(sub):
        if isinstance(inner, ast.Return) and isinstance(inner.value, ast.Dict):
          for key, value in zip(inner.value.keys, inner.value.values, strict=True):
            if (
              isinstance(key, ast.Constant)
              and key.value == "name"
              and isinstance(value, ast.Constant)
            ):
              return str(value.value)
  return None


def _manager_source() -> str:
  return MANAGER.read_text()


def _instantiated_tool_classes() -> set[str]:
  """Tool classes constructed anywhere in ``manager.py``."""
  names: set[str] = set()
  for node in ast.walk(_parse(MANAGER)):
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
      if node.func.id.endswith("Tool"):
        names.add(node.func.id)
  return names


def _listed_tool_attributes() -> set[str]:
  """Attributes on which ``get_tool_definition()`` is called in ``manager.py``.

  ``self.foo_tool.get_tool_definition()`` inside one of the
  ``_get_*_tool_definitions`` builders is what puts a tool in the list a
  client sees.
  """
  attrs: set[str] = set()
  for node in ast.walk(_parse(MANAGER)):
    if (
      isinstance(node, ast.Call)
      and isinstance(node.func, ast.Attribute)
      and node.func.attr == "get_tool_definition"
      and isinstance(node.func.value, ast.Attribute)
    ):
      attrs.add(node.func.value.attr)
  return attrs


def _assigned_tool_attributes() -> dict[str, str]:
  """``self.<attr> = <ClassName>(...)`` pairs in ``manager.py``."""
  mapping: dict[str, str] = {}
  for node in ast.walk(_parse(MANAGER)):
    if not isinstance(node, ast.Assign) or not isinstance(node.value, ast.Call):
      continue
    if not isinstance(node.value.func, ast.Name):
      continue
    if not node.value.func.id.endswith("Tool"):
      continue
    for target in node.targets:
      if isinstance(target, ast.Attribute):
        mapping[target.attr] = node.value.func.id
  return mapping


def _dispatchable_tool_names() -> set[str]:
  """String literals ``call_tool`` compares the incoming tool name against."""
  names: set[str] = set()
  for node in ast.walk(_parse(MANAGER)):
    if not isinstance(node, ast.AsyncFunctionDef) or node.name != "call_tool":
      continue
    for inner in ast.walk(node):
      if not isinstance(inner, ast.Compare):
        continue
      for comparator in inner.comparators:
        candidates = (
          comparator.elts
          if isinstance(comparator, ast.Tuple | ast.List | ast.Set)
          else [comparator]
        )
        for element in candidates:
          if isinstance(element, ast.Constant) and isinstance(element.value, str):
            names.add(element.value)
  return names


def _registrar_operation_names() -> set[str]:
  """``OperationSpec(name=...)`` declarations across every router."""
  names: set[str] = set()
  for path in ROUTERS_DIR.rglob("*.py"):
    for node in ast.walk(_parse(path)):
      if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Name):
        continue
      if node.func.id != "OperationSpec":
        continue
      for kw in node.keywords:
        if kw.arg == "name" and isinstance(kw.value, ast.Constant):
          names.add(str(kw.value.value))
  return names


def _rest_operation_names() -> set[str]:
  """Every operation mounted under an extensions ``/operations/`` path.

  Hand-written routes are matched from the metrics-decorator path, which
  carries the full template; registrar-mounted ones come from the specs.
  """
  names = _registrar_operation_names()
  pattern = re.compile(r'"/extensions/[a-z]+/\{graph_id\}/operations/([a-z0-9-]+)"')
  for path in EXTENSIONS_ROUTERS.rglob("*.py"):
    names.update(pattern.findall(path.read_text()))
  return names


def _mcp_tool_names() -> set[str]:
  """Every tool name a client can see: hand-written plus registrar-generated."""
  names = _registrar_operation_names()
  for class_name, path in _tool_classes().items():
    declared = _declared_tool_name(path, class_name)
    if declared:
      names.add(declared)
  return names


# ── Layer 1: the class exists but nothing reaches it ────────────────────────


def test_every_tool_class_is_instantiated():
  """A ``*Tool`` class not constructed in ``manager.py`` is unreachable."""
  classes = _tool_classes()
  assert len(classes) >= MIN_TOOL_CLASSES, (
    f"only found {len(classes)} tool classes — the scan is probably broken"
  )

  instantiated = _instantiated_tool_classes()
  orphaned = sorted(set(classes) - instantiated)
  assert not orphaned, (
    "MCP tool classes defined but never instantiated in manager.py — they "
    "cannot be listed or called, so the capability is unreachable: "
    f"{orphaned}. Wire them into GraphMCPTools.__init__ (and the matching "
    "definition builder + call_tool branch), or delete them."
  )


def test_every_instantiated_tool_is_listed():
  """A tool nobody lists is invisible, however well it works."""
  assigned = _assigned_tool_attributes()
  listed = _listed_tool_attributes()
  unlisted = sorted(attr for attr in assigned if attr not in listed)
  assert not unlisted, (
    "tools instantiated in manager.py whose get_tool_definition() is never "
    f"called, so they never appear in any tool list: {unlisted}. Add them to "
    "the matching _get_*_tool_definitions() builder."
  )


def test_every_declared_tool_name_is_dispatchable():
  """A listed tool with no ``call_tool`` branch fails when a client calls it."""
  undispatchable = []
  registrar_names = _registrar_operation_names()
  dispatchable = _dispatchable_tool_names()

  for class_name, path in sorted(_tool_classes().items()):
    declared = _declared_tool_name(path, class_name)
    if declared is None:
      continue
    if declared in registrar_names:
      # Registrar-generated: dispatched at Layer 0 by name lookup, and a
      # hand-written class for the same name would be dead code.
      undispatchable.append(
        f"{class_name} duplicates registrar operation '{declared}' "
        "(registrar dispatch wins, so this class is unreachable)"
      )
      continue
    if declared not in dispatchable:
      undispatchable.append(f"{class_name} declares '{declared}' with no branch")

  assert not undispatchable, (
    f"MCP tools that cannot be dispatched by the name they advertise: {undispatchable}"
  )


# ── Layer 2: the REST operation exists but MCP never got it ─────────────────


def test_every_rest_operation_is_exposed_or_held():
  """Each extensions operation reaches MCP, or records why it does not."""
  rest = _rest_operation_names()
  assert len(rest) >= MIN_REST_OPERATIONS, (
    f"only found {len(rest)} REST operations — the scan is probably broken"
  )
  assert len(_registrar_operation_names()) >= MIN_REGISTRAR_OPERATIONS, (
    "registrar spec scan found too few operations — probably broken"
  )

  exposed = _mcp_tool_names()
  undeclared = sorted(rest - exposed - set(MCP_EXPOSURE_HOLDS))
  assert not undeclared, (
    "REST operations with no MCP tool and no recorded reason: "
    f"{undeclared}. Either register them through OperationRegistrar (which "
    "mounts both surfaces from one OperationSpec) or add them to "
    "MCP_EXPOSURE_HOLDS in middleware/mcp/exposure_policy.py with a reason. "
    "Silence is what let capabilities ship unreachable."
  )


def test_exposure_holds_are_not_stale():
  """A hold naming an operation that no longer exists is rot, not policy."""
  rest = _rest_operation_names()
  stale = sorted(set(MCP_EXPOSURE_HOLDS) - rest)
  assert not stale, (
    f"MCP_EXPOSURE_HOLDS names operations that no longer exist: {stale}. "
    "Remove the entries."
  )


def test_exposure_holds_are_not_silently_shadowed():
  """A held operation that gained a tool should lose its hold."""
  exposed = _mcp_tool_names()
  contradictory = sorted(set(MCP_EXPOSURE_HOLDS) & exposed)
  assert not contradictory, (
    f"operations both held and exposed: {contradictory}. The hold is no "
    "longer true — delete it so the policy keeps meaning something."
  )


@pytest.mark.parametrize("operation", sorted(MCP_EXPOSURE_HOLDS))
def test_every_hold_gives_a_real_reason(operation: str):
  """The reason is the whole point; a placeholder defeats the guard.

  A short reason is accepted only when it points at another hold — a
  deliberate group ("same call as share-report") is evaluable, whereas a bare
  "TODO" or "not needed" is not.
  """
  reason = MCP_EXPOSURE_HOLDS[operation].strip()
  siblings = set(MCP_EXPOSURE_HOLDS) - {operation}
  cross_referenced = any(sibling in reason for sibling in siblings)
  assert len(reason) >= 40 or cross_referenced, (
    f"hold for '{operation}' needs a reason a future reader can evaluate — "
    f"either spell it out or point at the hold it follows. Got: {reason!r}"
  )
