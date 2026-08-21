"""A tool description is a prompt, so its shape is part of the contract.

Every string in a tool's ``description`` reaches the model that decides
whether to call the tool and how. Unlike a docstring, it is read at
selection time by something that cannot ask a follow-up question, so the
cost of an inconsistent one is not readability — it is the model picking
the wrong tool, or the right tool with the wrong arguments.

These tests pin the shape rather than the wording. The wording is a
judgement call per tool; the shape is what lets a model compare fifty of
them. Two rules:

1. **A tool says when to use it and what it returns.** Those two answer the
   selection question. A description that only says what the tool *is*
   leaves the model inferring applicability from the name.
2. **Section headers come from a fixed vocabulary.** ``PARAMETERS`` and
   ``INPUTS`` mean the same thing to a human and are two different tokens to
   a model comparing tools. Divergence here is invisible in review — nobody
   reads fifty descriptions side by side — which is exactly why it needs a
   test rather than a convention.

Deliberately not asserted: length. ``close-period`` is long because a close
has real preconditions and a caller that improvises one corrupts a ledger.
Brevity is not the goal; a shared skeleton is.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

TOOLS_DIR = Path(__file__).resolve().parents[3] / "robosystems/middleware/mcp/tools"

# The canonical section vocabulary. A header outside this set is either a
# synonym of one in it (use the canonical spelling) or a sign the description
# is carrying something that belongs in the tool's own docstring.
CANONICAL_SECTIONS = frozenset(
  {
    "WHEN TO USE",
    "PARAMETERS",
    "RETURNS",
    "NOTES",
    "RELATED TOOLS",
    "WORKFLOW",
  }
)

REQUIRED_SECTIONS = ("WHEN TO USE", "RETURNS")

# Tools whose description is a single declarative line and needs no skeleton:
# the name plus one sentence already answers both selection questions.
_SINGLE_LINE_EXEMPT: frozenset[str] = frozenset()


def _tool_descriptions() -> dict[str, tuple[str, str]]:
  """Every ``{"name": ..., "description": ...}`` literal under ``tools/``.

  Parsed rather than imported: a tool definition can require a live graph
  client to construct, and the shape of the string does not depend on it.
  """
  found: dict[str, tuple[str, str]] = {}
  for path in sorted(TOOLS_DIR.glob("*.py")):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
      if not isinstance(node, ast.Dict):
        continue
      keys = {
        k.value: v
        for k, v in zip(node.keys, node.values, strict=False)
        if isinstance(k, ast.Constant) and isinstance(k.value, str)
      }
      name_node, desc_node = keys.get("name"), keys.get("description")
      # ``inputSchema`` is what distinguishes a tool definition from a
      # parameter definition — both carry name/description, and keying off
      # anything else (length, whether the string is multi-line) silently
      # drops the short tool descriptions, which are the ones most likely to
      # be missing a section in the first place.
      if name_node is None or desc_node is None or "inputSchema" not in keys:
        continue
      if not (isinstance(name_node, ast.Constant) and isinstance(name_node.value, str)):
        continue
      try:
        description = ast.literal_eval(desc_node)
      except ValueError:
        continue  # an f-string or a computed value; not a static contract
      if isinstance(description, str):
        found[name_node.value] = (description, path.name)
  return found


def _sections(description: str) -> list[str]:
  """The ``**HEADER:**`` sections a description declares, in order."""
  import re

  return [
    m.group(1).strip()
    for m in re.finditer(r"\*\*([A-Z][A-Z0-9 /'&-]{2,}):?\*\*", description)
  ]


DESCRIPTIONS = _tool_descriptions()


@pytest.mark.unit
def test_the_registry_is_discoverable():
  """A parse failure here would make every test below vacuously pass."""
  assert len(DESCRIPTIONS) >= 40, (
    f"only found {len(DESCRIPTIONS)} tool descriptions — the parser probably "
    "stopped matching the definition shape"
  )


@pytest.mark.unit
@pytest.mark.parametrize("tool", sorted(DESCRIPTIONS))
def test_sections_come_from_the_canonical_vocabulary(tool):
  description, filename = DESCRIPTIONS[tool]
  unknown = [s for s in _sections(description) if s not in CANONICAL_SECTIONS]
  assert not unknown, (
    f"{tool} ({filename}) uses non-canonical section header(s) {unknown}. "
    f"Use one of {sorted(CANONICAL_SECTIONS)}, or move the content into the "
    "tool class's docstring if it is for a maintainer rather than the model."
  )


@pytest.mark.unit
@pytest.mark.parametrize("tool", sorted(DESCRIPTIONS))
def test_each_section_appears_once(tool):
  """Two ``**NOTES:**`` blocks in one description is a merge nobody finished.

  It reads as two unrelated lists to a model and hides the second from anyone
  skimming for the first.
  """
  import collections

  counts = collections.Counter(_sections(DESCRIPTIONS[tool][0]))
  repeated = {k: v for k, v in counts.items() if v > 1}
  assert not repeated, (
    f"{tool} ({DESCRIPTIONS[tool][1]}) repeats {repeated}. Merge them into one section."
  )


@pytest.mark.unit
@pytest.mark.parametrize("tool", sorted(DESCRIPTIONS))
def test_description_answers_the_selection_questions(tool):
  if tool in _SINGLE_LINE_EXEMPT:
    pytest.skip("single-line description; the name and sentence suffice")
  description, filename = DESCRIPTIONS[tool]
  present = set(_sections(description))
  missing = [s for s in REQUIRED_SECTIONS if s not in present]
  assert not missing, (
    f"{tool} ({filename}) is missing {missing}. A model choosing between "
    "tools needs to know when this one applies and what it hands back."
  )
