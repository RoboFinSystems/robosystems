"""GitHub Actions expression validation.

There is no actionlint in CI, so a malformed `${{ }}` expression is not caught
until the workflow is triggered — and for a reusable workflow like
`service-refresh.yml`, which `prod.yml` and `staging.yml` both call, that means
the first thing it breaks is a deploy.

This guards the one class of malformed expression that is easy to write and looks
correct: arithmetic. GitHub Actions expressions have no arithmetic operators. The
documented operator set is grouping, index, property dereference, `!`, the four
comparisons, `==`, `!=`, `&&`, and `||` — nothing else. A `+` inside `${{ }}` is
a parse error, not a sum.

https://docs.github.com/en/actions/reference/workflows-and-actions/expressions

Do the arithmetic in a `run:` step and pass it through a step or job output.
`service-refresh.yml`'s "Compute refresh job timeout" step is the worked example.
"""

import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
GITHUB_DIR = REPO_ROOT / ".github"

_EXPRESSION_RE = re.compile(r"\$\{\{(.*?)\}\}", re.DOTALL)
# String literals are stripped before the scan: single-quoted values legitimately
# contain the characters we look for ('us-east-1', 'refs/heads/main', '*/5 * * *').
_STRING_LITERAL_RE = re.compile(r"'[^']*'")
# Object filters are also stripped. `needs.*.result` and `steps.*.outputs` use `*`
# as a dereference wildcard, which is valid and unrelated to multiplication — a
# wildcard is always attached to a dot or brackets, never spaced as an operand.
_OBJECT_FILTER_RE = re.compile(r"\.\*|\[\*\]")

# `+` is unambiguous — it appears in no valid expression. `*`, `-` and `/` all
# have legitimate unquoted uses (object filters above; hyphenated job ids such as
# needs.deploy-graph-infra.outputs.x; paths), so they are flagged only when spaced
# like arithmetic, which none of those forms are.
_ARITHMETIC_PATTERNS = [
  (re.compile(r"\+"), "+"),
  (re.compile(r"\s\*\s"), "*"),
  (re.compile(r"\s-\s"), "-"),
  (re.compile(r"\s/\s"), "/"),
]


def _workflow_files() -> list[Path]:
  return sorted(
    [*GITHUB_DIR.glob("workflows/*.yml"), *GITHUB_DIR.glob("actions/*/action.yml")]
  )


def test_workflow_files_are_discovered():
  """A silent glob miss would make every assertion below vacuously pass."""
  files = _workflow_files()
  assert len(files) > 10, f"expected the .github tree, found {len(files)} files"
  assert any(f.name == "service-refresh.yml" for f in files)


@pytest.mark.parametrize("path", _workflow_files(), ids=lambda p: p.name)
def test_no_arithmetic_in_expressions(path: Path):
  violations: list[str] = []

  for match in _EXPRESSION_RE.finditer(path.read_text()):
    expression = match.group(1)
    scannable = _OBJECT_FILTER_RE.sub("", _STRING_LITERAL_RE.sub("''", expression))
    for pattern, operator in _ARITHMETIC_PATTERNS:
      if pattern.search(scannable):
        line = path.read_text()[: match.start()].count("\n") + 1
        violations.append(
          f"{path.relative_to(REPO_ROOT)}:{line}: '{operator}' in ${{{{{expression.strip()}}}}}"
        )
        break

  assert not violations, (
    "GitHub Actions expressions have no arithmetic operators; a '+' inside "
    "${{ }} is a parse error that breaks the whole workflow. Compute the value "
    "in a run: step and pass it via an output instead.\n  " + "\n  ".join(violations)
  )
