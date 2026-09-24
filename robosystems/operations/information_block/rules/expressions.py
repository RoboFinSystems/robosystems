"""Safe arithmetic expression parser and evaluator for rule expressions.

Rule expressions use a tight subset of arithmetic + equality:
  ``$Assets = ($Liabilities + $Equity)``

``$Name`` becomes ``_var_Name`` before ``ast.parse``; a whitelist walker then
rejects every node outside plain arithmetic and equality (no calls,
attributes or subscripts), and the tree is evaluated directly, never with
``eval()``.

``avg($X)`` and ``$X[t-1]`` are desugared to synthesized operands before
parsing, which the caller binds; any other call or bracket form still
reaches the whitelist and is rejected.
"""

from __future__ import annotations

import ast
import re
from dataclasses import dataclass

EQUALITY_TOLERANCE: float = 0.01

_ALLOWED_NODES = (
  ast.Expression,
  ast.Compare,
  ast.BinOp,
  ast.UnaryOp,
  ast.Name,
  ast.Constant,
  ast.Eq,
  ast.Add,
  ast.Sub,
  ast.Mult,
  ast.Div,
  ast.USub,
  ast.Load,
)


class InvalidRuleExpression(ValueError):
  """Raised when a rule expression cannot be parsed or evaluated safely."""


AVG_OPERAND_PREFIX = "__avg_"

_AVG_CALL_RE = re.compile(r"\bavg\(\s*\$([A-Za-z_]\w*)\s*\)")


def desugar_aggregates(expr: str) -> tuple[str, dict[str, str]]:
  """Rewrite ``avg($X)`` calls to synthesized ``$__avg_X`` operands.

  Returns the rewritten expression and a synthesized → base name map; the
  caller binds each as (begin + end) / 2. Any other form (``avg($A + $B)``)
  stays an ``ast.Call`` and is rejected by the whitelist.
  """
  synthesized: dict[str, str] = {}

  def _sub(match: re.Match[str]) -> str:
    base = match.group(1)
    name = f"{AVG_OPERAND_PREFIX}{base}"
    synthesized[name] = base
    return f"${name}"

  return _AVG_CALL_RE.sub(_sub, expr), synthesized


PRIOR_OPERAND_PREFIX = "__prior_"

_PRIOR_REF_RE = re.compile(r"\$([A-Za-z_]\w*)\[t-1\]")


def desugar_priors(expr: str) -> tuple[str, dict[str, str]]:
  """Rewrite ``$X[t-1]`` references to synthesized ``$__prior_X`` operands.

  Returns the rewritten expression and a synthesized → base name map; the
  caller binds each at the prior period. Any other bracket form stays an
  ``ast.Subscript`` and is rejected by the whitelist.
  """
  synthesized: dict[str, str] = {}

  def _sub(match: re.Match[str]) -> str:
    base = match.group(1)
    name = f"{PRIOR_OPERAND_PREFIX}{base}"
    synthesized[name] = base
    return f"${name}"

  return _PRIOR_REF_RE.sub(_sub, expr), synthesized


@dataclass
class ParsedExpression:
  tree: ast.Expression
  variable_names: list[str]


def _validate(node: ast.AST) -> None:
  for child in ast.walk(node):
    if not isinstance(child, _ALLOWED_NODES):
      if isinstance(child, ast.Call):
        raise InvalidRuleExpression(
          "disallowed AST node: Call — function calls are not allowed; "
          "the only aggregate form is avg($Var) in Derive expressions"
        )
      raise InvalidRuleExpression(f"disallowed AST node: {type(child).__name__}")


def require_single_equality(
  body: ast.expr, what: str = "equality pattern"
) -> ast.Compare:
  """Return ``body`` as a single ``LHS = RHS`` comparison, or raise.

  The whitelist doesn't bound the number of ``Eq`` operators, so
  ``$A = $B = $C`` must be rejected here.
  """
  if (
    not isinstance(body, ast.Compare)
    or len(body.ops) != 1
    or not isinstance(body.ops[0], ast.Eq)
  ):
    raise InvalidRuleExpression(
      f"{what} expects a single LHS = RHS expression, got: {ast.dump(body)}"
    )
  return body


def _normalize_equality(expr: str) -> str:
  """Replace XBRL-style bare ``=`` with ``==`` (leaving ``==``, ``<=``,
  ``>=``, ``!=`` alone)."""
  return re.sub(r"(?<![=<>!])=(?!=)", "==", expr)


_VARIABLE_REF_RE = re.compile(r"\$([A-Za-z_]\w*)")


def _bind_variables(expr: str, variable_names: list[str]) -> str:
  """Rewrite each ``$Name`` to ``_var_Name``, rejecting any unknown name.

  Tokenized, not substring-replaced, so a known name that prefixes an
  unknown one (``$Revenue`` vs ``$RevenueNet``) can't mask it. A stray
  ``$`` is caught by the leftover check.
  """
  known = set(variable_names)
  unknown: list[str] = []

  def _sub(match: re.Match[str]) -> str:
    name = match.group(1)
    if name not in known:
      unknown.append(name)
      return match.group(0)
    return f"_var_{name}"

  bound = _VARIABLE_REF_RE.sub(_sub, expr)

  if unknown:
    raise InvalidRuleExpression(
      f"unbound $Variable(s) in expression: {expr!r}: "
      f"{', '.join(sorted(set(unknown)))}. Known variables: {variable_names}"
    )
  if "$" in bound:
    raise InvalidRuleExpression(
      f"unbound $Variable in expression: {expr!r}. Known variables: {variable_names}"
    )
  return bound


def parse_arithmetic_expression(
  expr: str, variable_names: list[str]
) -> ParsedExpression:
  """Parse a rule expression string into a validated AST.

  1. Replaces each ``$Name`` token with ``_var_Name``, rejecting unknown names.
  2. Normalizes bare ``=`` to ``==`` (XBRL-style equality).
  3. Parses with ``ast.parse(mode='eval')``.
  4. Walks the tree and rejects any node outside the allowed whitelist.
  5. Dry-runs the evaluator to reject anything it could not evaluate.

  Step 5 matters because the whitelist is coarser than the evaluator (it
  admits any constant); without it a rule could save and then fail on every
  evaluation.

  Raises :class:`InvalidRuleExpression`.
  """
  preprocessed = _bind_variables(expr, variable_names)
  preprocessed = _normalize_equality(preprocessed)
  try:
    tree = ast.parse(preprocessed, mode="eval")
  except SyntaxError as exc:
    raise InvalidRuleExpression(f"syntax error in expression {expr!r}: {exc}") from exc
  _validate(tree)
  compare = require_single_equality(tree.body)
  _eval_arith(compare.left, {}, shape_only=True)
  _eval_arith(compare.comparators[0], {}, shape_only=True)
  return ParsedExpression(tree=tree, variable_names=variable_names)


def _eval_arith(
  node: ast.expr, values: dict[str, float], *, shape_only: bool = False
) -> float:
  """Recursively evaluate an arithmetic AST node to a float.

  With ``shape_only``, variables resolve to a placeholder, so the walk only
  checks the tree is evaluable. Authoring validation runs this rather than
  a second description of the grammar, which would drift.
  """
  if isinstance(node, ast.Constant):
    # bool is an int subclass; reject it explicitly.
    if isinstance(node.value, bool) or not isinstance(node.value, (int, float)):
      raise InvalidRuleExpression(f"non-numeric constant: {node.value!r}")
    return float(node.value)
  if isinstance(node, ast.Name):
    key = node.id
    if shape_only:
      # Not 0.0, which would trip the division guard.
      return 1.0
    if key not in values:
      raise InvalidRuleExpression(f"unbound name in expression: {key!r}")
    val = values[key]
    if val is None:
      raise InvalidRuleExpression(f"null value for variable {key!r}")
    return float(val)
  if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.USub):
    return -_eval_arith(node.operand, values, shape_only=shape_only)
  if isinstance(node, ast.BinOp):
    lhs = _eval_arith(node.left, values, shape_only=shape_only)
    rhs = _eval_arith(node.right, values, shape_only=shape_only)
    if isinstance(node.op, ast.Add):
      return lhs + rhs
    if isinstance(node.op, ast.Sub):
      return lhs - rhs
    if isinstance(node.op, ast.Mult):
      return lhs * rhs
    if isinstance(node.op, ast.Div):
      if rhs == 0:
        raise InvalidRuleExpression("division by zero in expression")
      return lhs / rhs
  raise InvalidRuleExpression(f"unexpected node in arithmetic: {ast.dump(node)}")


def evaluate_equality(
  parsed: ParsedExpression,
  values: dict[str, float],
  tolerance: float = EQUALITY_TOLERANCE,
) -> tuple[bool, float]:
  """Evaluate an equality expression and return ``(passed, residual)``.

  ``values`` is keyed by bare variable name and must cover every name in
  ``parsed.variable_names``.
  """
  compare = require_single_equality(parsed.tree.body)
  mapped: dict[str, float] = {}
  for name in parsed.variable_names:
    if name not in values:
      raise InvalidRuleExpression(f"missing value for variable {name!r}")
    mapped[f"_var_{name}"] = values[name]
  lhs = _eval_arith(compare.left, mapped)
  rhs = _eval_arith(compare.comparators[0], mapped)
  residual = abs(lhs - rhs)
  return residual <= tolerance, residual


def variable_names_in(node: ast.AST) -> list[str]:
  """Return the rule variable names (``$Name`` → ``Name``) used in a subtree,
  in ``ast.walk`` order."""
  names: list[str] = []
  for child in ast.walk(node):
    if isinstance(child, ast.Name) and child.id.startswith("_var_"):
      names.append(child.id[len("_var_") :])
  return names


def lhs_variable_names(parsed: ParsedExpression) -> list[str]:
  """Variable names on the left of the equality (a RollUp's parent, a
  Derive rule's target)."""
  return variable_names_in(require_single_equality(parsed.tree.body).left)


def evaluate_derivation(parsed: ParsedExpression, values: dict[str, float]) -> float:
  """Evaluate the RHS of a ``$Target = (expression)`` rule to a float.

  ``values`` needs only the RHS operands, keyed by variable name. Raises
  :class:`InvalidRuleExpression` for a non-equality expression, a missing
  or null operand, or division by zero.
  """
  compare = require_single_equality(parsed.tree.body, "derivation")
  mapped = {f"_var_{name}": value for name, value in values.items()}
  return _eval_arith(compare.comparators[0], mapped)


def build_rollup_expression(parent_name: str, children: list[tuple[str, float]]) -> str:
  """``$Parent = ($childA + $childB - $childC ...)``.

  Non-unit weights render as ``($child * w)``. The hand-maintained
  ``rs-gaap-rollup-rules/v1`` seed package must stay byte-identical to
  this output.
  """
  parts: list[str] = []
  for idx, (child_name, weight) in enumerate(children):
    var = f"${child_name}"
    if weight == 1.0:
      sign, term = "+", var
    elif weight == -1.0:
      sign, term = "-", var
    else:
      sign, term = "+", f"({var} * {weight})"
    if idx == 0:
      parts.append(term if sign == "+" else f"-{term}")
    else:
      parts.append(f"{sign} {term}")
  rhs = " ".join(parts)
  return f"${parent_name} = ({rhs})"


__all__ = [
  "AVG_OPERAND_PREFIX",
  "EQUALITY_TOLERANCE",
  "PRIOR_OPERAND_PREFIX",
  "InvalidRuleExpression",
  "ParsedExpression",
  "build_rollup_expression",
  "desugar_aggregates",
  "desugar_priors",
  "evaluate_derivation",
  "evaluate_equality",
  "lhs_variable_names",
  "parse_arithmetic_expression",
  "variable_names_in",
]
