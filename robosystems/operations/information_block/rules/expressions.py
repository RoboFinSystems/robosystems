"""Safe arithmetic expression parser and evaluator for rule expressions.

Rule expressions use a tight subset of arithmetic + equality:
  ``$Assets = ($Liabilities + $Equity)``

The ``$Name`` variable syntax is preprocessed to ``_var_Name`` before
parsing so ``ast.parse`` treats them as legal Python identifiers. A
whitelist walker then rejects any AST node that isn't in the allowed
set — no function calls, attribute access, subscripts, or other
constructs that could be exploited.

``eval()`` is never called. The AST walker evaluates the tree directly
by recursing over :class:`ast.BinOp` / :class:`ast.UnaryOp` nodes.

Aggregates are desugared, never evaluated: ``avg($X)`` rewrites to a
synthesized ``$__avg_X`` operand *before* parsing
(:func:`desugar_aggregates`), so the whitelist keeps rejecting every
``ast.Call`` and the evaluator stays pure arithmetic — averaging happens
at bind time in the caller, where the begin + end facts live. A future
``$X[t-1]`` prior-period operand rides the same mechanism (rewrite to a
synthesized operand, bind at a resolved prior period) — see
``compute-metrics``.
"""

from __future__ import annotations

import ast
import re
from dataclasses import dataclass
from typing import Any

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

  Returns the rewritten expression plus a map of synthesized variable
  name → base variable name. Callers append the synthesized names to
  the parse variable list and bind each one at evaluation time (the
  period-average of the base operand: begin + end over 2).

  Only the exact single-variable form matches; anything else —
  ``avg($A + $B)``, ``median($X)`` — survives as a genuine
  :class:`ast.Call` and is rejected by the whitelist walker, so the
  no-function-calls security posture is unchanged.
  """
  synthesized: dict[str, str] = {}

  def _sub(match: re.Match[str]) -> str:
    base = match.group(1)
    name = f"{AVG_OPERAND_PREFIX}{base}"
    synthesized[name] = base
    return f"${name}"

  return _AVG_CALL_RE.sub(_sub, expr), synthesized


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


def _normalize_equality(expr: str) -> str:
  """Replace bare ``=`` with ``==`` for Python's parser.

  Rule expressions use XBRL-style single ``=`` for equality (e.g.
  ``$Assets = ($L + $E)``). Python's AST requires ``==``. This
  replaces ``=`` that isn't already part of ``==``, ``<=``, ``>=``,
  or ``!=``.
  """
  return re.sub(r"(?<![=<>!])=(?!=)", "==", expr)


def parse_arithmetic_expression(
  expr: str, variable_names: list[str]
) -> ParsedExpression:
  """Parse a rule expression string into a validated AST.

  1. Replaces ``$Name`` with ``_var_Name``.
  2. Normalizes bare ``=`` to ``==`` (XBRL-style equality).
  3. Parses with ``ast.parse(mode='eval')``.
  4. Walks the tree and rejects any node outside the allowed whitelist.

  Raises :class:`InvalidRuleExpression` for unbound variables, syntax
  errors, or disallowed constructs.
  """
  preprocessed = expr
  for name in variable_names:
    preprocessed = preprocessed.replace(f"${name}", f"_var_{name}")
  if "$" in preprocessed:
    raise InvalidRuleExpression(
      f"unbound $Variable in expression: {expr!r}. Known variables: {variable_names}"
    )
  preprocessed = _normalize_equality(preprocessed)
  try:
    tree = ast.parse(preprocessed, mode="eval")
  except SyntaxError as exc:
    raise InvalidRuleExpression(f"syntax error in expression {expr!r}: {exc}") from exc
  _validate(tree)
  return ParsedExpression(tree=tree, variable_names=variable_names)


def _eval_arith(node: ast.expr, values: dict[str, float]) -> float:
  """Recursively evaluate an arithmetic AST node to a float."""
  if isinstance(node, ast.Constant):
    if not isinstance(node.value, (int, float)):
      raise InvalidRuleExpression(f"non-numeric constant: {node.value!r}")
    return float(node.value)
  if isinstance(node, ast.Name):
    key = node.id
    if key not in values:
      raise InvalidRuleExpression(f"unbound name in expression: {key!r}")
    val = values[key]
    if val is None:
      raise InvalidRuleExpression(f"null value for variable {key!r}")
    return float(val)
  if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.USub):
    return -_eval_arith(node.operand, values)
  if isinstance(node, ast.BinOp):
    lhs = _eval_arith(node.left, values)
    rhs = _eval_arith(node.right, values)
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

  Expects ``parsed.tree.body`` to be an :class:`ast.Compare` with a
  single :class:`ast.Eq` operator. ``values`` maps ``_var_Name`` →
  float for each variable name in ``parsed.variable_names``.

  The ``tolerance`` parameter defaults to :data:`EQUALITY_TOLERANCE`
  (``0.01``); callers can pass a rule-specific override.
  """
  compare = parsed.tree.body
  if (
    not isinstance(compare, ast.Compare)
    or len(compare.ops) != 1
    or not isinstance(compare.ops[0], ast.Eq)
  ):
    raise InvalidRuleExpression(
      f"equality pattern expects a single LHS = RHS expression, got: "
      f"{ast.dump(compare)}"
    )
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
  """Return the rule variable names (``$Name`` → ``Name``) used in a subtree.

  Walks the AST for ``_var_`` identifiers (the preprocessed form of
  ``$Name``) and strips the prefix. Order follows ``ast.walk``.
  """
  names: list[str] = []
  for child in ast.walk(node):
    if isinstance(child, ast.Name) and child.id.startswith("_var_"):
      names.append(child.id[len("_var_") :])
  return names


def lhs_variable_names(parsed: ParsedExpression) -> list[str]:
  """Variable names on the left of the equality — the subtotal being checked.

  Used by the ``RollUp`` evaluator to distinguish the parent subtotal
  (which must have a bound fact) from the RHS children (a missing child
  is treated as 0, matching the renderer's sum-of-present-children).
  """
  body = parsed.tree.body
  if (
    not isinstance(body, ast.Compare)
    or len(body.ops) != 1
    or not isinstance(body.ops[0], ast.Eq)
  ):
    raise InvalidRuleExpression(
      f"expected a single LHS = RHS expression, got: {ast.dump(body)}"
    )
  return variable_names_in(body.left)


def evaluate_arithmetic(parsed: ParsedExpression, values: dict[str, float]) -> float:
  """Evaluate a single arithmetic expression (no equality) to a float.

  Used by future derivation evaluators (metric blocks, etc.) that need
  a numeric result rather than a pass/fail outcome.
  """
  mapped: dict[str, Any] = {
    f"_var_{name}": values[name] for name in parsed.variable_names
  }
  return _eval_arith(parsed.tree.body, mapped)


def evaluate_derivation(parsed: ParsedExpression, values: dict[str, float]) -> float:
  """Evaluate the RHS of a ``$Target = (expression)`` rule to a float.

  The compute path for ``Derive`` rules (compute-metrics): the LHS names
  the element being computed, so only the RHS operands need bound values
  — pass ``values`` keyed by RHS variable name. Raises
  :class:`InvalidRuleExpression` for a non-equality expression, a missing
  or null operand, or division by zero.
  """
  compare = parsed.tree.body
  if (
    not isinstance(compare, ast.Compare)
    or len(compare.ops) != 1
    or not isinstance(compare.ops[0], ast.Eq)
  ):
    raise InvalidRuleExpression(
      f"derivation expects a single LHS = RHS expression, got: {ast.dump(compare)}"
    )
  mapped = {f"_var_{name}": value for name, value in values.items()}
  return _eval_arith(compare.comparators[0], mapped)


def build_rollup_expression(parent_name: str, children: list[tuple[str, float]]) -> str:
  """``$Parent = ($childA + $childB - $childC ...)``.

  Weight +1 -> ``+``, -1 -> ``-``, otherwise an explicit ``* weight``
  term (``($child * 0.5)``) so non-unit calc weights survive into the
  frozen expression. Shared by the seed rollup-rule generator
  (``taxonomy/scripts/generate_rollup_rules.py``) and tenant auto-rule
  emission (``operations/taxonomy_block/auto_rules.py``); callers pass
  final variable names — any qname-to-name mapping happens upstream.
  """
  parts: list[str] = []
  for idx, (child_name, weight) in enumerate(children):
    var = f"${child_name}"
    if weight == 1.0:
      sign, term = "+", var
    elif weight == -1.0:
      sign, term = "-", var
    else:
      # Render -0.0 cleanly and keep weight literal for non-unit weights.
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
  "InvalidRuleExpression",
  "ParsedExpression",
  "build_rollup_expression",
  "desugar_aggregates",
  "evaluate_arithmetic",
  "evaluate_derivation",
  "evaluate_equality",
  "lhs_variable_names",
  "parse_arithmetic_expression",
  "variable_names_in",
]
