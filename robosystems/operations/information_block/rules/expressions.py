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
at bind time in the caller, where the begin + end facts live. The
``$X[t-1]`` prior-period reference rides the same mechanism:
:func:`desugar_priors` rewrites it to a synthesized ``$__prior_X``
operand pre-parse, and the caller binds it at the resolved prior period
— ``compute-forecast`` binds the previous forward month's value in its
walk. ``ast.Subscript`` stays outside the
whitelist, so any other bracket form (``$X[t-2]``, ``$X[0]``) is
structurally rejected — the grammar ceiling is ``[t-1]`` + ``avg()``.
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


PRIOR_OPERAND_PREFIX = "__prior_"

_PRIOR_REF_RE = re.compile(r"\$([A-Za-z_]\w*)\[t-1\]")


def desugar_priors(expr: str) -> tuple[str, dict[str, str]]:
  """Rewrite ``$X[t-1]`` references to synthesized ``$__prior_X`` operands.

  The prior-period half of the forecast grammar, sibling to ``avg()``.
  Returns the rewritten expression plus a map of synthesized variable
  name → base variable name; callers append the synthesized names to
  the parse variable list and bind each one at the resolved prior
  period — the forecast walk binds the previous forward month's value
  (seeded from actuals at the base period).

  Only the exact ``[t-1]`` form matches. Any other bracket construct —
  ``$X[t-2]``, ``$X[0]``, ``$X[t]`` — survives as a genuine
  :class:`ast.Subscript`, which is outside the whitelist and rejected:
  the grammar ceiling stays enforced structurally, same as the
  no-function-calls posture for aggregates.
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

  The whitelist walker permits ``ast.Compare`` and ``ast.Eq`` without
  bounding how many of each, so ``$A = $B = $C`` is structurally legal
  while every consumer here requires exactly one operator. This was
  open-coded in four places — parse, equality evaluation, derivation
  evaluation, and LHS extraction — which is why the parser never grew the
  check the other three all assumed. ``what`` keeps each caller's wording.
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
  """Replace bare ``=`` with ``==`` for Python's parser.

  Rule expressions use XBRL-style single ``=`` for equality (e.g.
  ``$Assets = ($L + $E)``). Python's AST requires ``==``. This
  replaces ``=`` that isn't already part of ``==``, ``<=``, ``>=``,
  or ``!=``.
  """
  return re.sub(r"(?<![=<>!])=(?!=)", "==", expr)


_VARIABLE_REF_RE = re.compile(r"\$([A-Za-z_]\w*)")


def _bind_variables(expr: str, variable_names: list[str]) -> str:
  """Rewrite each ``$Name`` to ``_var_Name``, rejecting any unknown name.

  Tokenized rather than substring-replaced. Replacing ``$`` + each known name
  in turn let an unknown name through whenever a known one was a prefix of it:
  with ``Revenue`` bound, ``$RevenueNet`` became ``_var_RevenueNet``, no ``$``
  survived to trip the unbound check, and the rule saved cleanly — then raised
  ``unbound name`` on every evaluation for the rest of its life. That is the
  authoring-vs-evaluation split this module exists to close.

  A trailing ``$`` or one followed by a non-identifier character matches no
  token and is caught by the leftover check.
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

  Step 5 is what keeps authoring and evaluation from describing different
  grammars. The whitelist is structural — it exists to keep ``Call``,
  ``Subscript`` and friends out — and is deliberately coarser than the
  evaluator: it admits any ``ast.Constant`` and any number of ``Eq``
  operators. Anything in that gap used to save cleanly and then raise on
  every evaluation for the life of the rule.

  Raises :class:`InvalidRuleExpression` for unbound variables, syntax
  errors, or disallowed constructs.
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

  With ``shape_only``, variables resolve to a placeholder instead of a
  bound value, so the walk checks only whether the tree is *evaluable*.
  That is how :func:`parse_arithmetic_expression` validates at authoring
  time — by running this function rather than by maintaining a second
  description of the same grammar. The two used to be separate, and every
  shape one accepted and the other rejected produced a rule that saved
  cleanly and then raised on every evaluation, forever.
  """
  if isinstance(node, ast.Constant):
    # bool first: isinstance(True, int) is True in Python, so without this
    # `$Assets = True` passes the numeric check and silently evaluates as 1.0.
    if isinstance(node.value, bool) or not isinstance(node.value, (int, float)):
      raise InvalidRuleExpression(f"non-numeric constant: {node.value!r}")
    return float(node.value)
  if isinstance(node, ast.Name):
    key = node.id
    if shape_only:
      # Not 0.0 — that would trip the division guard below on a tree whose
      # only fault is that we haven't bound values yet.
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

  Expects ``parsed.tree.body`` to be an :class:`ast.Compare` with a
  single :class:`ast.Eq` operator. ``values`` maps ``_var_Name`` →
  float for each variable name in ``parsed.variable_names``.

  The ``tolerance`` parameter defaults to :data:`EQUALITY_TOLERANCE`
  (``0.01``); callers can pass a rule-specific override.
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
  return variable_names_in(require_single_equality(parsed.tree.body).left)


def evaluate_derivation(parsed: ParsedExpression, values: dict[str, float]) -> float:
  """Evaluate the RHS of a ``$Target = (expression)`` rule to a float.

  The compute path for ``Derive`` rules (compute-metrics): the LHS names
  the element being computed, so only the RHS operands need bound values
  — pass ``values`` keyed by RHS variable name. Raises
  :class:`InvalidRuleExpression` for a non-equality expression, a missing
  or null operand, or division by zero.
  """
  compare = require_single_equality(parsed.tree.body, "derivation")
  mapped = {f"_var_{name}": value for name, value in values.items()}
  return _eval_arith(compare.comparators[0], mapped)


def build_rollup_expression(parent_name: str, children: list[tuple[str, float]]) -> str:
  """``$Parent = ($childA + $childB - $childC ...)``.

  Weight +1 -> ``+``, -1 -> ``-``, otherwise an explicit ``* weight``
  term (``($child * 0.5)``) so non-unit calc weights survive into the
  frozen expression. Used by tenant auto-rule emission
  (``operations/taxonomy_block/auto_rules.py``) and by the hand-maintained
  ``rs-gaap-rollup-rules/v1`` seed package, whose expressions must stay
  byte-identical to what this produces; callers pass final variable names —
  any qname-to-name mapping happens upstream.
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
