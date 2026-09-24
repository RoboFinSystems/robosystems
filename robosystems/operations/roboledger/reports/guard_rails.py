"""Guard rails — structural and semantic validation for generated reports.

Structural checks are arithmetic (failures); semantic checks are warnings.
Every check runs once per rendered period column, and on a multi-column
statement each finding names its column (``[Prior] Balance sheet …``).
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field

from .fact_grid import (
  _CF_OPERATING_SUBTOTAL_QNAME,
  _CF_PLUG_WARN_RATIO,
  _CF_RECONCILING_LEAF_QNAME,
  FactRow,
  _infer_classification,
)

# Rounding tolerance for balance checks (dollars)
_TOLERANCE = 0.01

STATUS_PASSED = "passed"
STATUS_FAILED = "failed"
STATUS_INCONCLUSIVE = "inconclusive"


@dataclass
class ValidationResult:
  """Result of guard rail validation.

  ``status`` is ``passed``, ``failed``, or ``inconclusive`` (no rules exist
  for the block type). ``passed`` is True only for ``status == "passed"``.
  """

  passed: bool = True
  status: str = STATUS_PASSED
  checks: list[str] = field(default_factory=list)
  failures: list[str] = field(default_factory=list)
  warnings: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class _Column:
  """One rendered period column: its index into ``FactRow.values`` + a name."""

  index: int
  label: str
  # Empty on a single-column statement; "[<label>] " otherwise.
  prefix: str


_Validator = Callable[[list[FactRow], ValidationResult, _Column], None]


def validate_report(
  block_type: str,
  rows: list[FactRow],
  period_labels: list[str] | None = None,
) -> ValidationResult:
  """Run structural and semantic validation for a rendered structure.

  ``period_labels`` names the columns of ``rows[*].values``; missing labels
  fall back to ``column N``. Block types with no validators
  (``equity_statement``, ``comprehensive_income``) come back ``inconclusive``.
  """
  validator = _VALIDATORS.get(block_type)
  if validator is None:
    return ValidationResult(
      passed=False,
      status=STATUS_INCONCLUSIVE,
      checks=["no_validation_rules"],
      warnings=[f"No validation rules exist for '{block_type}' — nothing was checked."],
    )

  result = ValidationResult()
  columns = _columns(rows, period_labels)
  empty_columns = _empty_column_indexes(rows, columns)
  for column in columns:
    validator(rows, result, column)
    # A comparative column that is empty end to end is reported once by
    # ``_check_comparative_data``; per-section zero warnings on it are noise.
    if column.index == 0 or column.index not in empty_columns:
      _check_zero_subtotals(rows, result, column)
  _check_comparative_data(result, columns, empty_columns)

  result.passed = not result.failures
  result.status = STATUS_FAILED if result.failures else STATUS_PASSED
  return result


# ── Column helpers ────────────────────────────────────────────────────────


def _columns(rows: list[FactRow], period_labels: list[str] | None) -> list[_Column]:
  count = max((len(r.values) for r in rows), default=0) or 1
  columns: list[_Column] = []
  for index in range(count):
    label = ""
    if period_labels and index < len(period_labels):
      label = period_labels[index] or ""
    label = label or f"column {index + 1}"
    prefix = "" if count == 1 else f"[{label}] "
    columns.append(_Column(index=index, label=label, prefix=prefix))
  return columns


def _empty_column_indexes(rows: list[FactRow], columns: list[_Column]) -> set[int]:
  return {
    column.index
    for column in columns
    if all(_value(r, column.index) == 0.0 for r in rows)
  }


def _value(row: FactRow, col: int) -> float:
  """The row's value in column ``col`` — ``None`` and missing read as zero."""
  if col >= len(row.values):
    return 0.0
  return row.values[col] or 0.0


def _note_check(result: ValidationResult, name: str) -> None:
  """Record that a rule ran — once, however many columns it ran over."""
  if name not in result.checks:
    result.checks.append(name)


def _fail(result: ValidationResult, message: str) -> None:
  """Record a failure, deduplicated so column-independent findings appear once."""
  if message not in result.failures:
    result.failures.append(message)
  result.passed = False


# ── Structural checks ─────────────────────────────────────────────────────


def _validate_income_statement(
  rows: list[FactRow], result: ValidationResult, column: _Column
) -> None:
  _check_totals_foot(rows, result, column, sign_by_balance=True)

  # Net Income = Σ credit leaves - Σ debit leaves. Leaves avoid
  # double-counting and catch nonoperating items; balance_type (not
  # classification) nets contras correctly.
  _note_check(result, "net_income_equation")

  net_income_row = _net_income_row(rows)

  implied_ni = 0.0
  credit_leaves = 0
  debit_leaves = 0
  for row in rows:
    # The NI row is excluded by identity, even if is_subtotal is unset.
    if row.is_subtotal or row.is_abstract or row is net_income_row:
      continue
    value = _value(row, column.index)
    if row.balance_type == "credit":
      implied_ni += value
      credit_leaves += 1
    elif row.balance_type == "debit":
      implied_ni -= value
      debit_leaves += 1
    # Per-share / ratio metrics have no balance type.

  inconclusive = False
  if credit_leaves == 0 and debit_leaves == 0:
    # Subtotal-only statement: fall back to top-most Revenue - Expenses.
    revenue_row = _top_most_subtotal_for_classification(rows, "revenue", column.index)
    expense_row = _top_most_subtotal_for_classification(rows, "expense", column.index)
    if revenue_row is None or expense_row is None:
      missing: list[str] = []
      if revenue_row is None:
        missing.append("revenue")
      if expense_row is None:
        missing.append("expense")
      _fail(
        result,
        "Income statement validation inconclusive: missing classification "
        f"rollups for {missing}. Wire FASB elementsOfFinancialStatements "
        "traits onto the structure's elements, or ensure at least one "
        "subtotal row per classification is present.",
      )
      inconclusive = True
    else:
      implied_ni = _value(revenue_row, column.index) - _value(expense_row, column.index)
  elif credit_leaves == 0:
    _fail(
      result,
      "Income statement validation inconclusive: no revenue/income line "
      "found to reconcile Net Income against.",
    )
    inconclusive = True

  if inconclusive:
    return

  if net_income_row is not None:
    reported_ni = _value(net_income_row, column.index)
    diff = abs(reported_ni - implied_ni)
    if diff > _TOLERANCE:
      _fail(
        result,
        f"{column.prefix}Net Income mismatch: reported "
        f"'{net_income_row.element_name}' ({reported_ni:.2f}) ≠ "
        f"Σ(income − expense) ({implied_ni:.2f}), difference: {diff:.2f}",
      )
  elif implied_ni != 0.0:
    # Warning only: multi-step structures often end on another line.
    result.warnings.append(
      f"{column.prefix}No Net Income line found; implied NI = "
      f"Σ(income − expense) = {implied_ni:.2f}"
    )


def _top_most_subtotal_for_classification(
  rows: list[FactRow], classification: str, col: int = 0
) -> FactRow | None:
  """Pick the highest-priority row matching ``classification``.

  Priority order:

  1. Smallest depth wins.
  2. At equal depth, a subtotal beats a leaf (a leaf is still accepted: a
     single-step statement may report ``Revenues`` as a leaf).
  3. Then the larger absolute value in column ``col``, so a zero placeholder
     never beats the real rollup.

  Combined L+E rollups are skipped. Classification falls back to qname
  inference when ``row.classification`` is empty.
  """
  best: FactRow | None = None
  for row in rows:
    qname_lower = (row.element_qname or "").lower()
    if "liabilit" in qname_lower and "equity" in qname_lower:
      continue
    row_class = row.classification or _infer_classification(
      row.element_qname, row.balance_type
    )
    if row_class != classification:
      continue
    if best is None:
      best = row
      continue
    row_val = abs(_value(row, col))
    best_val = abs(_value(best, col))
    if row.depth < best.depth:
      best = row
    elif row.depth == best.depth:
      # Subtotal beats leaf at the same depth.
      if (row.is_subtotal and not best.is_subtotal) or (
        row.is_subtotal == best.is_subtotal and row_val > best_val
      ):
        best = row
  return best


def _net_income_row(rows: list[FactRow]) -> FactRow | None:
  """Find the row that reports Net Income (or Net Loss).

  Matches by qname token; among several, prefers a subtotal at the smallest
  depth.
  """
  candidates = [
    r
    for r in rows
    if "netincome" in (r.element_qname or "").lower()
    or "netloss" in (r.element_qname or "").lower()
  ]
  if not candidates:
    return None
  candidates.sort(key=lambda r: (not r.is_subtotal, r.depth))
  return candidates[0]


def _validate_balance_sheet(
  rows: list[FactRow], result: ValidationResult, column: _Column
) -> None:
  _check_totals_foot(rows, result, column, sign_by_balance=True)

  # Assets = Liabilities + Equity, using the top-most row per classification.
  _note_check(result, "accounting_equation")

  candidates: dict[str, FactRow] = {}
  for cls in ("asset", "liability", "equity"):
    pick = _top_most_subtotal_for_classification(rows, cls, column.index)
    if pick is not None:
      candidates[cls] = pick

  required = {"asset", "liability", "equity"}
  missing = required - candidates.keys()
  if missing:
    # No silent pass when a classification total is missing.
    _fail(
      result,
      "Balance sheet validation inconclusive: missing classification "
      f"rollups for {sorted(missing)}. Wire FASB elementsOfFinancialStatements "
      "traits onto the structure's elements, or ensure at least one "
      "subtotal row per classification is present.",
    )
    return

  total_assets = _value(candidates["asset"], column.index)
  total_liabilities = _value(candidates["liability"], column.index)
  total_equity = _value(candidates["equity"], column.index)
  diff = abs(total_assets - (total_liabilities + total_equity))
  if diff > _TOLERANCE:
    _fail(
      result,
      f"{column.prefix}Balance sheet does not balance: Assets "
      f"({total_assets:.2f}) ≠ Liabilities ({total_liabilities:.2f}) + "
      f"Equity ({total_equity:.2f}), difference: {diff:.2f}",
    )


def _validate_cash_flow(
  rows: list[FactRow], result: ValidationResult, column: _Column
) -> None:
  """Structural footing of the rendered cash flow statement.

  The ΔCash tie to the balance sheet is checked at the fact-bundle level
  (``fact_grid._check_cash_flow_tie_out``); rendered rows carry no cash
  balances.
  """
  # Cash-flow rows are cash-effect signed, so sections foot as plain sums.
  _check_totals_foot(rows, result, column, sign_by_balance=False)
  _check_operating_plug(rows, result, column)


_VALIDATORS: dict[str, _Validator] = {
  "income_statement": _validate_income_statement,
  "balance_sheet": _validate_balance_sheet,
  "cash_flow_statement": _validate_cash_flow,
}


# ── Shared check helpers ──────────────────────────────────────────────────


def _check_totals_foot(
  rows: list[FactRow],
  result: ValidationResult,
  column: _Column,
  *,
  sign_by_balance: bool,
) -> None:
  """Verify that subtotal rows equal the sum of their children.

  Rows are post-order (children, then their subtotal), so a subtotal's
  direct children are the preceding rows at depth + 1, back to the first row
  at its own depth or shallower.

  ``sign_by_balance`` applies the XBRL calculation-weight rule: a child whose
  balance type differs from its parent's enters at -1 (a contra, interest
  expense under nonoperating income). Unknown balance types enter at +1.

  Subtotals with no presentation children (Gross Profit) sum to zero and are
  skipped; the calc DAG and net-income equation cover them.
  """
  _note_check(result, "totals_foot")

  for idx, subtotal in enumerate(rows):
    if not subtotal.is_subtotal:
      continue
    if not any(v is not None for v in subtotal.values):
      continue
    child_sum = 0.0

    for j in range(idx - 1, -1, -1):
      child = rows[j]
      if child.depth <= subtotal.depth:
        break
      if child.depth == subtotal.depth + 1:
        child_sum += _child_weight(child, subtotal, sign_by_balance) * _value(
          child, column.index
        )

    subtotal_val = _value(subtotal, column.index)
    diff = abs(subtotal_val - child_sum)
    if diff > _TOLERANCE and child_sum != 0.0:
      result.warnings.append(
        f"{column.prefix}Subtotal '{subtotal.element_name}' ({subtotal_val:.2f}) "
        f"does not match sum of children ({child_sum:.2f}), "
        f"difference: {diff:.2f}"
      )


def _child_weight(child: FactRow, subtotal: FactRow, sign_by_balance: bool) -> float:
  if not sign_by_balance:
    return 1.0
  parent = subtotal.balance_type
  kid = child.balance_type
  if parent not in ("credit", "debit") or kid not in ("credit", "debit"):
    return 1.0
  return 1.0 if kid == parent else -1.0


def _check_zero_subtotals(
  rows: list[FactRow], result: ValidationResult, column: _Column
) -> None:
  """Warn about zero-balance subtotal sections."""
  _note_check(result, "zero_subtotals")
  for row in rows:
    if row.is_subtotal and row.depth <= 1 and _value(row, column.index) == 0.0:
      result.warnings.append(
        f"{column.prefix}Section '{row.element_name}' has zero balance"
      )


def _check_comparative_data(
  result: ValidationResult, columns: list[_Column], empty_columns: set[int]
) -> None:
  """Warn if multi-period data has empty columns."""
  _note_check(result, "comparative_data")
  for column in columns[1:]:
    if column.index in empty_columns:
      result.warnings.append(
        f"Period '{column.label}' has no data — column will be empty"
      )


def _check_operating_plug(
  rows: list[FactRow], result: ValidationResult, column: _Column
) -> None:
  """Warn when the operating-CF reconciling plug is large vs operating cash.

  The plug makes the indirect CF foot to actual cash by construction, so it
  silently absorbs misclassified flows too; a plug that dwarfs operating cash
  signals an un-itemized or mis-tagged item. The row may also carry
  tenant-mapped content, so its value only approximates the plug.
  """
  _note_check(result, "operating_plug")
  plug = next((r for r in rows if r.element_qname == _CF_RECONCILING_LEAF_QNAME), None)
  op = next((r for r in rows if r.element_qname == _CF_OPERATING_SUBTOTAL_QNAME), None)
  if plug is None or op is None:
    return
  plug_val = _value(plug, column.index)
  if abs(plug_val) <= _TOLERANCE:
    return
  op_val = _value(op, column.index)
  if abs(op_val) < _TOLERANCE or abs(plug_val) > _CF_PLUG_WARN_RATIO * abs(op_val):
    result.warnings.append(
      f"{column.prefix}Operating cash flow carries a large unattributed "
      f"reconciling adjustment in 'Other operating capital, net' "
      f"({plug_val:.2f} vs operating cash {op_val:.2f}) — likely an "
      f"un-itemized non-cash item (gain/loss on disposal, etc.) or a flow "
      f"misclassification; review."
    )
