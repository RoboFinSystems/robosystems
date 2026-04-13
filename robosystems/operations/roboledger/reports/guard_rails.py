"""Guard rails — structural and semantic validation for generated reports.

Structural checks are deterministic arithmetic (hard failures).
Semantic checks are pattern matching against known report structures (warnings).
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .fact_grid import FactRow

# Rounding tolerance for balance checks (dollars)
_TOLERANCE = 0.01


@dataclass
class ValidationResult:
  """Result of guard rail validation."""

  passed: bool = True
  checks: list[str] = field(default_factory=list)
  failures: list[str] = field(default_factory=list)
  warnings: list[str] = field(default_factory=list)


def validate_report(structure_type: str, rows: list[FactRow]) -> ValidationResult:
  """Run structural and semantic validation for a rendered structure.

  Note: `cash_flow_statement` is intentionally not handled — the
  roboledger renderer isn't implemented yet. When it is, re-wire the
  `_validate_cash_flow` helper below.
  """
  if structure_type == "income_statement":
    return _validate_income_statement(rows)
  elif structure_type == "balance_sheet":
    return _validate_balance_sheet(rows)
  return ValidationResult(checks=["no_validation_rules"])


# ── Structural checks ─────────────────────────────────────────────────────


def _validate_income_statement(rows: list[FactRow]) -> ValidationResult:
  result = ValidationResult()

  # Check: totals foot (subtotals equal sum of children)
  _check_totals_foot(rows, result)

  # Semantic: check for standard line items
  qnames = {r.element_qname for r in rows}

  result.checks.append("standard_line_items")
  if not any("Revenue" in q for q in qnames):
    result.warnings.append("No revenue line item found")
  if not any("CostOfRevenue" in q for q in qnames):
    result.warnings.append(
      "No Cost of Revenue — confirm this is correct for your business"
    )
  if not any("NetIncome" in q or "NetLoss" in q for q in qnames):
    result.warnings.append("No Net Income line item found")

  # Semantic: check for zero-balance subtotals
  _check_zero_subtotals(rows, result)

  # Semantic: check for comparative data
  _check_comparative_data(rows, result)

  return result


def _validate_balance_sheet(rows: list[FactRow]) -> ValidationResult:
  result = ValidationResult()

  # Check: totals foot
  _check_totals_foot(rows, result)

  # Structural: Assets = Liabilities + Equity
  total_assets = 0.0
  total_liabilities = 0.0
  total_equity = 0.0

  for row in rows:
    if row.is_subtotal and row.depth == 0:
      val = (row.values[0] or 0.0) if row.values else 0.0
      if row.classification == "asset":
        total_assets += val
      elif row.classification == "liability":
        total_liabilities += val
      elif row.classification == "equity":
        total_equity += val

  result.checks.append("accounting_equation")
  diff = abs(total_assets - (total_liabilities + total_equity))
  if diff > _TOLERANCE and (
    total_assets != 0 or total_liabilities != 0 or total_equity != 0
  ):
    result.failures.append(
      f"Balance sheet does not balance: Assets ({total_assets:.2f}) "
      f"≠ Liabilities ({total_liabilities:.2f}) + Equity ({total_equity:.2f}), "
      f"difference: {diff:.2f}"
    )
    result.passed = False

  # Semantic checks
  _check_zero_subtotals(rows, result)
  _check_comparative_data(rows, result)

  return result


def _validate_cash_flow(  # pyright: ignore[reportUnusedFunction]
  rows: list[FactRow],
) -> ValidationResult:
  """Cash flow validation — retained for when the renderer is implemented.

  Currently unreferenced; `validate_report` does not dispatch to this
  function. Wire it back into `validate_report` when the CF renderer
  lands in fact_grid and drop the `reportUnusedFunction` ignore above.
  """
  result = ValidationResult()

  _check_totals_foot(rows, result)
  _check_zero_subtotals(rows, result)
  _check_comparative_data(rows, result)

  return result


# ── Shared check helpers ──────────────────────────────────────────────────


def _check_totals_foot(rows: list[FactRow], result: ValidationResult) -> None:
  """Verify that subtotal rows equal the sum of their children.

  Uses the row ordering: a subtotal row is followed by its children
  (which have depth = subtotal.depth + 1) until the next row at the
  same or lower depth.
  """
  result.checks.append("totals_foot")

  subtotal_indices = [i for i, r in enumerate(rows) if r.is_subtotal]

  for idx in subtotal_indices:
    subtotal = rows[idx]
    child_sum = 0.0

    # Children follow the subtotal and have depth = subtotal.depth + 1
    for j in range(idx + 1, len(rows)):
      child = rows[j]
      if child.depth <= subtotal.depth:
        break
      # Only sum direct children (depth = subtotal.depth + 1),
      # not grandchildren — grandchildren are already rolled into their parent subtotals
      if child.depth == subtotal.depth + 1:
        child_sum += (child.values[0] or 0.0) if child.values else 0.0

    subtotal_val = (subtotal.values[0] or 0.0) if subtotal.values else 0.0
    diff = abs(subtotal_val - child_sum)
    if diff > _TOLERANCE and child_sum != 0.0:
      result.warnings.append(
        f"Subtotal '{subtotal.element_name}' ({subtotal_val:.2f}) "
        f"does not match sum of children ({child_sum:.2f}), "
        f"difference: {diff:.2f}"
      )


def _check_zero_subtotals(rows: list[FactRow], result: ValidationResult) -> None:
  """Warn about zero-balance subtotal sections."""
  result.checks.append("zero_subtotals")
  for row in rows:
    val = (row.values[0] or 0.0) if row.values else 0.0
    if row.is_subtotal and val == 0.0 and row.depth <= 1:
      result.warnings.append(f"Section '{row.element_name}' has zero balance")


def _check_comparative_data(rows: list[FactRow], result: ValidationResult) -> None:
  """Warn if multi-period data has empty columns."""
  result.checks.append("comparative_data")
  if not rows or not rows[0].values or len(rows[0].values) < 2:
    return
  # Check each period column beyond the first for all-zero
  for col_idx in range(1, len(rows[0].values)):
    all_zero = all(
      (r.values[col_idx] if col_idx < len(r.values) else 0.0) == 0.0 for r in rows
    )
    if all_zero:
      result.warnings.append(
        f"Period column {col_idx + 1} has no data — column will be empty"
      )
