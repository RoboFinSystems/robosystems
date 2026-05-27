"""Guard rails — structural and semantic validation for generated reports.

Structural checks are deterministic arithmetic (hard failures).
Semantic checks are pattern matching against known report structures (warnings).
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .fact_grid import FactRow, _infer_classification

# Rounding tolerance for balance checks (dollars)
_TOLERANCE = 0.01


@dataclass
class ValidationResult:
  """Result of guard rail validation."""

  passed: bool = True
  checks: list[str] = field(default_factory=list)
  failures: list[str] = field(default_factory=list)
  warnings: list[str] = field(default_factory=list)


def validate_report(block_type: str, rows: list[FactRow]) -> ValidationResult:
  """Run structural and semantic validation for a rendered structure.

  `_check_totals_foot` (shared) is the load-bearing CF check: it verifies the
  net-change-in-cash line foots to Op + Inv + Fin and that each section foots
  to its leaves — i.e. the investing/financing flows actually roll up. The
  cross-statement ΔCash reconciliation (CF net-change == BS cash movement)
  lives at the fact-bundle level in ``fact_grid._check_cash_flow_tie_out``,
  since the rendered CF rows carry no independent beginning/ending cash.

  `equity_statement` and `comprehensive_income` remain unhandled until their
  validators are added.
  """
  if block_type == "income_statement":
    return _validate_income_statement(rows)
  elif block_type == "balance_sheet":
    return _validate_balance_sheet(rows)
  elif block_type == "cash_flow_statement":
    return _validate_cash_flow(rows)
  return ValidationResult(checks=["no_validation_rules"])


# ── Structural checks ─────────────────────────────────────────────────────


def _validate_income_statement(rows: list[FactRow]) -> ValidationResult:
  result = ValidationResult()

  # Check: totals foot (subtotals equal sum of children)
  _check_totals_foot(rows, result)

  # Structural: Net Income must reconcile against EVERY income-statement
  # line by natural balance — credit-nature items (operating AND
  # nonoperating revenue, gains) add; debit-nature items (expenses, losses,
  # interest, tax) subtract. Summing leaves (not subtotals) avoids
  # double-counting, and keying on balance_type rather than classification
  # correctly handles contras (e.g. sales returns) and nonoperating lines.
  #
  # The earlier single-step identity (operating Revenue minus Expenses)
  # spuriously failed on multi-step statements that carry nonoperating
  # income, gains, or losses below operating income.
  result.checks.append("net_income_equation")

  net_income_row = _net_income_row(rows)

  # Reconcile Net Income against the income-statement components by natural
  # balance: credit-nature lines (operating AND nonoperating revenue, gains)
  # add; debit-nature lines (expenses, losses, interest, tax) subtract.
  # Summing the reported LEAVES (atomic facts) — not subtotals — captures
  # nonoperating items a single-step Revenue minus Expenses identity would miss,
  # and keying on balance_type rather than classification makes contras
  # (e.g. sales returns) net correctly.
  implied_ni = 0.0
  credit_leaves = 0
  debit_leaves = 0
  for row in rows:
    # Skip subtotals/abstracts (would double-count) and the reported Net
    # Income row itself (it's the target, not a component) — guarded by
    # identity so it's excluded even if its is_subtotal flag is unset.
    if row.is_subtotal or row.is_abstract or row is net_income_row:
      continue
    value = (row.values[0] or 0.0) if row.values else 0.0
    if row.balance_type == "credit":
      implied_ni += value
      credit_leaves += 1
    elif row.balance_type == "debit":
      implied_ni -= value
      debit_leaves += 1
    # else: per-share / ratio metrics carry no monetary balance — skip.

  inconclusive = False
  if credit_leaves == 0 and debit_leaves == 0:
    # Statement reported only at the subtotal level (no leaf detail). Fall
    # back to the top-most Revenue minus Expenses subtotals — the same approach
    # as the balance sheet, handling FAC's parallel subtotals + qname
    # inference.
    revenue_row = _top_most_subtotal_for_classification(rows, "revenue")
    expense_row = _top_most_subtotal_for_classification(rows, "expense")
    if revenue_row is None or expense_row is None:
      missing: list[str] = []
      if revenue_row is None:
        missing.append("revenue")
      if expense_row is None:
        missing.append("expense")
      result.failures.append(
        "Income statement validation inconclusive: missing classification "
        f"rollups for {missing}. Wire FASB elementsOfFinancialStatements "
        "traits onto the structure's elements, or ensure at least one "
        "subtotal row per classification is present."
      )
      result.passed = False
      inconclusive = True
    else:
      revenue = (revenue_row.values[0] or 0.0) if revenue_row.values else 0.0
      expense = (expense_row.values[0] or 0.0) if expense_row.values else 0.0
      implied_ni = revenue - expense
  elif credit_leaves == 0:
    # Leaf detail present but no revenue/income line — can't reconcile.
    result.failures.append(
      "Income statement validation inconclusive: no revenue/income line "
      "found to reconcile Net Income against."
    )
    result.passed = False
    inconclusive = True

  if not inconclusive:
    if net_income_row is not None:
      reported_ni = (net_income_row.values[0] or 0.0) if net_income_row.values else 0.0
      diff = abs(reported_ni - implied_ni)
      if diff > _TOLERANCE:
        result.failures.append(
          f"Net Income mismatch: reported '{net_income_row.element_name}' "
          f"({reported_ni:.2f}) ≠ Σ(income − expense) ({implied_ni:.2f}), "
          f"difference: {diff:.2f}"
        )
        result.passed = False
    elif implied_ni != 0.0:
      # Informational warning — not a failure. A missing NetIncome row
      # is common in multi-step structures whose final line is
      # "Income from Continuing Operations" or similar.
      result.warnings.append(
        f"No Net Income line found; implied NI = Σ(income − expense) = {implied_ni:.2f}"
      )

  # Semantic: check for zero-balance subtotals
  _check_zero_subtotals(rows, result)

  # Semantic: check for comparative data
  _check_comparative_data(rows, result)

  return result


def _top_most_subtotal_for_classification(
  rows: list[FactRow], classification: str
) -> FactRow | None:
  """Pick the highest-priority row matching ``classification``.

  Priority order:

  1. Smallest depth wins — top of the rollup tree.
  2. Among ties on depth, prefer the subtotal (Revenue rolled up across
     subcategories) over the leaf (a single Revenue line).
  3. Among ties on depth + subtotal-flag, prefer the larger absolute
     value (zero-valued placeholder rows must not beat the real rollup —
     see the FAC ``Temporary Equity`` tie scenario covered in
     ``test_among_ties_at_same_depth_largest_value_wins``).

  Combined L+E rollups are skipped via the qname check (won't classify
  cleanly as either liability or equity).

  Classification falls back to qname-based inference when ``row.classification``
  is empty (FAC, rs-gaap, rs-gaap-type-subtype reference taxonomies).

  Single-step income statements often have ``Revenues`` as a leaf row
  (no children to roll up). The validator must accept the leaf as the
  revenue total in that case — earlier the ``is_subtotal`` requirement
  filtered such rows out and the validator produced an "inconclusive
  failure" against a perfectly-renderable single-step IS.
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
    row_val = abs((row.values[0] or 0.0) if row.values else 0.0)
    best_val = abs((best.values[0] or 0.0) if best.values else 0.0)
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

  Matches by qname token (case-insensitive). When multiple candidates
  exist (e.g. FAC's ``fac:NetIncomeLoss`` plus a ``[Roll Up]`` parent),
  prefers a subtotal at the smallest depth so the canonical "bottom
  line" wins.
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


def _validate_balance_sheet(rows: list[FactRow]) -> ValidationResult:
  result = ValidationResult()

  # Check: totals foot
  _check_totals_foot(rows, result)

  # Structural: Assets = Liabilities + Equity.
  #
  # Resolution: pick the **top-most** subtotal per classification (smallest
  # depth) so we use the rolled-up parent rather than summing every
  # subtotal at every level (which would double-count). Combined "L+E"
  # rollups (qname containing both "liabilit" and "equity") are skipped —
  # they conflate two classifications and would inflate the equity total.
  #
  # Classification falls back to qname-based inference (FAC, rs-gaap,
  # rs-gaap-type-subtype reference taxonomies often lack FASB element_traits).
  result.checks.append("accounting_equation")

  # Reuses the IS validator's resolution chain (smallest depth → subtotal
  # over leaf → larger value) so a single-step BS variant where, for
  # example, ``Equity`` is a leaf rather than a roll-up still resolves
  # cleanly. See :func:`_top_most_subtotal_for_classification`.
  candidates: dict[str, FactRow] = {}
  for cls in ("asset", "liability", "equity"):
    pick = _top_most_subtotal_for_classification(rows, cls)
    if pick is not None:
      candidates[cls] = pick

  required = {"asset", "liability", "equity"}
  missing = required - candidates.keys()
  if missing:
    # No silent pass: if we couldn't identify totals for all three
    # classifications the validator can't make any claim about the
    # equation. Report explicitly so callers don't read a phantom green
    # check as proof of correctness.
    result.failures.append(
      "Balance sheet validation inconclusive: missing classification "
      f"rollups for {sorted(missing)}. Wire FASB elementsOfFinancialStatements "
      "traits onto the structure's elements, or ensure at least one "
      "subtotal row per classification is present."
    )
    result.passed = False
  else:
    total_assets = (
      (candidates["asset"].values[0] or 0.0) if candidates["asset"].values else 0.0
    )
    total_liabilities = (
      (candidates["liability"].values[0] or 0.0)
      if candidates["liability"].values
      else 0.0
    )
    total_equity = (
      (candidates["equity"].values[0] or 0.0) if candidates["equity"].values else 0.0
    )
    diff = abs(total_assets - (total_liabilities + total_equity))
    if diff > _TOLERANCE:
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


def _validate_cash_flow(rows: list[FactRow]) -> ValidationResult:
  """Cash flow validation — structural footing of the rendered CF.

  `_check_totals_foot` verifies the net-change line foots to Op + Inv + Fin
  and each section foots to its leaves (so the investing/financing flow facts
  emitted by ``fact_grid._emit_flow_facts`` actually roll up). The ΔCash
  reconciliation against the balance sheet is a fact-bundle check
  (``fact_grid._check_cash_flow_tie_out``), not a per-statement one.
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
