"""Guard rails — structural and semantic validation for generated reports.

Structural checks are deterministic arithmetic (hard failures).
Semantic checks are pattern matching against known report structures (warnings).

Every check runs once per rendered period column. A statement with a
comparative column is two statements sharing one row layout, and a green
result that inspected only the first column says nothing about the other —
so on a multi-column statement each failure and warning names the column it
was found in (``[Prior] Balance sheet does not balance …``).
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

  ``status`` is the load-bearing field: ``passed`` (every rule ran on every
  rendered column and produced zero failures), ``failed`` (at least one rule
  failed), or ``inconclusive`` (no rule exists for the block type — nothing
  was checked). ``passed`` is ``True`` only for ``status == "passed"``: a
  statement nobody checked is not a statement that passed.
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

  ``period_labels`` names the columns of ``rows[*].values`` (``Current`` /
  ``Prior``, a period end date, …) so a finding on a comparative statement
  says which column it belongs to. Missing labels fall back to ``column N``.

  `_check_totals_foot` (shared) is the load-bearing CF check: it verifies the
  net-change-in-cash line foots to Op + Inv + Fin and that each section foots
  to its leaves — i.e. the investing/financing flows actually roll up. The
  cross-statement ΔCash reconciliation (CF net-change == BS cash movement)
  lives at the fact-bundle level in ``fact_grid._check_cash_flow_tie_out``,
  since the rendered CF rows carry no independent beginning/ending cash.

  `equity_statement` and `comprehensive_income` have no validators yet, so
  they come back ``inconclusive`` rather than vacuously passed.
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
  """Record a failure — deduplicated, so a column-independent finding (a
  missing classification rollup) is stated once, not once per column."""
  if message not in result.failures:
    result.failures.append(message)
  result.passed = False


# ── Structural checks ─────────────────────────────────────────────────────


def _validate_income_statement(
  rows: list[FactRow], result: ValidationResult, column: _Column
) -> None:
  _check_totals_foot(rows, result, column)

  # Structural: Net Income must reconcile against EVERY income-statement
  # line by natural balance — credit-nature lines (operating AND
  # nonoperating revenue, gains) add; debit-nature lines (expenses, losses,
  # interest, tax) subtract.
  #
  # Summing the reported LEAVES (atomic facts) rather than subtotals avoids
  # double-counting and captures nonoperating items that a single-step
  # "Revenue minus Expenses" identity misses on a multi-step statement.
  # Keying on balance_type rather than classification makes contras
  # (e.g. sales returns) net correctly.
  _note_check(result, "net_income_equation")

  net_income_row = _net_income_row(rows)

  implied_ni = 0.0
  credit_leaves = 0
  debit_leaves = 0
  for row in rows:
    # Skip subtotals/abstracts (would double-count) and the reported Net
    # Income row itself (it's the target, not a component) — guarded by
    # identity so it's excluded even if its is_subtotal flag is unset.
    if row.is_subtotal or row.is_abstract or row is net_income_row:
      continue
    value = _value(row, column.index)
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
    # Leaf detail present but no revenue/income line — can't reconcile.
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
    # Informational warning — not a failure. A missing NetIncome row
    # is common in multi-step structures whose final line is
    # "Income from Continuing Operations" or similar.
    result.warnings.append(
      f"{column.prefix}No Net Income line found; implied NI = "
      f"Σ(income − expense) = {implied_ni:.2f}"
    )


def _top_most_subtotal_for_classification(
  rows: list[FactRow], classification: str, col: int = 0
) -> FactRow | None:
  """Pick the highest-priority row matching ``classification``.

  Priority order:

  1. Smallest depth wins — top of the rollup tree.
  2. Among ties on depth, prefer the subtotal (Revenue rolled up across
     subcategories) over the leaf (a single Revenue line).
  3. Among ties on depth + subtotal-flag, prefer the larger absolute
     value in column ``col`` (zero-valued placeholder rows must not beat
     the real rollup — see the FAC ``Temporary Equity`` tie scenario covered
     in ``test_among_ties_at_same_depth_largest_value_wins``).

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


def _validate_balance_sheet(
  rows: list[FactRow], result: ValidationResult, column: _Column
) -> None:
  _check_totals_foot(rows, result, column)

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
  _note_check(result, "accounting_equation")

  # Reuses the IS validator's resolution chain (smallest depth → subtotal
  # over leaf → larger value) so a single-step BS variant where, for
  # example, ``Equity`` is a leaf rather than a roll-up still resolves
  # cleanly. See :func:`_top_most_subtotal_for_classification`.
  candidates: dict[str, FactRow] = {}
  for cls in ("asset", "liability", "equity"):
    pick = _top_most_subtotal_for_classification(rows, cls, column.index)
    if pick is not None:
      candidates[cls] = pick

  required = {"asset", "liability", "equity"}
  missing = required - candidates.keys()
  if missing:
    # No silent pass: if we couldn't identify totals for all three
    # classifications the validator can't make any claim about the
    # equation. Report explicitly so callers don't read a phantom green
    # check as proof of correctness.
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
  """Cash flow validation — structural footing of the rendered CF.

  `_check_totals_foot` verifies the net-change line foots to Op + Inv + Fin
  and each section foots to its leaves (so the investing/financing flow facts
  emitted by ``fact_grid._emit_flow_facts`` actually roll up). The ΔCash
  reconciliation against the balance sheet is a fact-bundle check
  (``fact_grid._check_cash_flow_tie_out``), not a per-statement one.
  """
  _check_totals_foot(rows, result, column)
  _check_operating_plug(rows, result, column)


_VALIDATORS: dict[str, _Validator] = {
  "income_statement": _validate_income_statement,
  "balance_sheet": _validate_balance_sheet,
  "cash_flow_statement": _validate_cash_flow,
}


# ── Shared check helpers ──────────────────────────────────────────────────


def _check_totals_foot(
  rows: list[FactRow], result: ValidationResult, column: _Column
) -> None:
  """Verify that subtotal rows equal the sum of their children.

  ``_build_rows`` emits post-order — children first, then their parent
  subtotal — so a subtotal's children are the rows immediately BEFORE it
  with depth = subtotal.depth + 1, scanning back until a row at the same
  or lower depth. Scanning FORWARD instead would foot each subtotal against
  the next section's children (e.g. 'Revenues' against Cost of Revenue's
  leaves), producing spurious warnings while skipping the real check.

  Calc-target subtotals with no presentation children (Gross Profit,
  Operating Income) have no preceding deeper rows and fall out via the
  zero child_sum guard; their arithmetic is covered by the calc DAG and
  the net-income equation. Value-less structural headers (abstract rows
  rendered with all-None values) are skipped outright.
  """
  _note_check(result, "totals_foot")

  for idx, subtotal in enumerate(rows):
    if not subtotal.is_subtotal:
      continue
    if not any(v is not None for v in subtotal.values):
      continue
    child_sum = 0.0

    # Children precede the subtotal (post-order) at depth = subtotal.depth + 1.
    # Only sum direct children, not grandchildren — grandchildren are
    # already rolled into their parent subtotals.
    for j in range(idx - 1, -1, -1):
      child = rows[j]
      if child.depth <= subtotal.depth:
        break
      if child.depth == subtotal.depth + 1:
        child_sum += _value(child, column.index)

    subtotal_val = _value(subtotal, column.index)
    diff = abs(subtotal_val - child_sum)
    if diff > _TOLERANCE and child_sum != 0.0:
      result.warnings.append(
        f"{column.prefix}Subtotal '{subtotal.element_name}' ({subtotal_val:.2f}) "
        f"does not match sum of children ({child_sum:.2f}), "
        f"difference: {diff:.2f}"
      )


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

  ``fact_grid._reconcile_operating_to_cash`` foots the indirect CF to actual
  cash by booking the aggregate non-cash operating adjustment (gain/loss on
  disposal, unrealized MTM, write-offs, …) onto
  ``IncreaseDecreaseInOtherOperatingCapitalNet``. That makes the statement
  articulate *by construction* — and so silently absorbs any investing/financing
  misclassification too. A plug that dwarfs operating cash is the signal that a
  material item is un-itemized or mis-tagged; surface it so it isn't invisible.

  Row-level approximation: that line also carries any tenant-mapped "other
  operating capital" content, but it's a system catch-all rarely mapped
  directly, so the row value ≈ the plug in practice. Warning-only — the CF still
  foots and renders.
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
