"""The effective-interest curve: a loan's amortization table, one row per
monthly period, in cents. Pure arithmetic; the schedule service stores the
table on the Structure and drafts the legs from it."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from decimal import ROUND_HALF_UP, Decimal

# The table's columns, as stored under ``schedule_metadata.periodic_columns``.
LOAN_COLUMNS = (
  "opening",
  "interest",
  "interest_paid",
  "principal",
  "payment",
  "balance",
  "current_portion",
)
# The columns a leg may post.
LEG_COLUMNS = frozenset({"interest", "interest_paid", "principal", "payment"})

_FREQUENCY_MONTHS = {"monthly": 1, "quarterly": 3, "annual": 12}


@dataclass(frozen=True)
class LoanTerms:
  principal: int  # cents
  annual_rate: Decimal
  start_date: date
  day_count: str = "30/360"
  payment_frequency: str = "monthly"
  first_payment_date: date | None = None
  payment_amount: int | None = None  # cents
  interest_only: bool = False
  maturity_date: date | None = None
  capitalize_interest: bool = False

  @classmethod
  def from_dict(cls, raw: dict) -> LoanTerms:
    def _date(key: str) -> date | None:
      value = raw.get(key)
      if value is None or isinstance(value, date):
        return value
      return date.fromisoformat(value)

    start = _date("start_date")
    if start is None:
      raise ValueError("loan_terms.start_date is required.")
    return cls(
      principal=int(raw["principal"]),
      # Through str, so 0.099 is 0.099 and not its binary neighbour.
      annual_rate=Decimal(str(raw["annual_rate"])),
      start_date=start,
      day_count=raw.get("day_count") or "30/360",
      payment_frequency=raw.get("payment_frequency") or "monthly",
      first_payment_date=_date("first_payment_date"),
      payment_amount=raw.get("payment_amount"),
      interest_only=bool(raw.get("interest_only")),
      maturity_date=_date("maturity_date"),
      capitalize_interest=bool(raw.get("capitalize_interest")),
    )


def accrual_days(start: date, end_exclusive: date, convention: str) -> tuple[int, int]:
  """Days from ``start`` up to ``end_exclusive`` and the year's basis."""
  if convention == "30/360":
    # US 30/360: a 31st counts as the 30th, so every whole month is 30 days.
    d1 = min(start.day, 30)
    d2 = end_exclusive.day
    if d2 == 31 and d1 >= 30:
      d2 = 30
    days = (
      360 * (end_exclusive.year - start.year)
      + 30 * (end_exclusive.month - start.month)
      + (d2 - d1)
    )
    return days, 360
  if convention == "actual/365":
    return (end_exclusive - start).days, 365
  if convention == "actual/360":
    return (end_exclusive - start).days, 360
  raise ValueError(f"Unknown day_count {convention!r}.")


def _cents(value: Decimal) -> int:
  return int(value.quantize(Decimal(1), rounding=ROUND_HALF_UP))


def _month_index(day: date) -> int:
  return day.year * 12 + day.month


def _level_payment(principal: int, periodic_rate: Decimal, count: int) -> int:
  if count <= 0:
    raise ValueError("A level payment needs at least one payment before maturity.")
  if periodic_rate == 0:
    return _cents(Decimal(principal) / count)
  factor = (1 + periodic_rate) ** count
  return _cents(Decimal(principal) * periodic_rate * factor / (factor - 1))


def amortization_table(
  terms: LoanTerms, periods: list[tuple[date, date]]
) -> dict[str, list[int]]:
  """Each column of the table, one cents value per period.

  Interest accrues on the period's opening balance from ``start_date`` (or
  the period's first day) to the period's last day inclusive; a payment
  falls at the end of its month. ``current_portion`` is the principal due in
  the twelve months after each period, read from the table run to maturity
  when the schedule ends before it. Raises ``ValueError`` naming the period
  when the terms cannot be honoured: a payment short of the interest it
  settles (unless interest capitalizes), or a balance driven below zero.
  """
  if not periods:
    raise ValueError("A loan schedule needs at least one period.")
  first_start, first_end = periods[0]
  if not (first_start <= terms.start_date <= first_end):
    raise ValueError(
      f"loan_terms.start_date ({terms.start_date}) must fall in the "
      f"schedule's first month ({first_start:%Y-%m}): the schedule starts "
      "when the principal is drawn."
    )
  maturity_index = (
    _month_index(terms.maturity_date) if terms.maturity_date is not None else None
  )
  last_index = _month_index(periods[-1][1])
  if maturity_index is not None and maturity_index < last_index:
    raise ValueError(
      f"The schedule runs past the loan's maturity ({terms.maturity_date}); "
      "end it in the maturity month."
    )

  if terms.payment_frequency == "at_maturity":
    if maturity_index is None:
      raise ValueError("payment_frequency 'at_maturity' needs maturity_date.")
    payment_months = {maturity_index}
    step = None
  else:
    step = _FREQUENCY_MONTHS[terms.payment_frequency]
    first_payment_index = (
      _month_index(terms.first_payment_date)
      if terms.first_payment_date is not None
      else _month_index(first_end) + step
    )
    horizon = maturity_index if maturity_index is not None else last_index
    payment_months = set(range(first_payment_index, horizon + 1, step))
    if maturity_index is not None:
      payment_months.add(maturity_index)

  amortizing = not terms.interest_only and terms.payment_frequency != "at_maturity"
  payment_amount = terms.payment_amount
  if amortizing and payment_amount is None:
    if maturity_index is None:
      raise ValueError(
        "An amortizing loan needs maturity_date or payment_amount; a line "
        "of credit that only pays interest is interest_only."
      )
    count = len([m for m in payment_months if m <= maturity_index])
    periodic_rate = terms.annual_rate * (step or 12) / 12
    payment_amount = _level_payment(terms.principal, periodic_rate, count)

  # Past the window to maturity, so the last periods' current portion counts
  # principal due after the schedule ends.
  run = list(periods)
  while maturity_index is not None and _month_index(run[-1][1]) < maturity_index:
    following = run[-1][1] + timedelta(days=1)
    run.append((following, _month_end(following)))

  columns: dict[str, list[int]] = {name: [] for name in LOAN_COLUMNS}
  balance = terms.principal
  unpaid_interest = 0
  for p_start, p_end in run:
    accrual_start = max(p_start, terms.start_date)
    days, basis = accrual_days(
      accrual_start, p_end + timedelta(days=1), terms.day_count
    )
    interest = _cents(Decimal(balance) * terms.annual_rate * days / basis)
    opening = balance
    if terms.capitalize_interest:
      balance += interest
    else:
      unpaid_interest += interest

    interest_paid = principal = 0
    index = _month_index(p_end)
    if index in payment_months:
      interest_paid = unpaid_interest
      if index == maturity_index:
        principal = balance
      elif amortizing and payment_amount is not None:
        principal = payment_amount - interest_paid
        if principal < 0:
          raise ValueError(
            f"{p_end:%Y-%m}: the payment ({payment_amount / 100:.2f}) is less "
            f"than the interest it settles ({interest_paid / 100:.2f}), so the "
            "balance would grow; set capitalize_interest for a note that "
            "adds interest to principal."
          )
        principal = min(principal, balance)
      unpaid_interest -= interest_paid
      balance -= principal
    if balance < 0:
      raise ValueError(f"{p_end:%Y-%m}: the loan balance would fall below zero.")

    columns["opening"].append(opening)
    columns["interest"].append(interest)
    columns["interest_paid"].append(interest_paid)
    columns["principal"].append(principal)
    columns["payment"].append(interest_paid + principal)
    columns["balance"].append(balance)

  principal_paid = columns["principal"]
  columns["current_portion"] = [
    sum(principal_paid[i + 1 : i + 13]) for i in range(len(periods))
  ]
  return {name: values[: len(periods)] for name, values in columns.items()}


def _month_end(day: date) -> date:
  following = date(day.year + day.month // 12, day.month % 12 + 1, 1)
  return following - timedelta(days=1)


def reclass_cents(columns: dict[str, list[int]], index: int) -> int:
  """The period's move into the current-portion account: its target less
  the previous period's (none before the first)."""
  current = columns.get("current_portion") or []
  if index >= len(current):
    return 0
  return current[index] - (current[index - 1] if index > 0 else 0)
