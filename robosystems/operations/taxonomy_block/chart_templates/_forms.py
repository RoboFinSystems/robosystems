"""Equity rows by entity legal form — shared by every template.

The two equity accounts (3000 / 3100) map to the rs-gaap capital concept
that matches the entity's legal form, so the equity-form Reporting Style
renders its native capital line. Non-equity mappings are identical across
forms.
"""

from __future__ import annotations

EQUITY_CODES: tuple[str, str] = ("3000", "3100")

EQUITY_BY_FORM: dict[str, list[tuple[str, str]]] = {
  "corporation": [
    ("3000", "rs-gaap:AdditionalPaidInCapital"),
    ("3100", "rs-gaap:RetainedEarningsAccumulatedDeficit"),
  ],
  "partnership": [
    ("3000", "rs-gaap:PartnersCapital"),
    ("3100", "rs-gaap:PartnersCapital"),
  ],
  "llc": [
    ("3000", "rs-gaap:MembersEquity"),
    ("3100", "rs-gaap:MembersEquity"),
  ],
}


DEFAULT_FORM = "corporation"


def resolve_form(entity_type: str | None) -> str:
  """The legal form the equity rows are mapped for.

  Unknown or empty forms resolve to ``corporation`` — this is the value a
  caller should record and report, not the raw request string.
  """
  form = (entity_type or "").strip().lower()
  return form if form in EQUITY_BY_FORM else DEFAULT_FORM


def form_aware(
  mappings: list[tuple[str, str]], entity_type: str | None
) -> list[tuple[str, str]]:
  """``mappings`` with the equity rows swapped for the entity's legal form."""
  equity = EQUITY_BY_FORM[resolve_form(entity_type)]
  base = [m for m in mappings if m[0] not in EQUITY_CODES]
  return base + equity
