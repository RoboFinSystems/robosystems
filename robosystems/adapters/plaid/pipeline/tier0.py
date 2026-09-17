"""Tier-0 classification — Plaid's personal-finance category as a suggestion.

Every Plaid transaction carries ``personal_finance_category`` (v2: a
``primary`` and a ``detailed`` code, with a confidence level). It is a
*personal* finance taxonomy, so the map to a business chart is a hint and
nothing more: the detailed code wins when it is listed, the primary code
otherwise. ``None`` means the category is a movement — a transfer, a loan
payment or disbursement, income a business does not earn — and suggests no
account. A category not listed at all suggests nothing either.

The hint lands on the event as ``metadata.suggested_account_name`` +
``classification_source='plaid_category'`` and resolves against whatever
chart the graph has; nothing here posts anything.

Taxonomy: https://plaid.com/documents/pfc-taxonomy-all.csv (v2, a superset
of v1 — v1 detailed codes map the same way).
"""

from __future__ import annotations

from typing import Any

from robosystems.adapters.bank_feed.hints import HINTS, AccountHint

# Primary categories, the fallback when the detailed code is not listed.
PRIMARY: dict[str, str | None] = {
  "INCOME": None,
  "LOAN_DISBURSEMENTS": None,
  "LOAN_PAYMENTS": None,
  "TRANSFER_IN": None,
  "TRANSFER_OUT": None,
  "BANK_FEES": "BankFees",
  "ENTERTAINMENT": "Entertainment",
  "FOOD_AND_DRINK": "BusinessMeals",
  "GENERAL_MERCHANDISE": "OfficeSuppliesEquipment",
  "HOME_IMPROVEMENT": "RepairsMaintenance",
  "MEDICAL": None,
  "PERSONAL_CARE": None,
  "GENERAL_SERVICES": None,
  "GOVERNMENT_AND_NON_PROFIT": "TaxesLicenses",
  "TRANSPORTATION": "TravelTransportation",
  "TRAVEL": "TravelTransportation",
  "RENT_AND_UTILITIES": "Utilities",
  "OTHER": None,
}

# Detailed categories that differ from their primary's default.
DETAILED: dict[str, str | None] = {
  "INCOME_CONTRACTOR": "Revenue",
  "INCOME_GIG_ECONOMY": "Revenue",
  "INCOME_DIVIDENDS": "DividendIncome",
  "INCOME_INTEREST_EARNED": "InterestIncome",
  "INCOME_RENTAL": "RentalIncome",
  "INCOME_OTHER": "OtherIncome",
  "BANK_FEES_INTEREST_CHARGE": "InterestExpense",
  "GENERAL_MERCHANDISE_OFFICE_SUPPLIES": "OfficeSuppliesEquipment",
  "HOME_IMPROVEMENT_FURNITURE": "OfficeSuppliesEquipment",
  "GENERAL_SERVICES_ACCOUNTING_AND_FINANCIAL_PLANNING": "ProfessionalFees",
  "GENERAL_SERVICES_CONSULTING_AND_LEGAL": "ProfessionalFees",
  "GENERAL_SERVICES_AUTOMOTIVE": "VehicleExpenses",
  "GENERAL_SERVICES_EDUCATION": "TrainingEducation",
  "GENERAL_SERVICES_INSURANCE": "Insurance",
  "GENERAL_SERVICES_POSTAGE_AND_SHIPPING": "ShippingPostage",
  "GENERAL_SERVICES_STORAGE": "Rent",
  "GOVERNMENT_AND_NON_PROFIT_DONATIONS": "CharitableContributions",
  "TRANSPORTATION_GAS": "VehicleExpenses",
  "RENT_AND_UTILITIES_RENT": "Rent",
}

GOVERNMENT_PRIMARY = "GOVERNMENT_AND_NON_PROFIT"
GOVERNMENT_DONATIONS = "GOVERNMENT_AND_NON_PROFIT_DONATIONS"


def category(txn: dict[str, Any]) -> tuple[str | None, str | None, str | None]:
  """(primary, detailed, confidence_level) — ``None`` where Plaid sent none."""
  pfc = txn.get("personal_finance_category") or {}
  return pfc.get("primary"), pfc.get("detailed"), pfc.get("confidence_level")


def hint_for_category(
  primary: str | None, detailed: str | None
) -> tuple[AccountHint | None, bool]:
  """(hint, known) — ``known`` is True for a listed category even when it is
  a movement that suggests no account."""
  if detailed and detailed in DETAILED:
    key = DETAILED[detailed]
    return (HINTS[key] if key else None), True
  if primary and primary in PRIMARY:
    key = PRIMARY[primary]
    return (HINTS[key] if key else None), True
  return None, False
