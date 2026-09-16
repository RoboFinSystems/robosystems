"""Tier-0 classification — Mercury's own signals as account suggestions.

No model, no credits: three lookups in order of trust, each landing on the
shared hint vocabulary (``adapters/bank_feed/hints.py``).

1. ``glAllocations`` — the GL code the customer (or Mercury's auto-
   categorization) assigned in Mercury's Accounting tab, present when Mercury
   is linked to QuickBooks / Xero / NetSuite.
2. the custom category (``categoryData``) — Mercury's per-transaction
   category; the org's own or one Mercury seeds.
3. ``mercuryCategory`` — the merchant bucket Mercury stamps on card spend.

The first lookup that hits wins and lands on the event as
``metadata.suggested_account_name`` + ``metadata.classification_source``.
A hint names an account; the loader resolves it against whatever chart the
graph has (by name, then by code) and never creates one. An unresolved hint
stays a name-only hint. Nothing here posts anything.
"""

from __future__ import annotations

from robosystems.adapters.bank_feed.hints import (
  HINTS,
  AccountHint,
  infer_trait,
  parse_gl_code_name,
  slug,
)

# Mercury's default custom categories. ``None`` means "a movement, not
# income or expense" — no account is suggested.
CUSTOM_CATEGORIES: dict[str, str | None] = {
  "Revenue": "Revenue",
  "Interest Earned": "InterestIncome",
  "COGS": "CostOfRevenue",
  "Payroll": "Payroll",
  "Employee Benefits": "EmployeeBenefits",
  "Software & Subscriptions": "SoftwareSubscriptions",
  "Marketing & Advertising": "MarketingAdvertising",
  "Legal & Professional Services": "ProfessionalFees",
  "Insurance": "Insurance",
  "Bank Fees": "BankFees",
  "Payment Processing Fees": "PaymentProcessingFees",
  "Travel & Transportation": "TravelTransportation",
  "Business Meals": "BusinessMeals",
  "Entertainment": "Entertainment",
  "Office Supplies & Equipment": "OfficeSuppliesEquipment",
  "Shipping & Postage": "ShippingPostage",
  "Inventory & Materials": "InventoryMaterials",
  "Transfer": None,
  "Credit & Loan Payments": None,
  "Financing Proceeds": None,
}

# The merchant bucket Mercury assigns to card spend (the ``mercuryCategory``
# enum). Buckets not listed suggest nothing.
MERCURY_CATEGORIES: dict[str, str] = {
  "Software": "SoftwareSubscriptions",
  "Advertising": "MarketingAdvertising",
  "ProfessionalServices": "ProfessionalFees",
  "Insurance": "Insurance",
  "Fees": "BankFees",
  "Taxes": "TaxesLicenses",
  "GovernmentServices": "TaxesLicenses",
  "Utilities": "Utilities",
  "Airlines": "TravelTransportation",
  "Travel": "TravelTransportation",
  "Lodging": "TravelTransportation",
  "Transportation": "TravelTransportation",
  "Restaurants": "BusinessMeals",
  "Entertainment": "Entertainment",
  "OfficeSupplies": "OfficeSuppliesEquipment",
  "Shipping": "ShippingPostage",
}

# GL codes from the customer's accounting link, by Mercury ``glCodeName``.
# Names not listed become a hint of their own, trait inferred from the name.
GL_CODES: dict[str, str] = {
  "Credit card rewards": "CreditCardRewards",
}


def hint_for_gl_code(gl_code_name: str) -> AccountHint:
  """The hint a Mercury GL code maps to — listed, or derived from its name."""
  key = GL_CODES.get(gl_code_name)
  if key and key in HINTS:
    return HINTS[key]
  _code, name = parse_gl_code_name(gl_code_name)
  trait, balance = infer_trait(name)
  return AccountHint(
    key=f"gl_{slug(name)}", name=name, trait=trait, balance_type=balance
  )


def hint_for_custom_category(name: str | None) -> tuple[AccountHint | None, bool]:
  """(hint, known) for a custom category; ``known`` is True for a listed
  category even when it maps to no account (a movement)."""
  if name is None or name not in CUSTOM_CATEGORIES:
    return None, False
  key = CUSTOM_CATEGORIES[name]
  return (HINTS.get(key) if key else None), True


def hint_for_mercury_category(bucket: str | None) -> AccountHint | None:
  if bucket is None:
    return None
  key = MERCURY_CATEGORIES.get(bucket)
  return HINTS.get(key) if key else None
