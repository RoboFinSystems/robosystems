"""Tier-0 classification — Mercury's own signals as account suggestions.

No model, no credits: three lookups in order of trust.

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

Data rather than a config file: the package ships ``*.py`` only, and the
hints change with Mercury's category enum, not per tenant.
"""

from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class AccountHint:
  key: str
  name: str
  trait: str
  balance_type: str


def _hint(key: str, name: str, trait: str, balance_type: str) -> AccountHint:
  return AccountHint(key=key, name=name, trait=trait, balance_type=balance_type)


HINTS: dict[str, AccountHint] = {
  hint.key: hint
  for hint in (
    _hint("Revenue", "Revenue", "revenue", "credit"),
    _hint("InterestIncome", "Interest income", "revenue", "credit"),
    _hint("CreditCardRewards", "Credit card rewards", "revenue", "credit"),
    _hint("CostOfRevenue", "Cost of revenue", "expense", "debit"),
    _hint("Payroll", "Payroll", "expense", "debit"),
    _hint("EmployeeBenefits", "Employee benefits", "expense", "debit"),
    _hint("SoftwareSubscriptions", "Software & subscriptions", "expense", "debit"),
    _hint("MarketingAdvertising", "Marketing & advertising", "expense", "debit"),
    _hint("ProfessionalFees", "Legal & professional fees", "expense", "debit"),
    _hint("Insurance", "Insurance", "expense", "debit"),
    _hint("BankFees", "Bank fees", "expense", "debit"),
    _hint("PaymentProcessingFees", "Payment processing fees", "expense", "debit"),
    _hint("TravelTransportation", "Travel & transportation", "expense", "debit"),
    _hint("BusinessMeals", "Business meals", "expense", "debit"),
    _hint("Entertainment", "Entertainment", "expense", "debit"),
    _hint("OfficeSuppliesEquipment", "Office supplies & equipment", "expense", "debit"),
    _hint("ShippingPostage", "Shipping & postage", "expense", "debit"),
    _hint("InventoryMaterials", "Inventory & materials", "expense", "debit"),
    _hint("TaxesLicenses", "Taxes & licenses", "expense", "debit"),
    _hint("Utilities", "Utilities", "expense", "debit"),
    _hint("Rent", "Rent", "expense", "debit"),
  )
}

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

_REVENUE = re.compile(r"revenue|income|sales|rewards|cashback|interest earned", re.I)
_LIABILITY = re.compile(r"payable|liabilit|loan|note|credit card|deferred", re.I)
_ASSET = re.compile(r"prepaid|receivable|asset|equipment|inventory|deposit", re.I)
_EQUITY = re.compile(r"equity|capital|contribution|distribution|draw", re.I)
_GL_CODE_PREFIX = re.compile(
  r"^\s*(?P<code>[0-9][0-9.\-]*)\s*[-–:]\s*(?P<name>.+?)\s*$"
)


def infer_trait(name: str) -> tuple[str, str]:
  """Best-effort (trait, balance_type) for an account known only by name."""
  if _REVENUE.search(name):
    return "revenue", "credit"
  if _LIABILITY.search(name):
    return "liability", "credit"
  if _EQUITY.search(name):
    return "equity", "credit"
  if _ASSET.search(name):
    return "asset", "debit"
  return "expense", "debit"


def slug(value: str) -> str:
  """``General & Administrative:Technology`` → ``GeneralAdministrativeTechnology``."""
  parts = re.split(r"[^A-Za-z0-9]+", value)
  return "".join(part[:1].upper() + part[1:] for part in parts if part)


def parse_gl_code_name(gl_code_name: str) -> tuple[str | None, str]:
  """Split ``500 - Office Supplies`` into (``500``, ``Office Supplies``).

  Mercury's ``glCodeName`` is the linked ledger's display label; QuickBooks
  and Xero both render it as ``<code> - <name>`` when the account has a
  code. A label with no code prefix comes back as (``None``, label).
  """
  match = _GL_CODE_PREFIX.match(gl_code_name or "")
  if match:
    return match.group("code"), match.group("name")
  return None, (gl_code_name or "").strip()


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
