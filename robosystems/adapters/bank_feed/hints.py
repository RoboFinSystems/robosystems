"""The hint vocabulary — the account a bank line most likely belongs to.

No model, no credits. A source's own signal (Mercury's GL coding or
category, Plaid's personal-finance category) maps to one of these hints; the
hint names an account and the loader resolves it against whatever chart the
graph has (``chart.ChartIndex``) and never creates one. An unresolved hint
stays a name-only hint. Nothing here posts anything.

Data rather than a config file: the package ships ``*.py`` only, and the
hints change with the sources' category enums, not per tenant.
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
    _hint("DividendIncome", "Dividend income", "revenue", "credit"),
    _hint("RentalIncome", "Rental income", "revenue", "credit"),
    _hint("OtherIncome", "Other income", "revenue", "credit"),
    _hint("CreditCardRewards", "Credit card rewards", "revenue", "credit"),
    _hint("CostOfRevenue", "Cost of revenue", "expense", "debit"),
    _hint("Payroll", "Payroll", "expense", "debit"),
    _hint("EmployeeBenefits", "Employee benefits", "expense", "debit"),
    _hint("SoftwareSubscriptions", "Software & subscriptions", "expense", "debit"),
    _hint("MarketingAdvertising", "Marketing & advertising", "expense", "debit"),
    _hint("ProfessionalFees", "Legal & professional fees", "expense", "debit"),
    _hint("Insurance", "Insurance", "expense", "debit"),
    _hint("BankFees", "Bank fees", "expense", "debit"),
    _hint("InterestExpense", "Interest expense", "expense", "debit"),
    _hint("PaymentProcessingFees", "Payment processing fees", "expense", "debit"),
    _hint("TravelTransportation", "Travel & transportation", "expense", "debit"),
    _hint("VehicleExpenses", "Vehicle expenses", "expense", "debit"),
    _hint("BusinessMeals", "Business meals", "expense", "debit"),
    _hint("Entertainment", "Entertainment", "expense", "debit"),
    _hint("OfficeSuppliesEquipment", "Office supplies & equipment", "expense", "debit"),
    _hint("RepairsMaintenance", "Repairs & maintenance", "expense", "debit"),
    _hint("ShippingPostage", "Shipping & postage", "expense", "debit"),
    _hint("InventoryMaterials", "Inventory & materials", "expense", "debit"),
    _hint("TrainingEducation", "Training & education", "expense", "debit"),
    _hint("CharitableContributions", "Charitable contributions", "expense", "debit"),
    _hint("TaxesLicenses", "Taxes & licenses", "expense", "debit"),
    _hint("Utilities", "Utilities", "expense", "debit"),
    _hint("Rent", "Rent", "expense", "debit"),
  )
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

  A linked ledger's display label: QuickBooks and Xero both render an
  account with a code as ``<code> - <name>``. A label with no code prefix
  comes back as (``None``, label).
  """
  match = _GL_CODE_PREFIX.match(gl_code_name or "")
  if match:
    return match.group("code"), match.group("name")
  return None, (gl_code_name or "").strip()
