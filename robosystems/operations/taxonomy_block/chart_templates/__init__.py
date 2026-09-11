"""Shipped chart-of-accounts templates — read-only starting points.

A template is a chart (``ACCOUNTS`` rows) plus its CoA → rs-gaap mapping
(``mappings_for(entity_type)``), promoted out of ``examples/`` so a fresh
company can initialize its books from one operation
(``initialize-chart-of-accounts``) instead of importing them. Templates
are starting points, never a lock-in: customization is
``update-taxonomy-block`` afterwards.

Doctrine (``specs/ledger/native-accounting-cutover.md`` §4, from the
Mercury adapter spec §8.1): no chart is ever created by default, native
and synced ledgers never mix, and initializing a chart is an explicit
action.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from . import product, saas, services

# One chart row: (code, name, trait, sub_classification, balance_type,
# description). ``trait`` is the FASB metamodel trait (asset / liability /
# equity / revenue / expense); ``balance_type`` is "debit" | "credit".
Account = tuple[str, str, str, str, str, str | None]


@dataclass(frozen=True)
class ChartTemplate:
  key: str
  display_name: str
  description: str
  accounts: list[Account]
  mappings_for: Callable[[str], list[tuple[str, str]]]

  @property
  def account_count(self) -> int:
    return len(self.accounts)


CHART_TEMPLATES: dict[str, ChartTemplate] = {
  "saas": ChartTemplate(
    key="saas",
    display_name="SaaS / subscription software",
    description=(
      "Recurring revenue with annual prepayment (deferred revenue), cost of "
      "revenue, and operating expenses by function — R&D, sales & marketing, "
      "G&A. Cash, AR, prepaids, equipment."
    ),
    accounts=saas.ACCOUNTS,
    mappings_for=saas.mappings_for,
  ),
  "services": ChartTemplate(
    key="services",
    display_name="Professional services",
    description=(
      "Consulting, advisory and implementation revenue; payroll-led operating "
      "expenses; no inventory and no cost of goods sold. Cash, AR, prepaids, "
      "equipment, payroll liabilities."
    ),
    accounts=services.ACCOUNTS,
    mappings_for=services.mappings_for,
  ),
  "product": ChartTemplate(
    key="product",
    display_name="Product business (inventory and COGS)",
    description=(
      "Goods sold direct and wholesale, subscription revenue with deferred "
      "revenue, three-stage inventory (raw materials, work in process, "
      "finished goods), cost of goods sold, production equipment, "
      "fulfillment and marketing expenses."
    ),
    accounts=product.ACCOUNTS,
    mappings_for=product.mappings_for,
  ),
}

TEMPLATE_KEYS: tuple[str, ...] = tuple(CHART_TEMPLATES)


def get_template(key: str) -> ChartTemplate | None:
  return CHART_TEMPLATES.get((key or "").strip().lower())


def list_templates() -> list[ChartTemplate]:
  return list(CHART_TEMPLATES.values())


__all__ = [
  "CHART_TEMPLATES",
  "TEMPLATE_KEYS",
  "Account",
  "ChartTemplate",
  "get_template",
  "list_templates",
]
