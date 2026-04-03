"""Seed data for the standard US GAAP reporting taxonomy.

Defines SFAC 6 root elements, US GAAP reporting concepts, reporting
structures (Income Statement, Balance Sheet, Cash Flow), and their
hierarchy associations. Derived from the existing CanonicalConcept
definitions in adapters/sec/taxonomy/.

The seed function uses raw SQL (not ORM) for use in Alembic migrations.
All data goes into the public schema (shared across tenants).
"""

from sqlalchemy import text

# ──────────────────────────────────────────────────────────────────────────────
# IDs — stable, deterministic IDs for seed data (not ULIDs)
# ──────────────────────────────────────────────────────────────────────────────

TAXONOMY_ID = "tax_usgaap_reporting"
STRUCT_IS_ID = "struct_income_statement"
STRUCT_BS_ID = "struct_balance_sheet"
STRUCT_CF_ID = "struct_cash_flow"

# ──────────────────────────────────────────────────────────────────────────────
# SFAC 6 Root Elements (depth 0, all abstract)
# ──────────────────────────────────────────────────────────────────────────────

SFAC6_ELEMENTS = [
  {
    "id": "elem_sfac6_assets",
    "qname": "sfac6:Assets",
    "namespace": "sfac6",
    "name": "Assets",
    "classification": "asset",
    "balance_type": "debit",
    "period_type": "instant",
    "is_abstract": True,
    "element_type": "abstract",
    "source": "sfac6",
    "depth": 0,
  },
  {
    "id": "elem_sfac6_liabilities",
    "qname": "sfac6:Liabilities",
    "namespace": "sfac6",
    "name": "Liabilities",
    "classification": "liability",
    "balance_type": "credit",
    "period_type": "instant",
    "is_abstract": True,
    "element_type": "abstract",
    "source": "sfac6",
    "depth": 0,
  },
  {
    "id": "elem_sfac6_equity",
    "qname": "sfac6:Equity",
    "namespace": "sfac6",
    "name": "Equity",
    "classification": "equity",
    "balance_type": "credit",
    "period_type": "instant",
    "is_abstract": True,
    "element_type": "abstract",
    "source": "sfac6",
    "depth": 0,
  },
  {
    "id": "elem_sfac6_revenues",
    "qname": "sfac6:Revenues",
    "namespace": "sfac6",
    "name": "Revenues",
    "classification": "revenue",
    "balance_type": "credit",
    "period_type": "duration",
    "is_abstract": True,
    "element_type": "abstract",
    "source": "sfac6",
    "depth": 0,
  },
  {
    "id": "elem_sfac6_expenses",
    "qname": "sfac6:Expenses",
    "namespace": "sfac6",
    "name": "Expenses",
    "classification": "expense",
    "balance_type": "debit",
    "period_type": "duration",
    "is_abstract": True,
    "element_type": "abstract",
    "source": "sfac6",
    "depth": 0,
  },
  {
    "id": "elem_sfac6_gains",
    "qname": "sfac6:Gains",
    "namespace": "sfac6",
    "name": "Gains",
    "classification": "revenue",
    "balance_type": "credit",
    "period_type": "duration",
    "is_abstract": True,
    "element_type": "abstract",
    "source": "sfac6",
    "depth": 0,
  },
  {
    "id": "elem_sfac6_losses",
    "qname": "sfac6:Losses",
    "namespace": "sfac6",
    "name": "Losses",
    "classification": "expense",
    "balance_type": "debit",
    "period_type": "duration",
    "is_abstract": True,
    "element_type": "abstract",
    "source": "sfac6",
    "depth": 0,
  },
  {
    "id": "elem_sfac6_comprehensive_income",
    "qname": "sfac6:ComprehensiveIncome",
    "namespace": "sfac6",
    "name": "Comprehensive Income",
    "classification": "equity",
    "balance_type": "credit",
    "period_type": "duration",
    "is_abstract": True,
    "element_type": "abstract",
    "source": "sfac6",
    "depth": 0,
  },
  {
    "id": "elem_sfac6_investments_by_owners",
    "qname": "sfac6:InvestmentsByOwners",
    "namespace": "sfac6",
    "name": "Investments by Owners",
    "classification": "equity",
    "balance_type": "credit",
    "period_type": "duration",
    "is_abstract": True,
    "element_type": "abstract",
    "source": "sfac6",
    "depth": 0,
  },
  {
    "id": "elem_sfac6_distributions_to_owners",
    "qname": "sfac6:DistributionsToOwners",
    "namespace": "sfac6",
    "name": "Distributions to Owners",
    "classification": "equity",
    "balance_type": "debit",
    "period_type": "duration",
    "is_abstract": True,
    "element_type": "abstract",
    "source": "sfac6",
    "depth": 0,
  },
]

# ──────────────────────────────────────────────────────────────────────────────
# US GAAP Reporting Elements (depth 1-3)
# Each tuple: (id, qname, name, classification, balance_type, period_type,
#               is_abstract, parent_sfac6_id, statement)
# ──────────────────────────────────────────────────────────────────────────────


def _elem(
  slug: str,
  qname: str,
  name: str,
  classification: str,
  balance: str,
  period: str,
  parent_id: str,
  *,
  abstract: bool = False,
  depth: int = 1,
) -> dict:
  return {
    "id": f"elem_gaap_{slug}",
    "qname": f"us-gaap:{qname}",
    "namespace": "us-gaap",
    "name": name,
    "classification": classification,
    "balance_type": balance,
    "period_type": period,
    "is_abstract": abstract,
    "element_type": "abstract" if abstract else "concept",
    "source": "us-gaap",
    "parent_id": parent_id,
    "depth": depth,
  }


# -- Income Statement elements --
_IS = "elem_sfac6_revenues"
_EX = "elem_sfac6_expenses"
_GA = "elem_sfac6_gains"
_LO = "elem_sfac6_losses"

INCOME_STATEMENT_ELEMENTS = [
  # Revenue
  _elem("revenues", "Revenues", "Revenue", "revenue", "credit", "duration", _IS),
  # Cost of Revenue (under Expenses)
  _elem(
    "cost_of_revenue",
    "CostOfRevenue",
    "Cost of Revenue",
    "expense",
    "debit",
    "duration",
    _EX,
  ),
  # Gross Profit (derived, under Revenues for display)
  _elem(
    "gross_profit", "GrossProfit", "Gross Profit", "revenue", "credit", "duration", _IS
  ),
  # Operating Expenses (abstract grouping)
  _elem(
    "operating_expenses",
    "OperatingExpenses",
    "Operating Expenses",
    "expense",
    "debit",
    "duration",
    _EX,
    abstract=True,
  ),
  # Operating Expense detail (depth 2)
  _elem(
    "research_development",
    "ResearchAndDevelopmentExpense",
    "Research and Development",
    "expense",
    "debit",
    "duration",
    "elem_gaap_operating_expenses",
    depth=2,
  ),
  _elem(
    "sga",
    "SellingGeneralAndAdministrativeExpense",
    "Selling, General and Administrative",
    "expense",
    "debit",
    "duration",
    "elem_gaap_operating_expenses",
    depth=2,
  ),
  _elem(
    "depreciation_amortization",
    "DepreciationDepletionAndAmortization",
    "Depreciation and Amortization",
    "expense",
    "debit",
    "duration",
    "elem_gaap_operating_expenses",
    depth=2,
  ),
  _elem(
    "stock_compensation",
    "ShareBasedCompensation",
    "Stock-Based Compensation",
    "expense",
    "debit",
    "duration",
    "elem_gaap_operating_expenses",
    depth=2,
  ),
  _elem(
    "other_operating_expense",
    "OtherCostAndExpenseOperating",
    "Other Operating Expense",
    "expense",
    "debit",
    "duration",
    "elem_gaap_operating_expenses",
    depth=2,
  ),
  # Operating Income
  _elem(
    "operating_income",
    "OperatingIncomeLoss",
    "Operating Income",
    "revenue",
    "credit",
    "duration",
    _IS,
  ),
  # Non-operating (under Gains/Losses)
  _elem(
    "interest_income",
    "InvestmentIncomeInterest",
    "Interest Income",
    "revenue",
    "credit",
    "duration",
    _GA,
  ),
  _elem(
    "interest_expense",
    "InterestExpense",
    "Interest Expense",
    "expense",
    "debit",
    "duration",
    _LO,
  ),
  _elem(
    "nonoperating_income",
    "NonoperatingIncomeExpense",
    "Non-Operating Income (Expense)",
    "revenue",
    "credit",
    "duration",
    _GA,
  ),
  # Bottom line
  _elem(
    "pretax_income",
    "IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest",
    "Pretax Income",
    "revenue",
    "credit",
    "duration",
    _IS,
  ),
  _elem(
    "income_tax_expense",
    "IncomeTaxExpenseBenefit",
    "Income Tax Expense",
    "expense",
    "debit",
    "duration",
    _EX,
  ),
  _elem(
    "net_income", "NetIncomeLoss", "Net Income", "revenue", "credit", "duration", _IS
  ),
  # Comprehensive Income
  _elem(
    "other_comprehensive_income",
    "OtherComprehensiveIncomeLossNetOfTax",
    "Other Comprehensive Income",
    "equity",
    "credit",
    "duration",
    "elem_sfac6_comprehensive_income",
  ),
  _elem(
    "total_comprehensive_income",
    "ComprehensiveIncomeNetOfTax",
    "Comprehensive Income",
    "equity",
    "credit",
    "duration",
    "elem_sfac6_comprehensive_income",
  ),
]

# -- Balance Sheet elements --
_AS = "elem_sfac6_assets"
_LI = "elem_sfac6_liabilities"
_EQ = "elem_sfac6_equity"

BALANCE_SHEET_ELEMENTS = [
  # Current Assets (abstract)
  _elem(
    "assets_current",
    "AssetsCurrent",
    "Current Assets",
    "asset",
    "debit",
    "instant",
    _AS,
    abstract=True,
  ),
  _elem(
    "cash",
    "CashAndCashEquivalentsAtCarryingValue",
    "Cash and Cash Equivalents",
    "asset",
    "debit",
    "instant",
    "elem_gaap_assets_current",
    depth=2,
  ),
  _elem(
    "short_term_investments",
    "ShortTermInvestments",
    "Short-Term Investments",
    "asset",
    "debit",
    "instant",
    "elem_gaap_assets_current",
    depth=2,
  ),
  _elem(
    "accounts_receivable",
    "AccountsReceivableNetCurrent",
    "Accounts Receivable",
    "asset",
    "debit",
    "instant",
    "elem_gaap_assets_current",
    depth=2,
  ),
  _elem(
    "inventory",
    "InventoryNet",
    "Inventory",
    "asset",
    "debit",
    "instant",
    "elem_gaap_assets_current",
    depth=2,
  ),
  _elem(
    "prepaid_expenses",
    "PrepaidExpenseAndOtherAssetsCurrent",
    "Prepaid Expenses and Other Current Assets",
    "asset",
    "debit",
    "instant",
    "elem_gaap_assets_current",
    depth=2,
  ),
  # Non-current Assets (abstract)
  _elem(
    "assets_noncurrent",
    "AssetsNoncurrent",
    "Non-Current Assets",
    "asset",
    "debit",
    "instant",
    _AS,
    abstract=True,
  ),
  _elem(
    "ppe",
    "PropertyPlantAndEquipmentNet",
    "Property, Plant and Equipment",
    "asset",
    "debit",
    "instant",
    "elem_gaap_assets_noncurrent",
    depth=2,
  ),
  _elem(
    "goodwill",
    "Goodwill",
    "Goodwill",
    "asset",
    "debit",
    "instant",
    "elem_gaap_assets_noncurrent",
    depth=2,
  ),
  _elem(
    "intangibles",
    "IntangibleAssetsNetExcludingGoodwill",
    "Intangible Assets",
    "asset",
    "debit",
    "instant",
    "elem_gaap_assets_noncurrent",
    depth=2,
  ),
  _elem(
    "other_assets_noncurrent",
    "OtherAssetsNoncurrent",
    "Other Non-Current Assets",
    "asset",
    "debit",
    "instant",
    "elem_gaap_assets_noncurrent",
    depth=2,
  ),
  # Total Assets
  _elem("total_assets", "Assets", "Total Assets", "asset", "debit", "instant", _AS),
  # Current Liabilities (abstract)
  _elem(
    "liabilities_current",
    "LiabilitiesCurrent",
    "Current Liabilities",
    "liability",
    "credit",
    "instant",
    _LI,
    abstract=True,
  ),
  _elem(
    "accounts_payable",
    "AccountsPayableCurrent",
    "Accounts Payable",
    "liability",
    "credit",
    "instant",
    "elem_gaap_liabilities_current",
    depth=2,
  ),
  _elem(
    "accrued_liabilities",
    "AccruedLiabilitiesCurrent",
    "Accrued Liabilities",
    "liability",
    "credit",
    "instant",
    "elem_gaap_liabilities_current",
    depth=2,
  ),
  _elem(
    "debt_current",
    "LongTermDebtCurrent",
    "Current Portion of Long-Term Debt",
    "liability",
    "credit",
    "instant",
    "elem_gaap_liabilities_current",
    depth=2,
  ),
  _elem(
    "deferred_revenue_current",
    "DeferredRevenueCurrent",
    "Deferred Revenue (Current)",
    "liability",
    "credit",
    "instant",
    "elem_gaap_liabilities_current",
    depth=2,
  ),
  # Non-current Liabilities (abstract)
  _elem(
    "liabilities_noncurrent",
    "LiabilitiesNoncurrent",
    "Non-Current Liabilities",
    "liability",
    "credit",
    "instant",
    _LI,
    abstract=True,
  ),
  _elem(
    "debt_noncurrent",
    "LongTermDebtNoncurrent",
    "Long-Term Debt",
    "liability",
    "credit",
    "instant",
    "elem_gaap_liabilities_noncurrent",
    depth=2,
  ),
  _elem(
    "other_liabilities_noncurrent",
    "OtherLiabilitiesNoncurrent",
    "Other Non-Current Liabilities",
    "liability",
    "credit",
    "instant",
    "elem_gaap_liabilities_noncurrent",
    depth=2,
  ),
  # Total Liabilities
  _elem(
    "total_liabilities",
    "Liabilities",
    "Total Liabilities",
    "liability",
    "credit",
    "instant",
    _LI,
  ),
  # Equity
  _elem(
    "common_stock",
    "CommonStockValue",
    "Common Stock",
    "equity",
    "credit",
    "instant",
    _EQ,
  ),
  _elem(
    "additional_paid_in_capital",
    "AdditionalPaidInCapital",
    "Additional Paid-In Capital",
    "equity",
    "credit",
    "instant",
    _EQ,
  ),
  _elem(
    "retained_earnings",
    "RetainedEarningsAccumulatedDeficit",
    "Retained Earnings",
    "equity",
    "credit",
    "instant",
    _EQ,
  ),
  _elem(
    "treasury_stock",
    "TreasuryStockValue",
    "Treasury Stock",
    "equity",
    "debit",
    "instant",
    _EQ,
  ),
  _elem(
    "aoci",
    "AccumulatedOtherComprehensiveIncomeLossNetOfTax",
    "Accumulated Other Comprehensive Income",
    "equity",
    "credit",
    "instant",
    _EQ,
  ),
  _elem(
    "stockholders_equity",
    "StockholdersEquity",
    "Total Stockholders' Equity",
    "equity",
    "credit",
    "instant",
    _EQ,
  ),
  _elem(
    "liabilities_and_equity",
    "LiabilitiesAndStockholdersEquity",
    "Total Liabilities and Equity",
    "liability",
    "credit",
    "instant",
    _LI,
  ),
]

# -- Cash Flow elements --
CASH_FLOW_ELEMENTS = [
  # Operating
  _elem(
    "operating_cash_flow",
    "NetCashProvidedByUsedInOperatingActivities",
    "Cash from Operating Activities",
    "asset",
    "debit",
    "duration",
    _AS,
    abstract=True,
  ),
  # Investing
  _elem(
    "capex",
    "PaymentsToAcquirePropertyPlantAndEquipment",
    "Capital Expenditures",
    "asset",
    "credit",
    "duration",
    _AS,
  ),
  _elem(
    "investing_cash_flow",
    "NetCashProvidedByUsedInInvestingActivities",
    "Cash from Investing Activities",
    "asset",
    "debit",
    "duration",
    _AS,
    abstract=True,
  ),
  # Financing
  _elem(
    "dividends_paid",
    "PaymentsOfDividends",
    "Dividends Paid",
    "equity",
    "debit",
    "duration",
    _EQ,
  ),
  _elem(
    "share_repurchases",
    "PaymentsForRepurchaseOfCommonStock",
    "Share Repurchases",
    "equity",
    "debit",
    "duration",
    _EQ,
  ),
  _elem(
    "financing_cash_flow",
    "NetCashProvidedByUsedInFinancingActivities",
    "Cash from Financing Activities",
    "asset",
    "debit",
    "duration",
    _AS,
    abstract=True,
  ),
  # Net Change
  _elem(
    "net_change_in_cash",
    "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsPeriodIncreaseDecreaseIncludingExchangeRateEffect",
    "Net Change in Cash",
    "asset",
    "debit",
    "duration",
    _AS,
  ),
]

ALL_GAAP_ELEMENTS = (
  INCOME_STATEMENT_ELEMENTS + BALANCE_SHEET_ELEMENTS + CASH_FLOW_ELEMENTS
)

# ──────────────────────────────────────────────────────────────────────────────
# Structures
# ──────────────────────────────────────────────────────────────────────────────

STRUCTURES = [
  {
    "id": STRUCT_IS_ID,
    "name": "US GAAP Income Statement",
    "structure_type": "income_statement",
    "taxonomy_id": TAXONOMY_ID,
  },
  {
    "id": STRUCT_BS_ID,
    "name": "US GAAP Balance Sheet",
    "structure_type": "balance_sheet",
    "taxonomy_id": TAXONOMY_ID,
  },
  {
    "id": STRUCT_CF_ID,
    "name": "US GAAP Cash Flow Statement",
    "structure_type": "cash_flow_statement",
    "taxonomy_id": TAXONOMY_ID,
  },
]


def _structure_for_element(elem: dict) -> str:
  """Determine which structure an element belongs to based on its parent chain."""
  parent = elem.get("parent_id", "")
  eid = elem["id"]

  # Cash flow elements
  if eid.startswith("elem_gaap_") and any(
    k in eid
    for k in (
      "operating_cash_flow",
      "capex",
      "investing_cash_flow",
      "dividends_paid",
      "share_repurchases",
      "financing_cash_flow",
      "net_change_in_cash",
    )
  ):
    return STRUCT_CF_ID

  # Balance sheet elements
  if parent in (_AS, _LI, _EQ) or any(
    k in (elem.get("parent_id") or "")
    for k in (
      "assets_current",
      "assets_noncurrent",
      "liabilities_current",
      "liabilities_noncurrent",
    )
  ):
    if elem in BALANCE_SHEET_ELEMENTS:
      return STRUCT_BS_ID

  # Income statement elements
  if elem in INCOME_STATEMENT_ELEMENTS:
    return STRUCT_IS_ID

  # Default by classification
  if elem.get("period_type") == "instant":
    return STRUCT_BS_ID
  return STRUCT_IS_ID


# ──────────────────────────────────────────────────────────────────────────────
# Seed function
# ──────────────────────────────────────────────────────────────────────────────


def seed_reporting_taxonomy(connection) -> None:
  """Seed the standard US GAAP reporting taxonomy into the public schema.

  Idempotent: skips if taxonomy already exists.
  Uses raw SQL for compatibility with Alembic migrations.

  Args:
      connection: A SQLAlchemy connection (from op.get_bind() in migrations).
  """
  # Check if already seeded
  result = connection.execute(
    text("SELECT id FROM public.taxonomies WHERE id = :id"),
    {"id": TAXONOMY_ID},
  )
  if result.fetchone():
    return

  # 1. Create taxonomy
  connection.execute(
    text("""
            INSERT INTO public.taxonomies (id, name, description, taxonomy_type, version,
                standard, namespace_uri, is_shared, is_active, is_locked, metadata, created_by)
            VALUES (:id, :name, :desc, :type, :version, :standard, :ns, true, true, true, '{}'::jsonb, 'system')
        """),
    {
      "id": TAXONOMY_ID,
      "name": "US GAAP Reporting Taxonomy",
      "desc": "Standard US GAAP reporting hierarchy rooted in SFAC 6",
      "type": "reporting",
      "version": "2024",
      "standard": "us-gaap",
      "ns": "http://fasb.org/us-gaap/2024",
    },
  )

  # 2. Create structures
  for s in STRUCTURES:
    connection.execute(
      text("""
                INSERT INTO public.structures (id, name, structure_type, taxonomy_id,
                    is_active, metadata, created_by)
                VALUES (:id, :name, :type, :tax_id, true, '{}'::jsonb, 'system')
            """),
      {
        "id": s["id"],
        "name": s["name"],
        "type": s["structure_type"],
        "tax_id": s["taxonomy_id"],
      },
    )

  # 3. Create SFAC 6 root elements
  for e in SFAC6_ELEMENTS:
    _insert_element(connection, e)

  # 4. Create US GAAP elements
  for e in ALL_GAAP_ELEMENTS:
    _insert_element(connection, e)

  # 5. Create hierarchy associations (parent → child via structures)
  # Order per-parent so siblings sort correctly within each section
  # (global counter caused Revenue/Expenses/Gains/Losses to interleave)
  order_by_parent: dict[str, int] = {}
  assoc_seq = 0
  for e in ALL_GAAP_ELEMENTS:
    parent_id = e.get("parent_id")
    if not parent_id:
      continue
    struct_id = _structure_for_element(e)
    order_by_parent[parent_id] = order_by_parent.get(parent_id, 0) + 1
    assoc_seq += 1
    connection.execute(
      text("""
                INSERT INTO public.element_associations
                    (id, structure_id, from_element_id, to_element_id,
                     association_type, order_value, metadata, created_by)
                VALUES (:id, :struct, :from_id, :to_id, 'presentation', :ord, '{}'::jsonb, 'system')
            """),
      {
        "id": f"assoc_gaap_{assoc_seq:04d}",
        "struct": struct_id,
        "from_id": parent_id,
        "to_id": e["id"],
        "ord": float(order_by_parent[parent_id]),
      },
    )


def seed_tenant_reporting_taxonomy(connection, schema: str) -> None:
  """Copy the standard reporting taxonomy from public schema into a tenant schema.

  Called by provision_tenant_schema() so that each tenant can see the shared
  reporting taxonomy via its own tables (avoiding search_path shadowing).

  Idempotent: skips if taxonomy already exists in the tenant schema.

  Args:
      connection: A SQLAlchemy connection.
      schema: The tenant schema name (e.g., 'kg19d355cfe0460db38a').
  """
  result = connection.execute(
    text(f"SELECT id FROM {schema}.taxonomies WHERE id = :id"),
    {"id": TAXONOMY_ID},
  )
  if result.fetchone():
    return

  # Check that public seed data exists
  result = connection.execute(
    text("SELECT id FROM public.taxonomies WHERE id = :id"),
    {"id": TAXONOMY_ID},
  )
  if not result.fetchone():
    return

  # Use explicit column lists to handle different column ordering
  # between public schema (migration-managed) and tenant schemas (create_all).

  tax_cols = (
    "id, name, description, taxonomy_type, version, standard, namespace_uri, "
    "is_shared, source_taxonomy_id, target_taxonomy_id, is_active, is_locked, "
    "metadata, created_at, updated_at, created_by"
  )
  connection.execute(
    text(f"""
      INSERT INTO {schema}.taxonomies ({tax_cols})
        SELECT {tax_cols} FROM public.taxonomies WHERE id = :id
    """),
    {"id": TAXONOMY_ID},
  )

  struct_cols = (
    "id, name, description, structure_type, taxonomy_id, graph_structure_id, "
    "is_active, metadata, created_at, updated_at, created_by"
  )
  connection.execute(
    text(f"""
      INSERT INTO {schema}.structures ({struct_cols})
        SELECT {struct_cols} FROM public.structures WHERE taxonomy_id = :id
    """),
    {"id": TAXONOMY_ID},
  )

  elem_cols = (
    "id, code, name, description, qname, namespace, uri, classification, "
    "sub_classification, balance_type, period_type, is_abstract, is_monetary, "
    "element_type, parent_id, depth, path, taxonomy_id, source, currency, "
    "is_active, is_placeholder, external_id, external_source, metadata, "
    "version, created_at, updated_at, created_by"
  )
  connection.execute(
    text(f"""
      INSERT INTO {schema}.elements ({elem_cols})
        SELECT {elem_cols} FROM public.elements WHERE taxonomy_id = :id
    """),
    {"id": TAXONOMY_ID},
  )

  assoc_cols = (
    "id, structure_id, from_element_id, to_element_id, association_type, "
    "arcrole, order_value, weight, confidence, suggested_by, approved_by, "
    "approved_at, metadata, created_at, updated_at, created_by"
  )
  connection.execute(
    text(f"""
      INSERT INTO {schema}.element_associations ({assoc_cols})
        SELECT {assoc_cols} FROM public.element_associations
        WHERE structure_id IN (
          SELECT id FROM public.structures WHERE taxonomy_id = :id
        )
    """),
    {"id": TAXONOMY_ID},
  )


def _insert_element(connection, e: dict) -> None:
  """Insert a single element into public.elements."""
  connection.execute(
    text("""
            INSERT INTO public.elements
                (id, qname, namespace, name, classification, balance_type, period_type,
                 is_abstract, is_monetary, element_type, source, parent_id, depth,
                 path, taxonomy_id, currency, is_active, is_placeholder, metadata,
                 version, created_by, created_at, updated_at)
            VALUES
                (:id, :qname, :ns, :name, :cls, :bal, :period,
                 :abstract, true, :etype, :source, :parent, :depth,
                 '', :tax_id, 'USD', true, false, '{}'::jsonb,
                 1, 'system', now(), now())
        """),
    {
      "id": e["id"],
      "qname": e["qname"],
      "ns": e["namespace"],
      "name": e["name"],
      "cls": e["classification"],
      "bal": e["balance_type"],
      "period": e["period_type"],
      "abstract": e["is_abstract"],
      "etype": e["element_type"],
      "source": e["source"],
      "parent": e.get("parent_id"),
      "depth": e.get("depth", 0),
      "tax_id": TAXONOMY_ID,
    },
  )
