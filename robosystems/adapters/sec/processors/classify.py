"""Association classification via embedded LadybugDB.

Two classification layers:

1. **Structural** — Detects patterns (RollUp, RollForward, Hierarchy, etc.) by
   running Cypher pattern-matching queries against associations and elements.

2. **Semantic** — Identifies *what kind of* structure each association belongs to
   (e.g., "AssetsRollUp", "CashFlowStatement") by matching calculation root
   elements against Hoffman's disclosure mechanics taxonomy (143 mappings).

Results are returned as DataFrames for the existing parquet pipeline to pick up.
This module uses real_ladybug directly — no Graph API, no HTTP.
"""

from __future__ import annotations

import shutil
import tempfile
from pathlib import Path
from typing import Any

import pandas as pd

from robosystems.logger import logger
from robosystems.utils.uuid import generate_uuid7

# ---------------------------------------------------------------------------
# Disclosure mechanics concept map (Hoffman taxonomy, 143 entries)
# ---------------------------------------------------------------------------
# Maps us-gaap element local name → Hoffman disclosure name.
# Source: conceptArrangementPattern-requiresConcept + concept-allowedAlternativeConcept
# arcroles from the disclosure mechanics taxonomy.

DISCLOSURE_CONCEPT_MAP: dict[str, str] = {
  "AccountsAndOtherReceivablesNetCurrent": "AccountsNotesLoansAndFinancingReceivable",
  "AccountsNotesAndLoansReceivableNetCurrent": "AccountsNotesLoansAndFinancingReceivable",
  "AccountsPayableAndAccruedLiabilitiesCurrent": "AccountsPayableAndAccruedLiabilitiesRollUp",
  "AccountsPayableAndAccruedLiabilitiesCurrentAndNoncurrent": "AccountsPayableAndAccruedLiabilitiesRollUp",
  "AccountsPayableCurrent": "AccruedLiabilities",
  "AccountsReceivableNetCurrent": "AccountsNotesLoansAndFinancingReceivable",
  "AccruedLiabilitiesCurrent": "AccruedLiabilities",
  "AccumulatedOtherComprehensiveIncomeLossNetOfTax": "AccumulatedOtherComprehensiveIncomeLoss",
  "AgriculturalRelatedInventory": "InventoryNetRollUp",
  "AirlineRelatedInventory": "InventoryNetRollUp",
  "AllowanceForDoubtfulAccountsReceivable": "AllowanceForCreditLossesonFinancingReceivables",
  "AssetRetirementObligation": "AssetRetirementObligationRollForwardAnalysisRollForward",
  "Assets": "AssetsRollUp",
  "AssetsCurrent": "AssetsRollUp",
  "CapitalLeaseObligations": "CapitalLeasesFutureMinimumPaymentsPresentValueOfNetMinimumPaymentsRollUp",
  "CapitalLeasesFutureMinimumPaymentsDue": "CapitalLeasesFutureMinimumPaymentsPresentValueOfNetMinimumPaymentsRollUp",
  "CapitalLeasesFutureMinimumPaymentsPresentValueOfNetMinimumPayments": "CapitalLeasesFutureMinimumPaymentsPresentValueOfNetMinimumPaymentsRollUp",
  "CapitalLeasesFutureMinimumPaymentsReceivable": "CapitalLeasesFutureMinimumPaymentsReceivableRollUp",
  "CashAndCashEquivalentsAtCarryingValue": "CashAndCashEquivalentsDetails",
  "CashAndCashEquivalentsPeriodIncreaseDecrease": "CashFlowStatement",
  "CashAndCashEquivalentsPeriodIncreaseDecreaseExcludingExchangeRateEffect": "CashFlowStatement",
  "CashPeriodIncreaseDecrease": "CashFlowStatement",
  "ClassOfWarrantOrRightOutstanding": "StockholdersEquityNoteWarrantsOrRights",
  "ComprehensiveIncomeNetOfTax": "StatementOfComprehensiveIncome",
  "ComprehensiveIncomeNetOfTaxIncludingPortionAttributableToNoncontrollingInterest": "IncomeStatement",
  "ContractualObligation": "OperatingLeasesFutureMinimumPaymentsDueRollUp",
  "DebtAndCapitalLeaseObligations": "LongTermDebtMaturities",
  "DebtInstrumentCarryingAmount": "LongTermDebtInstruments",
  "DebtInstrumentFaceAmount": "LongTermDebtInstrumentsRollUp",
  "DebtLongtermAndShorttermCombinedAmount": "LongTermDebtInstruments",
  "DeferredIncomeTaxLiabilitiesNet": "DeferredTaxAssetsAndLiabilities",
  "DeferredTaxAssetsLiabilitiesNet": "DeferredTaxAssetsAndLiabilities",
  "DeferredTaxAssetsNet": "DeferredTaxAssetsAndLiabilities",
  "DeferredTaxLiabilities": "DeferredTaxAssetsAndLiabilities",
  "DefinedBenefitPlanAssumptionsUsedCalculatingBenefitObligationDiscountRate": "AssumptionsUsed",
  "DefinedBenefitPlanAssumptionsUsedCalculatingBenefitObligationRateOfCompensationIncrease": "AssumptionsUsed",
  "DefinedBenefitPlanAssumptionsUsedCalculatingNetPeriodicBenefitCostDiscountRate": "AssumptionsUsed",
  "DefinedBenefitPlanAssumptionsUsedCalculatingNetPeriodicBenefitCostExpectedLongTermReturnOnAssets": "AssumptionsUsed",
  "DefinedBenefitPlanAssumptionsUsedCalculatingNetPeriodicBenefitCostRateOfCompensationIncrease": "AssumptionsUsed",
  "DefinedBenefitPlanBenefitObligation": "DefinedBenefitPlanBenefitObligationRollForward",
  "DefinedBenefitPlanExpectedFutureBenefitPaymentsNextTwelveMonths": "ExpectedBenefitPayments",
  "DefinedBenefitPlanFairValueOfPlanAssets": "DefinedBenefitPlanFairValueOfPlanAssetsRollForward",
  "DefinedBenefitPlanNetPeriodicBenefitCost": "NetBenefitCosts",
  "DefinedBenefitPlanTargetPlanAssetAllocations": "AllocationOfPlanAssets",
  "DocumentFiscalPeriodFocus": "DocumentInformation",
  "EarningsPerShareBasic": "EarningsPerShareDisclosuresHierarchy",
  "EarningsPerShareBasicAndDiluted": "EarningsPerShareBasicAndDilutedRollUp",
  "EffectiveIncomeTaxRateContinuingOperations": "EffectiveIncomeTaxRateContinuingOperationsTaxRateReconciliationRollUp",
  "EnergyRelatedInventory": "InventoryNetRollUp",
  "EntityRegistrantName": "EntityInformation",
  "EntityWideDisclosureOnGeographicAreasDescriptionOfRevenueFromExternalCustomers": "RevenuefromExternalCustomersAttributedToForeignCountriesByGeographicArea",
  "EntityWideDisclosureOnGeographicAreasLongLivedAssets": "GeographicAreasLongLivedAssetsInIndividualForeignCountriesByCountryDisclosure",
  "EnvironmentalExitCostsCostsAccruedToDate": "EnvironmentalExitCost",
  "ExtendedProductWarrantyAccrual": "ProductWarrantyLiability",
  "ExtendedProductWarrantyAccrualCurrent": "ProductWarrantyLiability",
  "ExtendedProductWarrantyAccrualNoncurrent": "ProductWarrantyLiability",
  "FairValueMeasurementWithUnobservableInputsReconciliationRecurringBasisAssetValue": "FairValueAssetsMeasuredonRecurringBasisUnobservableInputReconciliationCalculationRollForward",
  "FairValueMeasurementWithUnobservableInputsReconciliationsRecurringBasisLiabilityValue": "FairValueAssetsMeasuredonRecurringBasisUnobservableInputReconciliationCalculationRollForward",
  "FinancingReceivableAllowanceForCreditLosses": "AllowanceForCreditLossesonFinancingReceivables",
  "FiniteLivedIntangibleAssetUsefulLife": "FiniteLivedIntangibleAssetsEstimatedUsefulLives",
  "FiniteLivedIntangibleAssetsAmortizationExpenseNextTwelveMonths": "FiniteLivedIntangibleAssetsFutureAmortizationExpenseCurrentAndFiveSucceedingFiscalYearsHierarchy",
  "FiniteLivedIntangibleAssetsAmortizationExpenseYearThree": "FiniteLivedIntangibleAssetsFutureAmortizationExpenseCurrentAndFiveSucceedingFiscalYearsHierarchy",
  "FiniteLivedIntangibleAssetsAmortizationExpenseYearTwo": "FiniteLivedIntangibleAssetsFutureAmortizationExpenseCurrentAndFiveSucceedingFiscalYearsHierarchy",
  "FiniteLivedIntangibleAssetsGross": "AcquiredFiniteLivedIntangibleAssetByMajorClass",
  "FiniteLivedIntangibleAssetsNet": "FiniteLivedIntangibleAssetsNetRollUp",
  "Goodwill": "GoodwillRollForward",
  "Hierarchy": "EquityMethodInvestments",
  "IncomeLossAttributableToParent": "IncomeStatement",
  "IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest": "IncomebeforeIncomeTaxDomesticAndForeign",
  "IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethodInvestments": "IncomebeforeIncomeTaxDomesticAndForeign",
  "IncomeLossIncludingPortionAttributableToNoncontrollingInterest": "IncomeStatement",
  "IncomeTaxExpenseBenefit": "IncomeTaxExpenseBenefitDetails",
  "IndefiniteLivedIntangibleAssetsExcludingGoodwill": "IndefinitelivedIntangibleAssetsRollForward",
  "IndefinitelivedIntangibleAssetsAcquired": "IndefinitelivedIntangibleAssets",
  "InterestAndOtherIncome": "InterestAndOtherIncomeRollUp",
  "InventoryNet": "InventoryNetRollUp",
  "InventoryNetOfAllowancesCustomerAdvancesAndProgressBillings": "InventoryNetRollUp",
  "LiabilitiesAndStockholdersEquity": "LiabilitiesAndEquityRollUp",
  "LoansAndLeasesReceivableNetReportedAmount": "AccountsNotesLoansAndFinancingReceivable",
  "LongTermDebt": "LongTermDebtMaturities",
  "LongTermDebtAndCapitalLeaseObligations": "LongTermDebtInstruments",
  "LongTermDebtAndCapitalLeaseObligationsIncludingCurrentMaturities": "LongTermDebtMaturities",
  "LongTermDebtAndCapitalLeaseObligationsMaturitiesRepaymentsOfPrincipalAfterYearFive": "LongTermDebtMaturities2",
  "LongTermDebtAndCapitalLeaseObligationsMaturitiesRepaymentsOfPrincipalInYearThree": "LongTermDebtMaturities2",
  "LongTermDebtAndCapitalLeaseObligationsMaturitiesRepaymentsOfPrincipalInYearTwo": "LongTermDebtMaturities2",
  "LongTermDebtAndCapitalLeaseObligationsRepaymentsOfPrincipalInNextTwelveMonths": "LongTermDebtMaturities2",
  "LongTermDebtMaturitiesRepaymentsOfPrincipalAfterYearFive": "LongTermDebtMaturities2",
  "LongTermDebtMaturitiesRepaymentsOfPrincipalInNextTwelveMonths": "LongTermDebtMaturities2",
  "LongTermDebtMaturitiesRepaymentsOfPrincipalInYearFive": "LongTermDebtMaturities2",
  "LongTermDebtMaturitiesRepaymentsOfPrincipalInYearFour": "LongTermDebtMaturities2",
  "LongTermDebtMaturitiesRepaymentsOfPrincipalInYearThree": "LongTermDebtMaturities2",
  "LongTermDebtMaturitiesRepaymentsOfPrincipalInYearTwo": "LongTermDebtMaturities2",
  "MembersEquity": "StatementOfChangesInEquity",
  "NetCashProvidedByUsedInContinuingOperations": "CashFlowStatement",
  "NetIncomeLoss": "IncomeStatement",
  "NetIncomeLossAvailableToCommonStockholdersBasic": "IncomeStatement",
  "NoncurrentAssets": "GeographicAreasLongLivedAssetsInIndividualForeignCountriesByCountryDisclosure",
  "NonmonetaryTransactionAmountOfBarterTransaction": "NonmonetaryTransactions",
  "NonoperatingIncomeExpense": "OtherNonoperatingIncomeExpense",
  "NotesReceivableNet": "AccountsNotesLoansAndFinancingReceivable",
  "OperatingLeasesFutureMinimumPaymentsDue": "OperatingLeasesFutureMinimumPaymentsDueRollUp",
  "OperatingLeasesFutureMinimumPaymentsDueCurrent": "FutureMinimumRentalPaymentsForOperatingLeases",
  "OperatingLeasesFutureMinimumPaymentsDueInThreeYears": "FutureMinimumRentalPaymentsForOperatingLeases",
  "OperatingLeasesFutureMinimumPaymentsDueInTwoYears": "FutureMinimumRentalPaymentsForOperatingLeases",
  "OperatingLeasesFutureMinimumPaymentsDueThereafter": "FutureMinimumRentalPaymentsForOperatingLeases",
  "OperatingLeasesFutureMinimumPaymentsReceivable": "OperatingLeasesFutureMinimumPaymentsReceivableRollUp",
  "OtherAssetsNoncurrent": "OtherAssetsNoncurrent",
  "OtherLiabilitiesNoncurrent": "OtherLiabilitiesNoncurrentHierarchy",
  "OtherNonoperatingIncomeExpense": "OtherNonoperatingIncomeExpense",
  "PartnersCapital": "StatementOfChangesInEquity",
  "ProductWarrantyAccrual": "StandardAndExtendedProductWarrantyMovementRollForward",
  "ProductWarrantyAccrualClassifiedCurrent": "ProductWarrantyLiability",
  "ProfitLoss": "StatementOfIncomeAndComprehensiveIncome",
  "PropertyPlantAndEquipmentEstimatedUsefulLives": "PropertyPlantAndEquipmentUsefulLives",
  "PropertyPlantAndEquipmentNet": "PropertyPlantAndEquipmentNetByTypeRollUp",
  "PropertyPlantAndEquipmentUsefulLife": "PropertyPlantAndEquipmentUsefulLives",
  "PublicUtilitiesInventory": "InventoryNetRollUp",
  "ReceivablesNetCurrent": "AccountsNotesLoansAndFinancingReceivable",
  "RestructuringAndRelatedCostExpectedCost1": "RestructuringAndRelatedCostHierarchy",
  "RestructuringAndRelatedCostExpectedCostRemaining1": "RestructuringReserveByTypeOfCost",
  "RestructuringAndRelatedCostIncurredCost": "RestructuringAndRelatedCostHierarchy",
  "RestructuringCharges": "RestructuringChargesRollUp",
  "RestructuringCostsAndAssetImpairmentCharges": "RestructuringAndRelatedCostHierarchy",
  "RestructuringReserve": "RestructuringReserveRollForward",
  "RetailRelatedInventory": "InventoryNetRollUp",
  "Revenues": "RevenuefromExternalCustomersAttributedToForeignCountriesByGeographicArea",
  "SalesRevenueGoodsNet": "RevenuefromExternalCustomersAttributedToForeignCountriesByGeographicArea",
  "SalesRevenueNet": "RevenuefromExternalCustomersAttributedToForeignCountriesByGeographicArea",
  "SalesRevenueServicesNet": "RevenuefromExternalCustomersAttributedToForeignCountriesByGeographicArea",
  "ShareBasedCompensationArrangementByShareBasedPaymentAwardEquityInstrumentsOtherThanOptionsNonvestedNumber": "SharebasedCompensationRestrictedStockUnitsAwardActivity",
  "ShareBasedCompensationArrangementByShareBasedPaymentAwardFairValueAssumptionsExpectedDividendRate": "SharebasedPaymentAwardStockOptionsValuationAssumptions",
  "ShareBasedCompensationArrangementByShareBasedPaymentAwardFairValueAssumptionsExpectedVolatilityRate": "SharebasedPaymentAwardStockOptionsValuationAssumptions",
  "ShareBasedCompensationArrangementByShareBasedPaymentAwardFairValueAssumptionsRiskFreeInterestRate": "SharebasedPaymentAwardStockOptionsValuationAssumptions",
  "ShareBasedCompensationArrangementByShareBasedPaymentAwardOptionsGrantsInPeriodWeightedAverageGrantDateFairValue": "SharebasedPaymentAwardStockOptionsValuationAssumptions",
  "ShareBasedCompensationArrangementByShareBasedPaymentAwardOptionsOutstandingNumber": "SharebasedCompensationArrangementsBySharebasedPaymentAward",
  "SharebasedCompensationArrangementBySharebasedPaymentAwardFairValueAssumptionsExpectedTerm1": "SharebasedPaymentAwardStockOptionsValuationAssumptions",
  "StandardProductWarrantyAccrual": "StandardProductWarrantyAccrualMovementRollForward",
  "StockholdersEquity": "AccumulatedOtherComprehensiveIncomeLoss",
  "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest": "StatementOfChangesInEquity",
  "SubsequentEventDescription": "SubsequentEventHierarchy",
  "TranslationAdjustmentFunctionalToReportingCurrencyNetOfTax": "TranslationAdjustmentFunctionalToReportingCurrencyRollForward",
  "UnrecognizedTaxBenefits": "UnrecognizedTaxBenefitsExcludingAmountsPertainingToExaminedTaxReturnsRollForward",
  "ValuationAllowancesAndReservesBalance": "CreditLossesForFinancingReceivablesCurrent",
}

# ---------------------------------------------------------------------------
# Disclosure-to-canonical concept bridge
# ---------------------------------------------------------------------------
# Maps disclosure names → canonical concept IDs. Only includes disclosures
# where the root element unambiguously maps to a single canonical concept.
# When an element is the calculation root of a known disclosure structure,
# this gives us near-certain canonical identification without embeddings.

DISCLOSURE_TO_CANONICAL: dict[str, str] = {
  # Balance Sheet
  "AssetsRollUp": "total_assets",
  "InventoryNetRollUp": "inventory",
  "GoodwillRollForward": "goodwill",
  "PropertyPlantAndEquipmentNetByTypeRollUp": "property_plant_equipment",
  "FiniteLivedIntangibleAssetsNetRollUp": "intangible_assets",
  "AccountsNotesLoansAndFinancingReceivable": "accounts_receivable",
  "LongTermDebtMaturities": "long_term_debt",
  "CashAndCashEquivalentsDetails": "cash_and_equivalents",
  # Income Statement
  "IncomeStatement": "net_income",
  "IncomeTaxExpenseBenefitDetails": "income_tax_expense",
  "EarningsPerShareDisclosuresHierarchy": "eps_basic",
  "EarningsPerShareBasicAndDilutedRollUp": "eps_basic",
  "EffectiveIncomeTaxRateContinuingOperationsTaxRateReconciliationRollUp": "income_tax_expense",
  "InterestAndOtherIncomeRollUp": "interest_expense",
  # Cash Flow
  "CashFlowStatement": "operating_cash_flow",
}

# ---------------------------------------------------------------------------
# Type mapping (subset of graph_api/core/ladybug/manager.py:_map_schema_type_to_lbug)
# ---------------------------------------------------------------------------

_TYPE_MAP = {
  "STRING": "STRING",
  "INT64": "INT64",
  "INT32": "INT32",
  "DOUBLE": "DOUBLE",
  "FLOAT": "FLOAT",
  "BOOLEAN": "BOOLEAN",
}


def _map_type(schema_type: str) -> str:
  mapped = _TYPE_MAP.get(schema_type.upper())
  if mapped:
    return mapped
  if "[" in schema_type:
    return schema_type
  return "STRING"


# ---------------------------------------------------------------------------
# Schema DDL generation (minimal subset for classification)
# ---------------------------------------------------------------------------


def _generate_ddl() -> list[str]:
  """Generate minimal CREATE TABLE DDL with only the columns used by classification queries.

  Uses a hardcoded minimal schema rather than the full schema definitions.
  This avoids column-count mismatches when loading parquet files that may not
  have all columns (e.g., in tests or when schema evolves).
  """
  return [
    # Association: only columns referenced in classification Cypher queries
    """CREATE NODE TABLE IF NOT EXISTS Association (
      identifier STRING,
      arcrole STRING,
      order_value DOUBLE,
      association_type STRING,
      weight DOUBLE,
      root STRING,
      preferred_label STRING,
      PRIMARY KEY (identifier)
    )""",
    # Element: only columns referenced in classification queries
    """CREATE NODE TABLE IF NOT EXISTS Element (
      identifier STRING,
      uri STRING,
      qname STRING,
      name STRING,
      period_type STRING,
      classification STRING,
      balance STRING,
      PRIMARY KEY (identifier)
    )""",
    # Structure: for semantic classification (mapping calc roots → disclosure names)
    """CREATE NODE TABLE IF NOT EXISTS Structure (
      identifier STRING,
      PRIMARY KEY (identifier)
    )""",
    # Relationship tables (no properties)
    "CREATE REL TABLE IF NOT EXISTS ASSOCIATION_HAS_FROM_ELEMENT (FROM Association TO Element)",
    "CREATE REL TABLE IF NOT EXISTS ASSOCIATION_HAS_TO_ELEMENT (FROM Association TO Element)",
    "CREATE REL TABLE IF NOT EXISTS STRUCTURE_HAS_ASSOCIATION (FROM Structure TO Association, association_context STRING)",
  ]


# ---------------------------------------------------------------------------
# TempLadybugContext
# ---------------------------------------------------------------------------


class TempLadybugContext:
  """Temporary embedded LadybugDB for per-filing Cypher-based analysis.

  Creates a LadybugDB database in a temp directory, installs schema DDL,
  and cleans up on exit. Uses real_ladybug directly — no Graph API.
  """

  def __init__(self, ddl_statements: list[str] | None = None):
    self._tmpdir: str | None = None
    self._db = None
    self._conn = None
    self._ddl = ddl_statements or _generate_ddl()

  def __enter__(self) -> TempLadybugContext:
    import real_ladybug as lbug

    self._tmpdir = tempfile.mkdtemp(prefix="lbug_classify_")
    db_path = str(Path(self._tmpdir) / "classify.lbug")

    self._db = lbug.Database(
      db_path,
      read_only=False,
      auto_checkpoint=True,
      checkpoint_threshold=67108864,  # 64MB — small single-filing DB
    )
    self._conn = lbug.Connection(self._db)

    # Install schema
    for stmt in self._ddl:
      try:
        self._conn.execute(stmt)
      except Exception as e:
        logger.debug(f"DDL statement skipped: {e}")

    return self

  def __exit__(self, exc_type, exc_val, exc_tb) -> None:
    self.close()

  def execute(self, cypher: str, params: dict[str, Any] | None = None) -> list[dict]:
    """Execute a Cypher query and return results as list of dicts."""
    if self._conn is None:
      raise RuntimeError("TempLadybugContext is not open")

    if params:
      result = self._conn.execute(cypher, params)
    else:
      result = self._conn.execute(cypher)

    # Convert to list of dicts
    if result is None:
      return []

    columns = result.get_column_names()
    rows = []
    while result.has_next():
      row_values = result.get_next()
      rows.append(dict(zip(columns, row_values, strict=True)))
    return rows

  def load_parquet(self, table_name: str, parquet_path: Path) -> None:
    """Load a parquet file into a table via COPY FROM.

    For node tables with extra columns in the parquet (e.g., full Element schema
    vs our minimal classification schema), we read with pandas, select only the
    columns we need, write a temp parquet, and load that instead.
    """
    if self._conn is None:
      raise RuntimeError("TempLadybugContext is not open")

    if not parquet_path.exists():
      logger.debug(f"Parquet file not found, skipping: {parquet_path}")
      return

    try:
      # All parquet I/O uses file handles (not paths) to avoid pyarrow
      # LocalFileSystem registration conflicts when DuckDB is in-process.
      import pyarrow.parquet as pq

      # Always load via pandas to ensure column selection and ordering match the DDL.
      # LadybugDB COPY FROM is position-based, so column order must match exactly.
      schema_cols = self._get_table_columns(table_name)
      if not schema_cols:
        # Relationship tables with no properties — direct COPY FROM
        with open(parquet_path, "rb") as f:
          table = pq.read_table(f)
        if table.num_rows == 0:
          return
        # Write to temp path via file handle for LadybugDB COPY
        tmp_path = Path(self._tmpdir) / f"{table_name}_copy.parquet"
        with open(tmp_path, "wb") as f:
          pq.write_table(table, f)
        self._conn.execute(f'COPY {table_name} FROM "{tmp_path}"')
        logger.debug(f"Loaded {table_name} from {parquet_path}")
        return

      # Read parquet header to get available columns
      with open(parquet_path, "rb") as f:
        pf = pq.ParquetFile(f)
        available_cols = pf.schema_arrow.names

      # Relationship tables with properties: table_info returns only property
      # columns, but COPY FROM expects from, to, then properties.
      is_rel_with_props = "from" in available_cols and "to" in available_cols
      if is_rel_with_props:
        schema_cols = ["from", "to", *schema_cols]

      cols_to_read = [c for c in schema_cols if c in available_cols]
      if not cols_to_read:
        logger.debug(f"No matching columns for {table_name}, skipping")
        return

      with open(parquet_path, "rb") as f:
        table = pq.read_table(f, columns=cols_to_read)
      df = table.to_pandas()
      # Reorder to match DDL and add missing columns as None
      for col in schema_cols:
        if col not in df.columns:
          df[col] = None
      df = df[schema_cols]

      tmp_path = Path(self._tmpdir) / f"{table_name}_ordered.parquet"
      import pyarrow as pa

      out_table = pa.Table.from_pandas(df, preserve_index=False)
      with open(tmp_path, "wb") as f:
        pq.write_table(out_table, f)
      self._conn.execute(f'COPY {table_name} FROM "{tmp_path}"')
      logger.debug(f"Loaded {table_name} from {parquet_path} ({len(df)} rows)")
    except Exception as e:
      logger.warning(f"Failed to load {table_name} from {parquet_path}: {e}")

  def _get_table_columns(self, table_name: str) -> list[str]:
    """Get column names for a node table from LadybugDB schema."""
    try:
      result = self._conn.execute(f"CALL table_info('{table_name}') RETURN name")
      cols = []
      while result.has_next():
        cols.append(result.get_next()[0])
      return cols
    except Exception:
      return []

  def close(self) -> None:
    """Close connection and clean up temp directory."""
    try:
      if self._conn is not None:
        self._conn.close()
        self._conn = None
      if self._db is not None:
        self._db.close()
        self._db = None
    finally:
      if self._tmpdir is not None:
        try:
          shutil.rmtree(self._tmpdir, ignore_errors=True)
        except Exception:
          pass
        self._tmpdir = None


# ---------------------------------------------------------------------------
# Classification queries
# ---------------------------------------------------------------------------

# Each query returns association_id values that match the pattern.
# Source for all deterministic rules is "arcrole_analysis" with confidence 1.0.
CLASSIFICATION_QUERIES: dict[str, str] = {
  # RollUp: presentation association where same element also appears in a calculation association
  "RollUp": """
    MATCH (a:Association)-[:ASSOCIATION_HAS_TO_ELEMENT]->(f:Element),
          (ac:Association)-[:ASSOCIATION_HAS_FROM_ELEMENT]->(f)
    WHERE a.association_type = 'Presentation'
      AND ac.association_type = 'Calculation'
    RETURN DISTINCT a.identifier AS association_id
  """,
  # RollForward: target element is instant with period start/end label
  "RollForward": """
    MATCH (a:Association)-[:ASSOCIATION_HAS_TO_ELEMENT]->(t:Element)
    WHERE a.association_type = 'Presentation'
      AND t.period_type = 'instant'
      AND a.preferred_label IN [
        'http://www.xbrl.org/2003/role/periodEndLabel',
        'http://www.xbrl.org/2003/role/periodStartLabel'
      ]
    RETURN DISTINCT a.identifier AS association_id
  """,
  # Hypercube: from-element is a hypercubeElement
  "Hypercube": """
    MATCH (a:Association)-[:ASSOCIATION_HAS_FROM_ELEMENT]->(f:Element)
    WHERE a.association_type = 'Presentation'
      AND f.classification = 'hypercubeElement'
    RETURN DISTINCT a.identifier AS association_id
  """,
  # LineItems: from-element is a lineItemsElement
  "LineItems": """
    MATCH (a:Association)-[:ASSOCIATION_HAS_FROM_ELEMENT]->(f:Element)
    WHERE a.association_type = 'Presentation'
      AND f.classification = 'lineItemsElement'
    RETURN DISTINCT a.identifier AS association_id
  """,
  # MemberList: from-element is a domainElement
  "MemberList": """
    MATCH (a:Association)-[:ASSOCIATION_HAS_FROM_ELEMENT]->(f:Element)
    WHERE a.association_type = 'Presentation'
      AND f.classification = 'domainElement'
    RETURN DISTINCT a.identifier AS association_id
  """,
  # Dimension: from-element is a dimensionElement
  "Dimension": """
    MATCH (a:Association)-[:ASSOCIATION_HAS_FROM_ELEMENT]->(f:Element)
    WHERE a.association_type = 'Presentation'
      AND f.classification = 'dimensionElement'
    RETURN DISTINCT a.identifier AS association_id
  """,
}


# ---------------------------------------------------------------------------
# AssociationClassifier
# ---------------------------------------------------------------------------


class AssociationClassifier:
  """Classifies associations using Cypher pattern detection on temp LadybugDB.

  Loads a filing's parquet output into a temporary embedded LadybugDB,
  runs classification queries, and returns Classification nodes and
  ASSOCIATION_HAS_CLASSIFICATION relationships as DataFrames.
  """

  def classify(
    self, output_dir: Path
  ) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, tuple[str, float]]]:
    """Run classification on a filing's parquet output.

    Runs two layers:
    1. Structural — Cypher pattern matching (RollUp, RollForward, etc.)
    2. Semantic — Disclosure mechanics lookup (AssetsRollUp, CashFlowStatement, etc.)

    Args:
        output_dir: Directory containing nodes/ and relationships/ parquet subdirs.

    Returns:
        (classifications_df, association_classifications_df, canonical_hints)
        The first two may be empty if no patterns are detected.
        canonical_hints maps element identifier → (canonical_concept_id, confidence)
        for elements that are disclosure roots with a known canonical mapping.
    """
    # Check that required parquet files exist
    nodes_dir = output_dir / "nodes"
    rels_dir = output_dir / "relationships"

    assoc_path = nodes_dir / "Association.parquet"
    elem_path = nodes_dir / "Element.parquet"
    from_path = rels_dir / "ASSOCIATION_HAS_FROM_ELEMENT.parquet"
    to_path = rels_dir / "ASSOCIATION_HAS_TO_ELEMENT.parquet"
    struct_path = nodes_dir / "Structure.parquet"
    struct_assoc_path = rels_dir / "STRUCTURE_HAS_ASSOCIATION.parquet"

    if not assoc_path.exists() or not elem_path.exists():
      logger.debug("Association or Element parquet not found, skipping classification")
      return pd.DataFrame(), pd.DataFrame(), {}

    classifications: list[dict] = []
    relationships: list[dict] = []

    with TempLadybugContext() as ctx:
      # Load parquet data
      ctx.load_parquet("Association", assoc_path)
      ctx.load_parquet("Element", elem_path)
      ctx.load_parquet("ASSOCIATION_HAS_FROM_ELEMENT", from_path)
      ctx.load_parquet("ASSOCIATION_HAS_TO_ELEMENT", to_path)
      ctx.load_parquet("Structure", struct_path)
      ctx.load_parquet("STRUCTURE_HAS_ASSOCIATION", struct_assoc_path)

      # Layer 1: Structural classification
      for classification_type, cypher in CLASSIFICATION_QUERIES.items():
        try:
          results = ctx.execute(cypher)
        except Exception as e:
          logger.warning(f"Classification query failed for {classification_type}: {e}")
          continue

        for row in results:
          assoc_id = row.get("association_id")
          if not assoc_id:
            continue

          class_id = generate_uuid7()
          classifications.append(
            {
              "identifier": class_id,
              "type": classification_type,
              "source": "arcrole_analysis",
              "confidence": 1.0,
            }
          )
          relationships.append(
            {
              "from": assoc_id,
              "to": class_id,
            }
          )

      # Layer 2: Semantic classification (disclosure mechanics)
      semantic_classes, semantic_rels, canonical_hints = self._classify_semantic(ctx)
      classifications.extend(semantic_classes)
      relationships.extend(semantic_rels)

    classifications_df = (
      pd.DataFrame(classifications) if classifications else pd.DataFrame()
    )
    relationships_df = pd.DataFrame(relationships) if relationships else pd.DataFrame()

    if not classifications_df.empty:
      logger.info(
        f"Association classification: {len(classifications_df)} classifications "
        f"({', '.join(f'{t}: {c}' for t, c in classifications_df['type'].value_counts().items())})"
      )

    if canonical_hints:
      logger.info(
        f"Canonical hints from disclosure roots: {len(canonical_hints)} elements"
      )

    return classifications_df, relationships_df, canonical_hints

  def _classify_semantic(
    self, ctx: TempLadybugContext
  ) -> tuple[list[dict], list[dict], dict[str, tuple[str, float]]]:
    """Identify disclosure mechanics from calculation root elements.

    For each structure, finds the root calculation association, extracts the
    element's local name, and looks it up in DISCLOSURE_CONCEPT_MAP. If found,
    creates a Classification node attached to ALL associations in that structure.

    Also builds canonical_hints: when a disclosure root maps to a known canonical
    concept via DISCLOSURE_TO_CANONICAL, the root element gets a high-confidence
    canonical concept hint.
    """
    classifications: list[dict] = []
    relationships: list[dict] = []
    canonical_hints: dict[str, tuple[str, float]] = {}

    # Find calculation root elements per structure (include element identifier)
    try:
      roots = ctx.execute(
        """
        MATCH (s:Structure)-[:STRUCTURE_HAS_ASSOCIATION]->(a:Association)-[:ASSOCIATION_HAS_FROM_ELEMENT]->(root:Element)
        WHERE a.association_type = 'Calculation' AND a.root = 'True'
        RETURN DISTINCT s.identifier AS structure_id, root.qname AS root_qname, root.identifier AS root_id
        """
      )
    except Exception as e:
      logger.debug(f"Semantic classification query failed: {e}")
      return classifications, relationships, canonical_hints

    for row in roots:
      structure_id = row.get("structure_id")
      root_qname = row.get("root_qname")
      root_id = row.get("root_id")
      if not structure_id or not root_qname:
        continue

      # Extract local name from qname (e.g., "us-gaap:Assets" → "Assets")
      local_name = root_qname.split(":")[-1] if ":" in root_qname else root_qname
      disclosure_name = DISCLOSURE_CONCEPT_MAP.get(local_name)
      if not disclosure_name:
        continue

      # Check if this disclosure root maps to a canonical concept
      canonical_id = DISCLOSURE_TO_CANONICAL.get(disclosure_name)
      if canonical_id and root_id and root_id not in canonical_hints:
        canonical_hints[root_id] = (canonical_id, 0.97)

      # Get all associations in this structure
      try:
        assocs = ctx.execute(
          """
          MATCH (s:Structure)-[:STRUCTURE_HAS_ASSOCIATION]->(a:Association)
          WHERE s.identifier = $structure_id
          RETURN a.identifier AS association_id
          """,
          {"structure_id": structure_id},
        )
      except Exception as e:
        logger.debug(f"Failed to get associations for structure {structure_id}: {e}")
        continue

      # Create one Classification node per structure, linked to all its associations
      class_id = generate_uuid7()
      classifications.append(
        {
          "identifier": class_id,
          "type": disclosure_name,
          "source": "disclosure_mechanics",
          "confidence": 1.0,
        }
      )
      for assoc_row in assocs:
        assoc_id = assoc_row.get("association_id")
        if assoc_id:
          relationships.append({"from": assoc_id, "to": class_id})

    if classifications:
      logger.debug(
        f"Semantic classification: {len(classifications)} disclosure patterns identified"
      )

    return classifications, relationships, canonical_hints
