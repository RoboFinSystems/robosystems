#!/usr/bin/env python3
"""
Generate Accounting Demo Data (Parquet Files)

This script generates comprehensive accounting data in Parquet format,
ready for upload to RoboSystems.

Generates:
NODE FILES:
- Entity.parquet (business entity)
- Element.parquet (chart of accounts)
- Transaction.parquet (financial transactions)
- LineItem.parquet (journal entry lines)
- Report.parquet (monthly financial reports)
- Fact.parquet (aggregated financial metrics)
- Period.parquet (time periods)
- Unit.parquet (units of measurement)

RELATIONSHIP FILES:
- ENTITY_HAS_TRANSACTION.parquet
- TRANSACTION_HAS_LINE_ITEM.parquet
- LINE_ITEM_RELATES_TO_ELEMENT.parquet
- ENTITY_HAS_REPORT.parquet
- REPORT_HAS_FACT.parquet
- FACT_HAS_ELEMENT.parquet
- FACT_HAS_PERIOD.parquet
- FACT_HAS_UNIT.parquet
- FACT_HAS_ENTITY.parquet

Usage:
    uv run 03_generate_data.py                    # Generate 6 months of data
    uv run 03_generate_data.py --months 12        # Generate 12 months
    uv run 03_generate_data.py --regenerate       # Force regenerate existing files
"""

import argparse
import random
import sys
import uuid
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

import json
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
  sys.path.insert(0, str(PROJECT_ROOT))

from examples.credentials.utils import get_graph_id

DATA_DIR = Path(__file__).parent / "data"
NODES_DIR = DATA_DIR / "nodes"
RELATIONSHIPS_DIR = DATA_DIR / "relationships"
CREDENTIALS_FILE = Path(__file__).resolve().parents[1] / "credentials" / "config.json"
DEMO_NAME = "accounting_demo"


class AccountingDataGenerator:
  """Generate realistic accounting data."""

  def __init__(self, num_months=6):
    self.num_months = num_months
    self.data_dir = DATA_DIR
    self.nodes_dir = NODES_DIR
    self.relationships_dir = RELATIONSHIPS_DIR
    self.nodes_dir.mkdir(parents=True, exist_ok=True)
    self.relationships_dir.mkdir(parents=True, exist_ok=True)
    graph_id = get_graph_id(CREDENTIALS_FILE, DEMO_NAME)
    if not graph_id:
      raise RuntimeError(
        "Graph ID not found. Run 02_create_graph.py before generating data."
      )
    self.graph_id = graph_id
    self.entity_identifier = f"entity_{graph_id}"

  def _load_credentials(self) -> dict:
    if not CREDENTIALS_FILE.exists():
      raise RuntimeError(
        f"Credentials not found at {CREDENTIALS_FILE}. "
        "Run 01_setup_credentials.py and 02_create_graph.py first."
      )
    with CREDENTIALS_FILE.open() as f:
      return json.load(f)

  def _create_transaction(
    self, identifier, date, description, transaction_type, amount=None
  ):
    """Create a complete transaction dictionary with all required fields."""
    return {
      "identifier": identifier,
      "uri": f"https://example.com/transaction/{identifier}",
      "transaction_number": identifier,
      "amount": amount,
      "description": description,
      "date": date,
      "transaction_date": date,
      "reference_number": identifier,
      "transaction_type": transaction_type,
      "type": transaction_type,
      "number": identifier,
      "sync_hash": None,
      "currency": "USD",
      "plaid_merchant_name": None,
      "plaid_category": None,
      "plaid_pending": False,
      "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

  def _create_line_item(self, identifier, debit_amount, credit_amount, description):
    """Create a complete line item dictionary with all required fields."""
    return {
      "identifier": identifier,
      "uri": f"https://example.com/lineitem/{identifier}",
      "description": description,
      "debit_amount": debit_amount,
      "credit_amount": credit_amount,
      "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

  def generate_entity(self):
    """Generate business entity data."""
    print("\n📋 Generating Entity data...")

    entity_data = {
      "identifier": [self.entity_identifier],
      "uri": [f"https://accounting.example.com/{self.graph_id}"],
      "scheme": ["https://accounting.example.com/"],
      "cik": [None],
      "ticker": [None],
      "exchange": [None],
      "name": ["Acme Consulting LLC"],
      "legal_name": ["Acme Consulting Limited Liability Company"],
      "industry": ["Professional Services"],
      "entity_type": ["LLC"],
      "sic": ["8742"],
      "sic_description": ["Management Consulting Services"],
      "category": ["Professional Services"],
      "state_of_incorporation": [None],
      "fiscal_year_end": [None],
      "ein": [None],
      "tax_id": [None],
      "lei": [None],
      "phone": [None],
      "website": [f"https://accounting.example.com/{self.graph_id}"],
      "status": ["active"],
      "is_parent": pd.Series([True], dtype="boolean"),
      "parent_entity_id": [None],
      "created_at": [datetime.now().strftime("%Y-%m-%d %H:%M:%S")],
      "updated_at": [datetime.now().strftime("%Y-%m-%d %H:%M:%S")],
    }

    df = pd.DataFrame(entity_data)
    output_file = self.nodes_dir / "Entity.parquet"
    self._write_parquet(df, output_file, "Entity")
    return df

  def generate_chart_of_accounts(self):
    """Generate chart of accounts (Element nodes)."""
    print("\n📊 Generating Chart of Accounts (Elements)...")

    accounts = [
      ("1000", "Assets:Cash", "Asset", "debit"),
      ("1100", "Assets:AccountsReceivable", "Asset", "debit"),
      ("1200", "Assets:PrepaidExpenses", "Asset", "debit"),
      ("1500", "Assets:Equipment", "Asset", "debit"),
      ("1600", "Assets:AccumulatedDepreciation", "Asset", "credit"),
      ("2000", "Liabilities:AccountsPayable", "Liability", "credit"),
      ("2100", "Liabilities:AccruedExpenses", "Liability", "credit"),
      ("2200", "Liabilities:DeferredRevenue", "Liability", "credit"),
      ("2500", "Liabilities:LoanPayable", "Liability", "credit"),
      ("3000", "Equity:CommonStock", "Equity", "credit"),
      ("3100", "Equity:RetainedEarnings", "Equity", "credit"),
      ("4000", "Revenue:ConsultingRevenue", "Revenue", "credit"),
      ("4100", "Revenue:TrainingRevenue", "Revenue", "credit"),
      ("5000", "Expenses:Salaries", "Expense", "debit"),
      ("5100", "Expenses:Rent", "Expense", "debit"),
      ("5200", "Expenses:Utilities", "Expense", "debit"),
      ("5300", "Expenses:Insurance", "Expense", "debit"),
      ("5400", "Expenses:Marketing", "Expense", "debit"),
      ("5500", "Expenses:OfficeSupplies", "Expense", "debit"),
      ("5600", "Expenses:ProfessionalFees", "Expense", "debit"),
    ]

    account_data = []
    for account_number, account_name, account_type, normal_balance in accounts:
      account_data.append(
        {
          "identifier": f"element_{account_number}",
          "uri": f"https://example.com/taxonomy/element#{account_name}",
          "qname": account_name,
          "name": account_name.split(":")[-1],
          "period_type": "instant"
          if account_type in ["Asset", "Liability", "Equity"]
          else "duration",
          "type": "Monetary",
          "balance": normal_balance,
          "is_abstract": False,
          "is_dimension_item": False,
          "is_domain_member": False,
          "is_hypercube_item": False,
          "is_integer": False,
          "is_numeric": True,
          "is_shares": False,
          "is_fraction": False,
          "is_textblock": False,
          "substitution_group": None,
          "item_type": "monetaryItemType",
          "classification": account_type.lower(),
        }
      )

    df = pd.DataFrame(account_data)
    output_file = self.nodes_dir / "Element.parquet"
    self._write_parquet(df, output_file, "Element")
    return df

  def generate_transactions(self):
    """Generate transactions and line items."""
    print(f"\n💰 Generating {self.num_months} months of transactions...")

    transactions = []
    line_items = []
    tx_id = 1
    li_id = 1

    start_date = datetime.now() - timedelta(days=self.num_months * 30)

    account_lookup = {
      "Assets:Cash": "1000",
      "Assets:AccountsReceivable": "1100",
      "Expenses:Rent": "5100",
      "Expenses:Salaries": "5000",
      "Expenses:Utilities": "5200",
      "Expenses:OfficeSupplies": "5500",
      "Expenses:Marketing": "5400",
      "Revenue:ConsultingRevenue": "4000",
      "Revenue:TrainingRevenue": "4100",
    }

    for month in range(self.num_months):
      month_start = start_date + timedelta(days=30 * month)

      tx_date = month_start + timedelta(days=1)
      transactions.append(
        self._create_transaction(
          f"TX{tx_id:04d}",
          tx_date.strftime("%Y-%m-%d"),
          f"Monthly rent payment - {tx_date.strftime('%B %Y')}",
          "Expense",
          2500.00,
        )
      )
      line_items.extend(
        [
          {
            **self._create_line_item(f"LI{li_id:04d}", 2500.00, 0.0, "Office rent"),
            "transaction_id": f"TX{tx_id:04d}",
            "account_name": "Expenses:Rent",
            "account_element": f"element_{account_lookup['Expenses:Rent']}",
          },
          {
            **self._create_line_item(
              f"LI{li_id + 1:04d}", 0.0, 2500.00, "Cash payment"
            ),
            "transaction_id": f"TX{tx_id:04d}",
            "account_name": "Assets:Cash",
            "account_element": f"element_{account_lookup['Assets:Cash']}",
          },
        ]
      )
      li_id += 2
      tx_id += 1

      for week in range(4):
        tx_date = month_start + timedelta(days=7 * week + 5)
        revenue_amount = round(random.uniform(3000, 8000), 2)

        transactions.append(
          self._create_transaction(
            f"TX{tx_id:04d}",
            tx_date.strftime("%Y-%m-%d"),
            f"Consulting services - Week {week + 1}",
            "Revenue",
            revenue_amount,
          )
        )
        line_items.extend(
          [
            {
              **self._create_line_item(
                f"LI{li_id:04d}", revenue_amount, 0.0, "Cash received"
              ),
              "transaction_id": f"TX{tx_id:04d}",
              "account_name": "Assets:Cash",
              "account_element": f"element_{account_lookup['Assets:Cash']}",
            },
            {
              **self._create_line_item(
                f"LI{li_id + 1:04d}", 0.0, revenue_amount, "Consulting revenue earned"
              ),
              "transaction_id": f"TX{tx_id:04d}",
              "account_name": "Revenue:ConsultingRevenue",
              "account_element": f"element_{account_lookup['Revenue:ConsultingRevenue']}",
            },
          ]
        )
        li_id += 2
        tx_id += 1

      tx_date = month_start + timedelta(days=15)
      salary_amount = round(random.uniform(8000, 12000), 2)
      transactions.append(
        self._create_transaction(
          f"TX{tx_id:04d}",
          tx_date.strftime("%Y-%m-%d"),
          f"Salary payment - {tx_date.strftime('%B %Y')}",
          "Expense",
          salary_amount,
        )
      )
      line_items.extend(
        [
          {
            **self._create_line_item(
              f"LI{li_id:04d}", salary_amount, 0.0, "Employee salaries"
            ),
            "transaction_id": f"TX{tx_id:04d}",
            "account_name": "Expenses:Salaries",
            "account_element": f"element_{account_lookup['Expenses:Salaries']}",
          },
          {
            **self._create_line_item(
              f"LI{li_id + 1:04d}", 0.0, salary_amount, "Cash payment"
            ),
            "transaction_id": f"TX{tx_id:04d}",
            "account_name": "Assets:Cash",
            "account_element": f"element_{account_lookup['Assets:Cash']}",
          },
        ]
      )
      li_id += 2
      tx_id += 1

      expense_types = [
        ("Expenses:Utilities", "Utility payment", 200, 500),
        ("Expenses:OfficeSupplies", "Office supplies purchase", 100, 400),
        ("Expenses:Marketing", "Marketing campaign", 300, 1000),
      ]

      for expense_account, desc, min_amt, max_amt in expense_types:
        tx_date = month_start + timedelta(days=random.randint(10, 25))
        amount = round(random.uniform(min_amt, max_amt), 2)

        transactions.append(
          self._create_transaction(
            f"TX{tx_id:04d}", tx_date.strftime("%Y-%m-%d"), desc, "Expense", amount
          )
        )
        line_items.extend(
          [
            {
              **self._create_line_item(f"LI{li_id:04d}", amount, 0.0, desc),
              "transaction_id": f"TX{tx_id:04d}",
              "account_name": expense_account,
              "account_element": f"element_{account_lookup[expense_account]}",
            },
            {
              **self._create_line_item(
                f"LI{li_id + 1:04d}", 0.0, amount, "Cash payment"
              ),
              "transaction_id": f"TX{tx_id:04d}",
              "account_name": "Assets:Cash",
              "account_element": f"element_{account_lookup['Assets:Cash']}",
            },
          ]
        )
        li_id += 2
        tx_id += 1

    tx_df = pd.DataFrame(transactions)
    li_df = pd.DataFrame(line_items)

    tx_output = self.nodes_dir / "Transaction.parquet"
    self._write_parquet(tx_df, tx_output, "Transaction")

    li_output = self.nodes_dir / "LineItem.parquet"
    li_df_clean = li_df.drop(
      columns=["transaction_id", "account_name", "account_element"]
    )
    self._write_parquet(li_df_clean, li_output, "LineItem")

    return tx_df, li_df

  def generate_reports_and_facts(self, tx_df, li_df):
    """Generate monthly financial reports with aggregated facts."""
    print("\n📊 Generating monthly financial reports...")

    start_date = datetime.now() - timedelta(days=self.num_months * 30)
    entity_id = self.entity_identifier

    reports = []
    facts = []
    periods = []
    units = []

    usd_unit_id = str(uuid.uuid4())
    units.append(
      {
        "identifier": usd_unit_id,
        "uri": "http://www.xbrl.org/2003/iso4217#USD",
        "measure": "iso4217:USD",
        "value": "USD",
        "numerator_uri": None,
        "denominator_uri": None,
      }
    )

    for month_idx in range(self.num_months):
      month_start = start_date + timedelta(days=30 * month_idx)
      month_end = month_start + timedelta(days=29)
      month_str = month_start.strftime("%Y-%m")

      report_id = str(uuid.uuid4())
      period_id = str(uuid.uuid4())

      reports.append(
        {
          "identifier": report_id,
          "uri": f"https://example.com/report/{month_str}",
          "name": f"Monthly Financial Report - {month_start.strftime('%B %Y')}",
          "accession_number": f"ACME-{month_str.replace('-', '')}",
          "form": "MONTHLY",
          "filing_date": month_end.strftime("%Y-%m-%d"),
          "report_date": month_end.strftime("%Y-%m-%d"),
          "acceptance_date": month_end.strftime("%Y-%m-%d"),
          "period_end_date": month_end.strftime("%Y-%m-%d"),
          "is_inline_xbrl": False,
          "xbrl_processor_version": "1.0.0",
          "processed": True,
          "failed": False,
          "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
      )

      periods.append(
        {
          "identifier": period_id,
          "uri": f"http://www.w3.org/2001/XMLSchema#dateTime#{month_start.strftime('%Y-%m-%d')}/{month_end.strftime('%Y-%m-%d')}",
          "instant_date": None,
          "start_date": month_start.strftime("%Y-%m-%d"),
          "end_date": month_end.strftime("%Y-%m-%d"),
          "forever_date": False,
          "fiscal_year": month_start.year,
          "fiscal_quarter": f"Q{((month_start.month - 1) // 3) + 1}",
          "is_annual": False,
          "is_quarterly": False,
          "days_in_period": 30,
          "period_type": "monthly",
          "is_ytd": False,
        }
      )

      month_txs = tx_df[tx_df["date"].str.startswith(month_str)]
      month_lis = li_df[li_df["transaction_id"].isin(month_txs["identifier"])]

      element_totals = defaultdict(
        lambda: {"debits": 0.0, "credits": 0.0, "element_info": {}}
      )

      for _, li in month_lis.iterrows():
        account = li["account_element"]
        account_name = li["account_name"]
        element_totals[account]["debits"] += li["debit_amount"]
        element_totals[account]["credits"] += li["credit_amount"]
        element_totals[account]["element_info"]["name"] = account_name

      for element_id, totals in element_totals.items():
        net_amount = totals["debits"] - totals["credits"]

        if totals["debits"] > 0:
          fact_id = str(uuid.uuid4())
          facts.append(
            {
              "identifier": fact_id,
              "uri": f"https://example.com/fact/{fact_id}",
              "value": str(totals["debits"]),
              "numeric_value": totals["debits"],
              "fact_type": "Numeric",
              "decimals": "2",
              "value_type": "inline",
              "content_type": None,
              "report_id": report_id,
              "element_id": element_id,
              "period_id": period_id,
              "unit_id": usd_unit_id,
              "entity_id": entity_id,
              "fact_name": f"{totals['element_info']['name']}_Debits",
            }
          )

        if totals["credits"] > 0:
          fact_id = str(uuid.uuid4())
          facts.append(
            {
              "identifier": fact_id,
              "uri": f"https://example.com/fact/{fact_id}",
              "value": str(totals["credits"]),
              "numeric_value": totals["credits"],
              "fact_type": "Numeric",
              "decimals": "2",
              "value_type": "inline",
              "content_type": None,
              "report_id": report_id,
              "element_id": element_id,
              "period_id": period_id,
              "unit_id": usd_unit_id,
              "entity_id": entity_id,
              "fact_name": f"{totals['element_info']['name']}_Credits",
            }
          )

        if abs(net_amount) > 0.01:
          fact_id = str(uuid.uuid4())
          facts.append(
            {
              "identifier": fact_id,
              "uri": f"https://example.com/fact/{fact_id}",
              "value": str(abs(net_amount)),
              "numeric_value": abs(net_amount),
              "fact_type": "Numeric",
              "decimals": "2",
              "value_type": "inline",
              "content_type": None,
              "report_id": report_id,
              "element_id": element_id,
              "period_id": period_id,
              "unit_id": usd_unit_id,
              "entity_id": entity_id,
              "fact_name": f"{totals['element_info']['name']}_NetBalance",
            }
          )

    reports_df = pd.DataFrame(reports)
    facts_df = pd.DataFrame(facts)
    periods_df = pd.DataFrame(periods)
    units_df = pd.DataFrame(units)

    self._write_parquet(reports_df, self.nodes_dir / "Report.parquet", "Report")

    facts_df_clean = facts_df.drop(
      columns=[
        "report_id",
        "element_id",
        "period_id",
        "unit_id",
        "entity_id",
        "fact_name",
      ]
    )
    self._write_parquet(facts_df_clean, self.nodes_dir / "Fact.parquet", "Fact")

    self._write_parquet(periods_df, self.nodes_dir / "Period.parquet", "Period")
    self._write_parquet(units_df, self.nodes_dir / "Unit.parquet", "Unit")

    return reports_df, facts_df, periods_df, units_df

  def generate_relationships(self, tx_df, li_df, reports_df=None, facts_df=None):
    """Generate relationship parquet files."""
    print("\n🔗 Generating relationship files...")

    entity_id = self.entity_identifier

    entity_tx_rels = []
    for _, row in tx_df.iterrows():
      entity_tx_rels.append(
        {
          "from": entity_id,
          "to": row["identifier"],
          "transaction_context": "general_ledger",
        }
      )
    entity_tx_df = pd.DataFrame(entity_tx_rels)
    entity_tx_path = self.relationships_dir / "ENTITY_HAS_TRANSACTION.parquet"
    self._write_parquet(entity_tx_df, entity_tx_path, "ENTITY_HAS_TRANSACTION")

    tx_li_rels = []
    for _, row in li_df.iterrows():
      tx_li_rels.append(
        {
          "from": row["transaction_id"],
          "to": row["identifier"],
          "line_item_context": "accounting_transaction",
        }
      )
    tx_li_df = pd.DataFrame(tx_li_rels)
    tx_li_path = self.relationships_dir / "TRANSACTION_HAS_LINE_ITEM.parquet"
    self._write_parquet(tx_li_df, tx_li_path, "TRANSACTION_HAS_LINE_ITEM")

    li_element_rels = []
    for _, row in li_df.iterrows():
      li_element_rels.append(
        {
          "from": row["identifier"],
          "to": row["account_element"],
          "mapping_context": "chart_of_accounts",
        }
      )
    li_element_df = pd.DataFrame(li_element_rels)
    li_element_path = self.relationships_dir / "LINE_ITEM_RELATES_TO_ELEMENT.parquet"
    self._write_parquet(li_element_df, li_element_path, "LINE_ITEM_RELATES_TO_ELEMENT")

    if reports_df is not None and facts_df is not None:
      entity_report_rels = []
      for _, row in reports_df.iterrows():
        entity_report_rels.append(
          {
            "from": entity_id,
            "to": row["identifier"],
            "filing_context": "monthly_reporting",
          }
        )
      entity_report_df = pd.DataFrame(entity_report_rels)
      entity_report_path = self.relationships_dir / "ENTITY_HAS_REPORT.parquet"
      self._write_parquet(entity_report_df, entity_report_path, "ENTITY_HAS_REPORT")

      report_fact_rels = []
      fact_element_rels = []
      fact_period_rels = []
      fact_unit_rels = []
      fact_entity_rels = []

      for _, row in facts_df.iterrows():
        report_fact_rels.append(
          {
            "from": row["report_id"],
            "to": row["identifier"],
            "fact_context": "aggregated_metric",
          }
        )
        fact_element_rels.append(
          {
            "from": row["identifier"],
            "to": row["element_id"],
          }
        )
        fact_period_rels.append(
          {
            "from": row["identifier"],
            "to": row["period_id"],
            "period_context": "monthly",
          }
        )
        fact_unit_rels.append(
          {
            "from": row["identifier"],
            "to": row["unit_id"],
            "unit_context": "currency",
          }
        )
        fact_entity_rels.append(
          {
            "from": row["identifier"],
            "to": row["entity_id"],
            "entity_context": "reporting_entity",
          }
        )

      report_fact_df = pd.DataFrame(report_fact_rels)
      report_fact_path = self.relationships_dir / "REPORT_HAS_FACT.parquet"
      self._write_parquet(report_fact_df, report_fact_path, "REPORT_HAS_FACT")

      fact_element_df = pd.DataFrame(fact_element_rels)
      fact_element_path = self.relationships_dir / "FACT_HAS_ELEMENT.parquet"
      self._write_parquet(fact_element_df, fact_element_path, "FACT_HAS_ELEMENT")

      fact_period_df = pd.DataFrame(fact_period_rels)
      fact_period_path = self.relationships_dir / "FACT_HAS_PERIOD.parquet"
      self._write_parquet(fact_period_df, fact_period_path, "FACT_HAS_PERIOD")

      fact_unit_df = pd.DataFrame(fact_unit_rels)
      fact_unit_path = self.relationships_dir / "FACT_HAS_UNIT.parquet"
      self._write_parquet(fact_unit_df, fact_unit_path, "FACT_HAS_UNIT")

      fact_entity_df = pd.DataFrame(fact_entity_rels)
      fact_entity_path = self.relationships_dir / "FACT_HAS_ENTITY.parquet"
      self._write_parquet(fact_entity_df, fact_entity_path, "FACT_HAS_ENTITY")

  def _write_parquet(self, df, output_file, table_name):
    """Write DataFrame to Parquet file."""
    table = pa.Table.from_pandas(df)
    pq.write_table(table, output_file)
    file_size = output_file.stat().st_size
    print(f"✅ {table_name}: {len(df)} rows, {file_size:,} bytes → {output_file.name}")

  def generate_all(self, regenerate=False):
    """Generate all data files."""
    if not regenerate and self._files_exist():
      print("\n⚠️  Data files already exist!")
      print("   Use --regenerate to force regenerate")
      print(f"   Data directory: {self.data_dir}")
      return

    print("\n" + "=" * 70)
    print("📊 Accounting Demo - Data Generation")
    print("=" * 70)
    print(f"Generating {self.num_months} months of accounting data...")

    self.generate_entity()
    self.generate_chart_of_accounts()
    tx_df, li_df = self.generate_transactions()
    reports_df, facts_df, periods_df, units_df = self.generate_reports_and_facts(
      tx_df, li_df
    )
    self.generate_relationships(tx_df, li_df, reports_df, facts_df)

    print("\n" + "=" * 70)
    print("✅ Data Generation Complete!")
    print("=" * 70)
    print(f"\nData files saved to: {self.data_dir}")
    print(f"  - {len(tx_df)} transactions")
    print(f"  - {len(li_df)} line items")
    print(f"  - {len(reports_df)} monthly reports")
    print(f"  - {len(facts_df)} aggregated facts")
    print("\n💡 Next step: uv run 04_upload_ingest.py")
    print("=" * 70 + "\n")

  def _files_exist(self):
    """Check if data files already exist."""
    node_files = [
      "Entity.parquet",
      "Element.parquet",
      "Transaction.parquet",
      "LineItem.parquet",
      "Report.parquet",
      "Fact.parquet",
      "Period.parquet",
      "Unit.parquet",
    ]
    relationship_files = [
      "ENTITY_HAS_TRANSACTION.parquet",
      "TRANSACTION_HAS_LINE_ITEM.parquet",
      "LINE_ITEM_RELATES_TO_ELEMENT.parquet",
      "ENTITY_HAS_REPORT.parquet",
      "REPORT_HAS_FACT.parquet",
      "FACT_HAS_ELEMENT.parquet",
      "FACT_HAS_PERIOD.parquet",
      "FACT_HAS_UNIT.parquet",
      "FACT_HAS_ENTITY.parquet",
    ]
    return all((self.nodes_dir / f).exists() for f in node_files) and all(
      (self.relationships_dir / f).exists() for f in relationship_files
    )


def main():
  parser = argparse.ArgumentParser(description="Generate accounting demo data")
  parser.add_argument(
    "--months",
    type=int,
    default=6,
    help="Number of months of data to generate (default: 6)",
  )
  parser.add_argument(
    "--regenerate",
    action="store_true",
    help="Force regenerate existing data files",
  )

  args = parser.parse_args()

  try:
    generator = AccountingDataGenerator(num_months=args.months)
    generator.generate_all(regenerate=args.regenerate)
  except Exception as e:
    print(f"\n❌ Error: {e}")
    import traceback

    traceback.print_exc()
    sys.exit(1)


if __name__ == "__main__":
  main()
