#!/usr/bin/env python3
"""
Test Financial Statement Views

Tests income statement and balance sheet views using US-GAAP mapped elements
from the subgraph workspace.

This demonstrates:
1. Querying mapped elements in the subgraph
2. Aggregating Chart of Accounts through US-GAAP taxonomy
3. Generating proper financial statement presentations
4. Validating account balance relationships

Usage:
    uv run 07_test_views.py
"""

import asyncio
import json
import sys
from pathlib import Path

from robosystems_client.extensions import (
    RoboSystemsExtensions,
    RoboSystemsExtensionConfig,
)
from robosystems.utils.query_output import (
    print_error,
    print_info_section,
    print_success,
    print_warning,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from examples.credentials.utils import get_graph_id

CREDENTIALS_FILE = Path(__file__).resolve().parents[1] / "credentials" / "config.json"
DEMO_NAME = "element_mapping_demo"


def format_currency(amount: float) -> str:
    """Format amount as currency with proper sign"""
    return f"${abs(amount):,.2f}"


async def test_income_statement(extensions, workspace_id: str):
    """Generate and display income statement using US-GAAP mappings"""
    print_info_section("Income Statement (US-GAAP Presentation)")

    query = """
    MATCH (usGaap:Element)--(a:Association)--(coaElement:Element)
    WHERE usGaap.uri CONTAINS 'us-gaap'
      AND usGaap.name IN ['RevenueFromContractWithCustomer', 'OperatingExpenses']
    MATCH (li:LineItem)-[:LINE_ITEM_RELATES_TO_ELEMENT]->(coaElement)
    WITH
        usGaap.name as us_gaap_element,
        CASE
            WHEN usGaap.name = 'RevenueFromContractWithCustomer'
            THEN sum(li.credit_amount) - sum(li.debit_amount)
            WHEN usGaap.name = 'OperatingExpenses'
            THEN sum(li.debit_amount) - sum(li.credit_amount)
        END as amount
    WHERE amount <> 0
    RETURN
        us_gaap_element,
        amount
    ORDER BY us_gaap_element
    """

    result = extensions.query.query(workspace_id, query)

    if not result or not result.data:
        print_warning("No income statement data found")
        return None

    revenue = 0.0
    expenses = 0.0

    for row in result.data:
        element = row["us_gaap_element"]
        amount = float(row["amount"])

        if element == "RevenueFromContractWithCustomer":
            revenue = amount
            print(f"Revenue from Contract with Customer:  {format_currency(revenue)}")
        elif element == "OperatingExpenses":
            expenses = amount
            print(f"Operating Expenses:                   {format_currency(expenses)}")

    net_income = revenue - expenses
    print(f"                                      {'─' * 15}")
    print(f"Net Income:                           {format_currency(net_income)}")

    print_success(f"\n✓ Income statement aggregated from {len(result.data)} US-GAAP elements")
    return net_income


async def test_balance_sheet(extensions, workspace_id: str):
    """Generate and display balance sheet using US-GAAP mappings"""
    print_info_section("Balance Sheet (US-GAAP Presentation)")

    query = """
    MATCH (usGaap:Element)--(a:Association)--(coaElement:Element)
    WHERE usGaap.uri CONTAINS 'us-gaap'
      AND usGaap.name = 'CashAndCashEquivalents'
    MATCH (li:LineItem)-[:LINE_ITEM_RELATES_TO_ELEMENT]->(coaElement)
    WITH
        usGaap.name as us_gaap_element,
        sum(li.debit_amount) - sum(li.credit_amount) as ending_balance
    WHERE ending_balance <> 0
    RETURN
        us_gaap_element,
        ending_balance
    """

    result = extensions.query.query(workspace_id, query)

    if not result or not result.data:
        print_warning("No balance sheet data found")
        return None

    print("Assets:")
    total_assets = 0.0

    for row in result.data:
        element = row["us_gaap_element"]
        balance = float(row["ending_balance"])
        total_assets += balance
        print(f"  Cash and Cash Equivalents:           {format_currency(balance)}")

    print(f"                                      {'─' * 15}")
    print(f"Total Assets:                         {format_currency(total_assets)}")

    print_success(f"\n✓ Balance sheet aggregated from {len(result.data)} US-GAAP elements")
    return total_assets


async def verify_mapping_counts(extensions, workspace_id: str):
    """Verify the mapping structure and association counts"""
    print_info_section("Mapping Verification")

    query = """
    MATCH (s:Structure)
    OPTIONAL MATCH (s)--(a:Association)
    WITH s, count(a) as association_count
    RETURN s.name as structure_name, s.identifier as structure_id, association_count
    """

    result = extensions.query.query(workspace_id, query)

    if not result or not result.data:
        print_warning("No mapping structures found")
        return

    for row in result.data:
        print(f"Structure: {row['structure_name']}")
        print(f"  ID: {row['structure_id']}")
        print(f"  Associations: {row['association_count']}")

    print_success(f"\n✓ Found {len(result.data)} mapping structure(s)")


async def verify_chart_of_accounts_detail(extensions, workspace_id: str):
    """Show detailed breakdown of which CoA accounts map to each US-GAAP element"""
    print_info_section("Chart of Accounts to US-GAAP Mapping Detail")

    query = """
    MATCH (usGaap:Element)--(a:Association)--(coaElement:Element)
    WHERE usGaap.uri CONTAINS 'us-gaap'
    RETURN
        usGaap.name as us_gaap_element,
        coaElement.name as coa_account,
        coaElement.classification as classification
    ORDER BY us_gaap_element, coa_account
    """

    result = extensions.query.query(workspace_id, query)

    if not result or not result.data:
        print_warning("No mapping detail found")
        return

    current_element = None
    count = 0

    for row in result.data:
        us_gaap = row["us_gaap_element"]
        coa = row["coa_account"]
        classification = row["classification"]

        if us_gaap != current_element:
            if current_element:
                print()
            print(f"\n{us_gaap}:")
            current_element = us_gaap
            count = 0

        count += 1
        print(f"  {count}. {coa} ({classification})")

    print_success(f"\n✓ Total mappings: {len(result.data)} Chart of Accounts → US-GAAP associations")


async def main():
    print_info_section("Element Mapping Demo - Step 7: Test Financial Statement Views")

    if not CREDENTIALS_FILE.exists():
        print_error(f"Credentials file not found: {CREDENTIALS_FILE}")
        print("Run 'just demo-user' to create credentials")
        sys.exit(1)

    with open(CREDENTIALS_FILE) as f:
        config_data = json.load(f)

    graph_id = get_graph_id(CREDENTIALS_FILE, DEMO_NAME)
    if not graph_id:
        print_error("No graph_id found for element_mapping_demo")
        print("Run the full demo first: uv run main.py")
        sys.exit(1)

    data_file = Path(__file__).parent / "data" / "workspace_mapping_ids.json"
    if not data_file.exists():
        print_error(f"Workspace IDs file not found: {data_file}")
        print("Run 06_create_subgraph.py first to create workspace and mappings")
        sys.exit(1)

    with open(data_file) as f:
        ids_data = json.load(f)

    workspace_id = ids_data["workspace_id"]
    print(f"Main graph: {graph_id}")
    print(f"Workspace: {workspace_id}\n")

    extensions = RoboSystemsExtensions(
        RoboSystemsExtensionConfig(
            base_url=config_data["base_url"],
            headers={"X-API-Key": config_data["api_key"]},
        )
    )

    # Step 1: Verify mapping structure
    await verify_mapping_counts(extensions, workspace_id)

    # Step 2: Show mapping detail
    await verify_chart_of_accounts_detail(extensions, workspace_id)

    # Step 3: Generate income statement
    net_income = await test_income_statement(extensions, workspace_id)

    # Step 4: Generate balance sheet
    total_assets = await test_balance_sheet(extensions, workspace_id)

    # Step 5: Validate financial relationships
    if net_income is not None and total_assets is not None:
        print_info_section("Financial Statement Validation")

        print(f"Net Income:    {format_currency(net_income)}")
        print(f"Total Assets:  {format_currency(total_assets)}")

        if abs(net_income - total_assets) < 0.01:
            print_success("\n✅ Validation passed: Net Income = Total Assets")
            print("  (As expected for a simple cash-basis scenario)")
        else:
            diff = abs(net_income - total_assets)
            print_warning(f"\n⚠ Difference: {format_currency(diff)}")

    print_success("\n✅ Financial statement view testing complete!")
    print("\n💡 Key Takeaways:")
    print(f"  • Granular Chart of Accounts (10 accounts) aggregated into US-GAAP elements")
    print(f"  • Income Statement shows Revenue and Expenses from mapped elements")
    print(f"  • Balance Sheet shows Asset positions from mapped elements")
    print(f"  • All aggregation handled through Element Mapping associations")
    print(f"  • Proper debit/credit treatment for each account classification")
    print(f"\nWorkspace preserved for further testing: {workspace_id}")


if __name__ == "__main__":
    asyncio.run(main())