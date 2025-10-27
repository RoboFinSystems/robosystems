#!/usr/bin/env python3
"""
Query Accounting Graph

Interactive script to query the accounting demo graph.

Usage:
    uv run 05_query_graph.py                        # Interactive mode
    uv run 05_query_graph.py --query "MATCH (n) RETURN count(n)"
    uv run 05_query_graph.py --preset trial_balance  # Run preset query
    uv run 05_query_graph.py --all                   # Run all presets
"""

import argparse
import json
import sys
from pathlib import Path

from robosystems_client.extensions import (
    RoboSystemsExtensions,
    RoboSystemsExtensionConfig,
)


CREDENTIALS_FILE = Path(__file__).parent / "credentials" / "config.json"


PRESET_QUERIES = {
    "counts": {
        "description": "Count all nodes by type",
        "query": """
MATCH (n)
RETURN labels(n)[0] AS type, count(n) AS count
ORDER BY count DESC
        """,
    },
    "chart_of_accounts": {
        "description": "View chart of accounts",
        "query": """
MATCH (e:Element)
WHERE e.classification IS NOT NULL
RETURN e.name AS account, e.classification AS type, e.balance AS normal_balance
ORDER BY e.name
LIMIT 20
        """,
    },
    "trial_balance": {
        "description": "Calculate trial balance",
        "query": """
MATCH (li:LineItem)-[:LINE_ITEM_RELATES_TO_ELEMENT]->(e:Element)
WITH
    e.name AS account,
    e.classification AS type,
    sum(li.debit_amount) AS total_debits,
    sum(li.credit_amount) AS total_credits
RETURN
    account,
    type,
    total_debits,
    total_credits,
    total_debits - total_credits AS net_balance
ORDER BY account
        """,
    },
    "income_statement": {
        "description": "Income statement (Revenue & Expenses)",
        "query": """
MATCH (li:LineItem)-[:LINE_ITEM_RELATES_TO_ELEMENT]->(e:Element)
WHERE e.classification IN ['revenue', 'expense']
WITH
    e.classification AS category,
    e.name AS account,
    sum(li.credit_amount) - sum(li.debit_amount) AS amount
RETURN category, account, amount
ORDER BY category, account
        """,
    },
    "cash_flow": {
        "description": "Cash flow transactions (most recent)",
        "query": """
MATCH (t:Transaction)-[:TRANSACTION_HAS_LINE_ITEM]->(li:LineItem)-[:LINE_ITEM_RELATES_TO_ELEMENT]->(e:Element)
WHERE e.name = 'Cash'
RETURN
    t.date AS date,
    t.description AS description,
    li.debit_amount AS cash_in,
    li.credit_amount AS cash_out
ORDER BY t.date DESC
LIMIT 20
        """,
    },
    "revenue_by_month": {
        "description": "Revenue trends by month",
        "query": """
MATCH (t:Transaction)-[:TRANSACTION_HAS_LINE_ITEM]->(li:LineItem)-[:LINE_ITEM_RELATES_TO_ELEMENT]->(e:Element)
WHERE e.classification = 'revenue'
RETURN
    substring(t.date, 1, 7) AS month,
    sum(li.credit_amount) AS total_revenue
ORDER BY month
        """,
    },
    "expense_by_month": {
        "description": "Expense trends by month",
        "query": """
MATCH (t:Transaction)-[:TRANSACTION_HAS_LINE_ITEM]->(li:LineItem)-[:LINE_ITEM_RELATES_TO_ELEMENT]->(e:Element)
WHERE e.classification = 'expense'
RETURN
    substring(t.date, 1, 7) AS month,
    sum(li.debit_amount) AS total_expenses
ORDER BY month
        """,
    },
    "profitability": {
        "description": "Profitability by month (Revenue - Expenses)",
        "query": """
MATCH (t:Transaction)-[:TRANSACTION_HAS_LINE_ITEM]->(li:LineItem)-[:LINE_ITEM_RELATES_TO_ELEMENT]->(e:Element)
WHERE e.classification IN ['revenue', 'expense']
WITH
    substring(t.date, 1, 7) AS month,
    e.classification AS type,
    li.credit_amount AS credit,
    li.debit_amount AS debit
WITH
    month,
    sum(CASE WHEN type = 'revenue' THEN credit ELSE 0 END) AS revenue,
    sum(CASE WHEN type = 'expense' THEN debit ELSE 0 END) AS expenses
RETURN
    month,
    revenue,
    expenses,
    revenue - expenses AS profit
ORDER BY month
        """,
    },
    "top_expenses": {
        "description": "Top expense categories",
        "query": """
MATCH (li:LineItem)-[:LINE_ITEM_RELATES_TO_ELEMENT]->(e:Element)
WHERE e.classification = 'expense'
WITH
    e.name AS expense_account,
    sum(li.debit_amount) AS total_amount,
    count(li) AS transaction_count
RETURN expense_account, total_amount, transaction_count
ORDER BY total_amount DESC
LIMIT 10
        """,
    },
    "recent_transactions": {
        "description": "Most recent transactions",
        "query": """
MATCH (t:Transaction)
RETURN t.date, t.description, t.type
ORDER BY t.date DESC
LIMIT 15
        """,
    },
    "monthly_reports": {
        "description": "List all monthly financial reports",
        "query": """
MATCH (e:Entity)-[:ENTITY_HAS_REPORT]->(r:Report)
RETURN r.name AS report_name, r.form AS form, r.report_date AS date, r.accession_number AS accession
ORDER BY r.report_date
        """,
    },
    "report_summary": {
        "description": "Summary of facts per monthly report",
        "query": """
MATCH (r:Report)-[:REPORT_HAS_FACT]->(f:Fact)-[:FACT_HAS_PERIOD]->(p:Period)
WITH r.name AS report, p.start_date AS period_start, p.end_date AS period_end, count(f) AS total_facts
RETURN report, period_start, period_end, total_facts
ORDER BY period_start
        """,
    },
    "account_facts": {
        "description": "Aggregated facts by account (from reports)",
        "query": """
MATCH (r:Report)-[:REPORT_HAS_FACT]->(f:Fact)-[:FACT_HAS_ELEMENT]->(e:Element)
WHERE r.name CONTAINS 'September'
WITH e.name AS account, count(f) AS facts, sum(f.numeric_value) AS total_amount
RETURN account, facts, total_amount
ORDER BY total_amount DESC
LIMIT 10
        """,
    },
    "report_lineage": {
        "description": "Data lineage: Transactions to Reports",
        "query": """
MATCH (t:Transaction)-[:TRANSACTION_HAS_LINE_ITEM]->(li:LineItem)-[:LINE_ITEM_RELATES_TO_ELEMENT]->(e:Element)
MATCH (r:Report)-[:REPORT_HAS_FACT]->(f:Fact)-[:FACT_HAS_ELEMENT]->(e)
WHERE substring(t.date, 1, 7) = '2025-09'
WITH e.name AS account, count(DISTINCT t) AS transactions, count(DISTINCT li) AS line_items, count(DISTINCT f) AS facts
RETURN account, transactions, line_items, facts
ORDER BY transactions DESC
LIMIT 10
        """,
    },
    "full_reporting_structure": {
        "description": "Complete reporting hierarchy",
        "query": """
MATCH (e:Entity)-[:ENTITY_HAS_REPORT]->(r:Report)-[:REPORT_HAS_FACT]->(f:Fact)
MATCH (f)-[:FACT_HAS_PERIOD]->(p:Period)
MATCH (f)-[:FACT_HAS_UNIT]->(u:Unit)
WITH e.name AS company, r.name AS report, p.start_date AS period_start, u.value AS unit, count(f) AS total_facts
RETURN company, report, period_start, unit, total_facts
ORDER BY period_start
LIMIT 10
        """,
    },
}


def load_config():
    """Load credentials and graph_id."""
    if not CREDENTIALS_FILE.exists():
        print(f"\n❌ No credentials found at {CREDENTIALS_FILE}")
        print("   Run: uv run 01_setup_credentials.py first")
        sys.exit(1)

    with open(CREDENTIALS_FILE) as f:
        return json.load(f)


def run_query(
    extensions: RoboSystemsExtensions, graph_id: str, query: str, description: str = None
):
    """Execute a query and display results."""
    if description:
        print(f"\n{'=' * 70}")
        print(f"📊 {description}")
        print("=" * 70)

    print("\nQuery:")
    print(query.strip())

    try:
        result = extensions.query.query(graph_id, query)

        if hasattr(result, "data") and result.data:
            print(f"\n✅ {len(result.data)} records:")
            for i, record in enumerate(result.data, 1):
                print(f"   {i}. {record}")
        else:
            print("\n⚠️  No results")

    except Exception as e:
        print(f"\n❌ Query failed: {e}")


def interactive_mode(extensions: RoboSystemsExtensions, graph_id: str):
    """Interactive query mode."""
    print("\n" + "=" * 70)
    print("📊 Interactive Query Mode")
    print("=" * 70)
    print("\nAvailable preset queries:")
    for key, preset in PRESET_QUERIES.items():
        print(f"   {key:20s} - {preset['description']}")

    print("\nCommands:")
    print("   <preset_name>   - Run a preset query")
    print("   all            - Run all preset queries")
    print("   quit/exit      - Exit interactive mode")
    print("\nOr enter a custom Cypher query:")

    while True:
        try:
            query_input = input("\n> ").strip()

            if not query_input:
                continue

            if query_input.lower() in ["quit", "exit", "q"]:
                print("\nGoodbye!")
                break

            if query_input.lower() == "all":
                for key, preset in PRESET_QUERIES.items():
                    run_query(extensions, graph_id, preset["query"], preset["description"])
                continue

            if query_input in PRESET_QUERIES:
                preset = PRESET_QUERIES[query_input]
                run_query(extensions, graph_id, preset["query"], preset["description"])
            else:
                run_query(extensions, graph_id, query_input)

        except KeyboardInterrupt:
            print("\n\nGoodbye!")
            break
        except EOFError:
            print("\n\nGoodbye!")
            break


def main():
    parser = argparse.ArgumentParser(description="Query accounting graph")
    parser.add_argument(
        "--base-url",
        default="http://localhost:8000",
        help="API base URL (default: http://localhost:8000)",
    )
    parser.add_argument(
        "--query",
        help="Run a custom Cypher query",
    )
    parser.add_argument(
        "--preset",
        choices=list(PRESET_QUERIES.keys()),
        help="Run a preset query",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Run all preset queries",
    )

    args = parser.parse_args()

    try:
        config_data = load_config()
        api_key = config_data.get("api_key")
        graph_id = config_data.get("graph_id")

        if not api_key or not graph_id:
            print("\n❌ Missing API key or graph_id in credentials")
            print("   Run: uv run 01_setup_credentials.py and uv run 02_create_graph.py first")
            sys.exit(1)

        print("\n" + "=" * 70)
        print("📊 Accounting Demo - Query Graph")
        print("=" * 70)
        print(f"Graph ID: {graph_id}")

        config = RoboSystemsExtensionConfig(
            base_url=args.base_url,
            headers={"X-API-Key": api_key},
        )
        extensions = RoboSystemsExtensions(config)

        if args.query:
            run_query(extensions, graph_id, args.query)
        elif args.preset:
            preset = PRESET_QUERIES[args.preset]
            run_query(extensions, graph_id, preset["query"], preset["description"])
        elif args.all:
            for key, preset in PRESET_QUERIES.items():
                run_query(extensions, graph_id, preset["query"], preset["description"])

            print("\n" + "=" * 70)
            print("💡 You can also query using the just command:")
            print("=" * 70)
            print(f'\njust graph-query {graph_id} "MATCH (n) RETURN count(n)"')
            print(f'just graph-query {graph_id} "MATCH (e:Element) RETURN e.name, e.classification ORDER BY e.name"')
            print("\n" + "=" * 70 + "\n")
        else:
            interactive_mode(extensions, graph_id)

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
