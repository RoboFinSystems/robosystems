#!/usr/bin/env python3
"""
Create Element Mappings (Updated for Subgraph Architecture with Fork)

Creates element mapping structures in a subgraph workspace using the new fork feature,
then publishes to main graph via parquet export/ingest.

This demonstrates the proper architecture where:
1. Main graph remains read-only (file-based ingestion)
2. Subgraph is created with fork_parent=True to copy data automatically
3. Mappings are created in write-enabled subgraph workspaces
4. Publishing happens via export → parquet → incremental ingest

Usage:
    uv run 06_create_subgraph.py
"""

import asyncio
import json
import sys
from pathlib import Path

from robosystems_client import AuthenticatedClient
from robosystems_client.extensions import (
    RoboSystemsExtensions,
    RoboSystemsExtensionConfig,
    ElementMappingClient,
    SubgraphWorkspaceClient,
    AggregationMethod,
    StorageType,
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


async def create_comprehensive_mapping(extensions, mapping_client, workspace_id: str, main_graph_id: str):
    print_info_section("Creating Comprehensive CoA to US-GAAP Mapping")

    # Create mapping structure in SUBGRAPH
    mapping = await mapping_client.create_mapping_structure(
        graph_id=workspace_id,  # Write to subgraph!
        name="Complete CoA to US-GAAP",
        description="Map all Chart of Accounts to US-GAAP taxonomy elements",
        taxonomy_uri="qb:chart-of-accounts",
        target_taxonomy_uri="us-gaap:2024"
    )

    mapping_id = mapping.identifier
    print_success(f"Created mapping structure: {mapping_id}\n")

    # Query elements from MAIN GRAPH
    query_cash = """
    MATCH (e:Element)
    WHERE e.classification = 'asset' AND toLower(e.name) CONTAINS 'cash'
       OR toLower(e.name) CONTAINS 'checking'
       OR toLower(e.name) CONTAINS 'savings'
    RETURN e.uri, e.name
    ORDER BY e.name
    """

    query_revenue = """
    MATCH (e:Element)
    WHERE e.classification = 'revenue'
    RETURN e.uri, e.name
    ORDER BY e.name
    """

    query_expense = """
    MATCH (e:Element)
    WHERE e.classification = 'expense'
    RETURN e.uri, e.name
    ORDER BY e.name
    """

    cash_result = extensions.query.query(main_graph_id, query_cash)
    revenue_result = extensions.query.query(main_graph_id, query_revenue)
    expense_result = extensions.query.query(main_graph_id, query_expense)

    cash_elements = cash_result.data if cash_result else []
    revenue_elements = revenue_result.data if revenue_result else []
    expense_elements = expense_result.data if expense_result else []

    total_mapped = 0

    # Create associations in SUBGRAPH
    if cash_elements:
        print(f"✓ Mapping {len(cash_elements)} cash accounts → us-gaap:CashAndCashEquivalents")
        for elem in cash_elements:
            await mapping_client.create_association(
                graph_id=workspace_id,  # Write to subgraph!
                structure_id=mapping_id,
                source_element=elem["e.uri"],
                target_element="us-gaap:CashAndCashEquivalents",
                aggregation_method=AggregationMethod.SUM,
                weight=1.0
            )
            print(f"  ✓ {elem['e.name']}")
        total_mapped += len(cash_elements)

    if revenue_elements:
        print(f"\n✓ Mapping {len(revenue_elements)} revenue accounts → us-gaap:RevenueFromContractWithCustomer")
        for elem in revenue_elements:
            await mapping_client.create_association(
                graph_id=workspace_id,
                structure_id=mapping_id,
                source_element=elem["e.uri"],
                target_element="us-gaap:RevenueFromContractWithCustomer",
                aggregation_method=AggregationMethod.SUM,
                weight=1.0
            )
            print(f"  ✓ {elem['e.name']}")
        total_mapped += len(revenue_elements)

    if expense_elements:
        print(f"\n✓ Mapping {len(expense_elements)} expense accounts → us-gaap:OperatingExpenses")
        for elem in expense_elements:
            await mapping_client.create_association(
                graph_id=workspace_id,
                structure_id=mapping_id,
                source_element=elem["e.uri"],
                target_element="us-gaap:OperatingExpenses",
                aggregation_method=AggregationMethod.SUM,
                weight=1.0
            )
            print(f"  ✓ {elem['e.name']}")
        total_mapped += len(expense_elements)

    print_success(f"\n✓ Comprehensive mapping complete: {total_mapped} associations created")
    print(f"  {len(cash_elements)} cash → 1 US-GAAP element")
    print(f"  {len(revenue_elements)} revenue → 1 US-GAAP element")
    print(f"  {len(expense_elements)} expense → 1 US-GAAP element")
    print(f"  Total: {total_mapped} accounts → 3 US-GAAP elements\n")

    return mapping_id


async def list_all_mappings(mapping_client, workspace_id: str):
    print_info_section("All Element Mappings")

    mappings = await mapping_client.list_mapping_structures(workspace_id)

    if not mappings:
        print_warning("No mappings found")
        return

    for mapping in mappings:
        print(f"\n📋 {mapping.name}")
        print(f"   ID: {mapping.identifier}")
        print(f"   Description: {mapping.description}")
        print(f"   Source: {mapping.taxonomy_uri}")
        print(f"   Target: {mapping.target_taxonomy_uri}")

    print_success(f"\nTotal mappings: {len(mappings)}")


def save_workspace_and_mapping_ids(workspace_id: str, mapping_id: str):
    """Save IDs for use in other scripts"""
    data_file = Path(__file__).parent / "data" / "workspace_mapping_ids.json"
    data_file.parent.mkdir(exist_ok=True)

    ids = {
        "workspace_id": workspace_id,
        "mapping_id": mapping_id,
        "mapping_ids": {
            "comprehensive": mapping_id
        }
    }

    with open(data_file, "w") as f:
        json.dump(ids, f, indent=2)

    print_success(f"Saved IDs to {data_file}")

    # Also save legacy mapping_ids.json for compatibility
    legacy_file = Path(__file__).parent / "data" / "mapping_ids.json"
    with open(legacy_file, "w") as f:
        json.dump({"comprehensive": mapping_id}, f, indent=2)


async def main():
    print_info_section("Element Mapping Demo - Step 6: Create Mappings (Fork Architecture)")

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

    print(f"Main graph: {graph_id}\n")

    # Initialize extensions with new clients
    extensions = RoboSystemsExtensions(
        RoboSystemsExtensionConfig(
            base_url=config_data["base_url"],
            headers={"X-API-Key": config_data["api_key"]},
        )
    )

    # Initialize API client for subgraph operations
    sdk_client = AuthenticatedClient(
        base_url=config_data["base_url"],
        token=config_data["api_key"],
        prefix="",
        auth_header_name="X-API-Key",
    )

    # Create client extensions
    mapping_client = ElementMappingClient(extensions.query)
    workspace_client = SubgraphWorkspaceClient(
        api_client=sdk_client,
        query_client=extensions.query
    )

    # Step 1: Create subgraph workspace with fork from parent
    print_info_section("Step 1: Creating Subgraph Workspace with Fork")

    print("🔄 Creating workspace and forking Element data from parent graph...")
    print("  This uses the new fork feature with SSE progress tracking!")

    # Create workspace with fork_parent=True to copy data during creation
    workspace = await workspace_client.create_workspace_with_fork(
        parent_graph_id=graph_id,
        name="mappings",
        display_name="Element Mappings Workspace",
        fork_parent=True,  # Enable forking from parent
        fork_options={
            "tables": [],  # Empty list = fork all tables
            "exclude_patterns": []  # No exclusions
        },
        progress_callback=lambda msg, pct: print(f"  [{pct:3.0f}%] {msg}")
    )

    workspace_id = workspace.graph_id
    print_success(f"Created workspace with fork: {workspace_id}")

    if workspace.fork_status:
        print(f"  Fork status: {workspace.fork_status.get('status', 'unknown')}")
        if workspace.fork_status.get('tables_copied'):
            print(f"  Tables copied: {', '.join(workspace.fork_status['tables_copied'])}")
        if workspace.fork_status.get('row_count'):
            print(f"  Rows copied: {workspace.fork_status['row_count']:,}")

    # Step 2: Create comprehensive mapping (in the forked workspace)
    comprehensive_id = await create_comprehensive_mapping(
        extensions, mapping_client, workspace_id, graph_id
    )

    # Step 3: List all mappings
    await list_all_mappings(mapping_client, workspace_id)

    # Step 4: Export and publish to main graph (DISABLED - not yet fully implemented)
    # print_info_section("Step 4: Publishing to Main Graph")
    #
    # print("🔄 Exporting workspace to parquet...")
    # export_result = await workspace_client.export_to_parquet(
    #     workspace_id=workspace_id,
    #     shared_filename="element_mappings.parquet",
    #     tables=["Structure", "Association", "Element"]
    # )
    # print_success(f"Exported {export_result.total_rows} rows")
    #
    # print("🔄 Publishing to main graph via incremental ingest...")
    # publish_result = await workspace_client.publish_to_main_graph(
    #     workspace_id=workspace_id,
    #     parent_graph_id=graph_id,
    #     shared_filename=export_result.shared_filename,
    #     delete_workspace=False  # Keep workspace for testing views
    # )
    #
    # if publish_result.success:
    #     print_success(f"✅ Published to main graph!")
    #     print(f"  Nodes created: {publish_result.nodes_created}")
    #     print(f"  Relationships created: {publish_result.relationships_created}")
    # else:
    #     print_error("Publishing failed")

    print_info_section("Export to Main Graph")
    print_warning("⚠️  Export functionality temporarily disabled (not yet fully implemented)")
    print("  Mappings remain in subgraph workspace for view generation testing")

    # Save IDs for next steps
    save_workspace_and_mapping_ids(workspace_id, comprehensive_id)

    print_success("\n✅ Element mapping creation complete!")
    print("\n🚀 Fork & Merge Architecture Benefits:")
    print("  • Main graph remains read-only (file-based)")
    print("  • Subgraph created with fork_parent=True for automatic data copy")
    print("  • SSE progress tracking during fork operation")
    print("  • Mappings created in isolated write-enabled workspace")
    print("  • Published atomically via parquet export/ingest")
    print("  • Won't be wiped by rebuild operations")
    print("\nNext step:")
    print("  uv run 07_test_views.py  # Test view generation with mappings")


if __name__ == "__main__":
    asyncio.run(main())