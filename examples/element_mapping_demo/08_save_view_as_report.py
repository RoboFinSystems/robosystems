#!/usr/bin/env python3
"""
Save View as Report Instance

Materializes computed financial statement views into a complete report structure with:
- Report metadata (period, entity, report type)
- Facts with all aspects (element, period, entity, unit)
- Presentation structure (how facts are displayed)
- Calculation structure (how facts roll up)

This creates an XBRL-like instance document that can be:
- Saved to the graph for persistence
- Exported as parquet for parent graph ingest
- Versioned and compared over time
- Used for SEC filings or management reporting

Usage:
    uv run 08_save_view_as_report.py
"""

import asyncio
import json
import sys
from pathlib import Path
from datetime import datetime

from robosystems_client import AuthenticatedClient
from robosystems_client.api.views import save_view
from robosystems_client.models import SaveViewRequest
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


async def save_report_metadata(report_id: str, workspace_id: str, response_data: dict):
    """Save report metadata for reference"""
    data_file = Path(__file__).parent / "data" / "report_metadata.json"

    metadata = {
        "report_id": report_id,
        "workspace_id": workspace_id,
        "created_at": datetime.utcnow().isoformat(),
        "parquet_prefix": report_id,
        "fact_count": response_data.get("fact_count", 0),
        "presentation_count": response_data.get("presentation_count", 0),
        "calculation_count": response_data.get("calculation_count", 0),
    }

    data_file.parent.mkdir(parents=True, exist_ok=True)
    with open(data_file, "w") as f:
        json.dump(metadata, f, indent=2)

    print_success(f"Saved report metadata: {data_file}")


async def main():
    print_info_section("Element Mapping Demo - Step 8: Save View as Report")

    if not CREDENTIALS_FILE.exists():
        print_error(f"Credentials file not found: {CREDENTIALS_FILE}")
        sys.exit(1)

    with open(CREDENTIALS_FILE) as f:
        config_data = json.load(f)

    graph_id = get_graph_id(CREDENTIALS_FILE, DEMO_NAME)
    if not graph_id:
        print_error("No graph_id found for element_mapping_demo")
        sys.exit(1)

    data_file = Path(__file__).parent / "data" / "workspace_mapping_ids.json"
    if not data_file.exists():
        print_error(f"Workspace IDs file not found: {data_file}")
        sys.exit(1)

    with open(data_file) as f:
        ids_data = json.load(f)

    workspace_id = ids_data["workspace_id"]
    print(f"Main graph: {graph_id}")
    print(f"Workspace: {workspace_id}\n")

    client = AuthenticatedClient(
        base_url=config_data["base_url"],
        token=config_data["api_key"],
        prefix="",
        auth_header_name="X-API-Key",
    )

    period_start = "2024-01-01"
    period_end = "2024-12-31"
    report_type = "Annual Report"

    print_info_section("Saving View as Report via API")
    print(f"  Period: {period_start} to {period_end}")
    print(f"  Type: {report_type}\n")

    request = SaveViewRequest(
        report_type=report_type,
        period_start=period_start,
        period_end=period_end,
        include_presentation=True,
        include_calculation=True,
    )

    try:
        response = save_view.sync(
            graph_id=workspace_id,
            client=client,
            body=request,
        )

        if response:
            print_success("✅ Report created successfully via API!")
            print_info_section("Report Summary")
            print(f"  Report ID: {response.report_id}")
            print(f"  Report Type: {response.report_type}")
            print(f"  Entity: {response.entity_name} ({response.entity_id})")
            print(f"  Period: {response.period_start} to {response.period_end}")
            print(f"  Facts: {response.fact_count}")
            print(f"  Presentation Structures: {response.presentation_count}")
            print(f"  Calculation Structures: {response.calculation_count}")
            print(f"  Parquet Export Prefix: {response.parquet_export_prefix}")

            if response.facts:
                print_info_section("Facts Created")
                for fact in response.facts[:5]:
                    print(f"  • {fact.element_name}: ${fact.numeric_value:,.2f}")
                if len(response.facts) > 5:
                    print(f"  ... and {len(response.facts) - 5} more facts")

            if response.structures:
                print_info_section("Structures Created")
                for structure in response.structures:
                    print(f"  • {structure.name} ({structure.structure_type}): {structure.element_count} elements")

            await save_report_metadata(
                response.report_id,
                workspace_id,
                {
                    "fact_count": response.fact_count,
                    "presentation_count": response.presentation_count,
                    "calculation_count": response.calculation_count,
                }
            )

            print_success("\n💡 What was created:")
            print(f"  • Report node with metadata and aspects")
            print(f"  • {response.fact_count} Fact nodes with period/entity/unit context")
            print(f"  • {response.presentation_count} Presentation structures")
            print(f"  • {response.calculation_count} Calculation structures")
            print(f"\n📦 Ready for export:")
            print(f"  • Report ID: {response.report_id}")
            print(f"  • Parquet prefix: {response.parquet_export_prefix}-*.parquet")
            print(f"  • Can be ingested into parent graph")
            print(f"\n🔄 Next steps:")
            print(f"  • Export to parquet files")
            print(f"  • Ingest to parent graph for persistence")
            print(f"  • Query across multiple report periods for trends")

            print_info_section("Testing Update Mode")
            print(f"Re-saving with same report_id will update the report...\n")

            update_request = SaveViewRequest(
                report_id=response.report_id,
                report_type=report_type,
                period_start=period_start,
                period_end=period_end,
                include_presentation=True,
                include_calculation=True,
            )

            update_response = save_view.sync(
                graph_id=workspace_id,
                client=client,
                body=update_request,
            )

            if update_response:
                print_success("✅ Report updated successfully!")
                print(f"  Same report_id: {update_response.report_id}")
                print(f"  Facts: {update_response.fact_count}")
                print(f"  (Old facts were deleted, new facts created)")

        else:
            print_error("Failed to create report - no response from API")

    except Exception as e:
        print_error(f"Failed to save view: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
