#!/usr/bin/env python3
"""
Create Custom Graph

This script creates a new generic graph for the custom graph demo. It uses saved
credentials from step 01.

Usage:
    uv run 02_create_graph.py                           # Auto-generate graph name
    uv run 02_create_graph.py --name "My Custom Graph"
    uv run 02_create_graph.py --reuse                   # Reuse existing graph

After running, graph_id is saved to credentials/config.json
"""

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Callable

from robosystems_client.extensions import (
  RoboSystemsExtensions,
  RoboSystemsExtensionConfig,
  GraphMetadata,
)
from robosystems_client.extensions.graph_client import GraphClient
from robosystems_client.client import AuthenticatedClient
from robosystems_client.api.graphs.create_graph import sync_detailed as api_create_graph
from robosystems_client.api.operations.get_operation_status import (
  sync_detailed as api_get_operation_status,
)
from robosystems_client.models import (
  CreateGraphRequest,
  GraphMetadata as APIGraphMetadata,
  CustomSchemaDefinition,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
  sys.path.insert(0, str(PROJECT_ROOT))

from examples.credentials.utils import get_graph_id, save_graph_id

CREDENTIALS_DIR = Path(__file__).resolve().parents[1] / "credentials"
DEFAULT_CREDENTIALS_FILE = CREDENTIALS_DIR / "config.json"
DEMO_NAME = "custom_graph_demo"


def build_custom_schema_definition() -> CustomSchemaDefinition:
  """Return the custom schema used for the generic custom graph demo."""
  schema_dict = {
    "name": "custom_graph_demo",
    "version": "1.0.0",
    "description": "People, companies, and projects schema for the custom graph demo",
    "extends": "base",
    "nodes": [
      {
        "name": "Company",
        "properties": [
          {"name": "identifier", "type": "STRING", "is_primary_key": True},
          {"name": "name", "type": "STRING", "is_required": True},
          {"name": "industry", "type": "STRING"},
          {"name": "location", "type": "STRING"},
          {"name": "founded_year", "type": "INT64"},
        ],
      },
      {
        "name": "Project",
        "properties": [
          {"name": "identifier", "type": "STRING", "is_primary_key": True},
          {"name": "name", "type": "STRING", "is_required": True},
          {"name": "status", "type": "STRING"},
          {"name": "budget", "type": "DOUBLE"},
          {"name": "start_date", "type": "STRING"},
          {"name": "end_date", "type": "STRING"},
          {"name": "sponsor_company", "type": "STRING"},
        ],
      },
      {
        "name": "Person",
        "properties": [
          {"name": "identifier", "type": "STRING", "is_primary_key": True},
          {"name": "name", "type": "STRING", "is_required": True},
          {"name": "age", "type": "INT64"},
          {"name": "title", "type": "STRING"},
          {"name": "interests", "type": "STRING"},
          {"name": "location", "type": "STRING"},
          {"name": "works_for", "type": "STRING"},
          {"name": "start_date", "type": "STRING"},
        ],
      },
    ],
    "relationships": [
      {
        "name": "PERSON_WORKS_FOR_COMPANY",
        "from_node": "Person",
        "to_node": "Company",
        "properties": [
          {"name": "role", "type": "STRING"},
          {"name": "started_on", "type": "STRING"},
        ],
      },
      {
        "name": "PERSON_WORKS_ON_PROJECT",
        "from_node": "Person",
        "to_node": "Project",
        "properties": [
          {"name": "hours_per_week", "type": "INT64"},
          {"name": "contribution", "type": "STRING"},
        ],
      },
      {
        "name": "COMPANY_SPONSORS_PROJECT",
        "from_node": "Company",
        "to_node": "Project",
        "properties": [
          {"name": "sponsorship_level", "type": "STRING"},
          {"name": "budget_committed", "type": "DOUBLE"},
        ],
      },
    ],
    "metadata": {"domain": "custom_graph_demo"},
  }

  return CustomSchemaDefinition.from_dict(schema_dict)


def create_graph_with_custom_schema(
  graph_client: GraphClient,
  metadata: GraphMetadata,
  custom_schema: CustomSchemaDefinition,
  timeout: int = 60,
  poll_interval: int = 2,
  on_progress: Callable[[str], None] | None = None,
) -> str:
  """Create a graph using a custom schema and wait for completion."""
  if not graph_client.token:
    raise ValueError("No API key provided. Set X-API-Key in headers.")

  client = AuthenticatedClient(
    base_url=graph_client.base_url,
    token=graph_client.token,
    prefix="",
    auth_header_name="X-API-Key",
    headers=graph_client.headers,
  )

  api_metadata = APIGraphMetadata(
    graph_name=metadata.graph_name,
    description=metadata.description,
    schema_extensions=metadata.schema_extensions or [],
    tags=metadata.tags or [],
  )

  graph_request = CreateGraphRequest(
    metadata=api_metadata,
    custom_schema=custom_schema,
    tags=metadata.tags or [],
  )

  if on_progress:
    on_progress(f"Creating graph: {metadata.graph_name}")

  response = api_create_graph(client=client, body=graph_request)

  if not response.parsed:
    raise RuntimeError(f"Failed to create graph: {response.status_code}")

  parsed_response = response.parsed
  if isinstance(parsed_response, dict):
    graph_id = parsed_response.get("graph_id")
    operation_id = parsed_response.get("operation_id")
  else:
    graph_id = getattr(parsed_response, "graph_id", None)
    operation_id = getattr(parsed_response, "operation_id", None)

  if graph_id:
    if on_progress:
      on_progress(f"Graph created: {graph_id}")
    return graph_id

  if not operation_id:
    raise RuntimeError("No graph_id or operation_id in response")

  if on_progress:
    on_progress(f"Graph creation queued (operation: {operation_id})")

  max_attempts = max(1, timeout // poll_interval)
  for _ in range(max_attempts):
    time.sleep(poll_interval)

    status_response = api_get_operation_status(operation_id=operation_id, client=client)
    if not status_response.parsed:
      continue

    status_data = status_response.parsed
    if isinstance(status_data, dict):
      status = status_data.get("status")
      result = status_data.get("result")
      error = status_data.get("error") or status_data.get("message")
    elif hasattr(status_data, "additional_properties"):
      props = status_data.additional_properties
      status = props.get("status")
      result = props.get("result")
      error = props.get("error") or props.get("message")
    else:
      status = getattr(status_data, "status", None)
      result = getattr(status_data, "result", None)
      error = getattr(status_data, "message", None)

    if status == "completed":
      graph_id = None
      if isinstance(result, dict):
        graph_id = result.get("graph_id")
      elif result and hasattr(result, "graph_id"):
        graph_id = getattr(result, "graph_id", None)

      if not graph_id and hasattr(status_data, "graph_id"):
        graph_id = getattr(status_data, "graph_id", None)

      if graph_id:
        if on_progress:
          on_progress(f"Graph created: {graph_id}")
        return graph_id

      raise RuntimeError("Operation completed but no graph_id in result")

    if status == "failed":
      raise RuntimeError(f"Graph creation failed: {error or 'Unknown error'}")

  raise TimeoutError(f"Graph creation timed out after {timeout}s")


def load_credentials(credentials_path: Path):
  """Load saved credentials."""
  if not credentials_path.exists():
    print(f"\n❌ No credentials found at {credentials_path}")
    print("   Run: uv run 01_setup_credentials.py first")
    sys.exit(1)

  with open(credentials_path) as f:
    return json.load(f)


def save_graph_data(graph_id: str, credentials_path: Path):
  """Save graph_id to credentials file."""
  graph_created_at = time.strftime("%Y-%m-%d %H:%M:%S")
  save_graph_id(credentials_path, DEMO_NAME, graph_id, graph_created_at)
  print(f"\n💾 Graph ID saved to: {credentials_path}")


def create_custom_graph(
  base_url: str,
  api_key: str,
  graph_name: str = None,
  reuse: bool = False,
  credentials_path: Path = DEFAULT_CREDENTIALS_FILE,
):
  """Create a new generic graph for custom graph demo."""

  credentials = load_credentials(credentials_path)

  existing_graph_id = get_graph_id(credentials_path, DEMO_NAME)
  if reuse and existing_graph_id:
    credentials_data = load_credentials(credentials_path)
    demo_graph_data = credentials_data.get("graphs", {}).get(DEMO_NAME, {})
    print("\n✅ Reusing existing graph")
    print(f"   Graph ID: {existing_graph_id}")
    print(f"   Created: {demo_graph_data.get('graph_created_at', 'unknown')}")
    return existing_graph_id

  timestamp = int(time.time())
  graph_name = graph_name or f"custom_graph_demo_{timestamp}"

  print("\n" + "=" * 70)
  print("📊 Custom Graph Demo - Graph Creation")
  print("=" * 70)

  config = RoboSystemsExtensionConfig(
    base_url=base_url,
    headers={"X-API-Key": api_key},
  )
  extensions = RoboSystemsExtensions(config)

  metadata = GraphMetadata(
    graph_name=graph_name,
    description="Custom graph demo with people, companies, and projects using generic schema",
    schema_extensions=[],  # No specific schema extensions for generic graphs
    tags=["custom", "demo", "generic"],
  )

  custom_schema = build_custom_schema_definition()

  try:
    graph_id = create_graph_with_custom_schema(
      graph_client=extensions.graphs,
      metadata=metadata,
      custom_schema=custom_schema,
      timeout=60,
      on_progress=lambda msg: print(f"   {msg}"),
    )
  except Exception as e:
    print(f"\n❌ Graph creation failed: {e}")
    sys.exit(1)

  save_graph_data(graph_id, credentials_path)

  print("\n" + "=" * 70)
  print("✅ Graph Created Successfully!")
  print("=" * 70)
  print(f"\nGraph ID: {graph_id}")
  print(f"Graph Name: {graph_name}")
  print("\n💡 Next steps:")
  print("   1. Generate data: uv run 03_generate_data.py")
  print("   2. Upload & ingest: uv run 04_upload_ingest.py")
  print("   3. Run queries: uv run 05_query_graph.py")
  print("=" * 70 + "\n")

  return graph_id


def main():
  parser = argparse.ArgumentParser(description="Create custom graph")
  parser.add_argument(
    "--base-url",
    default="http://localhost:8000",
    help="API base URL (default: http://localhost:8000)",
  )
  parser.add_argument(
    "--name",
    help="Graph name (auto-generated if not provided)",
  )
  parser.add_argument(
    "--reuse",
    action="store_true",
    help="Reuse existing graph if available",
  )
  parser.add_argument(
    "--credentials-file",
    default=str(DEFAULT_CREDENTIALS_FILE),
    help="Path to credentials file (default: credentials/config.json)",
  )

  args = parser.parse_args()
  credentials_path = Path(args.credentials_file).expanduser()

  try:
    credentials = load_credentials(credentials_path)
    api_key = credentials.get("api_key")

    if not api_key:
      print("\n❌ No API key found in credentials")
      print("   Run: uv run 01_setup_credentials.py first")
      sys.exit(1)

    create_custom_graph(
      base_url=args.base_url,
      api_key=api_key,
      graph_name=args.name,
      reuse=args.reuse,
      credentials_path=credentials_path,
    )
  except Exception as e:
    print(f"\n❌ Error: {e}")
    import traceback

    traceback.print_exc()
    sys.exit(1)


if __name__ == "__main__":
  main()
