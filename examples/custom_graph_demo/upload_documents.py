#!/usr/bin/env python3
"""
Upload Project Management Documents

Uploads markdown documents from the documents/ directory into the graph's
OpenSearch index via the RoboSystems SDK. Then runs sample searches
to demonstrate document search alongside graph data.

Usage:
    uv run upload_documents.py
    uv run upload_documents.py --base-url http://localhost:8000
"""

import argparse
import json
import os
import sys
from pathlib import Path

from robosystems_client.clients import (
  RoboSystemsClientConfig,
  RoboSystemsClients,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
  sys.path.insert(0, str(PROJECT_ROOT))

from examples._common.config import get_graph_id  # noqa: E402

CREDENTIALS_FILE = PROJECT_ROOT / ".local" / "config.json"
DEMO_NAME = "custom_graph_demo"
DOCUMENTS_DIR = Path(__file__).parent / "documents"

SAMPLE_SEARCHES = [
  "how do we staff an Autonomous Delivery project",
  "what happens when a project goes on hold",
  "cross-company collaboration IP ownership",
]


def load_credentials() -> dict:
  if not CREDENTIALS_FILE.exists():
    print("❌ Credentials not found. Run setup_credentials.py first.")
    sys.exit(1)
  with CREDENTIALS_FILE.open() as f:
    return json.load(f)


def _envelope_result(envelope) -> dict:
  """Pull the result payload out of an OperationEnvelope.

  ``index-document`` returns the CQRS ``OperationEnvelope`` now; the section
  counts live in its ``result`` payload rather than as attributes on the
  envelope itself. ``result`` is typed ``Any``, so it arrives as a plain dict.
  """
  result = getattr(envelope, "result", None)
  if hasattr(result, "to_dict"):
    return result.to_dict()
  if hasattr(result, "additional_properties"):
    return dict(result.additional_properties)
  return result if isinstance(result, dict) else {}


def upload_documents(clients, graph_id: str) -> None:
  """Upload all markdown documents from the documents/ directory."""
  if not DOCUMENTS_DIR.exists():
    print(f"❌ Documents directory not found: {DOCUMENTS_DIR}")
    sys.exit(1)

  md_files = sorted(DOCUMENTS_DIR.glob("*.md"))
  if not md_files:
    print("❌ No markdown files found in documents/")
    sys.exit(1)

  print(f"\n📄 Uploading {len(md_files)} documents to graph {graph_id}...")

  total_sections = 0
  for md_file in md_files:
    try:
      envelope = clients.documents.upload_file(graph_id=graph_id, file_path=md_file)
      sections = _envelope_result(envelope).get("sections_indexed", 0)
      total_sections += sections
      print(f"   ✅ {md_file.name}: {sections} sections indexed")
    except Exception as e:
      print(f"   ❌ {md_file.name}: {e}")

  print(
    f"\n📊 Total: {total_sections} sections indexed across {len(md_files)} documents"
  )

  # Verify via list endpoint
  doc_list = clients.documents.list(graph_id)
  print(f"📋 Documents in index: {doc_list.total}")
  for doc in doc_list.documents:
    print(
      f"   • {doc.document_title} ({doc.section_count} sections, {doc.source_type})"
    )


def run_sample_searches(clients, graph_id: str) -> None:
  """Run sample searches to demonstrate document discovery."""
  print("\n🔍 Running sample searches...")

  for query in SAMPLE_SEARCHES:
    result = clients.documents.search(graph_id=graph_id, query=query, size=3)

    print(f'\n   Query: "{query}"')
    print(f"   Results: {result.total} total")
    for hit in result.hits[:3]:
      title = hit.document_title or ""
      section = hit.section_label or ""
      print(f"     [{hit.score:.2f}] {title} > {section}")
      if hit.snippet:
        print(f"            {hit.snippet[:100]}...")


def main():
  parser = argparse.ArgumentParser(description="Upload project management documents")
  parser.add_argument(
    "--base-url",
    default=os.environ.get("ROBOSYSTEMS_API_URL", "http://localhost:8000"),
    help="API base URL (default: $ROBOSYSTEMS_API_URL or http://localhost:8000)",
  )
  args = parser.parse_args()

  credentials = load_credentials()
  api_key = credentials.get("api_key")
  graph_id = get_graph_id(CREDENTIALS_FILE, DEMO_NAME)

  if not api_key or not graph_id:
    print("❌ Missing API key or graph ID. Run previous demo steps first.")
    sys.exit(1)

  config = RoboSystemsClientConfig(
    base_url=args.base_url,
    headers={"X-API-Key": api_key},
  )
  clients = RoboSystemsClients(config)

  upload_documents(clients, graph_id)
  run_sample_searches(clients, graph_id)

  print("\n✅ Document upload complete!")
  print("💡 Search these documents alongside your graph data using the search API")


if __name__ == "__main__":
  main()
