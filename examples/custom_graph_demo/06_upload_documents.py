#!/usr/bin/env python3
"""
Upload Project Management Documents

Uploads markdown documents from the documents/ directory into the graph's
OpenSearch index via the document upload API. Then runs sample searches
to demonstrate document search alongside graph data.

Usage:
    uv run 06_upload_documents.py
    uv run 06_upload_documents.py --base-url http://localhost:8000
"""

import argparse
import json
import sys
from pathlib import Path

import httpx
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
  sys.path.insert(0, str(PROJECT_ROOT))

from examples.credentials.utils import get_graph_id

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
    print("❌ Credentials not found. Run 01_setup_credentials.py first.")
    sys.exit(1)
  with CREDENTIALS_FILE.open() as f:
    return json.load(f)


def parse_frontmatter(content: str) -> tuple[dict, str]:
  """Extract YAML frontmatter from markdown content."""
  stripped = content.lstrip()
  if not stripped.startswith("---"):
    return {}, content
  end_idx = stripped.find("---", 3)
  if end_idx == -1:
    return {}, content
  frontmatter = yaml.safe_load(stripped[3:end_idx].strip())
  remaining = stripped[end_idx + 3 :].lstrip("\n")
  return (frontmatter if isinstance(frontmatter, dict) else {}), remaining


def upload_documents(base_url: str, api_key: str, graph_id: str) -> None:
  """Upload all markdown documents from the documents/ directory."""
  if not DOCUMENTS_DIR.exists():
    print(f"❌ Documents directory not found: {DOCUMENTS_DIR}")
    sys.exit(1)

  md_files = sorted(DOCUMENTS_DIR.glob("*.md"))
  if not md_files:
    print("❌ No markdown files found in documents/")
    sys.exit(1)

  print(f"\n📄 Uploading {len(md_files)} documents to graph {graph_id}...")

  client = httpx.Client(
    base_url=f"{base_url}/v1/graphs/{graph_id}",
    headers={"X-API-Key": api_key, "Content-Type": "application/json"},
    timeout=60,
  )

  total_sections = 0
  for md_file in md_files:
    content = md_file.read_text()
    metadata, body = parse_frontmatter(content)

    title = metadata.get("title", md_file.stem.replace("-", " ").title())
    tags = metadata.get("tags")
    if isinstance(tags, str):
      tags = [t.strip() for t in tags.split(",") if t.strip()]
    folder = metadata.get("folder")

    payload = {
      "title": title,
      "content": content,
      "tags": tags,
      "folder": folder,
    }

    response = client.post("/documents", json=payload)

    if response.status_code == 200:
      result = response.json()
      sections = result["sections_indexed"]
      total_sections += sections
      print(f"   ✅ {md_file.name}: {sections} sections indexed")
    else:
      print(f"   ❌ {md_file.name}: {response.status_code} - {response.text}")

  print(f"\n📊 Total: {total_sections} sections indexed across {len(md_files)} documents")
  return client


def run_sample_searches(client: httpx.Client) -> None:
  """Run sample searches to demonstrate document discovery."""
  print("\n🔍 Running sample searches...")

  for query in SAMPLE_SEARCHES:
    response = client.post("/search", json={"query": query, "size": 3})
    if response.status_code != 200:
      print(f"   ❌ Search failed: {response.status_code}")
      continue

    result = response.json()
    total = result["total"]
    hits = result["hits"]

    print(f'\n   Query: "{query}"')
    print(f"   Results: {total} total")
    for hit in hits[:3]:
      title = hit.get("document_title", "")
      section = hit.get("section_label", "")
      score = hit.get("score", 0)
      snippet = hit.get("snippet", "")[:100]
      print(f"     [{score:.2f}] {title} > {section}")
      if snippet:
        print(f"            {snippet}...")


def main():
  parser = argparse.ArgumentParser(
    description="Upload project management documents"
  )
  parser.add_argument(
    "--base-url",
    default="http://localhost:8000",
    help="API base URL (default: http://localhost:8000)",
  )
  args = parser.parse_args()

  credentials = load_credentials()
  api_key = credentials.get("api_key")
  graph_id = get_graph_id(CREDENTIALS_FILE, DEMO_NAME)

  if not api_key or not graph_id:
    print("❌ Missing API key or graph ID. Run previous demo steps first.")
    sys.exit(1)

  client = upload_documents(args.base_url, api_key, graph_id)
  run_sample_searches(client)

  print("\n✅ Document upload complete!")
  print("💡 Search these documents alongside your graph data using the search API")


if __name__ == "__main__":
  main()
