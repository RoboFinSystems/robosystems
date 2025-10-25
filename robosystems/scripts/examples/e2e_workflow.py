# pyright: basic
"""
End-to-end workflow demonstration script.

This script demonstrates the complete RoboSystems workflow:
1. User creation (or use existing API key)
2. Graph creation
3. Parquet file upload to staging tables
4. Data ingestion into graph
5. Graph querying

Usage:
    uv run scripts/e2e_workflow.py --api-key <key>  # Use existing API key
    uv run scripts/e2e_workflow.py                  # Create new user
"""

import argparse
import json
import secrets
import string
import sys
import time
from pathlib import Path
from typing import Optional

import httpx
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


class RoboSystemsWorkflow:
  def __init__(self, base_url: str = "http://localhost:8000"):
    self.base_url = base_url.rstrip("/")
    self.api_key: Optional[str] = None
    self.graph_id: Optional[str] = None
    self.client = httpx.Client(timeout=60.0)

  def _headers(self) -> dict:
    if not self.api_key:
      raise ValueError("API key not set")
    return {
      "X-API-Key": self.api_key,
      "Content-Type": "application/json",
    }

  def create_user(self, name: str, email: str, password: str) -> dict:
    print("\n🔐 Creating new user...")

    response = self.client.post(
      f"{self.base_url}/v1/auth/register",
      json={"name": name, "email": email, "password": password},
    )
    response.raise_for_status()
    data = response.json()

    print(f"✅ User created: {name} ({email})")
    return data

  def login(self, email: str, password: str) -> str:
    print("\n🔑 Logging in...")

    response = self.client.post(
      f"{self.base_url}/v1/auth/login",
      json={"email": email, "password": password},
    )
    response.raise_for_status()
    data = response.json()

    token = data["token"]
    print("✅ Login successful")
    return token

  def create_api_key(self, token: str, key_name: str) -> str:
    print("\n🔑 Creating API key...")

    response = self.client.post(
      f"{self.base_url}/v1/user/api-keys",
      headers={"Authorization": f"Bearer {token}"},
      json={"name": key_name},
    )
    response.raise_for_status()
    data = response.json()

    api_key = data["key"]
    if isinstance(api_key, str):
      print(f"✅ API key created: {api_key[:20]}...")
    else:
      print(f"✅ API key created (type: {type(api_key)})")
    return api_key

  def create_graph(self, name: str, description: str = "") -> str:
    print(f"\n📊 Creating graph: {name}...")

    response = self.client.post(
      f"{self.base_url}/v1/graphs",
      headers=self._headers(),
      json={
        "metadata": {
          "graph_name": name,
          "description": description or f"E2E workflow demo graph - {name}",
        }
      },
    )
    response.raise_for_status()
    data = response.json()

    graph_id = data.get("graph_id")
    if not graph_id:
      print(f"⚠️  Graph creation queued. Operation ID: {data.get('operation_id')}")
      time.sleep(5)
      return self.wait_for_graph_creation(data.get("operation_id"))

    print(f"✅ Graph created: {graph_id}")
    return graph_id

  def wait_for_graph_creation(self, operation_id: str, timeout: int = 60) -> str:
    print("\n⏳ Waiting for graph creation...")
    start = time.time()

    while time.time() - start < timeout:
      try:
        response = self.client.get(
          f"{self.base_url}/v1/operations/{operation_id}/status",
          headers=self._headers(),
        )
        if response.status_code == 200:
          status_data = response.json()
          status = status_data.get("status")

          if status == "completed":
            result = status_data.get("result", {})
            graph_id = result.get("graph_id")
            if graph_id:
              print(f"✅ Graph created: {graph_id}")
              return graph_id
          elif status == "failed":
            error = status_data.get("error", "Unknown error")
            raise Exception(f"Graph creation failed: {error}")
      except Exception as e:
        if "failed" in str(e):
          raise
      time.sleep(2)

    raise TimeoutError("Graph creation timeout")

  def create_sample_parquet(self, output_path: Path, num_rows: int = 100):
    print(f"\n📄 Creating sample Parquet file with {num_rows} rows...")

    df = pd.DataFrame(
      {
        "identifier": [f"entity_{i:03d}" for i in range(1, num_rows + 1)],
        "uri": [None] * num_rows,
        "scheme": [None] * num_rows,
        "cik": [None] * num_rows,
        "ticker": [f"TKR{i}" if i % 10 == 0 else None for i in range(1, num_rows + 1)],
        "exchange": [None] * num_rows,
        "name": [f"Entity_{i}" for i in range(1, num_rows + 1)],
        "legal_name": [None] * num_rows,
        "industry": [None] * num_rows,
        "entity_type": [None] * num_rows,
        "sic": [None] * num_rows,
        "sic_description": [None] * num_rows,
        "category": [f"Category_{i % 5}" for i in range(1, num_rows + 1)],
        "state_of_incorporation": [None] * num_rows,
        "fiscal_year_end": [None] * num_rows,
        "ein": [None] * num_rows,
        "tax_id": [None] * num_rows,
        "lei": [None] * num_rows,
        "phone": [None] * num_rows,
        "website": [None] * num_rows,
        "status": [None] * num_rows,
        "is_parent": pd.Series([None] * num_rows, dtype="boolean"),
        "parent_entity_id": [None] * num_rows,
        "created_at": pd.date_range("2025-01-01", periods=num_rows, freq="h").strftime(
          "%Y-%m-%d %H:%M:%S"
        ),
        "updated_at": [None] * num_rows,
      }
    )

    table = pa.Table.from_pandas(df)
    pq.write_table(table, output_path)

    print(f"✅ Created {output_path.name} ({output_path.stat().st_size:,} bytes)")
    return df

  def upload_parquet(self, graph_id: str, table_name: str, file_path: Path) -> dict:
    print(f"\n📤 Uploading {file_path.name} to table '{table_name}'...")

    # Step 1: Get presigned upload URL
    response = self.client.post(
      f"{self.base_url}/v1/graphs/{graph_id}/tables/{table_name}/files",
      headers=self._headers(),
      json={"file_name": file_path.name, "content_type": "application/x-parquet"},
    )
    response.raise_for_status()
    upload_data = response.json()

    upload_url = upload_data["upload_url"]
    file_id = upload_data["file_id"]

    # Fix LocalStack URL for host machine access
    if "localstack:4566" in upload_url:
      upload_url = upload_url.replace("localstack:4566", "localhost:4566")
      print(f"   Upload URL (fixed for localhost): {upload_url[:100]}...")

    # Step 2: Upload file to S3
    with open(file_path, "rb") as f:
      file_content = f.read()
      file_size = len(file_content)

      s3_response = self.client.put(
        upload_url,
        content=file_content,
        headers={"Content-Type": "application/x-parquet"},
      )
      s3_response.raise_for_status()

    # Step 3: Update file metadata
    response = self.client.patch(
      f"{self.base_url}/v1/graphs/{graph_id}/tables/files/{file_id}",
      headers=self._headers(),
      json={"file_size_bytes": file_size, "row_count": 50},
    )
    response.raise_for_status()

    print(f"✅ Uploaded {file_path.name} ({file_size:,} bytes) to table '{table_name}'")
    return {"file_id": file_id, "file_size": file_size}

  def list_tables(self, graph_id: str) -> list:
    print("\n📋 Listing staging tables...")

    response = self.client.get(
      f"{self.base_url}/v1/graphs/{graph_id}/tables",
      headers=self._headers(),
    )
    response.raise_for_status()
    data = response.json()

    if data["tables"]:
      print("\nStaging Tables:")
      print(
        f"{'Table Name':<20} {'Row Count':<12} {'File Count':<12} {'Size (bytes)':<15}"
      )
      print("-" * 60)
      for tbl in data["tables"]:
        print(
          f"{tbl['table_name']:<20} {tbl['row_count']:<12} "
          f"{tbl['file_count']:<12} {tbl['total_size_bytes']:>14,}"
        )
    else:
      print("No tables found")

    return data["tables"]

  def ingest_tables(self, graph_id: str) -> dict:
    print("\n⚙️  Ingesting all tables to graph...")

    response = self.client.post(
      f"{self.base_url}/v1/graphs/{graph_id}/tables/ingest",
      headers=self._headers(),
      json={"ignore_errors": True, "rebuild": False},
    )
    response.raise_for_status()
    data = response.json()

    print("✅ Ingestion completed")
    return data

  def query_graph(self, graph_id: str, query: str) -> dict:
    print("\n🔍 Executing query...")
    print(f"   {query}")

    response = self.client.post(
      f"{self.base_url}/v1/graphs/{graph_id}/query?mode=sync",
      headers=self._headers(),
      json={"query": query},
    )
    response.raise_for_status()
    data = response.json()

    return data

  def display_query_results(self, results: dict):
    if results.get("error"):
      print(f"❌ Query error: {results['error']}")
      return

    records = results.get("data", results.get("results", results.get("records", [])))
    print(f"\n✅ Query returned {len(records)} records")

    if records:
      if len(records) <= 10:
        print("\nResults:")
        print(json.dumps(records, indent=2))
      else:
        print(f"\nShowing first 10 of {len(records)} records:")
        print(json.dumps(records[:10], indent=2))

  def generate_secure_password(self, length: int = 16) -> str:
    """
    Generate a cryptographically secure password.

    Args:
        length: Total password length (must be divisible by 4). Default: 16

    Returns:
        A secure password containing lowercase, uppercase, digits, and special chars
    """
    chars_per_type = length // 4
    password = (
      "".join(secrets.choice(string.ascii_lowercase) for _ in range(chars_per_type))
      + "".join(secrets.choice(string.ascii_uppercase) for _ in range(chars_per_type))
      + "".join(secrets.choice(string.digits) for _ in range(chars_per_type))
      + "".join(secrets.choice("!@#$%^&*") for _ in range(chars_per_type))
    )
    password_list = list(password)
    secrets.SystemRandom().shuffle(password_list)
    return "".join(password_list)

  def run_workflow(
    self,
    name: Optional[str] = None,
    email: Optional[str] = None,
    password: Optional[str] = None,
    api_key: Optional[str] = None,
  ):
    print("\n" + "=" * 60)
    print("🤖 RoboSystems E2E Workflow Demo")
    print("=" * 60)

    if api_key:
      print("\n✅ Using provided API key")
      self.api_key = api_key
    else:
      if not name or not email or not password:
        timestamp = int(time.time())
        name = name or f"Demo User {timestamp}"
        email = email or f"demo_{timestamp}@example.com"
        password = password or self.generate_secure_password()

        print("\n📧 Auto-generated credentials:")
        print(f"   Name: {name}")
        print(f"   Email: {email}")
        print(f"   Password: {password}")

      self.create_user(name, email, password)
      token = self.login(email, password)
      self.api_key = self.create_api_key(token, f"E2E Demo Key - {name}")

    graph_name = f"demo_graph_{int(time.time())}"
    self.graph_id = self.create_graph(graph_name)

    temp_dir = Path("/tmp/robosystems_demo")
    temp_dir.mkdir(exist_ok=True)
    parquet_file = temp_dir / "sample_data.parquet"

    _df = self.create_sample_parquet(parquet_file, num_rows=50)

    table_name = "Entity"
    self.upload_parquet(self.graph_id, table_name, parquet_file)

    self.list_tables(self.graph_id)

    self.ingest_tables(self.graph_id)

    queries = [
      "MATCH (n:Entity) RETURN count(n) AS total_nodes",
      "MATCH (n:Entity) RETURN n.identifier, n.name, n.ticker, n.category LIMIT 5",
      "MATCH (n:Entity) WHERE n.ticker IS NOT NULL RETURN n.identifier, n.name, n.ticker ORDER BY n.identifier DESC LIMIT 10",
    ]

    for query in queries:
      results = self.query_graph(self.graph_id, query)
      self.display_query_results(results)

    print("\n" + "=" * 60)
    print("✅ E2E Workflow Complete!")
    print("=" * 60)
    print(f"\n📊 Graph ID: {self.graph_id}")
    print(f"🔑 API Key: {self.api_key[:20]}...")
    print("\n💡 You can continue querying this graph using the API key\n")

    parquet_file.unlink()


def main():
  parser = argparse.ArgumentParser(description="RoboSystems E2E workflow demonstration")
  parser.add_argument(
    "--api-key",
    help="Use existing API key (skip user creation)",
  )
  parser.add_argument(
    "--name",
    help="Name for new user (auto-generated if not provided)",
  )
  parser.add_argument(
    "--email",
    help="Email for new user (auto-generated if not provided)",
  )
  parser.add_argument(
    "--password",
    help="Password for new user (auto-generated if not provided)",
  )
  parser.add_argument(
    "--base-url",
    default="http://localhost:8000",
    help="API base URL (default: http://localhost:8000)",
  )

  args = parser.parse_args()

  try:
    workflow = RoboSystemsWorkflow(base_url=args.base_url)
    workflow.run_workflow(
      name=args.name,
      email=args.email,
      password=args.password,
      api_key=args.api_key,
    )
  except httpx.HTTPStatusError as e:
    print(f"\n❌ HTTP Error: {e.response.status_code}")
    try:
      error_detail = e.response.json()
      print(f"   {json.dumps(error_detail, indent=2)}")
    except Exception:
      print(f"   {e.response.text}")
    sys.exit(1)
  except Exception as e:
    print(f"\n❌ Error: {e}")
    import traceback

    traceback.print_exc()
    sys.exit(1)


if __name__ == "__main__":
  main()
