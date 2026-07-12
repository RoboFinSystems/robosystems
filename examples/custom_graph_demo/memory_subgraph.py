#!/usr/bin/env python3
"""
Memory Subgraph Demo

Creates a `knowledge` subgraph within an existing parent graph and exercises
the two complementary memory modalities that share it:

  1. Structured knowledge graph — seeds Concept / Observation / Session nodes
     via Cypher and traverses them with exact graph queries.
  2. Semantic memory — stores free-text memories with `remember` and retrieves
     them by meaning (local vector embeddings) with `recall` / `list-memories`.

Together these show the AI-memory read split: query (exact graph traversal)
vs. search (ranked semantic recall), both scoped to the same subgraph.

The `knowledge` schema extension (formerly "memory") provides the structured
starter schema; semantic memory is backed by a per-graph vector store.

Usage:
    uv run memory_subgraph.py
    uv run memory_subgraph.py --base-url http://localhost:8000
    uv run memory_subgraph.py --name mymemory
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from enum import Enum
from pathlib import Path

from robosystems_client.api.content_operations.op_remember import (
  sync_detailed as api_remember,
)
from robosystems_client.api.graph_operations.op_create_subgraph import (
  sync_detailed as api_create_subgraph,
)
from robosystems_client.api.memory.list_memories import (
  sync_detailed as api_list_memories,
)
from robosystems_client.api.memory.recall_memory import (
  sync_detailed as api_recall_memory,
)
from robosystems_client.api.query.execute_cypher import (
  sync_detailed as api_execute_query,
)
from robosystems_client.client import AuthenticatedClient
from robosystems_client.models.create_subgraph_request import CreateSubgraphRequest
from robosystems_client.models.cypher_statement_request import CypherStatementRequest
from robosystems_client.models.cypher_statement_request_parameters_type_0 import (
  CypherStatementRequestParametersType0,
)
from robosystems_client.models.memory_recall_request import MemoryRecallRequest
from robosystems_client.models.remember_op import RememberOp
from robosystems_client.types import UNSET

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
  sys.path.insert(0, str(PROJECT_ROOT))

from examples._common.config import get_graph_id  # noqa: E402

CREDENTIALS_FILE = PROJECT_ROOT / ".local" / "config.json"
DEMO_NAME = "custom_graph_demo"


class SubgraphType(str, Enum):
  """Subgraph types the server accepts.

  The installed generated client predates the "memory" -> "knowledge" rename,
  so its own SubgraphType enum still exposes the retired "memory" value and
  lacks "knowledge". Declare the value the server expects locally until the
  client is regenerated. attrs does not type-validate the request field, and
  the client's to_dict() only reads ``.value``, so this drops in cleanly.
  """

  KNOWLEDGE = "knowledge"


def make_params(params: dict) -> CypherStatementRequestParametersType0:
  """Wrap a dict into the generated parameters model."""
  obj = CypherStatementRequestParametersType0()
  for k, v in params.items():
    obj.additional_properties[k] = v
  return obj


def load_credentials(path: Path = CREDENTIALS_FILE) -> dict:
  if not path.exists():
    print(f"❌ Credentials not found at {path}. Run setup_credentials.py first.")
    sys.exit(1)
  with path.open() as f:
    return json.load(f)


def build_client(base_url: str, api_key: str) -> AuthenticatedClient:
  return AuthenticatedClient(
    base_url=base_url,
    token=api_key,
    prefix="",
    auth_header_name="X-API-Key",
  )


def create_memory_subgraph(
  client: AuthenticatedClient, graph_id: str, name: str
) -> str:
  """Create a memory subgraph and return its graph_id."""
  print(f"\n🧠 Creating memory subgraph '{name}' on parent graph {graph_id}...")

  request = CreateSubgraphRequest(
    name=name,
    display_name=f"AI Memory - {name}",
    description="AI agent memory layer for concepts, observations, and sessions",
    schema_extensions=["knowledge"],
    subgraph_type=SubgraphType.KNOWLEDGE,
  )

  response = api_create_subgraph(graph_id=graph_id, client=client, body=request)

  if response.status_code not in (200, 202):
    # Handle duplicate subgraph (already exists) — reuse it
    if response.status_code in (400, 409):
      content = response.content.decode() if response.content else ""
      if (
        "unique" in content.lower()
        or "already exists" in content.lower()
        or "duplicate" in content.lower()
      ):
        existing_id = f"{graph_id}_{name}"
        print(f"   ⚠️  Subgraph '{name}' already exists, reusing: {existing_id}")
        return existing_id
    print(f"❌ Failed to create subgraph: {response.status_code}")
    if response.content:
      print(f"   {response.content.decode()}")
    sys.exit(1)

  # Response is an OperationEnvelope — extract graph_id from result
  envelope = response.parsed
  result = getattr(envelope, "result", None) or {}
  if isinstance(result, dict):
    subgraph_id = result.get("graph_id")
  else:
    subgraph_id = getattr(result, "graph_id", None)

  if not subgraph_id:
    # Fallback: construct from convention
    subgraph_id = f"{graph_id}_{name}"

  print(f"   ✅ Memory subgraph created: {subgraph_id}")
  return subgraph_id


def list_subgraphs(client: AuthenticatedClient, graph_id: str) -> None:
  """List all subgraphs for the parent graph.

  Uses a raw GET rather than the generated ``list_subgraphs`` op: the installed
  client's SubgraphType enum predates ``knowledge`` and its response parser
  raises ValueError when it deserializes a knowledge subgraph. Parsing the JSON
  ourselves keeps the demo working until the client is regenerated.
  """
  print(f"\n📋 Listing subgraphs for {graph_id}...")

  httpx_client = client.get_httpx_client()
  response = httpx_client.get(f"/v1/graphs/{graph_id}/subgraphs")

  if response.status_code != 200:
    print(f"   ⚠️  Failed to list subgraphs: {response.status_code}")
    return

  payload = response.json()
  subgraphs = payload.get("subgraphs", [])
  print(f"   Total: {len(subgraphs)} subgraph(s)")
  for sg in subgraphs:
    sg_name = sg.get("subgraph_name", "?")
    sg_type = sg.get("subgraph_type", "?")
    sg_status = sg.get("status", "?")
    sg_id = sg.get("graph_id", "?")
    print(f"   • {sg_name} ({sg_type}) — {sg_status} — {sg_id}")


def execute_query(
  client: AuthenticatedClient, graph_id: str, query: str, description: str
) -> dict | None:
  """Execute a Cypher query and print results."""
  print(f"\n   🔍 {description}")

  request = CypherStatementRequest(query=query)
  response = api_execute_query(graph_id=graph_id, client=client, body=request)

  if response.status_code != 200:
    print(f"      ❌ Query failed: {response.status_code}")
    return None

  parsed = response.parsed

  rows: list = []
  columns: list = []

  # Try parsed response first, fall back to raw JSON content
  if parsed is None:
    # Generated client couldn't parse (e.g. chunked/NDJSON response format)
    try:
      content = (
        response.content.decode()
        if isinstance(response.content, bytes)
        else response.content
      )
      for line in content.strip().splitlines():
        chunk = json.loads(line)
        if "rows" in chunk:
          rows = chunk["rows"]
        elif "data" in chunk:
          rows = chunk["data"]
        if "columns" in chunk:
          columns = chunk["columns"]
    except (json.JSONDecodeError, TypeError) as e:
      print(f"      ⚠️  Could not parse response: {e}")
  elif isinstance(parsed, dict):
    rows = parsed.get("data", parsed.get("rows", []))
    columns = parsed.get("columns", [])
  else:
    raw_data = getattr(parsed, "data", UNSET)
    raw_cols = getattr(parsed, "columns", UNSET)
    if not isinstance(raw_data, type(UNSET)) and raw_data:
      for item in raw_data:
        if hasattr(item, "additional_properties"):
          rows.append(item.additional_properties)
        elif hasattr(item, "to_dict"):
          rows.append(item.to_dict())
        else:
          rows.append(item)
    if not isinstance(raw_cols, type(UNSET)) and raw_cols:
      columns = list(raw_cols)

  if columns:
    print(f"      Columns: {columns}")
  if not rows:
    print("      (no results)")
  for row in rows[:10]:
    print(f"      {row}")

  return parsed


def seed_memory_data(client: AuthenticatedClient, subgraph_id: str) -> None:
  """Seed the memory subgraph with sample concepts, observations, and a session.

  Every write is an idempotent MERGE keyed on the primary key. A plain CREATE
  is unsafe here: the first write to a freshly-created (cold) subgraph can be
  slow enough that the graph client's retry logic re-sends a statement that
  already committed, and the retried CREATE then fails with a duplicate
  primary-key error — cascading failures that trip the graph's circuit breaker.
  MERGE also makes the whole demo safely re-runnable.
  """
  print("\n📝 Seeding memory subgraph with sample data...")

  now = time.strftime("%Y-%m-%dT%H:%M:%SZ")

  # Create concepts
  concepts = [
    {
      "identifier": "concept_graph_databases",
      "name": "Graph Databases",
      "description": "Databases that use graph structures for semantic queries with nodes, edges, and properties",
      "category": "technology",
      "confidence": 0.95,
      "source": "knowledge_base",
      "created_at": now,
      "updated_at": now,
      "access_count": 5,
      "last_accessed": now,
    },
    {
      "identifier": "concept_cypher",
      "name": "Cypher Query Language",
      "description": "Declarative graph query language for pattern matching in property graphs",
      "category": "technology",
      "confidence": 0.90,
      "source": "documentation",
      "created_at": now,
      "updated_at": now,
      "access_count": 3,
      "last_accessed": now,
    },
    {
      "identifier": "concept_knowledge_graphs",
      "name": "Knowledge Graphs",
      "description": "Graph-structured knowledge bases that integrate information from multiple sources",
      "category": "concept",
      "confidence": 0.85,
      "source": "research",
      "created_at": now,
      "updated_at": now,
      "access_count": 2,
      "last_accessed": now,
    },
  ]

  for concept in concepts:
    query = """
MERGE (c:Concept {identifier: $identifier})
SET c.name = $name,
    c.description = $description,
    c.category = $category,
    c.confidence = $confidence,
    c.source = $source,
    c.created_at = $created_at,
    c.updated_at = $updated_at,
    c.access_count = $access_count,
    c.last_accessed = $last_accessed
RETURN c.name AS name
"""
    request = CypherStatementRequest(query=query, parameters=make_params(concept))
    response = api_execute_query(graph_id=subgraph_id, client=client, body=request)
    if response.status_code == 200:
      print(f"   ✅ Concept: {concept['name']}")
    else:
      print(f"   ❌ Failed to create concept {concept['name']}: {response.status_code}")

  # Create observations
  observations = [
    {
      "identifier": "obs_ladybug_perf",
      "content": "LadybugDB handles concurrent read queries efficiently with connection pooling (default pool size 3)",
      "observation_type": "performance",
      "context": "load testing session",
      "confidence": 0.88,
      "created_at": now,
      "expires_at": "",
    },
    {
      "identifier": "obs_schema_ext",
      "content": "Schema extensions allow dynamic addition of node and relationship types without database migration",
      "observation_type": "architecture",
      "context": "schema review",
      "confidence": 0.92,
      "created_at": now,
      "expires_at": "",
    },
  ]

  for obs in observations:
    query = """
MERGE (o:Observation {identifier: $identifier})
SET o.content = $content,
    o.observation_type = $observation_type,
    o.context = $context,
    o.confidence = $confidence,
    o.created_at = $created_at,
    o.expires_at = $expires_at
RETURN o.identifier AS id
"""
    request = CypherStatementRequest(query=query, parameters=make_params(obs))
    response = api_execute_query(graph_id=subgraph_id, client=client, body=request)
    if response.status_code == 200:
      print(f"   ✅ Observation: {obs['identifier']}")
    else:
      print(
        f"   ❌ Failed to create observation {obs['identifier']}: {response.status_code}"
      )

  # Create a session
  session = {
    "identifier": "session_demo_001",
    "name": "Memory Demo Session",
    "purpose": "Demonstrate memory subgraph capabilities",
    "started_at": now,
    "ended_at": "",
    "status": "active",
  }

  query = """
MERGE (s:Session {identifier: $identifier})
SET s.name = $name,
    s.purpose = $purpose,
    s.started_at = $started_at,
    s.ended_at = $ended_at,
    s.status = $status
RETURN s.name AS name
"""
  request = CypherStatementRequest(query=query, parameters=make_params(session))
  response = api_execute_query(graph_id=subgraph_id, client=client, body=request)
  if response.status_code == 200:
    print(f"   ✅ Session: {session['name']}")
  else:
    print(f"   ❌ Failed to create session: {response.status_code}")

  # Create relationships
  print("\n🔗 Creating relationships...")

  relationships = [
    {
      "query": """
MATCH (a:Concept {identifier: 'concept_cypher'}),
      (b:Concept {identifier: 'concept_graph_databases'})
MERGE (a)-[r:CONCEPT_RELATES_TO]->(b)
SET r.relationship_type = 'used_by',
    r.strength = 0.95,
    r.created_at = $now
RETURN a.name, b.name
""",
      "description": "Cypher -> Graph Databases",
    },
    {
      "query": """
MATCH (a:Concept {identifier: 'concept_knowledge_graphs'}),
      (b:Concept {identifier: 'concept_graph_databases'})
MERGE (a)-[r:CONCEPT_RELATES_TO]->(b)
SET r.relationship_type = 'built_on',
    r.strength = 0.80,
    r.created_at = $now
RETURN a.name, b.name
""",
      "description": "Knowledge Graphs -> Graph Databases",
    },
    {
      "query": """
MATCH (o:Observation {identifier: 'obs_ladybug_perf'}),
      (c:Concept {identifier: 'concept_graph_databases'})
MERGE (o)-[r:OBSERVATION_ABOUT]->(c)
SET r.relevance = 0.90,
    r.created_at = $now
RETURN o.identifier, c.name
""",
      "description": "Performance obs -> Graph Databases",
    },
    {
      "query": """
MATCH (o:Observation {identifier: 'obs_schema_ext'}),
      (c:Concept {identifier: 'concept_knowledge_graphs'})
MERGE (o)-[r:OBSERVATION_ABOUT]->(c)
SET r.relevance = 0.85,
    r.created_at = $now
RETURN o.identifier, c.name
""",
      "description": "Schema obs -> Knowledge Graphs",
    },
    {
      "query": """
MATCH (s:Session {identifier: 'session_demo_001'}),
      (o:Observation {identifier: 'obs_ladybug_perf'})
MERGE (s)-[r:SESSION_PRODUCED]->(o)
SET r.created_at = $now
RETURN s.name, o.identifier
""",
      "description": "Session -> Performance observation",
    },
    {
      "query": """
MATCH (s:Session {identifier: 'session_demo_001'}),
      (c:Concept {identifier: 'concept_graph_databases'})
MERGE (s)-[r:SESSION_REFERENCED]->(c)
SET r.usage_type = 'discussed',
    r.created_at = $now
RETURN s.name, c.name
""",
      "description": "Session -> Graph Databases concept",
    },
  ]

  for rel in relationships:
    request = CypherStatementRequest(
      query=rel["query"], parameters=make_params({"now": now})
    )
    response = api_execute_query(graph_id=subgraph_id, client=client, body=request)
    if response.status_code == 200:
      print(f"   ✅ {rel['description']}")
    else:
      print(f"   ❌ {rel['description']}: {response.status_code}")


def verify_memory_graph(client: AuthenticatedClient, subgraph_id: str) -> None:
  """Run verification queries against the memory subgraph."""
  print("\n" + "=" * 70)
  print("🔍 Verifying Memory Subgraph")
  print("=" * 70)

  execute_query(
    client,
    subgraph_id,
    "MATCH (n) WITH labels(n) AS label, count(n) AS count RETURN label, count ORDER BY count DESC",
    "Node counts by type",
  )

  execute_query(
    client,
    subgraph_id,
    "MATCH ()-[r]->() RETURN count(r) AS total_relationships",
    "Total relationship count",
  )

  execute_query(
    client,
    subgraph_id,
    "MATCH (c:Concept) RETURN c.name AS concept, c.category AS category, c.confidence AS confidence ORDER BY c.confidence DESC",
    "All concepts (ordered by confidence)",
  )

  execute_query(
    client,
    subgraph_id,
    """
MATCH (s:Session)-[:SESSION_PRODUCED]->(o:Observation)-[:OBSERVATION_ABOUT]->(c:Concept)
RETURN s.name AS session, o.content AS observation, c.name AS concept
""",
    "Session -> Observation -> Concept path",
  )

  execute_query(
    client,
    subgraph_id,
    """
MATCH (a:Concept)-[r:CONCEPT_RELATES_TO]->(b:Concept)
RETURN a.name AS from_concept, r.relationship_type AS relation, b.name AS to_concept, r.strength AS strength
""",
    "Concept relationships",
  )


def _envelope_result(parsed) -> dict:
  """Pull the result payload out of an OperationEnvelope response."""
  result = getattr(parsed, "result", None)
  if result is None and isinstance(parsed, dict):
    result = parsed.get("result")
  if hasattr(result, "to_dict"):
    return result.to_dict()
  if hasattr(result, "additional_properties"):
    return dict(result.additional_properties)
  return result if isinstance(result, dict) else {}


def seed_semantic_memories(client: AuthenticatedClient, subgraph_id: str) -> None:
  """Store free-text memories via the `remember` content operation.

  Unlike the structured Concept/Observation graph above, these are embedded
  locally into a per-graph vector store and retrieved by meaning, not by
  exact key or traversal.
  """
  print("\n" + "=" * 70)
  print("🧠 Semantic Memory — remember (vector store)")
  print("=" * 70)

  memories = [
    {
      "text": "LadybugDB is the platform's embedded columnar graph database; it uses a default connection pool size of 3 in local development.",
      "memory_type": "fact",
      "tags": ["ladybugdb", "infrastructure"],
    },
    {
      "text": "Knowledge subgraphs start from the 'knowledge' schema extension with Concept, Observation, and Session node types, which agents can extend at runtime.",
      "memory_type": "fact",
      "tags": ["knowledge", "schema"],
    },
    {
      "text": "The user prefers Cypher for graph traversal and reserves SQL for staging-table work before materialization.",
      "memory_type": "preference",
      "tags": ["user", "querying"],
    },
    {
      "text": "Semantic recall ranks memories by embedding similarity, so a query about 'the graph engine' should surface the LadybugDB note even without matching keywords.",
      "memory_type": "note",
      "tags": ["recall", "vectors"],
    },
  ]

  for mem in memories:
    body = RememberOp(
      text=mem["text"],
      source="custom_graph_demo",
      memory_type=mem["memory_type"],
      tags=mem["tags"],
    )
    response = api_remember(graph_id=subgraph_id, client=client, body=body)
    if response.status_code in (200, 202):
      result = _envelope_result(response.parsed)
      mem_id = result.get("id", "?") if isinstance(result, dict) else "?"
      print(f"   ✅ remembered [{mem['memory_type']}] {mem_id}: {mem['text'][:60]}…")
    else:
      print(f"   ❌ remember failed: {response.status_code}")
      if response.content:
        print(f"      {response.content.decode()[:200]}")


def recall_semantic_memories(client: AuthenticatedClient, subgraph_id: str) -> None:
  """Retrieve memories by meaning (`recall`) and enumerate them (`list-memories`)."""
  print("\n" + "=" * 70)
  print("🔎 Semantic Memory — recall (ranked by meaning)")
  print("=" * 70)

  queries = [
    "What graph engine does the platform run on?",
    "How does the user like to write queries?",
  ]

  for query in queries:
    print(f"\n   🔍 recall: {query!r}")
    body = MemoryRecallRequest(query=query, k=3)
    response = api_recall_memory(graph_id=subgraph_id, client=client, body=body)
    if response.status_code != 200:
      print(f"      ❌ recall failed: {response.status_code}")
      if response.content:
        print(f"      {response.content.decode()[:200]}")
      continue

    parsed = response.parsed
    hits = getattr(parsed, "hits", None)
    if hits is None and isinstance(parsed, dict):
      hits = parsed.get("hits", [])
    if not hits:
      print("      (no matches)")
      continue
    for hit in hits:
      score = getattr(hit, "score", None)
      snippet = getattr(hit, "snippet", None)
      if isinstance(hit, dict):
        score = hit.get("score")
        snippet = hit.get("snippet")
      score_str = f"{score:.3f}" if isinstance(score, (int, float)) else "?"
      print(f"      [{score_str}] {str(snippet)[:80]}…")

  # Governance view: enumerate every memory stored on the subgraph.
  print("\n   📋 list-memories (governance enumeration)")
  response = api_list_memories(graph_id=subgraph_id, client=client, limit=100)
  if response.status_code != 200:
    print(f"      ⚠️  list-memories failed: {response.status_code}")
    return
  parsed = response.parsed
  total = getattr(parsed, "total", None)
  records = getattr(parsed, "memories", None)
  if isinstance(parsed, dict):
    total = parsed.get("total")
    records = parsed.get("memories", [])
  print(f"      Total memories: {total}")
  for rec in records or []:
    mem_type = getattr(rec, "memory_type", "?")
    text = getattr(rec, "text", "")
    if isinstance(rec, dict):
      mem_type = rec.get("memory_type", "?")
      text = rec.get("text", "")
    print(f"      • [{mem_type}] {str(text)[:70]}…")


def main():
  parser = argparse.ArgumentParser(description="Create and seed a memory subgraph")
  parser.add_argument(
    "--base-url",
    default=os.environ.get("ROBOSYSTEMS_API_URL", "http://localhost:8000"),
    help="API base URL (default: $ROBOSYSTEMS_API_URL or http://localhost:8000)",
  )
  parser.add_argument(
    "--name",
    default="memory",
    help="Subgraph name (default: memory, alphanumeric only)",
  )
  parser.add_argument(
    "--credentials-file",
    default=str(CREDENTIALS_FILE),
    help="Path to credentials file",
  )

  args = parser.parse_args()
  credentials_path = Path(args.credentials_file).expanduser()

  credentials = load_credentials(credentials_path)
  api_key = credentials.get("api_key")
  graph_id = get_graph_id(credentials_path, DEMO_NAME)

  if not api_key or not graph_id:
    print("❌ Missing API key or graph ID. Run previous demo steps first.")
    sys.exit(1)

  client = build_client(args.base_url, api_key)

  print("\n" + "=" * 70)
  print("🧠 Memory Subgraph Demo")
  print("=" * 70)
  print(f"Parent Graph: {graph_id}")
  print(f"Subgraph Name: {args.name}")
  print("=" * 70)

  # Create the knowledge subgraph
  subgraph_id = create_memory_subgraph(client, graph_id, args.name)

  # List all subgraphs
  list_subgraphs(client, graph_id)

  # --- Modality 1: structured knowledge graph (exact Cypher traversal) ---
  seed_memory_data(client, subgraph_id)
  verify_memory_graph(client, subgraph_id)

  # --- Modality 2: semantic memory (ranked vector recall) ---
  seed_semantic_memories(client, subgraph_id)
  recall_semantic_memories(client, subgraph_id)

  print("\n" + "=" * 70)
  print("✅ Memory Subgraph Demo Complete!")
  print("=" * 70)
  print(f"\nParent Graph: {graph_id}")
  print(f"Knowledge Subgraph: {subgraph_id}")
  print("\n💡 This subgraph now holds both memory modalities:")
  print(
    '   • Structured graph  → uv run query_graph.py --query "MATCH (c:Concept) RETURN c.name"'
  )
  print(f"   • Semantic recall   → POST /v1/graphs/{subgraph_id}/memory/recall")
  print("=" * 70 + "\n")


if __name__ == "__main__":
  main()
