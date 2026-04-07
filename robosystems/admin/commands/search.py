"""OpenSearch admin commands for querying prod/staging indexes."""

import base64
import json
import os
import subprocess
import textwrap

import click
from rich.console import Console
from rich.table import Table

from ..ssm_executor import SSMExecutor

console = Console()

# Self-contained Python script for bastion execution.
# Uses only stdlib (no boto3) — manual SigV4 signing via IMDSv2 credentials.
_OPENSEARCH_QUERY_SCRIPT = """
import json, urllib.request, ssl, hmac, hashlib, datetime, sys, os, base64

host = "{host}"
region = "{region}"
index_name = "{index}"

# IMDSv2 credentials
token_req = urllib.request.Request(
    "http://169.254.169.254/latest/api/token",
    method="PUT",
    headers={{"X-aws-ec2-metadata-token-ttl-seconds": "21600"}},
)
token = urllib.request.urlopen(token_req).read().decode()
meta_h = {{"X-aws-ec2-metadata-token": token}}

role_req = urllib.request.Request(
    "http://169.254.169.254/latest/meta-data/iam/security-credentials/",
    headers=meta_h,
)
role = urllib.request.urlopen(role_req).read().decode().strip()

cred_req = urllib.request.Request(
    f"http://169.254.169.254/latest/meta-data/iam/security-credentials/{{role}}",
    headers=meta_h,
)
creds = json.loads(urllib.request.urlopen(cred_req).read().decode())

access_key = creds["AccessKeyId"]
secret_key = creds["SecretAccessKey"]
session_token = creds["Token"]


def sign(key, msg):
    return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()


def get_signature_key(key, date_stamp, rn, sn):
    return sign(sign(sign(sign(("AWS4" + key).encode("utf-8"), date_stamp), rn), sn), "aws4_request")


def query_os(path, body=None, method="POST"):
    import urllib.parse
    has_body = body is not None
    data = json.dumps(body) if has_body else ""
    t = datetime.datetime.utcnow()
    amz_date = t.strftime("%Y%m%dT%H%M%SZ")
    date_stamp = t.strftime("%Y%m%d")

    # Split path and query string for proper SigV4 signing
    if "?" in path:
        canonical_uri, qs = path.split("?", 1)
        canonical_qs = urllib.parse.urlencode(sorted(urllib.parse.parse_qsl(qs)))
    else:
        canonical_uri = path
        canonical_qs = ""

    if has_body:
        canonical_headers = (
            f"content-type:application/json\\nhost:{{host}}\\n"
            f"x-amz-date:{{amz_date}}\\nx-amz-security-token:{{session_token}}\\n"
        )
        signed_headers = "content-type;host;x-amz-date;x-amz-security-token"
    else:
        canonical_headers = (
            f"host:{{host}}\\n"
            f"x-amz-date:{{amz_date}}\\nx-amz-security-token:{{session_token}}\\n"
        )
        signed_headers = "host;x-amz-date;x-amz-security-token"
    payload_hash = hashlib.sha256(data.encode("utf-8")).hexdigest()
    canonical_request = f"{{method}}\\n{{canonical_uri}}\\n{{canonical_qs}}\\n{{canonical_headers}}\\n{{signed_headers}}\\n{{payload_hash}}"

    credential_scope = f"{{date_stamp}}/{{region}}/es/aws4_request"
    string_to_sign = (
        f"AWS4-HMAC-SHA256\\n{{amz_date}}\\n{{credential_scope}}\\n"
        f"{{hashlib.sha256(canonical_request.encode('utf-8')).hexdigest()}}"
    )

    signing_key = get_signature_key(secret_key, date_stamp, region, "es")
    signature = hmac.new(signing_key, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()

    auth = (
        f"AWS4-HMAC-SHA256 Credential={{access_key}}/{{credential_scope}}, "
        f"SignedHeaders={{signed_headers}}, Signature={{signature}}"
    )

    headers = {{
        "X-Amz-Date": amz_date,
        "X-Amz-Security-Token": session_token,
        "Authorization": auth,
    }}
    if has_body:
        headers["Content-Type"] = "application/json"

    url = "https://" + host + path
    req = urllib.request.Request(url, data=data.encode() if has_body else None, headers=headers, method=method)
    resp = urllib.request.urlopen(req, context=ssl.create_default_context())
    return json.loads(resp.read().decode())


action = "{action}"
graph_id = "{graph_id}"
source_type = "{source_type}"
dry_run = "{dry_run}" == "true"

if action == "count":
    total = query_os(f"/{{index_name}}/_count", {{"query": {{"term": {{"graph_id": graph_id}}}}}})
    emb = query_os(f"/{{index_name}}/_count", {{"query": {{"bool": {{"must": [
        {{"term": {{"graph_id": graph_id}}}},
        {{"exists": {{"field": "embedding"}}}}
    ]}}}}}})
    aggs = query_os(f"/{{index_name}}/_search", {{
        "size": 0,
        "query": {{"term": {{"graph_id": graph_id}}}},
        "aggs": {{
            "by_source": {{"terms": {{"field": "source_type", "size": 10}}}},
            "with_embeddings": {{
                "filter": {{"exists": {{"field": "embedding"}}}},
                "aggs": {{"by_source": {{"terms": {{"field": "source_type", "size": 10}}}}}}
            }},
            "by_entity": {{"terms": {{"field": "entity_ticker", "size": 20}}}},
            "by_form": {{"terms": {{"field": "form_type", "size": 10}}}},
        }}
    }})
    result = {{
        "total": total["count"],
        "with_embeddings": emb["count"],
        "aggregations": aggs["aggregations"],
    }}
    print(json.dumps(result))

elif action == "search":
    query_text = base64.b64decode(os.environ.get("QUERY_TEXT_B64", "")).decode("utf-8")
    size = {size}
    search_body = {{
        "query": {{
            "bool": {{
                "should": [{{
                    "multi_match": {{
                        "query": query_text,
                        "fields": ["content", "section_label^2", "entity_name^1.5"],
                        "type": "best_fields",
                    }}
                }}],
                "minimum_should_match": 1,
                "filter": [{{"term": {{"graph_id": graph_id}}}}],
            }}
        }},
        "highlight": {{"fields": {{"content": {{"fragment_size": 200, "number_of_fragments": 2}}}}}},
        "size": size,
        "_source": [
            "entity_ticker", "entity_name", "source_type", "section_label",
            "form_type", "fiscal_year", "filing_date", "content_length",
            "embedding_model",
        ],
    }}
    result = query_os(f"/{{index_name}}/_search", search_body)
    print(json.dumps({{
        "total": result["hits"]["total"]["value"],
        "hits": result["hits"]["hits"],
    }}))

elif action == "delete":
    # Build query: filter by graph_id + optional source_type
    filters = [{{"term": {{"graph_id": graph_id}}}}]
    if source_type:
        filters.append({{"term": {{"source_type": source_type}}}})

    query = {{"query": {{"bool": {{"filter": filters}}}}}}

    # Count matching documents
    count = query_os(f"/{{index_name}}/_count", query)
    doc_count = count["count"]

    if doc_count == 0 or dry_run:
        print(json.dumps({{"count": doc_count, "deleted": 0, "task": None}}))
    else:
        # Async delete-by-query
        result = query_os(f"/{{index_name}}/_delete_by_query?wait_for_completion=false", query)
        print(json.dumps({{"count": doc_count, "deleted": doc_count, "task": result.get("task")}}))

elif action == "drop-index":
    # Force merge to 1 segment purges deleted doc tombstones and frees HNSW memory
    result = query_os(f"/{{index_name}}/_forcemerge?max_num_segments=1", {{}})
    print(json.dumps(result))
"""


def _get_opensearch_endpoint(environment: str, aws_profile: str) -> str:
  """Get OpenSearch VPC endpoint from CloudFormation stack."""
  stack_name = f"RoboSystemsOpenSearch{environment.capitalize()}"
  cmd = [
    "aws",
    "cloudformation",
    "describe-stacks",
    "--stack-name",
    stack_name,
    "--query",
    "Stacks[0].Outputs[?OutputKey==`OpenSearchEndpoint`].OutputValue",
    "--output",
    "text",
    "--profile",
    aws_profile,
    "--region",
    "us-east-1",
  ]
  # Strip AWS_ENDPOINT_URL to avoid hitting LocalStack when run locally
  sub_env = {k: v for k, v in os.environ.items() if k != "AWS_ENDPOINT_URL"}
  result = subprocess.run(cmd, capture_output=True, text=True, check=True, env=sub_env)
  endpoint = result.stdout.strip()
  if not endpoint or endpoint == "None":
    raise click.ClickException(f"OpenSearch endpoint not found for {environment}")
  # Strip https:// prefix — we need just the hostname
  return endpoint.replace("https://", "").replace("http://", "").rstrip("/")


def _run_opensearch_script(
  client,
  action: str,
  graph_id: str = "sec",
  query_text: str = "",
  size: int = 10,
  source_type: str = "",
  dry_run: bool = False,
) -> dict:
  """Run OpenSearch query script on bastion via SSM."""
  endpoint = _get_opensearch_endpoint(client.environment, client.aws_profile)

  script = _OPENSEARCH_QUERY_SCRIPT.format(
    host=endpoint,
    region="us-east-1",
    index="documents",
    action=action,
    graph_id=graph_id,
    size=size,
    source_type=source_type,
    dry_run="true" if dry_run else "false",
  )

  # Base64 encode script and query text to avoid shell quoting issues.
  # Query text is passed as env var to prevent template injection.
  script_b64 = base64.b64encode(script.encode()).decode()
  query_b64 = base64.b64encode(query_text.encode()).decode()

  executor = SSMExecutor(client.environment, aws_profile=client.aws_profile)
  stdout, stderr, _ = executor.execute(
    f"export QUERY_TEXT_B64={query_b64} && echo {script_b64} | base64 -d | python3",
    stream_output=False,
  )

  try:
    return json.loads(stdout.strip())
  except json.JSONDecodeError:
    raise click.ClickException(
      f"Failed to parse OpenSearch response:\n{stdout}\n{stderr}"
    )


@click.group()
def search():
  """OpenSearch index operations."""
  pass


@search.command("count")
@click.option("--graph-id", default="sec", help="Graph ID (default: sec)")
@click.pass_obj
def search_count(client, graph_id):
  """Show document count and embedding stats."""
  data = _run_opensearch_script(client, action="count", graph_id=graph_id)

  total = data["total"]
  with_emb = data["with_embeddings"]
  aggs = data["aggregations"]

  console.print(
    f"\n[bold cyan]OPENSEARCH INDEX STATS[/bold cyan] (graph_id={graph_id})"
  )
  console.print("=" * 60)
  console.print(f"\n[bold]Total documents:[/bold] {total:,}")
  console.print(
    f"[bold]With embeddings:[/bold] {with_emb:,} ({with_emb / total * 100:.1f}%)"
    if total
    else ""
  )

  # By source type
  if aggs.get("by_source", {}).get("buckets"):
    console.print("\n[bold]By source type:[/bold]")
    table = Table(show_header=True)
    table.add_column("Source Type")
    table.add_column("Total", justify="right")
    table.add_column("With Embeddings", justify="right")
    table.add_column("Coverage", justify="right")

    emb_by_source = {}
    for b in aggs.get("with_embeddings", {}).get("by_source", {}).get("buckets", []):
      emb_by_source[b["key"]] = b["doc_count"]

    for b in aggs["by_source"]["buckets"]:
      source = b["key"]
      count = b["doc_count"]
      emb_count = emb_by_source.get(source, 0)
      pct = f"{emb_count / count * 100:.1f}%" if count else "0%"
      table.add_row(source, f"{count:,}", f"{emb_count:,}", pct)

    console.print(table)

  # By entity
  if aggs.get("by_entity", {}).get("buckets"):
    console.print("\n[bold]Top entities:[/bold]")
    for b in aggs["by_entity"]["buckets"][:10]:
      console.print(f"  {b['key']}: {b['doc_count']:,}")

  # By form type
  if aggs.get("by_form", {}).get("buckets"):
    console.print("\n[bold]By form type:[/bold]")
    for b in aggs["by_form"]["buckets"]:
      console.print(f"  {b['key']}: {b['doc_count']:,}")

  console.print()


@search.command("query")
@click.argument("query_text")
@click.option("--graph-id", default="sec", help="Graph ID (default: sec)")
@click.option("--size", default=10, help="Max results (default: 10)")
@click.option("--json-output", is_flag=True, help="Output raw JSON")
@click.pass_obj
def search_query(client, query_text, graph_id, size, json_output):
  """Search OpenSearch documents with a text query."""
  data = _run_opensearch_script(
    client,
    action="search",
    graph_id=graph_id,
    query_text=query_text,
    size=size,
  )

  if json_output:
    console.print(json.dumps(data, indent=2))
    return

  total = data["total"]
  hits = data["hits"]

  console.print(f"\n{total} results (showing {len(hits)}):\n")

  if not hits:
    console.print("No results found.")
    return

  for i, hit in enumerate(hits, 1):
    src = hit["_source"]
    score = hit["_score"]
    ticker = src.get("entity_ticker", "")
    form = src.get("form_type", "")
    fy = src.get("fiscal_year", "")
    source = src.get("source_type", "")
    label = src.get("section_label", "")
    length = src.get("content_length", 0)
    date = src.get("filing_date", "")
    has_emb = "+" if src.get("embedding_model") else "-"

    console.print(f"  {i}. [{score:.2f}] {ticker} {form} FY{fy} -- {label}")
    console.print(f"     {source} | {date} | {length:,} chars | emb:{has_emb}")

    highlights = hit.get("highlight", {}).get("content", [])
    if highlights:
      snippet = highlights[0].replace("\n", " ")
      console.print(f"     {textwrap.shorten(snippet, width=120, placeholder='...')}")
    console.print()


@search.command("delete")
@click.argument("source_type")
@click.option("--graph-id", default="sec", help="Graph ID (default: sec)")
@click.option("--force", is_flag=True, help="Skip confirmation prompt")
@click.pass_obj
def search_delete(client, source_type, graph_id, force):
  """Delete documents by source_type from the index.

  Runs an async delete-by-query on OpenSearch. Use 'search count' to monitor progress.

  Examples:

    just admin prod search delete xbrl_textblock

    just admin prod search delete ixbrl_disclosure --graph-id sec

    just admin prod search delete narrative_section --force
  """
  # Dry run to get count
  data = _run_opensearch_script(
    client,
    action="delete",
    graph_id=graph_id,
    source_type=source_type,
    dry_run=True,
  )

  count = data.get("count", 0)
  if count == 0:
    console.print(
      f"\n[yellow]No documents found with source_type={source_type} in graph_id={graph_id}[/yellow]\n"
    )
    return

  if not force:
    console.print(
      f"\n[bold red]Will delete {count:,} documents[/bold red] "
      f"(source_type={source_type}, graph_id={graph_id})"
    )
    if not click.confirm("Proceed?"):
      console.print("[dim]Cancelled.[/dim]")
      return

  # Execute the delete
  data = _run_opensearch_script(
    client,
    action="delete",
    graph_id=graph_id,
    source_type=source_type,
  )

  task_id = data.get("task")
  deleted = data.get("deleted", 0)

  console.print(f"\n[green]Delete task started[/green] for {deleted:,} documents")
  if task_id:
    console.print(f"[dim]Task ID: {task_id}[/dim]")
  console.print(
    f"\nRun [bold]just admin {client.environment} search count --graph-id {graph_id}[/bold] to monitor progress.\n"
  )


@search.command("drop-index")
@click.option("--force", is_flag=True, help="Skip confirmation prompt")
@click.pass_obj
def search_drop_index(client, force):
  """Drop and recreate the entire OpenSearch index.

  Frees all memory including deleted segment tombstones. The index is
  automatically recreated with the correct mapping on the next indexing run.

  Examples:

    just admin prod search drop-index

    just admin prod search drop-index --force
  """
  if not force:
    console.print(
      "\n[bold red]This will DELETE the entire 'documents' index[/bold red] "
      f"on {client.environment}, including all source types."
    )
    if not click.confirm("Proceed?"):
      console.print("[dim]Cancelled.[/dim]")
      return

  data = _run_opensearch_script(client, action="drop-index")
  acknowledged = data.get("acknowledged", False)

  if acknowledged:
    console.print("\n[green]Index dropped successfully.[/green]")
    console.print("It will be recreated automatically on the next indexing run.\n")
  else:
    console.print(f"\n[red]Unexpected response:[/red] {data}\n")
