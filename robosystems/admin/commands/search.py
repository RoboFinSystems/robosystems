"""OpenSearch admin commands for querying prod/staging indexes."""

import base64
import json
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
import json, urllib.request, ssl, hmac, hashlib, datetime, sys

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


def query_os(path, body):
    url = "https://" + host + path
    data = json.dumps(body)
    t = datetime.datetime.utcnow()
    amz_date = t.strftime("%Y%m%dT%H%M%SZ")
    date_stamp = t.strftime("%Y%m%d")

    canonical_headers = (
        f"content-type:application/json\\nhost:{{host}}\\n"
        f"x-amz-date:{{amz_date}}\\nx-amz-security-token:{{session_token}}\\n"
    )
    signed_headers = "content-type;host;x-amz-date;x-amz-security-token"
    payload_hash = hashlib.sha256(data.encode("utf-8")).hexdigest()
    canonical_request = f"POST\\n{{path}}\\n\\n{{canonical_headers}}\\n{{signed_headers}}\\n{{payload_hash}}"

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
        "Content-Type": "application/json",
        "X-Amz-Date": amz_date,
        "X-Amz-Security-Token": session_token,
        "Authorization": auth,
    }}

    req = urllib.request.Request(url, data=data.encode(), headers=headers)
    ctx = ssl._create_unverified_context()
    resp = urllib.request.urlopen(req, context=ctx)
    return json.loads(resp.read().decode())


action = "{action}"
graph_id = "{graph_id}"

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
    query_text = "{query_text}"
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
  result = subprocess.run(cmd, capture_output=True, text=True, check=True)
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
) -> dict:
  """Run OpenSearch query script on bastion via SSM."""
  endpoint = _get_opensearch_endpoint(client.environment, client.aws_profile)

  script = _OPENSEARCH_QUERY_SCRIPT.format(
    host=endpoint,
    region="us-east-1",
    index="documents",
    action=action,
    graph_id=graph_id,
    query_text=query_text.replace('"', '\\"'),
    size=size,
  )

  # Base64 encode to avoid shell quoting issues
  script_b64 = base64.b64encode(script.encode()).decode()

  executor = SSMExecutor(client.environment, aws_profile=client.aws_profile)
  stdout, stderr, _ = executor.execute(
    f"echo {script_b64} | base64 -d | python3",
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
