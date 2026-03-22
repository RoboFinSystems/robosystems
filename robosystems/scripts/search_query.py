#!/usr/bin/env python3
"""
OpenSearch Query Tool

Command-line tool to search SEC filing text content indexed in OpenSearch.

Usage:
    # Basic search
    python -m robosystems.scripts.search_query --graph-id sec "revenue growth"

    # Filter by entity
    python -m robosystems.scripts.search_query --graph-id sec --entity NVDA "risk factors"

    # Filter by form type and fiscal year
    python -m robosystems.scripts.search_query --graph-id sec --form-type 10-K --fiscal-year 2025 "inventory"

    # Filter by source type
    python -m robosystems.scripts.search_query --graph-id sec --source-type narrative_section "business overview"

    # JSON output
    python -m robosystems.scripts.search_query --graph-id sec --format json "revenue"

    # Show document count (no query)
    python -m robosystems.scripts.search_query --graph-id sec --count
"""

import argparse
import json
import sys
import textwrap

from opensearchpy import OpenSearch

from robosystems.config import env


def main():
  parser = argparse.ArgumentParser(description="Search OpenSearch documents")
  parser.add_argument("query", nargs="?", default=None, help="Search query text")
  parser.add_argument(
    "--graph-id", type=str, default="sec", help="Graph ID (default: sec)"
  )
  parser.add_argument("--url", type=str, default=None, help="OpenSearch URL override")
  parser.add_argument("--index", type=str, default=None, help="Index name override")
  parser.add_argument("--entity", type=str, default=None, help="Filter by ticker/CIK")
  parser.add_argument("--form-type", type=str, default=None, help="Filter by form type")
  parser.add_argument(
    "--fiscal-year", type=int, default=None, help="Filter by fiscal year"
  )
  parser.add_argument(
    "--source-type",
    type=str,
    default=None,
    choices=["xbrl_textblock", "narrative_section", "ixbrl_disclosure"],
    help="Filter by source type",
  )
  parser.add_argument("--size", type=int, default=10, help="Max results (default: 10)")
  parser.add_argument(
    "--semantic",
    action=argparse.BooleanOptionalAction,
    default=False,
    help="Enable semantic (hybrid) search using vector embeddings",
  )
  parser.add_argument(
    "--format",
    type=str,
    default="table",
    choices=["table", "json"],
    help="Output format",
  )
  parser.add_argument(
    "--count", action="store_true", help="Show document count (no query needed)"
  )

  args = parser.parse_args()

  if not args.query and not args.count:
    parser.error("Either provide a search query or use --count")

  os_url = args.url or env.OPENSEARCH_URL
  index_name = args.index or env.OPENSEARCH_INDEX

  client = OpenSearch(
    hosts=[os_url],
    use_ssl=os_url.startswith("https"),
    verify_certs=False,
    ssl_show_warn=False,
  )

  # Count mode
  if args.count:
    try:
      result = client.count(
        index=index_name,
        body={"query": {"term": {"graph_id": args.graph_id}}},
      )
      total = result["count"]

      # Get breakdown by source_type
      agg_result = client.search(
        index=index_name,
        body={
          "size": 0,
          "query": {"term": {"graph_id": args.graph_id}},
          "aggs": {
            "by_source": {"terms": {"field": "source_type", "size": 10}},
            "by_entity": {"terms": {"field": "entity_ticker", "size": 20}},
            "by_form": {"terms": {"field": "form_type", "size": 10}},
          },
        },
      )

      if args.format == "json":
        print(
          json.dumps({"total": total, "aggs": agg_result["aggregations"]}, indent=2)
        )
      else:
        print(
          f"\nDocuments in '{index_name}' for graph_id='{args.graph_id}': {total}\n"
        )
        aggs = agg_result["aggregations"]

        if aggs["by_source"]["buckets"]:
          print("By source type:")
          for b in aggs["by_source"]["buckets"]:
            print(f"  {b['key']}: {b['doc_count']}")

        if aggs["by_entity"]["buckets"]:
          print("\nBy entity:")
          for b in aggs["by_entity"]["buckets"]:
            print(f"  {b['key']}: {b['doc_count']}")

        if aggs["by_form"]["buckets"]:
          print("\nBy form type:")
          for b in aggs["by_form"]["buckets"]:
            print(f"  {b['key']}: {b['doc_count']}")

        print()
    except Exception as e:
      print(f"Error: {e}", file=sys.stderr)
      sys.exit(1)
    return

  # Search mode
  filter_clauses = [{"term": {"graph_id": args.graph_id}}]

  if args.entity:
    filter_clauses.append(
      {
        "bool": {
          "should": [
            {"term": {"entity_ticker": args.entity.upper()}},
            {"term": {"entity_cik": args.entity}},
            {"match": {"entity_name": args.entity}},
          ],
          "minimum_should_match": 1,
        }
      }
    )
  if args.form_type:
    filter_clauses.append({"term": {"form_type": args.form_type.upper()}})
  if args.fiscal_year:
    filter_clauses.append({"term": {"fiscal_year": args.fiscal_year}})
  if args.source_type:
    filter_clauses.append({"term": {"source_type": args.source_type}})

  text_clause = {
    "multi_match": {
      "query": args.query,
      "fields": ["content", "section_label^2", "entity_name^1.5"],
      "type": "best_fields",
    }
  }

  should_clauses = [text_clause]

  # Semantic search: embed the query and add KNN clause
  if args.semantic:
    try:
      from robosystems.adapters.sec.enrichment import SemanticEnricher

      enricher = SemanticEnricher()
      query_embedding = enricher.embed_batch([args.query])[0]
      should_clauses.append(
        {"knn": {"embedding": {"vector": query_embedding, "k": args.size}}}
      )
      print("[semantic search enabled]\n")
    except Exception as e:
      print(
        f"[semantic search unavailable: {e}, falling back to text]\n", file=sys.stderr
      )

  search_body = {
    "query": {
      "bool": {
        "should": should_clauses,
        "minimum_should_match": 1,
        "filter": filter_clauses,
      }
    },
    "highlight": {
      "fields": {
        "content": {
          "fragment_size": 200,
          "number_of_fragments": 2,
        }
      }
    },
    "size": args.size,
    "_source": [
      "entity_ticker",
      "entity_name",
      "source_type",
      "section_label",
      "form_type",
      "fiscal_year",
      "filing_date",
      "accession_number",
      "content_length",
    ],
  }

  try:
    result = client.search(index=index_name, body=search_body)
  except Exception as e:
    print(f"Error: {e}", file=sys.stderr)
    sys.exit(1)

  hits = result["hits"]["hits"]
  total = result["hits"]["total"]["value"]

  if args.format == "json":
    print(json.dumps({"total": total, "hits": hits}, indent=2))
    return

  print(f"\n{total} results (showing {len(hits)}):\n")

  if not hits:
    print("No results found.")
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

    print(f"  {i}. [{score:.2f}] {ticker} {form} FY{fy} — {label}")
    print(f"     {source} | {date} | {length:,} chars")

    highlights = hit.get("highlight", {}).get("content", [])
    if highlights:
      snippet = highlights[0].replace("\n", " ")
      print(f"     {textwrap.shorten(snippet, width=120, placeholder='...')}")
    print()


if __name__ == "__main__":
  main()
