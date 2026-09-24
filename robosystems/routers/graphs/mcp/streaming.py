"""MCP streaming generators, plus `aggregate_streamed_results`, which folds an
event list into the single result a JSON-RPC `tools/call` returns."""

import json
from collections.abc import AsyncGenerator
from datetime import UTC, datetime
from typing import Any

from robosystems.logger import logger
from robosystems.middleware.mcp import GraphQueryComplexityError

from .handlers import is_tool_error_result, tool_error_kind


async def stream_mcp_tool_execution(
  handler: Any,
  tool_name: str,
  arguments: dict[str, Any],
  strategy: str,
  chunk_size: int = 1000,
) -> AsyncGenerator[dict[str, Any]]:
  """Stream an MCP tool execution as progress and result events."""
  start_time = datetime.now(UTC)

  try:
    yield {
      "event": "start",
      "data": {
        "tool": tool_name,
        "strategy": strategy,
        "timestamp": start_time.isoformat(),
        "message": f"Starting {tool_name} execution",
      },
    }

    if tool_name in ["read-graph-cypher", "read-neo4j-cypher", "read-ladybug-cypher"]:
      async for event in stream_cypher_query(handler, arguments, chunk_size):
        yield event

    elif tool_name in ["get-graph-schema", "get-neo4j-schema", "get-ladybug-schema"]:
      async for event in stream_schema_retrieval(handler, tool_name, arguments):
        yield event

    else:
      yield {
        "event": "progress",
        "data": {
          "tool": tool_name,
          "message": f"Executing {tool_name}...",
          "progress": 50,
        },
      }

      result = await handler.call_tool(tool_name, arguments)

      # Marked failure results become error events so aggregation fails.
      if is_tool_error_result(result):
        yield {
          "event": "error",
          "data": {
            "tool": tool_name,
            "error": result.get("text", "Tool execution failed"),
            "error_kind": tool_error_kind(result),
          },
        }
        return

      yield {
        "event": "result",
        "data": {
          "tool": tool_name,
          "result": result,
        },
      }

    execution_time = (datetime.now(UTC) - start_time).total_seconds()
    yield {
      "event": "complete",
      "data": {
        "tool": tool_name,
        "execution_time_seconds": execution_time,
        "timestamp": datetime.now(UTC).isoformat(),
      },
    }

  except TimeoutError:
    yield {
      "event": "error",
      "data": {
        "tool": tool_name,
        "error": "Execution timeout",
        "message": f"Tool {tool_name} execution timed out",
        "error_kind": "timeout",
      },
    }
  except Exception as e:
    logger.error(f"Error streaming MCP tool {tool_name}: {e}")
    yield {
      "event": "error",
      "data": {
        "tool": tool_name,
        "error": str(e),
        "error_type": type(e).__name__,
        "error_kind": "backend",
      },
    }


async def stream_cypher_query(
  handler: Any,
  arguments: dict[str, Any],
  chunk_size: int = 1000,
) -> AsyncGenerator[dict[str, Any]]:
  """Stream Cypher query results in chunks of `chunk_size` rows."""
  query = arguments.get("query", "")
  parameters = arguments.get("parameters", {})

  if hasattr(handler, "execute_query_streaming"):
    total_rows = 0
    chunk_count = 0
    all_columns = None

    stream = handler.execute_query_streaming(query, parameters, chunk_size=chunk_size)
    try:
      first = await anext(stream, None)
    except (ValueError, GraphQueryComplexityError) as e:
      yield {
        "event": "error",
        "data": {
          "tool": "read-graph-cypher",
          "error": str(e),
          "error_kind": "constraint",
        },
      }
      return

    async def _chunks():
      if first is not None:
        yield first
      async for rest in stream:
        yield rest

    async for chunk in _chunks():
      # A failed backend query arrives as an error chunk; without an error
      # event the stream would aggregate to a successful empty result.
      if isinstance(chunk, dict) and chunk.get("error"):
        error_type = str(chunk.get("error_type", ""))
        yield {
          "event": "error",
          "data": {
            "tool": "read-graph-cypher",
            "error": str(chunk["error"]),
            "error_type": error_type,
            "error_kind": "timeout" if "Timeout" in error_type else "backend",
          },
        }
        return

      chunk_count += 1
      rows_in_chunk = len(chunk.get("data", []))
      total_rows += rows_in_chunk

      if all_columns is None and "columns" in chunk:
        all_columns = chunk["columns"]

      yield {
        "event": "query_chunk",
        "data": {
          "chunk_number": chunk_count,
          "rows_in_chunk": rows_in_chunk,
          "total_rows_so_far": total_rows,
          "columns": all_columns,
          "data": chunk.get("data", []),
        },
      }

      if chunk_count % 5 == 0:
        yield {
          "event": "progress",
          "data": {
            "message": f"Processed {total_rows} rows",
            "rows_processed": total_rows,
          },
        }

    yield {
      "event": "query_complete",
      "data": {
        "total_rows": total_rows,
        "total_chunks": chunk_count,
        "columns": all_columns,
      },
    }
  else:
    yield {
      "event": "progress",
      "data": {
        "message": "Executing query...",
        "progress": 50,
      },
    }

    result = await handler.call_tool("read-graph-cypher", arguments)

    if is_tool_error_result(result):
      yield {
        "event": "error",
        "data": {
          "tool": "read-graph-cypher",
          "error": result.get("text", "Query execution failed"),
          "error_kind": tool_error_kind(result),
        },
      }
      return

    if isinstance(result, dict) and "text" in result:
      try:
        parsed = json.loads(result["text"])
        yield {
          "event": "query_result",
          "data": {
            "result": parsed,
            "row_count": len(parsed) if isinstance(parsed, list) else 1,
          },
        }
      except json.JSONDecodeError:
        yield {
          "event": "query_result",
          "data": {
            "result": result["text"],
            "row_count": 0,
          },
        }
    else:
      yield {
        "event": "query_result",
        "data": {
          "result": result,
          "row_count": 0,
        },
      }


async def stream_schema_retrieval(
  handler: Any,
  tool_name: str,
  arguments: dict[str, Any],
) -> AsyncGenerator[dict[str, Any]]:
  """Stream schema information in parts, so large schemas arrive incrementally."""
  yield {
    "event": "progress",
    "data": {
      "message": "Retrieving graph schema...",
      "progress": 10,
    },
  }

  schema_result = await handler.call_tool(tool_name, arguments)

  if is_tool_error_result(schema_result):
    yield {
      "event": "error",
      "data": {
        "tool": tool_name,
        "error": schema_result.get("text", "Schema retrieval failed"),
        "error_kind": tool_error_kind(schema_result),
      },
    }
    return

  if isinstance(schema_result, dict) and "text" in schema_result:
    try:
      schema = json.loads(schema_result["text"])
    except json.JSONDecodeError:
      schema = {"raw": schema_result["text"]}
  else:
    schema = schema_result

  if isinstance(schema, list):
    node_tables = [t for t in schema if t.get("category") == "Node Tables"]
    rel_tables = [t for t in schema if t.get("category") == "Relationship Tables"]

    if node_tables:
      yield {
        "event": "schema_nodes",
        "data": {
          "node_count": len(node_tables),
          "node_tables": node_tables,
        },
      }

    yield {
      "event": "progress",
      "data": {
        "message": "Processing relationships...",
        "progress": 50,
      },
    }

    if rel_tables:
      yield {
        "event": "schema_relationships",
        "data": {
          "relationship_count": len(rel_tables),
          "relationship_tables": rel_tables,
        },
      }
  else:
    yield {
      "event": "schema_complete",
      "data": {
        "schema": schema,
      },
    }

  yield {
    "event": "progress",
    "data": {
      "message": "Schema retrieval complete",
      "progress": 100,
    },
  }


def aggregate_streamed_results(events: list[dict[str, Any]]) -> dict[str, Any]:
  """Fold a list of streaming events into one result.

  Returns a `success: False` shape carrying the error when the stream failed
  or ended without a terminal event.
  """
  tool_name = None
  for event in events:
    if event.get("event") == "start":
      tool_name = event["data"].get("tool")
      break

  for event in events:
    if event.get("event") == "error":
      failure: dict[str, Any] = {
        "success": False,
        "error": event["data"].get("error", "Unknown error"),
        "tool": tool_name,
      }
      # Keep the kind: timeout/backend count against the breaker, constraint doesn't.
      kind = event["data"].get("error_kind")
      if isinstance(kind, str):
        failure["error_kind"] = kind
      return failure

  if any(e.get("event") == "query_chunk" for e in events):
    all_rows = []
    columns = None

    for event in events:
      if event.get("event") == "query_chunk":
        data = event["data"]
        if columns is None and "columns" in data:
          columns = data["columns"]
        all_rows.extend(data.get("data", []))

    return {
      "success": True,
      "tool": tool_name,
      "result": {
        "columns": columns,
        "data": all_rows,
        "row_count": len(all_rows),
      },
    }

  elif any(e.get("event") == "schema_nodes" for e in events):
    schema = {
      "node_tables": [],
      "relationship_tables": [],
    }

    for event in events:
      if event.get("event") == "schema_nodes":
        schema["node_tables"] = event["data"].get("node_tables", [])
      elif event.get("event") == "schema_relationships":
        schema["relationship_tables"] = event["data"].get("relationship_tables", [])

    return {
      "success": True,
      "tool": tool_name,
      "result": schema,
    }

  else:
    for event in events:
      if event.get("event") == "result":
        return {
          "success": True,
          "tool": tool_name,
          "result": event["data"].get("result"),
        }

    return {
      "success": False,
      "error": "Unable to aggregate results",
      "tool": tool_name,
    }
